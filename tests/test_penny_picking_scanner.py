"""Tests for PennyPickingScanner and DeduplicationTracker."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from src.models.penny_signal import PennyPickingSignal
from src.scanner.penny_picking_scanner import (
    DeduplicationTracker,
    PennyPickingScanner,
)


# endDate = tip-off time; 1 hour ago means game is in progress
_LIVE_END = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _raw_market(
    slug: str = "nba-nyk-bkn-2026-03-20",
    question: str = "Knicks vs. Nets",
    outcomes: str = '["Knicks", "Nets"]',
    prices: str = '["0.92", "0.08"]',
    tokens: str = '["tok-yes", "tok-no"]',
    liquidity: float = 5000,
    volume: float = 10000,
    event_slug: str = "nba-nyk-bkn",
    end_date: str = _LIVE_END,
) -> dict:
    return {
        "id": "m1",
        "slug": slug,
        "question": question,
        "outcomes": outcomes,
        "outcomePrices": prices,
        "clobTokenIds": tokens,
        "liquidity": str(liquidity),
        "volume": str(volume),
        "events": [{"slug": event_slug}],
        "endDate": end_date,
    }


def _mock_gamma(raw_markets: list[dict]) -> MagicMock:
    gamma = MagicMock()
    gamma.get_markets_by_tags.return_value = raw_markets
    return gamma


def _mock_clob(
    asks: list[dict] | None = None,
    bids: list[dict] | None = None,
) -> MagicMock:
    clob = MagicMock()
    clob.get_order_book.return_value = {
        "asks": asks or [],
        "bids": bids or [],
    }
    return clob


def _make_signal(
    slug: str = "nba-nyk-bkn",
    outcome: str = "Knicks",
    price_bucket: int = 97,
    ask_depth: float = 1000,
) -> PennyPickingSignal:
    return PennyPickingSignal(
        game_slug=slug,
        game_title="Knicks vs. Nets",
        sport="NBA",
        outcome=outcome,
        ask_price=price_bucket / 100,
        ask_depth=ask_depth,
        total_depth_in_range=ask_depth,
        token_id="tok-yes",
        timestamp="2026-03-20T10:00:00Z",
        price_bucket=price_bucket,
    )


# ---------------------------------------------------------------------------
# Pre-filter tests
# ---------------------------------------------------------------------------

class TestPreFilter:
    def test_pre_filter_nba_slug(self):
        """NBA slug is retained, non-sport slug is discarded."""
        raw = [
            _raw_market(slug="nba-nyk-bkn-2026-03-20", prices='["0.92", "0.08"]'),
            _raw_market(slug="presidential-election-2028", prices='["0.55", "0.45"]'),
        ]
        scanner = PennyPickingScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        candidates = scanner._pre_filter(raw)
        assert len(candidates) == 1
        assert candidates[0]["slug"] == "nba-nyk-bkn-2026-03-20"

    def test_pre_filter_football_excluded(self):
        """Football slugs are excluded — NBA only."""
        raw = [
            _raw_market(slug="epl-match-123", prices='["0.90", "0.10"]'),
            _raw_market(slug="laliga-match-456", prices='["0.90", "0.10"]'),
        ]
        scanner = PennyPickingScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        candidates = scanner._pre_filter(raw)
        assert len(candidates) == 0

    def test_pre_filter_future_enddate_skipped(self):
        """endDate in the future means game hasn't started — skip."""
        future_end = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        raw = [_raw_market(end_date=future_end, prices='["0.92", "0.08"]')]
        scanner = PennyPickingScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        candidates = scanner._pre_filter(raw)
        assert len(candidates) == 0

    def test_pre_filter_no_enddate_skipped(self):
        """Markets without endDate are filtered out."""
        raw = [_raw_market(end_date="", prices='["0.92", "0.08"]')]
        scanner = PennyPickingScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        candidates = scanner._pre_filter(raw)
        assert len(candidates) == 0

    def test_pre_filter_tipoff_1h_ago_kept(self):
        """endDate 1h ago = game tipped off 1h ago, still live — keep."""
        past_end = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        raw = [_raw_market(end_date=past_end, prices='["0.92", "0.08"]')]
        scanner = PennyPickingScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        candidates = scanner._pre_filter(raw)
        assert len(candidates) == 1

    def test_pre_filter_tipoff_too_long_ago_skipped(self):
        """endDate 6h ago = game long over — skip."""
        old_end = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        raw = [_raw_market(end_date=old_end, prices='["0.92", "0.08"]')]
        scanner = PennyPickingScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        candidates = scanner._pre_filter(raw)
        assert len(candidates) == 0

    def test_pre_filter_low_price_skipped(self):
        """Markets where all outcomes < pre_filter_price are filtered out."""
        raw = [_raw_market(prices='["0.50", "0.50"]')]
        scanner = PennyPickingScanner(
            _mock_gamma(raw), _mock_clob(), pre_filter_price=0.85, max_workers=1,
        )
        candidates = scanner._pre_filter(raw)
        assert len(candidates) == 0


# ---------------------------------------------------------------------------
# Signal detection tests
# ---------------------------------------------------------------------------

class TestSignalDetection:
    def test_signal_detected_at_97c(self):
        """97¢ ask with sufficient depth triggers a signal."""
        raw = [_raw_market(prices='["0.92", "0.08"]')]
        clob = _mock_clob(
            asks=[{"price": "0.97", "size": "200"}, {"price": "0.99", "size": "500"}],
            bids=[{"price": "0.95", "size": "100"}],
        )
        callback = MagicMock()
        scanner = PennyPickingScanner(
            _mock_gamma(raw), clob, on_signal=callback, max_workers=1,
        )
        signals = scanner.scan()
        assert len(signals) == 1
        assert signals[0].ask_price == 0.97
        assert signals[0].outcome == "Knicks"
        assert signals[0].price_bucket == 97

    def test_signal_skipped_below_threshold(self):
        """94¢ ask does not trigger (below min_ask_price=0.95)."""
        raw = [_raw_market(prices='["0.92", "0.08"]')]
        clob = _mock_clob(
            asks=[{"price": "0.94", "size": "500"}],
        )
        scanner = PennyPickingScanner(
            _mock_gamma(raw), clob, min_ask_price=0.95, max_workers=1,
        )
        signals = scanner.scan()
        assert len(signals) == 0

    def test_signal_skipped_low_depth(self):
        """Depth below min_depth_usd is skipped."""
        raw = [_raw_market(prices='["0.92", "0.08"]')]
        clob = _mock_clob(
            asks=[{"price": "0.97", "size": "10"}],  # 10 * 0.97 = $9.70 < $50
        )
        scanner = PennyPickingScanner(
            _mock_gamma(raw), clob, min_depth_usd=50, max_workers=1,
        )
        signals = scanner.scan()
        assert len(signals) == 0


# ---------------------------------------------------------------------------
# Deduplication tests
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_dedup_first_alert(self):
        tracker = DeduplicationTracker(cooldown_seconds=600)
        sig = _make_signal()
        assert tracker.should_alert(sig) is True

    def test_dedup_suppressed(self):
        tracker = DeduplicationTracker(cooldown_seconds=600)
        sig = _make_signal()
        tracker.should_alert(sig)
        assert tracker.should_alert(sig) is False

    def test_dedup_re_alert_after_cooldown(self):
        tracker = DeduplicationTracker(cooldown_seconds=0)  # instant cooldown
        sig = _make_signal()
        tracker.should_alert(sig)
        # With cooldown=0, next call should re-alert
        assert tracker.should_alert(sig) is True

    def test_dedup_re_alert_on_depth_change(self):
        tracker = DeduplicationTracker(cooldown_seconds=600, depth_change_pct=0.5)
        sig1 = _make_signal(ask_depth=1000)
        tracker.should_alert(sig1)
        # Depth changed by 100% (1000 → 2000) > 50% threshold
        sig2 = _make_signal(ask_depth=2000)
        assert tracker.should_alert(sig2) is True


# ---------------------------------------------------------------------------
# Scan window tests
# ---------------------------------------------------------------------------

class TestScanWindow:
    def test_is_scan_window(self):
        from src.penny_main import is_scan_window

        # No windows → always scan
        assert is_scan_window([]) is True

        # Matching window
        from unittest.mock import patch
        from datetime import datetime
        from zoneinfo import ZoneInfo

        fake_time = datetime(2026, 3, 20, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
        with patch("src.penny_main.datetime") as mock_dt:
            mock_dt.now.return_value = fake_time
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            windows = [{"start_hour": 8, "end_hour": 13, "label": "NBA"}]
            assert is_scan_window(windows) is True

        # Outside window
        fake_time = datetime(2026, 3, 20, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        with patch("src.penny_main.datetime") as mock_dt:
            mock_dt.now.return_value = fake_time
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            assert is_scan_window(windows) is False


# ---------------------------------------------------------------------------
# Callback test
# ---------------------------------------------------------------------------

class TestCallback:
    def test_callback_fires(self):
        """on_signal callback is called for each detected signal."""
        raw = [_raw_market(prices='["0.92", "0.08"]')]
        clob = _mock_clob(
            asks=[{"price": "0.97", "size": "200"}],
            bids=[{"price": "0.95", "size": "100"}],
        )
        callback = MagicMock()
        scanner = PennyPickingScanner(
            _mock_gamma(raw), clob, on_signal=callback, max_workers=1,
        )
        scanner.scan()
        assert callback.call_count == 1
        sig = callback.call_args[0][0]
        assert isinstance(sig, PennyPickingSignal)
        assert sig.sport == "NBA"


# ---------------------------------------------------------------------------
# Error handling test
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_clob_error_graceful(self):
        """CLOB exception for one market doesn't crash the scan."""
        raw = [
            _raw_market(slug="nba-a-b", prices='["0.92", "0.08"]', tokens='["t1", "t2"]'),
            _raw_market(slug="nba-c-d", prices='["0.95", "0.05"]', tokens='["t3", "t4"]'),
        ]
        clob = MagicMock()
        call_count = {"n": 0}

        def flaky_book(token_id: str) -> dict:
            call_count["n"] += 1
            if token_id == "t1":
                raise ConnectionError("timeout")
            return {
                "asks": [{"price": "0.97", "size": "200"}],
                "bids": [{"price": "0.95", "size": "100"}],
            }

        clob.get_order_book.side_effect = flaky_book
        scanner = PennyPickingScanner(
            _mock_gamma(raw), clob, max_workers=1,
        )
        # Should not raise
        signals = scanner.scan()
        # The second market should still produce a signal
        assert any(s.ask_price == 0.97 for s in signals)
