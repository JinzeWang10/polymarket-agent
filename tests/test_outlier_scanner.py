"""Tests for OutlierScanner — concurrent enrich + streaming detection."""
from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from src.models.opportunity import ConstraintType
from src.scanner.outlier_scanner import OutlierScanner


def _raw_market(
    mid: str = "m1",
    ltp: float = 0.95,
    best_ask: float = 0.90,
    liquidity: float = 1000,
    question: str = "Will X win?",
) -> dict:
    return {
        "id": mid,
        "lastTradePrice": str(ltp),
        "bestAsk": str(best_ask),
        "liquidity": str(liquidity),
        "outcomePrices": f'["{ltp}", "{1 - ltp}"]',
        "clobTokenIds": f'["yes-{mid}", "no-{mid}"]',
        "slug": f"slug-{mid}",
        "groupItemTitle": "",
        "question": question,
        "volume": "5000",
    }


def _mock_gamma(raw_markets: list[dict]) -> MagicMock:
    gamma = MagicMock()
    gamma.get_all_active_markets.return_value = raw_markets
    return gamma


def _mock_clob(
    yes_asks: list[dict] | None = None,
    no_asks: list[dict] | None = None,
    history: list[dict] | None = None,
) -> MagicMock:
    clob = MagicMock()

    def fake_book(token_id: str) -> dict:
        if token_id.startswith("yes-"):
            return {"asks": yes_asks or [], "bids": []}
        return {"asks": no_asks or [], "bids": []}

    clob.get_order_book.side_effect = fake_book
    clob.get_price_history.return_value = history or []
    return clob


class TestPreFilter:
    def test_yes_side_candidate(self):
        """ltp=0.95, bestAsk=0.90 → gap 5.3% > 3% → YES candidate."""
        raw = [_raw_market(ltp=0.95, best_ask=0.90)]
        scanner = OutlierScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        yes, no = scanner._pre_filter(raw)
        assert len(yes) == 1
        assert len(no) == 0

    def test_no_side_candidate(self):
        """ltp=0.05 → NO ref=0.95 ≥ 0.80 → NO candidate."""
        raw = [_raw_market(ltp=0.05, best_ask=0.05)]
        scanner = OutlierScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        yes, no = scanner._pre_filter(raw)
        assert len(yes) == 0
        assert len(no) == 1

    def test_neither_side(self):
        """ltp=0.50 → YES ref < 0.80, NO ref < 0.80 → skip."""
        raw = [_raw_market(ltp=0.50, best_ask=0.50)]
        scanner = OutlierScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        yes, no = scanner._pre_filter(raw)
        assert len(yes) == 0
        assert len(no) == 0

    def test_yes_no_gap_too_small(self):
        """ltp=0.95, bestAsk=0.94 → gap 1% < 3% → not YES candidate."""
        raw = [_raw_market(ltp=0.95, best_ask=0.94)]
        scanner = OutlierScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        yes, no = scanner._pre_filter(raw)
        assert len(yes) == 0

    def test_no_candidates_sorted_by_liquidity(self):
        """NO candidates sorted by liquidity descending."""
        raw = [
            _raw_market(mid="a", ltp=0.05, best_ask=0.05, liquidity=100),
            _raw_market(mid="b", ltp=0.03, best_ask=0.03, liquidity=5000),
            _raw_market(mid="c", ltp=0.10, best_ask=0.10, liquidity=2000),
        ]
        scanner = OutlierScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        _, no = scanner._pre_filter(raw)
        assert [m["id"] for m in no] == ["b", "c", "a"]


class TestStreamingCallback:
    def test_callback_called_per_signal(self):
        """on_signal fires for each detected outlier."""
        raw = [_raw_market(mid="m1", ltp=0.95, best_ask=0.85)]
        # YES orderbook has cheap ask at 0.85
        clob = _mock_clob(
            yes_asks=[{"price": "0.85", "size": "100"}, {"price": "0.96", "size": "500"}],
        )
        callback = MagicMock()
        scanner = OutlierScanner(
            _mock_gamma(raw), clob,
            on_signal=callback, max_workers=1,
        )
        opps = scanner.scan()
        assert len(opps) == 1
        assert callback.call_count == 1
        opp = callback.call_args[0][0]
        assert opp.constraint_type == ConstraintType.OUTLIER_ORDER
        assert opp.outlier_info.side == "YES"

    def test_no_callback_when_no_signal(self):
        """No outliers → callback never called."""
        raw = [_raw_market(mid="m1", ltp=0.95, best_ask=0.94)]
        clob = _mock_clob(
            yes_asks=[{"price": "0.95", "size": "500"}],
        )
        callback = MagicMock()
        scanner = OutlierScanner(
            _mock_gamma(raw), clob,
            on_signal=callback, max_workers=1,
        )
        opps = scanner.scan()
        assert len(opps) == 0
        assert callback.call_count == 0


class TestNoSideDetection:
    def test_no_side_outlier_detected(self):
        """NO side: ltp=0.05 → NO ref=0.95, NO ask at 0.85 → outlier."""
        raw = [_raw_market(mid="m1", ltp=0.05, best_ask=0.05)]
        clob = _mock_clob(
            no_asks=[{"price": "0.85", "size": "200"}, {"price": "0.96", "size": "500"}],
        )
        callback = MagicMock()
        scanner = OutlierScanner(
            _mock_gamma(raw), clob,
            on_signal=callback, max_workers=1,
        )
        opps = scanner.scan()
        assert len(opps) == 1
        assert opps[0].outlier_info.side == "NO"

    def test_no_only_enrichment_skips_yes(self):
        """NO-side candidates should not fetch YES orderbook."""
        raw = [_raw_market(mid="m1", ltp=0.05, best_ask=0.05)]
        clob = _mock_clob(
            no_asks=[{"price": "0.85", "size": "200"}],
        )
        scanner = OutlierScanner(
            _mock_gamma(raw), clob, max_workers=1,
        )
        scanner.scan()
        # Should only call get_order_book for the NO token
        token_ids = [c.kwargs.get("token_id") or c.args[0]
                     for c in clob.get_order_book.call_args_list]
        assert all(tid.startswith("no-") for tid in token_ids)


class TestCrossArb:
    def test_cross_arb_detected(self):
        """YES ask 0.40 + NO ask 0.40 < 1.0 → cross-arb."""
        raw = [_raw_market(mid="m1", ltp=0.95, best_ask=0.40)]
        clob = _mock_clob(
            yes_asks=[{"price": "0.40", "size": "100"}, {"price": "0.96", "size": "500"}],
            no_asks=[{"price": "0.40", "size": "100"}],
        )
        scanner = OutlierScanner(
            _mock_gamma(raw), clob, max_workers=1,
        )
        opps = scanner.scan()
        assert len(opps) >= 1
        cross = [o for o in opps if o.outlier_info and o.outlier_info.cross_arb]
        assert len(cross) >= 1
        assert cross[0].confidence == "high"


class TestMedianRef:
    def test_uses_median_when_available(self):
        """6h median=0.95, ltp=0.90, ask=0.85 → ref=0.95, gap=10c."""
        raw = [_raw_market(mid="m1", ltp=0.90, best_ask=0.85)]
        history = [{"t": i, "p": 0.95} for i in range(50)]
        clob = _mock_clob(
            yes_asks=[{"price": "0.85", "size": "100"}, {"price": "0.96", "size": "500"}],
            history=history,
        )
        scanner = OutlierScanner(
            _mock_gamma(raw), clob, max_workers=1,
        )
        opps = scanner.scan()
        assert len(opps) == 1
        # ref should be median 95c, not ltp 90c
        assert opps[0].outlier_info.levels[0].ref_cents == pytest.approx(95.0, abs=0.1)


class TestEnrichmentFailure:
    def test_enrich_failure_skips_market(self):
        """If CLOB call fails, market is skipped gracefully."""
        raw = [_raw_market(mid="m1", ltp=0.95, best_ask=0.85)]
        clob = _mock_clob()
        clob.get_order_book.side_effect = Exception("connection timeout")
        scanner = OutlierScanner(
            _mock_gamma(raw), clob, max_workers=1,
        )
        opps = scanner.scan()
        assert len(opps) == 0


class TestEmptyScan:
    def test_no_markets(self):
        scanner = OutlierScanner(_mock_gamma([]), _mock_clob(), max_workers=1)
        assert scanner.scan() == []

    def test_all_filtered_out(self):
        """ltp=0.50 → neither side qualifies."""
        raw = [_raw_market(ltp=0.50, best_ask=0.50)]
        scanner = OutlierScanner(_mock_gamma(raw), _mock_clob(), max_workers=1)
        assert scanner.scan() == []
