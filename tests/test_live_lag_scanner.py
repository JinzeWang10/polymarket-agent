"""Tests for LiveLagScanner — in-play match moves vs stale structural markets."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from src.config import WorldCupStage
from src.models.market import RawEvent
from src.models.opportunity import ConstraintType
from src.scanner.live_lag_scanner import LiveLagScanner

ADVANCE = WorldCupStage(slug="wc-advance", slots=2, level=1, label="出线")

_T0 = 1_750_000_000  # arbitrary epoch base for history points


def _hist(prices: list[float], step: int = 60) -> list[dict]:
    return [{"t": _T0 + i * step, "p": p} for i, p in enumerate(prices)]


# 11 minute-points: flat 60¢ for 10 min, then +10¢ in the last 10 minutes
RISING = _hist([0.60] * 5 + [0.60, 0.62, 0.64, 0.66, 0.68, 0.70])
FALLING = _hist([0.60] * 5 + [0.60, 0.58, 0.55, 0.52, 0.50, 0.48])
FLAT = _hist([0.30] * 11)
DRIFT = _hist([0.30] * 5 + [0.30, 0.31, 0.32, 0.33, 0.34, 0.34])  # +4¢ moved


def _match_market(mid: str, title: str) -> dict:
    return {
        "id": mid,
        "question": f"Will {title} win?",
        "slug": f"slug-{mid}",
        "groupItemTitle": title,
        "active": True,
        "closed": False,
        "clobTokenIds": json.dumps([f"yes-{mid}", f"no-{mid}"]),
    }


def _match_event(hours_since_kickoff: float = 1.0) -> RawEvent:
    kickoff = datetime.now(timezone.utc) - timedelta(hours=hours_since_kickoff)
    return RawEvent.model_validate({
        "id": "ev-match",
        "slug": "fifwc-mex-rsa-2026-06-11",
        "title": "Mexico vs. South Africa",
        "endDate": kickoff.isoformat().replace("+00:00", "Z"),
        "markets": [
            _match_market("match-mex", "Mexico"),
            _match_market("match-rsa", "South Africa"),
            _match_market("match-draw", "Draw (Mexico vs. South Africa)"),
        ],
    })


def _struct_event() -> RawEvent:
    def m(mid: str, nation: str) -> dict:
        return {
            "id": mid,
            "question": f"Will {nation} advance?",
            "slug": f"slug-{mid}",
            "groupItemTitle": nation,
            "bestAsk": 0.32,
            "bestBid": 0.28,
            "active": True,
            "closed": False,
            "clobTokenIds": json.dumps([f"yes-{mid}", f"no-{mid}"]),
        }

    return RawEvent.model_validate({
        "id": "ev-advance",
        "slug": "wc-advance",
        "title": "Advance to Knockouts",
        "markets": [m("adv-mex", "Mexico"), m("adv-rsa", "South Africa")],
    })


def _mock_gamma(match_events: list[RawEvent]) -> MagicMock:
    gamma = MagicMock()
    gamma.get_events_by_tag.return_value = match_events
    gamma.get_event_by_slug.side_effect = (
        lambda slug: _struct_event() if slug == "wc-advance" else None
    )
    return gamma


def _mock_clob(
    histories: dict[str, list[dict]],
    books: dict[str, list[dict]] | None = None,
) -> MagicMock:
    clob = MagicMock()
    clob.get_price_history.side_effect = (
        lambda token_id, **kwargs: histories.get(token_id, [])
    )
    books = books or {}
    clob.get_order_book.side_effect = (
        lambda token_id: {"asks": books.get(token_id, []), "bids": []}
    )
    return clob


def _scanner(gamma, clob) -> LiveLagScanner:
    return LiveLagScanner(
        gamma,
        clob,
        stages=[ADVANCE],
        group_slugs=[],
        match_move_cents=5.0,
        struct_move_cents=1.5,
        window_minutes=10,
        min_depth_usd=50.0,
        cooldown_seconds=900,
        max_workers=1,
    )


class TestLiveLag:
    def test_rising_match_stale_struct_buy_yes(self):
        clob = _mock_clob(
            histories={
                "yes-match-mex": RISING,
                "yes-match-rsa": FLAT,
                "yes-adv-mex": FLAT,
            },
            books={"yes-adv-mex": [{"price": "0.32", "size": "1000"}]},
        )
        scanner = _scanner(_mock_gamma([_match_event()]), clob)
        signals = scanner.scan()

        assert len(signals) == 1
        sig = signals[0]
        assert sig.constraint_type == ConstraintType.LIVE_LAG
        assert sig.team == "Mexico"
        assert sig.token_ids == ["yes-adv-mex"]
        assert "YES" in sig.description
        assert sig.violation_pct == 10.0

    def test_falling_match_stale_struct_buy_no(self):
        clob = _mock_clob(
            histories={
                "yes-match-mex": FALLING,
                "yes-match-rsa": FLAT,
                "yes-adv-mex": FLAT,
            },
            books={"no-adv-mex": [{"price": "0.75", "size": "1000"}]},
        )
        scanner = _scanner(_mock_gamma([_match_event()]), clob)
        signals = scanner.scan()

        assert len(signals) == 1
        assert signals[0].token_ids == ["no-adv-mex"]
        assert "NO" in signals[0].description

    def test_struct_already_repriced_no_signal(self):
        clob = _mock_clob(
            histories={
                "yes-match-mex": RISING,
                "yes-match-rsa": FLAT,
                "yes-adv-mex": DRIFT,  # moved 4¢ ≥ 1.5¢ threshold
            },
        )
        scanner = _scanner(_mock_gamma([_match_event()]), clob)
        assert scanner.scan() == []
        clob.get_order_book.assert_not_called()

    def test_no_live_matches_fast_exit(self):
        """Match kicked off 5h ago → outside live window, nothing scanned."""
        clob = _mock_clob(histories={})
        scanner = _scanner(_mock_gamma([_match_event(hours_since_kickoff=5)]), clob)
        assert scanner.scan() == []
        clob.get_price_history.assert_not_called()

    def test_sub_events_ignored(self):
        """Suffixed slugs (more-markets etc.) don't count as match events."""
        ev = _match_event()
        ev.slug = "fifwc-mex-rsa-2026-06-11-more-markets"
        clob = _mock_clob(histories={})
        scanner = _scanner(_mock_gamma([ev]), clob)
        assert scanner.scan() == []
        clob.get_price_history.assert_not_called()

    def test_cooldown_suppresses_repeat(self):
        clob = _mock_clob(
            histories={
                "yes-match-mex": RISING,
                "yes-match-rsa": FLAT,
                "yes-adv-mex": FLAT,
            },
            books={"yes-adv-mex": [{"price": "0.32", "size": "1000"}]},
        )
        scanner = _scanner(_mock_gamma([_match_event()]), clob)
        assert len(scanner.scan()) == 1
        assert scanner.scan() == []

    def test_thin_book_rejected(self):
        clob = _mock_clob(
            histories={
                "yes-match-mex": RISING,
                "yes-match-rsa": FLAT,
                "yes-adv-mex": FLAT,
            },
            books={"yes-adv-mex": [{"price": "0.32", "size": "10"}]},  # $3.2
        )
        scanner = _scanner(_mock_gamma([_match_event()]), clob)
        assert scanner.scan() == []
