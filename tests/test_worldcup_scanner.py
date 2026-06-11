"""Tests for WorldCupScanner — stage-chain and slot-sum structural arbitrage."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

from src.config import WorldCupStage
from src.models.market import RawEvent
from src.models.opportunity import ConstraintType
from src.scanner.worldcup_scanner import WorldCupScanner

WINNER = WorldCupStage(slug="wc-winner", slots=1, level=2, label="夺冠")
FINAL = WorldCupStage(slug="wc-final", slots=2, level=1, label="进决赛")


def _market(
    mid: str,
    nation: str,
    ask: float,
    bid: float,
    closed: bool = False,
    outcome_prices: list[str] | None = None,
) -> dict:
    return {
        "id": mid,
        "question": f"Will {nation}?",
        "slug": f"slug-{mid}",
        "groupItemTitle": nation,
        "bestAsk": ask,
        "bestBid": bid,
        "active": not closed,
        "closed": closed,
        "outcomePrices": json.dumps(outcome_prices or [str(bid), str(1 - bid)]),
        "clobTokenIds": json.dumps([f"yes-{mid}", f"no-{mid}"]),
    }


def _event(slug: str, title: str, markets: list[dict]) -> RawEvent:
    return RawEvent.model_validate(
        {"id": f"ev-{slug}", "slug": slug, "title": title, "markets": markets}
    )


def _mock_gamma(events: dict[str, RawEvent]) -> MagicMock:
    gamma = MagicMock()
    gamma.get_event_by_slug.side_effect = lambda slug: events.get(slug)
    return gamma


def _mock_clob(books: dict[str, list[dict]] | None = None) -> MagicMock:
    """books maps token_id → asks list."""
    clob = MagicMock()
    books = books or {}

    def fake_book(token_id: str) -> dict:
        return {"asks": books.get(token_id, []), "bids": []}

    clob.get_order_book.side_effect = fake_book
    return clob


def _scanner(gamma, clob, *, group_slugs: list[str] | None = None) -> WorldCupScanner:
    return WorldCupScanner(
        gamma,
        clob,
        stages=[WINNER, FINAL],
        group_slugs=group_slugs or [],
        min_edge_cents=1.0,
        min_sum_edge_cents=5.0,
        min_depth_usd=50.0,
        max_workers=1,
    )


def _consistent_universe() -> dict[str, RawEvent]:
    """Two nations priced with no structural contradiction."""
    return {
        "wc-winner": _event("wc-winner", "World Cup Winner", [
            _market("win-esp", "Spain", ask=0.55, bid=0.50),
            _market("win-fra", "France", ask=0.50, bid=0.45),
        ]),
        "wc-final": _event("wc-final", "Reach Final", [
            _market("fin-esp", "Spain", ask=0.60, bid=0.55),
            _market("fin-fra", "France", ask=0.55, bid=0.50),
        ]),
    }


class TestStageChain:
    def _violating_universe(self) -> dict[str, RawEvent]:
        """Spain: winner bid 85¢ but reach-final ask only 15¢ → inverted."""
        return {
            "wc-winner": _event("wc-winner", "World Cup Winner", [
                _market("win-esp", "Spain", ask=0.90, bid=0.85),
                _market("win-fra", "France", ask=0.20, bid=0.15),
            ]),
            "wc-final": _event("wc-final", "Reach Final", [
                _market("fin-esp", "Spain", ask=0.15, bid=0.10),
                _market("fin-fra", "France", ask=0.30, bid=0.25),
            ]),
        }

    def test_chain_violation_detected(self):
        clob = _mock_clob({
            "yes-fin-esp": [{"price": "0.15", "size": "1000"}],
            "no-win-esp": [{"price": "0.20", "size": "1000"}],
        })
        scanner = _scanner(_mock_gamma(self._violating_universe()), clob)
        opps = scanner.scan()

        assert len(opps) == 1
        opp = opps[0]
        assert opp.constraint_type == ConstraintType.SUBSET_VIOLATION
        assert opp.team == "Spain"
        assert opp.potential_profit_cents == 65.0  # 100 - (15 + 20)
        assert opp.token_ids == ["yes-fin-esp", "no-win-esp"]
        assert opp.confidence == "high"

    def test_consistent_prices_no_signal_no_clob_calls(self):
        clob = _mock_clob()
        scanner = _scanner(_mock_gamma(_consistent_universe()), clob)
        opps = scanner.scan()
        assert opps == []
        clob.get_order_book.assert_not_called()

    def test_chain_rejected_when_clob_disagrees(self):
        """Gamma screen triggers but real book has no edge → no signal."""
        clob = _mock_clob({
            "yes-fin-esp": [{"price": "0.55", "size": "1000"}],
            "no-win-esp": [{"price": "0.50", "size": "1000"}],
        })
        scanner = _scanner(_mock_gamma(self._violating_universe()), clob)
        assert scanner.scan() == []

    def test_chain_rejected_on_thin_depth(self):
        """Edge exists but executable size is below min_depth_usd."""
        clob = _mock_clob({
            "yes-fin-esp": [{"price": "0.15", "size": "10"}],  # $1.5 depth
            "no-win-esp": [{"price": "0.20", "size": "1000"}],
        })
        scanner = _scanner(_mock_gamma(self._violating_universe()), clob)
        assert scanner.scan() == []

    def test_nation_alias_joins_across_events(self):
        """'Curaçao' in winner event matches 'Curacao' in final event."""
        events = {
            "wc-winner": _event("wc-winner", "World Cup Winner", [
                _market("win-cur", "Curaçao", ask=0.90, bid=0.85),
                _market("win-fil", "Filler", ask=0.55, bid=0.10),
            ]),
            "wc-final": _event("wc-final", "Reach Final", [
                _market("fin-cur", "Curacao", ask=0.15, bid=0.10),
            ]),
        }
        clob = _mock_clob({
            "yes-fin-cur": [{"price": "0.15", "size": "1000"}],
            "no-win-cur": [{"price": "0.20", "size": "1000"}],
        })
        scanner = _scanner(_mock_gamma(events), clob)
        opps = scanner.scan()
        chain = [o for o in opps if o.constraint_type == ConstraintType.SUBSET_VIOLATION]
        assert len(chain) == 1
        assert chain[0].team == "Curacao"

    def test_dedup_suppresses_repeat_signals(self):
        clob = _mock_clob({
            "yes-fin-esp": [{"price": "0.15", "size": "1000"}],
            "no-win-esp": [{"price": "0.20", "size": "1000"}],
        })
        scanner = _scanner(_mock_gamma(self._violating_universe()), clob)
        assert len(scanner.scan()) == 1
        assert scanner.scan() == []


class TestSlotSum:
    def test_group_buy_all_yes_violation(self):
        """Group winner asks sum to 90¢ < 100¢ slot → MARKET_SUM signal."""
        group = _event("wc-group-a", "Group A Winner", [
            _market("ga-1", "A1", ask=0.30, bid=0.28),
            _market("ga-2", "A2", ask=0.25, bid=0.23),
            _market("ga-3", "A3", ask=0.20, bid=0.18),
            _market("ga-4", "A4", ask=0.10, bid=0.08),
            _market("ga-5", "Other", ask=0.05, bid=0.03),
        ])
        events = {**_consistent_universe(), "wc-group-a": group}
        books = {
            f"yes-ga-{i}": [{"price": str(p), "size": "2000"}]
            for i, p in zip(range(1, 6), [0.30, 0.25, 0.20, 0.10, 0.05])
        }
        scanner = _scanner(
            _mock_gamma(events), _mock_clob(books), group_slugs=["wc-group-a"],
        )
        opps = scanner.scan()

        assert len(opps) == 1
        opp = opps[0]
        assert opp.constraint_type == ConstraintType.MARKET_SUM
        assert opp.team == "Group A Winner"
        assert opp.potential_profit_cents == 10.0
        assert len(opp.token_ids) == 5

    def test_resolved_yes_disables_sum_check(self):
        """A group with its winner already resolved has no remaining slots."""
        group = _event("wc-group-a", "Group A Winner", [
            _market("ga-1", "A1", ask=0.0, bid=0.0, closed=True,
                    outcome_prices=["1", "0"]),
            _market("ga-2", "A2", ask=0.01, bid=0.005),
            _market("ga-3", "A3", ask=0.01, bid=0.005),
            _market("ga-4", "A4", ask=0.01, bid=0.005),
        ])
        events = {**_consistent_universe(), "wc-group-a": group}
        scanner = _scanner(
            _mock_gamma(events), _mock_clob(), group_slugs=["wc-group-a"],
        )
        assert scanner.scan() == []


class TestParsing:
    def test_closed_markets_excluded_from_legs(self):
        events = _consistent_universe()
        events["wc-winner"].markets[0].closed = True
        scanner = _scanner(_mock_gamma(events), _mock_clob())
        stage_events, _ = scanner._fetch_events()
        winner = next(e for e in stage_events if e.slug == "wc-winner")
        assert [leg.nation for leg in winner.legs] == ["France"]

    def test_missing_event_skipped(self):
        events = {"wc-winner": _consistent_universe()["wc-winner"]}
        scanner = _scanner(_mock_gamma(events), _mock_clob())
        assert scanner.scan() == []
