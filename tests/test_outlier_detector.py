from unittest.mock import MagicMock

import pytest

from src.models.bundle import MarketType
from src.models.market import ClassifiedMarket
from src.models.opportunity import ConstraintType
from src.scanner.outlier_detector import OutlierDetector


def _make_clob(history: list[dict] | None = None) -> MagicMock:
    """Create a mock ClobClient. Returns empty history by default (fallback to lastTradePrice)."""
    clob = MagicMock()
    clob.get_price_history.return_value = history or []
    return clob


def _make_market(
    team: str = "TestFC",
    market_type: MarketType = MarketType.WINNER,
    yes_ask_levels: list[tuple[float, float]] | None = None,
    no_ask_levels: list[tuple[float, float]] | None = None,
    yes_best_ask: float | None = None,
    no_best_ask: float | None = None,
    last_trade_price: float = 0.0,
    league: str = "EPL",
) -> ClassifiedMarket:
    return ClassifiedMarket(
        market_id=f"m-{team}-{market_type.value}",
        event_id="ev1",
        event_slug="test",
        league=league,
        team=team,
        market_type=market_type,
        yes_price=0.5,
        no_price=0.5,
        yes_token_id=f"tok-{team}-yes",
        no_token_id=f"tok-{team}-no",
        yes_best_ask=yes_best_ask,
        no_best_ask=no_best_ask,
        last_trade_price=last_trade_price,
        yes_ask_levels=yes_ask_levels or [],
        no_ask_levels=no_ask_levels or [],
        yes_ask_depth=sum(s for _, s in (yes_ask_levels or [])),
        no_ask_depth=sum(s for _, s in (no_ask_levels or [])),
    )


class TestLastTradeDetection:
    """Tests using lastTradePrice fallback (empty price history)."""

    def test_detects_ask_below_last_trade(self):
        """Ask at 80c when last trade was 99c -> outlier."""
        d = OutlierDetector(_make_clob(), min_gap_pct=0.05, min_gap_cents=3.0)
        market = _make_market(
            yes_ask_levels=[(0.80, 10), (0.99, 200)],
            yes_best_ask=0.80,
            last_trade_price=0.99,
        )
        opps = d.detect([market], "EPL")
        assert len(opps) == 1
        assert opps[0].constraint_type == ConstraintType.OUTLIER_ORDER
        info = opps[0].outlier_info
        assert info is not None
        assert info.side == "YES"
        assert len(info.levels) == 1
        assert info.levels[0].price_cents == pytest.approx(80.0, abs=0.1)
        assert info.levels[0].ref_cents == pytest.approx(99.0, abs=0.1)
        assert info.levels[0].gap_cents == pytest.approx(19.0, abs=0.1)

    def test_man_city_ucl_not_flagged(self):
        """Man City UCL: lastTrade=10c, ask=10c -> not an outlier."""
        d = OutlierDetector(_make_clob(), min_gap_pct=0.05, min_gap_cents=3.0)
        market = _make_market(
            yes_ask_levels=[(0.10, 50), (0.99, 200)],
            yes_best_ask=0.10,
            last_trade_price=0.10,
        )
        opps = d.detect([market], "EPL")
        assert len(opps) == 0

    def test_no_detection_when_gap_too_small(self):
        """Ask at 96c when last trade was 99c -> gap=3c, pct=3% < 5%."""
        d = OutlierDetector(_make_clob(), min_gap_pct=0.05, min_gap_cents=3.0)
        market = _make_market(
            yes_ask_levels=[(0.96, 100), (0.99, 200)],
            yes_best_ask=0.96,
            last_trade_price=0.99,
        )
        opps = d.detect([market], "EPL")
        assert len(opps) == 0

    def test_no_detection_when_gap_cents_too_small(self):
        """Low-price market: gap < 3c even if pct is large."""
        d = OutlierDetector(_make_clob(), min_gap_pct=0.05, min_gap_cents=3.0)
        market = _make_market(
            yes_ask_levels=[(0.003, 50), (0.01, 500)],
            yes_best_ask=0.003,
            last_trade_price=0.01,
        )
        opps = d.detect([market], "EPL")
        assert len(opps) == 0

    def test_no_detection_without_last_trade(self):
        """No lastTradePrice -> skip."""
        d = OutlierDetector(_make_clob())
        market = _make_market(
            yes_ask_levels=[(0.80, 10), (0.99, 200)],
            yes_best_ask=0.80,
            last_trade_price=0.0,
        )
        opps = d.detect([market], "EPL")
        assert len(opps) == 0

    def test_multiple_outliers(self):
        """Two asks both below last trade."""
        d = OutlierDetector(_make_clob(), min_gap_pct=0.05, min_gap_cents=3.0)
        market = _make_market(
            yes_ask_levels=[(0.80, 10), (0.81, 5), (0.99, 200)],
            yes_best_ask=0.80,
            last_trade_price=0.99,
        )
        opps = d.detect([market], "EPL")
        assert len(opps) == 1
        info = opps[0].outlier_info
        assert len(info.levels) == 2

    def test_ask_above_last_trade_not_flagged(self):
        """All asks above last trade -> nothing."""
        d = OutlierDetector(_make_clob(), min_gap_pct=0.05, min_gap_cents=3.0)
        market = _make_market(
            yes_ask_levels=[(0.50, 100), (0.55, 200)],
            yes_best_ask=0.50,
            last_trade_price=0.45,
        )
        opps = d.detect([market], "EPL")
        assert len(opps) == 0


class TestMedianRef:
    """Tests using 6h median price history as reference."""

    def test_uses_median_over_last_trade(self):
        """6h median=95c, lastTrade=90c, ask=80c -> ref=95c (from median), gap=15c -> outlier."""
        history = [{"t": i, "p": 0.95} for i in range(100)]
        d = OutlierDetector(_make_clob(history), min_gap_pct=0.05, min_gap_cents=3.0)
        market = _make_market(
            yes_ask_levels=[(0.80, 10), (0.96, 200)],
            yes_best_ask=0.80,
            last_trade_price=0.90,
        )
        opps = d.detect([market], "EPL")
        assert len(opps) == 1
        info = opps[0].outlier_info
        # ref should be median (95c), not lastTrade (90c)
        assert info.levels[0].ref_cents == pytest.approx(95.0, abs=0.1)

    def test_falls_back_to_last_trade_on_empty_history(self):
        """Empty history -> use lastTradePrice."""
        d = OutlierDetector(_make_clob([]), min_gap_pct=0.05, min_gap_cents=3.0)
        market = _make_market(
            yes_ask_levels=[(0.80, 10), (0.99, 200)],
            yes_best_ask=0.80,
            last_trade_price=0.99,
        )
        opps = d.detect([market], "EPL")
        assert len(opps) == 1
        assert opps[0].outlier_info.levels[0].ref_cents == pytest.approx(99.0, abs=0.1)

    def test_median_makes_low_ask_not_outlier(self):
        """6h median=50c, lastTrade=90c, ask=48c -> ref=50c, gap=2c < 3c -> not outlier."""
        history = [{"t": i, "p": 0.50} for i in range(100)]
        d = OutlierDetector(_make_clob(history), min_gap_pct=0.05, min_gap_cents=3.0)
        market = _make_market(
            yes_ask_levels=[(0.48, 10), (0.50, 200)],
            yes_best_ask=0.48,
            last_trade_price=0.90,
        )
        opps = d.detect([market], "EPL")
        # With lastTrade as ref: 90-48=42c -> flagged. With median: 50-48=2c < 3c -> not flagged
        assert len(opps) == 0


class TestNoSide:
    def test_no_side_uses_inverted_ref(self):
        """NO side ref = 1 - lastTradePrice. lastTrade=0.90 -> NO ref=0.10."""
        d = OutlierDetector(_make_clob(), min_gap_pct=0.05, min_gap_cents=3.0)
        market = _make_market(
            no_ask_levels=[(0.03, 50), (0.10, 200)],
            no_best_ask=0.03,
            last_trade_price=0.90,  # NO ref = 10c
        )
        opps = d.detect([market], "EPL")
        assert len(opps) == 1
        info = opps[0].outlier_info
        assert info.side == "NO"
        assert info.levels[0].ref_cents == pytest.approx(10.0, abs=0.1)


class TestCrossArb:
    def test_cross_arb_flagged(self):
        """YES ask + NO ask < 100c."""
        d = OutlierDetector(_make_clob(), min_gap_pct=0.05, min_gap_cents=3.0)
        market = _make_market(
            yes_ask_levels=[(0.15, 50), (0.50, 200)],
            yes_best_ask=0.15,
            no_best_ask=0.55,
            last_trade_price=0.50,
        )
        opps = d.detect([market], "EPL")
        assert len(opps) == 1
        info = opps[0].outlier_info
        assert info.cross_arb is True
        assert info.cross_arb_profit_cents > 0


class TestEmpty:
    def test_empty_levels(self):
        d = OutlierDetector(_make_clob())
        market = _make_market(last_trade_price=0.50)
        opps = d.detect([market], "EPL")
        assert len(opps) == 0
