import pytest

from src.config import ArbitrageThresholds
from src.models.bundle import MarketType, TeamMarketBundle
from src.models.market import ClassifiedMarket
from src.models.opportunity import ConstraintType
from src.scanner.arbitrage_detector import ArbitrageDetector


def _make_market(
    team: str,
    market_type: MarketType,
    yes_price: float = 0.0,
    no_price: float = 0.0,
    league: str = "EPL",
) -> ClassifiedMarket:
    return ClassifiedMarket(
        market_id=f"m-{team}-{market_type.value}",
        event_id="ev1",
        event_slug="test",
        league=league,
        team=team,
        market_type=market_type,
        yes_price=yes_price,
        no_price=no_price,
        yes_token_id=f"tok-{team}-{market_type.value}-yes",
        no_token_id=f"tok-{team}-{market_type.value}-no",
    )


def make_bundle(
    team: str,
    league: str = "EPL",
    winner_yes: float | None = None,
    winner_no: float | None = None,
    top4_yes: float | None = None,
    top4_no: float | None = None,
    relegation_yes: float | None = None,
    relegation_no: float | None = None,
    second_yes: float | None = None,
    second_no: float | None = None,
) -> TeamMarketBundle:
    bundle = TeamMarketBundle(team=team, league=league)
    if winner_yes is not None:
        bundle.winner = _make_market(
            team, MarketType.WINNER, winner_yes, winner_no or 0
        )
    if top4_yes is not None:
        bundle.top_4 = _make_market(
            team, MarketType.TOP_4, top4_yes, top4_no or 0
        )
    if relegation_yes is not None:
        bundle.relegation = _make_market(
            team, MarketType.RELEGATION, relegation_yes, relegation_no or 0
        )
    if second_yes is not None:
        bundle.second_place = _make_market(
            team, MarketType.SECOND_PLACE, second_yes, second_no or 0
        )
    return bundle


# ── Constraint 1: Mutual Exclusion ──


def test_mutual_exclusion_man_city_violated():
    """Real data: Man City top4=99.1% + relegation=4.5% = 103.6% > 100%"""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundle = make_bundle("Manchester City", top4_yes=0.991, relegation_yes=0.045)
    opps = det.check_mutual_exclusion(bundle)
    assert len(opps) >= 1
    assert opps[0].violation_pct == pytest.approx(3.6, abs=0.1)


def test_mutual_exclusion_arsenal_clean():
    """Arsenal top4=98.7% + relegation=1.0% = 99.7% ≤ 100%"""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundle = make_bundle("Arsenal", top4_yes=0.987, relegation_yes=0.01)
    opps = det.check_mutual_exclusion(bundle)
    assert len(opps) == 0


def test_mutual_exclusion_winner_second():
    """Arsenal winner=68% + second=33% = 101% > 100%"""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundle = make_bundle("Arsenal", winner_yes=0.68, second_yes=0.33)
    opps = det.check_mutual_exclusion(bundle)
    assert len(opps) >= 1


# ── Constraint 2: Subset ──


def test_subset_winner_exceeds_top4():
    """Artificial: winner=60% but top4=50% → violation"""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundle = make_bundle("TestTeam", winner_yes=0.60, top4_yes=0.50)
    opps = det.check_subset_constraint(bundle)
    assert len(opps) >= 1


def test_subset_winner_plus_second_exceeds_top4():
    """Arsenal: winner(68%) + second(33%) = 101% > top4(98.7%)"""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundle = make_bundle("Arsenal", winner_yes=0.68, second_yes=0.33, top4_yes=0.987)
    opps = det.check_subset_constraint(bundle)
    assert len(opps) >= 1
    assert opps[0].violation_pct == pytest.approx(2.3, abs=0.5)


def test_subset_clean():
    """winner=20% + second=15% = 35% < top4=90% → no violation"""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundle = make_bundle("TestTeam", winner_yes=0.20, second_yes=0.15, top4_yes=0.90)
    opps = det.check_subset_constraint(bundle)
    assert len(opps) == 0


# ── Constraint 3: Market Sum ──


def test_market_sum_overround():
    det = ArbitrageDetector(ArbitrageThresholds())
    bundles = {
        "TeamA": make_bundle("TeamA", winner_yes=0.60),
        "TeamB": make_bundle("TeamB", winner_yes=0.40),
        "TeamC": make_bundle("TeamC", winner_yes=0.20),
    }
    opps = det.check_market_sum(bundles, "EPL")
    # Sum = 120% → 20% overround
    winner_opps = [o for o in opps if "Winner" in o.description]
    assert len(winner_opps) == 1
    assert winner_opps[0].violation_pct == pytest.approx(20.0, abs=0.1)


# ── Constraint 4: No-Side Arb ──


def test_no_side_arb_original_report():
    """Original report: top4_no=1.3¢ + relegation_no=97.4¢ = 98.7¢ < 100¢"""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundle = make_bundle(
        "Manchester City",
        top4_yes=0.991, top4_no=0.013,
        relegation_yes=0.052, relegation_no=0.974,
    )
    opps = det.check_no_side_arbitrage(bundle)
    assert len(opps) == 1
    assert opps[0].potential_profit_cents == pytest.approx(1.3, abs=0.1)


def test_no_side_arb_current_no_arb():
    """top4_no=1.3¢ + relegation_no=99.6¢ = 100.9¢ > 100¢ → no arb"""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundle = make_bundle(
        "Manchester City",
        top4_yes=0.991, top4_no=0.013,
        relegation_yes=0.045, relegation_no=0.996,
    )
    opps = det.check_no_side_arbitrage(bundle)
    assert len(opps) == 0


# ── Constraint 5: Directional Mispricing ──


def test_directional_mispricing_man_city():
    """relegation_yes=4.5% but top4_no price=1.3¢ → ratio 3.46x"""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundle = make_bundle(
        "Manchester City",
        top4_yes=0.987, top4_no=0.013,
        relegation_yes=0.045, relegation_no=0.955,
    )
    opps = det.check_directional_mispricing(bundle)
    assert len(opps) == 1
    assert opps[0].violation_pct == pytest.approx(3.46, abs=0.5)


def test_directional_mispricing_not_triggered():
    """relegation_yes=1% vs top4_no=5% → ratio 0.2 → no trigger"""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundle = make_bundle(
        "TestTeam",
        top4_yes=0.95, top4_no=0.05,
        relegation_yes=0.01, relegation_no=0.99,
    )
    opps = det.check_directional_mispricing(bundle)
    assert len(opps) == 0


# ── Missing data edge case ──


def test_missing_market_graceful():
    """Team has top4 but no relegation market → skip mutual exclusion"""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundle = make_bundle("SomeTeam", top4_yes=0.80)
    opps = det.check_mutual_exclusion(bundle)
    assert len(opps) == 0


def test_detect_all_integrates():
    """detect_all runs all checks and filters by thresholds."""
    det = ArbitrageDetector(ArbitrageThresholds())
    bundles = {
        "Manchester City": make_bundle(
            "Manchester City",
            top4_yes=0.991, top4_no=0.013,
            relegation_yes=0.045, relegation_no=0.974,
        ),
    }
    opps = det.detect_all(bundles, "EPL")
    types = {o.constraint_type for o in opps}
    assert ConstraintType.MUTUAL_EXCLUSION in types
    assert ConstraintType.NO_SIDE_ARB in types
    assert ConstraintType.DIRECTIONAL_MISPRICING in types
