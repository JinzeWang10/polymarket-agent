import json

from src.alerts.formatter import AlertFormatter
from src.models.opportunity import ArbitrageOpportunity, ConstraintType


def test_format_opportunity_card():
    opp = ArbitrageOpportunity(
        constraint_type=ConstraintType.MUTUAL_EXCLUSION,
        team="Manchester City",
        league="EPL",
        description="top4 + relegation = 103.6%",
        markets_involved=["m1", "m2"],
        violation_pct=3.6,
        confidence="high",
        polymarket_urls=["https://polymarket.com/event/epl-top-4"],
        timestamp="2026-03-04T12:00:00Z",
    )
    cards = AlertFormatter().format_opportunities([opp])
    assert len(cards) == 1
    card_str = json.dumps(cards[0])
    assert "Manchester City" in card_str
    assert "103.6" in card_str


def test_format_empty():
    cards = AlertFormatter().format_opportunities([])
    assert len(cards) == 0


def test_format_groups_by_league():
    opps = [
        ArbitrageOpportunity(
            constraint_type=ConstraintType.MUTUAL_EXCLUSION,
            team="Team A", league="EPL", description="test1",
            violation_pct=3.0, timestamp="",
        ),
        ArbitrageOpportunity(
            constraint_type=ConstraintType.MARKET_SUM,
            team="Team B", league="La Liga", description="test2",
            violation_pct=5.0, timestamp="",
        ),
    ]
    cards = AlertFormatter().format_opportunities(opps)
    assert len(cards) == 2


def test_no_side_arb_includes_profit():
    opp = ArbitrageOpportunity(
        constraint_type=ConstraintType.NO_SIDE_ARB,
        team="Test", league="EPL", description="profit test",
        violation_pct=1.3, potential_profit_cents=1.3, timestamp="",
    )
    cards = AlertFormatter().format_opportunities([opp])
    card_str = json.dumps(cards[0])
    assert "1.3" in card_str
