from src.models.bundle import MarketType, TeamMarketBundle
from src.models.market import ClassifiedMarket
from src.scanner.team_resolver import TeamResolver


def _make_classified(
    team: str, market_type: MarketType, league: str = "EPL"
) -> ClassifiedMarket:
    return ClassifiedMarket(
        market_id=f"m-{team}-{market_type.value}",
        event_id="ev1",
        event_slug="test-event",
        league=league,
        team=team,
        market_type=market_type,
        yes_price=0.5,
        no_price=0.5,
    )


def test_normalize_aliases():
    r = TeamResolver()
    assert r.normalize("Man City") == "Manchester City"
    assert r.normalize("Man Utd") == "Manchester United"
    assert r.normalize("Nottm Forest") == "Nottingham Forest"
    assert r.normalize("Spurs") == "Tottenham Hotspur"


def test_normalize_already_canonical():
    r = TeamResolver()
    assert r.normalize("Manchester City") == "Manchester City"
    assert r.normalize("Arsenal") == "Arsenal"


def test_normalize_unknown_preserved():
    r = TeamResolver()
    assert r.normalize("Some New Club FC") == "Some New Club FC"


def test_group_by_team():
    r = TeamResolver()
    markets = [
        _make_classified("Manchester City", MarketType.WINNER),
        _make_classified("Man City", MarketType.TOP_4),
        _make_classified("Manchester City", MarketType.RELEGATION),
    ]
    bundles = r.group_by_team(markets)
    assert len(bundles) == 1
    bundle = bundles["Manchester City"]
    assert bundle.winner is not None
    assert bundle.top_4 is not None
    assert bundle.relegation is not None


def test_group_multiple_teams():
    r = TeamResolver()
    markets = [
        _make_classified("Manchester City", MarketType.WINNER),
        _make_classified("Arsenal", MarketType.WINNER),
        _make_classified("Man City", MarketType.TOP_4),
        _make_classified("Arsenal", MarketType.TOP_4),
    ]
    bundles = r.group_by_team(markets)
    assert len(bundles) == 2
    assert "Manchester City" in bundles
    assert "Arsenal" in bundles


def test_group_does_not_overwrite_first_market():
    """If a team has two markets of the same type, keep the first."""
    r = TeamResolver()
    m1 = _make_classified("Manchester City", MarketType.WINNER)
    m1.yes_price = 0.9
    m2 = _make_classified("Man City", MarketType.WINNER)
    m2.yes_price = 0.8
    bundles = r.group_by_team([m1, m2])
    assert bundles["Manchester City"].winner is not None
    assert bundles["Manchester City"].winner.yes_price == 0.9
