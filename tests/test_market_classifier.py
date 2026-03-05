from src.models.bundle import MarketType
from src.models.market import RawEvent, RawMarket
from src.scanner.market_classifier import MarketClassifier


def test_classify_winner_from_slug():
    c = MarketClassifier()
    assert c.detect_market_type("english-premier-league-winner", None) == MarketType.WINNER


def test_classify_top4_from_slug():
    c = MarketClassifier()
    assert c.detect_market_type("english-premier-league-top-4-finish", None) == MarketType.TOP_4


def test_classify_relegation_from_slug():
    c = MarketClassifier()
    assert c.detect_market_type("epl-which-clubs-get-relegated", None) == MarketType.RELEGATION


def test_classify_second_place():
    c = MarketClassifier()
    assert c.detect_market_type("epl-second-place-finish", None) == MarketType.SECOND_PLACE


def test_classify_unknown():
    c = MarketClassifier()
    assert c.detect_market_type("some-random-thing", None) == MarketType.UNKNOWN


def test_extract_team_from_group_item_title():
    c = MarketClassifier()
    m = RawMarket(
        id="1",
        question="Will Man City finish top 4?",
        slug="",
        outcomes=["Yes", "No"],
        outcome_prices=["0.991", "0.013"],
        clob_token_ids=["tok1", "tok2"],
        liquidity=1000,
        volume=500,
        active=True,
        closed=False,
        group_item_title="Manchester City",
    )
    assert c.extract_team_name(m) == "Manchester City"


def test_extract_team_from_question_fallback():
    c = MarketClassifier()
    m = RawMarket(
        id="1",
        question="Will Arsenal win the league?",
        outcomes=["Yes", "No"],
        outcome_prices=["0.5", "0.5"],
        clob_token_ids=["t1", "t2"],
    )
    assert c.extract_team_name(m) == "Arsenal"


def test_parse_prices():
    c = MarketClassifier()
    yes_p, no_p = c.parse_prices(["Yes", "No"], ["0.991", "0.013"])
    assert yes_p == 0.991
    assert no_p == 0.013


def test_is_season_long_event():
    c = MarketClassifier()
    season = RawEvent(id="1", slug="english-premier-league-winner", title="EPL Winner")
    matchday = RawEvent(id="2", slug="epl-2026-03-05-arsenal-vs-chelsea", title="Match")
    assert c.is_season_long_event(season) is True
    assert c.is_season_long_event(matchday) is False


def test_classify_event_full():
    c = MarketClassifier()
    event = RawEvent(
        id="ev1",
        slug="english-premier-league-top-4-finish",
        title="EPL Top 4",
        markets=[
            RawMarket(
                id="m1",
                question="Will Manchester City finish top 4?",
                outcomes=["Yes", "No"],
                outcome_prices=["0.991", "0.013"],
                clob_token_ids=["tok1", "tok2"],
                liquidity=5000,
                volume=10000,
                active=True,
                closed=False,
                group_item_title="Manchester City",
            ),
        ],
    )
    classified = c.classify_event(event, "EPL")
    assert len(classified) == 1
    assert classified[0].market_type == MarketType.TOP_4
    assert classified[0].team == "Manchester City"
    assert classified[0].yes_price == 0.991
    assert classified[0].no_price == 0.013
