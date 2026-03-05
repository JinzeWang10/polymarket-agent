import httpx
import respx

from src.alerts.feishu import FeishuAlerter
from src.alerts.formatter import AlertFormatter
from src.api.gamma import GammaClient
from src.config import ArbitrageThresholds, LeagueConfig
from src.models.opportunity import ConstraintType
from src.scanner.arbitrage_detector import ArbitrageDetector
from src.scanner.market_classifier import MarketClassifier
from src.scanner.market_fetcher import MarketFetcher
from src.scanner.pipeline import ScanPipeline
from src.scanner.team_resolver import TeamResolver

BASE = "https://gamma-api.polymarket.com"
FEISHU_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/test"


def _make_market_json(
    mid: str, team: str, yes_price: str, no_price: str
) -> dict:
    return {
        "id": mid,
        "question": f"Will {team} finish?",
        "slug": "",
        "outcomes": ["Yes", "No"],
        "outcomePrices": [yes_price, no_price],
        "clobTokenIds": [f"tok-{mid}-yes", f"tok-{mid}-no"],
        "liquidity": 5000,
        "volume": 10000,
        "active": True,
        "closed": False,
        "groupItemTitle": team,
    }


@respx.mock
def test_full_pipeline_detects_man_city_contradiction():
    """Feed realistic EPL events through full pipeline."""
    # Top 4 event
    top4_event = {
        "id": "ev-top4",
        "slug": "english-premier-league-top-4-finish",
        "title": "EPL Top 4",
        "markets": [
            _make_market_json("m-top4-mancity", "Manchester City", "0.991", "0.013"),
        ],
        "active": True,
        "closed": False,
    }
    # Relegation event
    relegation_event = {
        "id": "ev-rel",
        "slug": "epl-which-clubs-get-relegated",
        "title": "EPL Relegation",
        "markets": [
            _make_market_json("m-rel-mancity", "Manchester City", "0.045", "0.974"),
        ],
        "active": True,
        "closed": False,
    }

    route = respx.get(f"{BASE}/events")
    route.side_effect = [
        httpx.Response(200, json=[top4_event, relegation_event]),
    ]
    respx.post(FEISHU_URL).mock(return_value=httpx.Response(200, json={"code": 0}))

    league = LeagueConfig(
        name="EPL", sport_id=2, tag_ids=[82],
        season_slug_patterns=["english-premier-league-*", "epl-*"],
        relegated_count=3, top_n_count=4,
    )
    http = httpx.Client()
    gamma = GammaClient(BASE, http)
    fetcher = MarketFetcher(gamma, [league])
    classifier = MarketClassifier()
    resolver = TeamResolver()
    detector = ArbitrageDetector(ArbitrageThresholds())
    alerter = FeishuAlerter(FEISHU_URL, http)
    formatter = AlertFormatter()

    pipeline = ScanPipeline(fetcher, classifier, resolver, detector, alerter, formatter)
    opps = pipeline.run()

    man_city_opps = [o for o in opps if o.team == "Manchester City"]
    assert len(man_city_opps) >= 1
    types = {o.constraint_type for o in man_city_opps}
    assert ConstraintType.MUTUAL_EXCLUSION in types


@respx.mock
def test_pipeline_dedup_suppresses_repeat():
    """Second run with same data should not re-alert."""
    event = {
        "id": "ev1",
        "slug": "english-premier-league-top-4-finish",
        "title": "EPL Top 4",
        "markets": [
            _make_market_json("m1", "Manchester City", "0.991", "0.013"),
        ],
        "active": True,
        "closed": False,
    }
    rel_event = {
        "id": "ev2",
        "slug": "epl-which-clubs-get-relegated",
        "title": "EPL Relegation",
        "markets": [
            _make_market_json("m2", "Manchester City", "0.045", "0.974"),
        ],
        "active": True,
        "closed": False,
    }

    league = LeagueConfig(
        name="EPL", sport_id=2, tag_ids=[82],
        season_slug_patterns=["english-premier-league-*", "epl-*"],
        relegated_count=3, top_n_count=4,
    )
    http = httpx.Client()
    gamma = GammaClient(BASE, http)
    fetcher = MarketFetcher(gamma, [league])
    classifier = MarketClassifier()
    resolver = TeamResolver()
    detector = ArbitrageDetector(ArbitrageThresholds())
    alerter = FeishuAlerter(FEISHU_URL, http)
    formatter = AlertFormatter()
    pipeline = ScanPipeline(fetcher, classifier, resolver, detector, alerter, formatter)

    # First run
    route = respx.get(f"{BASE}/events")
    route.side_effect = [httpx.Response(200, json=[event, rel_event])]
    feishu_route = respx.post(FEISHU_URL).mock(
        return_value=httpx.Response(200, json={"code": 0})
    )
    pipeline.run()
    first_call_count = feishu_route.call_count

    # Second run with same data — dedup should suppress
    route.side_effect = [httpx.Response(200, json=[event, rel_event])]
    pipeline.run()
    # Feishu should not have been called again (dedup)
    assert feishu_route.call_count == first_call_count
