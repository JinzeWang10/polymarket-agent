import httpx
import respx

from src.api.gamma import GammaClient
from src.config import LeagueConfig

BASE = "https://gamma-api.polymarket.com"


def _event(eid: str, slug: str = "") -> dict:
    return {
        "id": eid,
        "slug": slug or f"e-{eid}",
        "title": f"Event {eid}",
        "markets": [],
        "active": True,
        "closed": False,
    }


@respx.mock
def test_get_events_by_tag_single_page():
    respx.get(f"{BASE}/events").mock(
        return_value=httpx.Response(200, json=[_event("1", "epl-winner")])
    )
    client = GammaClient(BASE, httpx.Client())
    events = client.get_events_by_tag(tag_id=82)
    assert len(events) == 1
    assert events[0].slug == "epl-winner"


@respx.mock
def test_get_events_paginates():
    route = respx.get(f"{BASE}/events")
    route.side_effect = [
        httpx.Response(200, json=[_event(str(i)) for i in range(100)]),
        httpx.Response(200, json=[_event(str(i)) for i in range(100, 150)]),
    ]
    client = GammaClient(BASE, httpx.Client())
    events = client.get_events_by_tag(tag_id=82)
    assert len(events) == 150


@respx.mock
def test_get_all_events_deduplicates():
    route = respx.get(f"{BASE}/events")
    route.side_effect = [
        httpx.Response(200, json=[_event("1"), _event("2")]),
        httpx.Response(200, json=[_event("2"), _event("3")]),
    ]
    league = LeagueConfig(
        name="EPL",
        sport_id=2,
        tag_ids=[82, 306],
        season_slug_patterns=["*"],
        relegated_count=3,
        top_n_count=4,
    )
    client = GammaClient(BASE, httpx.Client())
    events = client.get_all_events_for_league(league)
    assert len(events) == 3


@respx.mock
def test_empty_response():
    respx.get(f"{BASE}/events").mock(return_value=httpx.Response(200, json=[]))
    client = GammaClient(BASE, httpx.Client())
    events = client.get_events_by_tag(tag_id=82)
    assert len(events) == 0
