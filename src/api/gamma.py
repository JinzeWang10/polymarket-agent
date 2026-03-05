from __future__ import annotations

import structlog
import httpx

from src.config import LeagueConfig
from src.models.market import RawEvent

log = structlog.get_logger()

DEFAULT_PAGE_SIZE = 100


class GammaClient:
    def __init__(self, base_url: str, http: httpx.Client) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http

    def get_events_by_tag(
        self,
        tag_id: int,
        *,
        active: bool = True,
        closed: bool = False,
        limit: int = DEFAULT_PAGE_SIZE,
    ) -> list[RawEvent]:
        events: list[RawEvent] = []
        offset = 0
        while True:
            params: dict = {
                "tag_id": tag_id,
                "active": str(active).lower(),
                "closed": str(closed).lower(),
                "limit": limit,
                "offset": offset,
            }
            resp = self.http.get(f"{self.base_url}/events", params=params)
            resp.raise_for_status()
            page = resp.json()
            if not page:
                break
            for raw in page:
                events.append(RawEvent.model_validate(raw))
            if len(page) < limit:
                break
            offset += limit
        return events

    def get_all_events_for_league(self, league: LeagueConfig) -> list[RawEvent]:
        seen_ids: set[str] = set()
        result: list[RawEvent] = []
        for tag_id in league.tag_ids:
            events = self.get_events_by_tag(tag_id)
            for ev in events:
                if ev.id not in seen_ids:
                    seen_ids.add(ev.id)
                    result.append(ev)
        log.info(
            "fetched events for league",
            league=league.name,
            total=len(result),
            tag_ids=league.tag_ids,
        )
        return result
