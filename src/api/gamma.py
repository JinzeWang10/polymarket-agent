from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
import structlog

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

    def get_event_by_slug(self, slug: str) -> RawEvent | None:
        """Fetch a single event by slug."""
        resp = self.http.get(
            f"{self.base_url}/events",
            params={"slug": slug, "limit": 1},
        )
        resp.raise_for_status()
        page = resp.json()
        if page:
            return RawEvent.model_validate(page[0])
        return None

    # Gamma /markets silently caps page size at 100 (it used to honor 500).
    # Requesting more makes the short-page pagination check stop after the
    # first page, so the page size must match the server cap exactly.
    _MARKETS_PAGE_SIZE = 100

    def get_markets_by_tags(
        self,
        tag_ids: list[int],
        *,
        limit: int = _MARKETS_PAGE_SIZE,
        max_workers: int = 10,
        end_date_min: str | None = None,
        end_date_max: str | None = None,
    ) -> list[dict]:
        """Fetch all active markets for given tags with concurrent pagination.

        Returns raw JSON dicts (not RawMarket) so callers can access fields
        like bestAsk/bestBid that aren't in the RawMarket model.

        end_date_min/end_date_max (ISO timestamps) filter server-side. Use
        them whenever possible: Gamma rejects offsets beyond ~10k with 422,
        so an unfiltered big tag silently misses markets past that point.
        """
        all_markets: dict[str, dict] = {}

        def _fetch_page(tag_id: int, offset: int) -> list[dict]:
            params: dict = {
                "active": "true",
                "closed": "false",
                "tag_id": tag_id,
                "limit": limit,
                "offset": offset,
            }
            if end_date_min:
                params["end_date_min"] = end_date_min
            if end_date_max:
                params["end_date_max"] = end_date_max
            resp = self.http.get(f"{self.base_url}/markets", params=params)
            resp.raise_for_status()
            return resp.json()

        # Phase 1: fetch first page of each tag
        first_pages: dict[int, list[dict]] = {}
        for tag_id in tag_ids:
            page = _fetch_page(tag_id, 0)
            first_pages[tag_id] = page
            for m in page:
                all_markets[m["id"]] = m

        # Phase 2: advance in concurrent batches, stop at the first short page
        # (errors count as short — Gamma 422s on offsets beyond ~10k)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for tag_id, page in first_pages.items():
                if len(page) < limit:
                    continue
                offset = limit
                exhausted = False
                while not exhausted:
                    batch = [offset + i * limit for i in range(max_workers)]
                    futures = {
                        pool.submit(_fetch_page, tag_id, off): off for off in batch
                    }
                    for future in as_completed(futures):
                        try:
                            page = future.result()
                        except Exception as e:
                            log.debug(
                                "page fetch failed",
                                tag_id=tag_id, offset=futures[future], error=str(e),
                            )
                            page = []
                        if len(page) < limit:
                            exhausted = True
                        for m in page:
                            all_markets[m["id"]] = m
                    offset += max_workers * limit

        log.info(
            "fetched markets by tags",
            tag_ids=tag_ids,
            total=len(all_markets),
        )
        return list(all_markets.values())

    def get_all_active_markets(
        self,
        *,
        limit: int = _MARKETS_PAGE_SIZE,
        max_workers: int = 10,
        end_date_min: str | None = None,
        end_date_max: str | None = None,
    ) -> list[dict]:
        """Fetch ALL active markets (no tag filter) with concurrent pagination."""
        all_markets: dict[str, dict] = {}

        def _fetch_page(offset: int) -> list[dict]:
            params: dict = {
                "active": "true",
                "closed": "false",
                "limit": limit,
                "offset": offset,
            }
            if end_date_min:
                params["end_date_min"] = end_date_min
            if end_date_max:
                params["end_date_max"] = end_date_max
            resp = self.http.get(f"{self.base_url}/markets", params=params)
            resp.raise_for_status()
            return resp.json()

        # Phase 1: first page to confirm data exists
        first = _fetch_page(0)
        for m in first:
            all_markets[m["id"]] = m

        if len(first) >= limit:
            # Phase 2: advance in concurrent batches, stop at first short page
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                offset = limit
                exhausted = False
                while not exhausted:
                    batch = [offset + i * limit for i in range(max_workers)]
                    futures = {pool.submit(_fetch_page, off): off for off in batch}
                    for future in as_completed(futures):
                        try:
                            page = future.result()
                        except Exception as e:
                            log.debug(
                                "page fetch failed",
                                offset=futures[future], error=str(e),
                            )
                            page = []
                        if len(page) < limit:
                            exhausted = True
                        for m in page:
                            all_markets[m["id"]] = m
                    offset += max_workers * limit

        log.info("fetched all active markets", total=len(all_markets))
        return list(all_markets.values())

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
