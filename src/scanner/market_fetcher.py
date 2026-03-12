from __future__ import annotations

from fnmatch import fnmatch

import structlog

from src.api.gamma import GammaClient
from src.config import LeagueConfig
from src.models.market import RawEvent

log = structlog.get_logger()


class MarketFetcher:
    def __init__(self, gamma: GammaClient, leagues: list[LeagueConfig]) -> None:
        self.gamma = gamma
        self.leagues = leagues
        self._all_events: dict[str, list[RawEvent]] = {}

    def fetch_all_season_markets(self) -> dict[str, list[RawEvent]]:
        result: dict[str, list[RawEvent]] = {}
        for league in self.leagues:
            events = self.gamma.get_all_events_for_league(league)
            self._all_events[league.name] = events  # cache for match-day reuse
            season_events = [e for e in events if self._matches_season_pattern(e, league)]
            result[league.name] = season_events
            log.info(
                "filtered season events",
                league=league.name,
                total=len(events),
                season=len(season_events),
            )
        return result

    def get_match_events(self) -> dict[str, list[RawEvent]]:
        """Return non-season events per league (must call fetch_all_season_markets first)."""
        result: dict[str, list[RawEvent]] = {}
        for league in self.leagues:
            all_events = self._all_events.get(league.name, [])
            match = [e for e in all_events if not self._matches_season_pattern(e, league)]
            if match:
                result[league.name] = match
                log.info("match-day events", league=league.name, count=len(match))
        return result

    def fetch_all_football_events(self, tag_id: int) -> list[RawEvent]:
        """Fetch all active events with the given soccer tag (with retry)."""
        import time
        for attempt in range(3):
            try:
                events = self.gamma.get_events_by_tag(tag_id)
                log.info("fetched all football events", tag_id=tag_id, count=len(events))
                return events
            except Exception as e:
                log.warning(
                    "football events fetch attempt failed",
                    attempt=attempt + 1, error=str(e),
                )
                if attempt < 2:
                    time.sleep(2)
        return []

    def fetch_extra_football_events(self, slugs: list[str]) -> list[RawEvent]:
        """Fetch specific season-long events by slug for outlier scanning."""
        events: list[RawEvent] = []
        for slug in slugs:
            try:
                event = self.gamma.get_event_by_slug(slug)
                if event and event.active and not event.closed:
                    events.append(event)
            except Exception as e:
                log.debug("failed to fetch event", slug=slug, error=str(e))
        log.info("fetched extra football events", requested=len(slugs), found=len(events))
        return events

    def _matches_season_pattern(self, event: RawEvent, league: LeagueConfig) -> bool:
        slug = event.slug.lower()
        for pattern in league.season_slug_patterns:
            if fnmatch(slug, pattern.lower()):
                return True
        return False
