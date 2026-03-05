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

    def fetch_all_season_markets(self) -> dict[str, list[RawEvent]]:
        result: dict[str, list[RawEvent]] = {}
        for league in self.leagues:
            events = self.gamma.get_all_events_for_league(league)
            season_events = [e for e in events if self._matches_season_pattern(e, league)]
            result[league.name] = season_events
            log.info(
                "filtered season events",
                league=league.name,
                total=len(events),
                season=len(season_events),
            )
        return result

    def _matches_season_pattern(self, event: RawEvent, league: LeagueConfig) -> bool:
        slug = event.slug.lower()
        for pattern in league.season_slug_patterns:
            if fnmatch(slug, pattern.lower()):
                return True
        return False
