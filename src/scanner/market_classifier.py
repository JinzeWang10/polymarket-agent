from __future__ import annotations

import re
from fnmatch import fnmatch

import structlog

from src.models.bundle import MarketType
from src.models.market import ClassifiedMarket, RawEvent, RawMarket

log = structlog.get_logger()

# Slug patterns → MarketType (order matters: first match wins)
SLUG_PATTERNS: list[tuple[str, MarketType]] = [
    ("*winner*", MarketType.WINNER),
    ("*champion*", MarketType.WINNER),
    ("*win-the-*", MarketType.WINNER),
    ("*win-*-league*", MarketType.WINNER),
    ("*second-place*", MarketType.SECOND_PLACE),
    ("*runner-up*", MarketType.SECOND_PLACE),
    ("*2nd-place*", MarketType.SECOND_PLACE),
    ("*finish-second*", MarketType.SECOND_PLACE),
    ("*top-4*", MarketType.TOP_4),
    ("*top-four*", MarketType.TOP_4),
    ("*top4*", MarketType.TOP_4),
    ("*finish-in-the-top-4*", MarketType.TOP_4),
    ("*relegate*", MarketType.RELEGATION),
    ("*relegated*", MarketType.RELEGATION),
    ("*relegation*", MarketType.RELEGATION),
    ("*go-down*", MarketType.RELEGATION),
    ("*european-football*", MarketType.EUROPEAN_FOOTBALL),
    ("*europa*qualification*", MarketType.EUROPEAN_FOOTBALL),
    ("*points*", MarketType.POINTS_THRESHOLD),
    ("*ucl-winner*", MarketType.UCL_WINNER),
    ("*champions-league-winner*", MarketType.UCL_WINNER),
]

# Patterns that indicate match-day events (not season-long)
MATCH_DAY_PATTERNS = [
    re.compile(r"\d{4}-\d{2}-\d{2}"),  # date in slug
    re.compile(r"week-\d+"),
    re.compile(r"matchday-\d+"),
    re.compile(r"gameweek-\d+"),
    re.compile(r"round-\d+"),
]


class MarketClassifier:
    def detect_market_type(self, event_slug: str, market: RawMarket | None) -> MarketType:
        slug = event_slug.lower()
        for pattern, mtype in SLUG_PATTERNS:
            if fnmatch(slug, pattern):
                return mtype
        if market and market.question:
            q = market.question.lower()
            for pattern, mtype in SLUG_PATTERNS:
                if fnmatch(q.replace(" ", "-"), pattern):
                    return mtype
        return MarketType.UNKNOWN

    def extract_team_name(self, market: RawMarket) -> str:
        if market.group_item_title:
            return market.group_item_title.strip()
        # Fallback: parse from question
        q = market.question
        patterns = [
            r"^Will (.+?) (?:win|finish|get|be)",
            r"^(.+?) to (?:win|finish|get|be)",
        ]
        for pat in patterns:
            m = re.match(pat, q, re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return q.strip()

    def parse_prices(
        self, outcomes: list[str], outcome_prices: list[str]
    ) -> tuple[float, float]:
        yes_price = 0.0
        no_price = 0.0
        for outcome, price in zip(outcomes, outcome_prices):
            p = float(price)
            if outcome.lower() == "yes":
                yes_price = p
            elif outcome.lower() == "no":
                no_price = p
        return yes_price, no_price

    def parse_token_ids(
        self, outcomes: list[str], clob_token_ids: list[str]
    ) -> tuple[str, str]:
        yes_token = ""
        no_token = ""
        for outcome, tid in zip(outcomes, clob_token_ids):
            if outcome.lower() == "yes":
                yes_token = tid
            elif outcome.lower() == "no":
                no_token = tid
        return yes_token, no_token

    def is_season_long_event(self, event: RawEvent) -> bool:
        slug = event.slug.lower()
        for pat in MATCH_DAY_PATTERNS:
            if pat.search(slug):
                return False
        return True

    def classify_event(self, event: RawEvent, league: str) -> list[ClassifiedMarket]:
        if not self.is_season_long_event(event):
            return []
        results: list[ClassifiedMarket] = []
        for market in event.markets:
            if not market.active or market.closed:
                continue
            mtype = self.detect_market_type(event.slug, market)
            if mtype == MarketType.UNKNOWN:
                continue
            team = self.extract_team_name(market)
            yes_price, no_price = self.parse_prices(market.outcomes, market.outcome_prices)
            yes_token, no_token = self.parse_token_ids(market.outcomes, market.clob_token_ids)
            results.append(
                ClassifiedMarket(
                    market_id=market.id,
                    event_id=event.id,
                    event_slug=event.slug,
                    league=league,
                    team=team,
                    market_type=mtype,
                    yes_price=yes_price,
                    no_price=no_price,
                    yes_token_id=yes_token,
                    no_token_id=no_token,
                    liquidity=market.liquidity,
                    volume=market.volume,
                    question=market.question,
                    polymarket_url=f"https://polymarket.com/event/{event.slug}",
                )
            )
        return results
