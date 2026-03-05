from __future__ import annotations

import structlog

from src.models.bundle import MarketType, TeamMarketBundle
from src.models.market import ClassifiedMarket

log = structlog.get_logger()

# Alias → canonical name
ALIASES: dict[str, str] = {
    # EPL
    "man city": "Manchester City",
    "man utd": "Manchester United",
    "man united": "Manchester United",
    "spurs": "Tottenham Hotspur",
    "tottenham": "Tottenham Hotspur",
    "wolves": "Wolverhampton Wanderers",
    "wolverhampton": "Wolverhampton Wanderers",
    "nottm forest": "Nottingham Forest",
    "nottingham": "Nottingham Forest",
    "newcastle utd": "Newcastle United",
    "newcastle": "Newcastle United",
    "west ham": "West Ham United",
    "brighton": "Brighton & Hove Albion",
    "brighton and hove albion": "Brighton & Hove Albion",
    "leicester": "Leicester City",
    "ipswich": "Ipswich Town",
    "luton": "Luton Town",
    "sheffield utd": "Sheffield United",
    "sheffield united": "Sheffield United",
    "crystal palace": "Crystal Palace",
    # La Liga
    "barca": "Barcelona",
    "atletico": "Atletico Madrid",
    "atlético": "Atletico Madrid",
    "atlético madrid": "Atletico Madrid",
    "atletico de madrid": "Atletico Madrid",
    "real sociedad": "Real Sociedad",
    "athletic": "Athletic Bilbao",
    "athletic club": "Athletic Bilbao",
    "real betis": "Real Betis",
    "celta": "Celta Vigo",
    "celta de vigo": "Celta Vigo",
    # Bundesliga
    "bayern": "Bayern Munich",
    "bayern münchen": "Bayern Munich",
    "bayern munchen": "Bayern Munich",
    "dortmund": "Borussia Dortmund",
    "bvb": "Borussia Dortmund",
    "borussia m'gladbach": "Borussia Monchengladbach",
    "gladbach": "Borussia Monchengladbach",
    "leverkusen": "Bayer Leverkusen",
    "rb leipzig": "RB Leipzig",
    "leipzig": "RB Leipzig",
    "wolfsburg": "VfL Wolfsburg",
    "frankfurt": "Eintracht Frankfurt",
    "freiburg": "SC Freiburg",
    "hoffenheim": "TSG Hoffenheim",
    "mainz": "FSV Mainz 05",
    "augsburg": "FC Augsburg",
    "union berlin": "Union Berlin",
    # Serie A
    "juve": "Juventus",
    "inter": "Inter Milan",
    "internazionale": "Inter Milan",
    "ac milan": "AC Milan",
    "milan": "AC Milan",
    "napoli": "SSC Napoli",
    "ssc napoli": "SSC Napoli",
    "roma": "AS Roma",
    "as roma": "AS Roma",
    "lazio": "SS Lazio",
    "ss lazio": "SS Lazio",
    "fiorentina": "ACF Fiorentina",
    "acf fiorentina": "ACF Fiorentina",
    "atalanta": "Atalanta BC",
    # Ligue 1
    "psg": "Paris Saint-Germain",
    "paris sg": "Paris Saint-Germain",
    "paris saint germain": "Paris Saint-Germain",
    "marseille": "Olympique Marseille",
    "om": "Olympique Marseille",
    "lyon": "Olympique Lyonnais",
    "ol": "Olympique Lyonnais",
    "monaco": "AS Monaco",
    "lille": "LOSC Lille",
    "losc": "LOSC Lille",
}

# Build reverse lookup: lowercase canonical → canonical
_CANONICAL_LOWER: dict[str, str] = {}
for _alias, _canon in ALIASES.items():
    _CANONICAL_LOWER[_canon.lower()] = _canon


class TeamResolver:
    def __init__(self, extra_aliases: dict[str, str] | None = None) -> None:
        self.aliases = dict(ALIASES)
        if extra_aliases:
            self.aliases.update(extra_aliases)
        self._canonical_lower = dict(_CANONICAL_LOWER)
        for alias, canon in self.aliases.items():
            self._canonical_lower[canon.lower()] = canon

    def normalize(self, name: str) -> str:
        stripped = name.strip()
        lower = stripped.lower()
        # Direct alias match
        if lower in self.aliases:
            return self.aliases[lower]
        # Already a canonical name
        if lower in self._canonical_lower:
            return self._canonical_lower[lower]
        # Return original with preserved casing
        return stripped

    def group_by_team(
        self, markets: list[ClassifiedMarket]
    ) -> dict[str, TeamMarketBundle]:
        bundles: dict[str, TeamMarketBundle] = {}
        for m in markets:
            canonical = self.normalize(m.team)
            if canonical not in bundles:
                bundles[canonical] = TeamMarketBundle(team=canonical, league=m.league)
            bundle = bundles[canonical]
            field = _market_type_to_field(m.market_type)
            if field and getattr(bundle, field) is None:
                setattr(bundle, field, m)
        log.info("grouped markets into bundles", count=len(bundles))
        return bundles


def _market_type_to_field(mt: MarketType) -> str | None:
    mapping = {
        MarketType.WINNER: "winner",
        MarketType.TOP_4: "top_4",
        MarketType.SECOND_PLACE: "second_place",
        MarketType.RELEGATION: "relegation",
        MarketType.EUROPEAN_FOOTBALL: "european_football",
        MarketType.POINTS_THRESHOLD: "points_threshold",
        MarketType.UCL_WINNER: "ucl_winner",
    }
    return mapping.get(mt)
