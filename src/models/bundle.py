from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class MarketType(str, Enum):
    WINNER = "winner"
    TOP_4 = "top_4"
    SECOND_PLACE = "second_place"
    RELEGATION = "relegation"
    EUROPEAN_FOOTBALL = "european_football"
    POINTS_THRESHOLD = "points_threshold"
    UCL_WINNER = "ucl_winner"
    UNKNOWN = "unknown"


class TeamMarketBundle(BaseModel):
    team: str
    league: str
    winner: object | None = None
    top_4: object | None = None
    second_place: object | None = None
    relegation: object | None = None
    european_football: object | None = None
    points_threshold: object | None = None
    ucl_winner: object | None = None

    model_config = {"arbitrary_types_allowed": True}
