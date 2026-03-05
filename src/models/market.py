from __future__ import annotations

import json

from pydantic import BaseModel, Field, field_validator

from src.models.bundle import MarketType


def _parse_json_string_list(v: object) -> list[str]:
    """Gamma API sometimes returns JSON-encoded strings for list fields."""
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except (json.JSONDecodeError, TypeError):
            pass
        return [v]
    if isinstance(v, list):
        return [str(x) for x in v]
    return []


class RawMarket(BaseModel):
    id: str
    question: str = ""
    slug: str = ""
    outcomes: list[str] = []
    outcome_prices: list[str] = Field(default_factory=list, alias="outcomePrices")
    clob_token_ids: list[str] = Field(default_factory=list, alias="clobTokenIds")
    liquidity: float = 0.0
    volume: float = 0.0
    active: bool = True
    closed: bool = False
    group_item_title: str = Field(default="", alias="groupItemTitle")

    model_config = {"populate_by_name": True}

    @field_validator("outcomes", "outcome_prices", "clob_token_ids", mode="before")
    @classmethod
    def parse_json_strings(cls, v: object) -> list[str]:
        return _parse_json_string_list(v)


class RawEvent(BaseModel):
    id: str
    slug: str = ""
    title: str = ""
    markets: list[RawMarket] = []
    active: bool = True
    closed: bool = False

    model_config = {"populate_by_name": True}


class ClassifiedMarket(BaseModel):
    market_id: str
    event_id: str
    event_slug: str
    league: str
    team: str
    market_type: MarketType
    yes_price: float = 0.0
    no_price: float = 0.0
    yes_token_id: str = ""
    no_token_id: str = ""
    liquidity: float = 0.0
    volume: float = 0.0
    question: str = ""
    polymarket_url: str = ""
