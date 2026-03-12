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
    last_trade_price: float = Field(default=0.0, alias="lastTradePrice")

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
    # Mid-prices from Gamma API (reference only)
    yes_price: float = 0.0
    no_price: float = 0.0
    yes_token_id: str = ""
    no_token_id: str = ""
    liquidity: float = 0.0
    volume: float = 0.0
    question: str = ""
    polymarket_url: str = ""
    last_trade_price: float = 0.0  # most recent trade price from Gamma API
    # Orderbook prices (actual executable prices, filled by OrderbookEnricher)
    yes_best_ask: float | None = None  # cheapest price to BUY yes
    yes_best_bid: float | None = None  # highest price to SELL yes
    no_best_ask: float | None = None  # cheapest price to BUY no
    no_best_bid: float | None = None  # highest price to SELL no
    yes_ask_depth: float = 0.0  # total shares on ask side
    no_ask_depth: float = 0.0
    yes_ask_levels: list[tuple[float, float]] = []  # [(price, size), ...]
    no_ask_levels: list[tuple[float, float]] = []
    yes_bid_depth: float = 0.0
    no_bid_depth: float = 0.0
    spread: float | None = None  # yes_best_ask - yes_best_bid

    @property
    def has_orderbook(self) -> bool:
        return self.yes_best_ask is not None or self.no_best_ask is not None

    @property
    def has_liquidity(self) -> bool:
        return (self.yes_ask_depth > 0 or self.yes_bid_depth > 0
                or self.no_ask_depth > 0 or self.no_bid_depth > 0)
