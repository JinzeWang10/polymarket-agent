from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class ConstraintType(str, Enum):
    MUTUAL_EXCLUSION = "mutual_exclusion"
    SUBSET_VIOLATION = "subset_violation"
    MARKET_SUM = "market_sum"
    NO_SIDE_ARB = "no_side_arb"
    DIRECTIONAL_MISPRICING = "directional_mispricing"
    VALUE_MISPRICING = "value_mispricing"
    PENNY_OPPORTUNITY = "penny_opportunity"
    OUTLIER_ORDER = "outlier_order"


class OutlierDetail(BaseModel):
    """Structured data for a single outlier ask level."""
    price_cents: float
    size: float
    ref_cents: float    # reference price (6h median or lastTradePrice fallback)
    gap_cents: float    # ref - price
    gap_pct: float      # gap as percentage of ref


class OutlierInfo(BaseModel):
    """Structured data for an outlier order opportunity."""
    question: str = ""
    side: str = ""  # YES or NO
    last_trade_price_cents: float = 0.0
    levels: list[OutlierDetail] = []
    cross_arb: bool = False
    cross_arb_profit_cents: float | None = None
    opposite_ask_cents: float | None = None


class ArbitrageOpportunity(BaseModel):
    constraint_type: ConstraintType
    team: str
    league: str
    description: str
    markets_involved: list[str] = []
    violation_pct: float = 0.0
    potential_profit_cents: float | None = None
    profit_pct: float | None = None
    confidence: str = "medium"
    polymarket_urls: list[str] = []
    timestamp: str = ""
    token_ids: list[str] = []
    outlier_info: OutlierInfo | None = None
