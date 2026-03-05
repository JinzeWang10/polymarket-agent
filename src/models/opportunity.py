from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class ConstraintType(str, Enum):
    MUTUAL_EXCLUSION = "mutual_exclusion"
    SUBSET_VIOLATION = "subset_violation"
    MARKET_SUM = "market_sum"
    NO_SIDE_ARB = "no_side_arb"
    DIRECTIONAL_MISPRICING = "directional_mispricing"


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
