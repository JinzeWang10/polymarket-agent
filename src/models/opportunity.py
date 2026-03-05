from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class ConstraintType(str, Enum):
    MUTUAL_EXCLUSION = "mutual_exclusion"
    SUBSET_VIOLATION = "subset_violation"
    MARKET_SUM = "market_sum"
    NO_SIDE_ARB = "no_side_arb"
    DIRECTIONAL_MISPRICING = "directional_mispricing"


class OrderbookLevel(BaseModel):
    price_cents: float
    size: float


class OrderbookVerification(BaseModel):
    """Verification of an opportunity against actual CLOB orderbook."""
    verified: bool = False
    executable: bool = False
    # For NO_SIDE_ARB: the actual ask prices you'd pay
    actual_cost_cents: float | None = None
    actual_profit_cents: float | None = None
    actual_profit_pct: float | None = None
    # Depth available at the quoted price
    depth_token_a: float = 0.0  # shares available
    depth_token_b: float = 0.0
    best_ask_a: OrderbookLevel | None = None
    best_ask_b: OrderbookLevel | None = None
    # Whether there's any liquidity at all
    has_liquidity_a: bool = False
    has_liquidity_b: bool = False
    notes: str = ""


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
    # Token IDs needed for orderbook verification
    token_ids: list[str] = []
    # Orderbook verification results
    orderbook: OrderbookVerification | None = None
