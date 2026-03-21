from __future__ import annotations

from pydantic import BaseModel


class PennyPickingSignal(BaseModel):
    game_slug: str
    game_title: str
    sport: str  # "NBA" / "Football"
    outcome: str
    ask_price: float  # e.g. 0.97
    ask_depth: float  # shares at that price level
    total_depth_in_range: float  # total shares in 95-99¢ range
    best_bid: float | None = None
    spread: float | None = None
    liquidity: float = 0.0
    volume: float = 0.0
    polymarket_url: str = ""
    token_id: str = ""
    timestamp: str = ""
    price_bucket: int = 0  # 95/96/97/98/99, for dedup
