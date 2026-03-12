from __future__ import annotations

from datetime import datetime, timezone

import structlog

from src.models.bundle import MarketType
from src.models.market import ClassifiedMarket
from src.models.opportunity import ArbitrageOpportunity, ConstraintType

log = structlog.get_logger()

MAX_ASK_PRICE = 0.01  # 1¢ threshold


def _fmt_price(p: float | None) -> str:
    if p is None:
        return "N/A"
    return f"{p * 100:.2f}c"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PennyDetector:
    """Detect markets with YES ask <= 1 cent that have actual sell orders.

    These are extreme longshots where the market might be underpricing
    the true probability. Buying YES at <1c gives 100x+ return if it hits.
    """

    # Season-long market types worth scanning for penny opportunities.
    # Relegation is kept because it's realistic for mid-table teams;
    # winner/top4/second at <1c for weak teams is correctly priced noise.
    SEASON_MARKET_TYPES: set[MarketType] = {MarketType.RELEGATION}

    def detect(
        self,
        markets: list[ClassifiedMarket],
        league: str,
        season: bool = True,
    ) -> list[ArbitrageOpportunity]:
        opps: list[ArbitrageOpportunity] = []
        for market in markets:
            if season and market.market_type not in self.SEASON_MARKET_TYPES:
                continue
            if market.yes_best_ask is None:
                continue
            if market.yes_best_ask > MAX_ASK_PRICE:
                continue
            if market.yes_ask_depth <= 0:
                continue

            ask_cents = market.yes_best_ask * 100
            depth = market.yes_ask_depth
            profit = 100 - ask_cents
            roi_pct = profit / ask_cents * 100 if ask_cents > 0 else 0

            desc_parts = []
            if market.question:
                desc_parts.append(market.question)
            desc_parts.append(
                f"YES ask = {ask_cents:.2f}c"
                f" (depth {depth:.0f} shares)"
            )
            desc_parts.append(f"mid = {market.yes_price * 100:.1f}c")
            desc_parts.append(
                f"买入 YES @ {_fmt_price(market.yes_best_ask)}"
                f" -> 若命中获利 {profit:.1f}c (ROI {roi_pct:.0f}x)"
            )

            opps.append(
                ArbitrageOpportunity(
                    constraint_type=ConstraintType.PENNY_OPPORTUNITY,
                    team=market.team,
                    league=league,
                    description=" | ".join(desc_parts),
                    markets_involved=[market.market_id],
                    violation_pct=0,
                    potential_profit_cents=round(profit, 2),
                    profit_pct=round(roi_pct, 1),
                    confidence="speculative",
                    polymarket_urls=[market.polymarket_url] if market.polymarket_url else [],
                    token_ids=[market.yes_token_id] if market.yes_token_id else [],
                    timestamp=_now_iso(),
                )
            )

        log.info("penny market scan", league=league, found=len(opps))
        return opps
