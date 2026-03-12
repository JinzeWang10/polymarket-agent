"""Detect outlier ask orders priced significantly below recent market consensus.

Reference price = median of 6h price history (from CLOB /prices-history).
Falls back to lastTradePrice when history is unavailable.
An ask far below the reference may be a fat-finger or distressed order.
"""
from __future__ import annotations

import statistics
from datetime import datetime, timezone

import structlog

from src.api.clob import ClobClient
from src.models.market import ClassifiedMarket
from src.models.opportunity import (
    ArbitrageOpportunity,
    ConstraintType,
    OutlierDetail,
    OutlierInfo,
)

log = structlog.get_logger()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class OutlierDetector:
    """Detect ask orders priced significantly below 6h median price."""

    def __init__(
        self,
        clob: ClobClient,
        min_gap_pct: float = 0.03,
        min_gap_cents: float = 3.0,
        history_interval: str = "6h",
    ) -> None:
        self.clob = clob
        self.min_gap_pct = min_gap_pct
        self.min_gap_cents = min_gap_cents
        self.history_interval = history_interval
        # Cache: token_id -> median price (reset per detect() call)
        self._median_cache: dict[str, float | None] = {}

    def detect(
        self,
        markets: list[ClassifiedMarket],
        league: str,
        use_price_history: bool = True,
        min_ref: float = 0.0,
    ) -> list[ArbitrageOpportunity]:
        self._median_cache.clear()
        self._use_price_history = use_price_history
        self._min_ref = min_ref
        opps: list[ArbitrageOpportunity] = []
        for market in markets:
            opps.extend(self._check_ask_outliers(market, league))
        log.info("outlier order scan", league=league, signals=len(opps))
        return opps

    def _get_median_price(self, token_id: str) -> float | None:
        """Fetch 6h price history and return median. Cached per scan."""
        if token_id in self._median_cache:
            return self._median_cache[token_id]
        try:
            history = self.clob.get_price_history(
                token_id, interval=self.history_interval,
            )
            if history:
                prices = [pt["p"] for pt in history]
                median = statistics.median(prices)
                self._median_cache[token_id] = median
                return median
        except Exception as e:
            log.debug("price history fetch failed", token_id=token_id[:20], error=str(e))
        self._median_cache[token_id] = None
        return None

    def _has_cheap_asks(
        self, levels: list[tuple[float, float]], ltp: float,
    ) -> bool:
        """Quick pre-check: any ask below lastTradePrice?"""
        return bool(levels) and ltp > 0 and any(price < ltp for price, _ in levels)

    def _check_ask_outliers(
        self, market: ClassifiedMarket, league: str,
    ) -> list[ArbitrageOpportunity]:
        if market.last_trade_price <= 0:
            return []

        ltp = market.last_trade_price
        opps: list[ArbitrageOpportunity] = []

        # YES side: only fetch 6h median if asks are below lastTradePrice
        ref = ltp
        if self._use_price_history and market.yes_token_id:
            if self._has_cheap_asks(market.yes_ask_levels, ltp) or \
               self._has_cheap_asks(market.no_ask_levels, 1.0 - ltp):
                yes_median = self._get_median_price(market.yes_token_id)
                if yes_median and yes_median > 0:
                    ref = yes_median

        # YES side
        if market.yes_ask_levels and ref >= self._min_ref:
            outliers = self._find_outlier_levels(market.yes_ask_levels, ref)
            if outliers:
                opps.append(self._build_opportunity(
                    market, league, "YES", outliers, ref,
                    token_id=market.yes_token_id,
                    opposite_best_ask=market.no_best_ask,
                ))

        # NO side: ref = 1 - YES ref
        no_ref = 1.0 - ref
        if market.no_ask_levels and no_ref > 0 and no_ref >= self._min_ref:
            outliers = self._find_outlier_levels(market.no_ask_levels, no_ref)
            if outliers:
                opps.append(self._build_opportunity(
                    market, league, "NO", outliers, no_ref,
                    token_id=market.no_token_id,
                    opposite_best_ask=market.yes_best_ask,
                ))

        return opps

    def _find_outlier_levels(
        self,
        levels: list[tuple[float, float]],
        ref_price: float,
    ) -> list[OutlierDetail]:
        """Find ask levels significantly below the reference price."""
        if ref_price <= 0:
            return []

        outliers: list[OutlierDetail] = []
        ref_cents = round(ref_price * 100, 2)

        for price, size in levels:
            if price >= ref_price:
                continue

            gap = ref_price - price
            gap_cents = round(gap * 100, 2)
            gap_pct = round(gap / ref_price * 100, 2)

            if gap_pct / 100 < self.min_gap_pct or gap_cents < self.min_gap_cents:
                continue

            outliers.append(OutlierDetail(
                price_cents=round(price * 100, 2),
                size=size,
                ref_cents=ref_cents,
                gap_cents=gap_cents,
                gap_pct=gap_pct,
            ))

        return outliers

    def _build_opportunity(
        self,
        market: ClassifiedMarket,
        league: str,
        side: str,
        outliers: list[OutlierDetail],
        ref_price: float,
        token_id: str,
        opposite_best_ask: float | None,
    ) -> ArbitrageOpportunity:
        best_outlier_price = min(d.price_cents for d in outliers) / 100
        ref_cents = round(ref_price * 100, 2)
        profit_cents = round((ref_price - best_outlier_price) * 100, 2)

        # Cross-side arb check: ask + opposite_ask < 1.0
        cross_arb = False
        arb_profit: float | None = None
        if opposite_best_ask is not None and best_outlier_price + opposite_best_ask < 1.0:
            cross_arb = True
            arb_profit = round((1.0 - best_outlier_price - opposite_best_ask) * 100, 2)
            profit_cents = max(profit_cents, arb_profit)

        confidence = "high" if cross_arb else ("medium" if profit_cents > 5 else "low")

        best_price_cents = min(d.price_cents for d in outliers)
        desc = (
            f"{market.question or market.team} | "
            f"{side}侧 {len(outliers)} 个异常卖单, "
            f"最低 {best_price_cents:.1f}c vs 6h中位价 {ref_cents:.1f}c"
        )

        info = OutlierInfo(
            question=market.question or "",
            side=side,
            levels=outliers,
            cross_arb=cross_arb,
            cross_arb_profit_cents=arb_profit,
            opposite_ask_cents=round(opposite_best_ask * 100, 2) if opposite_best_ask is not None else None,
        )

        violation = round((ref_price - best_outlier_price) / ref_price * 100, 2) if ref_price > 0 else 0

        return ArbitrageOpportunity(
            constraint_type=ConstraintType.OUTLIER_ORDER,
            team=market.team,
            league=league,
            description=desc,
            markets_involved=[market.market_id],
            violation_pct=violation,
            potential_profit_cents=profit_cents,
            confidence=confidence,
            polymarket_urls=[market.polymarket_url] if market.polymarket_url else [],
            token_ids=[token_id] if token_id else [],
            timestamp=_now_iso(),
            outlier_info=info,
        )
