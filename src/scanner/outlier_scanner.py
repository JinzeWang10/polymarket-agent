"""Streaming outlier-order scanner with concurrent enrichment.

Fetches all active markets from Gamma, pre-filters, concurrently enriches
via CLOB orderbook, and emits signals through a callback as soon as found.
"""
from __future__ import annotations

import json
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Callable

import structlog

from src.api.clob import ClobClient
from src.api.gamma import GammaClient
from src.models.market import ClassifiedMarket, MarketType
from src.models.opportunity import (
    ArbitrageOpportunity,
    ConstraintType,
    OutlierDetail,
    OutlierInfo,
)

log = structlog.get_logger()

SignalCallback = Callable[[ArbitrageOpportunity], None]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class OutlierScanner:
    """Fetch → pre-filter → concurrent enrich+detect → stream signals."""

    def __init__(
        self,
        gamma: GammaClient,
        clob: ClobClient,
        *,
        tag_ids: list[int] | None = None,
        min_ref: float = 0.80,
        min_gap_pct: float = 0.03,
        min_gap_cents: float = 3.0,
        min_end_days: int = 3,
        history_interval: str = "6h",
        max_workers: int = 20,
        on_signal: SignalCallback | None = None,
    ) -> None:
        self.gamma = gamma
        self.clob = clob
        self.tag_ids = tag_ids
        self.min_ref = min_ref
        self.min_gap_pct = min_gap_pct
        self.min_gap_cents = min_gap_cents
        self.min_end_days = min_end_days
        self.history_interval = history_interval
        self.max_workers = max_workers
        self.on_signal = on_signal

        # Thread-safe price-history cache (reset per scan)
        self._median_cache: dict[str, float | None] = {}
        self._cache_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(self) -> list[ArbitrageOpportunity]:
        """Run a full scan. Returns all signals found."""
        self._median_cache.clear()

        if self.tag_ids:
            raw_markets = self.gamma.get_markets_by_tags(
                self.tag_ids, max_workers=self.max_workers,
            )
        else:
            raw_markets = self.gamma.get_all_active_markets(
                max_workers=self.max_workers,
            )

        yes_cands, no_cands = self._pre_filter(raw_markets)

        log.info(
            "outlier scan start",
            total_markets=len(raw_markets),
            yes_candidates=len(yes_cands),
            no_candidates=len(no_cands),
        )

        tasks: list[tuple[ClassifiedMarket, str]] = []
        for raw in yes_cands:
            tasks.append((self._to_classified(raw), "full"))
        for raw in no_cands:
            tasks.append((self._to_classified(raw), "no_only"))

        all_opps: list[ArbitrageOpportunity] = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(self._process_market, market, mode): market
                for market, mode in tasks
            }
            for future in as_completed(futures):
                market = futures[future]
                try:
                    opps = future.result()
                except Exception:
                    log.warning(
                        "market processing failed",
                        market_id=market.market_id,
                        exc_info=True,
                    )
                    continue
                for opp in opps:
                    all_opps.append(opp)
                    if self.on_signal:
                        self.on_signal(opp)

        log.info("outlier scan complete", signals=len(all_opps))
        return all_opps

    # ------------------------------------------------------------------
    # Pre-filter
    # ------------------------------------------------------------------

    def _pre_filter(
        self, raw_markets: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """Split into YES-side and NO-side candidate pools."""
        yes: list[dict] = []
        no: list[dict] = []

        cutoff = datetime.now(timezone.utc) + timedelta(days=self.min_end_days)

        for m in raw_markets:
            # Skip markets expiring too soon (live/daily markets)
            end_str = m.get("endDate") or m.get("endDateIso") or ""
            if end_str:
                try:
                    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    if end_dt < cutoff:
                        continue
                except (ValueError, TypeError):
                    pass

            ltp = float(m.get("lastTradePrice", 0) or 0)
            if ltp <= 0:
                continue

            ba = float(m.get("bestAsk") or 0)

            # YES side: Gamma bestAsk suspiciously below ltp
            if ltp >= self.min_ref and ba > 0 and ba < ltp * (1 - self.min_gap_pct):
                yes.append(m)
            # NO side: high NO ref, sort by liquidity later
            elif (1.0 - ltp) >= self.min_ref:
                no.append(m)

        # Sort NO by liquidity descending (most tradeable first)
        no.sort(key=lambda m: float(m.get("liquidity", 0) or 0), reverse=True)

        return yes, no

    # ------------------------------------------------------------------
    # Per-market processing (runs in thread)
    # ------------------------------------------------------------------

    def _process_market(
        self, market: ClassifiedMarket, mode: str,
    ) -> list[ArbitrageOpportunity]:
        """Enrich one market and detect outliers. Runs in worker thread."""
        try:
            if mode == "full":
                self._enrich_full(market)
            else:
                self._enrich_no_only(market)
        except Exception as e:
            log.debug("enrich failed", market_id=market.market_id, error=str(e))
            return []

        return self._detect(market)

    # ------------------------------------------------------------------
    # Enrichment (CLOB orderbook)
    # ------------------------------------------------------------------

    def _enrich_full(self, market: ClassifiedMarket) -> None:
        """Fetch YES + NO orderbooks."""
        if market.yes_token_id:
            self._fill_side(market, "yes")
        if market.no_token_id:
            self._fill_side(market, "no")

    def _enrich_no_only(self, market: ClassifiedMarket) -> None:
        """Fetch only NO orderbook."""
        if market.no_token_id:
            self._fill_side(market, "no")

    def _fill_side(self, market: ClassifiedMarket, side: str) -> None:
        token_id = market.yes_token_id if side == "yes" else market.no_token_id
        if not token_id:
            return
        book = self.clob.get_order_book(token_id)
        asks = sorted(book.get("asks", []), key=lambda x: float(x["price"]))
        bids = sorted(book.get("bids", []), key=lambda x: -float(x["price"]))
        if side == "yes":
            if asks:
                market.yes_best_ask = float(asks[0]["price"])
                market.yes_ask_depth = sum(float(a["size"]) for a in asks)
                market.yes_ask_levels = [(float(a["price"]), float(a["size"])) for a in asks]
            if bids:
                market.yes_best_bid = float(bids[0]["price"])
                market.yes_bid_depth = sum(float(b["size"]) for b in bids)
        else:
            if asks:
                market.no_best_ask = float(asks[0]["price"])
                market.no_ask_depth = sum(float(a["size"]) for a in asks)
                market.no_ask_levels = [(float(a["price"]), float(a["size"])) for a in asks]
            if bids:
                market.no_best_bid = float(bids[0]["price"])
                market.no_bid_depth = sum(float(b["size"]) for b in bids)

    # ------------------------------------------------------------------
    # Detection (per-market)
    # ------------------------------------------------------------------

    def _detect(self, market: ClassifiedMarket) -> list[ArbitrageOpportunity]:
        ltp = market.last_trade_price
        if ltp <= 0:
            return []

        ref = ltp

        # Fetch 6h median if any ask is suspiciously cheap
        if market.yes_token_id and (
            self._has_cheap_asks(market.yes_ask_levels, ltp)
            or self._has_cheap_asks(market.no_ask_levels, 1.0 - ltp)
        ):
            median = self._get_median(market.yes_token_id)
            if median and median > 0:
                ref = median

        opps: list[ArbitrageOpportunity] = []

        # YES side
        if market.yes_ask_levels and ref >= self.min_ref:
            outliers = self._find_outliers(market.yes_ask_levels, ref)
            if outliers:
                opps.append(self._build_opp(
                    market, "YES", outliers, ref,
                    token_id=market.yes_token_id,
                    opposite_best_ask=market.no_best_ask,
                ))

        # NO side
        no_ref = 1.0 - ref
        if market.no_ask_levels and no_ref > 0 and no_ref >= self.min_ref:
            outliers = self._find_outliers(market.no_ask_levels, no_ref)
            if outliers:
                opps.append(self._build_opp(
                    market, "NO", outliers, no_ref,
                    token_id=market.no_token_id,
                    opposite_best_ask=market.yes_best_ask,
                ))

        return opps

    @staticmethod
    def _has_cheap_asks(levels: list[tuple[float, float]], ref: float) -> bool:
        return bool(levels) and ref > 0 and any(p < ref for p, _ in levels)

    def _get_median(self, token_id: str) -> float | None:
        with self._cache_lock:
            if token_id in self._median_cache:
                return self._median_cache[token_id]

        # Fetch outside lock (I/O)
        result: float | None = None
        try:
            history = self.clob.get_price_history(
                token_id, interval=self.history_interval,
            )
            if history:
                result = statistics.median(pt["p"] for pt in history)
        except Exception as e:
            log.debug("price history failed", token_id=token_id[:20], error=str(e))

        with self._cache_lock:
            self._median_cache[token_id] = result
        return result

    def _find_outliers(
        self, levels: list[tuple[float, float]], ref_price: float,
    ) -> list[OutlierDetail]:
        if ref_price <= 0:
            return []
        ref_cents = round(ref_price * 100, 2)
        outliers: list[OutlierDetail] = []
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

    def _build_opp(
        self,
        market: ClassifiedMarket,
        side: str,
        outliers: list[OutlierDetail],
        ref_price: float,
        token_id: str,
        opposite_best_ask: float | None,
    ) -> ArbitrageOpportunity:
        best_price = min(d.price_cents for d in outliers) / 100
        ref_cents = round(ref_price * 100, 2)
        profit_cents = round((ref_price - best_price) * 100, 2)

        cross_arb = False
        arb_profit: float | None = None
        if opposite_best_ask is not None and best_price + opposite_best_ask < 1.0:
            cross_arb = True
            arb_profit = round((1.0 - best_price - opposite_best_ask) * 100, 2)
            profit_cents = max(profit_cents, arb_profit)

        confidence = "high" if cross_arb else ("medium" if profit_cents > 5 else "low")

        best_cents = min(d.price_cents for d in outliers)
        desc = (
            f"{market.question or market.team} | "
            f"{side}侧 {len(outliers)} 个异常卖单, "
            f"最低 {best_cents:.1f}c vs 6h中位价 {ref_cents:.1f}c"
        )

        violation = round((ref_price - best_price) / ref_price * 100, 2) if ref_price > 0 else 0

        return ArbitrageOpportunity(
            constraint_type=ConstraintType.OUTLIER_ORDER,
            team=market.team,
            league=market.league,
            description=desc,
            markets_involved=[market.market_id],
            violation_pct=violation,
            potential_profit_cents=profit_cents,
            confidence=confidence,
            polymarket_urls=[market.polymarket_url] if market.polymarket_url else [],
            token_ids=[token_id] if token_id else [],
            timestamp=_now_iso(),
            outlier_info=OutlierInfo(
                question=market.question or "",
                side=side,
                last_trade_price_cents=round(market.last_trade_price * 100, 2),
                levels=outliers,
                cross_arb=cross_arb,
                cross_arb_profit_cents=arb_profit,
                opposite_ask_cents=round(opposite_best_ask * 100, 2) if opposite_best_ask is not None else None,
            ),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_classified(raw: dict) -> ClassifiedMarket:
        prices = raw.get("outcomePrices", [])
        if isinstance(prices, str):
            prices = json.loads(prices)
        tokens = raw.get("clobTokenIds", [])
        if isinstance(tokens, str):
            tokens = json.loads(tokens)

        group_title = raw.get("groupItemTitle", "")
        question = raw.get("question", "")

        # Build URL from event slug (not market slug)
        url = ""
        events = raw.get("events", [])
        if events and isinstance(events, list):
            event_slug = events[0].get("slug", "")
            if event_slug:
                url = f"https://polymarket.com/event/{event_slug}"

        return ClassifiedMarket(
            market_id=str(raw.get("id", "")),
            event_id="",
            event_slug=raw.get("slug", ""),
            league="Sports",
            team=group_title or question[:40],
            market_type=MarketType.UNKNOWN,
            yes_price=float(prices[0]) if prices else 0.0,
            no_price=float(prices[1]) if len(prices) > 1 else 0.0,
            yes_token_id=tokens[0] if tokens else "",
            no_token_id=tokens[1] if len(tokens) > 1 else "",
            last_trade_price=float(raw.get("lastTradePrice", 0) or 0),
            liquidity=float(raw.get("liquidity", 0) or 0),
            volume=float(raw.get("volume", 0) or 0),
            question=question,
            polymarket_url=url,
        )
