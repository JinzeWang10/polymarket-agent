from __future__ import annotations

import structlog

from src.api.clob import ClobClient
from src.models.bundle import TeamMarketBundle
from src.models.market import ClassifiedMarket

log = structlog.get_logger()


class OrderbookEnricher:
    """Enrich ClassifiedMarkets with actual orderbook bid/ask prices before detection."""

    def __init__(self, clob: ClobClient) -> None:
        self.clob = clob

    def enrich_bundles(
        self, bundles: dict[str, TeamMarketBundle]
    ) -> dict[str, TeamMarketBundle]:
        """Fetch orderbook data for markets involved in potential arbs."""
        # Collect markets that need enrichment (have token IDs)
        markets_to_enrich: list[ClassifiedMarket] = []
        for team, bundle in bundles.items():
            for field in ("winner", "top_4", "second_place", "relegation"):
                m: ClassifiedMarket | None = getattr(bundle, field, None)
                if m is not None and (m.yes_token_id or m.no_token_id):
                    markets_to_enrich.append(m)

        log.info("enriching markets with orderbook data", count=len(markets_to_enrich))

        for market in markets_to_enrich:
            try:
                self._enrich_market(market)
            except Exception as e:
                log.warning(
                    "orderbook fetch failed",
                    market_id=market.market_id,
                    team=market.team,
                    error=str(e),
                )

        return bundles

    def enrich_markets(self, markets: list[ClassifiedMarket]) -> None:
        """Enrich a flat list of markets with orderbook data."""
        to_enrich = [
            m for m in markets
            if (m.yes_token_id or m.no_token_id) and not m.has_orderbook
        ]
        if not to_enrich:
            return
        log.info("enriching penny candidates", count=len(to_enrich))
        for market in to_enrich:
            try:
                self._enrich_market(market)
            except Exception as e:
                log.warning(
                    "orderbook fetch failed",
                    market_id=market.market_id,
                    error=str(e),
                )

    def enrich_markets_no_only(self, markets: list[ClassifiedMarket]) -> None:
        """Enrich only the NO side of markets (halves API calls)."""
        to_enrich = [
            m for m in markets
            if m.no_token_id and not m.no_ask_levels
        ]
        if not to_enrich:
            return
        log.info("enriching NO-side only", count=len(to_enrich))
        for market in to_enrich:
            try:
                book = self.clob.get_order_book(market.no_token_id)
                asks = sorted(book.get("asks", []), key=lambda x: float(x["price"]))
                bids = sorted(book.get("bids", []), key=lambda x: -float(x["price"]))
                if asks:
                    market.no_best_ask = float(asks[0]["price"])
                    market.no_ask_depth = sum(float(a["size"]) for a in asks)
                    market.no_ask_levels = [
                        (float(a["price"]), float(a["size"])) for a in asks
                    ]
                if bids:
                    market.no_best_bid = float(bids[0]["price"])
                    market.no_bid_depth = sum(float(b["size"]) for b in bids)
            except Exception as e:
                log.warning(
                    "NO orderbook fetch failed",
                    market_id=market.market_id,
                    error=str(e),
                )

    def _enrich_market(self, market: ClassifiedMarket) -> None:
        if market.yes_token_id:
            book = self.clob.get_order_book(market.yes_token_id)
            asks = sorted(book.get("asks", []), key=lambda x: float(x["price"]))
            bids = sorted(book.get("bids", []), key=lambda x: -float(x["price"]))
            if asks:
                market.yes_best_ask = float(asks[0]["price"])
                market.yes_ask_depth = sum(float(a["size"]) for a in asks)
                market.yes_ask_levels = [(float(a["price"]), float(a["size"])) for a in asks]
            if bids:
                market.yes_best_bid = float(bids[0]["price"])
                market.yes_bid_depth = sum(float(b["size"]) for b in bids)

        if market.no_token_id:
            book = self.clob.get_order_book(market.no_token_id)
            asks = sorted(book.get("asks", []), key=lambda x: float(x["price"]))
            bids = sorted(book.get("bids", []), key=lambda x: -float(x["price"]))
            if asks:
                market.no_best_ask = float(asks[0]["price"])
                market.no_ask_depth = sum(float(a["size"]) for a in asks)
                market.no_ask_levels = [(float(a["price"]), float(a["size"])) for a in asks]
            if bids:
                market.no_best_bid = float(bids[0]["price"])
                market.no_bid_depth = sum(float(b["size"]) for b in bids)

        # Compute spread
        if market.yes_best_ask is not None and market.yes_best_bid is not None:
            market.spread = market.yes_best_ask - market.yes_best_bid
