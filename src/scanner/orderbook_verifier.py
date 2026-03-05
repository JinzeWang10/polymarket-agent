from __future__ import annotations

import structlog

from src.api.clob import ClobClient
from src.models.opportunity import (
    ArbitrageOpportunity,
    ConstraintType,
    OrderbookLevel,
    OrderbookVerification,
)

log = structlog.get_logger()


class OrderbookVerifier:
    """Verify arbitrage opportunities against actual CLOB orderbook depth."""

    def __init__(self, clob: ClobClient) -> None:
        self.clob = clob

    def verify_all(
        self, opps: list[ArbitrageOpportunity]
    ) -> list[ArbitrageOpportunity]:
        for opp in opps:
            try:
                opp.orderbook = self._verify_one(opp)
            except Exception as e:
                log.warning("orderbook verification failed", team=opp.team, error=str(e))
                opp.orderbook = OrderbookVerification(
                    verified=True, executable=False, notes=f"Verification error: {e}"
                )
        return opps

    def _verify_one(self, opp: ArbitrageOpportunity) -> OrderbookVerification:
        if opp.constraint_type == ConstraintType.NO_SIDE_ARB:
            return self._verify_no_side_arb(opp)
        if opp.constraint_type == ConstraintType.MARKET_SUM:
            # Market sum is informational, not directly tradeable as a single arb
            return OrderbookVerification(
                verified=True, executable=False,
                notes="Market sum is informational; not a single tradeable position",
            )
        # For mutual_exclusion, subset, directional: check if any liquidity exists
        return self._verify_general_liquidity(opp)

    def _verify_no_side_arb(self, opp: ArbitrageOpportunity) -> OrderbookVerification:
        """For NO-side arb: check actual ASK prices on both NO tokens."""
        if len(opp.token_ids) < 2:
            return OrderbookVerification(
                verified=True, executable=False, notes="Missing NO token IDs",
            )

        token_a, token_b = opp.token_ids[0], opp.token_ids[1]
        book_a = self.clob.get_order_book(token_a)
        book_b = self.clob.get_order_book(token_b)

        asks_a = sorted(book_a.get("asks", []), key=lambda x: float(x["price"]))
        asks_b = sorted(book_b.get("asks", []), key=lambda x: float(x["price"]))

        has_liq_a = len(asks_a) > 0
        has_liq_b = len(asks_b) > 0

        best_ask_a = (
            OrderbookLevel(
                price_cents=float(asks_a[0]["price"]) * 100,
                size=float(asks_a[0]["size"]),
            )
            if has_liq_a else None
        )
        best_ask_b = (
            OrderbookLevel(
                price_cents=float(asks_b[0]["price"]) * 100,
                size=float(asks_b[0]["size"]),
            )
            if has_liq_b else None
        )

        depth_a = sum(float(a["size"]) for a in asks_a[:5])
        depth_b = sum(float(a["size"]) for a in asks_b[:5])

        if not has_liq_a or not has_liq_b:
            missing = []
            if not has_liq_a:
                missing.append("token A (top4 NO)")
            if not has_liq_b:
                missing.append("token B (relegation NO)")
            return OrderbookVerification(
                verified=True,
                executable=False,
                has_liquidity_a=has_liq_a,
                has_liquidity_b=has_liq_b,
                best_ask_a=best_ask_a,
                best_ask_b=best_ask_b,
                depth_token_a=depth_a,
                depth_token_b=depth_b,
                notes=f"No liquidity on {', '.join(missing)}",
            )

        actual_cost = best_ask_a.price_cents + best_ask_b.price_cents
        actual_profit = 100.0 - actual_cost
        executable = actual_profit > 0
        max_shares = min(best_ask_a.size, best_ask_b.size)

        return OrderbookVerification(
            verified=True,
            executable=executable,
            actual_cost_cents=round(actual_cost, 2),
            actual_profit_cents=round(actual_profit, 2) if executable else None,
            actual_profit_pct=round(actual_profit / actual_cost * 100, 2) if executable and actual_cost > 0 else None,
            has_liquidity_a=True,
            has_liquidity_b=True,
            best_ask_a=best_ask_a,
            best_ask_b=best_ask_b,
            depth_token_a=depth_a,
            depth_token_b=depth_b,
            notes=(
                f"Executable! Cost {actual_cost:.1f}c, profit {actual_profit:.1f}c on max {max_shares:.0f} shares"
                if executable
                else f"Not profitable at orderbook prices: cost {actual_cost:.1f}c >= 100c"
            ),
        )

    def _verify_general_liquidity(
        self, opp: ArbitrageOpportunity
    ) -> OrderbookVerification:
        """Check if the tokens involved have any orderbook depth."""
        if len(opp.token_ids) < 2:
            return OrderbookVerification(
                verified=True, executable=False, notes="Missing token IDs for verification",
            )

        token_a, token_b = opp.token_ids[0], opp.token_ids[1]
        book_a = self.clob.get_order_book(token_a)
        book_b = self.clob.get_order_book(token_b)

        asks_a = book_a.get("asks", [])
        bids_a = book_a.get("bids", [])
        asks_b = book_b.get("asks", [])
        bids_b = book_b.get("bids", [])

        has_liq_a = len(asks_a) > 0 or len(bids_a) > 0
        has_liq_b = len(asks_b) > 0 or len(bids_b) > 0

        depth_a = sum(float(x["size"]) for x in (asks_a + bids_a)[:10])
        depth_b = sum(float(x["size"]) for x in (asks_b + bids_b)[:10])

        best_ask_a = (
            OrderbookLevel(
                price_cents=float(sorted(asks_a, key=lambda x: float(x["price"]))[0]["price"]) * 100,
                size=float(sorted(asks_a, key=lambda x: float(x["price"]))[0]["size"]),
            )
            if asks_a else None
        )
        best_ask_b = (
            OrderbookLevel(
                price_cents=float(sorted(asks_b, key=lambda x: float(x["price"]))[0]["price"]) * 100,
                size=float(sorted(asks_b, key=lambda x: float(x["price"]))[0]["size"]),
            )
            if asks_b else None
        )

        notes_parts = []
        if not has_liq_a:
            notes_parts.append("Token A: no liquidity")
        else:
            notes_parts.append(f"Token A: depth={depth_a:.0f} shares")
        if not has_liq_b:
            notes_parts.append("Token B: no liquidity")
        else:
            notes_parts.append(f"Token B: depth={depth_b:.0f} shares")

        return OrderbookVerification(
            verified=True,
            executable=has_liq_a and has_liq_b,
            has_liquidity_a=has_liq_a,
            has_liquidity_b=has_liq_b,
            best_ask_a=best_ask_a,
            best_ask_b=best_ask_b,
            depth_token_a=depth_a,
            depth_token_b=depth_b,
            notes="; ".join(notes_parts),
        )
