from __future__ import annotations

from datetime import datetime, timezone

import structlog

from src.config import ArbitrageThresholds
from src.models.bundle import MarketType, TeamMarketBundle
from src.models.market import ClassifiedMarket
from src.models.opportunity import ArbitrageOpportunity, ConstraintType

log = structlog.get_logger()


def _yes_ask(m: ClassifiedMarket) -> float | None:
    """Actual cost to BUY a YES share (best ask). None if no liquidity."""
    return m.yes_best_ask


def _no_ask(m: ClassifiedMarket) -> float | None:
    """Actual cost to BUY a NO share (best ask). None if no liquidity."""
    return m.no_best_ask


def _fmt_price(p: float | None) -> str:
    if p is None:
        return "N/A"
    return f"{p * 100:.1f}c"


def _fmt_depth(d: float) -> str:
    if d == 0:
        return "no liq"
    if d < 100:
        return f"{d:.0f} shares"
    return f"{d:.0f}"


class ArbitrageDetector:
    def __init__(self, thresholds: ArbitrageThresholds) -> None:
        self.thresholds = thresholds

    def detect_all(
        self, bundles: dict[str, TeamMarketBundle], league: str
    ) -> list[ArbitrageOpportunity]:
        opportunities: list[ArbitrageOpportunity] = []
        for team, bundle in bundles.items():
            opportunities.extend(self.check_no_side_arbitrage(bundle))
            opportunities.extend(self.check_mutual_exclusion(bundle))
            opportunities.extend(self.check_subset_constraint(bundle))
            opportunities.extend(self.check_directional_mispricing(bundle))
        opportunities.extend(self.check_market_sum(bundles, league))
        return [o for o in opportunities if self._passes_thresholds(o)]

    # ── Constraint 1: NO-Side Arbitrage (primary, most actionable) ──
    # Buy NO on top4 + NO on relegation at actual ASK prices.
    # If total < 100c → guaranteed profit.

    def check_no_side_arbitrage(self, bundle: TeamMarketBundle) -> list[ArbitrageOpportunity]:
        top4: ClassifiedMarket | None = bundle.top_4
        relegation: ClassifiedMarket | None = bundle.relegation
        if top4 is None or relegation is None:
            return []

        top4_no_ask = _no_ask(top4)
        rel_no_ask = _no_ask(relegation)

        # No orderbook → skip
        if top4_no_ask is None or rel_no_ask is None:
            return []

        cost_cents = (top4_no_ask + rel_no_ask) * 100
        if cost_cents >= 100:
            return []

        profit = 100 - cost_cents
        max_shares = min(top4.no_ask_depth, relegation.no_ask_depth)

        token_ids = [top4.no_token_id, relegation.no_token_id]
        return [
            ArbitrageOpportunity(
                constraint_type=ConstraintType.NO_SIDE_ARB,
                team=bundle.team,
                league=bundle.league,
                description=(
                    f"Buy NO top4 @ {_fmt_price(top4_no_ask)} (depth {_fmt_depth(top4.no_ask_depth)}) + "
                    f"NO relegation @ {_fmt_price(rel_no_ask)} (depth {_fmt_depth(relegation.no_ask_depth)}) = "
                    f"{cost_cents:.1f}c < 100c | profit {profit:.1f}c on max {max_shares:.0f} shares"
                ),
                markets_involved=[top4.market_id, relegation.market_id],
                violation_pct=round(profit, 2),
                potential_profit_cents=round(profit, 2),
                profit_pct=round(profit / cost_cents * 100, 2),
                confidence="high" if max_shares >= 10 else "low",
                polymarket_urls=[top4.polymarket_url, relegation.polymarket_url],
                token_ids=token_ids,
                timestamp=_now_iso(),
            )
        ]

    # ── Constraint 2: Mutual Exclusion ──
    # Uses orderbook ask prices: if YES_ask(A) + YES_ask(B) > 100c,
    # there's a contradiction even at executable prices.
    # Note: the actionable trade is the NO-side arb (covered above).
    # This check flags the structural mispricing signal.

    def check_mutual_exclusion(self, bundle: TeamMarketBundle) -> list[ArbitrageOpportunity]:
        results: list[ArbitrageOpportunity] = []
        pairs = [
            ("top_4", "relegation", "top4 + relegation"),
            ("winner", "second_place", "winner + second_place"),
            ("winner", "relegation", "winner + relegation"),
        ]
        for field_a, field_b, label in pairs:
            market_a: ClassifiedMarket | None = getattr(bundle, field_a, None)
            market_b: ClassifiedMarket | None = getattr(bundle, field_b, None)
            if market_a is None or market_b is None:
                continue

            # Use best bid (what you'd receive selling YES) if available, else mid
            price_a = market_a.yes_best_bid if market_a.yes_best_bid is not None else market_a.yes_price
            price_b = market_b.yes_best_bid if market_b.yes_best_bid is not None else market_b.yes_price
            source = "bid" if market_a.yes_best_bid is not None else "mid"

            total = price_a + price_b
            if total <= 1.0:
                continue

            violation = (total - 1.0) * 100

            desc_parts = [
                f"{label} = {total * 100:.1f}% > 100% (using {source} prices)",
                f"{field_a}={price_a * 100:.1f}% [ask={_fmt_price(_yes_ask(market_a))}, bid={_fmt_price(market_a.yes_best_bid)}, spread={_fmt_price(market_a.spread)}]",
                f"{field_b}={price_b * 100:.1f}% [ask={_fmt_price(_yes_ask(market_b))}, bid={_fmt_price(market_b.yes_best_bid)}, spread={_fmt_price(market_b.spread)}]",
            ]

            token_ids = []
            if market_a.yes_token_id:
                token_ids.append(market_a.yes_token_id)
            if market_b.yes_token_id:
                token_ids.append(market_b.yes_token_id)

            results.append(
                ArbitrageOpportunity(
                    constraint_type=ConstraintType.MUTUAL_EXCLUSION,
                    team=bundle.team,
                    league=bundle.league,
                    description=" | ".join(desc_parts),
                    markets_involved=[market_a.market_id, market_b.market_id],
                    violation_pct=round(violation, 2),
                    confidence="high" if source == "bid" and violation > 3 else "medium" if source == "bid" else "low",
                    polymarket_urls=[market_a.polymarket_url, market_b.polymarket_url],
                    token_ids=token_ids,
                    timestamp=_now_iso(),
                )
            )
        return results

    # ── Constraint 3: Subset ──

    def check_subset_constraint(self, bundle: TeamMarketBundle) -> list[ArbitrageOpportunity]:
        results: list[ArbitrageOpportunity] = []
        winner: ClassifiedMarket | None = bundle.winner
        top4: ClassifiedMarket | None = bundle.top_4
        second: ClassifiedMarket | None = bundle.second_place

        if winner and top4:
            w_bid = winner.yes_best_bid if winner.yes_best_bid is not None else winner.yes_price
            t_ask = _yes_ask(top4)
            t_price = t_ask if t_ask is not None else top4.yes_price
            if w_bid > t_price:
                violation = (w_bid - t_price) * 100
                results.append(
                    ArbitrageOpportunity(
                        constraint_type=ConstraintType.SUBSET_VIOLATION,
                        team=bundle.team,
                        league=bundle.league,
                        description=(
                            f"winner bid({w_bid * 100:.1f}%) > top4 ask({t_price * 100:.1f}%)"
                        ),
                        markets_involved=[winner.market_id, top4.market_id],
                        violation_pct=round(violation, 2),
                        confidence="high",
                        polymarket_urls=[winner.polymarket_url, top4.polymarket_url],
                        timestamp=_now_iso(),
                    )
                )

        if winner and second and top4:
            w_bid = winner.yes_best_bid if winner.yes_best_bid is not None else winner.yes_price
            s_bid = second.yes_best_bid if second.yes_best_bid is not None else second.yes_price
            t_ask = _yes_ask(top4)
            t_price = t_ask if t_ask is not None else top4.yes_price
            combined = w_bid + s_bid
            if combined > t_price:
                violation = (combined - t_price) * 100
                results.append(
                    ArbitrageOpportunity(
                        constraint_type=ConstraintType.SUBSET_VIOLATION,
                        team=bundle.team,
                        league=bundle.league,
                        description=(
                            f"winner bid({w_bid * 100:.1f}%) + second bid({s_bid * 100:.1f}%) "
                            f"= {combined * 100:.1f}% > top4 ask({t_price * 100:.1f}%)"
                        ),
                        markets_involved=[winner.market_id, second.market_id, top4.market_id],
                        violation_pct=round(violation, 2),
                        confidence="high" if violation > 2 else "medium",
                        polymarket_urls=[winner.polymarket_url, second.polymarket_url, top4.polymarket_url],
                        timestamp=_now_iso(),
                    )
                )
        return results

    # ── Constraint 4: Market Sum ──

    def check_market_sum(
        self, bundles: dict[str, TeamMarketBundle], league: str
    ) -> list[ArbitrageOpportunity]:
        results: list[ArbitrageOpportunity] = []
        for field, label in [("winner", "Winner"), ("relegation", "Relegation")]:
            total = 0.0
            market_ids: list[str] = []
            urls: list[str] = []
            count = 0
            for team, bundle in bundles.items():
                m: ClassifiedMarket | None = getattr(bundle, field, None)
                if m is None:
                    continue
                # Use mid price for sum (structural indicator)
                total += m.yes_price
                market_ids.append(m.market_id)
                if m.polymarket_url:
                    urls.append(m.polymarket_url)
                count += 1
            if not market_ids:
                continue
            overround = (total - 1.0) * 100
            if abs(overround) > self.thresholds.min_violation_pct:
                results.append(
                    ArbitrageOpportunity(
                        constraint_type=ConstraintType.MARKET_SUM,
                        team=f"[All {field}]",
                        league=league,
                        description=(
                            f"{label} sum ({count} teams): "
                            f"{total * 100:.1f}% (overround {overround:+.1f}%)"
                        ),
                        markets_involved=market_ids,
                        violation_pct=round(abs(overround), 2),
                        confidence="info",
                        polymarket_urls=urls[:5],
                        timestamp=_now_iso(),
                    )
                )
        return results

    # ── Constraint 5: Directional Mispricing ──
    # Using orderbook: compare relegation YES best_bid vs top4 NO best_ask

    def check_directional_mispricing(
        self, bundle: TeamMarketBundle
    ) -> list[ArbitrageOpportunity]:
        top4: ClassifiedMarket | None = bundle.top_4
        relegation: ClassifiedMarket | None = bundle.relegation
        if top4 is None or relegation is None:
            return []

        # Use orderbook prices if available
        rel_yes = relegation.yes_best_bid if relegation.yes_best_bid is not None else relegation.yes_price
        top4_no = _no_ask(top4) if _no_ask(top4) is not None else top4.no_price

        if top4_no <= 0 or rel_yes <= 0:
            return []

        ratio = rel_yes / top4_no
        if ratio < self.thresholds.min_directional_ratio:
            return []

        source = "orderbook" if relegation.yes_best_bid is not None else "mid"
        token_ids = []
        if relegation.yes_token_id:
            token_ids.append(relegation.yes_token_id)
        if top4.no_token_id:
            token_ids.append(top4.no_token_id)

        return [
            ArbitrageOpportunity(
                constraint_type=ConstraintType.DIRECTIONAL_MISPRICING,
                team=bundle.team,
                league=bundle.league,
                description=(
                    f"relegation YES bid={_fmt_price(relegation.yes_best_bid)} vs "
                    f"top4 NO ask={_fmt_price(_no_ask(top4))} -> "
                    f"{ratio:.1f}x ({source} prices)"
                ),
                markets_involved=[relegation.market_id, top4.market_id],
                violation_pct=round(ratio, 2),
                confidence="high" if source == "orderbook" and ratio > 3 else "medium",
                polymarket_urls=[relegation.polymarket_url, top4.polymarket_url],
                token_ids=token_ids,
                timestamp=_now_iso(),
            )
        ]

    def _passes_thresholds(self, opp: ArbitrageOpportunity) -> bool:
        if opp.constraint_type == ConstraintType.NO_SIDE_ARB:
            return (opp.potential_profit_cents or 0) >= self.thresholds.min_profit_cents
        if opp.constraint_type == ConstraintType.DIRECTIONAL_MISPRICING:
            return opp.violation_pct >= self.thresholds.min_directional_ratio
        return opp.violation_pct >= self.thresholds.min_violation_pct


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
