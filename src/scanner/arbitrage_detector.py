from __future__ import annotations

from datetime import datetime, timezone

import structlog

from src.config import ArbitrageThresholds
from src.models.bundle import MarketType, TeamMarketBundle
from src.models.market import ClassifiedMarket
from src.models.opportunity import ArbitrageOpportunity, ConstraintType

log = structlog.get_logger()


class ArbitrageDetector:
    def __init__(self, thresholds: ArbitrageThresholds) -> None:
        self.thresholds = thresholds

    def detect_all(
        self, bundles: dict[str, TeamMarketBundle], league: str
    ) -> list[ArbitrageOpportunity]:
        opportunities: list[ArbitrageOpportunity] = []
        for team, bundle in bundles.items():
            opportunities.extend(self.check_mutual_exclusion(bundle))
            opportunities.extend(self.check_subset_constraint(bundle))
            opportunities.extend(self.check_no_side_arbitrage(bundle))
            opportunities.extend(self.check_directional_mispricing(bundle))
        opportunities.extend(self.check_market_sum(bundles, league))
        return [o for o in opportunities if self._passes_thresholds(o)]

    # ── Constraint 1: Mutual Exclusion ──
    # Two mutually exclusive outcomes cannot both be true.
    # P(top4) + P(relegation) ≤ 100%
    # P(winner) + P(second) ≤ 100%
    # P(winner) + P(relegation) ≤ 100%

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
            total = market_a.yes_price + market_b.yes_price
            if total > 1.0:
                violation = (total - 1.0) * 100
                results.append(
                    ArbitrageOpportunity(
                        constraint_type=ConstraintType.MUTUAL_EXCLUSION,
                        team=bundle.team,
                        league=bundle.league,
                        description=(
                            f"{label} = {total * 100:.1f}% > 100% "
                            f"({field_a}={market_a.yes_price * 100:.1f}%, "
                            f"{field_b}={market_b.yes_price * 100:.1f}%)"
                        ),
                        markets_involved=[market_a.market_id, market_b.market_id],
                        violation_pct=round(violation, 2),
                        confidence="high" if violation > 3 else "medium",
                        polymarket_urls=[market_a.polymarket_url, market_b.polymarket_url],
                        timestamp=_now_iso(),
                    )
                )
        return results

    # ── Constraint 2: Subset ──
    # P(winner) ≤ P(top4)
    # P(winner) + P(second) ≤ P(top4)

    def check_subset_constraint(self, bundle: TeamMarketBundle) -> list[ArbitrageOpportunity]:
        results: list[ArbitrageOpportunity] = []
        winner: ClassifiedMarket | None = bundle.winner
        top4: ClassifiedMarket | None = bundle.top_4
        second: ClassifiedMarket | None = bundle.second_place

        if winner and top4:
            if winner.yes_price > top4.yes_price:
                violation = (winner.yes_price - top4.yes_price) * 100
                results.append(
                    ArbitrageOpportunity(
                        constraint_type=ConstraintType.SUBSET_VIOLATION,
                        team=bundle.team,
                        league=bundle.league,
                        description=(
                            f"winner({winner.yes_price * 100:.1f}%) > "
                            f"top4({top4.yes_price * 100:.1f}%)"
                        ),
                        markets_involved=[winner.market_id, top4.market_id],
                        violation_pct=round(violation, 2),
                        confidence="high",
                        polymarket_urls=[winner.polymarket_url, top4.polymarket_url],
                        timestamp=_now_iso(),
                    )
                )

        if winner and second and top4:
            combined = winner.yes_price + second.yes_price
            if combined > top4.yes_price:
                violation = (combined - top4.yes_price) * 100
                results.append(
                    ArbitrageOpportunity(
                        constraint_type=ConstraintType.SUBSET_VIOLATION,
                        team=bundle.team,
                        league=bundle.league,
                        description=(
                            f"winner({winner.yes_price * 100:.1f}%) + "
                            f"second({second.yes_price * 100:.1f}%) = "
                            f"{combined * 100:.1f}% > "
                            f"top4({top4.yes_price * 100:.1f}%)"
                        ),
                        markets_involved=[
                            winner.market_id,
                            second.market_id,
                            top4.market_id,
                        ],
                        violation_pct=round(violation, 2),
                        confidence="high" if violation > 2 else "medium",
                        polymarket_urls=[
                            winner.polymarket_url,
                            second.polymarket_url,
                            top4.polymarket_url,
                        ],
                        timestamp=_now_iso(),
                    )
                )
        return results

    # ── Constraint 3: Market Sum ──
    # Sum of all winner_yes prices should ≈ 100%. Significant overround is noteworthy.

    def check_market_sum(
        self, bundles: dict[str, TeamMarketBundle], league: str
    ) -> list[ArbitrageOpportunity]:
        results: list[ArbitrageOpportunity] = []
        market_types_to_check = [
            ("winner", "Winner market sum"),
            ("relegation", "Relegation market sum"),
        ]
        for field, label in market_types_to_check:
            total = 0.0
            market_ids: list[str] = []
            urls: list[str] = []
            for team, bundle in bundles.items():
                m: ClassifiedMarket | None = getattr(bundle, field, None)
                if m is not None:
                    total += m.yes_price
                    market_ids.append(m.market_id)
                    if m.polymarket_url:
                        urls.append(m.polymarket_url)
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
                            f"{label}: Σ yes_prices = {total * 100:.1f}% "
                            f"(overround = {overround:+.1f}%)"
                        ),
                        markets_involved=market_ids,
                        violation_pct=round(abs(overround), 2),
                        confidence="medium",
                        polymarket_urls=urls[:5],
                        timestamp=_now_iso(),
                    )
                )
        return results

    # ── Constraint 4: No-Side Arbitrage ──
    # Buy NO on top4 + NO on relegation. If cost < 100¢, guaranteed profit.
    # Because: team must either finish top4 OR get relegated OR be in between
    # → at least one NO pays out 100¢.

    def check_no_side_arbitrage(self, bundle: TeamMarketBundle) -> list[ArbitrageOpportunity]:
        results: list[ArbitrageOpportunity] = []
        top4: ClassifiedMarket | None = bundle.top_4
        relegation: ClassifiedMarket | None = bundle.relegation

        if top4 is None or relegation is None:
            return results
        if top4.no_price <= 0 or relegation.no_price <= 0:
            return results

        cost_cents = (top4.no_price + relegation.no_price) * 100
        if cost_cents < 100:
            profit = 100 - cost_cents
            results.append(
                ArbitrageOpportunity(
                    constraint_type=ConstraintType.NO_SIDE_ARB,
                    team=bundle.team,
                    league=bundle.league,
                    description=(
                        f"Buy NO top4 ({top4.no_price * 100:.1f}¢) + "
                        f"NO relegation ({relegation.no_price * 100:.1f}¢) = "
                        f"{cost_cents:.1f}¢ < 100¢ → {profit:.1f}¢ profit"
                    ),
                    markets_involved=[top4.market_id, relegation.market_id],
                    violation_pct=round(profit, 2),
                    potential_profit_cents=round(profit, 2),
                    profit_pct=round(profit / cost_cents * 100, 2) if cost_cents > 0 else 0,
                    confidence="high",
                    polymarket_urls=[top4.polymarket_url, relegation.polymarket_url],
                    timestamp=_now_iso(),
                )
            )
        return results

    # ── Constraint 5: Directional Mispricing ──
    # If relegation_yes is high but top4_no is very low, there's a contradiction:
    # relegation implies NOT top4, so relegation_yes should ≤ top4_no (approximately).

    def check_directional_mispricing(
        self, bundle: TeamMarketBundle
    ) -> list[ArbitrageOpportunity]:
        results: list[ArbitrageOpportunity] = []
        top4: ClassifiedMarket | None = bundle.top_4
        relegation: ClassifiedMarket | None = bundle.relegation

        if top4 is None or relegation is None:
            return results
        if top4.no_price <= 0 or relegation.yes_price <= 0:
            return results

        ratio = relegation.yes_price / top4.no_price if top4.no_price > 0 else 0
        if ratio >= self.thresholds.min_directional_ratio:
            results.append(
                ArbitrageOpportunity(
                    constraint_type=ConstraintType.DIRECTIONAL_MISPRICING,
                    team=bundle.team,
                    league=bundle.league,
                    description=(
                        f"relegation_yes ({relegation.yes_price * 100:.1f}%) vs "
                        f"top4_no ({top4.no_price * 100:.1f}%) → "
                        f"{ratio:.1f}x mispricing"
                    ),
                    markets_involved=[relegation.market_id, top4.market_id],
                    violation_pct=round(ratio, 2),
                    confidence="high" if ratio > 3 else "medium",
                    polymarket_urls=[relegation.polymarket_url, top4.polymarket_url],
                    timestamp=_now_iso(),
                )
            )
        return results

    def _passes_thresholds(self, opp: ArbitrageOpportunity) -> bool:
        if opp.constraint_type == ConstraintType.NO_SIDE_ARB:
            return (opp.potential_profit_cents or 0) >= self.thresholds.min_profit_cents
        if opp.constraint_type == ConstraintType.DIRECTIONAL_MISPRICING:
            return opp.violation_pct >= self.thresholds.min_directional_ratio
        return opp.violation_pct >= self.thresholds.min_violation_pct


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
