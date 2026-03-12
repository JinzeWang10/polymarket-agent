from __future__ import annotations

from datetime import datetime, timezone

import structlog

from src.models.bundle import TeamMarketBundle
from src.models.market import ClassifiedMarket
from src.models.opportunity import ArbitrageOpportunity, ConstraintType

log = structlog.get_logger()


def _fmt_price(p: float | None) -> str:
    if p is None:
        return "N/A"
    return f"{p * 100:.1f}c"


def _fmt_pct(p: float | None) -> str:
    if p is None:
        return "N/A"
    return f"{p * 100:.1f}%"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ValueDetector:
    """Detect obviously mispriced markets using cross-market implied probabilities.

    Uses multiple price sources:
    - Mid-price: what appears "obviously wrong" to observers
    - NO-side implied: 1 - no_ask = market's executable view of YES probability
    - Domain heuristics: elite teams (winner > 5%) should have ~0% relegation

    Reports actionable trades via orderbook prices.
    """

    def __init__(self, min_edge_pct: float = 2.0) -> None:
        self.min_edge_pct = min_edge_pct

    def detect_all(
        self, bundles: dict[str, TeamMarketBundle], league: str
    ) -> list[ArbitrageOpportunity]:
        opps: list[ArbitrageOpportunity] = []
        for team, bundle in bundles.items():
            opps.extend(self._check_relegation_overpriced(bundle))
            opps.extend(self._check_winner_overpriced_weak_team(bundle))
            opps.extend(self._check_top4_underpriced(bundle))
        log.info("value mispricing scan", league=league, signals=len(opps))
        return opps

    def _check_relegation_overpriced(
        self, bundle: TeamMarketBundle,
    ) -> list[ArbitrageOpportunity]:
        """Strong team with overpriced relegation -> buy NO(relegation).

        Uses two ceilings:
        1. Mathematical: P(rel) <= 1 - P(top4)
        2. Domain: teams with winner_bid > 5% historically have ~0% relegation

        Uses mid-price as the visible signal, orderbook for actionable trade.
        """
        rel: ClassifiedMarket | None = bundle.relegation
        if rel is None:
            return []

        # Use mid-price as "what the market shows" (most visible to observers)
        rel_mid = rel.yes_price
        if rel_mid <= 0:
            return []

        # Also get NO-side implied relegation (executable price)
        no_ask = rel.no_best_ask
        rel_no_implied = (1.0 - no_ask) if no_ask is not None else None

        # Get team strength from top4 and winner markets
        strength, strength_desc = self._estimate_strength(bundle)
        if strength is None or strength < 0.15:
            return []

        # ── Build ceiling: tightest of mathematical + domain heuristics ──
        ceilings: list[tuple[float, str]] = []

        # Mathematical: P(rel) <= 1 - P(top4)
        math_ceiling = 1.0 - strength
        ceilings.append((math_ceiling, f"数学上限: 1 - {strength_desc} = {_fmt_pct(math_ceiling)}"))

        # Domain: graduated ceiling based on team strength
        # A team with 50%+ top4 chance has historically ~0% relegation risk
        domain_ceiling = None
        if strength >= 0.80:
            domain_ceiling = 0.003  # 0.3%
        elif strength >= 0.50:
            domain_ceiling = 0.005  # 0.5%
        elif strength >= 0.30:
            domain_ceiling = 0.01   # 1.0%
        elif strength >= 0.15:
            domain_ceiling = 0.02   # 2.0%
        if domain_ceiling is not None:
            ceilings.append((domain_ceiling,
                f"领域经验: {strength_desc} 的球队降级概率应 <={_fmt_pct(domain_ceiling)}"))

        # Winner-based: title contender can't be relegated
        winner_bid = None
        if bundle.winner:
            winner_bid = bundle.winner.yes_best_bid or bundle.winner.yes_price
        if winner_bid and winner_bid > 0.05:
            ceilings.append((0.005,
                f"夺冠概率 {_fmt_pct(winner_bid)} 的球队不可能降级"))

        # Use the tightest ceiling
        ceiling, ceiling_desc = min(ceilings, key=lambda x: x[0])

        # Edge based on mid-price vs ceiling
        edge_mid = round((rel_mid - ceiling) * 100, 4)
        if edge_mid < self.min_edge_pct:
            return []

        # Build description
        desc_parts = [
            f"降级被高估: {ceiling_desc} -> 降级应 <={_fmt_pct(ceiling)}, "
            f"mid-price {_fmt_pct(rel_mid)} (高出 {edge_mid:.1f}%)",
        ]

        # Orderbook context
        if rel_no_implied is not None:
            desc_parts.append(
                f"NO侧隐含降级={_fmt_pct(rel_no_implied)}"
            )

        # Bid-ask spread info
        yes_bid = rel.yes_best_bid
        yes_ask = rel.yes_best_ask
        if yes_bid is not None and yes_ask is not None:
            spread = (yes_ask - yes_bid) * 100
            desc_parts.append(
                f"YES bid/ask={_fmt_pct(yes_bid)}/{_fmt_pct(yes_ask)} (spread {spread:.1f}%)"
            )

        # Actionable trade: buy NO(relegation)
        buy_edge = None
        if no_ask is not None:
            implied_no_fair = 1.0 - ceiling
            buy_edge = round((implied_no_fair - no_ask) * 100, 2)
            desc_parts.append(
                f"买入 NO(降级) @ {_fmt_price(no_ask)} "
                f"(深度 {rel.no_ask_depth:.0f} shares), "
                f"edge {buy_edge:.1f}c"
            )

        market_ids = [rel.market_id]
        urls = []
        if rel.polymarket_url:
            urls.append(rel.polymarket_url)
        if bundle.top_4 and bundle.top_4.polymarket_url:
            urls.append(bundle.top_4.polymarket_url)

        token_ids = []
        if rel.no_token_id:
            token_ids.append(rel.no_token_id)

        # Confidence based on edge size and data quality
        conf = "high"
        if edge_mid < 3:
            conf = "medium"
        if no_ask is not None and buy_edge is not None and buy_edge <= 0:
            conf = "low"  # mid-price shows edge but orderbook doesn't

        return [ArbitrageOpportunity(
            constraint_type=ConstraintType.VALUE_MISPRICING,
            team=bundle.team,
            league=bundle.league,
            description=" | ".join(desc_parts),
            markets_involved=market_ids,
            violation_pct=round(edge_mid, 2),
            potential_profit_cents=buy_edge,
            confidence=conf,
            polymarket_urls=urls,
            token_ids=token_ids,
            timestamp=_now_iso(),
        )]

    def _check_winner_overpriced_weak_team(
        self, bundle: TeamMarketBundle,
    ) -> list[ArbitrageOpportunity]:
        """Weak team (high relegation mid-price) with overpriced winner."""
        winner: ClassifiedMarket | None = bundle.winner
        rel: ClassifiedMarket | None = bundle.relegation
        if winner is None or rel is None:
            return []

        # Weak team: mid-price relegation > 10%
        if rel.yes_price < 0.10:
            return []

        # Winner mid-price should be ~0%
        w_mid = winner.yes_price
        if w_mid < 0.005:
            return []

        edge_pct = round(w_mid * 100, 4)
        if edge_pct < self.min_edge_pct:
            return []

        desc_parts = [
            f"夺冠被高估: 降级 mid {_fmt_pct(rel.yes_price)} 的球队, "
            f"夺冠 mid={_fmt_pct(w_mid)} 明显不合理",
        ]

        no_ask = winner.no_best_ask
        buy_edge = None
        if no_ask is not None:
            buy_edge = round((1.0 - no_ask) * 100, 2)
            desc_parts.append(
                f"买入 NO(夺冠) @ {_fmt_price(no_ask)} "
                f"(深度 {winner.no_ask_depth:.0f} shares), "
                f"edge ~{buy_edge:.1f}c"
            )

        market_ids = [winner.market_id]
        urls = []
        if winner.polymarket_url:
            urls.append(winner.polymarket_url)

        token_ids = []
        if winner.no_token_id:
            token_ids.append(winner.no_token_id)

        return [ArbitrageOpportunity(
            constraint_type=ConstraintType.VALUE_MISPRICING,
            team=bundle.team,
            league=bundle.league,
            description=" | ".join(desc_parts),
            markets_involved=market_ids,
            violation_pct=round(edge_pct, 2),
            potential_profit_cents=buy_edge,
            confidence="medium",
            polymarket_urls=urls,
            token_ids=token_ids,
            timestamp=_now_iso(),
        )]

    def _check_top4_underpriced(
        self, bundle: TeamMarketBundle,
    ) -> list[ArbitrageOpportunity]:
        """Strong team (high winner probability) with underpriced top4 -> buy YES(top4).

        Math: P(top4) >= P(winner). If winner bid = 40% but top4 ask = 35%,
        top4 is underpriced by 5%.
        """
        winner: ClassifiedMarket | None = bundle.winner
        top4: ClassifiedMarket | None = bundle.top_4
        if winner is None or top4 is None:
            return []

        w_bid = winner.yes_best_bid
        t_ask = top4.yes_best_ask
        if w_bid is None or t_ask is None:
            return []

        edge_pct = round((w_bid - t_ask) * 100, 4)
        if edge_pct < self.min_edge_pct:
            return []

        desc_parts = [
            f"前4被低估: 夺冠 bid={_fmt_pct(w_bid)} 暗示 top4 >={_fmt_pct(w_bid)}, "
            f"但 top4 ask 仅 {_fmt_pct(t_ask)} (低估 {edge_pct:.1f}%)",
            f"买入 YES(top4) @ {_fmt_price(t_ask)} "
            f"(深度 {top4.yes_ask_depth:.0f} shares), edge {edge_pct:.1f}c",
        ]

        market_ids = [top4.market_id, winner.market_id]
        urls = []
        if top4.polymarket_url:
            urls.append(top4.polymarket_url)
        if winner.polymarket_url:
            urls.append(winner.polymarket_url)

        token_ids = []
        if top4.yes_token_id:
            token_ids.append(top4.yes_token_id)

        return [ArbitrageOpportunity(
            constraint_type=ConstraintType.VALUE_MISPRICING,
            team=bundle.team,
            league=bundle.league,
            description=" | ".join(desc_parts),
            markets_involved=market_ids,
            violation_pct=round(edge_pct, 2),
            potential_profit_cents=round(edge_pct, 2),
            confidence="high" if edge_pct > 5 else "medium",
            polymarket_urls=urls,
            token_ids=token_ids,
            timestamp=_now_iso(),
        )]

    def _estimate_strength(
        self, bundle: TeamMarketBundle,
    ) -> tuple[float | None, str]:
        """Estimate team strength from available markets. Returns (strength, description)."""
        signals: list[tuple[float, str]] = []

        if bundle.top_4:
            p = bundle.top_4.yes_best_bid
            if p is not None and p > 0:
                signals.append((p, f"top4 bid {_fmt_pct(p)}"))
            elif bundle.top_4.yes_price > 0:
                signals.append((bundle.top_4.yes_price, f"top4 mid {_fmt_pct(bundle.top_4.yes_price)}"))

        if bundle.winner:
            p = bundle.winner.yes_best_bid
            if p is not None and p > 0:
                signals.append((p, f"winner bid {_fmt_pct(p)}"))
            elif bundle.winner.yes_price > 0:
                signals.append((bundle.winner.yes_price, f"winner mid {_fmt_pct(bundle.winner.yes_price)}"))

        if not signals:
            return None, ""

        best = max(signals, key=lambda x: x[0])
        return best[0], best[1]
