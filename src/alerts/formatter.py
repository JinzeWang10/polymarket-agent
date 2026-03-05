from __future__ import annotations

from typing import Any

from src.models.opportunity import ArbitrageOpportunity, ConstraintType

# Priority colors based on constraint type / severity
_TEMPLATE_MAP = {
    ConstraintType.NO_SIDE_ARB: "red",
    ConstraintType.MUTUAL_EXCLUSION: "red",
    ConstraintType.SUBSET_VIOLATION: "orange",
    ConstraintType.DIRECTIONAL_MISPRICING: "orange",
    ConstraintType.MARKET_SUM: "yellow",
}

_TYPE_LABELS = {
    ConstraintType.MUTUAL_EXCLUSION: "互斥冲突",
    ConstraintType.SUBSET_VIOLATION: "子集违反",
    ConstraintType.MARKET_SUM: "市场总和异常",
    ConstraintType.NO_SIDE_ARB: "NO-Side 套利",
    ConstraintType.DIRECTIONAL_MISPRICING: "方向性定价错误",
}


class AlertFormatter:
    def format_opportunities(
        self, opportunities: list[ArbitrageOpportunity]
    ) -> list[dict[str, Any]]:
        if not opportunities:
            return []
        # Group by league
        by_league: dict[str, list[ArbitrageOpportunity]] = {}
        for opp in opportunities:
            by_league.setdefault(opp.league, []).append(opp)

        cards = []
        for league, opps in by_league.items():
            cards.append(self._build_card(league, opps))
        return cards

    def _build_card(
        self, league: str, opps: list[ArbitrageOpportunity]
    ) -> dict[str, Any]:
        # Use most severe priority color
        template = "yellow"
        for opp in opps:
            color = _TEMPLATE_MAP.get(opp.constraint_type, "yellow")
            if color == "red":
                template = "red"
                break
            if color == "orange" and template != "red":
                template = "orange"

        elements: list[dict[str, Any]] = []
        for opp in opps:
            label = _TYPE_LABELS.get(opp.constraint_type, opp.constraint_type.value)
            lines = [
                f"**类型**: {label}",
                f"**球队**: {opp.team}",
                f"**描述**: {opp.description}",
                f"**偏差**: {opp.violation_pct:.1f}%",
            ]
            if opp.potential_profit_cents is not None:
                lines.append(f"**潜在利润**: {opp.potential_profit_cents:.1f}¢")
            if opp.confidence:
                lines.append(f"**置信度**: {opp.confidence}")

            elements.append({"tag": "markdown", "content": "\n".join(lines)})

            # Add buttons for Polymarket links
            if opp.polymarket_urls:
                actions = []
                for i, url in enumerate(opp.polymarket_urls[:3]):
                    actions.append({
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": f"查看盘口 {i + 1}"},
                        "url": url,
                        "type": "primary" if i == 0 else "default",
                    })
                elements.append({"tag": "action", "actions": actions})

            elements.append({"tag": "hr"})

        return {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"Polymarket 套利机会 - {league} ({len(opps)}个)",
                },
                "template": template,
            },
            "elements": elements,
        }
