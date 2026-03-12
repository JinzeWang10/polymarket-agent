from __future__ import annotations

from typing import Any

import httpx
import structlog

from src.models.opportunity import ArbitrageOpportunity

log = structlog.get_logger()

_CONFIDENCE_COLOR = {"high": "red", "medium": "orange", "low": "yellow"}


class FeishuAlerter:
    def __init__(self, webhook_url: str, http: httpx.Client) -> None:
        self.webhook_url = webhook_url
        self.http = http

    def send_card(self, card: dict[str, Any]) -> bool:
        if not self.webhook_url:
            log.warning("feishu webhook URL not configured, skipping alert")
            return False
        payload = {"msg_type": "interactive", "card": card}
        try:
            resp = self.http.post(self.webhook_url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code", 0) != 0:
                log.error("feishu API error", code=data.get("code"), msg=data.get("msg"))
                return False
            log.info("feishu alert sent successfully")
            return True
        except httpx.HTTPError as e:
            log.error("feishu webhook failed", error=str(e))
            return False

    def send_outlier_signal(self, opp: ArbitrageOpportunity) -> bool:
        """Send a single outlier signal as a Feishu card."""
        if not self.webhook_url:
            return False

        info = opp.outlier_info
        if not info:
            return False

        is_cross = info.cross_arb
        color = "red" if is_cross else _CONFIDENCE_COLOR.get(opp.confidence, "yellow")
        conf_label = "跨侧套利" if is_cross else {"high": "高", "medium": "中", "low": "低"}.get(opp.confidence, "")

        # Market question
        lines = [f"**{info.question}**"]

        # Price context
        ref_cents = info.levels[0].ref_cents if info.levels else 0
        ltp_cents = info.last_trade_price_cents
        if info.side == "NO":
            ltp_display = f"{100 - ltp_cents:.1f}¢ (YES ltp {ltp_cents:.1f}¢)"
        else:
            ltp_display = f"{ltp_cents:.1f}¢"
        lines.append(f"最新成交价: {ltp_display}　|　6h中位价: **{ref_cents:.1f}¢**")
        lines.append("")

        # Outlier asks
        for lvl in info.levels:
            lines.append(
                f"• {info.side} ask **{lvl.price_cents:.1f}¢** × {lvl.size:,.0f} shares"
                f"　差价 **{lvl.gap_cents:.1f}¢** ({lvl.gap_pct:.1f}%)"
            )

        # Cross-arb
        if is_cross and info.cross_arb_profit_cents:
            lines.append("")
            lines.append(f"🔥 **跨侧套利 利润 {info.cross_arb_profit_cents:.1f}¢**")

        # Profit
        if opp.potential_profit_cents:
            lines.append("")
            lines.append(f"潜在利润: **{opp.potential_profit_cents:.1f}¢**　|　置信度: {conf_label}")

        elements: list[dict[str, Any]] = [
            {"tag": "markdown", "content": "\n".join(lines)},
        ]
        if opp.polymarket_urls:
            elements.append({
                "tag": "action",
                "actions": [{
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "查看盘口"},
                    "url": opp.polymarket_urls[0],
                    "type": "primary",
                }],
            })

        card = {
            "header": {
                "title": {"tag": "plain_text", "content": f"异常挂单 [{conf_label}] {opp.team}"},
                "template": color,
            },
            "elements": elements,
        }
        return self.send_card(card)
