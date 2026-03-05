from __future__ import annotations

from typing import Any

import httpx
import structlog

log = structlog.get_logger()


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
