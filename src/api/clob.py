from __future__ import annotations

from typing import Any

import httpx
import structlog

log = structlog.get_logger()


class ClobClient:
    def __init__(self, base_url: str, http: httpx.Client) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http

    def get_prices(self, token_ids: list[str], sides: list[str] | None = None) -> dict[str, float]:
        """Bulk fetch prices. Returns {token_id: price}."""
        if not token_ids:
            return {}
        params: dict[str, str] = {"token_ids": ",".join(token_ids)}
        if sides:
            params["sides"] = ",".join(sides)
        resp = self.http.get(f"{self.base_url}/prices", params=params)
        resp.raise_for_status()
        return {tid: float(p) for tid, p in resp.json().items()}

    def get_order_book(self, token_id: str) -> dict[str, Any]:
        """Get orderbook for a single token."""
        resp = self.http.get(f"{self.base_url}/book", params={"token_id": token_id})
        resp.raise_for_status()
        return resp.json()
