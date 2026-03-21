"""Penny picking scanner: detect 95-99¢ buy opportunities in live NBA games.

Scans live NBA match markets on Polymarket for outcomes priced at 95-99¢ on the
CLOB orderbook, indicating near-certain results in late-game situations.
Emits signals through a callback for alerting.
"""
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Callable

import structlog

from src.api.clob import ClobClient
from src.api.gamma import GammaClient
from src.models.penny_signal import PennyPickingSignal

log = structlog.get_logger()

SignalCallback = Callable[[PennyPickingSignal], None]

# Max hours until endDate — only keep markets ending within this window.
# NBA game ~2.5h, so 5h captures all in-progress games while excluding upcoming ones.
_LIVE_MAX_HOURS = 5


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DeduplicationTracker:
    """Suppress repeated alerts for the same signal within a cooldown window."""

    def __init__(self, cooldown_seconds: int = 600, depth_change_pct: float = 0.5) -> None:
        self.cooldown_seconds = cooldown_seconds
        self.depth_change_pct = depth_change_pct
        # key → (timestamp, depth)
        self._seen: dict[tuple[str, str, int], tuple[float, float]] = {}

    def should_alert(self, signal: PennyPickingSignal) -> bool:
        key = (signal.game_slug, signal.outcome, signal.price_bucket)
        now = time.monotonic()
        if key in self._seen:
            last_time, last_depth = self._seen[key]
            elapsed = now - last_time
            if elapsed < self.cooldown_seconds:
                # Re-alert if depth changed significantly
                if last_depth > 0 and abs(signal.ask_depth - last_depth) / last_depth > self.depth_change_pct:
                    self._seen[key] = (now, signal.ask_depth)
                    return True
                return False
        self._seen[key] = (now, signal.ask_depth)
        return True

    def cleanup(self, active_slugs: set[str]) -> None:
        """Remove entries for games that are no longer active."""
        to_remove = [k for k in self._seen if k[0] not in active_slugs]
        for k in to_remove:
            del self._seen[k]


class PennyPickingScanner:
    """Fetch live games → pre-filter → CLOB orderbook → detect 95-99¢ signals."""

    def __init__(
        self,
        gamma: GammaClient,
        clob: ClobClient,
        *,
        min_ask_price: float = 0.95,
        min_depth_usd: float = 50.0,
        pre_filter_price: float = 0.85,
        dedup_cooldown_seconds: int = 600,
        max_workers: int = 20,
        on_signal: SignalCallback | None = None,
    ) -> None:
        self.gamma = gamma
        self.clob = clob
        self.min_ask_price = min_ask_price
        self.min_depth_usd = min_depth_usd
        self.pre_filter_price = pre_filter_price
        self.max_workers = max_workers
        self.on_signal = on_signal
        self.dedup = DeduplicationTracker(cooldown_seconds=dedup_cooldown_seconds)

    def scan(self) -> list[PennyPickingSignal]:
        """Run a full scan cycle. Returns all signals found."""
        raw_markets = self.gamma.get_markets_by_tags([1], max_workers=self.max_workers)
        candidates = self._pre_filter(raw_markets)

        log.info(
            "penny picking scan start",
            total_markets=len(raw_markets),
            candidates=len(candidates),
        )

        all_signals: list[PennyPickingSignal] = []
        active_slugs: set[str] = set()

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(self._process_candidate, c): c
                for c in candidates
            }
            for future in as_completed(futures):
                candidate = futures[future]
                try:
                    signals = future.result()
                except Exception:
                    log.warning(
                        "candidate processing failed",
                        slug=candidate.get("slug", ""),
                    )
                    continue
                for sig in signals:
                    active_slugs.add(sig.game_slug)
                    if self.dedup.should_alert(sig):
                        all_signals.append(sig)
                        if self.on_signal:
                            self.on_signal(sig)

        self.dedup.cleanup(active_slugs)
        log.info("penny picking scan complete", signals=len(all_signals))
        return all_signals

    def _pre_filter(self, raw_markets: list[dict]) -> list[dict]:
        """Keep only live NBA markets with at least one high-price outcome."""
        now = datetime.now(timezone.utc)
        live_cutoff = now + timedelta(hours=_LIVE_MAX_HOURS)

        candidates: list[dict] = []
        for m in raw_markets:
            slug = (m.get("slug") or "").lower()

            if not slug.startswith("nba-"):
                continue

            # Only live markets: endDate must exist and be within the next 24h
            end_str = m.get("endDate") or m.get("endDateIso") or ""
            if not end_str:
                continue
            try:
                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                if end_dt < now or end_dt > live_cutoff:
                    continue
            except (ValueError, TypeError):
                continue

            outcomes = json.loads(m["outcomes"]) if isinstance(m.get("outcomes"), str) else m.get("outcomes", [])
            prices = json.loads(m["outcomePrices"]) if isinstance(m.get("outcomePrices"), str) else m.get("outcomePrices", [])
            tokens = json.loads(m["clobTokenIds"]) if isinstance(m.get("clobTokenIds"), str) else m.get("clobTokenIds", [])

            if not outcomes or not prices or not tokens:
                continue

            # At least one outcome >= pre_filter_price
            if not any(float(p) >= self.pre_filter_price for p in prices):
                continue

            candidates.append({
                **m,
                "_outcomes": outcomes,
                "_prices": [float(p) for p in prices],
                "_tokens": tokens,
                "_sport": "NBA",
            })
        return candidates

    def _process_candidate(self, candidate: dict) -> list[PennyPickingSignal]:
        """Check each high-price outcome against CLOB orderbook."""
        outcomes = candidate["_outcomes"]
        prices = candidate["_prices"]
        tokens = candidate["_tokens"]
        sport = candidate["_sport"]
        slug = candidate.get("slug", "")
        question = candidate.get("question", "")
        liquidity = float(candidate.get("liquidity", 0) or 0)
        volume = float(candidate.get("volume", 0) or 0)

        # Build polymarket URL from events
        url = ""
        events = candidate.get("events", [])
        if events and isinstance(events, list):
            event_slug = events[0].get("slug", "") if isinstance(events[0], dict) else ""
            if event_slug:
                url = f"https://polymarket.com/event/{event_slug}"

        signals: list[PennyPickingSignal] = []

        for i, (outcome, price, token_id) in enumerate(zip(outcomes, prices, tokens)):
            if price < self.pre_filter_price:
                continue

            try:
                book = self.clob.get_order_book(token_id)
            except Exception:
                log.debug("clob orderbook failed", token_id=token_id[:20])
                continue

            asks = sorted(book.get("asks", []), key=lambda x: float(x["price"]))
            bids = sorted(book.get("bids", []), key=lambda x: -float(x["price"]))

            # Find asks in the 95-99¢ range
            range_asks = [
                (float(a["price"]), float(a["size"]))
                for a in asks
                if self.min_ask_price <= float(a["price"]) <= 0.99
            ]
            if not range_asks:
                continue

            best_ask_price, best_ask_depth = range_asks[0]
            total_depth = sum(size for _, size in range_asks)

            # Minimum depth check
            cost = total_depth * best_ask_price
            if cost < self.min_depth_usd:
                continue

            best_bid = float(bids[0]["price"]) if bids else None
            spread = best_ask_price - best_bid if best_bid is not None else None
            price_bucket = int(best_ask_price * 100)

            signals.append(PennyPickingSignal(
                game_slug=slug,
                game_title=question,
                sport=sport,
                outcome=outcome,
                ask_price=best_ask_price,
                ask_depth=best_ask_depth,
                total_depth_in_range=total_depth,
                best_bid=best_bid,
                spread=spread,
                liquidity=liquidity,
                volume=volume,
                polymarket_url=url,
                token_id=token_id,
                timestamp=_now_iso(),
                price_bucket=price_bucket,
            ))

        return signals
