"""Penny picking scanner: detect 95-99¢ buy opportunities in live games.

Scans live match markets on Polymarket (NBA moneylines, World Cup 1X2) for
outcomes priced at 95-99¢ on the CLOB orderbook, indicating near-certain
results in late-game situations. Emits signals through a callback for alerting.
"""
from __future__ import annotations

import json
import re
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

# endDate on game markets = tip-off/kickoff time (not game end).
# A live game has endDate in the past 0-4h (game ~2.5h + buffer for OT/delays).
_LIVE_STARTED_WITHIN_HOURS = 4

# World Cup moneyline market slug: fifwc-<a>-<b>-YYYY-MM-DD-<abbr|draw>.
# Exactly one short segment after the date — sub-markets (spread-home-1pt5,
# total-0pt5, exact-score-0-0, halftime-result-home) carry longer suffixes
# and must NOT be penny-scanned: props rest at 95-99¢ even pre-match.
_WC_MONEYLINE_RE = re.compile(
    r"^fifwc-[a-z0-9]+-[a-z0-9]+-\d{4}-\d{2}-\d{2}-[a-z]{2,4}$"
)

# Window label → does this market slug belong to that sport's moneylines?
SPORT_SLUG_FILTERS: dict[str, Callable[[str], bool]] = {
    "NBA": lambda slug: slug.startswith("nba-"),
    "WorldCup": lambda slug: bool(_WC_MONEYLINE_RE.match(slug)),
}


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

    def cleanup(self) -> None:
        """Remove entries that have expired past the cooldown period."""
        now = time.monotonic()
        to_remove = [k for k, (ts, _) in self._seen.items() if now - ts >= self.cooldown_seconds]
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

    def scan(self, sports: list[str] | None = None) -> list[PennyPickingSignal]:
        """Run a full scan cycle for the given sports. Returns all signals found."""
        # Server-side endDate window = games that started 0-4h ago. Besides
        # being faster, this dodges Gamma's ~10k offset cap on big tags.
        now = datetime.now(timezone.utc)
        raw_markets = self.gamma.get_markets_by_tags(
            [1],
            max_workers=self.max_workers,
            end_date_min=(now - timedelta(hours=_LIVE_STARTED_WITHIN_HOURS))
            .strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_date_max=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        candidates = self._pre_filter(raw_markets, sports)

        log.info(
            "penny picking scan start",
            total_markets=len(raw_markets),
            candidates=len(candidates),
            sports=sports or ["NBA"],
        )

        all_signals: list[PennyPickingSignal] = []

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
                    if self.dedup.should_alert(sig):
                        all_signals.append(sig)
                        if self.on_signal:
                            self.on_signal(sig)

        self.dedup.cleanup()
        log.info("penny picking scan complete", signals=len(all_signals))
        return all_signals

    def _pre_filter(
        self, raw_markets: list[dict], sports: list[str] | None = None,
    ) -> list[dict]:
        """Keep only live moneyline markets with a high-price outcome."""
        now = datetime.now(timezone.utc)
        # endDate = tip-off time. A live game tipped off 0-4h ago.
        tipoff_floor = now - timedelta(hours=_LIVE_STARTED_WITHIN_HOURS)

        filters = {
            label: SPORT_SLUG_FILTERS[label]
            for label in (sports or ["NBA"])
            if label in SPORT_SLUG_FILTERS
        }

        candidates: list[dict] = []
        for m in raw_markets:
            slug = (m.get("slug") or "").lower()

            sport = next((s for s, f in filters.items() if f(slug)), None)
            if sport is None:
                continue

            # Only live markets: tipped off within the last 4 hours, not yet closed
            end_str = m.get("endDate") or m.get("endDateIso") or ""
            if not end_str:
                continue
            try:
                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                if end_dt > now or end_dt < tipoff_floor:
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
                "_sport": sport,
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
