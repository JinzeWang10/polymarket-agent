"""Live lag scanner: in-play match moves vs stale World Cup structural markets.

During a live World Cup match the moneyline reprices within seconds, while the
same nation's structural markets (group winner / advance / round of 16) are
quoted by different market makers and often lag by minutes. When the match
market moves sharply and a structural market has not followed, buying the
structural side in the direction of the move is a +EV entry (NOT risk-free —
the match can swing back and later matches still matter).

Detection per scan (intended cadence: ~60s):
1. Find live matches: events with slug `fifwc-<a>-<b>-YYYY-MM-DD` whose
   endDate (= kickoff time) is 0-2.5h in the past and not closed.
2. For each team market, compute the moneyline move over the last N minutes
   from CLOB price history.
3. If |move| >= match_move_cents, check the nation's structural markets over
   the same window; those that moved less than struct_move_cents and have a
   buyable book (ask < 97¢, depth >= min_depth_usd) produce a signal.
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone
from typing import Callable

import structlog

from src.api.clob import ClobClient
from src.api.gamma import GammaClient
from src.config import WorldCupStage
from src.models.opportunity import ArbitrageOpportunity, ConstraintType
from src.scanner.worldcup_scanner import (
    LEAGUE,
    Leg,
    _canonical,
    fetch_worldcup_events,
)

log = structlog.get_logger()

SignalCallback = Callable[[ArbitrageOpportunity], None]

# Base match event slug: fifwc-mex-rsa-2026-06-11 (sub-events carry suffixes)
_MATCH_SLUG_RE = re.compile(r"^fifwc-[a-z0-9]+-[a-z0-9]+-\d{4}-\d{2}-\d{2}$")

# Football match ≈ 2h incl. halftime + stoppage; buffer for delays
_LIVE_WINDOW_HOURS = 2.5

# Structural markets that react fast enough to live match moves:
# group winner (0), advance to knockouts (1), round of 16 (2)
_MAX_STRUCT_LEVEL = 2


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class LiveLagScanner:
    """Live match move detector → stale structural market signals."""

    def __init__(
        self,
        gamma: GammaClient,
        clob: ClobClient,
        *,
        stages: list[WorldCupStage],
        group_slugs: list[str],
        football_tag_id: int = 100350,
        match_move_cents: float = 5.0,
        struct_move_cents: float = 1.5,
        window_minutes: int = 10,
        min_depth_usd: float = 50.0,
        cooldown_seconds: int = 900,
        structure_ttl_minutes: int = 60,
        max_workers: int = 10,
        on_signal: SignalCallback | None = None,
    ) -> None:
        self.gamma = gamma
        self.clob = clob
        self.stages = stages
        self.group_slugs = group_slugs
        self.football_tag_id = football_tag_id
        self.match_move_cents = match_move_cents
        self.struct_move_cents = struct_move_cents
        self.window_minutes = window_minutes
        self.min_depth_usd = min_depth_usd
        self.cooldown_seconds = cooldown_seconds
        self.structure_ttl_minutes = structure_ttl_minutes
        self.max_workers = max_workers
        self.on_signal = on_signal

        self._structure: dict[str, dict[int, Leg]] | None = None
        self._structure_fetched_at: float = 0.0
        self._cooldown: dict[tuple[str, str, str], float] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(self) -> list[ArbitrageOpportunity]:
        live = self._find_live_matches()
        if not live:
            log.debug("live lag scan: no live matches")
            return []

        structure = self._get_structure()
        signals: list[ArbitrageOpportunity] = []

        for match_title, nation, yes_token in live:
            move = self._price_move(yes_token)
            if move is None:
                continue
            delta, latest = move
            if abs(delta) * 100 < self.match_move_cents:
                continue

            legs = structure.get(nation, {})
            struct_legs = [
                leg for lv, leg in legs.items() if lv <= _MAX_STRUCT_LEVEL
            ]
            if not struct_legs:
                continue
            log.info(
                "live match move detected",
                match=match_title,
                nation=nation,
                delta_cents=round(delta * 100, 1),
                struct_markets=len(struct_legs),
            )
            for leg in struct_legs:
                sig = self._check_struct_leg(
                    match_title, nation, delta, latest, leg,
                )
                if sig:
                    signals.append(sig)
                    if self.on_signal:
                        self.on_signal(sig)

        if signals:
            log.info("live lag scan complete", signals=len(signals))
        return signals

    # ------------------------------------------------------------------
    # Live match discovery
    # ------------------------------------------------------------------

    def _find_live_matches(self) -> list[tuple[str, str, str]]:
        """Return (match_title, nation, moneyline_yes_token) per live team."""
        now = datetime.now(timezone.utc)
        kickoff_floor = now - timedelta(hours=_LIVE_WINDOW_HOURS)

        try:
            events = self.gamma.get_events_by_tag(self.football_tag_id)
        except Exception as e:
            log.warning("live lag: event fetch failed", error=str(e))
            return []

        live: list[tuple[str, str, str]] = []
        for ev in events:
            if ev.closed or not _MATCH_SLUG_RE.match(ev.slug):
                continue
            if not ev.end_date:
                continue
            try:
                kickoff = datetime.fromisoformat(ev.end_date.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue
            if not (kickoff_floor <= kickoff <= now):
                continue
            for m in ev.markets:
                if m.closed or not m.active or len(m.clob_token_ids) < 2:
                    continue
                title = m.group_item_title
                if not title or title.startswith("Draw"):
                    continue
                live.append((ev.title or ev.slug, _canonical(title), m.clob_token_ids[0]))

        if live:
            log.info("live matches found", teams=len(live))
        return live

    # ------------------------------------------------------------------
    # Structural market map (cached)
    # ------------------------------------------------------------------

    def _get_structure(self) -> dict[str, dict[int, Leg]]:
        age = time.monotonic() - self._structure_fetched_at
        if self._structure is not None and age < self.structure_ttl_minutes * 60:
            return self._structure

        stage_events, group_events = fetch_worldcup_events(
            self.gamma, self.stages, self.group_slugs, self.max_workers,
        )
        structure: dict[str, dict[int, Leg]] = {}
        for ev in stage_events + group_events:
            for leg in ev.legs:
                if leg.nation == "Other":
                    continue
                structure.setdefault(leg.nation, {})[leg.level] = leg

        self._structure = structure
        self._structure_fetched_at = time.monotonic()
        log.info("live lag structure refreshed", nations=len(structure))
        return structure

    # ------------------------------------------------------------------
    # Price history + signal
    # ------------------------------------------------------------------

    def _price_move(self, token_id: str) -> tuple[float, float] | None:
        """(delta over window, latest price) from CLOB minute history."""
        try:
            history = self.clob.get_price_history(token_id, interval="1h", fidelity=1)
        except Exception as e:
            log.debug("live lag history failed", token=token_id[:16], error=str(e))
            return None
        if len(history) < 2:
            return None

        latest = history[-1]
        target_t = float(latest["t"]) - self.window_minutes * 60
        then = None
        for pt in history:
            if float(pt["t"]) <= target_t:
                then = pt
            else:
                break
        if then is None:
            return None
        return float(latest["p"]) - float(then["p"]), float(latest["p"])

    def _check_struct_leg(
        self,
        match_title: str,
        nation: str,
        match_delta: float,
        match_price: float,
        leg: Leg,
    ) -> ArbitrageOpportunity | None:
        struct_move = self._price_move(leg.yes_token)
        if struct_move is None:
            return None
        struct_delta, _ = struct_move
        if abs(struct_delta) * 100 >= self.struct_move_cents:
            return None  # already repriced

        side = "YES" if match_delta > 0 else "NO"
        token = leg.yes_token if side == "YES" else leg.no_token

        key = (nation, leg.market_id, side)
        now = time.monotonic()
        last = self._cooldown.get(key)
        if last is not None and now - last < self.cooldown_seconds:
            return None

        try:
            book = self.clob.get_order_book(token)
        except Exception:
            return None
        asks = sorted(book.get("asks", []), key=lambda x: float(x["price"]))
        if not asks:
            return None
        ask = float(asks[0]["price"])
        depth_usd = ask * float(asks[0]["size"])
        if ask >= 0.97 or depth_usd < self.min_depth_usd:
            return None

        self._cooldown[key] = now

        desc = (
            f"{nation} 滚球滞后 ({match_title}): "
            f"单场盘 {self.window_minutes}min 内 {match_delta * 100:+.1f}¢ "
            f"(现 {match_price * 100:.1f}¢), "
            f"但 [{leg.label}] 仅动 {struct_delta * 100:+.1f}¢ "
            f"→ 考虑买入 {side} @ {ask * 100:.1f}¢ "
            f"(深度 ${depth_usd:,.0f}, 非锁定)"
        )
        return ArbitrageOpportunity(
            constraint_type=ConstraintType.LIVE_LAG,
            team=nation,
            league=LEAGUE,
            description=desc,
            markets_involved=[leg.market_id],
            violation_pct=round(abs(match_delta) * 100, 2),
            potential_profit_cents=None,
            confidence="medium",
            polymarket_urls=[leg.url],
            token_ids=[token],
            timestamp=_now_iso(),
        )
