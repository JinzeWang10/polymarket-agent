"""World Cup structural arbitrage scanner.

Scans the 2026 World Cup stage-ladder events (winner / final / SF / QF / R16 /
advance-to-knockouts) plus the 12 group-winner events for two kinds of
risk-free pricing contradictions:

1. Stage-chain violations (SUBSET_VIOLATION): for a nation, reaching a later
   stage implies reaching every earlier stage. Buying YES on the easier stage
   plus NO on the harder stage pays at least $1 — if the combined ask is under
   $1 the difference is locked profit.

2. Slot-sum violations (MARKET_SUM): each stage event has a fixed number of
   qualifying slots, so the YES payouts across the event sum to exactly that
   number. Buying every YES (or every NO) below the slot count locks profit.

Screening uses Gamma bestAsk/bestBid (one request per event); only triggered
candidates are verified against real CLOB orderbooks before alerting.
"""
from __future__ import annotations

import statistics
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import structlog

from src.api.clob import ClobClient
from src.api.gamma import GammaClient
from src.config import WorldCupStage
from src.models.market import RawEvent, RawMarket
from src.models.opportunity import ArbitrageOpportunity, ConstraintType

log = structlog.get_logger()

SignalCallback = Callable[[ArbitrageOpportunity], None]

LEAGUE = "World Cup"

# groupItemTitle variants across events, mapped to canonical names
NATION_ALIASES = {
    "Curaçao": "Curacao",
    "Bosnia-Herzegovina": "Bosnia and Herzegovina",
    "Congo DR": "DR Congo",
}

# Pseudo-level for group-winner legs (group winner ⊆ advance to knockouts)
GROUP_LEVEL = 0

_DEDUP_DELTA = 2.0  # re-alert only if profit moves more than this many cents


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(nation: str) -> str:
    return NATION_ALIASES.get(nation, nation)


@dataclass
class Leg:
    """One nation's market inside one event, with Gamma screen prices."""
    nation: str
    level: int
    label: str
    event_slug: str
    market_id: str
    question: str
    yes_token: str
    no_token: str
    yes_ask: float | None  # Gamma bestAsk (screening only)
    yes_bid: float | None  # Gamma bestBid (screening only)

    @property
    def url(self) -> str:
        return f"https://polymarket.com/event/{self.event_slug}"


@dataclass
class EventLegs:
    """All open legs of one event plus slot bookkeeping."""
    slug: str
    title: str
    label: str
    slots: int
    legs: list[Leg]
    yes_resolved: int  # closed markets that resolved YES (slots already taken)

    @property
    def remaining_slots(self) -> int:
        return self.slots - self.yes_resolved


@dataclass
class BookTop:
    """Best ask of one CLOB orderbook."""
    ask: float
    depth_usd: float  # price * size at the best ask level


def _resolved_yes(m: RawMarket) -> bool:
    try:
        return bool(m.outcome_prices) and float(m.outcome_prices[0]) > 0.5
    except (ValueError, TypeError):
        return False


def _parse_event(
    event: RawEvent, slug: str, level: int, label: str, slots: int,
) -> EventLegs:
    legs: list[Leg] = []
    yes_resolved = 0
    for m in event.markets:
        if m.closed or not m.active:
            if m.closed and _resolved_yes(m):
                yes_resolved += 1
            continue
        if len(m.clob_token_ids) < 2:
            continue
        legs.append(Leg(
            nation=_canonical(m.group_item_title or m.question[:40]),
            level=level,
            label=label,
            event_slug=slug,
            market_id=m.id,
            question=m.question,
            yes_token=m.clob_token_ids[0],
            no_token=m.clob_token_ids[1],
            yes_ask=m.best_ask,
            yes_bid=m.best_bid,
        ))
    return EventLegs(
        slug=slug,
        title=event.title or slug,
        label=label,
        slots=slots,
        legs=legs,
        yes_resolved=yes_resolved,
    )


def fetch_worldcup_events(
    gamma: GammaClient,
    stages: list[WorldCupStage],
    group_slugs: list[str],
    max_workers: int = 10,
) -> tuple[list[EventLegs], list[EventLegs]]:
    """Fetch + parse all World Cup stage and group events by slug."""
    stage_by_slug = {s.slug: s for s in stages}
    all_slugs = list(stage_by_slug) + list(group_slugs)

    raw: dict[str, RawEvent] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(gamma.get_event_by_slug, slug): slug for slug in all_slugs
        }
        for future in as_completed(futures):
            slug = futures[future]
            try:
                event = future.result()
            except Exception as e:
                log.warning("worldcup event fetch failed", slug=slug, error=str(e))
                continue
            if event is not None:
                raw[slug] = event

    stage_events: list[EventLegs] = []
    for slug, cfg in stage_by_slug.items():
        if slug in raw:
            stage_events.append(_parse_event(
                raw[slug], slug, cfg.level, cfg.label or slug, cfg.slots,
            ))
    group_events: list[EventLegs] = []
    for slug in group_slugs:
        if slug in raw:
            group_events.append(_parse_event(
                raw[slug], slug, GROUP_LEVEL, "小组头名", slots=1,
            ))
    return stage_events, group_events


class WorldCupScanner:
    """Fetch stage/group events → Gamma screen → CLOB verify → signals."""

    def __init__(
        self,
        gamma: GammaClient,
        clob: ClobClient,
        *,
        stages: list[WorldCupStage],
        group_slugs: list[str],
        min_edge_cents: float = 1.0,
        min_sum_edge_cents: float = 5.0,
        min_depth_usd: float = 50.0,
        value_enabled: bool = True,
        value_min_mid: float = 0.02,
        value_ratio_low: float = 0.4,
        value_ratio_high: float = 2.5,
        value_min_edge_cents: float = 3.0,
        max_workers: int = 10,
        on_signal: SignalCallback | None = None,
    ) -> None:
        self.gamma = gamma
        self.clob = clob
        self.stages = sorted(stages, key=lambda s: s.level)
        self.group_slugs = group_slugs
        self.min_edge_cents = min_edge_cents
        self.min_sum_edge_cents = min_sum_edge_cents
        self.min_depth_usd = min_depth_usd
        self.value_enabled = value_enabled
        self.value_min_mid = value_min_mid
        self.value_ratio_low = value_ratio_low
        self.value_ratio_high = value_ratio_high
        self.value_min_edge_cents = value_min_edge_cents
        self.max_workers = max_workers
        self.on_signal = on_signal

        self._last_seen: dict[str, float] = {}
        self._book_cache: dict[str, BookTop | None] = {}
        self._book_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(self) -> list[ArbitrageOpportunity]:
        self._book_cache.clear()

        stage_events, group_events = self._fetch_events()
        if not stage_events:
            log.warning("worldcup scan: no stage events fetched")
            return []

        nations: dict[str, dict[int, Leg]] = {}
        for ev in stage_events:
            for leg in ev.legs:
                nations.setdefault(leg.nation, {})[leg.level] = leg
        for ev in group_events:
            for leg in ev.legs:
                if leg.nation == "Other":
                    continue
                nations.setdefault(leg.nation, {})[GROUP_LEVEL] = leg

        opps: list[ArbitrageOpportunity] = []
        opps.extend(self._scan_chains(nations))
        for ev in stage_events + group_events:
            opps.extend(self._scan_sums(ev))
        if self.value_enabled:
            opps.extend(self._scan_value(nations))

        new_opps = self._deduplicate(opps)
        log.info(
            "worldcup scan complete",
            nations=len(nations),
            signals=len(opps),
            new=len(new_opps),
        )
        if self.on_signal:
            for opp in new_opps:
                self.on_signal(opp)
        return new_opps

    # ------------------------------------------------------------------
    # Fetch + parse
    # ------------------------------------------------------------------

    def _fetch_events(self) -> tuple[list[EventLegs], list[EventLegs]]:
        return fetch_worldcup_events(
            self.gamma, self.stages, self.group_slugs, self.max_workers,
        )

    # ------------------------------------------------------------------
    # Stage-chain detection
    # ------------------------------------------------------------------

    def _scan_chains(
        self, nations: dict[str, dict[int, Leg]],
    ) -> list[ArbitrageOpportunity]:
        min_edge = self.min_edge_cents / 100.0
        candidates: list[tuple[Leg, Leg]] = []  # (subset=harder, superset=easier)

        for levels in nations.values():
            ordered = sorted(levels.values(), key=lambda l: l.level, reverse=True)
            for i, subset in enumerate(ordered):
                for superset in ordered[i + 1:]:
                    # Group-winner legs only chain against advance-to-KO (level 1)
                    if superset.level == GROUP_LEVEL:
                        continue
                    if subset.level == GROUP_LEVEL and superset.level != 1:
                        continue
                    if superset.yes_ask is None or subset.yes_bid is None:
                        continue
                    screen_cost = superset.yes_ask + (1.0 - subset.yes_bid)
                    if screen_cost < 1.0 - min_edge:
                        candidates.append((subset, superset))

        log.info("worldcup chain screen", candidates=len(candidates))
        opps: list[ArbitrageOpportunity] = []
        for subset, superset in candidates:
            opp = self._verify_chain(subset, superset)
            if opp:
                opps.append(opp)
        return opps

    def _verify_chain(
        self, subset: Leg, superset: Leg,
    ) -> ArbitrageOpportunity | None:
        """Re-check a screened chain violation against real CLOB books."""
        yes_top = self._get_book_top(superset.yes_token)
        no_top = self._get_book_top(subset.no_token)
        if yes_top is None or no_top is None:
            return None

        cost = yes_top.ask + no_top.ask
        profit_cents = round((1.0 - cost) * 100, 2)
        if profit_cents < self.min_edge_cents:
            return None
        if min(yes_top.depth_usd, no_top.depth_usd) < self.min_depth_usd:
            log.debug(
                "worldcup chain rejected on depth",
                nation=subset.nation,
                depth=min(yes_top.depth_usd, no_top.depth_usd),
            )
            return None

        desc = (
            f"{subset.nation} 阶段链倒挂: "
            f"买[{superset.label} YES @ {yes_top.ask * 100:.1f}¢] + "
            f"[{subset.label} NO @ {no_top.ask * 100:.1f}¢] "
            f"= {cost * 100:.1f}¢ < 100¢, 锁定利润 {profit_cents:.1f}¢/股"
        )
        return ArbitrageOpportunity(
            constraint_type=ConstraintType.SUBSET_VIOLATION,
            team=subset.nation,
            league=LEAGUE,
            description=desc,
            markets_involved=[superset.market_id, subset.market_id],
            violation_pct=profit_cents,
            potential_profit_cents=profit_cents,
            confidence="high",
            polymarket_urls=list(dict.fromkeys([superset.url, subset.url])),
            token_ids=[superset.yes_token, subset.no_token],
            timestamp=_now_iso(),
        )

    # ------------------------------------------------------------------
    # Slot-sum detection
    # ------------------------------------------------------------------

    def _scan_sums(self, ev: EventLegs) -> list[ArbitrageOpportunity]:
        slots = ev.remaining_slots
        n_legs = len(ev.legs)
        if slots < 1 or n_legs <= slots:
            return []
        edge = self.min_sum_edge_cents / 100.0

        opps: list[ArbitrageOpportunity] = []

        if all(leg.yes_ask is not None for leg in ev.legs):
            screen_sum = sum(leg.yes_ask for leg in ev.legs)
            if screen_sum < slots - edge:
                opp = self._verify_sum(ev, side="YES", target=slots)
                if opp:
                    opps.append(opp)

        if all(leg.yes_bid is not None for leg in ev.legs):
            screen_sum = sum(1.0 - leg.yes_bid for leg in ev.legs)
            if screen_sum < (n_legs - slots) - edge:
                opp = self._verify_sum(ev, side="NO", target=n_legs - slots)
                if opp:
                    opps.append(opp)

        return opps

    def _verify_sum(
        self, ev: EventLegs, side: str, target: int,
    ) -> ArbitrageOpportunity | None:
        """Re-check a screened slot-sum violation against real CLOB books."""
        tokens = [
            leg.yes_token if side == "YES" else leg.no_token for leg in ev.legs
        ]
        tops: dict[str, BookTop | None] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(self._get_book_top, t): t for t in tokens}
            for future in as_completed(futures):
                tops[futures[future]] = future.result()

        if any(tops.get(t) is None for t in tokens):
            return None

        total = sum(tops[t].ask for t in tokens)
        profit_cents = round((target - total) * 100, 2)
        if profit_cents < self.min_sum_edge_cents:
            return None
        min_depth = min(tops[t].depth_usd for t in tokens)
        if min_depth < self.min_depth_usd:
            log.debug("worldcup sum rejected on depth", slug=ev.slug, depth=min_depth)
            return None

        desc = (
            f"{ev.title} 名额求和套利: "
            f"买全 {side} {len(ev.legs)} 腿合计 {total * 100:.1f}¢ "
            f"< 名额 {target} × 100¢, 锁定利润 {profit_cents:.1f}¢/套"
        )
        return ArbitrageOpportunity(
            constraint_type=ConstraintType.MARKET_SUM,
            team=ev.title,
            league=LEAGUE,
            description=desc,
            markets_involved=[leg.market_id for leg in ev.legs],
            violation_pct=profit_cents,
            potential_profit_cents=profit_cents,
            confidence="high",
            polymarket_urls=[f"https://polymarket.com/event/{ev.slug}"],
            token_ids=tokens,
            timestamp=_now_iso(),
        )

    # ------------------------------------------------------------------
    # Soft-chain value detection (+EV, not risk-free)
    # ------------------------------------------------------------------

    _VALUE_MIN_PEERS = 8  # need enough nations for a meaningful peer median

    def _scan_value(
        self, nations: dict[str, dict[int, Leg]],
    ) -> list[ArbitrageOpportunity]:
        """Cross-sectional anomaly in conditional stage ratios.

        For adjacent stages, mid(harder)/mid(easier) is the market-implied
        conditional probability of advancing. A nation whose ratio is far
        from the peer median has one leg mispriced relative to the field.
        """
        opps: list[ArbitrageOpportunity] = []
        seen_buy_markets: set[str] = set()
        levels = [s.level for s in self.stages]  # ascending

        for easy_lv, hard_lv in zip(levels, levels[1:]):
            rows: list[tuple[Leg, Leg, float, float, float]] = []
            for legs in nations.values():
                easy, hard = legs.get(easy_lv), legs.get(hard_lv)
                if easy is None or hard is None:
                    continue
                mid_e = self._mid(easy)
                mid_h = self._mid(hard)
                if mid_e is None or mid_h is None:
                    continue
                if mid_e < self.value_min_mid or mid_h < 0.005:
                    continue
                rows.append((easy, hard, mid_e, mid_h, mid_h / mid_e))

            if len(rows) < self._VALUE_MIN_PEERS:
                continue
            med = statistics.median(r for *_, r in rows)
            if med <= 0:
                continue

            for easy, hard, mid_e, mid_h, ratio in rows:
                if ratio < med * self.value_ratio_low:
                    # harder stage suspiciously cheap vs peers
                    opp = self._verify_value(
                        buy=hard, ref=easy, fair=mid_e * med, med=med,
                    )
                elif ratio > med * self.value_ratio_high:
                    # easier stage suspiciously cheap vs peers
                    opp = self._verify_value(
                        buy=easy, ref=hard, fair=mid_h / med, med=med,
                    )
                else:
                    continue
                if opp and opp.markets_involved[0] not in seen_buy_markets:
                    seen_buy_markets.add(opp.markets_involved[0])
                    opps.append(opp)

        return opps

    @staticmethod
    def _mid(leg: Leg) -> float | None:
        if leg.yes_ask is None or leg.yes_bid is None:
            return None
        return (leg.yes_ask + leg.yes_bid) / 2.0

    def _verify_value(
        self, buy: Leg, ref: Leg, fair: float, med: float,
    ) -> ArbitrageOpportunity | None:
        """Confirm the cheap leg is actually buyable below peer-implied fair."""
        top = self._get_book_top(buy.yes_token)
        if top is None:
            return None
        edge_cents = round((fair - top.ask) * 100, 2)
        if edge_cents < self.value_min_edge_cents:
            return None
        if top.depth_usd < self.min_depth_usd:
            return None

        ref_mid = self._mid(ref)
        desc = (
            f"{buy.nation} 价值偏离: [{buy.label}] ask {top.ask * 100:.1f}¢, "
            f"但按同梯队条件概率中位 {med:.2f} 推算公允 ~{fair * 100:.1f}¢ "
            f"(参照 [{ref.label}] mid {ref_mid * 100:.1f}¢) "
            f"→ 期望边际 {edge_cents:.1f}¢/股 (非锁定)"
        )
        return ArbitrageOpportunity(
            constraint_type=ConstraintType.VALUE_MISPRICING,
            team=buy.nation,
            league=LEAGUE,
            description=desc,
            markets_involved=[buy.market_id, ref.market_id],
            violation_pct=edge_cents,
            potential_profit_cents=edge_cents,
            confidence="low",
            polymarket_urls=list(dict.fromkeys([buy.url, ref.url])),
            token_ids=[buy.yes_token],
            timestamp=_now_iso(),
        )

    # ------------------------------------------------------------------
    # CLOB verification helpers
    # ------------------------------------------------------------------

    def _get_book_top(self, token_id: str) -> BookTop | None:
        """Best ask + USD depth at that level, cached per scan."""
        with self._book_lock:
            if token_id in self._book_cache:
                return self._book_cache[token_id]

        result: BookTop | None = None
        try:
            book = self.clob.get_order_book(token_id)
            asks = sorted(book.get("asks", []), key=lambda x: float(x["price"]))
            if asks:
                price = float(asks[0]["price"])
                size = float(asks[0]["size"])
                result = BookTop(ask=price, depth_usd=price * size)
        except Exception as e:
            log.debug("worldcup book fetch failed", token=token_id[:16], error=str(e))

        with self._book_lock:
            self._book_cache[token_id] = result
        return result

    # ------------------------------------------------------------------
    # Dedup
    # ------------------------------------------------------------------

    def _deduplicate(
        self, opps: list[ArbitrageOpportunity],
    ) -> list[ArbitrageOpportunity]:
        new_opps: list[ArbitrageOpportunity] = []
        for opp in opps:
            key = "|".join([
                opp.team, opp.constraint_type.value, *sorted(opp.markets_involved),
            ])
            last = self._last_seen.get(key)
            if last is None or abs(opp.violation_pct - last) > _DEDUP_DELTA:
                new_opps.append(opp)
            self._last_seen[key] = opp.violation_pct
        return new_opps
