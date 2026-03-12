from __future__ import annotations

import structlog

from src.alerts.feishu import FeishuAlerter
from src.alerts.formatter import AlertFormatter
from src.api.gamma import GammaClient
from src.models.market import ClassifiedMarket, MarketType
from src.models.opportunity import ArbitrageOpportunity
from src.scanner.arbitrage_detector import ArbitrageDetector
from src.scanner.market_classifier import MarketClassifier
from src.scanner.market_fetcher import MarketFetcher
from src.scanner.orderbook_enricher import OrderbookEnricher
from src.scanner.outlier_detector import OutlierDetector
from src.scanner.penny_detector import PennyDetector
from src.scanner.team_resolver import TeamResolver
from src.scanner.value_detector import ValueDetector

log = structlog.get_logger()

_DEDUP_DELTA = 2.0


class ScanPipeline:
    def __init__(
        self,
        fetcher: MarketFetcher,
        classifier: MarketClassifier,
        resolver: TeamResolver,
        detector: ArbitrageDetector,
        alerter: FeishuAlerter,
        formatter: AlertFormatter,
        enricher: OrderbookEnricher | None = None,
        value_detector: ValueDetector | None = None,
        penny_detector: PennyDetector | None = None,
        outlier_detector: OutlierDetector | None = None,
        football_tag_id: int = 0,
        extra_football_slugs: list[str] | None = None,
        gamma: GammaClient | None = None,
        outlier_sport_tags: list[int] | None = None,
        outlier_min_ref: float = 0.80,
    ) -> None:
        self.fetcher = fetcher
        self.classifier = classifier
        self.resolver = resolver
        self.detector = detector
        self.alerter = alerter
        self.formatter = formatter
        self.enricher = enricher
        self.value_detector = value_detector
        self.penny_detector = penny_detector
        self.outlier_detector = outlier_detector
        self.football_tag_id = football_tag_id
        self.extra_football_slugs = extra_football_slugs or []
        self.gamma = gamma
        self.outlier_sport_tags = outlier_sport_tags or []
        self.outlier_min_ref = outlier_min_ref
        self._last_seen: dict[tuple[str, str, str], float] = {}

    def run(self) -> list[ArbitrageOpportunity]:
        log.info("starting scan pipeline")
        all_opps: list[ArbitrageOpportunity] = []
        all_season_classified: list = []

        league_events = self.fetcher.fetch_all_season_markets()
        for league_name, events in league_events.items():
            classified = []
            for event in events:
                classified.extend(self.classifier.classify_event(event, league_name))
            log.info("classified markets", league=league_name, count=len(classified))
            all_season_classified.extend(classified)

            bundles = self.resolver.group_by_team(classified)

            # Enrich with orderbook data BEFORE detection
            if self.enricher:
                bundles = self.enricher.enrich_bundles(bundles)

            opps = self.detector.detect_all(bundles, league_name)
            all_opps.extend(opps)

            value_count = 0
            if self.value_detector:
                value_opps = self.value_detector.detect_all(bundles, league_name)
                all_opps.extend(value_opps)
                value_count = len(value_opps)

            # Outlier scan on season-long markets (already enriched)
            outlier_count = 0
            if self.outlier_detector:
                outlier_opps = self.outlier_detector.detect(classified, league_name)
                all_opps.extend(outlier_opps)
                outlier_count = len(outlier_opps)

            # Penny scan on season-long markets (already enriched)
            penny_count = 0
            if self.penny_detector:
                penny_opps = self.penny_detector.detect(classified, league_name)
                all_opps.extend(penny_opps)
                penny_count = len(penny_opps)

            log.info("detected opportunities", league=league_name,
                     arb=len(opps), value=value_count,
                     outlier=outlier_count, penny=penny_count)

        # Broad sport outlier scan (Gamma bestAsk pre-filter + CLOB enrichment)
        if self.outlier_detector and self.outlier_sport_tags and self.gamma:
            scanned_ids = {m.market_id for m in all_season_classified}
            extra_outliers = self._scan_broad_outliers(scanned_ids)
            all_opps.extend(extra_outliers)

        # Penny scan on match-day markets
        if self.penny_detector:
            match_penny = self._scan_match_penny_markets()
            all_opps.extend(match_penny)

        new_opps = self._deduplicate(all_opps)
        log.info("scan complete", total=len(all_opps), new=len(new_opps))

        if new_opps:
            cards = self.formatter.format_opportunities(new_opps)
            for card in cards:
                self.alerter.send_card(card)

        return all_opps

    _NO_SIDE_CAP = 100  # max NO-side candidates to enrich per scan

    def _scan_broad_outliers(
        self, already_scanned: set[str],
    ) -> list[ArbitrageOpportunity]:
        """Scan sport markets for outlier asks on high-confidence (>min_ref) sides.

        YES side: Gamma bestAsk pre-filter (free, fast).
        NO side: ltp <= (1 - min_ref), capped by liquidity, NO-only enrichment.
        """
        if not self.gamma or not self.enricher:
            return []

        raw_markets = self.gamma.get_markets_by_tags(self.outlier_sport_tags)

        min_ref = self.outlier_min_ref
        gap_pct = self.outlier_detector.min_gap_pct

        yes_candidates: list[dict] = []
        no_candidates: list[dict] = []
        yes_candidate_ids: set[str] = set()

        for m in raw_markets:
            mid = str(m.get("id", ""))
            if mid in already_scanned:
                continue

            ltp = float(m.get("lastTradePrice", 0) or 0)
            if ltp <= 0:
                continue

            ba = float(m.get("bestAsk") or 0)

            # YES-side: Gamma bestAsk suspiciously below ltp
            if ltp >= min_ref and ba > 0 and ba < ltp * (1 - gap_pct):
                yes_candidates.append(m)
                yes_candidate_ids.add(mid)
            # NO-side: high NO ref, no Gamma pre-filter available
            elif (1.0 - ltp) >= min_ref:
                no_candidates.append(m)

        # Sort NO candidates by liquidity (most liquid = most interesting)
        no_candidates.sort(
            key=lambda m: float(m.get("liquidity", 0) or 0), reverse=True,
        )
        no_candidates = no_candidates[:self._NO_SIDE_CAP]

        log.info(
            "broad sport outlier scan",
            total_markets=len(raw_markets),
            yes_candidates=len(yes_candidates),
            no_candidates=len(no_candidates),
            min_ref=min_ref,
        )

        opps: list[ArbitrageOpportunity] = []

        # YES-side: full enrichment (both sides)
        if yes_candidates:
            yes_classified = [self._raw_market_to_classified(m) for m in yes_candidates]
            self.enricher.enrich_markets(yes_classified)
            opps.extend(self.outlier_detector.detect(
                yes_classified, "Sports", use_price_history=True, min_ref=min_ref,
            ))

        # NO-side: enrich only NO token (halves API calls)
        if no_candidates:
            no_classified = [self._raw_market_to_classified(m) for m in no_candidates]
            self.enricher.enrich_markets_no_only(no_classified)
            opps.extend(self.outlier_detector.detect(
                no_classified, "Sports", use_price_history=True, min_ref=min_ref,
            ))

        return opps

    @staticmethod
    def _raw_market_to_classified(raw: dict) -> ClassifiedMarket:
        """Convert a raw Gamma API market dict to ClassifiedMarket."""
        prices = raw.get("outcomePrices", [])
        if isinstance(prices, str):
            import json
            prices = json.loads(prices)
        tokens = raw.get("clobTokenIds", [])
        if isinstance(tokens, str):
            import json
            tokens = json.loads(tokens)

        yes_price = float(prices[0]) if prices else 0.0
        no_price = float(prices[1]) if len(prices) > 1 else 0.0

        slug = raw.get("slug", "")
        group_title = raw.get("groupItemTitle", "")
        question = raw.get("question", "")

        return ClassifiedMarket(
            market_id=str(raw.get("id", "")),
            event_id="",
            event_slug=slug,
            league="Sports",
            team=group_title or question[:40],
            market_type=MarketType.UNKNOWN,
            yes_price=yes_price,
            no_price=no_price,
            yes_token_id=tokens[0] if tokens else "",
            no_token_id=tokens[1] if len(tokens) > 1 else "",
            last_trade_price=float(raw.get("lastTradePrice", 0) or 0),
            liquidity=float(raw.get("liquidity", 0) or 0),
            volume=float(raw.get("volume", 0) or 0),
            question=question,
            polymarket_url=f"https://polymarket.com/event/{slug}" if slug else "",
        )

    def _scan_match_penny_markets(self) -> list[ArbitrageOpportunity]:
        """Scan match-day events for penny markets (moneyline only)."""
        opps: list[ArbitrageOpportunity] = []
        match_events = self.fetcher.get_match_events()
        for league_name, events in match_events.items():
            match_classified = []
            for event in events:
                match_classified.extend(
                    self.classifier.classify_match_event(event, league_name)
                )

            # Pre-filter by mid-price to reduce orderbook API calls
            candidates = [m for m in match_classified if m.yes_price <= 0.10]
            if self.enricher and candidates:
                self.enricher.enrich_markets(candidates)

            penny_opps = self.penny_detector.detect(candidates, league_name, season=False)
            opps.extend(penny_opps)
            if match_classified:
                log.info(
                    "match-day penny scan",
                    league=league_name,
                    match_markets=len(match_classified),
                    candidates=len(candidates),
                    penny=len(penny_opps),
                )
        return opps

    def _deduplicate(
        self, opps: list[ArbitrageOpportunity]
    ) -> list[ArbitrageOpportunity]:
        new_opps: list[ArbitrageOpportunity] = []
        for opp in opps:
            key = (opp.team, opp.league, opp.constraint_type.value)
            last_val = self._last_seen.get(key)
            if last_val is None or abs(opp.violation_pct - last_val) > _DEDUP_DELTA:
                new_opps.append(opp)
            self._last_seen[key] = opp.violation_pct
        return new_opps
