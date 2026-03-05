from __future__ import annotations

import structlog

from src.alerts.feishu import FeishuAlerter
from src.alerts.formatter import AlertFormatter
from src.models.opportunity import ArbitrageOpportunity
from src.scanner.arbitrage_detector import ArbitrageDetector
from src.scanner.market_classifier import MarketClassifier
from src.scanner.market_fetcher import MarketFetcher
from src.scanner.team_resolver import TeamResolver

log = structlog.get_logger()

# Dedup key: (team, league, constraint_type) → last violation_pct
_DEDUP_DELTA = 2.0  # Re-alert only if violation changes by >2%


class ScanPipeline:
    def __init__(
        self,
        fetcher: MarketFetcher,
        classifier: MarketClassifier,
        resolver: TeamResolver,
        detector: ArbitrageDetector,
        alerter: FeishuAlerter,
        formatter: AlertFormatter,
    ) -> None:
        self.fetcher = fetcher
        self.classifier = classifier
        self.resolver = resolver
        self.detector = detector
        self.alerter = alerter
        self.formatter = formatter
        self._last_seen: dict[tuple[str, str, str], float] = {}

    def run(self) -> list[ArbitrageOpportunity]:
        log.info("starting scan pipeline")
        all_opps: list[ArbitrageOpportunity] = []

        league_events = self.fetcher.fetch_all_season_markets()
        for league_name, events in league_events.items():
            classified = []
            for event in events:
                classified.extend(self.classifier.classify_event(event, league_name))
            log.info("classified markets", league=league_name, count=len(classified))

            bundles = self.resolver.group_by_team(classified)
            opps = self.detector.detect_all(bundles, league_name)
            all_opps.extend(opps)
            log.info("detected opportunities", league=league_name, count=len(opps))

        new_opps = self._deduplicate(all_opps)
        log.info("scan complete", total=len(all_opps), new=len(new_opps))

        if new_opps:
            cards = self.formatter.format_opportunities(new_opps)
            for card in cards:
                self.alerter.send_card(card)

        return all_opps

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
