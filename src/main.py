from __future__ import annotations

import argparse
import signal
from datetime import datetime

import httpx
import structlog

from src.alerts.feishu import FeishuAlerter
from src.alerts.formatter import AlertFormatter
from src.api.clob import ClobClient
from src.api.gamma import GammaClient
from src.config import Settings
from src.scanner.arbitrage_detector import ArbitrageDetector
from src.scanner.market_classifier import MarketClassifier
from src.scanner.market_fetcher import MarketFetcher
from src.scanner.orderbook_verifier import OrderbookVerifier
from src.scanner.pipeline import ScanPipeline
from src.scanner.team_resolver import TeamResolver
from src.utils.logging import setup_logging

log = structlog.get_logger()


def create_pipeline(settings: Settings) -> ScanPipeline:
    http = httpx.Client(timeout=30)
    gamma = GammaClient(settings.gamma_base_url, http)
    clob = ClobClient(settings.clob_base_url, http)
    fetcher = MarketFetcher(gamma, settings.leagues)
    classifier = MarketClassifier()
    resolver = TeamResolver()
    detector = ArbitrageDetector(settings.thresholds)
    alerter = FeishuAlerter(settings.feishu_webhook_url, http)
    formatter = AlertFormatter()
    verifier = OrderbookVerifier(clob)
    return ScanPipeline(fetcher, classifier, resolver, detector, alerter, formatter, verifier)


def run_once(settings: Settings) -> None:
    pipeline = create_pipeline(settings)
    opps = pipeline.run()
    executable = [o for o in opps if o.orderbook and o.orderbook.executable]
    print(f"Scan complete. Found {len(opps)} signals, {len(executable)} executable.")
    for opp in opps:
        desc = opp.description.replace("\u00a2", "c")
        status = ""
        if opp.orderbook:
            if opp.orderbook.executable:
                extra = ""
                if opp.orderbook.actual_profit_cents is not None:
                    extra = f", real profit={opp.orderbook.actual_profit_cents:.1f}c"
                status = f" [EXECUTABLE{extra}]"
            else:
                status = f" [NOT EXECUTABLE: {opp.orderbook.notes}]"
        print(f"  [{opp.constraint_type.value}] {opp.team} ({opp.league}): {desc}{status}")


def run_scheduler(settings: Settings) -> None:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.interval import IntervalTrigger

    scheduler = BlockingScheduler()
    pipeline = create_pipeline(settings)

    def job() -> None:
        try:
            opps = pipeline.run()
            log.info("scheduled scan complete", opportunities=len(opps))
        except Exception:
            log.exception("scan failed")

    scheduler.add_job(
        job,
        IntervalTrigger(minutes=settings.scan_interval_minutes),
        next_run_time=datetime.now(),
    )

    def shutdown(signum: int, frame: object) -> None:
        log.info("shutting down scheduler")
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log.info(
        "starting scheduler",
        interval_minutes=settings.scan_interval_minutes,
    )
    scheduler.start()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Polymarket Football Arbitrage Scanner"
    )
    parser.add_argument("--once", action="store_true", help="Run single scan and exit")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    args = parser.parse_args()

    settings = Settings(config_path=args.config)
    setup_logging(settings.log_level)

    if args.once:
        run_once(settings)
    else:
        run_scheduler(settings)


if __name__ == "__main__":
    main()
