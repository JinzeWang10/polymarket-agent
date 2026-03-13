from __future__ import annotations

import argparse
import signal
import sys
from datetime import datetime

import httpx
import structlog

from src.alerts.feishu import FeishuAlerter
from src.api.clob import ClobClient
from src.api.gamma import GammaClient
from src.config import Settings
from src.models.opportunity import ArbitrageOpportunity
from src.scanner.outlier_scanner import OutlierScanner
from src.utils.logging import setup_logging

log = structlog.get_logger()


def _print_signal(opp: ArbitrageOpportunity) -> None:
    """Print a signal to terminal as soon as it's found."""
    tag = "CROSS-ARB" if opp.outlier_info and opp.outlier_info.cross_arb else opp.confidence.upper()
    desc = opp.description.replace("\u00a2", "c").replace("\u2192", "->")
    profit = f"  profit: {opp.potential_profit_cents:.1f}c" if opp.potential_profit_cents else ""
    url = opp.polymarket_urls[0] if opp.polymarket_urls else ""

    print(f"\n  [{tag}] {opp.team}")
    print(f"  {desc}")
    if profit:
        print(profit)
    if url:
        print(f"  {url}")
    sys.stdout.flush()


def create_scanner(settings: Settings) -> OutlierScanner:
    http = httpx.Client(timeout=30)
    gamma = GammaClient(settings.gamma_base_url, http)
    clob = ClobClient(settings.clob_base_url, http)

    feishu = FeishuAlerter(settings.feishu_webhook_url, http)

    def on_signal(opp: ArbitrageOpportunity) -> None:
        _print_signal(opp)
        feishu.send_outlier_signal(opp)

    return OutlierScanner(
        gamma,
        clob,
        tag_ids=settings.outlier_sport_tags or None,
        min_ref=settings.outlier_min_ref,
        on_signal=on_signal,
    )


def run_once(settings: Settings) -> None:
    scanner = create_scanner(settings)
    print("Scanning...")
    opps = scanner.scan()
    print(f"\nDone. {len(opps)} signal(s) found.")


def run_scheduler(settings: Settings) -> None:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.interval import IntervalTrigger

    scheduler = BlockingScheduler()
    scanner = create_scanner(settings)

    def job() -> None:
        try:
            opps = scanner.scan()
            log.info("scheduled scan complete", signals=len(opps))
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

    log.info("starting scheduler", interval_minutes=settings.scan_interval_minutes)
    scheduler.start()


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket Outlier Scanner")
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
