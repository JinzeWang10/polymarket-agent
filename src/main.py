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
from src.scanner.live_lag_scanner import LiveLagScanner
from src.scanner.outlier_scanner import OutlierScanner
from src.scanner.worldcup_scanner import WorldCupScanner
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


def create_worldcup_scanner(settings: Settings) -> WorldCupScanner | None:
    if not settings.worldcup_enabled or not settings.worldcup_stages:
        return None
    http = httpx.Client(timeout=30)
    gamma = GammaClient(settings.gamma_base_url, http)
    clob = ClobClient(settings.clob_base_url, http)
    feishu = FeishuAlerter(settings.feishu_webhook_url, http)

    def on_signal(opp: ArbitrageOpportunity) -> None:
        _print_signal(opp)
        feishu.send_worldcup_signal(opp)

    return WorldCupScanner(
        gamma,
        clob,
        stages=settings.worldcup_stages,
        group_slugs=settings.worldcup_group_slugs,
        min_edge_cents=settings.worldcup_min_edge_cents,
        min_sum_edge_cents=settings.worldcup_min_sum_edge_cents,
        min_depth_usd=settings.worldcup_min_depth_usd,
        value_enabled=settings.worldcup_value_enabled,
        value_min_mid=settings.worldcup_value_min_mid,
        value_ratio_low=settings.worldcup_value_ratio_low,
        value_ratio_high=settings.worldcup_value_ratio_high,
        value_min_edge_cents=settings.worldcup_value_min_edge_cents,
        on_signal=on_signal,
    )


def create_live_lag_scanner(settings: Settings) -> LiveLagScanner | None:
    if not settings.worldcup_live_lag_enabled or not settings.worldcup_stages:
        return None
    http = httpx.Client(timeout=30)
    gamma = GammaClient(settings.gamma_base_url, http)
    clob = ClobClient(settings.clob_base_url, http)
    feishu = FeishuAlerter(settings.feishu_webhook_url, http)

    def on_signal(opp: ArbitrageOpportunity) -> None:
        _print_signal(opp)
        feishu.send_worldcup_signal(opp)

    return LiveLagScanner(
        gamma,
        clob,
        stages=settings.worldcup_stages,
        group_slugs=settings.worldcup_group_slugs,
        football_tag_id=settings.football_tag_id,
        match_move_cents=settings.worldcup_lag_match_move_cents,
        struct_move_cents=settings.worldcup_lag_struct_move_cents,
        window_minutes=settings.worldcup_lag_window_minutes,
        min_depth_usd=settings.worldcup_min_depth_usd,
        cooldown_seconds=settings.worldcup_lag_cooldown_seconds,
        on_signal=on_signal,
    )


def run_once(settings: Settings) -> None:
    scanner = create_scanner(settings)
    print("Scanning...")
    opps = scanner.scan()
    print(f"\nDone. {len(opps)} signal(s) found.")

    wc_scanner = create_worldcup_scanner(settings)
    if wc_scanner:
        print("World Cup scan...")
        wc_opps = wc_scanner.scan()
        print(f"Done. {len(wc_opps)} World Cup signal(s) found.")

    lag_scanner = create_live_lag_scanner(settings)
    if lag_scanner:
        print("Live lag scan...")
        lag_opps = lag_scanner.scan()
        print(f"Done. {len(lag_opps)} live lag signal(s) found.")


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

    wc_scanner = create_worldcup_scanner(settings)
    if wc_scanner:
        def wc_job() -> None:
            try:
                opps = wc_scanner.scan()
                log.info("worldcup scheduled scan complete", signals=len(opps))
            except Exception:
                log.exception("worldcup scan failed")

        scheduler.add_job(
            wc_job,
            IntervalTrigger(minutes=settings.worldcup_scan_interval_minutes),
            next_run_time=datetime.now(),
        )

    lag_scanner = create_live_lag_scanner(settings)
    if lag_scanner:
        def lag_job() -> None:
            try:
                lag_scanner.scan()
            except Exception:
                log.exception("live lag scan failed")

        scheduler.add_job(
            lag_job,
            IntervalTrigger(seconds=settings.worldcup_live_interval_seconds),
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
