"""Entry point for the penny picking scanner.

Usage:
    python -m src.penny_main --once    # Single scan and exit
    python -m src.penny_main           # Scheduler mode (runs every penny_scan_interval_seconds)
"""
from __future__ import annotations

import argparse
import signal
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import structlog

from src.alerts.feishu import FeishuAlerter
from src.api.clob import ClobClient
from src.api.gamma import GammaClient
from src.config import Settings
from src.models.penny_signal import PennyPickingSignal
from src.scanner.penny_picking_scanner import PennyPickingScanner
from src.utils.logging import setup_logging

log = structlog.get_logger()


def is_scan_window(windows: list[dict]) -> bool:
    """Check if current Beijing time is within any configured scan window."""
    if not windows:
        return True  # no windows configured → always scan
    return bool(active_sports(windows))


def active_sports(windows: list[dict]) -> list[str]:
    """Window labels whose Beijing-time range contains the current hour."""
    hour = datetime.now(ZoneInfo("Asia/Shanghai")).hour
    labels = [
        w["label"] for w in windows
        if w["start_hour"] <= hour < w["end_hour"]
    ]
    return list(dict.fromkeys(labels))


def _print_signal(sig: PennyPickingSignal) -> None:
    price_cents = int(sig.ask_price * 100)
    print(
        f"\n  [{sig.sport}] {sig.game_title}"
        f"\n  {sig.outcome} @ {price_cents}¢ ask"
        f"  depth: {sig.ask_depth:,.0f} shares"
    )
    if sig.polymarket_url:
        print(f"  {sig.polymarket_url}")
    sys.stdout.flush()


def create_scanner(settings: Settings) -> PennyPickingScanner:
    http = httpx.Client(timeout=30)
    gamma = GammaClient(settings.gamma_base_url, http)
    clob = ClobClient(settings.clob_base_url, http)
    webhook = settings.penny_feishu_webhook_url or settings.feishu_webhook_url
    feishu = FeishuAlerter(webhook, http)

    def on_signal(sig: PennyPickingSignal) -> None:
        _print_signal(sig)
        feishu.send_penny_signal(sig)

    return PennyPickingScanner(
        gamma,
        clob,
        min_ask_price=settings.penny_min_ask_price,
        min_depth_usd=settings.penny_min_depth_usd,
        pre_filter_price=settings.penny_pre_filter_price,
        dedup_cooldown_seconds=settings.penny_dedup_cooldown_seconds,
        on_signal=on_signal,
    )


def run_once(settings: Settings) -> None:
    scanner = create_scanner(settings)
    # --once ignores time windows: scan every configured sport
    sports = list(dict.fromkeys(
        w["label"] for w in settings.penny_scan_windows
    )) or None
    print(f"Scanning for penny picking signals ({sports or ['NBA']})...")
    signals = scanner.scan(sports=sports)
    print(f"\nDone. {len(signals)} signal(s) found.")


def run_scheduler(settings: Settings) -> None:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.interval import IntervalTrigger

    scheduler = BlockingScheduler()
    scanner = create_scanner(settings)
    interval_sec = settings.penny_scan_interval_seconds

    def job() -> None:
        windows = settings.penny_scan_windows
        sports = active_sports(windows) if windows else None
        if windows and not sports:
            log.debug("outside scan window, skipping")
            return
        try:
            signals = scanner.scan(sports=sports)
            log.info("penny scan complete", signals=len(signals), sports=sports)
        except Exception:
            log.exception("penny scan failed")

    scheduler.add_job(
        job,
        IntervalTrigger(seconds=interval_sec),
        next_run_time=datetime.now(),
    )

    def shutdown(signum: int, frame: object) -> None:
        log.info("shutting down penny scanner")
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log.info("starting penny scanner", interval_seconds=interval_sec)
    scheduler.start()


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket Penny Picking Scanner")
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
