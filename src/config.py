from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class LeagueConfig(BaseModel):
    name: str
    sport_id: int
    tag_ids: list[int]
    season_slug_patterns: list[str]
    relegated_count: int
    top_n_count: int


class ArbitrageThresholds(BaseModel):
    min_violation_pct: float = 1.0
    min_profit_cents: float = 0.5
    min_directional_ratio: float = 2.0
    min_liquidity_usd: float = 100.0
    min_volume_usd: float = 50.0
    min_value_edge_pct: float = 1.0


class Settings(BaseSettings):
    feishu_webhook_url: str = ""
    penny_feishu_webhook_url: str = ""
    scan_interval_minutes: int = 60
    leagues: list[LeagueConfig] = []
    thresholds: ArbitrageThresholds = ArbitrageThresholds()
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    football_tag_id: int = 100350
    extra_football_slugs: list[str] = []
    outlier_sport_tags: list[int] = [100350]
    outlier_min_ref: float = 0.80
    # Penny picking scanner
    penny_scan_interval_seconds: int = 300
    penny_min_ask_price: float = 0.95
    penny_min_depth_usd: float = 50.0
    penny_pre_filter_price: float = 0.85
    penny_dedup_cooldown_seconds: int = 600
    penny_scan_windows: list[dict] = []
    log_level: str = "INFO"
    config_path: str = "config.yaml"

    model_config = {"env_file": ".env", "extra": "ignore"}

    def model_post_init(self, __context: object) -> None:
        p = Path(self.config_path)
        if p.exists():
            data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
            if "scan_interval_minutes" in data:
                self.scan_interval_minutes = data["scan_interval_minutes"]
            if "leagues" in data:
                self.leagues = [LeagueConfig(**lc) for lc in data["leagues"]]
            if "thresholds" in data:
                self.thresholds = ArbitrageThresholds(**data["thresholds"])
            if "football_tag_id" in data:
                self.football_tag_id = data["football_tag_id"]
            if "extra_football_slugs" in data:
                self.extra_football_slugs = data["extra_football_slugs"]
            if "outlier_sport_tags" in data:
                self.outlier_sport_tags = data["outlier_sport_tags"]
            if "outlier_min_ref" in data:
                self.outlier_min_ref = data["outlier_min_ref"]
            for key in (
                "penny_scan_interval_seconds",
                "penny_min_ask_price",
                "penny_min_depth_usd",
                "penny_pre_filter_price",
                "penny_dedup_cooldown_seconds",
                "penny_scan_windows",
            ):
                if key in data:
                    setattr(self, key, data[key])
