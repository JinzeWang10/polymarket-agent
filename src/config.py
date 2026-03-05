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


class Settings(BaseSettings):
    feishu_webhook_url: str = ""
    scan_interval_minutes: int = 60
    leagues: list[LeagueConfig] = []
    thresholds: ArbitrageThresholds = ArbitrageThresholds()
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
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
