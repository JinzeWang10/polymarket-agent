from src.config import ArbitrageThresholds, LeagueConfig, Settings


def test_league_config_defaults():
    lc = LeagueConfig(
        name="EPL",
        sport_id=2,
        tag_ids=[82],
        season_slug_patterns=["epl-*"],
        relegated_count=3,
        top_n_count=4,
    )
    assert lc.name == "EPL"
    assert lc.relegated_count == 3


def test_thresholds_defaults():
    t = ArbitrageThresholds()
    assert t.min_violation_pct == 1.0
    assert t.min_directional_ratio == 2.0


def test_settings_loads_config_yaml(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("scan_interval_minutes: 30\nleagues: []\nthresholds: {}")
    env = tmp_path / ".env"
    env.write_text("FEISHU_WEBHOOK_URL=https://test.hook\n")
    s = Settings(_env_file=str(env), config_path=str(config))
    assert s.scan_interval_minutes == 30
    assert s.feishu_webhook_url == "https://test.hook"
