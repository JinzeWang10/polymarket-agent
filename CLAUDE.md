# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
python -m pytest tests/ -v

# Run a single test file or test
python -m pytest tests/test_arbitrage_detector.py -v
python -m pytest tests/test_arbitrage_detector.py::test_no_side_arb_original_report -v

# Single scan (live API, no scheduler)
python -m src.main --once

# Scheduler mode (runs every scan_interval_minutes)
python -m src.main

# Generate PDF report (runs a live scan then outputs PDF)
python generate_report.py

# Install dependencies
pip install -e ".[dev]"
```

## Architecture

This is a **football arbitrage scanner for Polymarket** prediction markets. It scans 7 European football leagues (EPL, La Liga, Bundesliga, Serie A, Ligue 1, UCL, UEL) for pricing contradictions across related markets.

### Pipeline Flow

```
MarketFetcher (Gamma API) → MarketClassifier (slug patterns) → TeamResolver (alias normalization)
  → OrderbookEnricher (CLOB API) → ArbitrageDetector + ValueDetector + PennyDetector → Alerts/Report
```

**Key design decision**: Orderbook enrichment happens BEFORE detection, not after. All detectors work with actual CLOB best ask/bid prices, not Gamma API mid-prices (which are unreliable for thin markets).

### Two API Sources

- **Gamma API** (`gamma-api.polymarket.com`): Market discovery — events, slugs, mid-prices. No auth needed.
- **CLOB API** (`clob.polymarket.com`): Orderbook data — actual executable bid/ask prices and depth. No auth needed.

### Market Classification

Events are classified by matching event slugs against glob patterns (e.g., `*winner*` → WINNER, `*relegate*` → RELEGATION). Markets are grouped into `TeamMarketBundle` per team, with one market per type (winner, top_4, relegation, second_place, etc.). Team names are normalized via an alias map in `team_resolver.py`.

### Three Detector Types

1. **ArbitrageDetector** — structural constraint violations using orderbook prices:
   - NO-side arb (buy NO on top4 + NO on relegation < 100¢ = guaranteed profit)
   - Mutual exclusion (top4 + relegation > 100%)
   - Subset violation (winner > top4)
   - Directional mispricing (relegation YES vs top4 NO ratio)
   - Market sum (overround)

2. **ValueDetector** — cross-market implied probability analysis:
   - Uses mid-price as the visible signal, orderbook for actionable trades
   - Graduated domain ceilings (team strength → max relegation probability)
   - Checks: relegation overpriced, winner overpriced for weak teams, top4 underpriced

3. **PennyDetector** — markets with YES ask ≤ 1¢ that have actual sell orders:
   - Scans both season-long and match-day markets
   - Match-day markets are filtered to moneyline only (win/draw/lose)
   - Pre-filters by mid-price ≤ 10¢ to minimize orderbook API calls

### Testing

Tests use `respx` to mock HTTP calls. The `_make_market` helper in `test_arbitrage_detector.py` creates `ClassifiedMarket` objects with orderbook fields populated (simulating enrichment). Test fixtures live in `tests/fixtures/`.

### Configuration

- `config.yaml` — league definitions (tag_ids, slug patterns, relegation/top-N counts), scan thresholds
- `.env` — `FEISHU_WEBHOOK_URL` for alerts, `LOG_LEVEL`
- Pydantic Settings loads `.env` first, then overlays `config.yaml` values in `model_post_init`

### UI/Report Language

All user-facing text (PDF reports, Feishu alerts, descriptions) is in **Chinese**. The PDF uses Microsoft YaHei font (`msyh.ttc`/`msyhbd.ttc`) from `C:/Windows/Fonts/`.
