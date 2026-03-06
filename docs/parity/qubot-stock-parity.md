# Qubot Stock Parity Checklist

Last reviewed: 2026-03-06

This checklist tracks stock-feature parity between:
- Source: `../qubot` (Python)
- Target: `qbot` (Rust)

Status legend:
- `DONE`: implemented and wired in runtime
- `PARTIAL`: implemented in part, or present but not fully exposed/wired
- `MISSING`: not implemented yet

## Feature Matrix

| Feature | Qubot (Python) | Qbot (Rust) | Status | Notes |
|---|---|---|---|---|
| Daily stock ingestion/backfill | `app/services/stock_history.py` | `src/services/stock_history.rs` | `DONE` | Rust runs scheduled fetch + first-run backfill. |
| Limit-up fetch + persistence | `app/services/limit_up.py` | `src/services/limit_up.rs` | `DONE` | Core daily fetch and save are present. |
| Sector fetch + persistence | `app/services/sector.py` | `src/services/sector.rs` | `DONE` | Core daily fetch and save are present. |
| 21-signal scanner engine | `app/services/scanner/signals/*` | `src/signals/*` + `src/services/scanner.rs` | `DONE` | Signal families and detector count are aligned. |
| Daily/weekly market report | `app/services/market_report.py` | `src/services/market_report.rs` | `DONE` | Scheduled jobs and Telegram push exist. |
| Chart data API (`/chart/data`) | `app/api/routes.py` | `src/api/routes.rs` | `PARTIAL` | Rust now exposes `/api/chart/data/{code}` with daily/weekly/monthly OHLCV from DB. |
| Chip distribution API (`/chart/chips`) | `app/services/chip_distribution.py`, `app/api/routes.py` | `src/services/chip_dist.rs`, `src/api/routes.rs` | `DONE` | Rust now computes/caches distribution and exposes `/api/chart/chips/{code}`. |
| Watchlist API + commands | `app/services/watchlist.py`, bot routers | `src/services/watchlist.rs` + webhook command dispatcher | `PARTIAL` | Rust now has `/watch` `/unwatch` `/mywatch` `/export` over Telegram webhook plus API endpoints. |
| Portfolio API/commands | `app/services/portfolio.py` + bot routers | `src/services/portfolio.rs` + webhook command dispatcher | `PARTIAL` | Rust now provides `/port` `/port add` `/port del` and portfolio APIs. |
| Trading simulator | `app/services/trading_simulator.py` | `src/services/trading_sim.rs` | `PARTIAL` | Rust now supports general sim buy/sell/balance/positions/stats APIs. |
| Daban simulator | `app/services/daban_simulator.py` | `src/services/daban_sim.rs` | `PARTIAL` | Rust now supports daban sim buy/sell/balance/positions/stats APIs. |
| Daban analysis/live/sentiment | `app/services/daban_service.py` | `src/services/daban.rs` | `PARTIAL` | Rust now provides scored report/top APIs and intraday live push loop; richer sentiment analytics still missing. |
| Market AI analysis | `app/services/market_ai_analysis.py` | `src/services/ai_analysis.rs`, `src/api/routes.rs` | `PARTIAL` | Rust now provides `/api/market/overview`, optional LLM narrative (`AI_API_KEY`), and scheduled push; prompt/analysis logic still simpler than Python service. |
| Multi-provider fallback | `app/services/data_provider/*` | `src/data/*` | `PARTIAL` | Rust now supports `tushare -> eastmoney -> tencent -> db`; eastmoney includes slower historical-by-date fallback and tencent provides kline/index fallback plus delegated stock-list/limit-up/sector. |
| Stock bot command surface | `app/bots/crawler/routers/*.py` | `src/api/routes.rs` (`/telegram/webhook`) | `PARTIAL` | Rust now supports webhook command handling for major stock commands; callback UI and some advanced flows remain unported. |

## Runtime Wiring Notes (Rust)

- Runtime-wired stock services are currently:
  - `src/services/mod.rs`: `ai_analysis`, `burst_monitor`, `chip_dist`, `daban`, `daban_sim`, `limit_up`, `market`, `market_report`, `portfolio`, `scanner`, `sector`, `stock_history`, `trading_sim`, `trend_analyzer`, `watchlist`

## Prioritized Porting Batches

1. `Batch C`: AI analysis depth parity
   - Align prompt structure and deep-dive logic closer to Python analyzers.
2. `Batch D`: External data provider robustness
   - Optimize historical-by-date fallback performance and retry policies.
   - Add truly independent provider coverage for sector/limit-up/list (not delegated).
