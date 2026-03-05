# Local Testing Design

**Goal:** Full end-to-end local test of qbot on the dev server — real Tushare data, real Telegram push, API live for manual job triggers.

**Constraints:** Docker available, Redis already running natively, no native PostgreSQL.

---

## Components

### 1. `scripts/local-test.sh`

One-shot bootstrap script:

1. `docker compose up -d postgres` (Redis already running natively)
2. Waits for PostgreSQL healthcheck
3. Copies `.env.example` → `.env` if missing, then exits with instructions to fill tokens
4. Runs `cargo run -- --run-now`

### 2. `--run-now` CLI flag (`src/main.rs`)

Parsed from CLI args on startup. After boot (DB connected, migrations applied, signal registry initialized), fires the 4 scheduler jobs in sequence:

```
StockHistoryService::update_today()       fetch today OHLCV from Tushare
LimitUpService::fetch_and_save(today)     fetch limit-up stocks
SectorService::fetch_and_save(today)      fetch sector performance
ScannerService::run_full_scan()           run all 21 signals, cache to Redis
MarketReportService::generate_daily()     assemble daily report
TelegramPusher::push(report_channel)      send to Telegram
MarketReportService::generate_weekly()    assemble weekly summary
TelegramPusher::push(report_channel)      send to Telegram
```

Then continues running with API server live on `:8080`.

**Error handling:** each step logs a warning on failure and continues — no abort.

**Non-trading days:** `update_today()` returns empty from Tushare; app runs cleanly with no data.

### 3. Job trigger API endpoints (`src/api/routes.rs`)

Four new endpoints, each spawns the job in the background and returns `{"status":"started"}`. Auth-protected by existing `check_auth`.

| Method | Path | Job |
|--------|------|-----|
| POST | `/api/jobs/fetch` | 15:05 data-fetch (history + limit-up + sector) |
| POST | `/api/jobs/scan` | 15:35 signal scan |
| POST | `/api/jobs/report/daily` | 16:00 daily report + Telegram push |
| POST | `/api/jobs/report/weekly` | Friday 20:00 weekly report + Telegram push |

---

## Data Flow

```
cargo run -- --run-now
  │
  ├── Boot (DB connect, migrations, signal registry)
  ├── fetch: OHLCV + limit-up + sector → PostgreSQL
  ├── scan: 21 signals → Redis cache
  ├── daily report → Telegram
  ├── weekly report → Telegram
  └── API server :8080 (stays alive)
```

---

## Usage

```bash
./scripts/local-test.sh        # first run: creates .env, exits with instructions
# edit .env: fill TUSHARE_TOKEN, TELEGRAM_BOT_TOKEN, channels
./scripts/local-test.sh        # second run: full end-to-end

# Manual job triggers after startup:
curl -s http://localhost:8080/health
curl -s -X POST http://localhost:8080/api/jobs/scan
curl -s http://localhost:8080/api/report/daily
```

---

## Files Changed

- **Create:** `scripts/local-test.sh`
- **Modify:** `src/main.rs` — parse `--run-now` arg, run jobs sequentially before API start
- **Modify:** `src/api/routes.rs` — add 4 job trigger endpoints
