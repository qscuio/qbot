# qbot

A-share stock analysis bot. Fetches daily market data from Tushare, runs 21 signal detectors, generates reports, and pushes them to a Telegram channel on a cron schedule.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                      main.rs                        │
│  boot: DB + Redis + migrations + signal registry    │
│  optional: --run-now fires all jobs immediately     │
└────────────┬──────────────┬───────────────┬─────────┘
             │              │               │
     ┌───────▼──────┐  ┌───▼────┐  ┌──────▼──────────┐
     │  Scheduler   │  │  API   │  │  Data Provider  │
     │  (cron jobs) │  │ :8080  │  │  TushareClient  │
     └───────┬──────┘  └───┬────┘  │  SinaClient     │
             │              │       └──────┬──────────┘
     ┌───────▼──────────────▼──────┐       │
     │          Services           │◄──────┘
     │  StockHistory  Scanner      │
     │  LimitUp       Sector       │
     │  Market        MarketReport │
     └───────┬─────────────────────┘
             │
     ┌───────▼──────────────────────┐
     │          Storage             │
     │  PostgreSQL  (persistent)    │
     │  Redis       (intraday cache)│
     └───────┬──────────────────────┘
             │
     ┌───────▼──────────────────────┐
     │  Telegram Pusher             │
     │  report_channel / alerts     │
     └──────────────────────────────┘
```

### Signal System (21 detectors across 6 groups)

| Group | Signals |
|-------|---------|
| Trend | MA crossover, golden cross, trend reversal |
| Volume | Volume surge, accumulation |
| Pattern | Hammer, engulfing, morning star, doji |
| Momentum | RSI oversold, MACD crossover |
| Board | Limit-up continuation, daban |
| Comprehensive | Multi-factor composite |

### Key Files

| Path | Purpose |
|------|---------|
| `src/main.rs` | Boot sequence, `--run-now` flag |
| `src/scheduler/mod.rs` | 4 cron jobs + reusable job functions |
| `src/api/routes.rs` | REST API routes incl. job trigger endpoints |
| `src/signals/` | All 21 signal detectors |
| `src/services/` | Business logic (scanner, reports, limit-up, etc.) |
| `src/storage/` | PostgreSQL helpers + Redis cache |
| `src/telegram/` | Pusher + message formatter |
| `migrations/` | 9 SQL migration files (SQLx embedded) |
| `deploy/` | Docker Compose, systemd service, setup script |
| `scripts/local-test.sh` | Local end-to-end bootstrap |

---

## Prerequisites

- Rust (stable via `rustup`) — `cargo --version`
- Docker — `docker --version`
- Redis running locally — `redis-cli ping` → `PONG`
- A [Tushare](https://tushare.pro) account with API token
- A Telegram bot token + channel ID

---

## Environment Variables

Copy `.env.example` to `.env` and fill in:

| Variable | Required | Description |
|----------|----------|-------------|
| `TUSHARE_TOKEN` | Yes | Tushare API token |
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram bot token |
| `REPORT_CHANNEL` | Yes | Channel ID for daily/weekly reports (e.g. `-1001234567890`) |
| `STOCK_ALERT_CHANNEL` | No | Channel ID for signal burst alerts |
| `API_KEY` | No | Bearer token for REST API (leave empty = open) |
| `DATA_PROXY` | No | HTTP/SOCKS5 proxy for Tushare/Sina (e.g. `socks5://127.0.0.1:1080`) |
| `DATABASE_URL` | Yes | PostgreSQL URL (default: `postgresql://qbot:qbot@127.0.0.1/qbot`) |
| `REDIS_URL` | Yes | Redis URL (default: `redis://127.0.0.1:6379`) |
| `API_PORT` | No | REST API port (default: `8080`) |

---

## Local Testing

Runs the full stack locally: PostgreSQL via Docker, Redis native, all 4 jobs fired immediately.

```bash
# First run: creates .env from .env.example, then exits
./scripts/local-test.sh

# Edit .env with your tokens, then re-run
./scripts/local-test.sh
```

After jobs complete, the API stays live. Trigger individual jobs manually:

```bash
# Health check
curl http://localhost:8080/health

# Trigger jobs individually
curl -X POST http://localhost:8080/api/jobs/fetch          # fetch OHLCV + limit-up + sector
curl -X POST http://localhost:8080/api/jobs/scan           # run 21 signal detectors
curl -X POST http://localhost:8080/api/jobs/report/daily   # generate + push daily report
curl -X POST http://localhost:8080/api/jobs/report/weekly  # generate + push weekly report

# Read results
curl http://localhost:8080/api/scan/latest
curl http://localhost:8080/api/report/daily
```

If `API_KEY` is set, add `-H "Authorization: Bearer <key>"` to protected endpoints.

---

## Scheduler

Runs automatically when the binary starts (no `--run-now`):
Jobs are scheduled with fixed `UTC+08:00` in code (`Job::new_async_tz`).

| Time (Beijing) | Days | Job |
|----------------|------|-----|
| 15:05 | Mon–Fri | Fetch OHLCV, limit-up stocks, sector data |
| 15:35 | Mon–Fri | Run full signal scan, cache to Redis |
| 16:00 | Mon–Fri | Generate daily report, push to Telegram |
| 20:00 | Friday | Generate weekly report, push to Telegram |

---

## Deployment

### GitHub Actions (automated)

Push to `main` triggers `.github/workflows/deploy.yml`, which:

1. Writes `.env` to `/opt/qbot/.env` from GitHub secrets
2. SSH into VPS → `git pull` → `cargo build --release`
3. Restarts `qbot.service` via systemd
4. Hits `/health` to confirm

**GitHub secrets used by deploy workflow** (Settings → Environments → `VPS`):

| Secret | Required | Description |
|--------|----------|-------------|
| `DEPLOY_ENABLED` | Yes | Must be `true` or deploy steps are skipped |
| `VPS_HOST` | Yes | VPS IP or hostname |
| `VPS_USER` | Yes | SSH user (for example `ubuntu`) |
| `VPS_SSH_KEY` | Yes | Private SSH key content (full PEM block) |
| `TUSHARE_TOKEN` | Yes | Tushare API token |
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram bot token |
| `REPORT_CHANNEL` | Yes | Telegram channel ID for reports |
| `STOCK_ALERT_CHANNEL` | No | Telegram channel ID for burst alerts |
| `DABAN_CHANNEL` | No | Telegram channel ID for daban notifications |
| `API_KEY` | No | REST API bearer token |
| `AI_API_KEY` | No | Optional key for AI analysis features |
| `DATA_PROXY` | No | Optional HTTP/SOCKS proxy URL |

### VPS First-Run Setup

```bash
# On the VPS, clone repo and run setup
git clone <repo> /opt/qbot
cd /opt/qbot
./deploy/setup.sh
```

`setup.sh` installs Rust + Docker if missing, starts PostgreSQL + Redis via Docker Compose, builds the binary, and installs + starts the systemd service.

```bash
# Check service status
sudo systemctl status qbot
sudo journalctl -u qbot -f
```

---

## REST API

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | No | Service health check |
| GET | `/api/signals` | No | List all 21 signals |
| GET | `/api/scan/latest` | Yes | Latest scan results from Redis |
| GET | `/api/report/daily` | Yes | Latest daily report from DB |
| POST | `/api/scan/trigger` | Yes | Trigger scan (background) |
| POST | `/api/jobs/fetch` | Yes | Trigger data fetch job |
| POST | `/api/jobs/scan` | Yes | Trigger signal scan job |
| POST | `/api/jobs/report/daily` | Yes | Trigger daily report + push |
| POST | `/api/jobs/report/weekly` | Yes | Trigger weekly report + push |

---

## Database

9 migrations applied automatically on startup via SQLx:

| Table | Contents |
|-------|----------|
| `stock_daily_bars` | OHLCV + indicators per stock per day |
| `scan_results` | Signal hits per scan run |
| `limit_up_stocks` | Limit-up stocks per day |
| `sector_daily` | Sector performance per day |
| `chip_distribution` | Chip distribution snapshots |
| `portfolio` | Portfolio positions |
| `watchlist` | Watchlist stocks |
| `simulators` | Trading simulation records |
| `reports` | Generated report content |

---

## Gotchas

- **Non-trading days**: `update_today()` returns empty from Tushare on weekends/holidays — jobs run cleanly but store no data.
- **First run backfill**: On first start with no data, a 3-year OHLCV backfill runs in the background. This takes time; the API is live during backfill.
- **Rust version**: Requires stable ≥ 1.85 (via `rustup`). The system `/usr/lib/rust-1.80/` is too old — `getrandom 0.4+` requires edition 2024.
- **Redis**: Assumed to be running natively. `scripts/local-test.sh` checks this before starting.
- **Telegram rate limits**: The pusher auto-retries once on HTTP 429 with a 5 s delay.
- **`API_KEY` empty = open**: If `API_KEY` is not set, all endpoints are unauthenticated. Set it in production.
