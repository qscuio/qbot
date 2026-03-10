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
     ┌───────▼──────┐  ┌───▼────┐  ┌──────▼──────────────────────┐
     │  Scheduler   │  │  API   │  │  Data Provider              │
     │  (cron jobs) │  │ :8080  │  │ Tushare -> Eastmoney ->     │
     └───────┬──────┘  └───┬────┘  │ Tencent -> DB               │
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
| `TELEGRAM_WEBHOOK_SECRET` | No | Secret token for Telegram webhook header verification (`X-Telegram-Bot-Api-Secret-Token`) |
| `WEBHOOK_URL` | No | Public base URL used for Telegram webhook registration (e.g. `https://bot.example.com`) |
| `REPORT_CHANNEL` | Yes | Channel ID for daily/weekly reports (e.g. `-1001234567890`) |
| `STOCK_ALERT_CHANNEL` | No | Channel ID for signal burst alerts |
| `DABAN_CHANNEL` | No | Channel ID for daban live/sentiment pushes |
| `API_KEY` | No | Bearer token for REST API (leave empty = open) |
| `AI_API_KEY` | No | API key used by AI narrative generation (`/api/market/overview` and AI report loop) |
| `AI_BASE_URL` | No | Chat Completions base URL (default `https://api.openai.com/v1`) |
| `AI_MODEL` | No | AI model name for narrative generation (default `gpt-4o-mini`) |
| `DATA_PROXY` | No | HTTP/SOCKS5 proxy for Tushare/Sina (e.g. `socks5://127.0.0.1:1080`) |
| `DATABASE_URL` | Yes | PostgreSQL URL (default: `postgresql://qbot:qbot@127.0.0.1/qbot`) |
| `REDIS_URL` | Yes | Redis URL (default: `redis://127.0.0.1:6379`) |
| `API_PORT` | No | REST API port (default: `8080`) |
| `ENABLE_DABAN_LIVE` | No | Enable intraday daban live loop (`true`/`false`, default `false`) |
| `ENABLE_AI_ANALYSIS` | No | Enable scheduled AI market analysis push (`true`/`false`, default `false`) |
| `ENABLE_CHIP_DIST` | No | Enable scheduled chip-distribution refresh (`true`/`false`, default `true`) |
| `ENABLE_SIGNAL_AUTO_TRADING` | No | Enable signal-based auto paper trading loop with Telegram action pushes (`true`/`false`, default `false`) |

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
curl http://localhost:8080/api/scan/prestart
curl http://localhost:8080/api/scan/stats
curl http://localhost:8080/api/report/daily
```

If `API_KEY` is set, add `-H "Authorization: Bearer <key>"` to protected endpoints.

---

## Scheduler

Runs automatically when the binary starts (no `--run-now`):
Jobs are scheduled with fixed `UTC+08:00` in code (`Job::new_async_tz`).

| Time (Beijing) | Days | Job |
|----------------|------|-----|
| 17:00 | Mon–Fri | Fetch OHLCV, limit-up stocks, sector data |
| 17:30 | Mon–Fri | Run full signal scan, cache to Redis |
| 18:00 | Mon–Fri | Generate daily report, push to Telegram |
| 20:00 | Friday | Generate weekly report, push to Telegram |

Time basis:
- Runtime date/time logic uses fixed `UTC+08:00` (`Asia/Shanghai` equivalent), not server local timezone.

---

## Deployment

### GitHub Actions (automated)

Push to `main` triggers `.github/workflows/deploy.yml`, which:

1. Writes `.env` to `/opt/qbot/.env` from GitHub secrets
2. SSH into VPS → `git pull` → `cargo build --release`
3. Auto-configures Nginx reverse proxy when `WEBHOOK_URL` is set
4. Restarts `qbot.service` via systemd
5. Hits `/health` to confirm

**GitHub secrets used by deploy workflow** (Settings → Environments → `VPS`):

| Secret | Required | Description |
|--------|----------|-------------|
| `DEPLOY_ENABLED` | Yes | Must be `true` or deploy steps are skipped |
| `VPS_HOST` | Yes | VPS IP or hostname |
| `VPS_USER` | Yes | SSH user (for example `ubuntu`) |
| `VPS_SSH_KEY` | Yes | Private SSH key content (full PEM block) |
| `TUSHARE_TOKEN` | Yes | Tushare API token |
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram bot token |
| `TELEGRAM_WEBHOOK_SECRET` | No | Secret token for Telegram webhook header verification |
| `WEBHOOK_URL` | No | Public base URL (used to auto-register webhook as `${WEBHOOK_URL}/telegram/webhook`) |
| `LETSENCRYPT_EMAIL` | No | Optional override email for Let's Encrypt. If empty, deploy uses `admin@<webhook-domain>` |
| `REPORT_CHANNEL` | Yes | Telegram channel ID for reports |
| `STOCK_ALERT_CHANNEL` | No | Telegram channel ID for burst alerts |
| `DABAN_CHANNEL` | No | Telegram channel ID for daban notifications |
| `ENABLE_DABAN_LIVE` | No | Set to `true` to run intraday daban live loop |
| `ENABLE_AI_ANALYSIS` | No | Set to `true` to run daily AI market overview push |
| `ENABLE_CHIP_DIST` | No | Set to `true` to run daily chip distribution refresh |
| `API_KEY` | No | REST API bearer token |
| `AI_API_KEY` | No | Optional key for AI narrative analysis features |
| `AI_BASE_URL` | No | Optional override for AI API base URL (default OpenAI v1) |
| `AI_MODEL` | No | Optional override for AI narrative model |
| `DATA_PROXY` | No | Optional HTTP/SOCKS proxy URL |

### VPS First-Run Setup

```bash
# On the VPS, clone repo and run setup
git clone <repo> /opt/qbot
cd /opt/qbot
./deploy/setup.sh
```

`setup.sh` installs Rust + Docker if missing, starts PostgreSQL + Redis via Docker Compose, optionally configures Nginx reverse proxy from `.env` (`WEBHOOK_URL`), builds the binary, and installs + starts the systemd service.

```bash
# Check service status
sudo systemctl status qbot
sudo journalctl -u qbot -f
```

---

## Telegram Mode

- Rust project now supports:
  - Outbound Telegram push (`sendMessage`) for reports/alerts
  - Inbound Telegram webhook commands via `POST /telegram/webhook`
- For webhook commands, set these two secrets:
  - `WEBHOOK_URL` (public HTTPS base URL)
  - `TELEGRAM_WEBHOOK_SECRET` (shared secret header token)
- If `WEBHOOK_URL` is set, qbot auto-registers Telegram webhook to:
  - `${WEBHOOK_URL}/telegram/webhook`
- Webhook subscription includes `message`, `edited_message`, and `callback_query` (button navigation).
- If `TELEGRAM_WEBHOOK_SECRET` is set, webhook requests must include:
  - `X-Telegram-Bot-Api-Secret-Token: <TELEGRAM_WEBHOOK_SECRET>`
- If `WEBHOOK_URL` is `https://...`, deploy auto-attempts Let's Encrypt issuance.
  - `LETSENCRYPT_EMAIL` is optional; default is `admin@<domain>`.
  - For Cloudflare domains, set DNS to **gray cloud (DNS only)** during issuance/renewal.
  - Deploy is strict: if HTTPS cert issuance fails, deployment fails (no HTTP fallback).

Supported command set (webhook):
- `/start`, `/help`
- `/menu` (打开按钮导航)
- `/watch`, `/unwatch`, `/mywatch`, `/export`
- `/port`, `/port add`, `/port del`
- `/scan`
- `/prestart`
- `/scan_stats`
- `/autosim`, `/autosim_report`
- `/daban`, `/daban portfolio`, `/daban stats`
- `/industry`, `/concept`, `/hot7`, `/hot14`, `/hot30`, `/sector_sync`
- `/ai_analysis`
- `/history`, `/chart`
- `/dbcheck`, `/dbsync`

`/scan` behavior:
- Runs full signal scan (all enabled signals).
- Sends summary + per-signal button lists.
- Buttons open self-hosted K-line miniapp (`/miniapp/chart`) when `WEBHOOK_URL` is set.
- Supports signal-specific scan via button menu (`/menu` → `信号扫描`).

Telegram button navigation:
- Main menu + submenus for Watchlist / Portfolio / Daban / Sector+AI / Tools.
- All commands and subcommands have corresponding buttons.

Self-hosted chart miniapp:
- Ported from `../qubot` and served by Rust service at:
  - `${WEBHOOK_URL}/miniapp/chart/?code=600519`
- Scan result buttons and `/chart` use this miniapp URL.

Command menu:
- qbot now auto-calls Telegram `setMyCommands` on startup.
- If Telegram still does not show the slash menu, restart bot and reopen chat.

Telegram debug checklist (VPS):

```bash
# 1) Live logs (webhook receive / command start / command done / secret mismatch)
sudo journalctl -u qbot -f

# 2) Service status
sudo systemctl status qbot --no-pager

# 3) Telegram-side webhook status
curl -s "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getWebhookInfo" | jq
```

Expected:
- `getWebhookInfo.url` = `${WEBHOOK_URL%/}/telegram/webhook`
- `last_error_date` is empty or `0`
- `pending_update_count` should not keep growing

Nginx reverse proxy example (VPS):

```nginx
server {
    listen 443 ssl http2;
    server_name bot.example.com;

    # ... SSL cert config ...

    client_max_body_size 2m;

    location /telegram/webhook {
        proxy_pass http://127.0.0.1:8080/telegram/webhook;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_connect_timeout 10s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Manual webhook registration (optional, if you prefer explicit setup):

```bash
curl -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/setWebhook" \
  -H "Content-Type: application/json" \
  -d "{
    \"url\": \"${WEBHOOK_URL%/}/telegram/webhook\",
    \"secret_token\": \"${TELEGRAM_WEBHOOK_SECRET}\",
    \"allowed_updates\": [\"message\", \"edited_message\"]
  }"
```

---

## REST API

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | No | Service health check |
| POST | `/telegram/webhook` | No* | Telegram inbound command webhook (`*` validated by `TELEGRAM_WEBHOOK_SECRET` if configured) |
| GET | `/api/signals` | No | List all 21 signals |
| GET | `/api/scan/latest` | Yes | Latest scan results from Redis |
| GET | `/api/scan/prestart` | Yes | Pre-start candidate pool with A-tier `3/5` resonance and B-tier `core + auxiliary` setup from `ma_bullish/volume_price/slow_bull/small_bullish/triple_bullish` |
| GET | `/api/scan/stats` | Yes | Forward-return stats by signal (`days`, optional `signal_id`, optional `limit`) |
| GET | `/api/report/daily` | Yes | Latest daily report from DB |
| GET | `/api/report/signal_auto` | Yes | Latest signal auto-trading daily report from DB |
| GET | `/api/report/limitup` | Yes | Latest standalone limit-up report from DB |
| GET | `/api/report/strong` | Yes | Latest standalone strong-stock report from DB |
| GET | `/api/signal-auto/accounts` | Yes | Per-signal strategy account snapshots |
| GET | `/api/market/overview` | Yes | Market overview with sector breadth, top stock trend, and report text |
| GET | `/api/chart/data/{code}` | Yes | OHLCV chart data (`days`, `period=daily|weekly|monthly`) |
| GET | `/api/chart/chips/{code}` | Yes | Chip distribution data (`date=YYYY-MM-DD` optional) |
| GET | `/api/chart/search` | Yes | Search stocks (`q`, optional `limit`) |
| POST | `/api/chart/watchlist/add` | Yes | Add watchlist item (`user_id`, `code`) |
| POST | `/api/chart/watchlist/remove` | Yes | Remove watchlist item (`user_id`, `code`) |
| GET | `/api/chart/watchlist/status` | Yes | Check watchlist status (`user_id`, `code`) |
| GET | `/api/chart/watchlist/list` | Yes | List watchlist (`user_id`) |
| GET | `/api/portfolio/list` | Yes | List portfolio positions (`user_id`) |
| POST | `/api/portfolio/add` | Yes | Add/update portfolio position (`user_id`, `code`, `cost_price`, `shares`) |
| POST | `/api/portfolio/remove` | Yes | Remove portfolio position (`user_id`, `code`) |
| GET | `/api/sim/balance` | Yes | Get simulator cash balance (`sim_type`, default `general`) |
| GET | `/api/sim/positions` | Yes | List open simulator positions (`sim_type`) |
| POST | `/api/sim/buy` | Yes | Open simulator position (`code`, `price`, `shares`, optional `name`) |
| POST | `/api/sim/sell` | Yes | Close simulator position (`position_id`, `price`, optional `reason`) |
| GET | `/api/sim/stats` | Yes | Simulator summary stats (`sim_type`) |
| GET | `/api/daban/report` | Yes | Daban scored report (optional `date=YYYY-MM-DD`, `limit`) |
| GET | `/api/daban/top` | Yes | Daban top ranked list (optional `date=YYYY-MM-DD`, `limit`) |
| GET | `/api/daban/sim/balance` | Yes | Daban simulator cash balance |
| GET | `/api/daban/sim/positions` | Yes | Daban simulator open positions |
| POST | `/api/daban/sim/buy` | Yes | Open daban sim position (`code`, `price`, `shares`, optional `name`, `score`) |
| POST | `/api/daban/sim/sell` | Yes | Close daban sim position (`position_id`, `price`, optional `reason`) |
| GET | `/api/daban/sim/stats` | Yes | Daban simulator summary stats |
| POST | `/api/scan/trigger` | Yes | Trigger scan (background) |
| POST | `/api/jobs/fetch` | Yes | Trigger data fetch job |
| POST | `/api/jobs/scan` | Yes | Trigger signal scan job |
| POST | `/api/jobs/report/daily` | Yes | Trigger daily report + push |
| POST | `/api/jobs/report/weekly` | Yes | Trigger weekly report + push |

---

## Database

11 migrations applied automatically on startup via SQLx:

| Table | Contents |
|-------|----------|
| `stock_daily_bars` | OHLCV + indicators per stock per day |
| `scan_results` | Signal hits per scan run |
| `limit_up_stocks` | Limit-up stocks per day |
| `sector_daily` | Sector performance per day |
| `chip_distribution` | Chip distribution snapshots |
| `user_portfolio` | Portfolio positions |
| `user_watchlist` | Watchlist stocks |
| `trading_sim_positions` / `daban_sim_positions` / `sim_capital` | Trading simulation records |
| `signal_strategy_accounts` / `signal_strategy_candidates` / `signal_strategy_positions` / `signal_strategy_events` | Auto paper-trading accounts, candidates, trades, and event logs. Includes pre-start signal accounts plus synthetic `auto_daban` and `auto_strong` accounts. Pre-start signals only buy `A`-tier setups and log `B`-tier setups as watch-only observations |
| `startup_watchlist` | One-limit-up-in-30-days startup tracking |
| `reports` | Generated daily/weekly/limitup/strong report content |

---

## Gotchas

- **Non-trading days**: `update_today()` returns empty from Tushare on weekends/holidays — jobs run cleanly but store no data.
- **First run backfill**: On first start with no data, a 3-year OHLCV backfill runs in the background. This takes time; the API is live during backfill.
- **Rust version**: Requires stable ≥ 1.85 (via `rustup`). The system `/usr/lib/rust-1.80/` is too old — `getrandom 0.4+` requires edition 2024.
- **Redis**: Assumed to be running natively. `scripts/local-test.sh` checks this before starting.
- **Telegram rate limits**: The pusher auto-retries once on HTTP 429 with a 5 s delay.
- **`API_KEY` empty = open**: If `API_KEY` is not set, all endpoints are unauthenticated. Set it in production.
