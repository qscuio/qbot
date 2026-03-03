# qbot Design Document

**Date:** 2026-03-03
**Status:** Approved
**Language:** Rust
**Market:** A-shares (China), extensible to US/HK

---

## 1. Overview

qbot is a Rust-based A-share stock analysis and reporting system that:

- Scans all ~5000 A-share stocks daily using a modular signal detection engine
- Tracks limit-up boards, sector performance, and market overview
- Runs paper trading simulators (general + 打板 strategy)
- Monitors real-time intraday abnormal movements
- Pushes daily/weekly/monthly reports to a Telegram channel
- Deploys to a VPS via GitHub Actions + systemd

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        qbot daemon                           │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐   │
│  │  Scheduler  │  │  REST API    │  │  Telegram Pusher  │   │
│  │(tokio-cron) │  │   (axum)     │  │  (channel push)   │   │
│  └──────┬──────┘  └──────────────┘  └─────────┬─────────┘   │
│         │                                      │             │
│  ┌──────▼──────────────────────────────────────▼──────────┐  │
│  │                    Services Layer                       │  │
│  │  stock_history │ scanner │ limit_up │ daban            │  │
│  │  sector │ market │ burst_monitor │ portfolio           │  │
│  │  watchlist │ trading_sim │ chip_dist │ trend_analyzer  │  │
│  │  market_report │ ai_analysis                           │  │
│  └──────────────────────────┬──────────────────────────────┘ │
│                             │                                │
│  ┌──────────────────────────▼──────────────────────────────┐ │
│  │                    Data Layer                           │  │
│  │  tushare.rs (primary)  │  sina.rs (real-time only)     │  │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
              │                    │
        PostgreSQL               Redis
      (history, results)    (intraday cache)
```

### Tech Stack

| Component | Crate |
|-----------|-------|
| Async runtime | `tokio` |
| HTTP server | `axum` |
| HTTP client | `reqwest` |
| Database | `sqlx` → PostgreSQL |
| Cache | `redis` |
| Scheduler | `tokio-cron-scheduler` |
| Serialization | `serde` / `serde_json` |
| Logging | `tracing` + `tracing-subscriber` |
| Config | `dotenvy` + custom Config struct |
| Date/time | `chrono` |
| Error handling | `anyhow` + `thiserror` |
| Numerics | `ta` (technical analysis crate) |

---

## 3. Complete Feature List

| # | Service | Feature |
|---|---------|---------|
| 1 | `stock_history` | OHLCV backfill + daily incremental updates |
| 2 | `scanner` | Full signal scan, ~5000 stocks, batched |
| 3 | `signals/*` | 17 signals across 6 groups (see §5) |
| 4 | `limit_up` | 涨停 tracking: 连板, 首板, 炸板, 封单 |
| 5 | `daban` | 打板 post-close scoring: seal, timing, board type, turnover, cap |
| 6 | `daban_sim` | 打板 paper trading: 100K, max 2 positions |
| 7 | `sector` | Industry + Concept sector performance |
| 8 | `market` | Index overview: 上证/深证/创业板/科创50, breadth, flows |
| 9 | `trend_analyzer` | TrendStatus (7), VolumeStatus (5), BuySignal scoring (6) |
| 10 | `chip_dist` | 筹码分布: avg cost, profit ratio, concentration |
| 11 | `burst_monitor` | Real-time: 快速拉升 >3%, 放量异动 5x, 涨停异动 |
| 12 | `portfolio` | User positions, P&L, stop-loss/take-profit alerts |
| 13 | `watchlist` | User watchlists, daily 17:00 performance report |
| 14 | `trading_sim` | Paper trade: 1M RMB, max 5 pos, trailing stop, T+0 |
| 15 | `market_report` | Daily/weekly/monthly market report |
| 16 | `ai_analysis` | LLM-powered market insight (pluggable provider) |

---

## 4. Module Structure

```
qbot/
├── Cargo.toml
├── .env.example
├── src/
│   ├── main.rs                    # Boot: DB → services → scheduler → axum
│   ├── config.rs                  # All env vars (token, DB, Telegram, etc.)
│   ├── error.rs                   # AppError + Result alias
│   │
│   ├── data/                      # Data providers
│   │   ├── provider.rs            # DataProvider trait
│   │   ├── tushare.rs             # PRIMARY: all daily/historical data
│   │   ├── sina.rs                # SUPPLEMENT: real-time quotes (burst monitor)
│   │   └── types.rs               # StockInfo, Candle, Quote, SectorData, LimitUpStock
│   │
│   ├── signals/                   # Modular signal system
│   │   ├── base.rs                # SignalDetector trait, SignalResult
│   │   ├── registry.rs            # register!, run_all(), get_enabled()
│   │   ├── volume/
│   │   │   ├── surge.rs           # 放量突破
│   │   │   └── price.rs           # 量价配合
│   │   ├── trend/
│   │   │   ├── ma_bullish.rs      # 均线多头
│   │   │   ├── ma_pullback.rs     # 均线回踩
│   │   │   ├── strong_pullback.rs # 强势回调
│   │   │   ├── uptrend_break.rs   # 上升突破
│   │   │   ├── downtrend_rev.rs   # 下降反转
│   │   │   └── linreg.rs          # 线性回归
│   │   ├── pattern/
│   │   │   ├── slow_bull.rs       # 缓慢牛
│   │   │   ├── small_bullish.rs   # 小阳线
│   │   │   ├── triple_bullish.rs  # 三阳开泰
│   │   │   ├── fanbao.rs          # 反包
│   │   │   └── weekly_monthly.rs  # 周月多头
│   │   ├── board/
│   │   │   ├── broken_board.rs    # 炸板反包
│   │   │   └── strong_first_neg.rs# 强势首阴
│   │   ├── momentum/
│   │   │   ├── breakout.rs        # 突破信号
│   │   │   ├── startup.rs         # 启动信号
│   │   │   └── kuangbiao.rs       # 狂飙信号
│   │   └── comprehensive/
│   │       ├── bottom_quick.rs    # 底部快速启动
│   │       ├── long_cycle_rev.rs  # 长周期反转
│   │       └── low_accum.rs       # 低位积累启动
│   │
│   ├── services/
│   │   ├── stock_history.rs       # OHLCV backfill (date-by-date) + daily update
│   │   ├── scanner.rs             # Orchestrate batch scan, stream results
│   │   ├── limit_up.rs            # Daily 涨停 tracker
│   │   ├── daban.rs               # 打板 post-close reviewer + scorer
│   │   ├── daban_sim.rs           # 打板 simulator
│   │   ├── sector.rs              # Sector performance tracking
│   │   ├── market.rs              # Index overview + market breadth
│   │   ├── trend_analyzer.rs      # TrendStatus/VolumeStatus/BuySignal
│   │   ├── chip_dist.rs           # 筹码分布 calculation
│   │   ├── burst_monitor.rs       # Real-time intraday alerts (every 30s)
│   │   ├── portfolio.rs           # User positions + P&L alerts
│   │   ├── watchlist.rs           # User watchlists
│   │   ├── trading_sim.rs         # General paper trading simulator
│   │   ├── market_report.rs       # Daily/weekly/monthly report generator
│   │   └── ai_analysis.rs         # LLM market analysis
│   │
│   ├── telegram/
│   │   ├── pusher.rs              # Push to channel (rate-limited)
│   │   └── formatter.rs           # Telegram Markdown/HTML helpers
│   │
│   ├── storage/
│   │   ├── postgres.rs            # sqlx pool, migrations runner
│   │   └── redis_cache.rs         # Intraday cache, TTL helpers
│   │
│   ├── api/
│   │   └── routes.rs              # /health, /scan/trigger, /report/latest, /signals
│   │
│   └── scheduler/
│       └── mod.rs                 # All tokio-cron jobs
│
├── migrations/
│   ├── 001_stock_daily_bars.sql
│   ├── 002_scan_results.sql
│   ├── 003_limit_up.sql
│   ├── 004_sector_daily.sql
│   ├── 005_chip_distribution.sql
│   ├── 006_portfolio.sql
│   ├── 007_watchlist.sql
│   ├── 008_trading_sim.sql
│   └── 009_reports.sql
│
├── deploy/
│   ├── qbot.service               # systemd unit file
│   ├── docker-compose.yml         # PostgreSQL + Redis
│   └── setup.sh                   # First-run VPS bootstrap
│
└── .github/
    └── workflows/
        ├── deploy.yml             # push to main → SSH → build → restart
        └── test.yml               # cargo test + cargo clippy
```

---

## 5. Signal Library

### Trait Definition

```rust
pub trait SignalDetector: Send + Sync {
    fn signal_id(&self) -> &'static str;
    fn display_name(&self) -> &'static str;
    fn icon(&self) -> &'static str;
    fn group(&self) -> &'static str;
    fn min_bars(&self) -> usize { 21 }
    fn priority(&self) -> i32 { 100 }
    fn enabled(&self) -> bool { true }
    fn detect(&self, bars: &[Candle], info: &StockInfo) -> SignalResult;
}

pub struct SignalResult {
    pub triggered: bool,
    pub metadata: HashMap<String, serde_json::Value>,
}
```

### Signal Registry

Signals are registered at startup via a macro or inventory pattern:
```rust
register_signal!(VolumeSurgeSignal);
register_signal!(MaBullishSignal);
// ... all 17 signals
```

### Signal Groups

| Group | Signals |
|-------|---------|
| volume | volume_surge, volume_price |
| trend | ma_bullish, ma_pullback, strong_pullback, uptrend_breakout, downtrend_reversal, linreg |
| pattern | slow_bull, small_bullish, triple_bullish, fanbao, weekly_monthly |
| board | broken_board, strong_first_neg |
| momentum | breakout, startup, kuangbiao |
| comprehensive | bottom_quick_start, long_cycle_reversal, low_accumulation |

---

## 6. Data Layer

### Tushare (Primary)

Base URL: `https://api.tushare.pro`
Method: `POST application/json`
Auth: `{ "token": "...", "api_name": "...", "params": {...}, "fields": "..." }`

| API | Usage |
|-----|-------|
| `stock_basic` | Stock universe (code, name, market, industry) |
| `daily` | OHLCV — date-by-date for backfill, code-by-code for updates |
| `daily_basic` | PE, PB, turnover_rate, total_mv, float_share |
| `limit_list_d` | Limit-up/down: seal amount, burst count, streak, limit time |
| `index_daily` | Index bars (sh000001, sz399001, sz399006, sh000688) |
| `trade_cal` | Trading calendar — is_open flag |
| `ths_index` | Sector/concept index list |
| `ths_daily` | Sector daily performance |
| `moneyflow_hsgt` | Northbound/Southbound capital flows |

### Backfill Strategy

- **Backfill**: `daily(trade_date=YYYYMMDD)` — one call = all stocks for that date
  - Walk backwards through trading calendar from today to 10 years ago
  - Configurable rate: default 200ms between calls
- **Daily update**: `daily(ts_code=X, start_date=yesterday)` — fetch changed stocks only

### Sina Finance (Real-time Supplement)

Used only by `burst_monitor` for intraday polling (every 30s, trading hours only):
```
http://hq.sinajs.cn/list=sh000001,sz000002,...
```
Free, no auth, handles batch quote fetches.

---

## 7. PostgreSQL Schema

```sql
-- OHLCV history
CREATE TABLE stock_daily_bars (
    code        VARCHAR(10) NOT NULL,
    trade_date  DATE NOT NULL,
    open        NUMERIC(10,2),
    high        NUMERIC(10,2),
    low         NUMERIC(10,2),
    close       NUMERIC(10,2),
    volume      BIGINT,
    amount      NUMERIC(18,2),
    turnover    NUMERIC(8,4),     -- from daily_basic
    pe          NUMERIC(10,2),    -- from daily_basic
    pb          NUMERIC(8,4),     -- from daily_basic
    PRIMARY KEY (code, trade_date)
);

-- Signal scan results
CREATE TABLE scan_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL,
    code        VARCHAR(10) NOT NULL,
    name        VARCHAR(50),
    signal_id   VARCHAR(50) NOT NULL,
    metadata    JSONB,
    scanned_at  TIMESTAMPTZ NOT NULL
);

-- Limit-up tracking
CREATE TABLE limit_up_stocks (
    code        VARCHAR(10) NOT NULL,
    trade_date  DATE NOT NULL,
    name        VARCHAR(50),
    streak      INT,              -- 连板数
    limit_time  TIME,             -- 涨停时间
    seal_amount NUMERIC(18,2),    -- 封单额
    burst_count INT,              -- 炸板次数
    score       NUMERIC(5,2),     -- 打板评分
    board_type  VARCHAR(20),      -- 首板/连板/一字板
    PRIMARY KEY (code, trade_date)
);

-- Sector performance
CREATE TABLE sector_daily (
    code        VARCHAR(20) NOT NULL,
    name        VARCHAR(100),
    sector_type VARCHAR(20),      -- industry / concept
    change_pct  NUMERIC(8,4),
    amount      NUMERIC(18,2),
    trade_date  DATE NOT NULL,
    PRIMARY KEY (code, trade_date)
);

-- Chip distribution
CREATE TABLE chip_distribution (
    code         VARCHAR(10) NOT NULL,
    trade_date   DATE NOT NULL,
    distribution JSONB NOT NULL,
    avg_cost     NUMERIC(10,2),
    profit_ratio NUMERIC(5,2),
    concentration NUMERIC(5,2),
    PRIMARY KEY (code, trade_date)
);

-- User portfolio
CREATE TABLE user_portfolio (
    user_id    BIGINT NOT NULL,
    code       VARCHAR(10) NOT NULL,
    cost_price NUMERIC(10,2) NOT NULL,
    shares     INT NOT NULL,
    added_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, code)
);

-- User watchlist
CREATE TABLE user_watchlist (
    user_id  BIGINT NOT NULL,
    code     VARCHAR(10) NOT NULL,
    added_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, code)
);

-- General paper trading
CREATE TABLE trading_sim_positions (
    id           BIGSERIAL PRIMARY KEY,
    code         VARCHAR(10) NOT NULL,
    name         VARCHAR(50),
    entry_price  NUMERIC(10,2),
    shares       INT,
    peak_price   NUMERIC(10,2),
    entry_date   DATE,
    exit_price   NUMERIC(10,2),
    exit_date    DATE,
    pnl_pct      NUMERIC(8,4)
);

-- 打板 simulator
CREATE TABLE daban_sim_positions (
    id          BIGSERIAL PRIMARY KEY,
    code        VARCHAR(10) NOT NULL,
    name        VARCHAR(50),
    entry_price NUMERIC(10,2),
    shares      INT,
    score       NUMERIC(5,2),
    entry_date  DATE,
    exit_price  NUMERIC(10,2),
    exit_date   DATE,
    exit_reason VARCHAR(50)
);

-- Reports archive
CREATE TABLE reports (
    id            BIGSERIAL PRIMARY KEY,
    report_type   VARCHAR(50) NOT NULL,  -- daily/weekly/monthly/daban
    content       TEXT NOT NULL,
    generated_at  TIMESTAMPTZ DEFAULT NOW()
);
```

---

## 8. Redis Usage

| Key Pattern | Content | TTL |
|-------------|---------|-----|
| `scan:latest` | Serialized scan results | Until next trading day |
| `stocks:universe` | Full stock list | 24h |
| `quotes:realtime:{code}` | Latest quote | 60s |
| `burst:alerted:{code}` | Burst alert cooldown flag | 5min |
| `chip:{code}:{date}` | Chip distribution result | 7 days |
| `report:daily:{date}` | Latest daily report | 24h |
| `tushare:ratelimit` | API call counter | 1min |

---

## 9. Scheduler

All times in CST (UTC+8).

| Time | Job | Description |
|------|-----|-------------|
| 09:20 weekdays | pre_market | Fetch pre-open data snapshot |
| 09:30–15:00 weekdays | burst_monitor | Poll real-time quotes every 30s |
| 15:05 weekdays | fetch_daily | Fetch today's OHLCV + limit-up final data via Tushare |
| 15:35 weekdays | full_scan | Run all signals across ~5000 stocks |
| 16:00 weekdays | daily_report | Generate + push daily report to Telegram |
| 17:00 weekdays | watchlist_report | Push watchlist daily performance |
| 20:00 Friday | weekly_report | Weekly strongest/weakest + sector trends |
| 20:00 last trading day | monthly_report | Monthly market report |

---

## 10. Telegram Delivery

Mode: **Channel push only** — no interactive commands needed initially.

Output channels (configurable via env):
- `STOCK_ALERT_CHANNEL` — scanner signal hits, burst alerts, limit-up alerts
- `REPORT_CHANNEL` — daily/weekly/monthly reports
- `DABAN_CHANNEL` — 打板 scoring results and simulator trades

Message format: Telegram HTML (bold, links, pre-formatted code blocks).

---

## 11. REST API

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness check |
| `GET /api/scan/latest` | Latest scan results |
| `POST /api/scan/trigger` | Manually trigger a scan |
| `GET /api/report/daily` | Latest daily report |
| `GET /api/signals` | List all registered signals |
| `GET /api/stocks/universe` | Full stock list |
| `GET /api/market/overview` | Latest market overview |

All API routes protected by `Authorization: Bearer <API_KEY>`.

---

## 12. Deployment

### GitHub Actions

```yaml
# .github/workflows/deploy.yml
on:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - checkout
      - cargo test
      - cargo clippy
      - SSH to VPS:
          - git pull
          - cargo build --release
          - systemctl restart qbot
```

### systemd

```ini
# deploy/qbot.service
[Unit]
Description=qbot A-Share Analysis Service
After=network.target postgresql.service redis.service

[Service]
ExecStart=/opt/qbot/qbot
Restart=always
EnvironmentFile=/opt/qbot/.env
WorkingDirectory=/opt/qbot

[Install]
WantedBy=multi-user.target
```

### VPS Stack

- qbot binary (Rust, compiled on GHA or on-VPS)
- PostgreSQL 16 + Redis 7 via `docker-compose.yml`
- Nginx (optional reverse proxy for REST API)

---

## 13. Extensibility

### Adding a new signal

```rust
// src/signals/volume/my_signal.rs
pub struct MySignal;

impl SignalDetector for MySignal {
    fn signal_id(&self) -> &'static str { "my_signal" }
    fn detect(&self, bars: &[Candle], info: &StockInfo) -> SignalResult {
        SignalResult { triggered: bars.last().map(|b| b.close > b.open).unwrap_or(false), ..Default::default() }
    }
    // ...
}

// src/signals/registry.rs — add one line:
register_signal!(MySignal);
```

### Adding a new market (future)

Implement `DataProvider` trait for the new source (e.g., `YFinanceProvider` for US stocks). The scanner, signals, and report system are market-agnostic — only the data layer changes.

---

## 14. Environment Variables

```env
# Tushare
TUSHARE_TOKEN=your_token_here

# Telegram
TELEGRAM_BOT_TOKEN=xxx
STOCK_ALERT_CHANNEL=-100xxx
REPORT_CHANNEL=-100xxx
DABAN_CHANNEL=-100xxx

# Database
DATABASE_URL=postgresql://qbot:qbot@localhost/qbot
REDIS_URL=redis://localhost:6379

# API security
API_KEY=your_api_key

# Optional: LLM for AI analysis
AI_PROVIDER=claude          # claude | openai | gemini
AI_API_KEY=xxx

# Optional: data proxy
DATA_PROXY=http://proxy:port
```
