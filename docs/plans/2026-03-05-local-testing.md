# Local Testing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable full end-to-end local testing of qbot — `--run-now` CLI flag fires all 4 scheduler jobs on startup, 4 new API endpoints trigger individual jobs, and a `scripts/local-test.sh` bootstraps PostgreSQL + runs the binary.

**Architecture:** Extract 4 reusable async job functions from `src/scheduler/mod.rs`. Add `provider` and `pusher` to `AppState` so API handlers can access them. Wire `--run-now` in `main.rs` to call the job functions sequentially before starting the API server.

**Tech Stack:** Rust stable (rustup), Axum, Docker Compose (PostgreSQL only — Redis already running natively)

---

## Phase 1: Refactor Scheduler Into Reusable Job Functions

### Task 1: Extract job functions from scheduler

**Files:**
- Modify: `src/scheduler/mod.rs`

The scheduler currently has all job logic inlined inside cron closures. Extract each into a standalone `pub async fn` so they can be called from `--run-now` and from API endpoints without duplicating code.

**Step 1: Replace `src/scheduler/mod.rs` with this:**

```rust
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, warn};

use crate::data::tushare::TushareClient;
use crate::services::{
    limit_up::LimitUpService,
    market::MarketService,
    market_report::MarketReportService,
    scanner::ScannerService,
    sector::SectorService,
    stock_history::StockHistoryService,
};
use crate::state::AppState;
use crate::telegram::pusher::TelegramPusher;

/// Fetch today's OHLCV, limit-up stocks, and sector data (15:05 job).
pub async fn run_fetch_job(state: Arc<AppState>, provider: Arc<TushareClient>) {
    let today = chrono::Local::now().naive_local().date();
    info!("Fetch job: OHLCV + limit-up + sector for {}", today);

    let history_svc = StockHistoryService::new(state.clone(), provider.clone());
    if let Err(e) = history_svc.update_today().await {
        warn!("Daily data fetch failed: {}", e);
    }

    let limit_svc = LimitUpService::new(state.clone(), provider.clone());
    match limit_svc.fetch_and_save(today).await {
        Ok(stocks) => info!("Limit-up: {} stocks", stocks.len()),
        Err(e) => warn!("Limit-up fetch failed: {}", e),
    }

    let sector_svc = SectorService::new(state.clone(), provider.clone());
    if let Err(e) = sector_svc.fetch_and_save(today).await {
        warn!("Sector data failed: {}", e);
    }
}

/// Run all 21 signal detectors and cache results to Redis (15:35 job).
pub async fn run_scan_job(state: Arc<AppState>) {
    info!("Scan job: running full signal scan");
    let scanner = ScannerService::new(state);
    if let Err(e) = scanner.run_full_scan().await {
        warn!("Scan failed: {}", e);
    }
}

/// Generate daily market report and push to Telegram (16:00 job).
pub async fn run_daily_report_job(
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
    pusher: Arc<TelegramPusher>,
) {
    let today = chrono::Local::now().naive_local().date();
    info!("Daily report job for {}", today);

    let market_svc = Arc::new(MarketService::new(state.clone(), provider.clone()));
    let limit_svc = Arc::new(LimitUpService::new(state.clone(), provider.clone()));
    let sector_svc = Arc::new(SectorService::new(state.clone(), provider.clone()));
    let scanner_svc = Arc::new(ScannerService::new(state.clone()));
    let report_svc = MarketReportService::new(
        state.clone(), market_svc, limit_svc, sector_svc, scanner_svc,
    );

    match report_svc.generate_daily(today).await {
        Ok(report) => {
            if let Some(channel) = &state.config.report_channel {
                if let Err(e) = pusher.push(channel, &report).await {
                    warn!("Telegram push failed: {}", e);
                }
            }
        }
        Err(e) => warn!("Daily report failed: {}", e),
    }
}

/// Generate weekly market report and push to Telegram (Friday 20:00 job).
pub async fn run_weekly_report_job(
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
    pusher: Arc<TelegramPusher>,
) {
    info!("Weekly report job");

    let market_svc = Arc::new(MarketService::new(state.clone(), provider.clone()));
    let limit_svc = Arc::new(LimitUpService::new(state.clone(), provider.clone()));
    let sector_svc = Arc::new(SectorService::new(state.clone(), provider.clone()));
    let scanner_svc = Arc::new(ScannerService::new(state.clone()));
    let report_svc = MarketReportService::new(
        state.clone(), market_svc, limit_svc, sector_svc, scanner_svc,
    );

    match report_svc.generate_weekly().await {
        Ok(report) => {
            if let Some(channel) = &state.config.report_channel {
                if let Err(e) = pusher.push(channel, &report).await {
                    warn!("Telegram push failed: {}", e);
                }
            }
        }
        Err(e) => warn!("Weekly report failed: {}", e),
    }
}

pub async fn start_scheduler(
    state: Arc<AppState>,
    provider: Arc<TushareClient>,
    pusher: Arc<TelegramPusher>,
) -> anyhow::Result<JobScheduler> {
    let sched = JobScheduler::new().await?;

    // 15:05 weekdays
    {
        let s = state.clone();
        let p = provider.clone();
        sched.add(Job::new_async("0 5 15 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone(); let p = p.clone();
            Box::pin(async move { run_fetch_job(s, p).await })
        })?).await?;
    }

    // 15:35 weekdays
    {
        let s = state.clone();
        sched.add(Job::new_async("0 35 15 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone();
            Box::pin(async move { run_scan_job(s).await })
        })?).await?;
    }

    // 16:00 weekdays
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched.add(Job::new_async("0 0 16 * * Mon,Tue,Wed,Thu,Fri", move |_, _| {
            let s = s.clone(); let p = p.clone(); let push = push.clone();
            Box::pin(async move { run_daily_report_job(s, p, push).await })
        })?).await?;
    }

    // 20:00 Friday
    {
        let s = state.clone();
        let p = provider.clone();
        let push = pusher.clone();
        sched.add(Job::new_async("0 0 20 * * Fri", move |_, _| {
            let s = s.clone(); let p = p.clone(); let push = push.clone();
            Box::pin(async move { run_weekly_report_job(s, p, push).await })
        })?).await?;
    }

    sched.start().await?;
    info!("Scheduler started with 4 jobs");
    Ok(sched)
}
```

**Step 2: Compile check**

```bash
cargo check 2>&1
```

Expected: `Finished` with only warnings, no errors.

**Step 3: Commit**

```bash
git add src/scheduler/mod.rs
git commit -m "refactor: extract scheduler job functions for reuse"
```

---

## Phase 2: AppState — Add Provider and Pusher

### Task 2: Add provider and pusher to AppState

**Files:**
- Modify: `src/state.rs`
- Modify: `src/main.rs`

API job endpoints need `provider` and `pusher`. The cleanest way is to put them in `AppState` so Axum handlers receive them via `State<Arc<AppState>>`.

**Step 1: Replace `src/state.rs` with:**

```rust
use std::sync::Arc;
use crate::config::Config;
use crate::data::tushare::TushareClient;
use crate::telegram::pusher::TelegramPusher;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: sqlx::PgPool,
    pub redis: redis::aio::ConnectionManager,
    pub provider: Arc<TushareClient>,
    pub pusher: Arc<TelegramPusher>,
}
```

**Step 2: Update AppState construction in `src/main.rs`**

Find this block (lines 44–48):
```rust
    let state = Arc::new(state::AppState {
        config: Arc::new(config.clone()),
        db,
        redis,
    });
```

Replace with:
```rust
    let state = Arc::new(state::AppState {
        config: Arc::new(config.clone()),
        db,
        redis,
        provider: provider.clone(),
        pusher: pusher.clone(),
    });
```

Note: `provider` and `pusher` are already created before this block in `main.rs` — just move the `state` construction to after them. The current order in `main.rs` is:
1. config, db, redis
2. `let state = Arc::new(...)` ← move this DOWN
3. signal registry
4. `let provider = ...`
5. `let pusher = ...`

Reorder so `state` is built after `provider` and `pusher`:

```rust
    // Initialize signal registry
    signals::registry::SignalRegistry::init();

    // Data provider and Telegram pusher
    let provider = Arc::new(data::tushare::TushareClient::new(
        config.tushare_token.clone(),
        config.data_proxy.as_deref(),
    ));
    let pusher = Arc::new(telegram::TelegramPusher::new(config.telegram_bot_token.clone()));

    let state = Arc::new(state::AppState {
        config: Arc::new(config.clone()),
        db,
        redis,
        provider: provider.clone(),
        pusher: pusher.clone(),
    });
```

Remove the old `signals::registry::SignalRegistry::init();` and old `let state = ...` lines.

**Step 3: Compile check**

```bash
cargo check 2>&1
```

Expected: `Finished` with only warnings, no errors.

**Step 4: Commit**

```bash
git add src/state.rs src/main.rs
git commit -m "feat: add provider and pusher to AppState"
```

---

## Phase 3: `--run-now` Flag

### Task 3: Add `--run-now` to main.rs

**Files:**
- Modify: `src/main.rs`

**Step 1: Add `--run-now` detection and sequential job execution**

After the AppState construction block (and after the backfill check), add:

```rust
    // --run-now: fire all 4 jobs sequentially for local testing
    if std::env::args().any(|a| a == "--run-now") {
        info!("--run-now: firing all jobs sequentially");
        scheduler::run_fetch_job(state.clone(), provider.clone()).await;
        scheduler::run_scan_job(state.clone()).await;
        scheduler::run_daily_report_job(state.clone(), provider.clone(), pusher.clone()).await;
        scheduler::run_weekly_report_job(state.clone(), provider.clone(), pusher.clone()).await;
        info!("--run-now: all jobs complete, starting API server");
    }
```

Place this block AFTER the backfill check and BEFORE `scheduler::start_scheduler(...)`.

**Step 2: Compile check**

```bash
cargo check 2>&1
```

Expected: `Finished` with only warnings, no errors.

**Step 3: Commit**

```bash
git add src/main.rs
git commit -m "feat: add --run-now flag to fire all scheduler jobs on startup"
```

---

## Phase 4: Job Trigger API Endpoints

### Task 4: Add 4 job trigger endpoints to routes.rs

**Files:**
- Modify: `src/api/routes.rs`

**Step 1: Add routes to `build_router`**

Find:
```rust
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/signals", get(list_signals))
        .route("/api/scan/latest", get(get_scan_latest))
        .route("/api/scan/trigger", post(trigger_scan))
        .route("/api/report/daily", get(get_daily_report))
        .route("/api/market/overview", get(market_overview_stub))
        .with_state(state)
}
```

Replace with:
```rust
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/signals", get(list_signals))
        .route("/api/scan/latest", get(get_scan_latest))
        .route("/api/scan/trigger", post(trigger_scan))
        .route("/api/report/daily", get(get_daily_report))
        .route("/api/market/overview", get(market_overview_stub))
        .route("/api/jobs/fetch", post(trigger_fetch))
        .route("/api/jobs/scan", post(trigger_scan_job))
        .route("/api/jobs/report/daily", post(trigger_daily_report))
        .route("/api/jobs/report/weekly", post(trigger_weekly_report))
        .with_state(state)
}
```

**Step 2: Add the 4 handler functions** (append to end of `src/api/routes.rs`):

```rust
async fn trigger_fetch(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }
    let s = state.clone();
    let p = state.provider.clone();
    tokio::spawn(async move {
        crate::scheduler::run_fetch_job(s, p).await;
    });
    Ok(Json(json!({"status": "started", "job": "fetch"})))
}

async fn trigger_scan_job(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }
    let s = state.clone();
    tokio::spawn(async move {
        crate::scheduler::run_scan_job(s).await;
    });
    Ok(Json(json!({"status": "started", "job": "scan"})))
}

async fn trigger_daily_report(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }
    let s = state.clone();
    let p = state.provider.clone();
    let push = state.pusher.clone();
    tokio::spawn(async move {
        crate::scheduler::run_daily_report_job(s, p, push).await;
    });
    Ok(Json(json!({"status": "started", "job": "report/daily"})))
}

async fn trigger_weekly_report(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> ApiResult {
    if !check_auth(&headers, state.config.api_key.as_deref()) {
        return Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))));
    }
    let s = state.clone();
    let p = state.provider.clone();
    let push = state.pusher.clone();
    tokio::spawn(async move {
        crate::scheduler::run_weekly_report_job(s, p, push).await;
    });
    Ok(Json(json!({"status": "started", "job": "report/weekly"})))
}
```

**Step 3: Compile check**

```bash
cargo check 2>&1
```

Expected: `Finished` with only warnings, no errors.

**Step 4: Run all unit tests**

```bash
cargo test --lib 2>&1
```

Expected: `7 passed; 0 failed`

**Step 5: Commit**

```bash
git add src/api/routes.rs
git commit -m "feat: add job trigger API endpoints (/api/jobs/fetch|scan|report/daily|weekly)"
```

---

## Phase 5: Bootstrap Script

### Task 5: Create scripts/local-test.sh

**Files:**
- Create: `scripts/local-test.sh`

**Step 1: Create the script**

```bash
mkdir -p scripts
```

Write `scripts/local-test.sh`:

```bash
#!/bin/bash
# scripts/local-test.sh -- Full local end-to-end test
# Redis is assumed to be already running (redis-cli ping should return PONG).
# PostgreSQL is started via Docker Compose.
set -e

cd "$(dirname "$0")/.."

# 1. Check Redis
if ! redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "ERROR: Redis is not running. Start it first."
    exit 1
fi

# 2. Start PostgreSQL via Docker Compose
echo "Starting PostgreSQL..."
docker compose -f deploy/docker-compose.yml up -d postgres

echo "Waiting for PostgreSQL to be ready..."
until docker compose -f deploy/docker-compose.yml exec -T postgres \
    pg_isready -U qbot -d qbot -q 2>/dev/null; do
    sleep 1
done
echo "PostgreSQL ready."

# 3. Check for .env
if [ ! -f .env ]; then
    cp .env.example .env
    echo ""
    echo "STOP: .env created from .env.example"
    echo "Fill in at minimum:"
    echo "  TUSHARE_TOKEN=<your token>"
    echo "  TELEGRAM_BOT_TOKEN=<your bot token>"
    echo "  REPORT_CHANNEL=<channel id>"
    echo ""
    echo "Then re-run: ./scripts/local-test.sh"
    exit 1
fi

# 4. Run with --run-now (jobs fire, then API stays alive)
echo ""
echo "Starting qbot with --run-now..."
echo "API will be live at http://localhost:8080 after jobs complete."
echo "Press Ctrl+C to stop."
echo ""
cargo run -- --run-now
```

**Step 2: Make executable**

```bash
chmod +x scripts/local-test.sh
```

**Step 3: Commit**

```bash
git add scripts/local-test.sh
git commit -m "feat: add scripts/local-test.sh for full local end-to-end testing"
```

---

## Phase 6: Final Verification

### Task 6: Full compile and test

**Step 1: Clean build**

```bash
cargo build 2>&1
```

Expected: `Finished dev [unoptimized + debuginfo]`

**Step 2: All unit tests**

```bash
cargo test --lib 2>&1
```

Expected: `7 passed; 0 failed`

**Step 3: Verify script is runnable**

```bash
bash -n scripts/local-test.sh && echo "syntax ok"
```

Expected: `syntax ok`

**Step 4: Commit if any fixes needed, then done**

The binary is now ready to run end-to-end locally with:
```bash
./scripts/local-test.sh
```

And individual jobs can be triggered via:
```bash
curl -s -X POST http://localhost:8080/api/jobs/fetch
curl -s -X POST http://localhost:8080/api/jobs/scan
curl -s -X POST http://localhost:8080/api/jobs/report/daily
curl -s -X POST http://localhost:8080/api/jobs/report/weekly
curl -s http://localhost:8080/api/report/daily
```
