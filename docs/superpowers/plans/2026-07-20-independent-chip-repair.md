# Independent Chip Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce canonical chip snapshots independently of long-running financial and dividend backfills.

**Architecture:** Add explicit company-data and chip repair modes while retaining the combined compatibility mode. GitHub Actions replaces the legacy combined transient worker with a company-data worker and starts a separately named chip worker, each using existing checkpoint, lease, and validation services.

**Tech Stack:** Rust/Tokio, PostgreSQL/sqlx, systemd transient units, GitHub Actions, Node deployment-contract tests.

## Global Constraints

- Preserve estimator provenance and never mark estimates official or validated without a persisted successful benchmark decision.
- Reuse existing resumable checkpoints and database leases.
- Do not run chip estimation inside a web request.
- Do not modify the Telegram mini app or trading behavior.
- Production mutations happen only through GitHub Actions.

---

### Task 1: Dedicated Repair Modes

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Consumes: `CompanyIntelligenceService::{backfill_financials, backfill_dividends, run_chip_benchmark, backfill_chips}`.
- Produces: `RepairMode::CompanyData`, `RepairMode::Chips`, `run_company_data_repair`, and `run_chip_repair` while preserving `RepairMode::CompanyIntelligence`.

- [ ] **Step 1: Write failing argument and phase-isolation tests**

```rust
#[test]
fn repair_modes_are_explicit_and_mutually_exclusive() {
    let strings = |args: &[&str]| args.iter().map(|value| (*value).to_string()).collect::<Vec<_>>();
    assert_eq!(repair_mode(strings(&["qbot", "--repair-company-data"])).unwrap(), RepairMode::CompanyData);
    assert_eq!(repair_mode(strings(&["qbot", "--repair-chips"])).unwrap(), RepairMode::Chips);
    assert!(repair_mode(strings(&["qbot", "--repair-company-data", "--repair-chips"])).is_err());
}

#[tokio::test]
async fn chip_repair_runs_benchmark_then_backfill_only() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    run_chip_repair(
        {
            let calls = calls.clone();
            move || async move {
                calls.lock().unwrap().push("chip_benchmark");
                Ok(ChipBenchmarkReport { reused: false, decision: ChipSourceDecision::Estimate })
            }
        },
        {
            let calls = calls.clone();
            move || async move {
                calls.lock().unwrap().push("chip_backfill");
                Ok(ChipBackfillReport { completed: 2, failed: 0, pending: 0, snapshots: 500 })
            }
        },
    ).await.unwrap();
    assert_eq!(*calls.lock().unwrap(), ["chip_benchmark", "chip_backfill"]);
}
```

Add incomplete-report tests proving pending or failed backfill returns an error.

- [ ] **Step 2: Run focused Rust tests and verify failure**

Run: `cargo test --locked repair_mode -- --nocapture && cargo test --locked chip_repair -- --nocapture`

Expected: FAIL because the new modes and runner do not exist.

- [ ] **Step 3: Implement the new modes and runners**

```rust
enum RepairMode { None, DailyBars, CompanyData, Chips, CompanyIntelligence }

async fn run_chip_repair<Benchmark, BenchmarkFuture, Backfill, BackfillFuture>(
    benchmark: Benchmark,
    backfill: Backfill,
) -> Result<()>
where
    Benchmark: FnOnce() -> BenchmarkFuture,
    BenchmarkFuture: Future<Output = crate::error::Result<ChipBenchmarkReport>>,
    Backfill: FnOnce() -> BackfillFuture,
    BackfillFuture: Future<Output = crate::error::Result<ChipBackfillReport>>,
{
    let benchmark = benchmark().await?;
    info!("Chip benchmark finished: reused={}, decision={}", benchmark.reused, benchmark.decision.as_str());
    let report = backfill().await?;
    if report.failed > 0 || report.pending > 0 {
        anyhow::bail!("chip backfill incomplete: completed={}, failed={}, pending={}, snapshots={}", report.completed, report.failed, report.pending, report.snapshots);
    }
    Ok(())
}
```

Dispatch `CompanyData` to financials/dividends, `Chips` to benchmark/backfill, and retain the existing combined path for compatibility.

- [ ] **Step 4: Run focused and complete Rust tests**

Run: `cargo test --locked repair_ -- --nocapture && cargo test --locked`

Expected: all Rust tests PASS.

- [ ] **Step 5: Commit repair-mode isolation**

```bash
git add src/main.rs
git commit -m "feat: isolate chip repair mode"
```

### Task 2: Two Independent Deployment Workers

**Files:**
- Modify: `.github/workflows/deploy.yml`
- Modify: `web/dashboard/tests/deployment.test.mjs`

**Interfaces:**
- Consumes: `--repair-company-data` and `--repair-chips` from Task 1.
- Produces: transient units `qbot-company-intelligence-repair` and `qbot-chip-repair`, both started after the health check.

- [ ] **Step 1: Write failing deployment-contract assertions**

```js
assert.match(repair, /unit="qbot-company-intelligence-repair"[\s\S]*?--repair-company-data/);
assert.match(repair, /chip_unit="qbot-chip-repair"/);
assert.match(repair, /--unit="\$chip_unit"[\s\S]*?\/opt\/qbot\/qbot --repair-chips/);
assert.ok(repairStart > healthCheck);
```

Extend lifecycle scenarios to prove a running old company unit and a running chip unit are each stopped before replacement, and a chip launch failure fails the deployment step with its journal output.

- [ ] **Step 2: Run deployment tests and verify failure**

Run: `cd web/dashboard && node --test tests/deployment.test.mjs`

Expected: FAIL because the workflow still starts one combined worker.

- [ ] **Step 3: Generalize the transient-unit shell helper and start both modes**

```bash
start_repair_unit() {
  unit="$1"
  description="$2"
  mode="$3"
  clear_existing_transient_unit "$unit"
  sudo systemd-run --no-block \
    --unit="$unit" \
    --description="$description" \
    --property=Type=exec \
    --property=RemainAfterExit=yes \
    --property=EnvironmentFile=/opt/qbot/.env \
    --property="User=$service_user" \
    --property=UMask=0077 \
    --property=StandardOutput=journal \
    --property=StandardError=journal \
    --working-directory=/opt/qbot \
    /opt/qbot/qbot "$mode"
  observe_repair_launch "$unit"
}

start_repair_unit "qbot-company-intelligence-repair" \
  "QBot resumable company financial and dividend repair" \
  "--repair-company-data"
start_repair_unit "qbot-chip-repair" \
  "QBot resumable chip validation and backfill" \
  "--repair-chips"
```

The helper must keep the existing idempotent stale-unit cleanup, fast-success handling, active-running acceptance, failure diagnostics, and no-wait behavior for both units.

- [ ] **Step 4: Run deployment-contract and full frontend tests**

Run: `cd web/dashboard && node --test tests/deployment.test.mjs && npm test`

Expected: PASS.

- [ ] **Step 5: Commit workflow isolation**

```bash
git add .github/workflows/deploy.yml web/dashboard/tests/deployment.test.mjs web/dashboard/tests/fixtures
git commit -m "ci: start independent chip repair worker"
```

### Task 3: Repository-Wide Verification

**Files:**
- Test only; no production changes expected.

**Interfaces:**
- Consumes: completed repair modes and workflow.
- Produces: verified backend/deployment change ready for GitHub Actions.

- [ ] **Step 1: Run formatting and static checks**

Run: `cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings`

Expected: PASS with no warnings.

- [ ] **Step 2: Run all Rust and dashboard checks**

Run: `cargo test --locked && cd web/dashboard && npm ci && npm test && npx playwright test && node scripts/check-assets.mjs`

Expected: every test passes.

- [ ] **Step 3: Confirm the change scope**

Run: `git diff 3a52436 --stat && git diff 3a52436 -- web/miniapp src/telegram.rs src/api/routes.rs && git status --short`

Expected: the first command lists only dashboard, repair, workflow, test, and plan files; the second command has no output; the worktree is clean after commits.

- [ ] **Step 4: Commit any verification-only corrections**

```bash
git add src/main.rs .github/workflows/deploy.yml web/dashboard/tests
git diff --cached --quiet || git commit -m "test: verify independent chip repair"
```

### Task 4: GitHub Actions Deployment and Read-Only Production Check

**Files:**
- No additional source files expected.

**Interfaces:**
- Consumes: verified commits from Tasks 1-3 and the chart-profile plan.
- Produces: deployed dashboard and independently running chip backfill.

- [ ] **Step 1: Push the feature branch through the existing repository workflow**

Run: `git push origin feature/professional-scan-dashboard`

Expected: push succeeds without force.

- [ ] **Step 2: Verify GitHub Actions checks and deployment**

Run the repository's existing GitHub Actions status command for the pushed commit.

Expected: Rust tests, dashboard unit/browser tests, asset checks, and deployment all succeed.

- [ ] **Step 3: Perform read-only production checks**

```bash
ssh -i ~/.ssh/nerd.key root@107.173.82.4 \
  'systemctl show qbot qbot-company-intelligence-repair qbot-chip-repair --property=Id,ActiveState,SubState,ExecMainStatus --no-pager; journalctl -u qbot-chip-repair -n 80 --no-pager'
```

Expected: `qbot` is active; the two repair units are running or successfully exited; chip logs show benchmark/backfill progress rather than waiting for financial completion.

- [ ] **Step 4: Verify canonical rows begin growing without writing the database**

Run a read-only `SELECT count(*), count(DISTINCT code) FROM chip_distribution` through the PostgreSQL container.

Expected: counts rise above the pre-deployment baseline of `0, 0` as the independent worker commits snapshots.
