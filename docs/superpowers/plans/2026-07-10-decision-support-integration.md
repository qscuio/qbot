# Decision-Support Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only DecisionSupport layer that combines existing scan-ranker output, shadow pattern candidates, market snapshots, and event context without changing auto trading.

**Architecture:** Introduce one deep Rust module that reads existing and new analysis outputs, produces a versioned daily decision-support artifact, and exposes separate API/Telegram views. Event contribution begins at zero and can only be enabled later through a bounded, auditable configuration after shadow validation.

**Tech Stack:** Rust 2021, Axum, SQLx/PostgreSQL, Serde, existing Telegram integration.

## Global Constraints

- Phases 0-3 must be complete.
- No write path to `signal_strategy_candidates`.
- Existing `scan_ranker` remains available.
- Event adjustment defaults to `0`.
- Maximum future event adjustment is `5` absolute points.
- An event cannot promote Reject directly to A.
- Existing `AiAnalysisService` becomes a compatibility adapter and receives no new logic.
- Migration number is `017`.

---

### Task 1: Add decision-support schema

**Files:**
- Create: `migrations/017_decision_support.sql`
- Create: `src/storage/decision_support_repository.rs`
- Modify: `src/storage/mod.rs`
- Test: `src/storage/decision_support_repository.rs`

**Interfaces:**
- Produces immutable daily artifacts and candidate details.

- [x] **Step 1: Create migration**

```sql
CREATE TABLE analysis_decision_support_runs (
    run_id              UUID PRIMARY KEY,
    trade_date          DATE NOT NULL,
    support_version     VARCHAR(32) NOT NULL,
    market_snapshot_version VARCHAR(32) NOT NULL,
    pattern_set_id      UUID,
    event_brief_version VARCHAR(32),
    event_score_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    event_score_limit   NUMERIC(8,4) NOT NULL DEFAULT 0,
    status              VARCHAR(20) NOT NULL,
    input_fingerprint   VARCHAR(64) NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    error_message       TEXT,
    UNIQUE (trade_date, support_version)
);

CREATE TABLE analysis_decision_candidates (
    run_id              UUID NOT NULL REFERENCES analysis_decision_support_runs(run_id),
    code                VARCHAR(12) NOT NULL,
    name                VARCHAR(100) NOT NULL,
    horizon             VARCHAR(20) NOT NULL,
    base_source         VARCHAR(40) NOT NULL,
    base_score          NUMERIC(10,4) NOT NULL,
    pattern_score       NUMERIC(10,4),
    event_adjustment    NUMERIC(10,4) NOT NULL DEFAULT 0,
    risk_adjustment     NUMERIC(10,4) NOT NULL DEFAULT 0,
    final_score         NUMERIC(10,4) NOT NULL,
    support_tier        VARCHAR(20) NOT NULL,
    facts               JSONB NOT NULL DEFAULT '[]',
    calculations        JSONB NOT NULL DEFAULT '[]',
    inferences          JSONB NOT NULL DEFAULT '[]',
    unknowns            JSONB NOT NULL DEFAULT '[]',
    risk_flags          JSONB NOT NULL DEFAULT '[]',
    invalidations       JSONB NOT NULL DEFAULT '[]',
    source_refs         JSONB NOT NULL DEFAULT '[]',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, code, horizon)
);

CREATE TABLE analysis_decision_daily_briefs (
    run_id              UUID PRIMARY KEY REFERENCES analysis_decision_support_runs(run_id),
    trade_date          DATE NOT NULL,
    content             TEXT NOT NULL,
    structured_payload  JSONB NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_decision_candidates_rank
    ON analysis_decision_candidates(run_id, support_tier, final_score DESC);
```

- [x] **Step 2: Add repository tests**

Verify:

- one run per trade date/version.
- event adjustment defaults to zero.
- candidate facts/calculations/inferences/unknowns remain separate.
- repeated build upserts by deterministic run ID or rejects duplicate version.

- [x] **Step 3: Implement repository**

```rust
pub async fn create_run(&self, row: &DecisionSupportRunRow) -> Result<Uuid>;
pub async fn save_candidates(&self, rows: &[DecisionCandidateRow]) -> Result<usize>;
pub async fn save_brief(&self, row: &DecisionBriefRow) -> Result<()>;
pub async fn latest_run(&self) -> Result<Option<DecisionSupportRunRow>>;
pub async fn list_candidates(&self, run_id: Uuid) -> Result<Vec<DecisionCandidateRow>>;
```

- [x] **Step 4: Verify and commit**

```bash
cargo test storage::decision_support_repository -- --nocapture
git add migrations/017_decision_support.sql src/storage
git commit -m "feat: add decision-support persistence"
```

---

### Task 2: Add DecisionSupport contracts and deep module

**Files:**
- Create: `src/analysis/decision_support/mod.rs`
- Create: `src/analysis/decision_support/contracts.rs`
- Create: `src/analysis/decision_support/builder.rs`
- Modify: `src/analysis/mod.rs`
- Test: `src/analysis/decision_support/builder.rs`

**Interfaces:**
- Produces `DecisionSupport::build_daily`.
- Reads but does not mutate scanner, pattern, event, or trading data.

- [x] **Step 1: Define contracts**

```rust
pub struct DecisionSupport {
    market_repo: MarketRepository,
    pattern_repo: PatternRepository,
    event_repo: EventRepository,
    decision_repo: DecisionSupportRepository,
}

pub struct DailyDecisionSupport {
    pub trade_date: NaiveDate,
    pub run_id: Uuid,
    pub candidates: Vec<DecisionCandidate>,
    pub market_summary: MarketSummary,
    pub event_summary: Option<DailyEventBrief>,
    pub data_status: DataStatus,
}

pub struct DecisionCandidate {
    pub code: String,
    pub name: String,
    pub horizon: String,
    pub base_source: String,
    pub base_score: f64,
    pub pattern_score: Option<f64>,
    pub event_adjustment: f64,
    pub risk_adjustment: f64,
    pub final_score: f64,
    pub support_tier: String,
    pub facts: Vec<SupportStatement>,
    pub calculations: Vec<SupportStatement>,
    pub inferences: Vec<SupportStatement>,
    pub unknowns: Vec<SupportStatement>,
    pub risk_flags: Vec<String>,
    pub invalidations: Vec<String>,
}
```

- [x] **Step 2: Add classification tests**

A source-backed event fact must appear in `facts`.

A pattern similarity and Lift statement must appear in `calculations`.

An impact hypothesis must appear in `inferences`.

Missing status data must appear in `unknowns`.

- [x] **Step 3: Implement builder interface**

```rust
impl DecisionSupport {
    pub async fn build_daily(
        &self,
        trade_date: NaiveDate,
        config: DecisionSupportConfig,
    ) -> Result<DailyDecisionSupport>;
}
```

- [x] **Step 4: Verify and commit**

```bash
cargo test analysis::decision_support
git add src/analysis/decision_support src/analysis/mod.rs
git commit -m "feat: add read-only decision-support module"
```

---

### Task 3: Add existing scan-ranker baseline adapter

**Files:**
- Create: `src/analysis/decision_support/scan_ranker_adapter.rs`
- Modify: `src/services/scan_ranker.rs`
- Test: `src/analysis/decision_support/scan_ranker_adapter.rs`

**Interfaces:**
- Reads latest archived pool results.
- Converts them to a stable baseline contract.
- Does not duplicate scan-ranker scoring logic.

- [x] **Step 1: Expose a stable metadata parser**

Add a public read-only type:

```rust
pub struct RankedPoolEvidence {
    pub pool_id: String,
    pub line_type: String,
    pub tier: String,
    pub trigger_id: String,
    pub trigger_name: String,
    pub score: f64,
    pub reasons: Vec<String>,
    pub risk_flags: Vec<String>,
    pub factor_breakdown: Vec<(String, f64)>,
}
```

Add:

```rust
pub fn ranked_pool_evidence(hit: &SignalHit) -> Option<RankedPoolEvidence>;
```

Do not expose internal `Metrics` or classifier functions.

- [x] **Step 2: Implement adapter**

```rust
pub async fn load_scan_ranker_baseline(
    pool: &PgPool,
    trade_date: NaiveDate,
) -> Result<Vec<BaselineCandidate>>;
```

Read `daily_signal_scan_results` for pool IDs only.

- [x] **Step 3: Test no rescoring**

A metadata fixture must produce the same score and reasons without recalculating from candles.

- [x] **Step 4: Verify and commit**

```bash
cargo test scan_ranker_adapter
git add src/analysis/decision_support/scan_ranker_adapter.rs src/services/scan_ranker.rs
git commit -m "feat: adapt scan-ranker output for decision support"
```

---

### Task 4: Combine baseline and shadow pattern candidates

**Files:**
- Create: `src/analysis/decision_support/pattern_adapter.rs`
- Modify: `src/analysis/decision_support/builder.rs`
- Test: `src/analysis/decision_support/builder.rs`

**Interfaces:**
- Joins candidates by `(code, horizon)`.
- Preserves source-specific scores.

- [x] **Step 1: Define merge rules**

```text
scan-ranker only      -> base_source=scan_ranker
pattern only          -> base_source=pattern_shadow
both                  -> base_source=combined
```

Do not average unrelated raw score scales.

- [x] **Step 2: Normalize evidence, not raw scores**

Use configured rank percentiles:

```text
scan_ranker_percentile
pattern_percentile
```

Final first-version score:

```text
base_score = max(scan_ranker_percentile, pattern_percentile)
pattern_score = pattern_percentile or null
```

- [x] **Step 3: Add conflict tests**

If scan-ranker says A and pattern says Reject:

- support tier cannot be A.
- add a disagreement risk flag.
- retain both explanations.

- [x] **Step 4: Verify and commit**

```bash
cargo test analysis::decision_support::builder
git add src/analysis/decision_support
git commit -m "feat: combine baseline and shadow pattern evidence"
```

---

### Task 5: Add event context with zero score

**Files:**
- Create: `src/analysis/decision_support/event_adapter.rs`
- Modify: `src/analysis/decision_support/builder.rs`
- Test: `src/analysis/decision_support/event_adapter.rs`

**Interfaces:**
- Adds direct event facts and industry context.
- Returns `event_adjustment = 0.0`.

- [x] **Step 1: Implement direct-context selection**

Include events when:

- stock is a directly linked entity, or
- stock's point-in-time industry matches a directly affected industry.

Do not include fuzzy beneficiary lists.

- [x] **Step 2: Test zero contribution**

```rust
assert_eq!(candidate.event_adjustment, 0.0);
assert_eq!(candidate.final_score, candidate.base_score + candidate.risk_adjustment);
```

- [x] **Step 3: Separate statements**

- ClaimGraph content -> facts.
- Market observation -> calculations.
- ImpactHypothesisGraph -> inferences.
- Missing direct mapping -> unknowns.

- [x] **Step 4: Verify and commit**

```bash
cargo test event_adapter
git add src/analysis/decision_support
git commit -m "feat: add zero-weight event context"
```

---

### Task 6: Add bounded optional event adjustment

**Files:**
- Modify: `src/config.rs`
- Modify: `.env.example`
- Modify: `src/analysis/decision_support/contracts.rs`
- Modify: `src/analysis/decision_support/builder.rs`
- Test: `src/analysis/decision_support/builder.rs`

**Interfaces:**
- Adds a disabled-by-default configuration.
- Maximum absolute adjustment is hard-capped at `5`.

- [ ] **Step 1: Add config**

```rust
pub enable_event_score_adjustment: bool,
pub max_event_score_adjustment: f64,
```

Environment:

```text
ENABLE_EVENT_SCORE_ADJUSTMENT=false
MAX_EVENT_SCORE_ADJUSTMENT=0
```

Parsing must clamp:

```rust
value.clamp(0.0, 5.0)
```

- [ ] **Step 2: Add gate tests**

Assert:

- disabled -> zero.
- configured 10 -> cap 5.
- Reject cannot become A.
- data-incomplete candidate cannot receive positive adjustment.
- only direct entity or reviewed industry relation is eligible.

- [ ] **Step 3: Implement audit payload**

Every adjustment records:

```text
event_id
entity_relation
market_alignment
causal_confidence
raw_adjustment
applied_adjustment
cap
reason
```

- [ ] **Step 4: Verify and commit**

```bash
cargo test decision_support::builder
git add src/config.rs .env.example src/analysis/decision_support
git commit -m "feat: gate bounded event score adjustments"
```

This task may merge with event score disabled. Do not enable in production as part of the same commit.

---

### Task 7: Add read-only API and Telegram report

**Files:**
- Create: `src/api/decision_support_routes.rs`
- Modify: `src/api/mod.rs`
- Modify: `src/api/routes.rs`
- Modify: `src/main.rs`
- Test: `src/api/decision_support_routes.rs`

**Interfaces:**
- Adds daily and candidate detail endpoints.
- Adds read-only Telegram commands.

- [ ] **Step 1: Add endpoints**

```text
GET  /api/analysis/decision-support/latest
GET  /api/analysis/decision-support/:date
GET  /api/analysis/decision-support/:date/:code
POST /api/jobs/analysis/decision-support
```

- [ ] **Step 2: Add Telegram commands**

```text
/decision
/decision_detail <code>
```

- [ ] **Step 3: Ensure response labels**

Every statement includes:

```text
fact
calculation
inference
unknown
```

- [ ] **Step 4: Add auth and safety tests**

No endpoint may create or update:

```text
signal_strategy_candidates
signal_strategy_positions
trading_sim_positions
daban_sim_positions
```

- [ ] **Step 5: Verify and commit**

```bash
cargo test api::decision_support_routes -- --nocapture
git add src/api src/main.rs
git commit -m "feat: expose read-only decision support"
```

---

### Task 8: Schedule daily DecisionSupport build

**Files:**
- Modify: `src/scheduler/mod.rs`
- Modify: `README.md`
- Test: `src/scheduler/mod.rs`

**Interfaces:**
- Builds after pattern and event jobs.
- Failure does not affect existing reports.

- [ ] **Step 1: Add job**

```rust
pub async fn run_decision_support_job(state: Arc<AppState>);
```

- [ ] **Step 2: Add schedule**

```rust
const DECISION_SUPPORT_JOB_CRON: &str = "0 55 17 * * Mon,Tue,Wed,Thu,Fri";
```

- [ ] **Step 3: Add degradation behavior**

If pattern results are missing:

- use scan-ranker baseline.
- show missing pattern status.

If event brief is missing:

- event context is absent.
- event adjustment remains zero.

If market snapshot is incomplete:

- build a data-status report.
- do not assign A.

- [ ] **Step 4: Verify and commit**

```bash
cargo test scheduler::tests
git add src/scheduler/mod.rs README.md
git commit -m "feat: schedule daily decision-support build"
```

---

### Task 9: Migrate `AiAnalysisService` to compatibility mode

**Files:**
- Modify: `src/services/ai_analysis.rs`
- Modify: `src/api/routes.rs`
- Modify: `src/services/mod.rs`
- Test: `src/services/ai_analysis.rs`

**Interfaces:**
- Existing `/api/market/overview` continues working.
- Old free-form AI prompt is no longer the source of market facts.

- [ ] **Step 1: Add compatibility adapter**

Replace internal market narrative generation with:

```rust
pub async fn market_overview(
    &self,
    date: Option<NaiveDate>,
) -> Result<MarketOverviewResponse> {
    let trade_date = date.unwrap_or_else(beijing_today);
    let decision = self
        .decision_support
        .load_or_build(trade_date)
        .await?;
    Ok(MarketOverviewResponse::from(decision))
}
```

- [ ] **Step 2: Remove scheduled free-form loop**

Do not run a separate 15:30 AI loop. Daily DecisionSupport is generated by scheduler.

- [ ] **Step 3: Preserve one compatibility cycle**

Keep response field names where practical. Mark `aiNarrative` deprecated and populate it from the structured brief rendering.

- [ ] **Step 4: Test no direct LLM call**

A compatibility test uses a fake LLM endpoint that would fail if called; `market_overview` must still pass from stored structured data.

- [ ] **Step 5: Verify and commit**

```bash
cargo test services::ai_analysis -- --nocapture
git add src/services/ai_analysis.rs src/api/routes.rs src/services/mod.rs
git commit -m "refactor: route market overview through decision support"
```

---

### Task 10: Full safety verification

**Files:**
- Modify: `docs/reviews/2026-07-10-analysis-platform-review-resolution.md`
- Modify: `README.md`

**Interfaces:**
- Documents actual release status and disabled features.

- [ ] **Step 1: Run full Rust verification**

```bash
cargo fmt --all -- --check
cargo test --all --locked
git diff --check
```

- [ ] **Step 2: Run Python verification**

```bash
cd research
python -m pytest -q
python -m ruff check .
python -m mypy qbot_research
```

- [ ] **Step 3: Run database assertions**

Execute SQL proving no DecisionSupport run inserted into trading tables:

```sql
SELECT COUNT(*) FROM signal_strategy_candidates
WHERE signal_metadata ? 'decision_support_run_id';
```

Expected: `0`.

- [ ] **Step 4: Document production flags**

README must show:

```text
ENABLE_EVENT_SCORE_ADJUSTMENT=false
MAX_EVENT_SCORE_ADJUSTMENT=0
```

- [ ] **Step 5: Commit**

```bash
git add README.md docs/reviews/2026-07-10-analysis-platform-review-resolution.md
git commit -m "docs: record decision-support release gates"
```

---

## Phase Completion Checklist

- [ ] DecisionSupport is read-only.
- [ ] Facts, calculations, inferences, and unknowns are separate.
- [ ] `scan_ranker` remains the baseline.
- [ ] Shadow patterns are not silently promoted to production.
- [ ] Event context begins at zero weight.
- [ ] Optional event adjustment is capped at 5.
- [ ] Reject cannot become A because of events.
- [ ] Existing market overview is a compatibility adapter.
- [ ] No new analysis output enters auto trading.
