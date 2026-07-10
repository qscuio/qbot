# Strong-Stock Pattern Shadow Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build week and month strong-stock archetype research with matched controls, purged walk-forward validation, baseline comparison, manual model publishing, and Rust shadow matching.

**Architecture:** Python creates versioned Parquet datasets, discovers archetypes, validates discrimination against matched controls, and writes immutable draft model versions. Rust loads only manually published models and produces shadow candidates through a small `PatternEngine` interface; no output reaches auto trading.

**Tech Stack:** Python 3.12, Polars, PyArrow, DuckDB, scikit-learn, SciPy, Pydantic, pytest, Rust 2021, SQLx/PostgreSQL.

## Global Constraints

- Phase 0 must be complete.
- Only `week` and `month` can publish.
- `quarter` may run research-only; `year` is descriptive-only.
- Event features are excluded.
- Rust does not start Python.
- New output is shadow-only.
- `scan_ranker` is a baseline.
- Model publishing is manual.
- Migration number is `014`.

---

### Task 1: Add pattern research and shadow schema

**Files:**
- Create: `migrations/014_pattern_research_and_shadow.sql`
- Create: `src/storage/pattern_repository.rs`
- Modify: `src/storage/mod.rs`
- Test: `src/storage/pattern_repository.rs`

**Interfaces:**
- Produces training-run, dataset, model-version, model-set, and shadow-candidate tables.
- Later Python and Rust tasks use these tables as the contract.

- [x] **Step 1: Create the migration**

```sql
CREATE TABLE analysis_dataset_manifests (
    dataset_version      VARCHAR(80) PRIMARY KEY,
    schema_version       VARCHAR(32) NOT NULL,
    feature_version      VARCHAR(32) NOT NULL,
    horizon              VARCHAR(20) NOT NULL,
    data_cutoff          DATE NOT NULL,
    available_at_cutoff  TIMESTAMPTZ NOT NULL,
    row_count            BIGINT NOT NULL,
    date_from             DATE NOT NULL,
    date_to               DATE NOT NULL,
    manifest              JSONB NOT NULL,
    input_fingerprint     VARCHAR(64) NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE analysis_training_runs (
    run_id                UUID PRIMARY KEY,
    horizon               VARCHAR(20) NOT NULL,
    dataset_version       VARCHAR(80) NOT NULL REFERENCES analysis_dataset_manifests(dataset_version),
    status                VARCHAR(20) NOT NULL,
    config                JSONB NOT NULL,
    code_version          VARCHAR(80) NOT NULL,
    started_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at          TIMESTAMPTZ,
    error_message         TEXT
);

CREATE TABLE analysis_pattern_versions (
    pattern_version_id    UUID PRIMARY KEY,
    pattern_id            VARCHAR(80) NOT NULL,
    horizon               VARCHAR(20) NOT NULL,
    pattern_type          VARCHAR(40) NOT NULL,
    status                VARCHAR(20) NOT NULL,
    schema_version        VARCHAR(32) NOT NULL,
    feature_version       VARCHAR(32) NOT NULL,
    logic_version         VARCHAR(32) NOT NULL,
    dataset_version       VARCHAR(80) NOT NULL REFERENCES analysis_dataset_manifests(dataset_version),
    model_payload         JSONB NOT NULL,
    validation_payload    JSONB NOT NULL,
    trained_from          DATE NOT NULL,
    trained_until         DATE NOT NULL,
    available_at_cutoff   TIMESTAMPTZ NOT NULL,
    approved_by           VARCHAR(100),
    published_at          TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pattern_id, pattern_version_id)
);

CREATE TABLE analysis_pattern_sets (
    pattern_set_id        UUID PRIMARY KEY,
    name                  VARCHAR(100) NOT NULL,
    status                VARCHAR(20) NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at          TIMESTAMPTZ
);

CREATE TABLE analysis_pattern_set_members (
    pattern_set_id        UUID NOT NULL REFERENCES analysis_pattern_sets(pattern_set_id),
    pattern_version_id    UUID NOT NULL REFERENCES analysis_pattern_versions(pattern_version_id),
    member_order          INT NOT NULL,
    PRIMARY KEY (pattern_set_id, pattern_version_id),
    UNIQUE (pattern_set_id, member_order)
);

CREATE TABLE analysis_pattern_examples (
    pattern_version_id    UUID NOT NULL REFERENCES analysis_pattern_versions(pattern_version_id),
    example_type          VARCHAR(20) NOT NULL,
    code                  VARCHAR(12) NOT NULL,
    trade_date            DATE NOT NULL,
    similarity            NUMERIC(10,6),
    metadata              JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (pattern_version_id, example_type, code, trade_date)
);

CREATE TABLE analysis_shadow_candidates (
    trade_date            DATE NOT NULL,
    code                  VARCHAR(12) NOT NULL,
    horizon               VARCHAR(20) NOT NULL,
    pattern_version_id    UUID NOT NULL REFERENCES analysis_pattern_versions(pattern_version_id),
    pattern_set_id        UUID NOT NULL REFERENCES analysis_pattern_sets(pattern_set_id),
    pattern_type          VARCHAR(40) NOT NULL,
    similarity_score      NUMERIC(10,6) NOT NULL,
    validated_lift        NUMERIC(10,6) NOT NULL,
    final_score           NUMERIC(10,4) NOT NULL,
    shadow_tier           VARCHAR(20) NOT NULL,
    matched_features      JSONB NOT NULL,
    risk_flags            JSONB NOT NULL,
    supporting_signals    JSONB NOT NULL,
    invalidations         JSONB NOT NULL,
    input_fingerprint     VARCHAR(64) NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, code, horizon, pattern_version_id)
);

CREATE INDEX idx_pattern_versions_status
    ON analysis_pattern_versions(status, horizon, pattern_type);

CREATE INDEX idx_shadow_candidates_date
    ON analysis_shadow_candidates(trade_date DESC, horizon, shadow_tier, final_score DESC);
```

- [x] **Step 2: Add SQLx tests**

Test:

- draft model insertion.
- published model query returns only `status='published'` members from a published set.
- a pattern set cannot contain duplicate members or duplicate member order.
- duplicate shadow candidate upserts deterministically.

- [x] **Step 3: Implement `PatternRepository`**

Public methods:

```rust
pub async fn list_published_patterns(&self, pattern_set_id: Uuid) -> Result<Vec<PatternVersionRow>>;
pub async fn upsert_shadow_candidates(&self, rows: &[ShadowCandidateRow]) -> Result<usize>;
pub async fn latest_published_set(&self) -> Result<Option<PatternSetRow>>;
pub async fn list_shadow_candidates(&self, trade_date: NaiveDate) -> Result<Vec<ShadowCandidateRow>>;
```

- [x] **Step 4: Verify and commit**

```bash
cargo test storage::pattern_repository -- --nocapture
git add migrations/014_pattern_research_and_shadow.sql src/storage
git commit -m "feat: add pattern research and shadow schema"
```

---

### Task 2: Create the independent Python research package

**Files:**
- Create: `research/pyproject.toml`
- Create: `research/qbot_research/__init__.py`
- Create: `research/qbot_research/contracts.py`
- Create: `research/tests/test_contracts.py`
- Create: `deploy/qbot-research.service`
- Create: `deploy/qbot-research.timer`

**Interfaces:**
- Produces the `qbot-research` CLI package.
- Python is independently deployable.
- No Rust subprocess integration.

- [x] **Step 1: Define dependencies**

```toml
[project]
name = "qbot-research"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
  "duckdb>=1.2,<2",
  "numpy>=2,<3",
  "polars>=1.20,<2",
  "pyarrow>=18,<20",
  "pydantic>=2.10,<3",
  "psycopg[binary]>=3.2,<4",
  "scikit-learn>=1.6,<2",
  "scipy>=1.15,<2",
  "typer>=0.15,<1"
]

[project.optional-dependencies]
dev = [
  "mypy>=1.14,<2",
  "pytest>=8.3,<9",
  "ruff>=0.9,<1"
]

[project.scripts]
qbot-research = "qbot_research.cli:app"

[tool.pytest.ini_options]
testpaths = ["tests"]

[tool.ruff]
line-length = 100

[tool.mypy]
strict = true
```

- [x] **Step 2: Define Pydantic contracts**

```python
from datetime import date, datetime
from typing import Literal
from pydantic import BaseModel, Field

Horizon = Literal["week", "month", "quarter", "year"]
PatternType = Literal["trend", "vcp_breakout", "oversold_reversal"]

class DatasetManifest(BaseModel):
    dataset_version: str
    schema_version: str
    feature_version: str
    horizon: Horizon
    data_cutoff: date
    available_at_cutoff: datetime
    row_count: int = Field(ge=0)
    date_from: date
    date_to: date
    files: list[str]
    file_checksums: dict[str, str]
    input_fingerprint: str

class PatternModelPayload(BaseModel):
    required_features: list[str]
    scaler_mean: dict[str, float]
    scaler_scale: dict[str, float]
    centroid: dict[str, float]
    distance_metric: Literal["euclidean", "mahalanobis", "gmm_probability"]
    similarity_thresholds: dict[str, float]
    necessary_conditions: list[dict]
    risk_conditions: list[dict]

class ValidationPayload(BaseModel):
    positive_sample_count: int
    control_sample_count: int
    effective_sample_count: float
    base_rate: float
    precision: float
    lift_over_base_rate: float
    coverage: float
    false_positive_rate: float
    cost_adjusted_return: float
    max_drawdown: float
    majority_windows_positive_lift: bool
    baseline_comparison: dict[str, float]
```

- [x] **Step 3: Add validation tests**

Assert invalid horizons, negative row counts, and missing model features fail.

- [x] **Step 4: Add independent service files**

`qbot-research.service` must run:

```ini
ExecStart=/opt/qbot/research/.venv/bin/qbot-research train-all --config /etc/qbot/research.toml
```

`qbot-research.timer` runs weekly and must not depend on `qbot.service` beyond network/database availability.

- [x] **Step 5: Verify and commit**

```bash
cd research
python -m pytest -q
python -m ruff check .
python -m mypy qbot_research
cd ..
git add research deploy/qbot-research.service deploy/qbot-research.timer
git commit -m "feat: scaffold independent research worker"
```

---

### Task 3: Export point-in-time datasets to Parquet

**Files:**
- Create: `research/qbot_research/datasets.py`
- Create: `research/qbot_research/cli.py`
- Create: `research/tests/test_datasets.py`

**Interfaces:**
- Produces `build_dataset(horizon, as_of, output_dir) -> DatasetManifest`.
- Reads only records whose `available_at <= cutoff`.
- Writes Parquet and manifest.

- [x] **Step 1: Write fixture-based tests**

Use DuckDB temporary tables to assert:

- a bar available after cutoff is excluded.
- current sector membership is not used before `valid_from`.
- a delisted stock remains in historical data.
- rows missing critical adjustment/status data are excluded and counted.

- [x] **Step 2: Implement dataset query**

Build a Polars frame with one row per `(trade_date, code)` and explicit columns for:

```text
adjusted OHLC
amount
turnover
security status
historical industry
index returns
market breadth
available_at_cutoff
```

- [x] **Step 3: Write partitioned Parquet**

Partition by:

```text
dataset_version
horizon
year
```

Compute SHA-256 for every file and save `manifest.json`.

- [x] **Step 4: Register the manifest in PostgreSQL**

Insert into `analysis_dataset_manifests` only after all files and checksums are complete.

- [x] **Step 5: Verify and commit**

```bash
cd research
python -m pytest tests/test_datasets.py -q
python -m ruff check qbot_research/datasets.py
git add research
git commit -m "feat: export point-in-time research datasets"
```

---

### Task 4: Implement labels and matched controls

**Files:**
- Create: `research/qbot_research/labels.py`
- Create: `research/qbot_research/controls.py`
- Create: `research/tests/test_labels.py`
- Create: `research/tests/test_controls.py`

**Interfaces:**
- Produces `label_samples(frame, horizon)`.
- Produces `match_controls(samples, candidates, config)`.

- [x] **Step 1: Test label timing**

Assert feature date `t` never reads a future value except the label columns.

- [x] **Step 2: Implement labels**

Output:

```text
future_return
future_market_excess
future_industry_excess
future_max_drawdown
future_max_favorable_excursion
tradable_sample
strength_score
is_positive
```

- [x] **Step 3: Test matched controls**

Given a positive sample, select controls using:

```text
same date
same historical industry
market-cap bucket
price bucket
20-day amount bucket
20-day volatility bucket
tradable state
```

Assert the positive sample cannot match itself.

- [x] **Step 4: Add failure-control types**

Label controls as:

```text
ordinary
failed_breakout
negative_excess
```

- [x] **Step 5: Verify and commit**

```bash
cd research
python -m pytest tests/test_labels.py tests/test_controls.py -q
git add research
git commit -m "feat: add strong-stock labels and matched controls"
```

---

### Task 5: Implement archetype discovery

**Files:**
- Create: `research/qbot_research/archetypes.py`
- Create: `research/tests/test_archetypes.py`

**Interfaces:**
- Produces `discover_archetypes(train_frame, pattern_type, config)`.
- Outputs serializable candidate archetypes.

- [x] **Step 1: Define deterministic pattern-family gates**

Implement three pure masks:

```python
def trend_family(frame: pl.DataFrame) -> pl.Expr: ...
def vcp_breakout_family(frame: pl.DataFrame) -> pl.Expr: ...
def oversold_reversal_family(frame: pl.DataFrame) -> pl.Expr: ...
```

- [x] **Step 2: Test unclassified behavior**

Samples not satisfying any family remain unclassified; do not force assignment.

- [x] **Step 3: Implement K-Means and GMM comparison**

Use only training-window data. Save:

- scaler parameters.
- centroids or mixture parameters.
- silhouette/BIC.
- random seed.
- high-contribution features.

- [x] **Step 4: Reject unstable or tiny clusters**

Candidate archetypes below configured sample size are not exported.

- [x] **Step 5: Verify and commit**

```bash
cd research
python -m pytest tests/test_archetypes.py -q
git add research
git commit -m "feat: discover interpretable strong-stock archetypes"
```

---

### Task 6: Implement purged walk-forward validation and baselines

**Files:**
- Create: `research/qbot_research/validation.py`
- Create: `research/qbot_research/baselines.py`
- Create: `research/tests/test_validation.py`
- Create: `research/tests/test_baselines.py`

**Interfaces:**
- Produces `purged_walk_forward_splits`.
- Produces `validate_archetype`.
- Produces baseline metrics with the same result schema.

- [x] **Step 1: Test purge and embargo**

For a 20-day horizon, assert:

- no train label window overlaps validation start.
- embargo contains at least 20 trading days after validation.

- [x] **Step 2: Implement split generator**

```python
def purged_walk_forward_splits(
    dates: list[date],
    train_months: int,
    validation_months: int,
    step_months: int,
    horizon_days: int,
) -> list[Split]:
    ...
```

- [x] **Step 3: Implement baselines**

Required:

```text
relative_strength_20_60
ma20_ma60_trend
volatility_contraction_breakout
scan_ranker_a
```

- [x] **Step 4: Implement validation metrics**

Calculate:

```text
base_rate
precision
lift
coverage
false_positive_rate
precision_at_10
precision_at_50
cost_adjusted_return
max_drawdown
turnover
yearly_results
regime_results
top_stock_contribution
top_period_contribution
```

- [x] **Step 5: Enforce release gate**

A candidate can become `validated` only if:

```python
majority_windows_positive_lift is True
and cost_adjusted_return > best_required_baseline_return
and top_stock_contribution <= config.max_single_stock_contribution
and top_period_contribution <= config.max_single_period_contribution
```

- [x] **Step 6: Verify and commit**

```bash
cd research
python -m pytest tests/test_validation.py tests/test_baselines.py -q
git add research
git commit -m "feat: validate patterns with purged walk-forward"
```

---

### Task 7: Export draft model versions

**Files:**
- Create: `research/qbot_research/export.py`
- Modify: `research/qbot_research/cli.py`
- Create: `research/tests/test_export.py`

**Interfaces:**
- Produces immutable `analysis_pattern_versions` rows.
- Never writes `published`.

- [x] **Step 1: Test export contract**

Assert exported payload validates with Pydantic and status is only:

```text
draft
validated
```

- [x] **Step 2: Implement CLI commands**

```text
qbot-research build-dataset --horizon week --as-of YYYY-MM-DD
qbot-research train --horizon week --dataset-version ...
qbot-research train-all --as-of YYYY-MM-DD
```

- [x] **Step 3: Save examples**

Write typical positive and failed examples to `analysis_pattern_examples`.

- [x] **Step 4: Verify and commit**

```bash
cd research
python -m pytest -q
python -m ruff check .
python -m mypy qbot_research
git add research
git commit -m "feat: export validated pattern versions"
```

---

### Task 8: Implement Rust model loading and matching

**Files:**
- Create: `src/analysis/patterns/mod.rs`
- Create: `src/analysis/patterns/model.rs`
- Create: `src/analysis/patterns/matcher.rs`
- Create: `src/analysis/patterns/ranking.rs`
- Create: `src/analysis/patterns/explanation.rs`
- Modify: `src/analysis/mod.rs`
- Test: `src/analysis/patterns/matcher.rs`

**Interfaces:**
- Produces `PatternEngine::match_market`.
- Consumes published model payloads.
- Writes shadow candidates only.

- [x] **Step 1: Define Rust model contract**

Mirror Python fields exactly with `serde(deny_unknown_fields)`.

- [x] **Step 2: Add golden fixture test**

Place a JSON fixture under:

```text
tests/fixtures/pattern_model_v1.json
```

Assert:

- valid payload loads.
- unknown Schema rejects.
- missing feature rejects.
- fixed feature vector produces fixed similarity.

- [x] **Step 3: Implement matching**

```rust
pub struct PatternEngine {
    pattern_repo: PatternRepository,
    market_repo: MarketRepository,
}

impl PatternEngine {
    pub async fn match_market(
        &self,
        trade_date: NaiveDate,
        pattern_set_id: Uuid,
    ) -> Result<Vec<PatternCandidate>>;
}
```

No event inputs.

- [x] **Step 4: Implement tiers**

```text
shadow_a
shadow_b
watch
reject
```

A model validation Lift is part of the score; similarity alone cannot create `shadow_a`.

- [x] **Step 5: Persist output**

Use `analysis_shadow_candidates`, never `signal_strategy_candidates`.

- [x] **Step 6: Verify and commit**

```bash
cargo test analysis::patterns -- --nocapture
git add src/analysis/patterns tests/fixtures
git commit -m "feat: match published patterns in shadow mode"
```

---

### Task 9: Add shadow scheduler and reporting

**Files:**
- Create: `src/api/pattern_routes.rs`
- Modify: `src/api/mod.rs`
- Modify: `src/api/routes.rs`
- Modify: `src/scheduler/mod.rs`
- Modify: `README.md`
- Test: `src/api/pattern_routes.rs`
- Test: `src/scheduler/mod.rs`

**Interfaces:**
- Adds shadow read endpoints.
- Adds a 17:40 job after market snapshot and scan.

- [ ] **Step 1: Add endpoints**

```text
GET  /api/analysis/patterns/shadow
GET  /api/analysis/patterns/shadow/:code
POST /api/jobs/analysis/pattern-match
```

- [ ] **Step 2: Add scheduler function**

```rust
pub async fn run_pattern_shadow_job(state: Arc<AppState>) {
    // load latest published set
    // skip if snapshot incomplete
    // persist shadow candidates
}
```

- [ ] **Step 3: Add cron**

```rust
const PATTERN_SHADOW_JOB_CRON: &str = "0 40 17 * * Mon,Tue,Wed,Thu,Fri";
```

- [ ] **Step 4: Add safety tests**

Assert:

- no insertion into `signal_strategy_candidates`.
- existing scan result counts do not change.
- missing published model skips safely.
- incomplete snapshot skips safely.

- [ ] **Step 5: Verify and commit**

```bash
cargo fmt --all -- --check
cargo test --all --locked
cd research && python -m pytest -q && cd ..
git diff --check
git add src README.md
git commit -m "feat: report strong-stock shadow candidates"
```

---

## Phase Completion Checklist

- [ ] Week and month datasets are point-in-time safe.
- [ ] Controls are matched and persisted.
- [ ] Purge and embargo tests pass.
- [ ] All four baselines run.
- [ ] Pattern validation reports Lift and effective sample size.
- [ ] Python exports only draft/validated models.
- [ ] Publishing remains manual.
- [ ] Rust loads only published models.
- [ ] Shadow candidates never reach auto trading.
- [ ] Existing `scan_ranker` remains unchanged and comparable.
