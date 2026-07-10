# Event Evolution and Market Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the evidence MVP with event mentions, two-stage event clustering, EventDelta, frozen impact hypotheses, GDELT macro input, and non-causal market-alignment observation.

**Architecture:** Keep evidence and ClaimGraph immutable, add event clusters as versioned views over evidence, derive deltas between event versions, freeze impact hypotheses before observing market data, and store abnormal-return observations separately from causal confidence.

**Tech Stack:** Rust 2021, SQLx/PostgreSQL, Reqwest, Serde, existing market snapshots, optional LLM extraction, GDELT adapter.

## Global Constraints

- Phases 0 and 2 must be complete.
- Event score remains `0`.
- `market_aligned` is not causal confirmation.
- A frozen hypothesis cannot be edited by market-observation code.
- GDELT is supplementary, not a company-fact source.
- No non-direct beneficiary stock list.
- Migration number is `016`.

---

### Task 1: Add event evolution schema

**Files:**
- Create: `migrations/016_event_evolution.sql`
- Modify: `src/storage/event_repository.rs`
- Test: `src/storage/event_repository.rs`

**Interfaces:**
- Produces mentions, clusters, deltas, hypotheses, observations, and type-statistics tables.

- [ ] **Step 1: Create the migration**

```sql
CREATE TABLE market_event_mentions (
    mention_id             UUID PRIMARY KEY,
    evidence_id            UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    event_cluster_id       UUID,
    cluster_version        INT,
    mention_time           TIMESTAMPTZ NOT NULL,
    adds_new_fact          BOOLEAN NOT NULL,
    source_independence    NUMERIC(8,6) NOT NULL,
    mention_payload        JSONB NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (event_cluster_id IS NULL AND cluster_version IS NULL)
        OR (event_cluster_id IS NOT NULL AND cluster_version IS NOT NULL)
    )
);

CREATE TABLE market_event_clusters (
    event_cluster_id       UUID NOT NULL,
    cluster_version        INT NOT NULL,
    canonical_title        TEXT NOT NULL,
    event_time             TIMESTAMPTZ,
    first_seen_at          TIMESTAMPTZ NOT NULL,
    last_seen_at           TIMESTAMPTZ NOT NULL,
    lifecycle_status       VARCHAR(20) NOT NULL,
    primary_evidence_id    UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    representative_ids     UUID[] NOT NULL,
    source_entropy         NUMERIC(10,6) NOT NULL,
    independent_sources    INT NOT NULL,
    mention_count          INT NOT NULL,
    cluster_payload        JSONB NOT NULL,
    supersedes_version     INT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_cluster_id, cluster_version)
);

ALTER TABLE market_event_mentions
    ADD CONSTRAINT fk_event_mentions_cluster
    FOREIGN KEY (event_cluster_id, cluster_version)
    REFERENCES market_event_clusters(event_cluster_id, cluster_version)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE market_event_deltas (
    event_cluster_id       UUID NOT NULL,
    from_version           INT NOT NULL,
    to_version             INT NOT NULL,
    delta_payload          JSONB NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_cluster_id, from_version, to_version),
    FOREIGN KEY (event_cluster_id, from_version)
        REFERENCES market_event_clusters(event_cluster_id, cluster_version),
    FOREIGN KEY (event_cluster_id, to_version)
        REFERENCES market_event_clusters(event_cluster_id, cluster_version),
    CHECK (to_version > from_version)
);

CREATE TABLE market_event_hypotheses (
    hypothesis_id          UUID PRIMARY KEY,
    event_cluster_id       UUID NOT NULL,
    cluster_version        INT NOT NULL,
    hypothesis_version     INT NOT NULL,
    schema_version         VARCHAR(32) NOT NULL,
    graph_payload          JSONB NOT NULL,
    frozen_at              TIMESTAMPTZ NOT NULL,
    based_on_claim_ids     UUID[] NOT NULL,
    review_status          VARCHAR(20) NOT NULL,
    supersedes_id          UUID REFERENCES market_event_hypotheses(hypothesis_id),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_cluster_id, cluster_version, hypothesis_version),
    FOREIGN KEY (event_cluster_id, cluster_version)
        REFERENCES market_event_clusters(event_cluster_id, cluster_version)
);

CREATE TABLE market_event_market_observations (
    hypothesis_id          UUID NOT NULL REFERENCES market_event_hypotheses(hypothesis_id),
    entity_type            VARCHAR(40) NOT NULL,
    entity_id              VARCHAR(100) NOT NULL,
    trade_date             DATE NOT NULL,
    observation_status     VARCHAR(30) NOT NULL,
    market_alignment_score NUMERIC(10,6),
    causal_confidence      NUMERIC(10,6) NOT NULL,
    abnormal_market_return NUMERIC(12,6),
    abnormal_industry_return NUMERIC(12,6),
    market_metrics         JSONB NOT NULL,
    confounding_events     JSONB NOT NULL DEFAULT '[]',
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (hypothesis_id, entity_type, entity_id, trade_date)
);

CREATE TABLE market_event_type_statistics (
    event_type             VARCHAR(50) NOT NULL,
    event_subtype          VARCHAR(50) NOT NULL,
    entity_type            VARCHAR(40) NOT NULL,
    observation_window     VARCHAR(20) NOT NULL,
    sample_count           INT NOT NULL,
    median_abnormal_return NUMERIC(12,6),
    positive_rate          NUMERIC(10,6),
    turnover_response      NUMERIC(12,6),
    breadth_response       NUMERIC(12,6),
    time_to_peak           NUMERIC(12,6),
    failure_rate           NUMERIC(10,6),
    data_cutoff            DATE NOT NULL,
    logic_version          VARCHAR(32) NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (
        event_type, event_subtype, entity_type,
        observation_window, data_cutoff, logic_version
    )
);
```

- [ ] **Step 2: Add repository tests**

Verify:

- cluster versions append.
- a delta references adjacent versions.
- frozen hypothesis payload cannot be updated.
- market observations cannot exist without a hypothesis.
- observation status accepts only configured values at the application layer.

- [ ] **Step 3: Implement repository methods**

```rust
pub async fn save_event_cluster_version(&self, row: &EventClusterRow) -> Result<()>;
pub async fn save_event_delta(&self, row: &EventDeltaRow) -> Result<()>;
pub async fn save_frozen_hypothesis(&self, row: &EventHypothesisRow) -> Result<Uuid>;
pub async fn save_market_observation(&self, row: &MarketObservationRow) -> Result<()>;
pub async fn latest_cluster_version(&self, id: Uuid) -> Result<Option<EventClusterRow>>;
```

- [ ] **Step 4: Verify and commit**

```bash
cargo test storage::event_repository -- --nocapture
git add migrations/016_event_evolution.sql src/storage/event_repository.rs
git commit -m "feat: add event evolution schema"
```

---

### Task 2: Add EventMention and two-stage clustering

**Files:**
- Create: `src/analysis/events/mentions.rs`
- Create: `src/analysis/events/clustering.rs`
- Test: `src/analysis/events/clustering.rs`

**Interfaces:**
- Produces incremental candidate clusters.
- Produces end-of-day refined cluster versions.
- Preserves evidence and duplicate groups.

- [ ] **Step 1: Define clustering contracts**

```rust
pub struct EventMention {
    pub mention_id: Uuid,
    pub evidence_id: Uuid,
    pub event_time: Option<DateTime<Utc>>,
    pub entity_ids: Vec<String>,
    pub action_tokens: Vec<String>,
    pub location_tokens: Vec<String>,
    pub semantic_vector: Vec<f32>,
    pub adds_new_fact: bool,
    pub source_independence: f64,
}

pub struct ClusterDecision {
    pub event_cluster_id: Uuid,
    pub confidence: f64,
    pub reason_codes: Vec<String>,
}
```

- [ ] **Step 2: Test incremental clustering**

Cases:

- same entities, action, date, and high text similarity join.
- same company but different action does not auto-join.
- low confidence becomes review required.
- duplicate-group members do not count as independent sources.

- [ ] **Step 3: Implement low-cost incremental scoring**

```text
time proximity
entity overlap
action overlap
location overlap
semantic similarity
```

Automatic join requires all hard conditions plus configured score threshold.

- [ ] **Step 4: Implement end-of-day refinement**

Refinement can:

- merge fragmented clusters.
- split over-broad clusters.
- choose representative evidence.
- calculate source entropy.
- respect user-locked merge/split relations.

- [ ] **Step 5: Verify and commit**

```bash
cargo test analysis::events::clustering -- --nocapture
git add src/analysis/events
git commit -m "feat: cluster evolving market events"
```

---

### Task 3: Add GDELT supplementary adapter

**Files:**
- Create: `src/analysis/adapters/gdelt.rs`
- Modify: `src/analysis/adapters/mod.rs`
- Modify: `src/config.rs`
- Modify: `.env.example`
- Test: `src/analysis/adapters/gdelt.rs`

**Interfaces:**
- Implements `EventSource`.
- Marks all rows as macro/geopolitical supplementary evidence.
- Does not create company facts without another source.

- [ ] **Step 1: Add configuration**

```text
ENABLE_GDELT_EVENTS=false
GDELT_EVENT_QUERY=
GDELT_MAX_RECORDS=250
```

- [ ] **Step 2: Parse a fixture**

Map:

```text
source_item_id
published_at
title
source_url
language
themes
locations
organizations
raw_payload
```

- [ ] **Step 3: Enforce source role**

Set metadata:

```json
{
  "sourceRole": "macro_supplement",
  "companyFactEligible": false
}
```

- [ ] **Step 4: Add cursor and idempotency tests**

Repeated fetches must not duplicate evidence.

- [ ] **Step 5: Verify and commit**

```bash
cargo test analysis::adapters::gdelt
git add src/config.rs .env.example src/analysis/adapters
git commit -m "feat: add GDELT macro event adapter"
```

---

### Task 4: Implement EventDelta

**Files:**
- Create: `src/analysis/events/deltas.rs`
- Test: `src/analysis/events/deltas.rs`

**Interfaces:**
- Produces deterministic deltas between cluster versions.
- Does not use market prices.

- [ ] **Step 1: Define delta payload**

```rust
pub struct EventDelta {
    pub new_claim_ids: Vec<Uuid>,
    pub repeated_claim_ids: Vec<Uuid>,
    pub revised_values: Vec<RevisedValue>,
    pub removed_claim_ids: Vec<Uuid>,
    pub status_changes: Vec<StatusChange>,
    pub expectation_gap: Option<ExpectationGap>,
    pub new_uncertainties: Vec<String>,
    pub resolved_uncertainties: Vec<String>,
}
```

- [ ] **Step 2: Test numeric revisions**

Example:

```text
old order amount: 1 billion
new order amount: 0.8 billion
```

Must produce a revised value, not a second unrelated claim.

- [ ] **Step 3: Implement deterministic comparison**

Use canonical claim IDs, normalized units, entity roles, and dates.

- [ ] **Step 4: Verify and commit**

```bash
cargo test analysis::events::deltas
git add src/analysis/events/deltas.rs
git commit -m "feat: track event information deltas"
```

---

### Task 5: Implement ImpactHypothesisGraph and freeze semantics

**Files:**
- Create: `src/analysis/events/hypotheses.rs`
- Test: `src/analysis/events/hypotheses.rs`

**Interfaces:**
- Produces a hypothesis graph from ClaimGraph and deterministic templates.
- Freezes before market observation.

- [ ] **Step 1: Define graph contracts**

```rust
pub struct ImpactHypothesisGraph {
    pub schema_version: String,
    pub nodes: Vec<HypothesisNode>,
    pub edges: Vec<HypothesisEdge>,
    pub based_on_claim_ids: Vec<Uuid>,
    pub frozen_at: DateTime<Utc>,
}

pub struct HypothesisEdge {
    pub from: String,
    pub to: String,
    pub relation: String,
    pub generation_method: String,
    pub logic_rule_id: Option<String>,
    pub confidence: f64,
    pub assumptions: Vec<String>,
    pub expected_horizon: String,
    pub observable_indicators: Vec<String>,
    pub counter_scenario: Vec<String>,
    pub invalidation_conditions: Vec<String>,
}
```

- [ ] **Step 2: Add deterministic templates**

Required templates:

```text
policy_subsidy_v1
supply_restriction_v1
demand_shock_v1
liquidity_rate_v1
company_order_v1
company_accident_v1
```

- [ ] **Step 3: Enforce freeze**

After `frozen_at`:

- graph payload cannot be updated.
- market observation can only append separate rows.
- new facts create a new hypothesis version.

- [ ] **Step 4: Limit stock scope**

Hypothesis output may name:

- direct company entities.
- industries.
- upstream/downstream archetypes.

It may not generate indirect stock codes.

- [ ] **Step 5: Verify and commit**

```bash
cargo test analysis::events::hypotheses
git add src/analysis/events/hypotheses.rs
git commit -m "feat: freeze evidence-based impact hypotheses"
```

---

### Task 6: Implement market-alignment observation

**Files:**
- Create: `src/analysis/events/market_observation.rs`
- Test: `src/analysis/events/market_observation.rs`

**Interfaces:**
- Produces market observation rows.
- Consumes frozen hypotheses and point-in-time market snapshots.
- Never mutates hypotheses.

- [ ] **Step 1: Define statuses**

```rust
pub enum MarketObservationStatus {
    NotObserved,
    MarketAligned,
    MarketContradicted,
    Ambiguous,
    Confounded,
    Expired,
}
```

- [ ] **Step 2: Test abnormal return**

```text
stock return 5%
market return 2%
industry return 3%
market abnormal = 3%
industry abnormal = 2%
```

- [ ] **Step 3: Add confounder rules**

Mark `Confounded` when the same entity/window contains:

- earnings.
- suspension/resumption.
- regulatory penalty.
- major corporate action.
- another high-importance event.

- [ ] **Step 4: Keep causal confidence separate**

`market_alignment_score` can change by observed prices.

`causal_confidence` is derived from evidence, timing, confounders, and identification quality; it must not increase solely because returns align.

- [ ] **Step 5: Verify and commit**

```bash
cargo test analysis::events::market_observation
git add src/analysis/events/market_observation.rs
git commit -m "feat: observe event market alignment without causal claims"
```

---

### Task 7: Build historical event-type baselines

**Files:**
- Create: `src/analysis/events/event_statistics.rs`
- Test: `src/analysis/events/event_statistics.rs`

**Interfaces:**
- Produces versioned historical statistics.
- Uses only events available before the statistics cutoff.

- [ ] **Step 1: Define aggregation key**

```text
event_type
event_subtype
entity_type
observation_window
data_cutoff
logic_version
```

- [ ] **Step 2: Test point-in-time cutoff**

An event first seen after cutoff must not enter the baseline.

- [ ] **Step 3: Calculate metrics**

```text
sample_count
median_abnormal_return
positive_rate
turnover_response
breadth_response
time_to_peak
failure_rate
```

- [ ] **Step 4: Verify and commit**

```bash
cargo test analysis::events::event_statistics
git add src/analysis/events/event_statistics.rs
git commit -m "feat: add historical event-impact baselines"
```

---

### Task 8: Add event-evolution jobs and reports

**Files:**
- Modify: `src/analysis/events/reporting.rs`
- Modify: `src/analysis/events/mod.rs`
- Modify: `src/scheduler/mod.rs`
- Modify: `src/api/event_routes.rs`
- Modify: `README.md`
- Test: `src/analysis/events/reporting.rs`
- Test: `src/scheduler/mod.rs`

**Interfaces:**
- Adds evolution detail and market-logic brief.
- Keeps event score zero.

- [ ] **Step 1: Add jobs**

```rust
pub async fn run_event_cluster_refinement_job(state: Arc<AppState>);
pub async fn run_event_market_observation_job(state: Arc<AppState>);
```

- [ ] **Step 2: Add report sections**

```text
今日事件增量
已冻结影响假设
市场对齐/矛盾/混杂
观察指标
反向情景
失效条件
同类历史基线
```

- [ ] **Step 3: Add endpoints**

```text
GET /api/analysis/events/:id/evolution
GET /api/analysis/events/:id/hypothesis
GET /api/analysis/events/:id/market-observations
GET /api/analysis/events/market-logic-brief
```

- [ ] **Step 4: Add safety tests**

Assert:

- event score remains zero.
- report labels hypotheses as inference.
- report does not claim market causality.
- indirect stock-code lists are absent.

- [ ] **Step 5: Verify and commit**

```bash
cargo fmt --all -- --check
cargo test --all --locked
git diff --check
git add src README.md
git commit -m "feat: report evolving events and market alignment"
```

---

## Phase Completion Checklist

- [ ] GDELT is supplementary and idempotent.
- [ ] EventMention and EventCluster are versioned.
- [ ] Two-stage clustering respects manual locks.
- [ ] EventDelta highlights new information.
- [ ] Hypotheses freeze before market observation.
- [ ] Market alignment and causal confidence are separate.
- [ ] Confounded windows are explicit.
- [ ] Event-type baselines are point-in-time safe.
- [ ] Event score remains zero.
- [ ] No indirect beneficiary stock list is generated.
