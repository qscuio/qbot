# Event Evidence MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an evidence-first event MVP with manual input, one official source, exact/near-duplicate handling, direct entity linking, ClaimGraph, and a daily fact brief.

**Architecture:** Add a deep `EventIntelligence` module with a small public interface. Persist immutable evidence and claim versions in PostgreSQL, split event HTTP routes out of the existing large routes file, and keep all event score contribution at zero.

**Tech Stack:** Rust 2021, Axum, SQLx/PostgreSQL, Reqwest, Serde JSON Schema validation, existing OpenAI-compatible client.

## Global Constraints

- Phase 0 must be complete.
- Event contribution to stock ranking is exactly `0`.
- No GDELT in this phase.
- No complex cross-source EventCluster in this phase.
- No ImpactHypothesisGraph in this phase.
- No non-direct beneficiary stock list.
- Every published fact references evidence.
- Official-source content retention must be configurable.
- Migration number is `015`.

---

### Task 1: Add event evidence MVP schema

**Files:**
- Create: `migrations/015_event_evidence_mvp.sql`
- Create: `src/storage/event_repository.rs`
- Modify: `src/storage/mod.rs`
- Test: `src/storage/event_repository.rs`

**Interfaces:**
- Produces immutable evidence, duplicate groups, extraction, claims, entities, ClaimGraph, revision, and brief tables.

- [x] **Step 1: Create the migration**

```sql
CREATE TABLE market_event_evidence (
    evidence_id             UUID PRIMARY KEY,
    source_id               VARCHAR(80) NOT NULL,
    source_item_id          VARCHAR(200) NOT NULL,
    source_url              TEXT,
    source_tier             VARCHAR(10) NOT NULL,
    source_terms_version    VARCHAR(80) NOT NULL,
    occurred_at             TIMESTAMPTZ,
    published_at            TIMESTAMPTZ,
    first_seen_at           TIMESTAMPTZ NOT NULL,
    available_at            TIMESTAMPTZ NOT NULL,
    effective_trade_date    DATE NOT NULL,
    title                   TEXT NOT NULL,
    content                 TEXT,
    language                VARCHAR(20) NOT NULL,
    content_hash            VARCHAR(64) NOT NULL,
    raw_payload             JSONB NOT NULL DEFAULT '{}',
    version                 INT NOT NULL,
    supersedes_evidence_id  UUID REFERENCES market_event_evidence(evidence_id),
    status                  VARCHAR(20) NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_id, source_item_id, version)
);

CREATE TABLE market_event_duplicate_groups (
    duplicate_group_id UUID PRIMARY KEY,
    relation_type      VARCHAR(20) NOT NULL,
    confidence         NUMERIC(8,6) NOT NULL,
    locked_by_user     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE market_event_duplicate_members (
    duplicate_group_id UUID NOT NULL REFERENCES market_event_duplicate_groups(duplicate_group_id),
    evidence_id        UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    is_representative  BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (duplicate_group_id, evidence_id)
);

CREATE TABLE market_event_extractions (
    extraction_id       UUID PRIMARY KEY,
    evidence_id         UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    schema_version      VARCHAR(32) NOT NULL,
    prompt_version      VARCHAR(32),
    model_name          VARCHAR(100),
    model_parameters    JSONB NOT NULL DEFAULT '{}',
    extracted_payload   JSONB NOT NULL,
    validation_status   VARCHAR(20) NOT NULL,
    validation_errors   JSONB NOT NULL DEFAULT '[]',
    input_fingerprint   VARCHAR(64) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE market_event_claims (
    claim_id           UUID PRIMARY KEY,
    extraction_id      UUID NOT NULL REFERENCES market_event_extractions(extraction_id),
    claim_type         VARCHAR(40) NOT NULL,
    claim_text         TEXT NOT NULL,
    confidence         NUMERIC(8,6) NOT NULL,
    review_status      VARCHAR(20) NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE market_event_claim_evidence (
    claim_id     UUID NOT NULL REFERENCES market_event_claims(claim_id),
    evidence_id  UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    PRIMARY KEY (claim_id, evidence_id)
);

CREATE TABLE market_event_entities (
    entity_link_id     UUID PRIMARY KEY,
    evidence_id        UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    raw_name           TEXT NOT NULL,
    canonical_type     VARCHAR(40) NOT NULL,
    canonical_id       VARCHAR(100),
    role               VARCHAR(50) NOT NULL,
    match_method       VARCHAR(30) NOT NULL,
    confidence         NUMERIC(8,6) NOT NULL,
    review_status      VARCHAR(20) NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE market_event_claim_graphs (
    claim_graph_id      UUID PRIMARY KEY,
    evidence_id         UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    graph_version       INT NOT NULL,
    schema_version      VARCHAR(32) NOT NULL,
    graph_payload       JSONB NOT NULL,
    review_status       VARCHAR(20) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (evidence_id, graph_version)
);

CREATE TABLE market_event_revisions (
    revision_id         UUID PRIMARY KEY,
    object_type         VARCHAR(40) NOT NULL,
    object_id           UUID NOT NULL,
    previous_payload    JSONB NOT NULL,
    revised_payload     JSONB NOT NULL,
    revised_by          VARCHAR(100) NOT NULL,
    reason              TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE market_event_daily_briefs (
    trade_date          DATE PRIMARY KEY,
    brief_version       VARCHAR(32) NOT NULL,
    content             TEXT NOT NULL,
    structured_payload  JSONB NOT NULL,
    input_fingerprint   VARCHAR(64) NOT NULL,
    generated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_event_evidence_available
    ON market_event_evidence(available_at, effective_trade_date, status);

CREATE INDEX idx_event_claims_review
    ON market_event_claims(review_status, created_at DESC);

CREATE INDEX idx_event_entities_canonical
    ON market_event_entities(canonical_type, canonical_id, review_status);
```

- [x] **Step 2: Add SQLx tests**

Verify:

- evidence versions are append-only.
- every published claim can be joined to at least one evidence row.
- user-locked duplicate groups remain locked after upsert.
- daily brief upserts by trade date.

- [x] **Step 3: Implement `EventRepository`**

Public methods:

```rust
pub async fn insert_evidence(&self, row: &EventEvidenceRow) -> Result<Uuid>;
pub async fn find_existing_source_item(&self, source_id: &str, source_item_id: &str) -> Result<Vec<EventEvidenceRow>>;
pub async fn find_by_content_hash(&self, hash: &str) -> Result<Vec<EventEvidenceRow>>;
pub async fn save_duplicate_group(&self, group: &DuplicateGroupRow) -> Result<Uuid>;
pub async fn save_extraction(&self, extraction: &ExtractionRow) -> Result<Uuid>;
pub async fn save_claim_graph(&self, graph: &ClaimGraphRow) -> Result<Uuid>;
pub async fn save_daily_brief(&self, brief: &DailyEventBriefRow) -> Result<()>;
pub async fn list_publishable_evidence(&self, trade_date: NaiveDate) -> Result<Vec<EventEvidenceRow>>;
```

- [x] **Step 4: Verify and commit**

```bash
cargo test storage::event_repository -- --nocapture
git add migrations/015_event_evidence_mvp.sql src/storage
git commit -m "feat: add event evidence MVP schema"
```

---

### Task 2: Add event contracts and deep module interface

**Files:**
- Create: `src/analysis/events/mod.rs`
- Create: `src/analysis/events/contracts.rs`
- Modify: `src/analysis/mod.rs`
- Test: `src/analysis/events/contracts.rs`

**Interfaces:**
- Produces the public `EventIntelligence` types.
- Keeps internal dedup/extraction details private.

- [x] **Step 1: Define contracts**

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ManualEventInput {
    pub title: String,
    pub content: Option<String>,
    pub source_url: Option<String>,
    pub submitted_by: String,
    pub published_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EventEvidence {
    pub evidence_id: uuid::Uuid,
    pub source_id: String,
    pub source_item_id: String,
    pub source_tier: String,
    pub published_at: Option<chrono::DateTime<chrono::Utc>>,
    pub first_seen_at: chrono::DateTime<chrono::Utc>,
    pub available_at: chrono::DateTime<chrono::Utc>,
    pub effective_trade_date: chrono::NaiveDate,
    pub title: String,
    pub content_hash: String,
    pub status: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DailyEventBrief {
    pub trade_date: chrono::NaiveDate,
    pub new_facts: Vec<BriefFact>,
    pub revisions: Vec<BriefRevision>,
    pub unconfirmed: Vec<BriefUnconfirmed>,
    pub direct_entities: Vec<BriefEntity>,
    pub sources: Vec<BriefSource>,
    pub input_fingerprint: String,
}

pub trait TradingDateResolver: Send + Sync {
    fn effective_trade_date(
        &self,
        available_at: chrono::DateTime<chrono::Utc>,
    ) -> crate::error::Result<chrono::NaiveDate>;
}
```

- [x] **Step 2: Test time classification**

Tests must cover:

- 14:30 on a trading day maps to the same date.
- 15:30 maps to the next trading date.
- Saturday maps to Monday or the next open date.

- [x] **Step 3: Add public module interface**

```rust
pub struct EventIntelligence {
    repo: EventRepository,
    resolver: Arc<dyn TradingDateResolver>,
    extractor: Arc<dyn EventExtractor>,
}

impl EventIntelligence {
    pub async fn submit_manual_event(
        &self,
        input: ManualEventInput,
    ) -> Result<EventEvidence>;

    pub async fn process_pending(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<EventProcessingSummary>;

    pub async fn build_daily_brief(
        &self,
        trade_date: NaiveDate,
    ) -> Result<DailyEventBrief>;
}
```

- [x] **Step 4: Verify and commit**

```bash
cargo test analysis::events::contracts
git add src/analysis/events src/analysis/mod.rs
git commit -m "feat: add event intelligence contracts"
```

---

### Task 3: Implement manual event ingestion

**Files:**
- Create: `src/analysis/events/evidence.rs`
- Create: `src/analysis/events/time.rs`
- Modify: `src/analysis/events/mod.rs`
- Test: `src/analysis/events/evidence.rs`

**Interfaces:**
- Produces normalized immutable evidence from manual input.
- Uses trading calendar to calculate effective trade date.

- [x] **Step 1: Write normalization tests**

Assert:

- whitespace is normalized.
- source URL is canonicalized.
- content hash is stable.
- repeated submission returns the existing evidence relation instead of creating silent duplicates.

- [x] **Step 2: Implement evidence normalization**

```rust
fn normalize_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn content_hash(title: &str, content: Option<&str>) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(normalize_text(title));
    if let Some(content) = content {
        hasher.update([0]);
        hasher.update(normalize_text(content));
    }
    format!("{:x}", hasher.finalize())
}
```

Add `sha2 = "0.10"` to `Cargo.toml`.

- [x] **Step 3: Implement immutable insertion**

Manual source ID:

```text
manual:telegram
manual:rest
```

Source item ID is a UUID generated at ingestion; repeated content is linked through duplicate handling rather than reusing the ID.

- [x] **Step 4: Verify and commit**

```bash
cargo test analysis::events::evidence
git add Cargo.toml Cargo.lock src/analysis/events
git commit -m "feat: ingest manual market-event evidence"
```

---

### Task 4: Add exact and conservative near-duplicate handling

**Files:**
- Create: `src/analysis/events/dedup.rs`
- Test: `src/analysis/events/dedup.rs`

**Interfaces:**
- Produces `DuplicateDecision`.
- Does not implement EventCluster.

- [ ] **Step 1: Define decisions**

```rust
pub enum DuplicateDecision {
    Exact { representative_id: Uuid },
    NearDuplicate { representative_id: Uuid, confidence: f64 },
    Independent,
    ReviewRequired { candidate_ids: Vec<Uuid> },
}
```

- [ ] **Step 2: Test exact duplicate rules**

Exact when:

- same source ID and source item ID/version.
- canonical URL match.
- content hash match.

- [ ] **Step 3: Implement conservative near duplicate**

Use title token Jaccard plus normalized content prefix similarity. Automatic near-duplicate requires a configured threshold at least `0.92`; otherwise return `ReviewRequired`.

Do not use an LLM for duplicate decisions in Phase 2.

- [ ] **Step 4: Test locked relations**

A user-locked `Independent` or duplicate relation cannot be overwritten by reprocessing.

- [ ] **Step 5: Verify and commit**

```bash
cargo test analysis::events::dedup
git add src/analysis/events
git commit -m "feat: deduplicate event evidence conservatively"
```

---

### Task 5: Add one official-source adapter

**Files:**
- Create: `src/analysis/adapters/mod.rs`
- Create: `src/analysis/adapters/official_event_source.rs`
- Modify: `src/analysis/mod.rs`
- Modify: `src/config.rs`
- Modify: `.env.example`
- Test: `src/analysis/adapters/official_event_source.rs`

**Interfaces:**
- Produces `EventSource`.
- The selected source configuration is environment-driven.
- Content retention policy is explicit.

- [ ] **Step 1: Define `EventSource`**

```rust
#[async_trait::async_trait]
pub trait EventSource: Send + Sync {
    fn source_id(&self) -> &'static str;
    fn retention_policy(&self) -> ContentRetentionPolicy;
    async fn fetch(
        &self,
        cursor: Option<String>,
        until: DateTime<Utc>,
    ) -> Result<FetchBatch>;
}
```

- [ ] **Step 2: Add configuration**

```rust
pub official_event_feed_url: Option<String>,
pub official_event_feed_api_key: Option<String>,
pub official_event_source_id: String,
pub official_event_store_full_content: bool,
```

Environment names:

```text
OFFICIAL_EVENT_FEED_URL
OFFICIAL_EVENT_FEED_API_KEY
OFFICIAL_EVENT_SOURCE_ID
OFFICIAL_EVENT_STORE_FULL_CONTENT
```

- [ ] **Step 3: Implement adapter parsing**

The adapter must map:

```text
source_item_id
published_at
title
content or permitted summary
source_url
raw_payload
```

When full content retention is disabled, discard full content before persistence.

- [ ] **Step 4: Add fixture tests**

No live network calls in tests. Parse a local JSON fixture and assert retention behavior.

- [ ] **Step 5: Verify and commit**

```bash
cargo test analysis::adapters::official_event_source
git add src/config.rs .env.example src/analysis/adapters
git commit -m "feat: add official market-event source adapter"
```

---

### Task 6: Add strict extraction Schema and adapter

**Files:**
- Create: `src/analysis/events/extraction.rs`
- Create: `src/analysis/adapters/llm_event_extractor.rs`
- Create: `tests/fixtures/event_extraction_v1.json`
- Test: `src/analysis/events/extraction.rs`

**Interfaces:**
- Produces `EventExtractor`.
- Outputs candidate claims only.
- Invalid Schema never reaches the published layer.

- [ ] **Step 1: Define strict Rust Schema**

Use `#[serde(deny_unknown_fields)]`:

```rust
pub struct EventExtractionV1 {
    pub event_type: String,
    pub event_subtype: Option<String>,
    pub claims: Vec<ExtractedClaim>,
    pub entities: Vec<ExtractedEntity>,
    pub amounts: Vec<ExtractedAmount>,
    pub dates: Vec<ExtractedDate>,
    pub uncertainties: Vec<String>,
    pub missing_information: Vec<String>,
}

pub struct ExtractedClaim {
    pub claim_type: ClaimType,
    pub text: String,
    pub evidence_ids: Vec<Uuid>,
    pub confidence: f64,
}
```

`ClaimType`:

```text
fact
direct_quote
third_party_claim
journalist_interpretation
rumor
unknown
```

- [ ] **Step 2: Add Schema tests**

Assert:

- a `fact` without evidence IDs fails.
- confidence outside `[0,1]` fails validation.
- unknown JSON fields fail.
- a fixture round-trips.

- [ ] **Step 3: Implement the LLM adapter**

Reuse OpenAI-compatible HTTP settings but use a dedicated system prompt and temperature `0`.

The adapter must:

1. Ask for JSON only.
2. Parse once.
3. Retry once with a repair prompt.
4. Return a validation error after the second failure.
5. Save prompt version and model parameters.

- [ ] **Step 4: Add deterministic validation**

Validate:

- every evidence ID belongs to the extraction input.
- date and amount values appear in source content.
- direct stock codes map to known `stock_info`.
- rumor/opinion claims cannot be promoted to facts.

- [ ] **Step 5: Verify and commit**

```bash
cargo test analysis::events::extraction -- --nocapture
git add src/analysis tests/fixtures
git commit -m "feat: extract evidence-backed market-event claims"
```

---

### Task 7: Implement direct entity linking and ClaimGraph

**Files:**
- Create: `src/analysis/events/entity_linking.rs`
- Create: `src/analysis/events/claims.rs`
- Test: `src/analysis/events/entity_linking.rs`
- Test: `src/analysis/events/claims.rs`

**Interfaces:**
- Produces direct `EntityLink` rows.
- Produces ClaimGraph with evidence-backed nodes and edges.
- No industry-chain beneficiary expansion.

- [ ] **Step 1: Test direct entity linking**

Fixture cases:

- exact stock code.
- exact company name.
- known alias.
- ambiguous short name returns review required.
- unknown organization remains unmapped.

- [ ] **Step 2: Implement link priority**

```text
explicit security code
exact legal name
reviewed alias
exact official industry name
otherwise unresolved
```

- [ ] **Step 3: Define ClaimGraph payload**

```rust
pub struct ClaimGraph {
    pub schema_version: String,
    pub nodes: Vec<ClaimNode>,
    pub edges: Vec<ClaimEdge>,
}

pub struct ClaimNode {
    pub node_id: String,
    pub node_type: String,
    pub label: String,
    pub evidence_ids: Vec<Uuid>,
    pub confidence: f64,
}

pub struct ClaimEdge {
    pub from: String,
    pub to: String,
    pub relation: String,
    pub evidence_ids: Vec<Uuid>,
    pub confidence: f64,
}
```

- [ ] **Step 4: Enforce evidence**

Graph construction returns an error if any node or edge has no evidence IDs.

- [ ] **Step 5: Verify and commit**

```bash
cargo test analysis::events::entity_linking analysis::events::claims
git add src/analysis/events
git commit -m "feat: build evidence-backed event claim graphs"
```

---

### Task 8: Add event API and Telegram commands

**Files:**
- Create: `src/api/event_routes.rs`
- Modify: `src/api/mod.rs`
- Modify: `src/api/routes.rs`
- Modify: `src/main.rs`
- Test: `src/api/event_routes.rs`

**Interfaces:**
- Adds manual submission and read endpoints.
- Keeps event handlers outside the existing large route file.

- [ ] **Step 1: Add endpoints**

```text
POST /api/analysis/events/manual
GET  /api/analysis/events
GET  /api/analysis/events/:id
POST /api/analysis/events/:id/review
GET  /api/analysis/events/daily-brief
```

- [ ] **Step 2: Implement manual submission response**

```json
{
  "evidenceId": "...",
  "duplicateStatus": "independent",
  "processingStatus": "collected",
  "effectiveTradeDate": "2026-07-13"
}
```

- [ ] **Step 3: Add Telegram commands**

```text
/event
/events
/event_detail
/event_review
/market_facts
```

Only command parsing remains in `routes.rs`; event business logic stays in `EventIntelligence`.

- [ ] **Step 4: Test auth and validation**

Reject:

- empty title and content.
- malformed URL.
- unauthorized review action.
- invalid evidence ID.

- [ ] **Step 5: Verify and commit**

```bash
cargo test api::event_routes -- --nocapture
git add src/api src/main.rs
git commit -m "feat: expose market-event evidence workflows"
```

---

### Task 9: Build and schedule the daily fact brief

**Files:**
- Create: `src/analysis/events/reporting.rs`
- Modify: `src/analysis/events/mod.rs`
- Modify: `src/scheduler/mod.rs`
- Modify: `README.md`
- Test: `src/analysis/events/reporting.rs`
- Test: `src/scheduler/mod.rs`

**Interfaces:**
- Produces `DailyEventBrief`.
- Pushes facts only.
- Event score remains zero.

- [ ] **Step 1: Write report golden tests**

Output sections:

```text
今日新增事实
今日修订
未确认内容
直接涉及公司与行业
来源
```

Assert every fact includes at least one source reference.

- [ ] **Step 2: Implement report builder**

Do not ask an LLM to compose the fact brief. Render from structured claims and sources. LLM use is limited to extraction.

- [ ] **Step 3: Add jobs**

```rust
pub async fn run_event_ingestion_job(state: Arc<AppState>);
pub async fn run_event_fact_brief_job(state: Arc<AppState>);
```

Suggested cron:

```text
event ingestion: hourly during configured hours
fact brief: 17:50 trading days
```

The hourly job must use provider cursor state and be idempotent.

- [ ] **Step 4: Add failure isolation**

A failed event source or extraction must not fail the existing daily market report.

- [ ] **Step 5: Verify and commit**

```bash
cargo fmt --all -- --check
cargo test --all --locked
git diff --check
git add src README.md
git commit -m "feat: publish daily evidence-backed market facts"
```

---

## Phase Completion Checklist

- [ ] Manual input works.
- [ ] One official source works through an adapter.
- [ ] Retention policy is enforced.
- [ ] Evidence versions are immutable.
- [ ] Exact and near duplicates are conservative and auditable.
- [ ] Every published fact has evidence.
- [ ] Direct entity mapping does not guess ambiguous stocks.
- [ ] ClaimGraph contains facts only.
- [ ] No GDELT or complex clustering exists yet.
- [ ] No event score reaches candidate ranking.
- [ ] No non-direct beneficiary stock list is generated.
