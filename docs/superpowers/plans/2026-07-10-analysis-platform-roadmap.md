# QBot Analysis Platform Roadmap

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the analysis platform through five independently testable phases without coupling research, event intelligence, or decision support to the existing trading loop.

**Architecture:** Build point-in-time market data first, then a shadow-only pattern engine, then an evidence-first event MVP, then event evolution and market observation, and finally a read-only decision-support layer. Rust remains the production runtime; Python research runs as an independent batch worker and exchanges versioned contracts through PostgreSQL and Parquet.

**Tech Stack:** Rust 2021, Axum, SQLx/PostgreSQL, Tokio, Python 3.12, Polars, PyArrow, DuckDB, scikit-learn, Pydantic, Parquet.

## Global Constraints

- Rust must not spawn or supervise the Python research worker.
- New pattern output is shadow-only until explicit release approval.
- Event score is `0` through Phases 2 and 3.
- Any future event adjustment starts at a maximum absolute contribution of `5` points.
- No new analysis output may enter `signal_strategy_candidates`.
- No new model may auto-publish.
- `available_at` determines whether data is usable in historical replay.
- PostgreSQL stores source-of-truth records and recent online state; Parquet stores historical training matrices.
- Existing `scan_ranker` remains the baseline and compatibility adapter.
- Existing `AiAnalysisService` receives no new event or causal-analysis logic.
- Kafka, Flink, and a graph database are out of scope.

---

## Phase Order

1. [Point-in-time data foundation](2026-07-10-point-in-time-data-foundation.md)
2. [Strong-stock pattern shadow engine](2026-07-10-strong-stock-pattern-shadow-engine.md)
3. [Event evidence MVP](2026-07-10-event-evidence-mvp.md)
4. [Event evolution and market alignment](2026-07-10-event-evolution-market-alignment.md)
5. [Decision-support integration](2026-07-10-decision-support-integration.md)

## Release Gates

### Gate 0: Data foundation

Required before pattern or event backtests:

- Append-only daily bar observations.
- Daily market-cap and valuation observations.
- Historical security master and delisted names.
- Adjustment factors and corporate-action-safe returns.
- Daily security status.
- Historical sector membership.
- Persisted index history.
- Market breadth snapshots.
- `available_at` and data-quality metadata.
- Capability and completeness status for every required input.

### Gate 1: Pattern shadow engine

Required before any pattern becomes visible outside a shadow report:

- Versioned Parquet datasets and reproducible manifests.
- Week and month horizons only.
- Purged walk-forward and embargo.
- Matched controls.
- Positive lift in a majority of validation windows.
- Comparison against simple baselines and `scan_ranker`.
- Manual publish gate.
- No write path to auto trading.

### Gate 2: Event evidence MVP

Required before event logic is shown as a trusted daily brief:

- Every published fact has evidence references.
- No invalid JSON Schema result reaches the published layer.
- Official-source timestamps preserve `published_at`, `first_seen_at`, and `available_at`.
- Exact and near-duplicate handling is idempotent.
- ClaimGraph contains no unsupported inference.
- No non-direct beneficiary stock list.

### Gate 3: Event evolution

Required before event context can affect ranking:

- Frozen initial hypotheses.
- EventDelta history.
- Market status uses `market_aligned`, `market_contradicted`, `ambiguous`, `confounded`, or `expired`.
- Abnormal return uses both market and industry benchmarks.
- Causal confidence remains separate from market alignment.
- Historical event-type baselines are point-in-time safe.

### Gate 4: Limited integration

Required before enabling non-zero event contribution:

- Shadow comparison demonstrates incremental value.
- Event adjustment remains within `[-5, 5]`.
- Hard tradability and risk filters remain dominant.
- An event cannot promote Reject directly to A.
- Every score contribution is auditable.

## Operational Model

```text
qbot.service
  Rust API, scheduler, snapshots, online matching, event processing

qbot-research.timer
  invokes qbot-research.service weekly

qbot-research.service
  Python batch CLI; reads PostgreSQL/Parquet and writes draft model versions

PostgreSQL
  source-of-truth, versions, manifests, online results, event evidence

Parquet storage
  historical feature/label/control datasets
```

## Migration Number Allocation

- `013_point_in_time_market_data.sql`
- `014_pattern_research_and_shadow.sql`
- `015_event_evidence_mvp.sql`
- `016_event_evolution.sql`
- `017_decision_support.sql`

Do not reuse migration numbers or combine phases into one migration.

## Verification after every phase

```bash
cargo fmt --all -- --check
cargo test --all --locked
git diff --check
```

Phases containing Python must additionally run:

```bash
cd research
python -m pytest -q
python -m ruff check .
python -m mypy qbot_research
```

Database integration tests require a valid `DATABASE_URL` or SQLx test database support. A phase is not complete if database tests are merely skipped without being reported.

---

### Task 1: Complete Gate 0

**Files:**
- Execute: `docs/superpowers/plans/2026-07-10-point-in-time-data-foundation.md`

**Interfaces:**
- Produces the point-in-time data contracts required by all later phases.
- Later tasks must not start until `/api/analysis/data-status` reports all critical prerequisites available.

- [x] Execute every checkbox in the linked plan.
- [x] Run the Rust verification commands above.
- [x] Confirm no pattern, event, or decision-support candidate table is populated.

### Task 2: Complete Gate 1

**Files:**
- Execute: `docs/superpowers/plans/2026-07-10-strong-stock-pattern-shadow-engine.md`

**Interfaces:**
- Consumes Gate 0 datasets.
- Produces manually published pattern versions and shadow candidates only.

- [ ] Execute every checkbox in the linked plan.
- [ ] Run Rust and Python verification.
- [ ] Confirm `signal_strategy_candidates` contains no shadow-model writes.

### Task 3: Complete Gate 2

**Files:**
- Execute: `docs/superpowers/plans/2026-07-10-event-evidence-mvp.md`

**Interfaces:**
- Consumes Gate 0 time and security contracts.
- Produces evidence-backed facts and ClaimGraph; event score remains zero.

- [ ] Execute every checkbox in the linked plan.
- [ ] Confirm every published fact has evidence.
- [ ] Confirm no indirect beneficiary stock list is produced.

### Task 4: Complete Gate 3

**Files:**
- Execute: `docs/superpowers/plans/2026-07-10-event-evolution-market-alignment.md`

**Interfaces:**
- Consumes Gate 2 evidence and Gate 0 market snapshots.
- Produces frozen hypotheses, event deltas, and market observations.

- [ ] Execute every checkbox in the linked plan.
- [ ] Confirm market alignment never mutates frozen hypotheses.
- [ ] Confirm causal confidence remains separate from market alignment.

### Task 5: Complete Gate 4

**Files:**
- Execute: `docs/superpowers/plans/2026-07-10-decision-support-integration.md`

**Interfaces:**
- Consumes scan-ranker, shadow patterns, event intelligence, and market snapshots.
- Produces read-only decision-support artifacts.

- [ ] Execute every checkbox in the linked plan.
- [ ] Confirm event adjustment defaults to zero and is hard-capped at five.
- [ ] Confirm no new analysis output reaches trading tables.
