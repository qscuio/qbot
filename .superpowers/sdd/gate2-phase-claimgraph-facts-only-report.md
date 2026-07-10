# Gate 2 Phase Report: ClaimGraph Facts Only

## Changed files

- `src/analysis/events/claims.rs`
- `.superpowers/sdd/gate2-phase-claimgraph-facts-only-report.md`

## Tests run

- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::claims -- --nocapture`
- `cargo fmt --all -- --check`
- `git diff --check`

## RED evidence

Command:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::claims -- --nocapture
```

Result:

```text
running 4 tests
test analysis::events::claims::tests::claim_graph_accepts_evidence_backed_nodes_and_edges ... ok
test analysis::events::claims::tests::claim_graph_rejects_edges_without_evidence_ids ... ok
test analysis::events::claims::tests::claim_graph_rejects_nodes_without_evidence_ids ... ok
thread 'analysis::events::claims::tests::claim_graph_rejects_non_fact_node_types' panicked:
called `Result::unwrap_err()` on an `Ok` value: ClaimGraph { ... node_type: "ImpactHypothesis" ... }
test analysis::events::claims::tests::claim_graph_rejects_non_fact_node_types ... FAILED

test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 318 filtered out
```

## GREEN evidence

Command:

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::claims -- --nocapture
```

Result:

```text
running 4 tests
test analysis::events::claims::tests::claim_graph_accepts_evidence_backed_nodes_and_edges ... ok
test analysis::events::claims::tests::claim_graph_rejects_nodes_without_evidence_ids ... ok
test analysis::events::claims::tests::claim_graph_rejects_non_fact_node_types ... ok
test analysis::events::claims::tests::claim_graph_rejects_edges_without_evidence_ids ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 318 filtered out
```

Formatting:

```text
cargo fmt --all -- --check
exit code: 0
```

Diff hygiene:

```text
git diff --check
exit code: 0
```

## Implementation summary

- Added a fact-only node type allowlist in `ClaimGraph::new`.
- Rejected unsupported node types before the existing evidence checks.
- Left public struct shapes unchanged.
- Left node and edge evidence enforcement unchanged.

## Commit hash

- `93abd9e`

## Concerns

- The requested input file `.superpowers/sdd/gate2-phase-claimgraph-facts-only-brief.md` was not present in the worktree. I used `.superpowers/sdd/gate2-task-7-brief.md`, the target module, and the ClaimGraph facts-only spec in `docs/superpowers/specs/2026-07-10-market-event-reasoning-design.md`.
