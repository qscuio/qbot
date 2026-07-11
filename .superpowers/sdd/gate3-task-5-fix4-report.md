# Gate 3 Task 5 Fix 4 Report

## Findings fixed

- Replaced the public frozen-hypothesis evolve path with an `EventDelta`-backed API that derives added claim IDs only from `EventDelta::new_claim_ids`, unions them with the prior frozen claim set, and rejects non-additive deltas for hypothesis versioning.
- Rejected frozen-hypothesis evolution when the delta contains removed claim IDs, revised values, status changes, expectation gaps, or no new claim IDs, while preserving the existing strict prior-graph payload preservation gate.
- Expanded stock-code-only/list detection to treat comma-, slash-, and semicolon-separated stock codes as lists before sanitization, so linked target classification and company fallback/source-subject handling both reject them.
- Added regressions for EventDelta-backed evolution success and failure cases, including new-fact success plus removed/revised/status/expectation-only/no-new-fact rejection, preserved-payload mutation rejection, and punctuation-separated stock-code-list rejection.

## Files changed

- `src/analysis/events/hypotheses.rs`
- `.superpowers/sdd/gate3-task-5-fix4-report.md`

## Commit hash

- Code fix commit: `abcb330`

## Commands run

### RED

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses::tests::frozen_hypotheses_require_new_version_when_new_facts_arrive -- --nocapture
```

- Exit code: `101`
- Summary: failed as expected because the tests required an `EventDelta`-backed evolve API while `FrozenImpactHypothesis::evolve` still accepted arbitrary `Vec<Uuid>` claim IDs.

### Final verification

```bash
DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test analysis::events::hypotheses -- --nocapture
```

- Exit code: `0`
- Summary: `19 passed; 0 failed; 355 filtered out`

```bash
cargo fmt --all -- --check
```

- Exit code: `0`

```bash
git diff --check
```

- Exit code: `0`

## Concerns

- The report records the code-fix commit hash (`abcb330`). The final branch tip also includes this report in a follow-up commit because a report file cannot self-embed the hash of the commit that introduces that same file.
