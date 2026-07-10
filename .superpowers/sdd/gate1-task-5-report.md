# Gate 1 Task 5 Report: Implement Archetype Discovery

## Implementation Summary

- Added `research/qbot_research/archetypes.py`.
- Added pure Polars expression gates:
  - `trend_family(frame)`
  - `vcp_breakout_family(frame)`
  - `oversold_reversal_family(frame)`
- Added `ArchetypeDiscoveryConfig` and `discover_archetypes(train_frame, pattern_type, config)`.
- Discovery uses only the provided `train_frame`; it does not read files, databases, environment variables, validation frames, or future windows.
- Discovery filters candidate rows by the requested pattern-family gate and does not force assignment for rows outside all families.
- K-Means and GMM are both fitted on candidate family rows with the configured random seed.
- Returned archetype payloads are JSON/Pydantic-friendly primitives and include:
  - scaler mean and scale parameters
  - K-Means centroids or GMM mixture parameters
  - silhouette scores and GMM BIC
  - random seed
  - high-contribution features
- Tiny clusters and unstable model fits are rejected explicitly through rejection records.
- Missing required columns fail clearly with `ValueError`.
- No validation, baselines, export, Rust matching, scheduler, reporting integration, event features, publishing, or `signal_strategy_candidates` changes were added.

## TDD Evidence

### RED

Command:

```bash
cd research && .venv/bin/python -m pytest tests/test_archetypes.py -q
```

Result before implementation:

```text
ModuleNotFoundError: No module named 'qbot_research.archetypes'
1 error in 0.49s
```

### GREEN

Command:

```bash
cd research && .venv/bin/python -m pytest tests/test_archetypes.py -q
```

Result after implementation:

```text
6 passed in 1.59s
```

## Verification Commands

```bash
cd research && .venv/bin/python -m pytest tests/test_archetypes.py -q
```

Result:

```text
6 passed in 1.59s
```

```bash
cd research && .venv/bin/python -m ruff check qbot_research/archetypes.py
```

Result:

```text
All checks passed!
```

```bash
cd research && .venv/bin/python -m mypy qbot_research
```

Result:

```text
Success: no issues found in 7 source files
```

```bash
git diff --check
```

Result: passed with no output.

Supplemental staged check:

```bash
git diff --cached --check
```

Result: passed with no output.

## Files Changed

- `research/qbot_research/archetypes.py`
- `research/tests/test_archetypes.py`
- `.superpowers/sdd/gate1-task-5-report.md`

## Self-Review

- Confirmed family masks leave non-matching samples unclassified.
- Confirmed discovery is deterministic for identical inputs and config.
- Confirmed accepted archetypes contain only JSON-serializable primitives.
- Confirmed tiny cluster rejection is explicit and prevents archetype output.
- Confirmed unstable model rejection is explicit and prevents archetype output.
- Confirmed missing required columns raise `ValueError`.
- Confirmed no output path, database, environment, validation, publishing, Rust, or baseline code was introduced.

## Concerns

None.

## Review Fix Section

### Findings Fixed

- Critical: overlarge cluster counts are now recorded as `unsupported_cluster_count` rejections for K-Means and GMM, then discovery continues to later cluster counts.
- Critical: fitted labels are now checked before silhouette/BIC/stability metrics. Degenerate label sets are recorded as `degenerate_labels`, and tiny clusters are recorded before metrics are computed or archetypes are exported.
- Important: `stability_iterations=1` now fails config validation with `ValueError` instead of returning a synthetic stability score of `1.0`.
- Minor: discovery tests now exercise real `vcp_breakout` and `oversold_reversal` family gates and verify nonmatching rows are not counted as candidates.

### RED Evidence

Command:

```bash
cd research && .venv/bin/python -m pytest tests/test_archetypes.py -q
```

Result before production fixes:

```text
....FFF....
FAILED tests/test_archetypes.py::test_discover_archetypes_rejects_overlarge_cluster_counts_and_continues
ValueError: cluster_counts must be smaller than the candidate family row count for silhouette scoring

FAILED tests/test_archetypes.py::test_discover_archetypes_rejects_degenerate_fits_before_metrics
AssertionError: silhouette must not be computed for degenerate labels

FAILED tests/test_archetypes.py::test_discover_archetypes_requires_multiple_stability_iterations
Failed: DID NOT RAISE <class 'ValueError'>

3 failed, 8 passed in 4.80s
```

### GREEN Verification

Command:

```bash
cd research && .venv/bin/python -m pytest tests/test_archetypes.py -q
```

Result:

```text
11 passed in 1.99s
```

Command:

```bash
cd research && .venv/bin/python -m ruff check qbot_research/archetypes.py tests/test_archetypes.py
```

Result:

```text
All checks passed!
```

Command:

```bash
cd research && .venv/bin/python -m mypy qbot_research
```

Result:

```text
Success: no issues found in 7 source files
```

Command:

```bash
git diff --check
```

Result: passed with no output.

### Files Changed

- `research/qbot_research/archetypes.py`
- `research/tests/test_archetypes.py`
- `.superpowers/sdd/gate1-task-5-report.md`

### Concerns

None.
