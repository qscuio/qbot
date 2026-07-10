# Gate 1 Task 3 Report: Export Point-in-Time Datasets to Parquet

## Status

DONE_WITH_CONCERNS

## Implementation Summary

Implemented the Gate 1 Task 3 dataset export path under `research/`:

- Added `research/qbot_research/datasets.py`
  - public `build_dataset(horizon, as_of, output_dir) -> DatasetManifest`
  - DuckDB-backed internal `build_dataset_for_connection(...)` for fixture tests
  - point-in-time query logic that selects only rows with `available_at <= available_at_cutoff`
  - one-row-per-`(trade_date, code)` dataset shape
  - adjusted OHLC, amount, turnover, security status, historical sector membership, index returns, market breadth, and `available_at_cutoff`
  - exclusion accounting for missing critical adjustment/status rows
  - partitioned Parquet output by `dataset_version`, `horizon`, and `year`
  - SHA-256 checksums for each Parquet file
  - `manifest.json` writing
  - manifest registration helpers for DuckDB tests and PostgreSQL production use
- Updated `research/qbot_research/cli.py`
  - kept `train-all` as scaffold only
  - added `build-dataset --horizon ... --as-of YYYY-MM-DD --output-dir ...`
  - added CLI validation for `--horizon` and `--as-of`
- Added `research/tests/test_datasets.py`
  - fixture-backed DuckDB temp tables
  - registration-order verification with a recording stub
  - CLI command wiring coverage

## TDD RED/GREEN Evidence

### RED

Wrote `research/tests/test_datasets.py` before creating `research/qbot_research/datasets.py`.

Ran:

```bash
cd research
.venv/bin/python -m pytest tests/test_datasets.py -q
```

Observed expected failure:

- `ModuleNotFoundError: No module named 'qbot_research.datasets'`

This confirmed the new dataset module and command path did not exist yet.

### GREEN

After implementing the dataset module and CLI command, reran:

```bash
cd research
.venv/bin/python -m pytest tests/test_datasets.py -q
```

Result:

- `3 passed in 0.84s`

The dataset tests now prove:

- a bar version available after cutoff is excluded
- sector membership is not used before `valid_from`
- a delisted stock remains in historical rows
- missing adjustment/status rows are excluded and counted
- registration happens after Parquet files and `manifest.json` exist

## Commands Run and Results

### Initial RED run

```bash
cd research
.venv/bin/python -m pytest tests/test_datasets.py -q
```

Result:

- failed during collection with `ModuleNotFoundError: No module named 'qbot_research.datasets'`

### Focused GREEN run

```bash
cd research
.venv/bin/python -m pytest tests/test_datasets.py -q
```

Result:

- `3 passed in 0.84s`

### Required verification

```bash
cd research
.venv/bin/python -m pytest tests/test_datasets.py -q
.venv/bin/python -m ruff check qbot_research/datasets.py
.venv/bin/python -m mypy qbot_research
cd ..
git diff --check
```

Results:

- `pytest tests/test_datasets.py -q`: `3 passed in 0.84s`
- `ruff check qbot_research/datasets.py`: `All checks passed!`
- `mypy qbot_research`: `Success: no issues found in 4 source files`
- `git diff --check`: clean

### Additional verification

```bash
cd research
.venv/bin/python -m pytest -q
```

Result:

- `11 passed in 0.79s`

## Files Changed

- `research/qbot_research/datasets.py`
- `research/qbot_research/cli.py`
- `research/tests/test_datasets.py`
- `.superpowers/sdd/gate1-task-3-report.md`

## Self-Review

- Scope stayed within dataset export only.
- No labels, controls, archetypes, validation, model export, scheduler, reporting, or Rust integration were added.
- No trading tables, pattern-version tables, or shadow-candidate tables are written here.
- The dataset builder uses the latest version per source row subject to `available_at <= cutoff`.
- Sector membership is constrained by both `available_at` and historical `valid_from` / `valid_to`.
- Missing adjustment/status rows are excluded from the final dataset and counted in the manifest payload.
- Parquet output is partitioned under:
  - `dataset_version=<...>/horizon=<...>/year=<...>/part-000.parquet`
- Checksums are computed before registration.
- Registration runs only after all output files and `manifest.json` are present.
- Production registration errors are not swallowed; they propagate.

## Concerns

1. The tested path uses DuckDB temp tables plus a DuckDB registration target, which is what the brief asked for. The public production path that attaches PostgreSQL through DuckDB and registers through psycopg is implemented and type-checked, but it was not exercised against a live PostgreSQL dataset in this task.
2. The public path currently relies on DuckDB's PostgreSQL extension being available at runtime (`INSTALL/LOAD postgres`). If that extension is unavailable in the deployment environment, `build_dataset(...)` will fail fast instead of degrading silently.

## Review fix

### RED evidence

Added review-fix tests in `research/tests/test_datasets.py` before changing `research/qbot_research/datasets.py`.

Ran:

```bash
cd research
.venv/bin/python -m pytest tests/test_datasets.py -q
```

Observed the expected blocking failures on the pre-fix implementation:

- `test_build_dataset_input_fingerprint_changes_when_dataset_inputs_change`
  - failed because changing `stock_adjustment_factors.adj_factor` changed the dataset content but did not change `manifest.input_fingerprint`
- `test_build_dataset_public_path_uses_staged_source_loader`
  - failed because public `build_dataset(...)` still called the legacy DuckDB PostgreSQL attach path instead of a testable staged-source loader

The new schema test also ran in this RED cycle and passed against the pre-fix implementation, which confirmed the missing coverage was the review gap rather than a current schema defect.

### GREEN evidence

Implemented the review fixes in `research/qbot_research/datasets.py`:

- public `build_dataset(...)` now stages PostgreSQL source tables into in-memory DuckDB through `psycopg`, avoiding runtime `duckdb INSTALL/LOAD postgres`
- the public path is exercised by test through injected source staging and injected registration without requiring PostgreSQL or DuckDB extensions
- `input_fingerprint` now hashes deterministic row-level dataset inputs, including bar values, adjustment factors, security status, sector membership, index and breadth values, snapshot fingerprints, and exclusion reasons
- frame and Parquet schema coverage now asserts the required persisted columns end to end

Ran:

```bash
cd research
.venv/bin/python -m pytest tests/test_datasets.py -q
.venv/bin/python -m ruff check qbot_research/datasets.py
.venv/bin/python -m mypy qbot_research
cd ..
git diff --check
```

Results:

- `pytest tests/test_datasets.py -q`: `6 passed in 1.29s`
- `ruff check qbot_research/datasets.py`: `All checks passed!`
- `mypy qbot_research`: `Success: no issues found in 4 source files`
- `git diff --check`: clean

### Files changed

- `research/qbot_research/datasets.py`
- `research/tests/test_datasets.py`
- `.superpowers/sdd/gate1-task-3-report.md`

### Concerns

- The public production path is now unit-tested without DuckDB PostgreSQL extensions, but this pass still did not run against a live PostgreSQL instance.
