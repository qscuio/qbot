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

## Second review fix

### RED evidence

Added failing review-fix tests in `research/tests/test_datasets.py` before changing the implementation again.

Ran:

```bash
cd research
.venv/bin/python -m pytest tests/test_datasets.py -q
```

Observed the expected blocking failures on the pre-fix implementation:

- `test_build_dataset_input_fingerprint_changes_when_security_master_inputs_change[name-'Alpha Prime']`
  - failed because changing `security_master_versions.name` did not change `manifest.input_fingerprint`
- `test_build_dataset_input_fingerprint_changes_when_security_master_inputs_change[available_at-?]`
  - failed because changing only `security_master_versions.available_at` did not change `manifest.input_fingerprint`
- `test_build_dataset_for_connection_rejects_non_publishable_horizons_before_writes_or_registration[quarter]`
- `test_build_dataset_for_connection_rejects_non_publishable_horizons_before_writes_or_registration[year]`
  - both failed because `build_dataset_for_connection(...)` still wrote publish outputs for `quarter` and `year`
- `test_build_dataset_rejects_non_publishable_horizons_before_staging[quarter]`
- `test_build_dataset_rejects_non_publishable_horizons_before_staging[year]`
  - both failed because public `build_dataset(...)` still staged source tables before rejecting non-publishable horizons
- `test_cli_build_dataset_command_rejects_non_publishable_horizons[quarter]`
- `test_cli_build_dataset_command_rejects_non_publishable_horizons[year]`
  - both failed because the CLI still allowed the builder path to run for `quarter` and `year`

The tightened schema test also ran in this RED cycle and stayed green, which confirmed the dtype assertions matched the persisted output and did not require a production change.

### GREEN evidence

Implemented the second review fixes in `research/qbot_research/datasets.py` and `research/qbot_research/cli.py`:

- added a shared publishability gate so only `week` and `month` can reach `build_dataset(...)`, `build_dataset_for_connection(...)`, and the `build-dataset` CLI command
- enforced the gate before staging, writing, or registration side effects
- removed the dead legacy DuckDB PostgreSQL attach helper and its related constant
- extended the staged security-master projection with `master_available_at`
- added `name`, `list_status`, `list_date`, `delist_date`, and `master_available_at` to the deterministic `input_fingerprint`
- tightened schema coverage to assert full Polars and persisted Parquet dtypes for all required columns

Ran:

```bash
cd research
.venv/bin/python -m pytest tests/test_datasets.py -q
```

Result:

- `14 passed in 2.02s`

### Files changed

- `research/qbot_research/datasets.py`
- `research/qbot_research/cli.py`
- `research/tests/test_datasets.py`
- `.superpowers/sdd/gate1-task-3-report.md`

### Concerns

- None beyond the previously noted lack of a live PostgreSQL integration run.
