# Gate 1 Task 2 Report: Independent Python Research Package

## Status

DONE_WITH_CONCERNS

## Implementation Summary

Implemented the independent Python research scaffold under `research/` and added the required systemd units under `deploy/`.

Delivered:

- `research/pyproject.toml`
  - Declares `qbot-research`
  - Keeps `requires-python = ">=3.12"`
  - Includes the requested runtime and dev dependencies
  - Adds the console-script entry point `qbot-research = "qbot_research.cli:app"`
  - Adds setuptools build metadata so local installation works
- `research/qbot_research/__init__.py`
  - Exports the contract types
- `research/qbot_research/contracts.py`
  - Defines `DatasetManifest`, `PatternModelPayload`, and `ValidationPayload` with Pydantic v2
  - Uses typed aliases compatible with strict mypy
  - Adds contract validation for:
    - non-negative `row_count`
    - valid `horizon`
    - manifest date ordering
    - checksum coverage for listed files
    - non-empty `required_features`
    - required feature coverage across `scaler_mean`, `scaler_scale`, and `centroid`
- `research/qbot_research/cli.py`
  - Minimal Typer `app` object only
  - No dataset export, training, `train-all`, DB access, or Task 3 behavior
- `research/tests/test_contracts.py`
  - Covers valid manifest construction
  - Covers invalid horizon rejection
  - Covers negative row-count rejection
  - Covers rejection when a required feature payload is missing
  - Covers importability of the Typer `app`
- `deploy/qbot-research.service`
  - Independent oneshot service
  - Uses the required `ExecStart=/opt/qbot/research/.venv/bin/qbot-research train-all --config /etc/qbot/research.toml`
  - Depends only on `network-online.target`, not `qbot.service`
- `deploy/qbot-research.timer`
  - Runs weekly via `OnCalendar=weekly`
  - Uses `Persistent=true`

## TDD RED/GREEN Evidence

### RED

Wrote `research/tests/test_contracts.py` before creating any production Python package files.

Ran:

```bash
cd research
.venv/bin/python -m pytest tests/test_contracts.py -q
```

Observed failure:

- `ModuleNotFoundError: No module named 'qbot_research.cli'`

This was the expected failure because the package scaffold had not been implemented yet.

### GREEN

After creating `__init__.py`, `contracts.py`, and `cli.py`, re-ran:

```bash
cd research
.venv/bin/python -m pytest tests/test_contracts.py -q
```

Result:

- `5 passed in 0.11s`

## Environment Setup

Global `python3` had `pytest` available, but `ruff` and `mypy` were not installed.

Per task instruction, created an isolated local virtual environment:

```bash
cd research
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install '.[dev]'
.venv/bin/python -m pip install -e '.[dev]'
```

The editable install ensures the local environment exposes the package metadata and console-script entry point without committing `.venv`.

## Commands Run and Results

### Context and task inspection

```bash
git rev-parse --git-dir
git rev-parse --git-common-dir
git branch --show-current
sed -n '1,220p' .superpowers/sdd/gate1-task-2-brief.md
sed -n '170,420p' docs/superpowers/plans/2026-07-10-strong-stock-pattern-shadow-engine.md
sed -n '1,220p' deploy/qbot.service
```

Result:

- Confirmed work is happening in the existing linked worktree on `feat/point-in-time-data-foundation`
- Confirmed Task 2 requirements and the revised CLI boundary
- Confirmed the new Python service must remain independent of Rust supervision

### Dependency availability check

```bash
python3 -m pytest --version
python3 -m ruff --version
python3 -m mypy --version
```

Result:

- `pytest` present globally
- `ruff` missing globally
- `mypy` missing globally

### Required verification

```bash
cd research
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy qbot_research
cd ..
git diff --check
```

Results:

- `pytest`: `5 passed in 0.13s`
- `ruff`: `All checks passed!`
- `mypy`: `Success: no issues found in 3 source files`
- `git diff --check`: clean

## Files Changed

- `research/pyproject.toml`
- `research/qbot_research/__init__.py`
- `research/qbot_research/contracts.py`
- `research/qbot_research/cli.py`
- `research/tests/test_contracts.py`
- `deploy/qbot-research.service`
- `deploy/qbot-research.timer`
- `.superpowers/sdd/gate1-task-2-report.md`

Local-only environment/workflow changes:

- Created `research/.venv/` for verification
- Added `research/.venv/` to local git exclude via repository metadata so it is not staged

## Self-Review

- Scope stayed within Task 2 plus the explicitly approved minimal `cli.py` stub.
- No Task 3 behavior was implemented.
- No Rust subprocess or supervision integration was added.
- The Python contract layer is importable, typed, and passes strict mypy.
- The timer is independent of `qbot.service` and only relies on network availability.
- All requested verification commands were run fresh after implementation.

## Concerns

1. `deploy/qbot-research.service` intentionally references `qbot-research train-all`, but Task 2 does not implement that command. This matches the revised task boundary and preserves Task 3 as the place where the command surface is created, but the service should not be activated before Task 3 lands.
2. `qbot-research.timer` is scaffolded correctly for weekly scheduling, but end-to-end service execution is deferred until the Task 3 CLI commands exist.

## Review fix

### RED evidence

Added the review-driven tests first in `research/tests/test_contracts.py`:

- a `typer.testing.CliRunner` test that invokes `qbot-research train-all --config /tmp/research.toml`
- parametrized negative coverage for missing required features across `scaler_mean`, `scaler_scale`, and `centroid`

Ran:

```bash
cd research
.venv/bin/python -m pytest -q tests/test_contracts.py
```

Observed failure against the pre-fix CLI scaffold:

- `RuntimeError: Could not get a command for this Typer instance`
- `1 failed, 7 passed`

This reproduces the blocking review finding that the console entry point targeted a bare `Typer()` object without a callable command surface.

### GREEN evidence

Implemented a minimal `train-all` scaffold in `research/qbot_research/cli.py` that:

- keeps the CLI deployable as a Typer group
- parses `--config`
- exits successfully with an explicit scaffold message
- does not perform dataset export, training, DB access, or model publishing

Re-ran the targeted test file:

```bash
cd research
.venv/bin/python -m pytest -q tests/test_contracts.py
```

Result:

- `8 passed in 0.11s`

Ran the required full verification:

```bash
cd research
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy qbot_research
cd ..
git diff --check
```

Results:

- `pytest`: `8 passed in 0.11s`
- `ruff`: `All checks passed!`
- `mypy`: `Success: no issues found in 3 source files`
- `git diff --check`: clean

### Files changed

- `research/qbot_research/cli.py`
- `research/tests/test_contracts.py`
- `.superpowers/sdd/gate1-task-2-report.md`

### Concerns

- The `train-all` command is intentionally a no-op scaffold for Task 2. It proves the CLI boot path is deployable and non-crashing, but it does not claim that training ran.
