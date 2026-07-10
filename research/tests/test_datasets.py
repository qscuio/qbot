from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pytest
from typer.testing import CliRunner

from qbot_research.cli import app
from qbot_research.contracts import DatasetManifest
from qbot_research.datasets import DatasetBuildResult, build_dataset, build_dataset_for_connection

RUNNER = CliRunner()


def dt(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


@dataclass
class RecordingRegistrationTarget:
    records: list[dict[str, Any]]

    def register(
        self,
        manifest: DatasetManifest,
        manifest_payload: dict[str, Any],
        output_root: Path,
    ) -> None:
        self.records.append(
            {
                "manifest": manifest,
                "manifest_payload": manifest_payload,
                "output_root": output_root,
                "all_files_exist": all((output_root / path).exists() for path in manifest.files),
                "manifest_json_exists": (output_root / "manifest.json").exists(),
            }
        )


def _create_source_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE stock_daily_bar_versions (
            code VARCHAR,
            trade_date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            amount DOUBLE,
            turnover DOUBLE,
            pe DOUBLE,
            pb DOUBLE,
            available_at TIMESTAMPTZ
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE stock_adjustment_factors (
            code VARCHAR,
            trade_date DATE,
            adj_factor DOUBLE,
            available_at TIMESTAMPTZ
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE security_daily_status (
            code VARCHAR,
            trade_date DATE,
            listed_days INTEGER,
            is_st BOOLEAN,
            is_suspended BOOLEAN,
            price_limit_pct DOUBLE,
            available_at TIMESTAMPTZ
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE stock_sector_membership (
            code VARCHAR,
            sector_code VARCHAR,
            sector_name VARCHAR,
            sector_type VARCHAR,
            valid_from DATE,
            valid_to DATE,
            available_at TIMESTAMPTZ
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE index_daily_bars (
            code VARCHAR,
            trade_date DATE,
            close DOUBLE,
            change_pct DOUBLE,
            volume BIGINT,
            amount DOUBLE,
            available_at TIMESTAMPTZ
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE market_daily_snapshots (
            trade_date DATE,
            snapshot_version VARCHAR,
            available_at TIMESTAMPTZ,
            data_complete BOOLEAN,
            metrics JSON,
            missing_inputs JSON,
            input_fingerprint VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE security_master_versions (
            code VARCHAR,
            name VARCHAR,
            list_status VARCHAR,
            list_date DATE,
            delist_date DATE,
            available_at TIMESTAMPTZ
        )
        """
    )


def _create_manifest_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE analysis_dataset_manifests (
            dataset_version VARCHAR,
            schema_version VARCHAR,
            feature_version VARCHAR,
            horizon VARCHAR,
            data_cutoff DATE,
            available_at_cutoff TIMESTAMPTZ,
            row_count BIGINT,
            date_from DATE,
            date_to DATE,
            manifest JSON,
            input_fingerprint VARCHAR,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )


def _seed_dataset_source(connection: duckdb.DuckDBPyConnection) -> None:
    _create_source_tables(connection)

    connection.executemany(
        """
        INSERT INTO stock_daily_bar_versions
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAA", date(2026, 7, 9), 10.0, 11.0, 9.5, 10.5, 1000, 10000.0, 1.1, 15.0, 1.5, dt(2026, 7, 9, 17)),
            ("AAA", date(2026, 7, 10), 10.0, 11.0, 9.8, 10.0, 1200, 12000.0, 1.2, 16.0, 1.6, dt(2026, 7, 10, 17)),
            ("AAA", date(2026, 7, 10), 90.0, 100.0, 80.0, 99.0, 9999, 99999.0, 9.9, 16.0, 1.6, dt(2026, 7, 11, 9)),
            ("BBB", date(2026, 7, 8), 20.0, 21.0, 19.0, 20.5, 800, 15000.0, 0.7, 12.0, 1.2, dt(2026, 7, 8, 17)),
            ("CCC", date(2026, 7, 10), 30.0, 32.0, 29.0, 31.0, 500, 18000.0, 0.5, 10.0, 1.0, dt(2026, 7, 10, 16)),
            ("DDD", date(2026, 7, 10), 40.0, 42.0, 39.0, 41.0, 300, 9000.0, 0.3, 8.0, 0.8, dt(2026, 7, 10, 16)),
            ("EEE", date(2026, 7, 10), 50.0, 52.0, 49.0, 51.0, 200, 6000.0, 0.2, 9.0, 0.9, dt(2026, 7, 11, 0, 5)),
        ],
    )
    connection.executemany(
        """
        INSERT INTO stock_adjustment_factors
        VALUES (?, ?, ?, ?)
        """,
        [
            ("AAA", date(2026, 7, 9), 1.1, dt(2026, 7, 9, 18)),
            ("AAA", date(2026, 7, 10), 1.2, dt(2026, 7, 10, 18)),
            ("BBB", date(2026, 7, 8), 0.9, dt(2026, 7, 8, 18)),
            ("DDD", date(2026, 7, 10), 1.0, dt(2026, 7, 10, 18)),
            ("EEE", date(2026, 7, 10), 1.0, dt(2026, 7, 10, 18)),
        ],
    )
    connection.executemany(
        """
        INSERT INTO security_daily_status
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAA", date(2026, 7, 9), 400, False, False, 10.0, dt(2026, 7, 9, 18)),
            ("AAA", date(2026, 7, 10), 401, False, False, 10.0, dt(2026, 7, 10, 18)),
            ("BBB", date(2026, 7, 8), 250, True, False, 5.0, dt(2026, 7, 8, 18)),
            ("CCC", date(2026, 7, 10), 150, False, False, 10.0, dt(2026, 7, 10, 18)),
            ("DDD", date(2026, 7, 10), 80, False, True, 10.0, dt(2026, 7, 11, 8)),
        ],
    )
    connection.executemany(
        """
        INSERT INTO stock_sector_membership
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAA", "TECH", "Technology", "industry", date(2026, 7, 10), None, dt(2026, 7, 10, 18, 30)),
            ("BBB", "FIN", "Finance", "industry", date(2020, 1, 1), None, dt(2026, 1, 1, 0)),
        ],
    )
    connection.executemany(
        """
        INSERT INTO index_daily_bars
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("000001.SH", date(2026, 7, 8), 3200.0, 0.8, 100000, 1000000.0, dt(2026, 7, 8, 18)),
            ("399001.SZ", date(2026, 7, 8), 11800.0, 0.4, 90000, 900000.0, dt(2026, 7, 8, 18)),
            ("000001.SH", date(2026, 7, 9), 3210.0, 0.3, 110000, 1050000.0, dt(2026, 7, 9, 18)),
            ("399001.SZ", date(2026, 7, 9), 11850.0, 0.2, 95000, 910000.0, dt(2026, 7, 9, 18)),
            ("000001.SH", date(2026, 7, 10), 3225.0, 1.2, 120000, 1100000.0, dt(2026, 7, 10, 18)),
            ("399001.SZ", date(2026, 7, 10), 11920.0, 0.6, 97000, 920000.0, dt(2026, 7, 10, 18)),
        ],
    )
    connection.executemany(
        """
        INSERT INTO market_daily_snapshots
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                date(2026, 7, 8),
                "snap-0708",
                dt(2026, 7, 8, 19),
                True,
                json.dumps({"breadth": {"up_count": 100, "down_count": 50, "flat_count": 25, "above_ma20_count": 80, "new_high_20_count": 12, "new_low_20_count": 3, "limit_up_count": 5, "limit_down_count": 1}}),
                "[]",
                "fp-0708",
            ),
            (
                date(2026, 7, 9),
                "snap-0709",
                dt(2026, 7, 9, 19),
                True,
                json.dumps({"breadth": {"up_count": 110, "down_count": 40, "flat_count": 20, "above_ma20_count": 85, "new_high_20_count": 15, "new_low_20_count": 2, "limit_up_count": 6, "limit_down_count": 1}}),
                "[]",
                "fp-0709",
            ),
            (
                date(2026, 7, 10),
                "snap-0710",
                dt(2026, 7, 10, 19),
                True,
                json.dumps({"breadth": {"up_count": 120, "down_count": 30, "flat_count": 10, "above_ma20_count": 90, "new_high_20_count": 18, "new_low_20_count": 1, "limit_up_count": 8, "limit_down_count": 0}}),
                "[]",
                "fp-0710",
            ),
        ],
    )
    connection.executemany(
        """
        INSERT INTO security_master_versions
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAA", "Alpha", "L", date(2020, 1, 1), None, dt(2026, 1, 1, 0)),
            ("BBB", "Beta", "D", date(2019, 1, 1), date(2026, 7, 9), dt(2026, 7, 9, 20)),
            ("CCC", "Gamma", "L", date(2025, 1, 1), None, dt(2026, 1, 1, 0)),
            ("DDD", "Delta", "L", date(2025, 6, 1), None, dt(2026, 1, 1, 0)),
        ],
    )


@pytest.fixture()
def dataset_connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(database=":memory:")
    _seed_dataset_source(connection)
    _create_manifest_table(connection)

    try:
        yield connection
    finally:
        connection.close()


def test_build_dataset_for_connection_obeys_point_in_time_rules_and_writes_outputs(
    dataset_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    registration_target = RecordingRegistrationTarget(records=[])

    result = build_dataset_for_connection(
        connection=dataset_connection,
        horizon="week",
        as_of=date(2026, 7, 10),
        output_dir=tmp_path,
        registration_target=registration_target,
    )

    assert isinstance(result, DatasetBuildResult)
    assert result.manifest.horizon == "week"
    assert result.manifest.row_count == 3
    assert result.excluded_row_count == 2
    assert result.excluded_rows_by_reason == {"missing_adjustment": 1, "missing_status": 1}

    frame = result.frame.sort(["trade_date", "code"])
    assert frame.select(["trade_date", "code"]).to_dicts() == [
        {"trade_date": date(2026, 7, 8), "code": "BBB"},
        {"trade_date": date(2026, 7, 9), "code": "AAA"},
        {"trade_date": date(2026, 7, 10), "code": "AAA"},
    ]

    july_ninth = frame.filter((pl.col("code") == "AAA") & (pl.col("trade_date") == date(2026, 7, 9))).row(0, named=True)
    july_tenth = frame.filter((pl.col("code") == "AAA") & (pl.col("trade_date") == date(2026, 7, 10))).row(0, named=True)
    delisted = frame.filter(pl.col("code") == "BBB").row(0, named=True)

    assert july_tenth["adjusted_close"] == pytest.approx(12.0)
    assert july_tenth["adjusted_open"] == pytest.approx(12.0)
    assert july_tenth["sse_change_pct"] == pytest.approx(1.2)
    assert july_tenth["breadth_up_count"] == 120
    assert july_ninth["sector_name"] is None
    assert july_tenth["sector_name"] == "Technology"
    assert delisted["list_status"] == "D"
    assert delisted["delist_date"] == date(2026, 7, 9)

    output_root = tmp_path / f"dataset_version={result.manifest.dataset_version}"
    parquet_path = output_root / "horizon=week" / "year=2026" / "part-000.parquet"
    manifest_path = output_root / "manifest.json"

    assert parquet_path.exists()
    assert manifest_path.exists()

    manifest_payload = json.loads(manifest_path.read_text())
    expected_checksum = hashlib.sha256(parquet_path.read_bytes()).hexdigest()
    assert manifest_payload["file_checksums"]["horizon=week/year=2026/part-000.parquet"] == expected_checksum
    assert manifest_payload["excluded_row_count"] == 2
    assert manifest_payload["excluded_rows_by_reason"] == {"missing_adjustment": 1, "missing_status": 1}

    assert len(registration_target.records) == 1
    registration_record = registration_target.records[0]
    assert registration_record["all_files_exist"] is True
    assert registration_record["manifest_json_exists"] is True
    assert registration_record["manifest_payload"]["excluded_rows_by_reason"]["missing_status"] == 1


def test_build_dataset_for_connection_persists_required_frame_and_parquet_schema(
    dataset_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    result = build_dataset_for_connection(
        connection=dataset_connection,
        horizon="week",
        as_of=date(2026, 7, 10),
        output_dir=tmp_path,
    )

    expected_columns = [
        "trade_date",
        "code",
        "adjusted_open",
        "adjusted_high",
        "adjusted_low",
        "adjusted_close",
        "amount",
        "turnover",
        "listed_days",
        "is_st",
        "is_suspended",
        "price_limit_pct",
        "name",
        "list_status",
        "list_date",
        "delist_date",
        "sector_code",
        "sector_name",
        "sector_type",
        "sse_change_pct",
        "szse_change_pct",
        "chinext_change_pct",
        "star50_change_pct",
        "breadth_up_count",
        "breadth_down_count",
        "breadth_flat_count",
        "breadth_above_ma20_count",
        "breadth_new_high_20_count",
        "breadth_new_low_20_count",
        "breadth_limit_up_count",
        "breadth_limit_down_count",
        "market_data_complete",
        "market_snapshot_version",
        "available_at_cutoff",
        "dataset_version",
        "horizon",
        "year",
    ]

    assert result.frame.columns == expected_columns

    parquet_frame = pl.read_parquet(
        tmp_path
        / f"dataset_version={result.manifest.dataset_version}"
        / "horizon=week"
        / "year=2026"
        / "part-000.parquet"
    )
    assert parquet_frame.columns == expected_columns
    assert parquet_frame.row(0, named=True)["available_at_cutoff"] == result.manifest.available_at_cutoff


def test_build_dataset_for_connection_registers_manifest_contents_in_duckdb(
    dataset_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    result = build_dataset_for_connection(
        connection=dataset_connection,
        horizon="month",
        as_of=date(2026, 7, 10),
        output_dir=tmp_path,
    )

    stored = dataset_connection.execute(
        """
        SELECT dataset_version, horizon, row_count, manifest, input_fingerprint
        FROM analysis_dataset_manifests
        WHERE dataset_version = ?
        """,
        [result.manifest.dataset_version],
    ).fetchone()

    assert stored is not None
    assert stored[0] == result.manifest.dataset_version
    assert stored[1] == "month"
    assert stored[2] == 3
    stored_manifest = json.loads(stored[3])
    assert stored_manifest["dataset_version"] == result.manifest.dataset_version
    assert stored_manifest["excluded_row_count"] == 2
    assert stored[4] == result.manifest.input_fingerprint


def test_build_dataset_input_fingerprint_changes_when_dataset_inputs_change(tmp_path: Path) -> None:
    baseline_connection = duckdb.connect(database=":memory:")
    changed_connection = duckdb.connect(database=":memory:")

    try:
        _seed_dataset_source(baseline_connection)
        _seed_dataset_source(changed_connection)

        changed_connection.execute(
            """
            DELETE FROM stock_adjustment_factors
            WHERE code = 'AAA' AND trade_date = DATE '2026-07-10'
            """
        )
        changed_connection.execute(
            """
            INSERT INTO stock_adjustment_factors
            VALUES ('AAA', DATE '2026-07-10', 1.25, ?)
            """,
            [dt(2026, 7, 10, 18)],
        )

        baseline_result = build_dataset_for_connection(
            connection=baseline_connection,
            horizon="week",
            as_of=date(2026, 7, 10),
            output_dir=tmp_path / "baseline",
            registration_target=RecordingRegistrationTarget(records=[]),
        )
        changed_result = build_dataset_for_connection(
            connection=changed_connection,
            horizon="week",
            as_of=date(2026, 7, 10),
            output_dir=tmp_path / "changed",
            registration_target=RecordingRegistrationTarget(records=[]),
        )
    finally:
        baseline_connection.close()
        changed_connection.close()

    assert baseline_result.frame.row(2, named=True)["adjusted_close"] == pytest.approx(12.0)
    assert changed_result.frame.row(2, named=True)["adjusted_close"] == pytest.approx(12.5)
    assert changed_result.manifest.input_fingerprint != baseline_result.manifest.input_fingerprint


def test_build_dataset_public_path_uses_staged_source_loader(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import qbot_research.datasets as datasets_module

    registration_target = RecordingRegistrationTarget(records=[])

    def fail_legacy_attach(*args: Any, **kwargs: Any) -> str:
        raise AssertionError("legacy DuckDB postgres attach path should not run")

    def fake_stage_postgres_source(
        connection: duckdb.DuckDBPyConnection,
        database_url: str,
        as_of: date,
        available_at_cutoff: datetime,
    ) -> None:
        assert database_url == "postgresql://stubbed.example/qbot"
        assert as_of == date(2026, 7, 10)
        assert available_at_cutoff == datetime.combine(date(2026, 7, 10), time.max, tzinfo=timezone.utc)
        _seed_dataset_source(connection)

    monkeypatch.setenv("DATABASE_URL", "postgresql://stubbed.example/qbot")
    monkeypatch.setattr(datasets_module, "_attach_postgres_source", fail_legacy_attach)
    monkeypatch.setattr(
        datasets_module,
        "_stage_postgres_source",
        fake_stage_postgres_source,
        raising=False,
    )
    monkeypatch.setattr(
        datasets_module,
        "PostgresRegistrationTarget",
        lambda database_url: registration_target,
    )

    manifest = build_dataset("week", date(2026, 7, 10), tmp_path)

    assert manifest.row_count == 3
    assert registration_target.records[0]["manifest"].dataset_version == manifest.dataset_version


def test_cli_build_dataset_command_invokes_builder(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    expected_manifest = DatasetManifest(
        dataset_version="ptf-v1-week-20260710",
        schema_version="1",
        feature_version="1",
        horizon="week",
        data_cutoff=date(2026, 7, 10),
        available_at_cutoff=dt(2026, 7, 10, 23, 59),
        row_count=3,
        date_from=date(2026, 7, 8),
        date_to=date(2026, 7, 10),
        files=["horizon=week/year=2026/part-000.parquet"],
        file_checksums={"horizon=week/year=2026/part-000.parquet": "abc123"},
        input_fingerprint="fingerprint-1",
    )
    calls: dict[str, Any] = {}

    def fake_build_dataset(horizon: str, as_of: date, output_dir: Path) -> DatasetManifest:
        calls["horizon"] = horizon
        calls["as_of"] = as_of
        calls["output_dir"] = output_dir
        return expected_manifest

    monkeypatch.setattr("qbot_research.cli.build_dataset", fake_build_dataset)

    result = RUNNER.invoke(
        app,
        ["build-dataset", "--horizon", "week", "--as-of", "2026-07-10", "--output-dir", str(tmp_path)],
    )

    assert result.exit_code == 0, result.stdout
    assert calls == {
        "horizon": "week",
        "as_of": date(2026, 7, 10),
        "output_dir": tmp_path,
    }
    assert "ptf-v1-week-20260710" in result.stdout
