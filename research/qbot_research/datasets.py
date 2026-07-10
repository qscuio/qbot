from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any, Protocol, cast

import duckdb
import polars as pl
import psycopg
from psycopg.types.json import Jsonb

from qbot_research.contracts import DatasetManifest, Horizon

SCHEMA_VERSION = "1"
FEATURE_VERSION = "1"
DEFAULT_DATABASE_URL = "postgresql://qbot:qbot@127.0.0.1/qbot"
POSTGRES_ATTACH_ALIAS = "source_db"
@dataclass(frozen=True)
class DatasetBuildResult:
    manifest: DatasetManifest
    frame: pl.DataFrame
    manifest_payload: dict[str, Any]
    output_root: Path
    excluded_row_count: int
    excluded_rows_by_reason: dict[str, int]


class RegistrationTarget(Protocol):
    def register(
        self,
        manifest: DatasetManifest,
        manifest_payload: dict[str, Any],
        output_root: Path,
    ) -> None: ...


class DuckDbRegistrationTarget:
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        table_name: str = "analysis_dataset_manifests",
    ) -> None:
        self._connection = connection
        self._table_name = table_name

    def register(
        self,
        manifest: DatasetManifest,
        manifest_payload: dict[str, Any],
        output_root: Path,
    ) -> None:
        del output_root
        self._connection.execute(
            f"""
            INSERT INTO {self._table_name}
            (dataset_version, schema_version, feature_version, horizon, data_cutoff,
             available_at_cutoff, row_count, date_from, date_to, manifest, input_fingerprint)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                manifest.dataset_version,
                manifest.schema_version,
                manifest.feature_version,
                manifest.horizon,
                manifest.data_cutoff,
                manifest.available_at_cutoff,
                manifest.row_count,
                manifest.date_from,
                manifest.date_to,
                json.dumps(manifest_payload, sort_keys=True),
                manifest.input_fingerprint,
            ],
        )


class PostgresRegistrationTarget:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    def register(
        self,
        manifest: DatasetManifest,
        manifest_payload: dict[str, Any],
        output_root: Path,
    ) -> None:
        del output_root
        with psycopg.connect(self._database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO analysis_dataset_manifests
                    (dataset_version, schema_version, feature_version, horizon, data_cutoff,
                     available_at_cutoff, row_count, date_from, date_to, manifest, input_fingerprint)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        manifest.dataset_version,
                        manifest.schema_version,
                        manifest.feature_version,
                        manifest.horizon,
                        manifest.data_cutoff,
                        manifest.available_at_cutoff,
                        manifest.row_count,
                        manifest.date_from,
                        manifest.date_to,
                        Jsonb(manifest_payload),
                        manifest.input_fingerprint,
                    ),
                )
            connection.commit()


def build_dataset(horizon: Horizon, as_of: date, output_dir: Path) -> DatasetManifest:
    database_url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    output_path = Path(output_dir)

    with duckdb.connect(database=":memory:") as connection:
        source_prefix = _attach_postgres_source(connection, database_url)
        result = build_dataset_for_connection(
            connection=connection,
            horizon=horizon,
            as_of=as_of,
            output_dir=output_path,
            registration_target=PostgresRegistrationTarget(database_url),
            source_prefix=source_prefix,
        )
    return result.manifest


def build_dataset_for_connection(
    connection: duckdb.DuckDBPyConnection,
    horizon: Horizon,
    as_of: date,
    output_dir: Path,
    registration_target: RegistrationTarget | None = None,
    source_prefix: str = "",
) -> DatasetBuildResult:
    available_at_cutoff = datetime.combine(as_of, time.max, tzinfo=UTC)
    raw_frame = _load_dataset_frame(
        connection=connection,
        as_of=as_of,
        available_at_cutoff=available_at_cutoff,
        source_prefix=source_prefix,
    )
    excluded_frame = raw_frame.filter(pl.col("exclusion_reason").is_not_null())
    excluded_rows_by_reason = {
        str(row["exclusion_reason"]): int(row["len"])
        for row in excluded_frame.group_by("exclusion_reason").len().iter_rows(named=True)
    }
    frame = (
        raw_frame.filter(pl.col("exclusion_reason").is_null())
        .drop(["exclusion_reason", "snapshot_input_fingerprint"])
        .with_columns(
            [
                pl.lit(_dataset_version(horizon, as_of)).alias("dataset_version"),
                pl.lit(horizon).alias("horizon"),
                pl.col("trade_date").dt.year().alias("year"),
            ]
        )
        .sort(["trade_date", "code"])
    )

    dataset_version = _dataset_version(horizon, as_of)
    output_root = Path(output_dir) / f"dataset_version={dataset_version}"
    file_checksums = _write_partitioned_parquet(frame, output_root, horizon)
    date_from, date_to = _frame_date_range(frame, as_of)
    manifest = DatasetManifest(
        dataset_version=dataset_version,
        schema_version=SCHEMA_VERSION,
        feature_version=FEATURE_VERSION,
        horizon=horizon,
        data_cutoff=as_of,
        available_at_cutoff=available_at_cutoff,
        row_count=frame.height,
        date_from=date_from,
        date_to=date_to,
        files=list(file_checksums.keys()),
        file_checksums=file_checksums,
        input_fingerprint=_input_fingerprint(raw_frame, horizon, as_of, available_at_cutoff),
    )
    manifest_payload = {
        **manifest.model_dump(mode="json"),
        "excluded_row_count": excluded_frame.height,
        "excluded_rows_by_reason": excluded_rows_by_reason,
    }
    _write_manifest(output_root / "manifest.json", manifest_payload)

    target = registration_target or DuckDbRegistrationTarget(connection)
    target.register(manifest, manifest_payload, output_root)
    return DatasetBuildResult(
        manifest=manifest,
        frame=frame,
        manifest_payload=manifest_payload,
        output_root=output_root,
        excluded_row_count=excluded_frame.height,
        excluded_rows_by_reason=excluded_rows_by_reason,
    )


def _attach_postgres_source(connection: duckdb.DuckDBPyConnection, database_url: str) -> str:
    escaped_database_url = database_url.replace("'", "''")
    connection.execute("INSTALL postgres")
    connection.execute("LOAD postgres")
    connection.execute(f"ATTACH '{escaped_database_url}' AS {POSTGRES_ATTACH_ALIAS} (TYPE POSTGRES)")
    return f"{POSTGRES_ATTACH_ALIAS}.public."


def _dataset_version(horizon: Horizon, as_of: date) -> str:
    return f"ptf-v1-{horizon}-{as_of:%Y%m%d}"


def normalize_horizon(value: str) -> Horizon:
    valid_horizons = {"week", "month", "quarter", "year"}
    if value not in valid_horizons:
        raise ValueError(f"invalid horizon {value!r}; expected one of {sorted(valid_horizons)}")
    return cast(Horizon, value)


def _load_dataset_frame(
    connection: duckdb.DuckDBPyConnection,
    as_of: date,
    available_at_cutoff: datetime,
    source_prefix: str,
) -> pl.DataFrame:
    query = f"""
        WITH latest_bars AS (
            SELECT *
            FROM (
                SELECT
                    code,
                    trade_date,
                    CAST(open AS DOUBLE) AS open,
                    CAST(high AS DOUBLE) AS high,
                    CAST(low AS DOUBLE) AS low,
                    CAST(close AS DOUBLE) AS close,
                    volume,
                    CAST(amount AS DOUBLE) AS amount,
                    CAST(turnover AS DOUBLE) AS turnover,
                    available_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY code, trade_date
                        ORDER BY available_at DESC
                    ) AS row_num
                FROM {source_prefix}stock_daily_bar_versions
                WHERE available_at <= ?
                  AND trade_date <= ?
            )
            WHERE row_num = 1
        ),
        latest_adjustments AS (
            SELECT *
            FROM (
                SELECT
                    code,
                    trade_date,
                    CAST(adj_factor AS DOUBLE) AS adj_factor,
                    ROW_NUMBER() OVER (
                        PARTITION BY code, trade_date
                        ORDER BY available_at DESC
                    ) AS row_num
                FROM {source_prefix}stock_adjustment_factors
                WHERE available_at <= ?
                  AND trade_date <= ?
            )
            WHERE row_num = 1
        ),
        latest_status AS (
            SELECT *
            FROM (
                SELECT
                    code,
                    trade_date,
                    listed_days,
                    is_st,
                    is_suspended,
                    CAST(price_limit_pct AS DOUBLE) AS price_limit_pct,
                    ROW_NUMBER() OVER (
                        PARTITION BY code, trade_date
                        ORDER BY available_at DESC
                    ) AS row_num
                FROM {source_prefix}security_daily_status
                WHERE available_at <= ?
                  AND trade_date <= ?
            )
            WHERE row_num = 1
        ),
        latest_master AS (
            SELECT *
            FROM (
                SELECT
                    code,
                    name,
                    list_status,
                    list_date,
                    delist_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY code
                        ORDER BY available_at DESC
                    ) AS row_num
                FROM {source_prefix}security_master_versions
                WHERE available_at <= ?
            )
            WHERE row_num = 1
        ),
        latest_index AS (
            SELECT *
            FROM (
                SELECT
                    code,
                    trade_date,
                    CAST(change_pct AS DOUBLE) AS change_pct,
                    ROW_NUMBER() OVER (
                        PARTITION BY code, trade_date
                        ORDER BY available_at DESC
                    ) AS row_num
                FROM {source_prefix}index_daily_bars
                WHERE available_at <= ?
                  AND trade_date <= ?
            )
            WHERE row_num = 1
        ),
        index_pivot AS (
            SELECT
                trade_date,
                MAX(change_pct) FILTER (WHERE code = '000001.SH') AS sse_change_pct,
                MAX(change_pct) FILTER (WHERE code = '399001.SZ') AS szse_change_pct,
                MAX(change_pct) FILTER (WHERE code = '399006.SZ') AS chinext_change_pct,
                MAX(change_pct) FILTER (WHERE code = '000688.SH') AS star50_change_pct
            FROM latest_index
            GROUP BY trade_date
        ),
        effective_sector AS (
            SELECT
                code,
                trade_date,
                sector_code,
                sector_name,
                sector_type
            FROM (
                SELECT
                    bars.code,
                    bars.trade_date,
                    memberships.sector_code,
                    memberships.sector_name,
                    memberships.sector_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY bars.code, bars.trade_date
                        ORDER BY
                            memberships.available_at DESC NULLS LAST,
                            memberships.valid_from DESC NULLS LAST,
                            memberships.valid_to DESC NULLS LAST,
                            memberships.sector_code ASC NULLS LAST
                    ) AS row_num
                FROM latest_bars AS bars
                LEFT JOIN {source_prefix}stock_sector_membership AS memberships
                    ON memberships.code = bars.code
                   AND memberships.available_at <= ?
                   AND memberships.valid_from <= bars.trade_date
                   AND (
                        memberships.valid_to IS NULL
                        OR memberships.valid_to >= bars.trade_date
                   )
            )
            WHERE row_num = 1
        ),
        latest_snapshot AS (
            SELECT *
            FROM (
                SELECT
                    trade_date,
                    snapshot_version,
                    data_complete,
                    metrics,
                    input_fingerprint,
                    ROW_NUMBER() OVER (
                        PARTITION BY trade_date
                        ORDER BY available_at DESC, snapshot_version DESC
                    ) AS row_num
                FROM {source_prefix}market_daily_snapshots
                WHERE available_at <= ?
                  AND trade_date <= ?
            )
            WHERE row_num = 1
        )
        SELECT
            bars.trade_date,
            bars.code,
            bars.open * adjustments.adj_factor AS adjusted_open,
            bars.high * adjustments.adj_factor AS adjusted_high,
            bars.low * adjustments.adj_factor AS adjusted_low,
            bars.close * adjustments.adj_factor AS adjusted_close,
            bars.amount,
            bars.turnover,
            statuses.listed_days,
            statuses.is_st,
            statuses.is_suspended,
            statuses.price_limit_pct,
            master.name,
            master.list_status,
            master.list_date,
            master.delist_date,
            sectors.sector_code,
            sectors.sector_name,
            sectors.sector_type,
            indices.sse_change_pct,
            indices.szse_change_pct,
            indices.chinext_change_pct,
            indices.star50_change_pct,
            CAST(JSON_EXTRACT_STRING(snapshot.metrics, '$.breadth.up_count') AS INTEGER) AS breadth_up_count,
            CAST(JSON_EXTRACT_STRING(snapshot.metrics, '$.breadth.down_count') AS INTEGER) AS breadth_down_count,
            CAST(JSON_EXTRACT_STRING(snapshot.metrics, '$.breadth.flat_count') AS INTEGER) AS breadth_flat_count,
            CAST(JSON_EXTRACT_STRING(snapshot.metrics, '$.breadth.above_ma20_count') AS INTEGER) AS breadth_above_ma20_count,
            CAST(JSON_EXTRACT_STRING(snapshot.metrics, '$.breadth.new_high_20_count') AS INTEGER) AS breadth_new_high_20_count,
            CAST(JSON_EXTRACT_STRING(snapshot.metrics, '$.breadth.new_low_20_count') AS INTEGER) AS breadth_new_low_20_count,
            CAST(JSON_EXTRACT_STRING(snapshot.metrics, '$.breadth.limit_up_count') AS INTEGER) AS breadth_limit_up_count,
            CAST(JSON_EXTRACT_STRING(snapshot.metrics, '$.breadth.limit_down_count') AS INTEGER) AS breadth_limit_down_count,
            snapshot.data_complete AS market_data_complete,
            snapshot.snapshot_version AS market_snapshot_version,
            snapshot.input_fingerprint AS snapshot_input_fingerprint,
            ? AS available_at_cutoff,
            CASE
                WHEN adjustments.code IS NULL THEN 'missing_adjustment'
                WHEN statuses.code IS NULL THEN 'missing_status'
                ELSE NULL
            END AS exclusion_reason
        FROM latest_bars AS bars
        LEFT JOIN latest_adjustments AS adjustments
            ON adjustments.code = bars.code
           AND adjustments.trade_date = bars.trade_date
        LEFT JOIN latest_status AS statuses
            ON statuses.code = bars.code
           AND statuses.trade_date = bars.trade_date
        LEFT JOIN latest_master AS master
            ON master.code = bars.code
        LEFT JOIN effective_sector AS sectors
            ON sectors.code = bars.code
           AND sectors.trade_date = bars.trade_date
        LEFT JOIN index_pivot AS indices
            ON indices.trade_date = bars.trade_date
        LEFT JOIN latest_snapshot AS snapshot
            ON snapshot.trade_date = bars.trade_date
        ORDER BY bars.trade_date, bars.code
    """
    parameters = [
        available_at_cutoff,
        as_of,
        available_at_cutoff,
        as_of,
        available_at_cutoff,
        as_of,
        available_at_cutoff,
        available_at_cutoff,
        as_of,
        available_at_cutoff,
        available_at_cutoff,
        as_of,
        available_at_cutoff,
    ]
    arrow_table = connection.execute(query, parameters).to_arrow_table()
    return cast(pl.DataFrame, pl.from_arrow(arrow_table))


def _write_partitioned_parquet(
    frame: pl.DataFrame,
    output_root: Path,
    horizon: Horizon,
) -> dict[str, str]:
    output_root.mkdir(parents=True, exist_ok=True)
    file_checksums: dict[str, str] = {}

    if frame.height == 0:
        return file_checksums

    for year in frame.get_column("year").unique().sort().to_list():
        partition = frame.filter(pl.col("year") == year)
        relative_path = Path(f"horizon={horizon}") / f"year={year}" / "part-000.parquet"
        absolute_path = output_root / relative_path
        absolute_path.parent.mkdir(parents=True, exist_ok=True)
        partition.write_parquet(absolute_path)
        file_checksums[relative_path.as_posix()] = hashlib.sha256(absolute_path.read_bytes()).hexdigest()

    return file_checksums


def _write_manifest(path: Path, manifest_payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")


def _frame_date_range(frame: pl.DataFrame, fallback: date) -> tuple[date, date]:
    if frame.height == 0:
        return fallback, fallback

    min_value = frame.select(pl.col("trade_date").min()).item()
    max_value = frame.select(pl.col("trade_date").max()).item()
    if not isinstance(min_value, date) or not isinstance(max_value, date):
        raise TypeError("trade_date range must resolve to date values")
    return min_value, max_value


def _input_fingerprint(
    raw_frame: pl.DataFrame,
    horizon: Horizon,
    as_of: date,
    available_at_cutoff: datetime,
) -> str:
    payload = {
        "horizon": horizon,
        "as_of": as_of.isoformat(),
        "available_at_cutoff": available_at_cutoff.isoformat(),
        "snapshot_input_fingerprints": sorted(
            {
                value
                for value in raw_frame.get_column("snapshot_input_fingerprint").drop_nulls().to_list()
            }
        ),
        "row_count": raw_frame.height,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
