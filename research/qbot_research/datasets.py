from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any, Final, Protocol, cast

import duckdb
import polars as pl
import psycopg
from psycopg.types.json import Jsonb

from qbot_research.contracts import DatasetManifest, Horizon

SCHEMA_VERSION = "1"
FEATURE_VERSION = "1"
DEFAULT_DATABASE_URL = "postgresql://qbot:qbot@127.0.0.1/qbot"
PUBLISHABLE_HORIZONS: Final[frozenset[Horizon]] = frozenset({"week", "month"})

STAGING_ONLY_COLUMNS = [
    "exclusion_reason",
    "snapshot_input_fingerprint",
    "snapshot_available_at",
    "bar_open",
    "bar_high",
    "bar_low",
    "bar_close",
    "bar_available_at",
    "adjustment_factor",
    "adjustment_available_at",
    "status_available_at",
    "master_available_at",
    "sector_valid_from",
    "sector_valid_to",
    "sector_available_at",
]

FINGERPRINT_COLUMNS = [
    "trade_date",
    "code",
    "bar_open",
    "bar_high",
    "bar_low",
    "bar_close",
    "amount",
    "turnover",
    "bar_available_at",
    "adjustment_factor",
    "adjustment_available_at",
    "listed_days",
    "is_st",
    "is_suspended",
    "price_limit_pct",
    "status_available_at",
    "name",
    "list_status",
    "list_date",
    "delist_date",
    "master_available_at",
    "sector_code",
    "sector_name",
    "sector_type",
    "sector_valid_from",
    "sector_valid_to",
    "sector_available_at",
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
    "snapshot_available_at",
    "snapshot_input_fingerprint",
    "exclusion_reason",
]


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
    validate_publishable_horizon(horizon)
    database_url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    output_path = Path(output_dir)
    available_at_cutoff = datetime.combine(as_of, time.max, tzinfo=UTC)

    with duckdb.connect(database=":memory:") as connection:
        _stage_postgres_source(connection, database_url, as_of, available_at_cutoff)
        result = build_dataset_for_connection(
            connection=connection,
            horizon=horizon,
            as_of=as_of,
            output_dir=output_path,
            registration_target=PostgresRegistrationTarget(database_url),
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
    validate_publishable_horizon(horizon)
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
        .drop(STAGING_ONLY_COLUMNS)
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


def _stage_postgres_source(
    connection: duckdb.DuckDBPyConnection,
    database_url: str,
    as_of: date,
    available_at_cutoff: datetime,
) -> None:
    with psycopg.connect(database_url) as source_connection:
        with source_connection.cursor() as cursor:
            _stage_query_results(
                connection=connection,
                cursor=cursor,
                table_name="stock_daily_bar_versions",
                create_sql="""
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
                """,
                select_sql="""
                    SELECT code, trade_date, open, high, low, close, volume, amount,
                           turnover, pe, pb, available_at
                    FROM public.stock_daily_bar_versions
                    WHERE available_at <= %s
                      AND trade_date <= %s
                    ORDER BY trade_date, code, available_at
                """,
                parameters=(available_at_cutoff, as_of),
            )
            _stage_query_results(
                connection=connection,
                cursor=cursor,
                table_name="stock_adjustment_factors",
                create_sql="""
                    CREATE TABLE stock_adjustment_factors (
                        code VARCHAR,
                        trade_date DATE,
                        adj_factor DOUBLE,
                        available_at TIMESTAMPTZ
                    )
                """,
                select_sql="""
                    SELECT code, trade_date, adj_factor, available_at
                    FROM public.stock_adjustment_factors
                    WHERE available_at <= %s
                      AND trade_date <= %s
                    ORDER BY trade_date, code, available_at
                """,
                parameters=(available_at_cutoff, as_of),
            )
            _stage_query_results(
                connection=connection,
                cursor=cursor,
                table_name="security_daily_status",
                create_sql="""
                    CREATE TABLE security_daily_status (
                        code VARCHAR,
                        trade_date DATE,
                        listed_days INTEGER,
                        is_st BOOLEAN,
                        is_suspended BOOLEAN,
                        price_limit_pct DOUBLE,
                        available_at TIMESTAMPTZ
                    )
                """,
                select_sql="""
                    SELECT code, trade_date, listed_days, is_st, is_suspended,
                           price_limit_pct, available_at
                    FROM public.security_daily_status
                    WHERE available_at <= %s
                      AND trade_date <= %s
                    ORDER BY trade_date, code, available_at
                """,
                parameters=(available_at_cutoff, as_of),
            )
            _stage_query_results(
                connection=connection,
                cursor=cursor,
                table_name="stock_sector_membership",
                create_sql="""
                    CREATE TABLE stock_sector_membership (
                        code VARCHAR,
                        sector_code VARCHAR,
                        sector_name VARCHAR,
                        sector_type VARCHAR,
                        valid_from DATE,
                        valid_to DATE,
                        available_at TIMESTAMPTZ
                    )
                """,
                select_sql="""
                    SELECT code, sector_code, sector_name, sector_type, valid_from,
                           valid_to, available_at
                    FROM public.stock_sector_membership
                    WHERE available_at <= %s
                      AND valid_from <= %s
                    ORDER BY code, valid_from, available_at
                """,
                parameters=(available_at_cutoff, as_of),
            )
            _stage_query_results(
                connection=connection,
                cursor=cursor,
                table_name="index_daily_bars",
                create_sql="""
                    CREATE TABLE index_daily_bars (
                        code VARCHAR,
                        trade_date DATE,
                        close DOUBLE,
                        change_pct DOUBLE,
                        volume BIGINT,
                        amount DOUBLE,
                        available_at TIMESTAMPTZ
                    )
                """,
                select_sql="""
                    SELECT code, trade_date, close, change_pct, volume, amount, available_at
                    FROM public.index_daily_bars
                    WHERE available_at <= %s
                      AND trade_date <= %s
                    ORDER BY trade_date, code, available_at
                """,
                parameters=(available_at_cutoff, as_of),
            )
            _stage_query_results(
                connection=connection,
                cursor=cursor,
                table_name="market_daily_snapshots",
                create_sql="""
                    CREATE TABLE market_daily_snapshots (
                        trade_date DATE,
                        snapshot_version VARCHAR,
                        available_at TIMESTAMPTZ,
                        data_complete BOOLEAN,
                        metrics JSON,
                        missing_inputs JSON,
                        input_fingerprint VARCHAR
                    )
                """,
                select_sql="""
                    SELECT trade_date, snapshot_version, available_at, data_complete,
                           metrics::text, missing_inputs::text, input_fingerprint
                    FROM public.market_daily_snapshots
                    WHERE available_at <= %s
                      AND trade_date <= %s
                    ORDER BY trade_date, available_at, snapshot_version
                """,
                parameters=(available_at_cutoff, as_of),
            )
            _stage_query_results(
                connection=connection,
                cursor=cursor,
                table_name="security_master_versions",
                create_sql="""
                    CREATE TABLE security_master_versions (
                        code VARCHAR,
                        name VARCHAR,
                        list_status VARCHAR,
                        list_date DATE,
                        delist_date DATE,
                        available_at TIMESTAMPTZ
                    )
                """,
                select_sql="""
                    SELECT code, name, list_status, list_date, delist_date, available_at
                    FROM public.security_master_versions
                    WHERE available_at <= %s
                    ORDER BY code, available_at
                """,
                parameters=(available_at_cutoff,),
            )


def _stage_query_results(
    connection: duckdb.DuckDBPyConnection,
    cursor: psycopg.Cursor[Any],
    table_name: str,
    create_sql: str,
    select_sql: str,
    parameters: tuple[object, ...],
) -> None:
    cursor.execute(select_sql, parameters)
    rows = cursor.fetchall()
    connection.execute(create_sql)
    if not rows:
        return

    placeholders = ", ".join(["?"] * len(rows[0]))
    connection.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)


def _dataset_version(horizon: Horizon, as_of: date) -> str:
    return f"ptf-v1-{horizon}-{as_of:%Y%m%d}"


def normalize_horizon(value: str) -> Horizon:
    valid_horizons = {"week", "month", "quarter", "year"}
    if value not in valid_horizons:
        raise ValueError(f"invalid horizon {value!r}; expected one of {sorted(valid_horizons)}")
    return cast(Horizon, value)


def validate_publishable_horizon(value: Horizon) -> Horizon:
    if value not in PUBLISHABLE_HORIZONS:
        raise ValueError("only 'week' and 'month' horizons may publish datasets")
    return value


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
                    available_at,
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
                    available_at,
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
                    available_at AS master_available_at,
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
                sector_type,
                valid_from,
                valid_to,
                available_at
            FROM (
                SELECT
                    bars.code,
                    bars.trade_date,
                    memberships.sector_code,
                    memberships.sector_name,
                    memberships.sector_type,
                    memberships.valid_from,
                    memberships.valid_to,
                    memberships.available_at,
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
                    available_at,
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
            bars.open AS bar_open,
            bars.high AS bar_high,
            bars.low AS bar_low,
            bars.close AS bar_close,
            bars.available_at AS bar_available_at,
            adjustments.adj_factor AS adjustment_factor,
            adjustments.available_at AS adjustment_available_at,
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
            statuses.available_at AS status_available_at,
            master.name,
            master.list_status,
            master.list_date,
            master.delist_date,
            master.master_available_at,
            sectors.sector_code,
            sectors.sector_name,
            sectors.sector_type,
            sectors.valid_from AS sector_valid_from,
            sectors.valid_to AS sector_valid_to,
            sectors.available_at AS sector_available_at,
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
            snapshot.available_at AS snapshot_available_at,
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
    hasher = hashlib.sha256()
    metadata_payload = {
        "horizon": horizon,
        "as_of": as_of.isoformat(),
        "available_at_cutoff": available_at_cutoff.isoformat(),
        "row_count": raw_frame.height,
    }
    hasher.update(json.dumps(metadata_payload, sort_keys=True).encode("utf-8"))

    fingerprint_frame = raw_frame.select(FINGERPRINT_COLUMNS).sort(["trade_date", "code"])
    for row in fingerprint_frame.iter_rows(named=True):
        canonical_row = {
            key: (value.isoformat() if isinstance(value, (date, datetime)) else value)
            for key, value in row.items()
        }
        hasher.update(json.dumps(canonical_row, sort_keys=True).encode("utf-8"))

    return hasher.hexdigest()
