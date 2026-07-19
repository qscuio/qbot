-- Expand pre-window completed backfills into the exact annual keys consumed by
-- the service. Keep the original broad row as an audit record.
WITH completed_ranges AS MATERIALIZED (
    SELECT phase, code, start_date, end_date, attempts,
           created_at, updated_at, completed_at
    FROM company_data_repair_checkpoints
    WHERE status = 'completed'
      AND phase IN ('financials', 'dividends')
      -- Migration 023 uses these sentinels for legacy open bounds. They are
      -- audit coverage, not real dates, and must be refetched per exact year.
      AND start_date <> DATE '0001-01-01'
      AND end_date <> DATE '9999-12-31'
), yearly_windows AS (
    SELECT phase,
           code,
           GREATEST(start_date, make_date(year_number, 1, 1)) AS start_date,
           LEAST(end_date, make_date(year_number, 12, 31)) AS end_date,
           attempts,
           created_at,
           updated_at,
           completed_at
    FROM completed_ranges
    CROSS JOIN LATERAL generate_series(
        EXTRACT(YEAR FROM start_date)::integer,
        EXTRACT(YEAR FROM end_date)::integer
    ) AS years(year_number)
)
INSERT INTO company_data_repair_checkpoints
    (phase, code, start_date, end_date, status, attempts, last_error,
     lease_token, lease_expires_at, created_at, updated_at, completed_at)
SELECT phase, code, start_date, end_date, 'completed', attempts, NULL,
       NULL, NULL, created_at, updated_at, completed_at
FROM yearly_windows
WHERE start_date <= end_date
-- An exact row is more authoritative than legacy broad coverage: it may record
-- a newer failed attempt or an active owner and must never be overwritten.
ON CONFLICT (phase, code, start_date, end_date) DO NOTHING;

-- Date-scoped latest phases were an interim implementation. Collapse every
-- code/fiscal-year group into one stable phase row using the most recently
-- updated internally-consistent state, then remove the redundant audit rows.
WITH dated_latest AS MATERIALIZED (
    SELECT split_part(phase, ':', 1) AS stable_phase,
           code,
           start_date,
           make_date(EXTRACT(YEAR FROM end_date)::integer, 12, 31) AS end_date,
           status,
           attempts,
           last_error,
           lease_token,
           lease_expires_at,
           created_at,
           updated_at,
           completed_at,
           ROW_NUMBER() OVER (
               PARTITION BY split_part(phase, ':', 1), code, start_date,
                            EXTRACT(YEAR FROM end_date)
               ORDER BY updated_at DESC, completed_at DESC NULLS LAST,
                        phase DESC, attempts DESC
           ) AS rank
    FROM company_data_repair_checkpoints
    WHERE phase ~ '^(financials|dividends)_latest:[0-9]{4}-[0-9]{2}-[0-9]{2}$'
)
INSERT INTO company_data_repair_checkpoints
    (phase, code, start_date, end_date, status, attempts, last_error,
     lease_token, lease_expires_at, created_at, updated_at, completed_at)
SELECT stable_phase, code, start_date, end_date, status, attempts, last_error,
       lease_token, lease_expires_at, created_at, updated_at, completed_at
FROM dated_latest
WHERE rank = 1
ON CONFLICT (phase, code, start_date, end_date) DO UPDATE SET
    status = EXCLUDED.status,
    attempts = EXCLUDED.attempts,
    last_error = EXCLUDED.last_error,
    lease_token = EXCLUDED.lease_token,
    lease_expires_at = EXCLUDED.lease_expires_at,
    created_at = LEAST(company_data_repair_checkpoints.created_at,
                       EXCLUDED.created_at),
    updated_at = EXCLUDED.updated_at,
    completed_at = EXCLUDED.completed_at
WHERE EXCLUDED.updated_at > company_data_repair_checkpoints.updated_at;

DELETE FROM company_data_repair_checkpoints
WHERE phase ~ '^(financials|dividends)_latest:[0-9]{4}-[0-9]{2}-[0-9]{2}$';
