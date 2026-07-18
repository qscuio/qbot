CREATE TABLE IF NOT EXISTS scan_runs (
    run_id UUID PRIMARY KEY,
    status VARCHAR(16) NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    stocks_checked INTEGER NOT NULL DEFAULT 0,
    hit_count INTEGER NOT NULL DEFAULT 0,
    error_summary VARCHAR(500)
);

CREATE INDEX IF NOT EXISTS idx_scan_runs_completed
    ON scan_runs (completed_at DESC)
    WHERE status = 'completed';

-- Make the most recent pre-dashboard scans visible immediately after rollout.
INSERT INTO scan_runs (
    run_id,
    status,
    started_at,
    completed_at,
    stocks_checked,
    hit_count
)
SELECT
    run_id,
    'completed',
    MIN(scanned_at),
    MAX(scanned_at),
    0,
    COUNT(*)::INTEGER
FROM scan_results
GROUP BY run_id
ON CONFLICT (run_id) DO NOTHING;
