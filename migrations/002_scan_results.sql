CREATE TABLE IF NOT EXISTS scan_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL,
    code        VARCHAR(12) NOT NULL,
    name        VARCHAR(50),
    signal_id   VARCHAR(50) NOT NULL,
    metadata    JSONB DEFAULT '{}',
    scanned_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_scan_run ON scan_results(run_id);
CREATE INDEX IF NOT EXISTS idx_scan_date ON scan_results(scanned_at);
CREATE INDEX IF NOT EXISTS idx_scan_signal ON scan_results(signal_id);
