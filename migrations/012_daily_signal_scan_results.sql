CREATE TABLE IF NOT EXISTS daily_signal_scan_results (
    id          BIGSERIAL PRIMARY KEY,
    scan_date   DATE NOT NULL,
    run_id      UUID NOT NULL,
    code        VARCHAR(12) NOT NULL,
    name        VARCHAR(50),
    signal_id   VARCHAR(50) NOT NULL,
    signal_name VARCHAR(100),
    icon        VARCHAR(16),
    metadata    JSONB NOT NULL DEFAULT '{}',
    scanned_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (scan_date, signal_id, code)
);

CREATE INDEX IF NOT EXISTS idx_daily_signal_scan_results_date
    ON daily_signal_scan_results(scan_date DESC);

CREATE INDEX IF NOT EXISTS idx_daily_signal_scan_results_signal
    ON daily_signal_scan_results(signal_id, scan_date DESC);

CREATE INDEX IF NOT EXISTS idx_daily_signal_scan_results_run
    ON daily_signal_scan_results(run_id);
