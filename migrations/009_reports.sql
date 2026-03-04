CREATE TABLE IF NOT EXISTS reports (
    id           BIGSERIAL PRIMARY KEY,
    report_type  VARCHAR(50) NOT NULL,
    content      TEXT NOT NULL,
    generated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_reports_type ON reports(report_type, generated_at DESC);
