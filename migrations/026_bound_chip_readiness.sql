-- Daily chip readiness scans only a small recent set of status dates. Lead
-- with trade_date so PostgreSQL can discover those candidates without walking
-- the code-leading point-in-time index.
CREATE INDEX IF NOT EXISTS idx_security_status_trade_date_code
    ON security_daily_status
       (trade_date DESC, code, available_at DESC, ingested_at DESC);
