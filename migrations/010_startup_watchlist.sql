CREATE TABLE IF NOT EXISTS startup_watchlist (
    code              VARCHAR(12) PRIMARY KEY,
    name              VARCHAR(50),
    first_limit_date  DATE NOT NULL,
    first_limit_close NUMERIC(10,3) DEFAULT 0,
    refreshed_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_startup_watchlist_first_limit_date
    ON startup_watchlist(first_limit_date DESC);
