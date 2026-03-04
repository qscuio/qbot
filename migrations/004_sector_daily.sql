CREATE TABLE IF NOT EXISTS sector_daily (
    code        VARCHAR(20) NOT NULL,
    name        VARCHAR(100),
    sector_type VARCHAR(20),
    change_pct  NUMERIC(8,4),
    amount      NUMERIC(18,2),
    trade_date  DATE NOT NULL,
    PRIMARY KEY (code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_sector_date ON sector_daily(trade_date);
