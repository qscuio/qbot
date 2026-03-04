CREATE TABLE IF NOT EXISTS stock_daily_bars (
    code        VARCHAR(12) NOT NULL,
    trade_date  DATE NOT NULL,
    open        NUMERIC(10,3),
    high        NUMERIC(10,3),
    low         NUMERIC(10,3),
    close       NUMERIC(10,3),
    volume      BIGINT,
    amount      NUMERIC(18,2),
    turnover    NUMERIC(8,4),
    pe          NUMERIC(12,4),
    pb          NUMERIC(8,4),
    PRIMARY KEY (code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_bars_date ON stock_daily_bars(trade_date);
CREATE INDEX IF NOT EXISTS idx_bars_code ON stock_daily_bars(code);

CREATE TABLE IF NOT EXISTS stock_info (
    code        VARCHAR(12) PRIMARY KEY,
    name        VARCHAR(50) NOT NULL,
    market      VARCHAR(10),
    industry    VARCHAR(100),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
