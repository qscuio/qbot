CREATE TABLE IF NOT EXISTS limit_up_stocks (
    code        VARCHAR(12) NOT NULL,
    trade_date  DATE NOT NULL,
    name        VARCHAR(50),
    streak      INT DEFAULT 1,
    limit_time  VARCHAR(10),
    seal_amount NUMERIC(18,2) DEFAULT 0,
    burst_count INT DEFAULT 0,
    score       NUMERIC(5,2) DEFAULT 0,
    board_type  VARCHAR(20),
    close       NUMERIC(10,3),
    pct_chg     NUMERIC(8,4),
    strth       NUMERIC(8,4),
    PRIMARY KEY (code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_limit_date ON limit_up_stocks(trade_date);
