CREATE TABLE IF NOT EXISTS chip_distribution (
    code          VARCHAR(12) NOT NULL,
    trade_date    DATE NOT NULL,
    distribution  JSONB NOT NULL DEFAULT '{}',
    avg_cost      NUMERIC(10,3),
    profit_ratio  NUMERIC(6,4),
    concentration NUMERIC(6,4),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (code, trade_date)
);
