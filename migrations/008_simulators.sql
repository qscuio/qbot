CREATE TABLE IF NOT EXISTS trading_sim_positions (
    id           BIGSERIAL PRIMARY KEY,
    code         VARCHAR(12) NOT NULL,
    name         VARCHAR(50),
    entry_price  NUMERIC(10,3),
    shares       INT,
    peak_price   NUMERIC(10,3),
    entry_date   DATE,
    exit_price   NUMERIC(10,3),
    exit_date    DATE,
    exit_reason  VARCHAR(50),
    pnl_pct      NUMERIC(8,4),
    is_open      BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS daban_sim_positions (
    id          BIGSERIAL PRIMARY KEY,
    code        VARCHAR(12) NOT NULL,
    name        VARCHAR(50),
    entry_price NUMERIC(10,3),
    shares      INT,
    score       NUMERIC(5,2),
    entry_date  DATE,
    exit_price  NUMERIC(10,3),
    exit_date   DATE,
    exit_reason VARCHAR(50),
    is_open     BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS sim_capital (
    sim_type    VARCHAR(20) PRIMARY KEY,
    balance     NUMERIC(18,2) NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
INSERT INTO sim_capital (sim_type, balance) VALUES ('general', 1000000.00)
    ON CONFLICT (sim_type) DO NOTHING;
INSERT INTO sim_capital (sim_type, balance) VALUES ('daban', 100000.00)
    ON CONFLICT (sim_type) DO NOTHING;
