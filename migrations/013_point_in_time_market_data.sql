CREATE TABLE IF NOT EXISTS stock_daily_bar_versions (
    code                 VARCHAR(12) NOT NULL,
    trade_date           DATE NOT NULL,
    open                 NUMERIC(10,3),
    high                 NUMERIC(10,3),
    low                  NUMERIC(10,3),
    close                NUMERIC(10,3),
    volume               BIGINT,
    amount               NUMERIC(18,2),
    turnover             NUMERIC(8,4),
    pe                   NUMERIC(12,4),
    pb                   NUMERIC(8,4),
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS stock_daily_basic_versions (
    code                 VARCHAR(12) NOT NULL,
    trade_date           DATE NOT NULL,
    turnover_rate        NUMERIC(10,4),
    volume_ratio         NUMERIC(10,4),
    pe                   NUMERIC(14,4),
    pb                   NUMERIC(14,4),
    ps                   NUMERIC(14,4),
    total_share          NUMERIC(20,4),
    float_share          NUMERIC(20,4),
    total_mv             NUMERIC(20,4),
    circ_mv              NUMERIC(20,4),
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS security_master_versions (
    code                 VARCHAR(12) NOT NULL,
    name                 VARCHAR(100) NOT NULL,
    market               VARCHAR(20),
    exchange             VARCHAR(20),
    list_status          VARCHAR(10) NOT NULL,
    list_date            DATE,
    delist_date          DATE,
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, available_at)
);

CREATE TABLE IF NOT EXISTS corporate_action_versions (
    source               VARCHAR(32) NOT NULL,
    action_key           VARCHAR(200) NOT NULL,
    code                 VARCHAR(12) NOT NULL,
    action_type          VARCHAR(40) NOT NULL,
    announcement_date    DATE,
    record_date          DATE,
    ex_date              DATE,
    pay_date             DATE,
    cash_dividend        NUMERIC(18,8),
    stock_ratio          NUMERIC(18,8),
    rights_ratio         NUMERIC(18,8),
    rights_price         NUMERIC(18,8),
    raw_payload          JSONB NOT NULL DEFAULT '{}',
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source, action_key, available_at)
);

CREATE TABLE IF NOT EXISTS sector_daily_versions (
    code                 VARCHAR(20) NOT NULL,
    name                 VARCHAR(100),
    sector_type          VARCHAR(20),
    change_pct           NUMERIC(8,4),
    amount               NUMERIC(18,2),
    trade_date           DATE NOT NULL,
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS limit_up_stock_versions (
    code                 VARCHAR(12) NOT NULL,
    trade_date           DATE NOT NULL,
    name                 VARCHAR(50),
    streak               INT,
    limit_time           VARCHAR(10),
    seal_amount          NUMERIC(18,2),
    burst_count          INT,
    score                NUMERIC(5,2),
    board_type           VARCHAR(20),
    close                NUMERIC(10,3),
    pct_chg              NUMERIC(8,4),
    strth                NUMERIC(8,4),
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS stock_adjustment_factors (
    code                 VARCHAR(12) NOT NULL,
    trade_date           DATE NOT NULL,
    adj_factor           NUMERIC(18,8) NOT NULL,
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS security_daily_status (
    code                 VARCHAR(12) NOT NULL,
    trade_date           DATE NOT NULL,
    listed_days          INT,
    is_st                BOOLEAN NOT NULL DEFAULT FALSE,
    is_suspended         BOOLEAN NOT NULL DEFAULT FALSE,
    price_limit_pct      NUMERIC(8,4),
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS index_daily_bars (
    code                 VARCHAR(20) NOT NULL,
    trade_date           DATE NOT NULL,
    close                NUMERIC(14,4) NOT NULL,
    change_pct           NUMERIC(10,4),
    volume               BIGINT,
    amount               NUMERIC(20,2),
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, trade_date, available_at)
);

CREATE TABLE IF NOT EXISTS stock_sector_membership (
    code                 VARCHAR(12) NOT NULL,
    sector_code          VARCHAR(20) NOT NULL,
    sector_name          VARCHAR(100) NOT NULL,
    sector_type          VARCHAR(20) NOT NULL,
    valid_from           DATE NOT NULL,
    valid_to             DATE,
    available_at         TIMESTAMPTZ NOT NULL,
    availability_quality VARCHAR(20) NOT NULL,
    source               VARCHAR(32) NOT NULL,
    source_updated_at    TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, sector_code, valid_from, available_at)
);

CREATE TABLE IF NOT EXISTS market_daily_snapshots (
    trade_date        DATE NOT NULL,
    snapshot_version  VARCHAR(32) NOT NULL,
    available_at      TIMESTAMPTZ NOT NULL,
    data_complete     BOOLEAN NOT NULL,
    metrics           JSONB NOT NULL,
    missing_inputs    JSONB NOT NULL DEFAULT '[]',
    input_fingerprint VARCHAR(64) NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, snapshot_version)
);

CREATE TABLE IF NOT EXISTS analysis_data_runs (
    run_id            UUID PRIMARY KEY,
    run_type          VARCHAR(50) NOT NULL,
    trade_date        DATE,
    status            VARCHAR(20) NOT NULL,
    input_fingerprint VARCHAR(64),
    details           JSONB NOT NULL DEFAULT '{}',
    error_message     TEXT,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_bar_versions_available
    ON stock_daily_bar_versions(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_daily_basic_versions_available
    ON stock_daily_basic_versions(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_security_master_available
    ON security_master_versions(code, available_at);

CREATE INDEX IF NOT EXISTS idx_corporate_actions_available
    ON corporate_action_versions(code, available_at, ex_date);

CREATE INDEX IF NOT EXISTS idx_sector_versions_available
    ON sector_daily_versions(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_limit_versions_available
    ON limit_up_stock_versions(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_adjustment_available
    ON stock_adjustment_factors(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_security_status_available
    ON security_daily_status(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_index_daily_available
    ON index_daily_bars(code, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_sector_membership_effective
    ON stock_sector_membership(code, valid_from, valid_to, available_at);

CREATE INDEX IF NOT EXISTS idx_market_snapshot_date
    ON market_daily_snapshots(trade_date DESC, snapshot_version);
