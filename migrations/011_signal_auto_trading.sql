CREATE TABLE IF NOT EXISTS signal_strategy_accounts (
    id                  BIGSERIAL PRIMARY KEY,
    signal_id           VARCHAR(50) NOT NULL UNIQUE,
    signal_name         VARCHAR(100) NOT NULL,
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    initial_capital     NUMERIC(18,2) NOT NULL DEFAULT 100000.00,
    cash_balance        NUMERIC(18,2) NOT NULL DEFAULT 100000.00,
    stop_loss_pct       NUMERIC(8,4) NOT NULL DEFAULT 5.0000,
    trailing_stop_pct   NUMERIC(8,4) NOT NULL DEFAULT 3.5000,
    max_positions       INT NOT NULL DEFAULT 1,
    last_candidate_date DATE,
    last_trade_date     DATE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_strategy_candidates (
    id                  BIGSERIAL PRIMARY KEY,
    account_id          BIGINT NOT NULL REFERENCES signal_strategy_accounts(id) ON DELETE CASCADE,
    signal_id           VARCHAR(50) NOT NULL,
    signal_date         DATE NOT NULL,
    code                VARCHAR(12) NOT NULL,
    name                VARCHAR(50),
    score               NUMERIC(10,4) NOT NULL,
    selection_reason    TEXT NOT NULL,
    signal_metadata     JSONB NOT NULL DEFAULT '{}',
    candidate_status    VARCHAR(20) NOT NULL DEFAULT 'pending',
    planned_entry_date  DATE,
    entry_reason        TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, signal_date)
);

CREATE INDEX IF NOT EXISTS idx_signal_strategy_candidates_status
    ON signal_strategy_candidates(candidate_status, planned_entry_date, signal_date);

CREATE TABLE IF NOT EXISTS signal_strategy_positions (
    id                  BIGSERIAL PRIMARY KEY,
    account_id          BIGINT NOT NULL REFERENCES signal_strategy_accounts(id) ON DELETE CASCADE,
    candidate_id        BIGINT REFERENCES signal_strategy_candidates(id) ON DELETE SET NULL,
    signal_id           VARCHAR(50) NOT NULL,
    code                VARCHAR(12) NOT NULL,
    name                VARCHAR(50),
    score               NUMERIC(10,4),
    entry_price         NUMERIC(10,3) NOT NULL,
    shares              INT NOT NULL,
    peak_price          NUMERIC(10,3) NOT NULL,
    stop_loss_price     NUMERIC(10,3) NOT NULL,
    trailing_stop_pct   NUMERIC(8,4) NOT NULL,
    entry_date          DATE NOT NULL,
    entry_time          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    entry_reason        TEXT,
    exit_price          NUMERIC(10,3),
    exit_date           DATE,
    exit_time           TIMESTAMPTZ,
    exit_reason         TEXT,
    pnl_pct             NUMERIC(8,4),
    is_open             BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_signal_strategy_positions_open
    ON signal_strategy_positions(account_id, is_open, entry_date);

CREATE TABLE IF NOT EXISTS signal_strategy_events (
    id                  BIGSERIAL PRIMARY KEY,
    account_id          BIGINT NOT NULL REFERENCES signal_strategy_accounts(id) ON DELETE CASCADE,
    position_id         BIGINT REFERENCES signal_strategy_positions(id) ON DELETE SET NULL,
    candidate_id        BIGINT REFERENCES signal_strategy_candidates(id) ON DELETE SET NULL,
    signal_id           VARCHAR(50) NOT NULL,
    event_type          VARCHAR(30) NOT NULL,
    code                VARCHAR(12),
    title               VARCHAR(120) NOT NULL,
    detail              TEXT NOT NULL,
    event_time          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signal_strategy_events_time
    ON signal_strategy_events(event_time DESC, signal_id);
