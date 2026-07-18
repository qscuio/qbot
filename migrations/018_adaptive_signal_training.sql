CREATE TABLE IF NOT EXISTS adaptive_signal_training_samples (
    id                    BIGSERIAL PRIMARY KEY,
    trade_date            DATE NOT NULL,
    code                  VARCHAR(12) NOT NULL,
    name                  VARCHAR(50),
    signal_id             VARCHAR(50) NOT NULL,
    signal_name           VARCHAR(100),
    market_regime         VARCHAR(50) NOT NULL,
    features              JSONB NOT NULL DEFAULT '{}',
    entry_close           NUMERIC(12,4) NOT NULL,
    return_1d_pct         NUMERIC(10,4),
    return_3d_pct         NUMERIC(10,4),
    return_5d_pct         NUMERIC(10,4),
    return_10d_pct        NUMERIC(10,4),
    return_20d_pct        NUMERIC(10,4),
    max_drawdown_5d_pct   NUMERIC(10,4),
    max_drawdown_20d_pct  NUMERIC(10,4),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (trade_date, signal_id, code)
);

CREATE INDEX IF NOT EXISTS idx_adaptive_training_samples_date
    ON adaptive_signal_training_samples(trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_adaptive_training_samples_signal_regime
    ON adaptive_signal_training_samples(signal_id, market_regime, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_adaptive_training_samples_regime
    ON adaptive_signal_training_samples(market_regime, trade_date DESC);

CREATE TABLE IF NOT EXISTS adaptive_signal_model_weights (
    id              BIGSERIAL PRIMARY KEY,
    signal_id       VARCHAR(50) NOT NULL,
    signal_name     VARCHAR(100),
    market_regime   VARCHAR(50) NOT NULL,
    horizon_days    INTEGER NOT NULL,
    samples         INTEGER NOT NULL,
    avg_return_pct  NUMERIC(10,4) NOT NULL,
    win_rate_pct    NUMERIC(10,4) NOT NULL,
    score           NUMERIC(10,4) NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (signal_id, market_regime, horizon_days)
);

CREATE INDEX IF NOT EXISTS idx_adaptive_signal_model_weights_score
    ON adaptive_signal_model_weights(market_regime, horizon_days, score DESC);
