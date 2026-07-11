CREATE TABLE analysis_decision_support_runs (
    run_id                  UUID PRIMARY KEY,
    trade_date              DATE NOT NULL,
    support_version         VARCHAR(32) NOT NULL,
    market_snapshot_version VARCHAR(32) NOT NULL,
    pattern_set_id          UUID,
    event_brief_version     VARCHAR(32),
    event_score_enabled     BOOLEAN NOT NULL DEFAULT FALSE,
    event_score_limit       NUMERIC(8,4) NOT NULL DEFAULT 0,
    status                  VARCHAR(20) NOT NULL,
    input_fingerprint       VARCHAR(64) NOT NULL,
    started_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at            TIMESTAMPTZ,
    error_message           TEXT,
    UNIQUE (trade_date, support_version)
);

CREATE TABLE analysis_decision_candidates (
    run_id              UUID NOT NULL REFERENCES analysis_decision_support_runs(run_id),
    code                VARCHAR(12) NOT NULL,
    name                VARCHAR(100) NOT NULL,
    horizon             VARCHAR(20) NOT NULL,
    base_source         VARCHAR(40) NOT NULL,
    base_score          NUMERIC(10,4) NOT NULL,
    pattern_score       NUMERIC(10,4),
    event_adjustment    NUMERIC(10,4) NOT NULL DEFAULT 0,
    risk_adjustment     NUMERIC(10,4) NOT NULL DEFAULT 0,
    final_score         NUMERIC(10,4) NOT NULL,
    support_tier        VARCHAR(20) NOT NULL,
    facts               JSONB NOT NULL DEFAULT '[]',
    calculations        JSONB NOT NULL DEFAULT '[]',
    inferences          JSONB NOT NULL DEFAULT '[]',
    unknowns            JSONB NOT NULL DEFAULT '[]',
    risk_flags          JSONB NOT NULL DEFAULT '[]',
    invalidations       JSONB NOT NULL DEFAULT '[]',
    source_refs         JSONB NOT NULL DEFAULT '[]',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, code, horizon)
);

CREATE TABLE analysis_decision_daily_briefs (
    run_id             UUID PRIMARY KEY REFERENCES analysis_decision_support_runs(run_id),
    trade_date         DATE NOT NULL,
    content            TEXT NOT NULL,
    structured_payload JSONB NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_decision_candidates_rank
    ON analysis_decision_candidates(run_id, support_tier, final_score DESC);
