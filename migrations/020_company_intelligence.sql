CREATE TABLE IF NOT EXISTS stock_financial_report_versions (
    source               VARCHAR(32) NOT NULL CHECK (source <> ''),
    code                 VARCHAR(12) NOT NULL CHECK (code <> ''),
    end_date             DATE NOT NULL,
    announcement_date    DATE,
    report_type          VARCHAR(16) NOT NULL CHECK (report_type <> ''),
    frequency            VARCHAR(16) NOT NULL
                         CHECK (frequency IN ('annual', 'quarterly')),
    source_revision      VARCHAR(64) NOT NULL CHECK (source_revision <> ''),
    total_revenue        NUMERIC(24,4),
    revenue              NUMERIC(24,4),
    operating_profit     NUMERIC(24,4),
    total_profit         NUMERIC(24,4),
    net_profit_parent    NUMERIC(24,4),
    deducted_net_profit  NUMERIC(24,4),
    basic_eps            NUMERIC(18,6),
    diluted_eps          NUMERIC(18,6),
    roe                  NUMERIC(18,6),
    gross_margin         NUMERIC(18,6),
    net_margin           NUMERIC(18,6),
    revenue_yoy          NUMERIC(18,6),
    net_profit_yoy       NUMERIC(18,6),
    raw_payload          JSONB NOT NULL DEFAULT '{}',
    available_at         TIMESTAMPTZ NOT NULL,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source, code, end_date, report_type, source_revision)
);

ALTER TABLE corporate_action_versions
    ADD COLUMN IF NOT EXISTS implementation_status VARCHAR(24),
    ADD COLUMN IF NOT EXISTS cash_dividend_tax NUMERIC(18,8),
    ADD COLUMN IF NOT EXISTS source_revision VARCHAR(64) NOT NULL DEFAULT 'legacy';

CREATE TABLE IF NOT EXISTS company_data_repair_checkpoints (
    phase          VARCHAR(32) NOT NULL CHECK (phase <> ''),
    code           VARCHAR(12) NOT NULL CHECK (code <> ''),
    start_date     DATE,
    end_date       DATE,
    status         VARCHAR(16) NOT NULL
                   CHECK (status IN ('running', 'completed', 'failed')),
    attempts       INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error     VARCHAR(500),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at   TIMESTAMPTZ,
    PRIMARY KEY (phase, code),
    CHECK (start_date IS NULL OR end_date IS NULL OR start_date <= end_date)
);

CREATE INDEX IF NOT EXISTS idx_financial_report_versions_latest
    ON stock_financial_report_versions
       (code, frequency, end_date DESC, report_type DESC,
        announcement_date DESC NULLS LAST, available_at DESC);

CREATE INDEX IF NOT EXISTS idx_corporate_action_dividend_date
    ON corporate_action_versions
       (code, ex_date DESC NULLS LAST, source, action_key, available_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_corporate_action_source_revision
    ON corporate_action_versions (source, action_key, source_revision)
    WHERE source_revision <> 'legacy';

CREATE INDEX IF NOT EXISTS idx_company_repair_checkpoints_pending
    ON company_data_repair_checkpoints (phase, status, updated_at)
    WHERE status <> 'completed';
