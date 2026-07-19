-- Keep the legacy corporate-action version key intact for MarketRepository.
-- CompanyRepository owns a separate immutable dividend revision entity.
DROP INDEX IF EXISTS uq_corporate_action_source_revision;

UPDATE corporate_action_versions
SET source_revision = 'legacy'
WHERE source_revision = '';

ALTER TABLE corporate_action_versions
    ADD CONSTRAINT corporate_action_source_revision_not_blank
    CHECK (source_revision <> '');

CREATE TABLE stock_dividend_versions (
    source                 VARCHAR(32) NOT NULL CHECK (source <> ''),
    action_key             VARCHAR(200) NOT NULL CHECK (action_key <> ''),
    code                   VARCHAR(12) NOT NULL CHECK (code <> ''),
    announcement_date      DATE,
    record_date            DATE,
    ex_date                DATE,
    pay_date               DATE,
    implementation_status  VARCHAR(24) NOT NULL
                           CHECK (implementation_status IN
                                  ('proposed', 'approved', 'implemented', 'unknown')),
    cash_dividend          NUMERIC(18,8),
    cash_dividend_tax      NUMERIC(18,8),
    stock_ratio            NUMERIC(18,8),
    source_revision        VARCHAR(64) NOT NULL CHECK (source_revision <> ''),
    raw_payload            JSONB NOT NULL DEFAULT '{}',
    available_at           TIMESTAMPTZ NOT NULL,
    ingested_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source, action_key, source_revision)
);

DROP INDEX IF EXISTS idx_financial_report_versions_latest;

CREATE INDEX idx_financial_report_versions_latest
    ON stock_financial_report_versions
       (code, frequency, end_date DESC, report_type DESC, available_at DESC,
        source, source_revision DESC);

CREATE INDEX idx_dividend_versions_latest
    ON stock_dividend_versions
       (code, source, action_key, available_at DESC, source_revision DESC);

CREATE INDEX idx_dividend_versions_effective_date
    ON stock_dividend_versions
       (code,
        (COALESCE(ex_date, record_date, announcement_date, DATE '0001-01-01')) DESC,
        source DESC, action_key DESC);

ALTER TABLE company_data_repair_checkpoints
    ADD COLUMN lease_token UUID;

-- Claims created before leases existed cannot have a valid owner. Preserve their
-- audit trail but make them explicitly retryable as failed work.
UPDATE company_data_repair_checkpoints
SET status = 'failed',
    last_error = COALESCE(last_error, 'claim invalidated during lease migration'),
    completed_at = NULL,
    lease_token = NULL,
    updated_at = NOW()
WHERE status = 'running';

UPDATE company_data_repair_checkpoints
SET last_error = NULL,
    completed_at = COALESCE(completed_at, updated_at),
    lease_token = NULL
WHERE status = 'completed';

UPDATE company_data_repair_checkpoints
SET last_error = COALESCE(last_error, 'unknown checkpoint failure'),
    completed_at = NULL,
    lease_token = NULL
WHERE status = 'failed';

ALTER TABLE company_data_repair_checkpoints
    ADD CONSTRAINT company_repair_checkpoint_state_consistent CHECK (
        (status = 'running'
         AND lease_token IS NOT NULL
         AND completed_at IS NULL
         AND last_error IS NULL)
        OR
        (status = 'completed'
         AND lease_token IS NULL
         AND completed_at IS NOT NULL
         AND last_error IS NULL)
        OR
        (status = 'failed'
         AND lease_token IS NULL
         AND completed_at IS NULL
         AND last_error IS NOT NULL)
    );
