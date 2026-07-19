-- Company synchronization is resumable per provider request window. Existing
-- stock-level rows are retained as one normalized window each.
ALTER TABLE company_data_repair_checkpoints
    DROP CONSTRAINT company_repair_checkpoint_state_consistent;

ALTER TABLE company_data_repair_checkpoints
    ADD COLUMN lease_expires_at TIMESTAMPTZ;

UPDATE company_data_repair_checkpoints
SET start_date = COALESCE(start_date, DATE '0001-01-01'),
    end_date = COALESCE(end_date, DATE '9999-12-31'),
    lease_expires_at = CASE
        WHEN status = 'running' THEN NOW()
        ELSE NULL
    END;

ALTER TABLE company_data_repair_checkpoints
    ALTER COLUMN start_date SET NOT NULL,
    ALTER COLUMN end_date SET NOT NULL,
    DROP CONSTRAINT company_data_repair_checkpoints_pkey,
    ADD PRIMARY KEY (phase, code, start_date, end_date),
    ADD CONSTRAINT company_repair_checkpoint_state_consistent CHECK (
        (status = 'running'
         AND lease_token IS NOT NULL
         AND lease_expires_at IS NOT NULL
         AND completed_at IS NULL
         AND last_error IS NULL)
        OR
        (status = 'completed'
         AND lease_token IS NULL
         AND lease_expires_at IS NULL
         AND completed_at IS NOT NULL
         AND last_error IS NULL)
        OR
        (status = 'failed'
         AND lease_token IS NULL
         AND lease_expires_at IS NULL
         AND completed_at IS NULL
         AND last_error IS NOT NULL)
    );

DROP INDEX IF EXISTS idx_company_repair_checkpoints_pending;

CREATE INDEX idx_company_repair_checkpoints_pending
    ON company_data_repair_checkpoints
       (phase, status, lease_expires_at, code, start_date, end_date)
    WHERE status <> 'completed';
