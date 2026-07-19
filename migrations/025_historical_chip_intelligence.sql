-- Preserve migration-005 rows exactly. Their `percentage` values are relative
-- peak intensities, not normalized probability mass, so provenance includes an
-- explicit encoding that the repository handles separately.
ALTER TABLE chip_distribution
    ADD COLUMN IF NOT EXISTS source VARCHAR(32),
    ADD COLUMN IF NOT EXISTS model_version VARCHAR(64),
    ADD COLUMN IF NOT EXISTS dominant_peak_price NUMERIC(18,6),
    ADD COLUMN IF NOT EXISTS validated BOOLEAN,
    ADD COLUMN IF NOT EXISTS source_updated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS distribution_format VARCHAR(32);

UPDATE chip_distribution
SET source = COALESCE(source, 'legacy'),
    validated = COALESCE(validated, FALSE),
    source_updated_at = COALESCE(source_updated_at, updated_at, NOW()),
    distribution_format = COALESCE(distribution_format, 'legacy_peak_relative')
WHERE source IS NULL
   OR validated IS NULL
   OR source_updated_at IS NULL
   OR distribution_format IS NULL;

ALTER TABLE chip_distribution
    ALTER COLUMN source SET DEFAULT 'legacy',
    ALTER COLUMN source SET NOT NULL,
    ALTER COLUMN validated SET DEFAULT FALSE,
    ALTER COLUMN validated SET NOT NULL,
    ALTER COLUMN source_updated_at SET DEFAULT NOW(),
    ALTER COLUMN source_updated_at SET NOT NULL,
    ALTER COLUMN distribution_format SET DEFAULT 'legacy_peak_relative',
    ALTER COLUMN distribution_format SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chip_distribution_source_valid'
          AND conrelid = 'chip_distribution'::regclass
    ) THEN
        ALTER TABLE chip_distribution
            ADD CONSTRAINT chip_distribution_source_valid
            CHECK (source IN ('legacy', 'qbot_estimate', 'tushare'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chip_distribution_format_valid'
          AND conrelid = 'chip_distribution'::regclass
    ) THEN
        ALTER TABLE chip_distribution
            ADD CONSTRAINT chip_distribution_format_valid
            CHECK (distribution_format IN
                   ('legacy_peak_relative', 'normalized_probability'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chip_distribution_validation_provenance'
          AND conrelid = 'chip_distribution'::regclass
    ) THEN
        ALTER TABLE chip_distribution
            ADD CONSTRAINT chip_distribution_validation_provenance
            CHECK (NOT validated OR source <> 'legacy');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS chip_model_states (
    code           VARCHAR(12) NOT NULL CHECK (code <> ''),
    model_version  VARCHAR(64) NOT NULL CHECK (model_version <> ''),
    through_date   DATE NOT NULL,
    distribution   JSONB NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, model_version)
);

CREATE TABLE IF NOT EXISTS chip_model_validation_runs (
    run_id              UUID PRIMARY KEY,
    model_version       VARCHAR(64) NOT NULL CHECK (model_version <> ''),
    sample_definition   JSONB NOT NULL,
    aggregate_metrics   JSONB NOT NULL,
    subgroup_metrics    JSONB NOT NULL,
    decision            VARCHAR(16)
                        CHECK (decision IN ('estimate', 'official')),
    started_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    error_summary       TEXT,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (completed_at IS NULL OR completed_at >= started_at)
);

CREATE INDEX IF NOT EXISTS idx_chip_distribution_latest
    ON chip_distribution (code, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_chip_validation_latest_decision
    ON chip_model_validation_runs
       (model_version, completed_at DESC, run_id DESC)
    WHERE completed_at IS NOT NULL
      AND decision IS NOT NULL
      AND error_summary IS NULL;
