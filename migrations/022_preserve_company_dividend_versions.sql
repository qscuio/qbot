-- Migration 020 stored CompanyRepository dividend revisions in the legacy
-- corporate-action table. Migration 021 gave those revisions a dedicated
-- table, so preserve all unambiguous pre-021 revisions before readers rely on
-- the new table exclusively.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM corporate_action_versions
        WHERE source_revision <> 'legacy'
          AND (
              action_type <> 'dividend'
              OR implementation_status IS NULL
              OR implementation_status NOT IN
                 ('proposed', 'approved', 'implemented', 'unknown')
          )
    ) THEN
        RAISE EXCEPTION
            'unrepresentable nonlegacy corporate action revision during dividend migration';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM corporate_action_versions
        WHERE source_revision <> 'legacy'
        GROUP BY source, action_key, source_revision
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'ambiguous nonlegacy corporate action revisions during dividend migration';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM corporate_action_versions AS source_revision
        JOIN stock_dividend_versions AS destination_revision
          ON destination_revision.source = source_revision.source
         AND destination_revision.action_key = source_revision.action_key
         AND destination_revision.source_revision = source_revision.source_revision
        WHERE source_revision.source_revision <> 'legacy'
          AND ROW(
              destination_revision.code,
              destination_revision.announcement_date,
              destination_revision.record_date,
              destination_revision.ex_date,
              destination_revision.pay_date,
              destination_revision.implementation_status,
              destination_revision.cash_dividend,
              destination_revision.cash_dividend_tax,
              destination_revision.stock_ratio,
              destination_revision.raw_payload,
              destination_revision.available_at,
              destination_revision.ingested_at
          ) IS DISTINCT FROM ROW(
              source_revision.code,
              source_revision.announcement_date,
              source_revision.record_date,
              source_revision.ex_date,
              source_revision.pay_date,
              source_revision.implementation_status,
              source_revision.cash_dividend,
              source_revision.cash_dividend_tax,
              source_revision.stock_ratio,
              source_revision.raw_payload,
              source_revision.available_at,
              source_revision.ingested_at
          )
    ) THEN
        RAISE EXCEPTION
            'immutable dividend migration conflict with existing destination revision';
    END IF;
END
$$;

INSERT INTO stock_dividend_versions (
    source,
    action_key,
    code,
    announcement_date,
    record_date,
    ex_date,
    pay_date,
    implementation_status,
    cash_dividend,
    cash_dividend_tax,
    stock_ratio,
    source_revision,
    raw_payload,
    available_at,
    ingested_at
)
SELECT
    source,
    action_key,
    code,
    announcement_date,
    record_date,
    ex_date,
    pay_date,
    implementation_status,
    cash_dividend,
    cash_dividend_tax,
    stock_ratio,
    source_revision,
    raw_payload,
    available_at,
    ingested_at
FROM corporate_action_versions
WHERE source_revision <> 'legacy'
ORDER BY source, action_key, source_revision
ON CONFLICT (source, action_key, source_revision) DO NOTHING;
