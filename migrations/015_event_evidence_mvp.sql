CREATE TABLE market_event_evidence (
    evidence_id             UUID PRIMARY KEY,
    source_id               VARCHAR(80) NOT NULL,
    source_item_id          VARCHAR(200) NOT NULL,
    source_url              TEXT,
    source_tier             VARCHAR(10) NOT NULL,
    source_terms_version    VARCHAR(80) NOT NULL,
    occurred_at             TIMESTAMPTZ,
    published_at            TIMESTAMPTZ,
    first_seen_at           TIMESTAMPTZ NOT NULL,
    available_at            TIMESTAMPTZ NOT NULL,
    effective_trade_date    DATE NOT NULL,
    title                   TEXT NOT NULL,
    content                 TEXT,
    language                VARCHAR(20) NOT NULL,
    content_hash            VARCHAR(64) NOT NULL,
    raw_payload             JSONB NOT NULL DEFAULT '{}',
    version                 INT NOT NULL,
    supersedes_evidence_id  UUID REFERENCES market_event_evidence(evidence_id),
    status                  VARCHAR(20) NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_id, source_item_id, version)
);

CREATE TABLE market_event_duplicate_groups (
    duplicate_group_id UUID PRIMARY KEY,
    relation_type      VARCHAR(20) NOT NULL,
    confidence         NUMERIC(8,6) NOT NULL,
    locked_by_user     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE market_event_duplicate_members (
    duplicate_group_id UUID NOT NULL REFERENCES market_event_duplicate_groups(duplicate_group_id),
    evidence_id        UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    is_representative  BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (duplicate_group_id, evidence_id)
);

CREATE TABLE market_event_extractions (
    extraction_id       UUID PRIMARY KEY,
    evidence_id         UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    schema_version      VARCHAR(32) NOT NULL,
    prompt_version      VARCHAR(32),
    model_name          VARCHAR(100),
    model_parameters    JSONB NOT NULL DEFAULT '{}',
    extracted_payload   JSONB NOT NULL,
    validation_status   VARCHAR(20) NOT NULL,
    validation_errors   JSONB NOT NULL DEFAULT '[]',
    input_fingerprint   VARCHAR(64) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE market_event_claims (
    claim_id           UUID PRIMARY KEY,
    extraction_id      UUID NOT NULL REFERENCES market_event_extractions(extraction_id),
    claim_type         VARCHAR(40) NOT NULL,
    claim_text         TEXT NOT NULL,
    confidence         NUMERIC(8,6) NOT NULL,
    review_status      VARCHAR(20) NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE market_event_claim_evidence (
    claim_id     UUID NOT NULL REFERENCES market_event_claims(claim_id),
    evidence_id  UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    PRIMARY KEY (claim_id, evidence_id)
);

CREATE TABLE market_event_entities (
    entity_link_id     UUID PRIMARY KEY,
    evidence_id        UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    raw_name           TEXT NOT NULL,
    canonical_type     VARCHAR(40) NOT NULL,
    canonical_id       VARCHAR(100),
    role               VARCHAR(50) NOT NULL,
    match_method       VARCHAR(30) NOT NULL,
    confidence         NUMERIC(8,6) NOT NULL,
    review_status      VARCHAR(20) NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE market_event_claim_graphs (
    claim_graph_id      UUID PRIMARY KEY,
    evidence_id         UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    graph_version       INT NOT NULL,
    schema_version      VARCHAR(32) NOT NULL,
    graph_payload       JSONB NOT NULL,
    review_status       VARCHAR(20) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (evidence_id, graph_version)
);

CREATE TABLE market_event_revisions (
    revision_id         UUID PRIMARY KEY,
    object_type         VARCHAR(40) NOT NULL,
    object_id           UUID NOT NULL,
    previous_payload    JSONB NOT NULL,
    revised_payload     JSONB NOT NULL,
    revised_by          VARCHAR(100) NOT NULL,
    reason              TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE market_event_daily_briefs (
    trade_date          DATE PRIMARY KEY,
    brief_version       VARCHAR(32) NOT NULL,
    content             TEXT NOT NULL,
    structured_payload  JSONB NOT NULL,
    input_fingerprint   VARCHAR(64) NOT NULL,
    generated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_event_evidence_available
    ON market_event_evidence(available_at, effective_trade_date, status);

CREATE INDEX idx_event_claims_review
    ON market_event_claims(review_status, created_at DESC);

CREATE INDEX idx_event_entities_canonical
    ON market_event_entities(canonical_type, canonical_id, review_status);

CREATE OR REPLACE FUNCTION market_event_assert_published_claim_has_evidence()
RETURNS TRIGGER AS $$
DECLARE
    checked_claim_id UUID;
BEGIN
    checked_claim_id := COALESCE(NEW.claim_id, OLD.claim_id);

    IF EXISTS (
        SELECT 1
        FROM market_event_claims c
        WHERE c.claim_id = checked_claim_id
          AND c.review_status = 'published'
          AND NOT EXISTS (
              SELECT 1
              FROM market_event_claim_evidence ce
              WHERE ce.claim_id = c.claim_id
          )
    ) THEN
        RAISE EXCEPTION 'published market event claim % must reference evidence', checked_claim_id;
    END IF;

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_market_event_claims_require_evidence
AFTER INSERT OR UPDATE OF review_status ON market_event_claims
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION market_event_assert_published_claim_has_evidence();

CREATE CONSTRAINT TRIGGER trg_market_event_claim_evidence_keep_published_linked
AFTER DELETE OR UPDATE OF claim_id ON market_event_claim_evidence
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION market_event_assert_published_claim_has_evidence();
