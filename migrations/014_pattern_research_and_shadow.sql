CREATE TABLE analysis_dataset_manifests (
    dataset_version      VARCHAR(80) PRIMARY KEY,
    schema_version       VARCHAR(32) NOT NULL,
    feature_version      VARCHAR(32) NOT NULL,
    horizon              VARCHAR(20) NOT NULL,
    data_cutoff          DATE NOT NULL,
    available_at_cutoff  TIMESTAMPTZ NOT NULL,
    row_count            BIGINT NOT NULL,
    date_from            DATE NOT NULL,
    date_to              DATE NOT NULL,
    manifest             JSONB NOT NULL,
    input_fingerprint    VARCHAR(64) NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE analysis_training_runs (
    run_id               UUID PRIMARY KEY,
    horizon              VARCHAR(20) NOT NULL,
    dataset_version      VARCHAR(80) NOT NULL REFERENCES analysis_dataset_manifests(dataset_version),
    status               VARCHAR(20) NOT NULL,
    config               JSONB NOT NULL,
    code_version         VARCHAR(80) NOT NULL,
    started_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at         TIMESTAMPTZ,
    error_message        TEXT
);

CREATE TABLE analysis_pattern_versions (
    pattern_version_id   UUID PRIMARY KEY,
    pattern_id           VARCHAR(80) NOT NULL,
    horizon              VARCHAR(20) NOT NULL,
    pattern_type         VARCHAR(40) NOT NULL,
    status               VARCHAR(20) NOT NULL,
    schema_version       VARCHAR(32) NOT NULL,
    feature_version      VARCHAR(32) NOT NULL,
    logic_version        VARCHAR(32) NOT NULL,
    dataset_version      VARCHAR(80) NOT NULL REFERENCES analysis_dataset_manifests(dataset_version),
    model_payload        JSONB NOT NULL,
    validation_payload   JSONB NOT NULL,
    trained_from         DATE NOT NULL,
    trained_until        DATE NOT NULL,
    available_at_cutoff  TIMESTAMPTZ NOT NULL,
    approved_by          VARCHAR(100),
    published_at         TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pattern_id, pattern_version_id),
    UNIQUE (pattern_version_id, horizon, pattern_type),
    CONSTRAINT analysis_pattern_versions_published_contract_check
        CHECK (
            status <> 'published'
            OR (
                horizon IN ('week', 'month')
                AND approved_by IS NOT NULL
                AND published_at IS NOT NULL
            )
        )
);

CREATE TABLE analysis_pattern_sets (
    pattern_set_id       UUID PRIMARY KEY,
    name                 VARCHAR(100) NOT NULL,
    status               VARCHAR(20) NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at         TIMESTAMPTZ,
    CONSTRAINT analysis_pattern_sets_published_contract_check
        CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE TABLE analysis_pattern_set_members (
    pattern_set_id       UUID NOT NULL REFERENCES analysis_pattern_sets(pattern_set_id),
    pattern_version_id   UUID NOT NULL REFERENCES analysis_pattern_versions(pattern_version_id),
    member_order         INT NOT NULL,
    PRIMARY KEY (pattern_set_id, pattern_version_id),
    UNIQUE (pattern_set_id, member_order)
);

CREATE TABLE analysis_pattern_examples (
    pattern_version_id   UUID NOT NULL REFERENCES analysis_pattern_versions(pattern_version_id),
    example_type         VARCHAR(20) NOT NULL,
    code                 VARCHAR(12) NOT NULL,
    trade_date           DATE NOT NULL,
    similarity           NUMERIC(10,6),
    metadata             JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (pattern_version_id, example_type, code, trade_date)
);

CREATE TABLE analysis_shadow_candidates (
    trade_date           DATE NOT NULL,
    code                 VARCHAR(12) NOT NULL,
    name                 VARCHAR(120),
    horizon              VARCHAR(20) NOT NULL,
    pattern_version_id   UUID NOT NULL REFERENCES analysis_pattern_versions(pattern_version_id),
    pattern_set_id       UUID NOT NULL REFERENCES analysis_pattern_sets(pattern_set_id),
    pattern_type         VARCHAR(40) NOT NULL,
    similarity_score     NUMERIC(10,6) NOT NULL,
    validated_lift       NUMERIC(10,6) NOT NULL,
    final_score          NUMERIC(10,4) NOT NULL,
    shadow_tier          VARCHAR(20) NOT NULL,
    matched_features     JSONB NOT NULL,
    risk_flags           JSONB NOT NULL,
    supporting_signals   JSONB NOT NULL,
    invalidations        JSONB NOT NULL,
    input_fingerprint    VARCHAR(64) NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, code, horizon, pattern_version_id),
    CONSTRAINT analysis_shadow_candidates_pattern_version_fk
        FOREIGN KEY (pattern_version_id, horizon, pattern_type)
        REFERENCES analysis_pattern_versions(pattern_version_id, horizon, pattern_type),
    CONSTRAINT analysis_shadow_candidates_pattern_set_member_fk
        FOREIGN KEY (pattern_set_id, pattern_version_id)
        REFERENCES analysis_pattern_set_members(pattern_set_id, pattern_version_id)
);

CREATE INDEX idx_pattern_versions_status
    ON analysis_pattern_versions(status, horizon, pattern_type);

CREATE INDEX idx_shadow_candidates_date
    ON analysis_shadow_candidates(trade_date DESC, horizon, shadow_tier, final_score DESC);
