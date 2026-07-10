CREATE TABLE market_event_mentions (
    mention_id             UUID PRIMARY KEY,
    evidence_id            UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    event_cluster_id       UUID,
    cluster_version        INT,
    mention_time           TIMESTAMPTZ NOT NULL,
    adds_new_fact          BOOLEAN NOT NULL,
    source_independence    NUMERIC(8,6) NOT NULL,
    mention_payload        JSONB NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (event_cluster_id IS NULL AND cluster_version IS NULL)
        OR (event_cluster_id IS NOT NULL AND cluster_version IS NOT NULL)
    )
);

CREATE TABLE market_event_clusters (
    event_cluster_id       UUID NOT NULL,
    cluster_version        INT NOT NULL,
    canonical_title        TEXT NOT NULL,
    event_time             TIMESTAMPTZ,
    first_seen_at          TIMESTAMPTZ NOT NULL,
    last_seen_at           TIMESTAMPTZ NOT NULL,
    lifecycle_status       VARCHAR(20) NOT NULL,
    primary_evidence_id    UUID NOT NULL REFERENCES market_event_evidence(evidence_id),
    representative_ids     UUID[] NOT NULL,
    source_entropy         NUMERIC(10,6) NOT NULL,
    independent_sources    INT NOT NULL,
    mention_count          INT NOT NULL,
    cluster_payload        JSONB NOT NULL,
    supersedes_version     INT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_cluster_id, cluster_version)
);

ALTER TABLE market_event_mentions
    ADD CONSTRAINT fk_event_mentions_cluster
    FOREIGN KEY (event_cluster_id, cluster_version)
    REFERENCES market_event_clusters(event_cluster_id, cluster_version)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE market_event_deltas (
    event_cluster_id       UUID NOT NULL,
    from_version           INT NOT NULL,
    to_version             INT NOT NULL,
    delta_payload          JSONB NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_cluster_id, from_version, to_version),
    FOREIGN KEY (event_cluster_id, from_version)
        REFERENCES market_event_clusters(event_cluster_id, cluster_version),
    FOREIGN KEY (event_cluster_id, to_version)
        REFERENCES market_event_clusters(event_cluster_id, cluster_version),
    CHECK (to_version > from_version)
);

CREATE TABLE market_event_hypotheses (
    hypothesis_id          UUID PRIMARY KEY,
    event_cluster_id       UUID NOT NULL,
    cluster_version        INT NOT NULL,
    hypothesis_version     INT NOT NULL,
    schema_version         VARCHAR(32) NOT NULL,
    graph_payload          JSONB NOT NULL,
    frozen_at              TIMESTAMPTZ NOT NULL,
    based_on_claim_ids     UUID[] NOT NULL,
    review_status          VARCHAR(20) NOT NULL,
    supersedes_id          UUID REFERENCES market_event_hypotheses(hypothesis_id),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_cluster_id, cluster_version, hypothesis_version),
    FOREIGN KEY (event_cluster_id, cluster_version)
        REFERENCES market_event_clusters(event_cluster_id, cluster_version)
);

CREATE TABLE market_event_market_observations (
    hypothesis_id          UUID NOT NULL REFERENCES market_event_hypotheses(hypothesis_id),
    entity_type            VARCHAR(40) NOT NULL,
    entity_id              VARCHAR(100) NOT NULL,
    trade_date             DATE NOT NULL,
    observation_status     VARCHAR(30) NOT NULL,
    market_alignment_score NUMERIC(10,6),
    causal_confidence      NUMERIC(10,6) NOT NULL,
    abnormal_market_return NUMERIC(12,6),
    abnormal_industry_return NUMERIC(12,6),
    market_metrics         JSONB NOT NULL,
    confounding_events     JSONB NOT NULL DEFAULT '[]',
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (hypothesis_id, entity_type, entity_id, trade_date)
);

CREATE TABLE market_event_type_statistics (
    event_type             VARCHAR(50) NOT NULL,
    event_subtype          VARCHAR(50) NOT NULL,
    entity_type            VARCHAR(40) NOT NULL,
    observation_window     VARCHAR(20) NOT NULL,
    sample_count           INT NOT NULL,
    median_abnormal_return NUMERIC(12,6),
    positive_rate          NUMERIC(10,6),
    turnover_response      NUMERIC(12,6),
    breadth_response       NUMERIC(12,6),
    time_to_peak           NUMERIC(12,6),
    failure_rate           NUMERIC(10,6),
    data_cutoff            DATE NOT NULL,
    logic_version          VARCHAR(32) NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (
        event_type, event_subtype, entity_type,
        observation_window, data_cutoff, logic_version
    )
);

CREATE OR REPLACE FUNCTION market_event_reject_hypothesis_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'market_event_hypotheses is append-only; % is not allowed', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_market_event_hypotheses_reject_update
BEFORE UPDATE ON market_event_hypotheses
FOR EACH ROW
EXECUTE FUNCTION market_event_reject_hypothesis_mutation();

CREATE TRIGGER trg_market_event_hypotheses_reject_delete
BEFORE DELETE ON market_event_hypotheses
FOR EACH ROW
EXECUTE FUNCTION market_event_reject_hypothesis_mutation();
