CREATE TABLE IF NOT EXISTS user_portfolio (
    user_id    BIGINT NOT NULL,
    code       VARCHAR(12) NOT NULL,
    cost_price NUMERIC(10,3) NOT NULL,
    shares     INT NOT NULL,
    added_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, code)
);
