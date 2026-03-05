CREATE TABLE IF NOT EXISTS user_watchlist (
    user_id  BIGINT NOT NULL,
    code     VARCHAR(12) NOT NULL,
    added_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, code)
);
