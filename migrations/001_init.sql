-- API keys. Only the SHA-256 of the key is stored; the plaintext is shown
-- once at creation and never again, so a database leak doesn't leak keys.
CREATE TABLE IF NOT EXISTS api_keys (
    id               TEXT PRIMARY KEY,
    key_hash         BYTEA NOT NULL UNIQUE,
    name             TEXT NOT NULL,
    rps              DOUBLE PRECISION NOT NULL DEFAULT 0,   -- 0 = unlimited
    burst            INTEGER NOT NULL DEFAULT 0,
    monthly_cap_usd  DOUBLE PRECISION NOT NULL DEFAULT 0,   -- 0 = no cap
    default_strategy TEXT NOT NULL DEFAULT '',              -- '' = model default
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at       TIMESTAMPTZ
);

-- Raw events, used for deduplication: delivery is at-least-once, so the same
-- event can arrive twice (producer retry, consumer restart before offset
-- commit). event_id as primary key makes the second insert a no-op.
-- Production: partition by day and drop partitions after the dedupe window.
CREATE TABLE IF NOT EXISTS usage_events (
    event_id      UUID PRIMARY KEY,
    ts            TIMESTAMPTZ NOT NULL,
    request_id    TEXT NOT NULL,
    key_id        TEXT NOT NULL,
    model         TEXT NOT NULL,
    deployment    TEXT NOT NULL,
    status        TEXT NOT NULL,
    stream        BOOLEAN NOT NULL,
    cache_hit     BOOLEAN NOT NULL,
    attempts      INTEGER NOT NULL,
    input_tokens  INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    estimated     BOOLEAN NOT NULL,
    latency_ms    DOUBLE PRECISION NOT NULL,
    ttft_ms       DOUBLE PRECISION NOT NULL,
    cost_usd      DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS usage_events_ts ON usage_events (ts);

-- Daily aggregates: what dashboards and billing read.
CREATE TABLE IF NOT EXISTS usage_daily (
    day           DATE NOT NULL,
    key_id        TEXT NOT NULL,
    model         TEXT NOT NULL,
    deployment    TEXT NOT NULL,
    requests      BIGINT NOT NULL,
    errors        BIGINT NOT NULL,
    cache_hits    BIGINT NOT NULL,
    input_tokens  BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    cost_usd      DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (day, key_id, model, deployment)
);
