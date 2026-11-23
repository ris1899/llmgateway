-- API keys: only SHA-256 of the key is stored, plaintext shown once.
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

-- Raw events; event_id PK dedupes at-least-once Kafka delivery.
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
