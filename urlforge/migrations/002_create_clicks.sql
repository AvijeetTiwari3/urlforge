-- 002_create_clicks.sql
CREATE TABLE IF NOT EXISTS click_events (
    id          BIGSERIAL,
    short_code  VARCHAR(50) NOT NULL,
    clicked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ip_hash     VARCHAR(64),
    country     VARCHAR(10),
    device      VARCHAR(20),
    referrer    TEXT
);
