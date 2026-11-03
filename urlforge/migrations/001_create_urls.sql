-- 001_create_urls.sql
CREATE TABLE IF NOT EXISTS urls (
    id            BIGINT PRIMARY KEY,
    short_code    VARCHAR(50) NOT NULL UNIQUE,
    original_url  TEXT NOT NULL,
    user_id       VARCHAR(64),
    custom_alias  VARCHAR(50) UNIQUE,
    expires_at    TIMESTAMPTZ,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
