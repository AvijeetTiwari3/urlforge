-- 003_create_indexes.sql

-- Index short_code for active lookups
CREATE INDEX IF NOT EXISTS idx_urls_short_code_active 
    ON urls(short_code) WHERE is_active = TRUE;

-- Index expiration to support automated cleanup sweeps
CREATE INDEX IF NOT EXISTS idx_urls_expires_at 
    ON urls(expires_at) WHERE expires_at IS NOT NULL;

-- Index click events by short_code and timestamp for fast range scans
CREATE INDEX IF NOT EXISTS idx_clicks_short_code_time 
    ON click_events(short_code, clicked_at DESC);
