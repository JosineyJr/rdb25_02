CREATE UNLOGGED TABLE IF NOT EXISTS payments (
    amount BIGINT NOT NULL,
    processor TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_processor_time ON payments (processor, requested_at);
