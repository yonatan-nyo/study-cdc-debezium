CREATE TABLE public.order_changes (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('r', 'c', 'u', 'd')),
    order_id BIGINT NOT NULL,
    order_ref TEXT,
    payload JSONB NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (topic, partition_id, kafka_offset)
);

CREATE INDEX order_changes_ref_idx ON public.order_changes (order_ref);

