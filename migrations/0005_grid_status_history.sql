-- Phase 2 (db-per-service): Metering domain — grid_status_history.
--
-- Periodic snapshots of aggregate grid metrics for historical analytics.
-- Extracted verbatim from the shared `gridtokenx` schema (pg_dump). Self-contained.

CREATE TABLE public.grid_status_history (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    total_generation double precision NOT NULL,
    total_consumption double precision NOT NULL,
    net_balance double precision NOT NULL,
    active_meters bigint NOT NULL,
    co2_saved_kg double precision NOT NULL,
    "timestamp" timestamp with time zone DEFAULT now() NOT NULL,
    zones_data jsonb,
    CONSTRAINT grid_status_history_pkey PRIMARY KEY (id)
);

COMMENT ON TABLE public.grid_status_history IS 'Stores periodic snapshots of aggregate grid metrics for historical analytics.';
COMMENT ON COLUMN public.grid_status_history.zones_data IS 'JSONB snapshot of per-zone grid metrics {zone_id: {generation, consumption, net_balance, active_meters}}';

CREATE INDEX idx_grid_status_history_timestamp ON public.grid_status_history USING btree ("timestamp" DESC);
