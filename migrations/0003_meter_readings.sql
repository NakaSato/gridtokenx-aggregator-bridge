-- Phase 2 (db-per-service): Metering domain — meter_readings (partitioned).
--
-- This is the aggregator's WRITE target (append-only INSERT…SELECT at
-- crates/aggregator-persistence/src/infra/pg_readings.rs:183).
--
-- Partition scheme (preserved exactly from the shared `gridtokenx` schema):
--   * meter_readings          — partitioned parent, RANGE (reading_timestamp),
--                               PK (id, reading_timestamp) (partition key in PK).
--   * meter_readings_YYYY_MM  — one monthly range partition per existing month
--                               (2024_11, 2024_12, 2025_01..03, 2026_07..12).
--   * meter_readings_default  — DEFAULT partition (catch-all for out-of-range rows).
--   * meter_readings_archive  — STANDALONE table (NOT a partition; retention sink
--                               for archive_old_meter_readings()).
--   * meter_readings_all      — VIEW: current UNION ALL archive.
--
-- Faithful-but-cleaner reproduction: the source pg_dump created each child as a
-- standalone table then ATTACHed it; here children use `PARTITION OF` and indexes
-- are declared on the parent so Postgres auto-propagates them to every child
-- (existing + future). Net on-disk structure is identical. Self-contained: the FK
-- to IAM `users` is dropped; the FK to owned meter_registry is kept.

-- ---------------------------------------------------------------------------
-- Partitioned parent
-- ---------------------------------------------------------------------------
CREATE TABLE public.meter_readings (
    id uuid NOT NULL,
    meter_serial character varying(50),
    meter_id uuid,
    user_id uuid,
    wallet_address character varying(88) NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    energy_generated numeric(12,4),
    energy_consumed numeric(12,4),
    surplus_energy numeric(12,4),
    deficit_energy numeric(12,4),
    kwh_amount numeric(12,4),
    battery_level numeric(5,2),
    temperature numeric(5,2),
    voltage numeric(8,2),
    current numeric(8,2),
    minted boolean DEFAULT false,
    mint_signature character varying(88),
    mint_tx_signature character varying(88),
    blockchain_tx_signature character varying(88),
    blockchain_tx_type character varying(50) DEFAULT 'meter_reading'::character varying,
    blockchain_status character varying(20) DEFAULT 'pending'::character varying,
    blockchain_attempts integer DEFAULT 0,
    blockchain_last_error text,
    blockchain_submitted_at timestamp with time zone,
    blockchain_confirmed_at timestamp with time zone,
    blockchain_registered boolean DEFAULT false,
    on_chain_confirmed boolean DEFAULT false,
    on_chain_slot bigint,
    on_chain_confirmed_at timestamp with time zone,
    verification_status character varying(20) DEFAULT 'legacy_unverified'::character varying,
    reading_timestamp timestamp with time zone DEFAULT now() NOT NULL,
    submitted_at timestamp with time zone DEFAULT now(),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    current_amps double precision,
    power_factor double precision,
    frequency double precision,
    latitude double precision,
    longitude double precision,
    weather_condition character varying(50),
    rec_eligible boolean DEFAULT false,
    carbon_offset double precision,
    max_sell_price double precision,
    max_buy_price double precision,
    meter_signature text,
    meter_type character varying(50),
    thd_voltage double precision,
    thd_current double precision,
    health_score double precision DEFAULT 100.0,
    zone_id integer,
    CONSTRAINT meter_readings_partitioned_pkey PRIMARY KEY (id, reading_timestamp)
)
PARTITION BY RANGE (reading_timestamp);
-- NOTE: original had FK meter_readings_user_id_fkey -> users(id) ON DELETE SET NULL.
-- Dropped for self-containment. The meter_id FK -> meter_registry(id) (owned) is
-- re-added at the bottom of this file after the partitions exist.

COMMENT ON COLUMN public.meter_readings.temperature IS 'Temperature in Celsius';
COMMENT ON COLUMN public.meter_readings.voltage IS 'Grid voltage in Volts';
COMMENT ON COLUMN public.meter_readings.current_amps IS 'Current in Amperes';
COMMENT ON COLUMN public.meter_readings.power_factor IS 'Power factor (0-1)';
COMMENT ON COLUMN public.meter_readings.frequency IS 'Grid frequency in Hz';
COMMENT ON COLUMN public.meter_readings.rec_eligible IS 'Renewable Energy Certificate eligible';

-- ---------------------------------------------------------------------------
-- Monthly range partitions (mirror the source ATTACH bounds exactly)
-- ---------------------------------------------------------------------------
CREATE TABLE public.meter_readings_2024_11 PARTITION OF public.meter_readings
    FOR VALUES FROM ('2024-11-01 00:00:00+00') TO ('2024-12-01 00:00:00+00');
CREATE TABLE public.meter_readings_2024_12 PARTITION OF public.meter_readings
    FOR VALUES FROM ('2024-12-01 00:00:00+00') TO ('2025-01-01 00:00:00+00');
CREATE TABLE public.meter_readings_2025_01 PARTITION OF public.meter_readings
    FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2025-02-01 00:00:00+00');
CREATE TABLE public.meter_readings_2025_02 PARTITION OF public.meter_readings
    FOR VALUES FROM ('2025-02-01 00:00:00+00') TO ('2025-03-01 00:00:00+00');
CREATE TABLE public.meter_readings_2025_03 PARTITION OF public.meter_readings
    FOR VALUES FROM ('2025-03-01 00:00:00+00') TO ('2025-04-01 00:00:00+00');
CREATE TABLE public.meter_readings_2026_07 PARTITION OF public.meter_readings
    FOR VALUES FROM ('2026-07-01 00:00:00+00') TO ('2026-08-01 00:00:00+00');
CREATE TABLE public.meter_readings_2026_08 PARTITION OF public.meter_readings
    FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-09-01 00:00:00+00');
CREATE TABLE public.meter_readings_2026_09 PARTITION OF public.meter_readings
    FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');
CREATE TABLE public.meter_readings_2026_10 PARTITION OF public.meter_readings
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');
CREATE TABLE public.meter_readings_2026_11 PARTITION OF public.meter_readings
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');
CREATE TABLE public.meter_readings_2026_12 PARTITION OF public.meter_readings
    FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');

-- DEFAULT partition (catch-all)
CREATE TABLE public.meter_readings_default PARTITION OF public.meter_readings DEFAULT;

-- ---------------------------------------------------------------------------
-- Indexes on the partitioned parent — auto-propagated to all partitions.
-- (Source dumped these as ON ONLY parent + per-child ATTACH; declaring on the
-- live parent yields the same partitioned-index tree.)
-- ---------------------------------------------------------------------------
CREATE INDEX idx_meter_readings_meter_timestamp ON public.meter_readings USING btree (meter_id, "timestamp" DESC);
CREATE INDEX idx_meter_readings_part_blockchain_status ON public.meter_readings USING btree (blockchain_status);
CREATE INDEX idx_meter_readings_part_blockchain_submitted ON public.meter_readings USING btree (blockchain_submitted_at);
CREATE INDEX idx_meter_readings_part_meter ON public.meter_readings USING btree (meter_serial);
CREATE INDEX idx_meter_readings_part_mint_tx ON public.meter_readings USING btree (mint_tx_signature);
CREATE INDEX idx_meter_readings_part_on_chain_confirmed ON public.meter_readings USING btree (on_chain_confirmed);
CREATE INDEX idx_meter_readings_part_on_chain_slot ON public.meter_readings USING btree (on_chain_slot);
CREATE INDEX idx_meter_readings_part_reading_timestamp ON public.meter_readings USING btree (reading_timestamp);
CREATE INDEX idx_meter_readings_part_registered ON public.meter_readings USING btree (blockchain_registered);
CREATE INDEX idx_meter_readings_part_timestamp ON public.meter_readings USING btree ("timestamp");
CREATE INDEX idx_meter_readings_part_tx_signature ON public.meter_readings USING btree (blockchain_tx_signature);
CREATE INDEX idx_meter_readings_part_tx_type ON public.meter_readings USING btree (blockchain_tx_type);
CREATE INDEX idx_meter_readings_part_user ON public.meter_readings USING btree (user_id);
CREATE INDEX idx_meter_readings_part_wallet ON public.meter_readings USING btree (wallet_address);
CREATE INDEX idx_meter_readings_resolved_mint ON public.meter_readings USING btree ("timestamp" DESC)
    WHERE (COALESCE(minted, false) OR COALESCE(on_chain_confirmed, false)
           OR ((blockchain_status)::text = 'failed'::text) OR (blockchain_last_error IS NOT NULL));
CREATE INDEX idx_meter_readings_serial_timestamp ON public.meter_readings USING btree (meter_serial, "timestamp" DESC);
CREATE INDEX idx_meter_readings_timestamp ON public.meter_readings USING brin ("timestamp");
CREATE INDEX idx_meter_readings_wallet_timestamp ON public.meter_readings USING btree (wallet_address, "timestamp" DESC);
CREATE INDEX idx_meter_readings_zone_id ON public.meter_readings USING btree (zone_id);

-- Triggers on the partitioned parent (fire per-row across all partitions).
CREATE TRIGGER update_meter_readings_blockchain_status BEFORE UPDATE ON public.meter_readings
    FOR EACH ROW EXECUTE FUNCTION public.update_blockchain_status_on_hash();
CREATE TRIGGER update_meter_readings_updated_at BEFORE UPDATE ON public.meter_readings
    FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

-- ---------------------------------------------------------------------------
-- Archive — standalone table (NOT a partition). Fewer columns than the parent
-- (matches source: no current_amps..zone_id tail). Retention sink.
-- ---------------------------------------------------------------------------
CREATE TABLE public.meter_readings_archive (
    id uuid NOT NULL,
    meter_serial character varying(50),
    meter_id uuid,
    user_id uuid,
    wallet_address character varying(88) NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    energy_generated numeric(12,4),
    energy_consumed numeric(12,4),
    surplus_energy numeric(12,4),
    deficit_energy numeric(12,4),
    kwh_amount numeric(12,4),
    battery_level numeric(5,2),
    temperature numeric(5,2),
    voltage numeric(8,2),
    current numeric(8,2),
    minted boolean DEFAULT false,
    mint_signature character varying(88),
    mint_tx_signature character varying(88),
    blockchain_tx_signature character varying(88),
    blockchain_tx_type character varying(50) DEFAULT 'meter_reading'::character varying,
    blockchain_status character varying(20) DEFAULT 'pending'::character varying,
    blockchain_attempts integer DEFAULT 0,
    blockchain_last_error text,
    blockchain_submitted_at timestamp with time zone,
    blockchain_confirmed_at timestamp with time zone,
    blockchain_registered boolean DEFAULT false,
    on_chain_confirmed boolean DEFAULT false,
    on_chain_slot bigint,
    on_chain_confirmed_at timestamp with time zone,
    verification_status character varying(20) DEFAULT 'legacy_unverified'::character varying,
    reading_timestamp timestamp with time zone DEFAULT now() NOT NULL,
    submitted_at timestamp with time zone DEFAULT now(),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT meter_readings_archive_pkey PRIMARY KEY (id, reading_timestamp)
);

COMMENT ON TABLE public.meter_readings_archive IS 'Archive for meter readings older than 90 days';

CREATE INDEX idx_meter_readings_archive_timestamp ON public.meter_readings_archive USING btree (reading_timestamp);
CREATE INDEX idx_meter_readings_archive_user ON public.meter_readings_archive USING btree (user_id);
CREATE INDEX idx_meter_readings_archive_wallet ON public.meter_readings_archive USING btree (wallet_address);
CREATE INDEX meter_readings_archive_blockchain_registered_idx ON public.meter_readings_archive USING btree (blockchain_registered);
CREATE INDEX meter_readings_archive_blockchain_status_idx ON public.meter_readings_archive USING btree (blockchain_status);
CREATE INDEX meter_readings_archive_blockchain_submitted_at_idx ON public.meter_readings_archive USING btree (blockchain_submitted_at);
CREATE INDEX meter_readings_archive_blockchain_tx_signature_idx ON public.meter_readings_archive USING btree (blockchain_tx_signature);
CREATE INDEX meter_readings_archive_blockchain_tx_type_idx ON public.meter_readings_archive USING btree (blockchain_tx_type);
CREATE INDEX meter_readings_archive_meter_id_timestamp_idx ON public.meter_readings_archive USING btree (meter_id, "timestamp" DESC);
CREATE INDEX meter_readings_archive_meter_serial_idx ON public.meter_readings_archive USING btree (meter_serial);
CREATE INDEX meter_readings_archive_mint_tx_signature_idx ON public.meter_readings_archive USING btree (mint_tx_signature);
CREATE INDEX meter_readings_archive_on_chain_confirmed_idx ON public.meter_readings_archive USING btree (on_chain_confirmed);
CREATE INDEX meter_readings_archive_on_chain_slot_idx ON public.meter_readings_archive USING btree (on_chain_slot);
CREATE INDEX meter_readings_archive_reading_timestamp_idx ON public.meter_readings_archive USING btree (reading_timestamp);
CREATE INDEX meter_readings_archive_timestamp_idx ON public.meter_readings_archive USING btree ("timestamp");
CREATE INDEX meter_readings_archive_timestamp_idx1 ON public.meter_readings_archive USING brin ("timestamp");
CREATE INDEX meter_readings_archive_user_id_idx ON public.meter_readings_archive USING btree (user_id);
CREATE INDEX meter_readings_archive_wallet_address_idx ON public.meter_readings_archive USING btree (wallet_address);
CREATE INDEX meter_readings_archive_wallet_address_timestamp_idx ON public.meter_readings_archive USING btree (wallet_address, "timestamp" DESC);

-- ---------------------------------------------------------------------------
-- meter_readings_all — combined current + archive view.
-- ---------------------------------------------------------------------------
CREATE VIEW public.meter_readings_all AS
 SELECT meter_readings.id, meter_readings.meter_serial, meter_readings.meter_id,
    meter_readings.user_id, meter_readings.wallet_address, meter_readings."timestamp",
    meter_readings.energy_generated, meter_readings.energy_consumed,
    meter_readings.surplus_energy, meter_readings.deficit_energy, meter_readings.kwh_amount,
    meter_readings.battery_level, meter_readings.temperature, meter_readings.voltage,
    meter_readings.current, meter_readings.minted, meter_readings.mint_signature,
    meter_readings.mint_tx_signature, meter_readings.blockchain_tx_signature,
    meter_readings.blockchain_tx_type, meter_readings.blockchain_status,
    meter_readings.blockchain_attempts, meter_readings.blockchain_last_error,
    meter_readings.blockchain_submitted_at, meter_readings.blockchain_confirmed_at,
    meter_readings.blockchain_registered, meter_readings.on_chain_confirmed,
    meter_readings.on_chain_slot, meter_readings.on_chain_confirmed_at,
    meter_readings.verification_status, meter_readings.reading_timestamp,
    meter_readings.submitted_at, meter_readings.created_at, meter_readings.updated_at,
    false AS is_archived
   FROM public.meter_readings
UNION ALL
 SELECT meter_readings_archive.id, meter_readings_archive.meter_serial, meter_readings_archive.meter_id,
    meter_readings_archive.user_id, meter_readings_archive.wallet_address, meter_readings_archive."timestamp",
    meter_readings_archive.energy_generated, meter_readings_archive.energy_consumed,
    meter_readings_archive.surplus_energy, meter_readings_archive.deficit_energy, meter_readings_archive.kwh_amount,
    meter_readings_archive.battery_level, meter_readings_archive.temperature, meter_readings_archive.voltage,
    meter_readings_archive.current, meter_readings_archive.minted, meter_readings_archive.mint_signature,
    meter_readings_archive.mint_tx_signature, meter_readings_archive.blockchain_tx_signature,
    meter_readings_archive.blockchain_tx_type, meter_readings_archive.blockchain_status,
    meter_readings_archive.blockchain_attempts, meter_readings_archive.blockchain_last_error,
    meter_readings_archive.blockchain_submitted_at, meter_readings_archive.blockchain_confirmed_at,
    meter_readings_archive.blockchain_registered, meter_readings_archive.on_chain_confirmed,
    meter_readings_archive.on_chain_slot, meter_readings_archive.on_chain_confirmed_at,
    meter_readings_archive.verification_status, meter_readings_archive.reading_timestamp,
    meter_readings_archive.submitted_at, meter_readings_archive.created_at, meter_readings_archive.updated_at,
    true AS is_archived
   FROM public.meter_readings_archive;

COMMENT ON VIEW public.meter_readings_all IS 'Combined view of current and archived meter readings';

-- Intra-domain FK (both tables owned by metering): re-add after partitions exist.
ALTER TABLE public.meter_readings
    ADD CONSTRAINT meter_readings_meter_id_fkey FOREIGN KEY (meter_id)
    REFERENCES public.meter_registry(id) ON DELETE SET NULL;
