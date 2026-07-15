-- Phase 2 (db-per-service): Metering domain — device registry tables.
--
-- Tables: meters, meter_registry, meter_verification_attempts.
-- Extracted verbatim from the shared `gridtokenx` schema (pg_dump). Self-contained:
-- every FK to IAM `users` is intentionally OMITTED (users lives in gridtokenx_iam
-- after the split). See docs/db-split-phase2.md for the meters vs meter-service
-- ownership question — `meters` is WRITTEN by meter-service, not the aggregator.

-- ---------------------------------------------------------------------------
-- meters — canonical device roster (written by meter-service POST /api/v1/meters;
-- READ by the aggregator to resolve serial -> user_id).
-- ---------------------------------------------------------------------------
CREATE TABLE public.meters (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    serial_number character varying(100) NOT NULL,
    meter_type character varying(50),
    location text,
    is_verified boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    latitude double precision,
    longitude double precision,
    zone_id integer,
    public_key character varying(255),
    status character varying(50) DEFAULT 'active'::character varying,
    CONSTRAINT meters_pkey PRIMARY KEY (id),
    CONSTRAINT meters_serial_number_key UNIQUE (serial_number)
);
-- NOTE: original had FK meters_user_id_fkey -> users(id) ON DELETE CASCADE. Dropped
-- for self-containment; user_id is now a soft reference into gridtokenx_iam.

COMMENT ON COLUMN public.meters.latitude IS 'Latitude coordinate for map display';
COMMENT ON COLUMN public.meters.longitude IS 'Longitude coordinate for map display';
COMMENT ON COLUMN public.meters.public_key IS 'Ed25519 public key (base58 encoded) used for IoT device signature verification';
COMMENT ON COLUMN public.meters.status IS 'Operating status of the meter (active, maintenance, decommissioning)';

CREATE INDEX idx_meters_coordinates ON public.meters USING btree (latitude, longitude) WHERE ((latitude IS NOT NULL) AND (longitude IS NOT NULL));
CREATE UNIQUE INDEX idx_meters_public_key ON public.meters USING btree (public_key) WHERE (public_key IS NOT NULL);
CREATE INDEX idx_meters_serial_active ON public.meters USING btree (serial_number, zone_id);
CREATE INDEX idx_meters_serial_number ON public.meters USING btree (serial_number);
CREATE INDEX idx_meters_user_id ON public.meters USING btree (user_id);
CREATE INDEX idx_meters_zone_serial ON public.meters USING btree (zone_id, serial_number) WHERE (zone_id IS NOT NULL);
CREATE UNIQUE INDEX uq_meters_serial_number_canonical ON public.meters USING btree (public.canonicalize_meter_serial((serial_number)::text));

CREATE TRIGGER update_meters_updated_at BEFORE UPDATE ON public.meters
    FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

-- ---------------------------------------------------------------------------
-- meter_registry — verification/enrollment registry (currently dormant in the
-- aggregator; meter_readings.meter_id FK targets it).
-- ---------------------------------------------------------------------------
CREATE TABLE public.meter_registry (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    meter_serial character varying(255) NOT NULL,
    meter_key_hash character varying(255) NOT NULL,
    verification_method character varying(50) DEFAULT 'serial'::character varying NOT NULL,
    verification_status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    manufacturer character varying(255),
    meter_type character varying(50),
    location_address text,
    installation_date date,
    verified_at timestamp with time zone,
    verified_by uuid,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    verification_proof text,
    metadata jsonb DEFAULT '{}'::jsonb,
    meter_public_key character varying(255),
    latitude double precision,
    longitude double precision,
    zone_id integer,
    CONSTRAINT meter_registry_pkey PRIMARY KEY (id),
    CONSTRAINT meter_registry_meter_serial_key UNIQUE (meter_serial)
);
-- NOTE: original had FKs meter_registry_user_id_fkey -> users(id) ON DELETE CASCADE
-- and meter_registry_verified_by_fkey -> users(id) ON DELETE SET NULL. Both dropped
-- for self-containment (users lives in gridtokenx_iam).

COMMENT ON COLUMN public.meter_registry.meter_public_key IS 'Ed25519 public key (base58 encoded) for signature verification';

CREATE UNIQUE INDEX idx_meter_public_key ON public.meter_registry USING btree (meter_public_key) WHERE (meter_public_key IS NOT NULL);
CREATE INDEX idx_meter_registry_serial ON public.meter_registry USING btree (meter_serial);
CREATE INDEX idx_meter_registry_status ON public.meter_registry USING btree (verification_status);
CREATE INDEX idx_meter_registry_user_id ON public.meter_registry USING btree (user_id);
CREATE INDEX idx_meter_registry_zone_id ON public.meter_registry USING btree (zone_id);

-- ---------------------------------------------------------------------------
-- meter_verification_attempts — audit trail of enrollment attempts.
-- ---------------------------------------------------------------------------
CREATE TABLE public.meter_verification_attempts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    meter_serial character varying(255) NOT NULL,
    verification_method character varying(50) NOT NULL,
    attempt_status character varying(20) NOT NULL,
    attempt_result character varying(20),
    ip_address inet,
    user_agent text,
    attempted_at timestamp with time zone DEFAULT now(),
    failure_reason text,
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT meter_verification_attempts_pkey PRIMARY KEY (id)
);
-- NOTE: original had FK meter_verification_attempts_user_id_fkey -> users(id)
-- ON DELETE CASCADE. Dropped for self-containment.

CREATE INDEX idx_meter_verification_attempts_attempted_at ON public.meter_verification_attempts USING btree (attempted_at);
CREATE INDEX idx_meter_verification_attempts_user_id ON public.meter_verification_attempts USING btree (user_id);
