-- Phase 2 (db-per-service): Metering domain — oracle_submissions.
--
-- Audit trail for meter readings submitted via Oracle Bridge to Solana.
-- Extracted verbatim from the shared `gridtokenx` schema (pg_dump). Self-contained
-- (bigint identity via its own sequence; no cross-domain FKs in source).

CREATE SEQUENCE public.oracle_submissions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE public.oracle_submissions (
    id bigint DEFAULT nextval('public.oracle_submissions_id_seq'::regclass) NOT NULL,
    reading_id uuid NOT NULL,
    meter_id uuid NOT NULL,
    meter_serial character varying(255) NOT NULL,
    user_id uuid NOT NULL,
    wallet_address character varying(255) NOT NULL,
    zone_id integer,
    kwh numeric(12,9) DEFAULT 0 NOT NULL,
    energy_generated numeric(12,9),
    energy_consumed numeric(12,9),
    reading_timestamp bigint NOT NULL,
    signature character varying(255) NOT NULL,
    status character varying(50) DEFAULT 'submitted'::character varying NOT NULL,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    CONSTRAINT oracle_submissions_pkey PRIMARY KEY (id),
    CONSTRAINT oracle_submissions_reading_id_key UNIQUE (reading_id)
);

ALTER SEQUENCE public.oracle_submissions_id_seq OWNED BY public.oracle_submissions.id;

COMMENT ON TABLE public.oracle_submissions IS 'Audit trail for meter readings submitted via Oracle Bridge to Solana blockchain';
COMMENT ON COLUMN public.oracle_submissions.reading_id IS 'Unique identifier for the reading from Oracle Bridge';
COMMENT ON COLUMN public.oracle_submissions.reading_timestamp IS 'Unix timestamp of the meter reading';
COMMENT ON COLUMN public.oracle_submissions.signature IS 'Solana transaction signature for on-chain submission';
COMMENT ON COLUMN public.oracle_submissions.status IS 'Submission status: submitted, confirmed, failed';

CREATE INDEX idx_oracle_submissions_created_at ON public.oracle_submissions USING btree (created_at DESC);
CREATE INDEX idx_oracle_submissions_meter_id ON public.oracle_submissions USING btree (meter_id);
CREATE INDEX idx_oracle_submissions_meter_serial ON public.oracle_submissions USING btree (meter_serial);
CREATE INDEX idx_oracle_submissions_reading_id ON public.oracle_submissions USING btree (reading_id);
CREATE INDEX idx_oracle_submissions_reading_timestamp ON public.oracle_submissions USING btree (reading_timestamp DESC);
CREATE INDEX idx_oracle_submissions_signature ON public.oracle_submissions USING btree (signature);
CREATE INDEX idx_oracle_submissions_status ON public.oracle_submissions USING btree (status);
CREATE INDEX idx_oracle_submissions_user_id ON public.oracle_submissions USING btree (user_id);
