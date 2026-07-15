-- Phase 2 (db-per-service): Metering domain — helper & trigger functions.
--
-- Extracted verbatim from the shared `gridtokenx` schema (pg_dump), scoped to the
-- functions the metering tables depend on. Self-contained: no IAM/trading deps.
--
-- Cutover target DB: `gridtokenx_meter` (see docs/db-split-phase2.md). This file
-- is AUTHORED ONLY — not applied. The main thread owns the physical DB + cutover.
--
-- Ordering note: these run first because later migrations reference them
-- (unique index on meters uses canonicalize_meter_serial; triggers use the two
-- trigger functions; the partition-management helpers are used operationally).

-- Canonicalizes a meter serial: a UUID in any dash/case form collapses to the
-- canonical hyphenated-lowercase text; anything else is trimmed passthrough.
-- Required by the uq_meters_serial_number_canonical unique index (migration 0002).
CREATE OR REPLACE FUNCTION public.canonicalize_meter_serial(raw text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    -- A UUID in any dash/case form casts to the canonical hyphenated-lowercase
    -- text; anything else raises and falls through to the trimmed passthrough.
    RETURN trim(raw)::uuid::text;
EXCEPTION
    WHEN others THEN
        RETURN trim(raw);
END;
$$;

-- Generic updated_at bumper — attached BEFORE UPDATE on meters + meter_readings.
CREATE OR REPLACE FUNCTION public.update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- Auto-advances a meter_readings row pending -> submitted the first time a
-- blockchain tx signature is set; never downgrades an app-set status.
CREATE OR REPLACE FUNCTION public.update_blockchain_status_on_hash() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Only auto-advance pending -> submitted on first tx-signature set; never
    -- downgrade a status the application explicitly set (e.g. 'confirmed').
    IF NEW.blockchain_tx_signature IS NOT NULL
       AND OLD.blockchain_tx_signature IS NULL
       AND NEW.blockchain_status = 'pending' THEN
        NEW.blockchain_status = 'submitted';
        NEW.blockchain_submitted_at = NOW();
    END IF;

    RETURN NEW;
END;
$$;

-- Operational helper: create one monthly partition of meter_readings on demand.
CREATE OR REPLACE FUNCTION public.create_meter_readings_partition(partition_date date) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    partition_name TEXT;
    start_date TEXT;
    end_date TEXT;
BEGIN
    partition_name := 'meter_readings_' || TO_CHAR(partition_date, 'YYYY_MM');
    start_date := TO_CHAR(partition_date, 'YYYY-MM-01 00:00:00+00');
    end_date := TO_CHAR(partition_date + INTERVAL '1 month', 'YYYY-MM-01 00:00:00+00');

    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = partition_name
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF meter_readings FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
        RAISE NOTICE 'Created partition: %', partition_name;
    ELSE
        RAISE NOTICE 'Partition already exists: %', partition_name;
    END IF;
END;
$$;

-- Operational helper: move readings older than retention_days into the archive
-- table. Reproduced as-is from the source schema (see docs/db-split-phase2.md
-- for the known SELECT * column-arity caveat vs meter_readings_archive).
CREATE OR REPLACE FUNCTION public.archive_old_meter_readings(retention_days integer DEFAULT 90)
    RETURNS TABLE(archived_count bigint)
    LANGUAGE plpgsql
    AS $$
DECLARE
    cutoff_date TIMESTAMPTZ;
    rows_archived BIGINT;
BEGIN
    cutoff_date := NOW() - (retention_days || ' days')::INTERVAL;

    -- Insert old readings into archive
    WITH archived AS (
        INSERT INTO meter_readings_archive
        SELECT * FROM meter_readings
        WHERE reading_timestamp < cutoff_date
        RETURNING *
    )
    SELECT COUNT(*) INTO rows_archived FROM archived;

    -- Delete archived readings from main table
    DELETE FROM meter_readings
    WHERE reading_timestamp < cutoff_date;

    RAISE NOTICE 'Archived % meter readings older than %', rows_archived, cutoff_date;

    RETURN QUERY SELECT rows_archived;
END;
$$;
