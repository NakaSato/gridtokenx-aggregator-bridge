-- Phase 2 (db-per-service): Metering domain — meter_owner_read_model.
--
-- NEW table (not in the shared schema). This is the durable local read-model that
-- replaces the cross-domain read of IAM `users.wallet_address`:
--
--   Today the aggregator resolves serial -> (user_id, wallet_address) by JOINing
--   the shared `meters` table to IAM-owned `users` (pg_readings.rs:196-197;
--   meter_registry.rs:118-120). After the split, `users` lives in gridtokenx_iam
--   and that JOIN is forbidden (no cross-service SQL).
--
-- This table is the durable promotion of the existing Redis hot cache
-- (gridtokenx:meters:{serial}:user_id | :wallet, meter_registry.rs:149,157). It is
-- kept current by an IAM NATS event feed (user.wallet.* / user.registered) and
-- backfilled once on first boot. See docs/db-split-phase2.md for the full design.
--
-- TODO(db-split): once the aggregator reads owner+wallet from THIS table instead
-- of the meters⋈users JOIN, drop the FOREIGN read at pg_readings.rs:196-197 and
-- meter_registry.rs:118-120. Do NOT remove the JOIN until the read-model is
-- populated + verified (Phase 2 cutover, main thread).

CREATE TABLE public.meter_owner_read_model (
    serial_number   character varying(100) NOT NULL,
    user_id         uuid NOT NULL,
    wallet_address  character varying(88),
    updated_at      timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT meter_owner_read_model_pkey PRIMARY KEY (serial_number)
);

COMMENT ON TABLE public.meter_owner_read_model IS
    'Local read-model: serial_number -> (user_id, owner wallet). Durable promotion of the Redis owner cache, kept current by IAM NATS events + first-boot backfill. Replaces the cross-domain meters⋈users(wallet_address) JOIN.';
COMMENT ON COLUMN public.meter_owner_read_model.wallet_address IS
    'Owner custodial wallet (base58). NULLable: a meter may be registered before its owner has a wallet — the mint path skips a NULL wallet, same as today.';
COMMENT ON COLUMN public.meter_owner_read_model.updated_at IS
    'Last time this row was updated from an IAM event or backfill; used for staleness / last-writer-wins on out-of-order events.';

-- Serial is canonicalized on write (same canonicalize_meter_serial used by the
-- meters unique index), so an exact-serial lookup is the hot path; this partial
-- index covers the wallet-resolution filter (skip owners with no wallet yet).
CREATE INDEX idx_meter_owner_read_model_user_id ON public.meter_owner_read_model USING btree (user_id);
CREATE INDEX idx_meter_owner_read_model_has_wallet ON public.meter_owner_read_model USING btree (serial_number)
    WHERE (wallet_address IS NOT NULL);
