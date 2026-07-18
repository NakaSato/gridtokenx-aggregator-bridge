-- Phase 2 (db-per-service): Metering domain — user_wallet_read_model.
--
-- Order-independence fix for the owner/wallet feed. The two event streams that
-- build meter_owner_read_model ride separate Kafka topics with different
-- partition keys, so there is NO cross-stream ordering guarantee:
--
--   * meter_events  (meter-service): MeterRegistered/MeterUpdated — serial → user
--   * user_events   (IAM):           UserOnboarded/UserWalletLinked/… — user → wallet
--
-- When a user's wallet event is consumed BEFORE their first MeterRegistered row
-- exists, update_wallet_by_user matched 0 rows and the wallet was silently lost —
-- the read-model row stayed wallet_address NULL forever and every surplus mint
-- for that meter deferred with "no wallet registered" (observed live 2026-07-18,
-- fresh user claiming a meter immediately after e-mail verify).
--
-- This table is the durable user → primary-wallet edge, written by every IAM
-- wallet event regardless of whether the user owns meters yet. upsert_by_serial
-- consults it to fill a wallet the meter event didn't carry, making the merge
-- correct under either arrival order.

CREATE TABLE public.user_wallet_read_model (
    user_id         uuid NOT NULL,
    wallet_address  character varying(88),
    updated_at      timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT user_wallet_read_model_pkey PRIMARY KEY (user_id)
);

COMMENT ON TABLE public.user_wallet_read_model IS
    'Local read-model: user_id -> primary custodial wallet (base58). Written by IAM user events (UserOnboarded/UserWalletLinked/UserWalletPrimaryChanged/UserWalletUnlinked) independent of meter ownership, so a wallet event arriving before the user''s first meter row is never lost. Consulted by upsert_by_serial to fill wallets missing from meter events.';
COMMENT ON COLUMN public.user_wallet_read_model.wallet_address IS
    'Primary wallet (base58). NULLable: an unlink clears it; a NULL means "user currently has no mint-recipient wallet".';
