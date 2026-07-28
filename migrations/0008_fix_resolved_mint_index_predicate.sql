-- Fix: idx_meter_readings_resolved_mint predicate is missing the
-- blockchain_status = 'no_surplus' branch that meter-service's
-- list_resolved_mint_readings query (meter-persistence repository/meter.rs)
-- now includes in its WHERE clause. A partial index is only usable when its
-- predicate covers the query's, so the planner falls back to backward
-- timestamp scans over every partition, wading through pending rows
-- (observed 7-9s per poll at ~120k rows, firing sqlx slow-statement warnings).
--
-- Recreate with the no_surplus branch so the poller predicate is covered
-- again. Parent-level DDL cascades to all partitions, current and future.
-- Idempotent (IF EXISTS / IF NOT EXISTS) so a live-applied copy is safe.

DROP INDEX IF EXISTS idx_meter_readings_resolved_mint;

CREATE INDEX IF NOT EXISTS idx_meter_readings_resolved_mint
ON public.meter_readings USING btree ("timestamp" DESC)
WHERE (COALESCE(minted, false) OR COALESCE(on_chain_confirmed, false)
       OR ((blockchain_status)::text = 'failed'::text)
       OR (blockchain_last_error IS NOT NULL)
       OR ((blockchain_status)::text = 'no_surplus'::text));
