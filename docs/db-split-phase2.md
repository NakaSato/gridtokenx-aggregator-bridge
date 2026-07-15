# DB-per-Service — Phase 2 (Metering → `gridtokenx_meter`)

> Status: **Draft / authored, not cut over** · Owner: aggregator-bridge
> Parent plan: superproject `docs/design-docs/db-per-service-migration.md` §3.2, §5 Phase 2.
> Scope of THIS doc: the aggregator-bridge slice of Phase 2 — the migrations under
> `migrations/`, the owner/wallet read-model that removes the last cross-domain read,
> and the open `meters` ownership question. **Nothing here is deployed.** Creating the
> physical `gridtokenx_meter` DB, editing `docker-compose.yml`/`pgdog.toml`, and flipping
> `AGGREGATOR_PG_READINGS`/`DATABASE_URL` are the main thread's cutover job.

---

## 1. What the metering domain owns

Extracted verbatim (current live DDL) from the shared `gridtokenx` pg_dump into
ordered, self-contained migrations under `migrations/`:

| File | Objects |
|------|---------|
| `0001_metering_functions.sql` | `canonicalize_meter_serial`, `update_updated_at_column`, `update_blockchain_status_on_hash`, `create_meter_readings_partition`, `archive_old_meter_readings` |
| `0002_meter_registry.sql` | `meters`, `meter_registry`, `meter_verification_attempts` (+ indexes + `update_meters_updated_at` trigger) |
| `0003_meter_readings.sql` | `meter_readings` (partitioned parent) + 11 monthly partitions + `meter_readings_default` (DEFAULT partition) + `meter_readings_archive` (standalone) + `meter_readings_all` view + parent indexes + 2 triggers |
| `0004_oracle_submissions.sql` | `oracle_submissions` + its bigint sequence + indexes |
| `0005_grid_status_history.sql` | `grid_status_history` + index |
| `0006_meter_owner_read_model.sql` | **NEW** `meter_owner_read_model` — the owner/wallet read-model (§3) |

**Self-containment:** every FK into IAM `users` present in the source
(`meters_user_id_fkey`, `meter_registry_user_id_fkey`, `meter_registry_verified_by_fkey`,
`meter_verification_attempts_user_id_fkey`, `meter_readings_user_id_fkey`) is **dropped** —
those columns become soft references into `gridtokenx_iam`. The one intra-domain FK,
`meter_readings.meter_id -> meter_registry(id)`, is **kept** (both tables are metering-owned).
No custom enum/type dependencies exist — every status column is `varchar`.

### Partition scheme (preserved exactly)

`meter_readings` is `PARTITION BY RANGE (reading_timestamp)` with PK `(id, reading_timestamp)`
(the partition key must be in the PK). Live layout:

- Monthly range partitions: `2024_11`, `2024_12`, `2025_01`, `2025_02`, `2025_03`,
  `2026_07`, `2026_08`, `2026_09`, `2026_10`, `2026_11`, `2026_12` — each `FOR VALUES FROM
  (month) TO (next month)`, bounds copied 1:1 from the source `ATTACH PARTITION` statements.
- `meter_readings_default` — the `DEFAULT` partition (catch-all for out-of-range rows).
- `meter_readings_archive` — a **standalone** table, *not* a partition (fewer columns: no
  `current_amps..zone_id` tail); the retention sink for `archive_old_meter_readings()`.
- `meter_readings_all` — a view: current `UNION ALL` archive with an `is_archived` flag.

Faithful-but-cleaner reproduction: the source pg_dump created each child as a standalone
table then `ATTACH`ed it, and dumped indexes as `ON ONLY parent` + per-child `ATTACH`.
The migration instead uses `CREATE TABLE … PARTITION OF` for children and declares indexes
on the live parent, so Postgres auto-creates + attaches the matching child indexes (existing
and future). The resulting on-disk partition/index tree is identical; new months are added
operationally via `create_meter_readings_partition(date)`.

> Caveat carried over as-is: `archive_old_meter_readings()` does
> `INSERT INTO meter_readings_archive SELECT * FROM meter_readings`, but the archive table
> has fewer columns than the parent — the source function has this arity mismatch too. We
> reproduce it verbatim rather than silently "fixing" it; fixing archive is out of Phase 2 scope.

---

## 2. The aggregator's Postgres surface (what actually cuts over)

Verified against code, not migration authorship:

| Access | Object | Site |
|--------|--------|------|
| **WRITE** (append-only INSERT…SELECT) | `meter_readings` | `crates/aggregator-persistence/src/infra/pg_readings.rs:183` |
| **READ** (serial → user_id) | `meters` | `pg_readings.rs:196`; `meter_registry.rs:119` |
| **FOREIGN READ** (user_id → wallet) | `users.wallet_address` (IAM) | `pg_readings.rs:197`; `meter_registry.rs:119` |

`meter_registry`, `meter_verification_attempts`, `oracle_submissions`, `grid_status_history`
are **domain-owned but not read from Postgres by the aggregator today** (api-key auth is IAM
gRPC; device keys are Redis/Vault; oracle audit + grid history are written by other paths).
They move with the domain for ownership correctness, but they are not on the aggregator's hot
path. The only FOREIGN dependency to unwind is the `users.wallet_address` read.

---

## 3. Removing the FOREIGN read — the owner/wallet read-model

### Today

`MeterRegistry` (`crates/aggregator-persistence/src/infra/meter_registry.rs`) resolves
`serial -> (user_id, wallet)` in **three tiers**: local in-process cache → Redis
(`gridtokenx:meters:{serial}:user_id` / `:wallet`) → **Postgres** (`meters ⋈ users`). A
Postgres hit backfills Redis + the local cache (`backfill`, `meter_registry.rs:132`). The
same `users` JOIN also fills `wallet_address` directly in the `meter_readings` sink
(`pg_readings.rs:187`). Both are the forbidden cross-domain read.

### Target — promote the Redis cache to a durable read-model

`0006_meter_owner_read_model.sql` adds:

```
meter_owner_read_model(
    serial_number   varchar(100) PRIMARY KEY,   -- canonicalized on write
    user_id         uuid NOT NULL,
    wallet_address  varchar(88),                 -- NULLable (owner may lack a wallet yet)
    updated_at      timestamptz NOT NULL
)
```

This is the **durable form of the existing Redis owner cache** — same key contents
(`serial -> user_id, wallet`), now surviving a Redis flush/restart in the aggregator's own DB.
The Postgres tier of `MeterRegistry` changes from `meters ⋈ users` (cross-domain) to a single
local read: `SELECT user_id, wallet_address FROM meter_owner_read_model WHERE serial_number = $1`.
Redis stays the hot cache in front of it; the local in-process cache stays in front of Redis.
The `meter_readings` sink stops JOINing `users` and instead uses the wallet already resolved by
`MeterRegistry` (pass it into the batch, or JOIN `meter_owner_read_model` locally).

### Keeping it current — IAM NATS event feed (event-carried state transfer)

IAM already has a transactional outbox (`iam_outbox_events`) draining to NATS/Kafka. The
read-model is maintained by consuming IAM domain events (no synchronous IAM calls on the hot
path):

- `user.registered` / `user.wallet.created` / `user.wallet.updated` → upsert
  `(serial?, user_id, wallet_address)`. Wallet-only events update every serial owned by that
  `user_id` (`UPDATE … SET wallet_address = $, updated_at = now() WHERE user_id = $`).
- Meter enrollment events (from meter-service — see §4) supply the `serial -> user_id` edge
  when a meter is registered before/without a wallet.

Upserts are last-writer-wins on `updated_at` so out-of-order delivery can't regress a newer
wallet. This mirrors the Trading-service read-model pattern in the parent plan (§2, §5 Phase 1).

### First-boot backfill

On first boot after cutover the read-model is empty. Backfill once from a snapshot of the
current owner mapping — either:

1. a one-shot seed query run by the cutover tooling against the *old* shared DB
   (`INSERT INTO meter_owner_read_model SELECT canonicalize_meter_serial(m.serial_number),
   m.user_id, u.wallet_address, now() FROM meters m JOIN users u ON u.id = m.user_id`), or
2. an IAM replay of `user.*` + meter events onto NATS at cutover.

After backfill the NATS feed keeps it live. The existing lazy per-serial backfill from
Postgres (`MeterRegistry::backfill`) also naturally warms Redis + the read-model on any miss,
so the system self-heals even if a single event is dropped (the IAM publish is fire-and-forget
today — see the superproject memory note on Kafka event loss; the lazy DB re-resolve is the
safety net).

### Migration status of the code change

Not done in this phase — Phase 2 **authors** the schema + read-model and marks the swap sites.
`TODO(db-split)` comments are in place at both foreign-read sites (`pg_readings.rs:181` above
the INSERT, `meter_registry.rs` above `fetch_owner_from_db`'s query) with the exact replacement
query. The JOINs are **intentionally left intact** until the read-model is populated + verified
at cutover (per the plan's "no phase deletes source reads until the new DB is verified" rule).

---

## 4. Recommendation — where should `meters` + `meter_registry` live?

**`meters` is written by meter-service** (`POST /api/v1/meters`), not the aggregator; the
aggregator only READs it. `meter_registry` is the meter-service enrollment/verification
registry (dormant in the aggregator; only referenced as the `meter_readings.meter_id` FK
target). `meter_verification_attempts` is likewise meter-service's enrollment audit.

Two options:

- **(A) One `gridtokenx_meter` DB shared by meter-service + aggregator** (the parent plan's
  current row: "Aggregator Bridge / meter-service"). Simple; both are the "metering" bounded
  context. But it re-creates a mini shared-DB (two services, one DB) — the exact anti-pattern
  the migration exists to kill, just at smaller scope. Ownership of writes is still split.
- **(B) `meters`/`meter_registry`/`meter_verification_attempts` live in a meter-service DB;
  `meter_readings`(+partitions/archive), `oracle_submissions`, `grid_status_history`, and
  `meter_owner_read_model` live in the aggregator DB.** The aggregator never SQL-reads
  meter-service tables — it resolves owners purely from its local `meter_owner_read_model`
  (fed by meter-service meter-enrollment events + IAM wallet events).

**Recommendation: (B) — split by writer.** The clean DB-per-service rule is "one writer per
table." `meters` has exactly one writer (meter-service) and one reader (aggregator); a reader
should hold a **read-model**, not the table. Since Phase 2 already introduces
`meter_owner_read_model` and the NATS feed to remove the `users` JOIN, extending that same feed
to carry the `serial -> user_id` edge from meter-service costs almost nothing and eliminates the
*second* cross-domain read (`meters`) at the same time — leaving the aggregator DB with zero
foreign reads. `meter_registry` + `meter_verification_attempts` follow their writer
(meter-service). The `meter_readings.meter_id -> meter_registry` FK is already effectively dead
(the aggregator writes `meter_id = NULL`, comment at `pg_readings.rs:160`), so severing it to
land `meter_readings` in the aggregator DB costs nothing.

Practical note: the migrations here include `meters`/`meter_registry`/`meter_verification_attempts`
so the aggregator DB is **runnable standalone today** (and so the read paths keep working through a
dual-write/verify window). If (B) is adopted, those three tables move to the meter-service migration
set and are dropped from here in a follow-up; `meter_owner_read_model` + its event feed already make
the aggregator independent of them. Recommend the main thread confirm meter-service's DB target
before finalizing which set owns `meters`.

---

## 5. Remaining cutover steps (main thread — NOT done here)

1. Create the physical `gridtokenx_meter` DB + a least-privilege role; add the pgdog
   `[[databases]]` route (mirroring the `gridtokenx_noti` reference model).
2. ~~Apply `migrations/` to `gridtokenx_meter` (decide runner)~~ **DONE — boot-time runner.**
   `infra::db` (`crates/aggregator-persistence/src/infra/db.rs`) embeds `migrations/` via
   `sqlx::migrate!` and applies them at boot **when `METER_DATABASE_URL` is set** (its own
   `_sqlx_migrations` ledger; sqlx advisory-locked, so replica-safe). Idempotent + degrade-safe
   (migration failure ⇒ loud `warn!`, non-fatal). Never runs against the shared `DATABASE_URL`
   (would collide with IAM's ledger). Verified by `crates/aggregator-persistence/tests/meter_db_migrations.rs`
   (all 6 migrations apply to a fresh `gridtokenx_meter` + idempotent + schema present) — gated on
   `METER_TEST_DATABASE_URL`.
3. Backfill `meter_owner_read_model` from a snapshot (§3) and stand up the IAM (+meter-service)
   NATS consumer that maintains it.
4. Swap the two foreign reads (`pg_readings.rs`, `meter_registry.rs`) to the local read-model;
   remove the `JOIN users` (and, under recommendation B, the `JOIN meters`).
5. Optional dual-write/verify window: keep writing the old shared `meter_readings` while the new
   DB is validated.
6. Set **`METER_DATABASE_URL`** to `gridtokenx_meter` (the seam is wired: when set, the aggregator
   uses it as the metering pool AND runs its migrations at boot; unset ⇒ legacy shared `DATABASE_URL`,
   no migrations). **Do this only AFTER step 4** — until the read-model swap lands, the meter⋈users
   owner read still needs the shared DB. Verify the ingest → owner-resolve → zone-stream → 15-min bin
   → surplus-mint hops (`just` / telemetry-hops).
7. Update `ARCHITECTURE.md` topology and resolve §4 ownership of `meters` in the parent plan.
8. Rollback: flip the env back to `gridtokenx`; no source table is dropped until the new DB
   passes e2e.
