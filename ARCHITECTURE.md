# Aggregator Bridge — Architecture

> Source of truth for this service. Scoped to what this submodule actually runs —
> the layers above (edge hardware) and below (market/chain) are summarized in §7
> as boundaries, not described as if they lived here.
> Companion: [PROTOCOL.md](PROTOCOL.md) (v4 UTT-S+ frame layout + OBIS dictionary),
> [CLAUDE.md](CLAUDE.md) (conventions, invariants, full env reference).
> Last reviewed: 2026-07-28

---

## 1. What this service is

The **central connection point for every device node** in the smart grid: a
high-throughput ingestion and VPP convergence layer. It verifies inbound
telemetry cryptographically, aggregates it into billing windows, disseminates it
to the operational path, and drives flex dispatch.

Three things follow from that role and explain most of the design:

- **The security path is synchronous and fail-closed.** Verification never
  degrades to "accept and log". Every sink *behind* it is fire-and-forget, so a
  sink outage never stalls realtime ingest (§6).
- **The service is chain-light.** It carries no Solana dependency and never calls
  Solana RPC. Surplus minting sends *intent* to Chain Bridge over NATS (§3.4).
- **Degraded-mode is by design.** Redis, Kafka, RabbitMQ, InfluxDB and IAM all
  fall back to disabled on connect failure with a `warn!`; the process still
  starts. That is deliberate, not a bug to fix.

Two servers run concurrently from the wiring-only binary `src/main.rs`: the HTTP
IoT gateway on `IOT_GATEWAY_PORT` (default `4010`, `src/main.rs:134`) and the
gRPC ingestion service on `GRPC_PORT` (default `5030`). Six crates sit behind
them in a strict one-way dependency chain — `core ← protocol ← stacks ←
persistence ← logic ← api ← binary`.

---

## 2. Ingest pipeline (verify → decrypt → disseminate)

### 2.1 Authentication at the edge

API-key auth (`api_key_auth`, `crates/aggregator-api/src/auth.rs:194`) gates every
ingest route; `/health` and `/metrics` are exempt. Verdicts come from IAM over
gRPC and are **cached** to bound IAM load — sustained ingest would otherwise cost
IAM a Redis event plus a DB write per request. Positive verdicts hold for
`API_KEY_POS_CACHE_TTL_SECS` (default 60); **definitive rejects** hold for the
shorter `API_KEY_NEG_CACHE_TTL_SECS` (default 10), so a replayed bad or rotated
key still costs at most one round-trip per TTL. IAM *connection errors* are never
cached — they must reach the static `GRIDTOKENX_API_KEYS` fallback. A definitive
IAM reject returns 401 without trying static keys.

### 2.2 Signature verification (fail-closed, self-healing)

Ed25519 telemetry signatures are checked against device pubkeys in Redis at
`gridtokenx:devices:{meter_id}:pubkey`. `SignatureVerifier`
(`crates/aggregator-persistence/src/infra/crypto.rs:97`) holds a Redis *URL* and a
lazily-rebuilt connection; a transport error rebuilds and retries once
(`get_with_retry`, `crates/aggregator-persistence/src/infra/crypto.rs:165`), which
is why a Redis restart no longer freezes verification.

**Redis-unreachable returns a loud `Err`, never a silent `Ok(false)`** — fail-closed
but observable. The pubkey *lookup* is cached (parsed `VerifyingKey`, positive TTL
`PUBKEY_CACHE_TTL_SECS` default 60, negative `PUBKEY_NEG_CACHE_TTL_SECS` default
10) to stop per-reading Redis floods, but the signature itself is verified on
every call — only the static key fetch is cached.

> **The positive TTL *is* the revocation latency.** A key removed or rotated in
> Redis stays accepted for up to one TTL. Keep it short. Fail-closed is preserved
> across the cache: a miss with Redis down still `Err`s, absent keys reject, and
> malformed keys are never cached.

### 2.3 Secure DLMS decryption (binary gRPC path)

The v4 UTT-S+ binary frame is AES-256-GCM encrypted, but its header (version,
manufacturer ID, LDN, timestamp) is plaintext and precedes the ciphertext. So the
resolve order is `parse_header → meter_id (LDN) → fetch enckey → parse(bytes,
Some(key))`. The header-only parse runs CRC-32 and a version check with no
decrypt (`DlmsHeader` / `parse_header`,
`crates/aggregator-stacks/src/binary_decoder.rs:32`, `:48`); full decrypt is
`parse(payload, Some(key))` (`crates/aggregator-stacks/src/binary_decoder.rs:107`).

The per-device AES-256 key lives at `gridtokenx:devices:{meter_id}:enckey`
(64-char hex, 32 bytes), fetched by the self-healing `DeviceKeyRegistry` which
mirrors the verifier. gRPC ingest resolves and decrypts in `decode_secure_frame`
(`crates/aggregator-api/src/grpc/service.rs:55`); the branch policy is the pure
`apply_dlms_key_policy` (`crates/aggregator-api/src/grpc/service.rs:106`).

**Fail-closed:** under `ENVIRONMENT=production` a frame whose `enckey` is missing
is *skipped*, never decoded as plaintext. The dev/legacy plaintext fallback is
gated behind `ALLOW_PLAINTEXT_DLMS=true` and logged loud. Redis-unreachable or a
malformed key ⇒ loud skip, never a silent plaintext decode.

### 2.4 REST `dlms-enc` envelope (JSON counterpart)

The HTTP IoT gateway is mTLS. A meter POSTs:

```json
{"protocol":"dlms-enc","device_id":"…",
 "payload":{"enc":{"counter":0,"nonce":"<b64 12B>","ciphertext":"<b64>","kid":1}}}
```

`decrypt_dlms_envelope` (`crates/aggregator-api/src/handlers.rs:236`) AES-256-GCM
decrypts with AAD `device_id:counter`. The plaintext is the canonical
(sorted-keys, no-space) OBIS JSON — the same object a plaintext `dlms` frame
carries, including its inner Ed25519 signature — so post-decrypt the path is
identical to plaintext (`payload.protocol` is rewritten to `dlms`).

> **Order matters: authenticate before touching the replay store.** GCM
> authentication runs first, so a forged frame never advances the counter and
> cannot lock a meter out by bumping it past its legitimate sequence. Only an
> authenticated frame bumps it, via `check_and_bump_counter`
> (called at `crates/aggregator-api/src/handlers.rs:321`, defined at
> `crates/aggregator-persistence/src/infra/crypto.rs:759`). A replayed *valid*
> frame authenticates but is then caught by the `<= last` check.

Outcomes: decrypt or contract failure ⇒ `400`; replayed/non-increasing counter ⇒
`409`; under secure mode a non-`dlms-enc` meter frame ⇒ `426` (`secure_mode_gate`,
`crates/aggregator-api/src/handlers.rs:87`).

Note this is a **monotonic sequence guard, not a time window** — there is no
freshness or max-age check on a reading's own timestamp on any ingest path.

### 2.5 Rotated (versioned) per-device key

When the envelope carries a `kid`, the key is the Vault-Transit-wrapped GUEK at
`gridtokenx:devices:{meter_id}:enckey:v{kid}` (absent `kid` ⇒ the legacy
unversioned `enckey`). `get_device_aes_key_versioned`
(`crates/aggregator-persistence/src/infra/crypto.rs:665`) reads the wrapped blob,
unwraps via Vault, and caches by `(meter_id, kid)`. The sender keeps a small
**grace window** of prior versions live so frames signed under the previous key
still decode across a rotation; the simulator reconciles its prune state from
Redis on restart, so old `enckey:v*` versions stay bounded to the grace count
rather than leaking across restarts.

Because rotation goes through this versioned path, the `enckey` cache TTL can
safely be longer (`DEVICE_ENCKEY_CACHE_TTL_SECS`, default 300) than the pubkey's.

### 2.6 Protocol resolution

DLMS/COSEM is the only meter protocol. `protocol = "auto"` (or omitted) resolves
to `dlms` (`resolve_protocol`, `crates/aggregator-api/src/handlers.rs:66`); the
only other accepted value is `simulator`, an unsigned dev bypass
(`is_supported_protocol`, `crates/aggregator-api/src/handlers.rs:77`). Both single
and batch ingest dispatch to the lone `dlms_stack`.

### 2.7 Secure mode (locked-down deployments)

`AGGREGATOR_REQUIRE_SECURE=true` (`secure_mode_enabled`,
`crates/aggregator-api/src/handlers.rs:38`) hard-overrides **every** ingest bypass,
fail-closed regardless of the dev env vars:

| Bypass | Normally | Under secure mode |
| --- | --- | --- |
| REST unverified-telemetry hatch | honored | ignored (`signature_enforcement_disabled` ⇒ false) |
| Unsigned `simulator` protocol (single + batch) | accepted | refused (`simulator_bypass_allowed` ⇒ false) |
| REST plaintext meter frame | accepted | `426 UPGRADE_REQUIRED`, no downgrade |
| gRPC `SKIP_SIG_VERIFY` bulk bypass | honored | forced off (`bulk_skip_verify_allowed`) |
| `ALLOW_PLAINTEXT_DLMS` | honored | forced off (`plaintext_dlms_allowed`) |

Default **off**, so dev and e2e keep their bypasses. Don't make it default-on.

### 2.8 Dissemination

Verified readings fan out to zone-partitioned Redis Streams
(`gridtokenx:events:zone_<n>`) via `Router::disseminate`
(`crates/aggregator-logic/src/router.rs:184`), which rebuilds its connection and
retries the `XADD` once on transport error. Zone selection is
`calculate_zone_index` (`crates/aggregator-logic/src/router.rs:445`): a numeric
`zone_code` suffix routes directly when `idx < IOT_NUM_ZONES` (default 10,
`src/main.rs:144`), otherwise the string is hashed.

The zone stream is the **operational spine** and the only hop that can
back-pressure the caller. It is also the only place a reading survives *whole*:
`Router::disseminate` serializes the full `DeviceReading`, so decoded OBIS
metadata (per-phase V/I, reactive power, frequency, PF, DR status, active tariff)
lives only here. Downstream sinks project it — `reading_to_point`
(`crates/aggregator-logic/src/router.rs:314`) keeps only
`generated/consumed/net_kwh` for InfluxDB, the Kafka `MeterReadingEvent`
cherry-picks `voltage_v`/`frequency_hz`/`power_factor`/`signature`, and settlement
uses energy plus `frequency`.

The zone ingester consumes with `XREAD block(2000).count(25)`
(`crates/aggregator-api/src/ingester/zone_ingester.rs:471`), which sets the 0–2 s
hop in §6.

> `ZoneIngester::get_zone_index`
> (`crates/aggregator-api/src/ingester/zone_ingester.rs:232`) is a **second,
> divergent copy** of the routing rule — it hashes unconditionally instead of
> parsing the numeric suffix, so it would scatter a correctly-zoned fleet across
> all streams. It is `#[allow(dead_code)]`, so nothing is broken today, but it is
> a live trap for the next caller. See [docs/physics-zone-ids.md](docs/physics-zone-ids.md) §G5.

---

## 3. Aggregation & settlement (15-min bins → mint)

### 3.1 Billing windows

Readings accumulate into **15-minute billing bins**, wall-clock aligned to
:00/:15/:30/:45 — `WINDOW_MINUTES` (`crates/aggregator-logic/src/aggregator.rs:10`)
floors the minute-of-hour in `get_window_start`
(`crates/aggregator-logic/src/aggregator.rs:191`). The window size is a compile-time
constant, not configurable.

A bin's identity is `(meter_id, window_start)`
(`handle_reading`, `crates/aggregator-logic/src/aggregator.rs:84`). `end_time` is
stored **per bin** rather than recomputed, and `peek_completed_bins`
(`crates/aggregator-logic/src/aggregator.rs:163`) filters on it — so bins restored
from a previous run settle on their own original boundaries.

Each bin carries a **time-of-use split** (peak = tariff rate 1, off-peak = rate 2)
and the window's **max net import demand** in kW, for demand-charge billing. The
tariff comes from the meter's own active-tariff register — this service defines no
peak/off-peak clock windows, it only records which tariff was active per reading.
Any tariff value other than 1 or 2 leaves the split untouched while the totals stay
authoritative. TOU/demand fields are `#[serde(default)]` so bins persisted before
they existed still deserialize on crash recovery.

### 3.2 Durable bins (crash safety)

`DURABLE_BINS` (default **on** when Redis is reachable, `src/main.rs:256`)
write-throughs in-flight bins to a Redis hash `gridtokenx:billing:bins`, field
`{meter_id}:{window_start_ms}` (`BinStore`,
`crates/aggregator-logic/src/bin_store.rs:46`). They are deleted on settlement
eviction and **restored** at startup via `restore_bins`
(`crates/aggregator-logic/src/aggregator.rs:184`), so a crash mid-window no longer
loses the partial totals. The write is fire-and-forget — Redis latency never
blocks ingest; a Redis fault degrades to memory-only with a `warn!`.

### 3.3 The settlement sweep

A flush loop (`src/main.rs:506`) periodically calls `peek_completed_bins(grace)`
and, for each completed bin:

1. Converts it to the InfluxDB `billing` measurement via `bin_to_billing_point`
   (`crates/aggregator-logic/src/billing_sink.rs:26`) — TOU split plus window
   `max_demand_kw`, tagged `user_id`, timestamped at `end_time`.
2. Decides whether to mint via the pure `plan_mint`
   (`crates/aggregator-logic/src/billing_sink.rs:73`), which fires only when
   `net_surplus_kwh()` is `Some` — net generation exceeded consumption
   (`crates/aggregator-logic/src/aggregator.rs:58`).
3. **Evicts** the bin, which is what bounds the otherwise-unbounded `active_bins`
   map.

Tunables: `BILLING_FLUSH_INTERVAL_SECS` (default 30) and
`BILLING_FLUSH_GRACE_SECS` (default 120). The loop runs whenever InfluxDB **or**
minting is enabled, so eviction does not depend on InfluxDB.

### 3.4 Surplus minting (Chain Bridge over NATS)

Surplus is minted to the meter owner by publishing `chain.tx.mint` intent — the
service holds no Solana dependency. The recipient wallet resolves through
`MeterRegistry` (`crates/aggregator-persistence/src/infra/meter_registry.rs:52`),
a three-tier resolver (local cache → Redis → Postgres source of truth) that
backfills Redis and the local cache on a Postgres hit
(`resolve_user_id`,
`crates/aggregator-persistence/src/infra/meter_registry.rs:205`). The bridge is
read-only on the `meters` table; meter-service owns registration.

**Idempotency** is `mint:{serial}:{window_start_ms}`, with the on-chain
`(meter_id, window_start_ms)` PDA as the backstop — so a retry of a mint that
actually landed but whose reply was lost does not double-mint.

`NatsMintGateway` (`crates/aggregator-persistence/src/infra/mint.rs:300`)
publishes to **JetStream and awaits the PubAck** before awaiting the reply,
rather than a fire-and-forget core NATS publish — closing the silent-drop gap
when the consumer is momentarily absent. The reply rides core NATS.

### 3.5 Per-zone energy balance (observation only)

Groundwork for a mint-on-surplus / burn-on-consumption model, which needs to know
whether a zone's generation and consumption actually net out. **Nothing in this
path mints, burns, dispatches, or gates settlement** — the numbers are logged at
each sweep (`src/main.rs:552`) so the real imbalance can be observed before any
behaviour is built on it.

`zone_balances` (`crates/aggregator-logic/src/zone_balance.rs:75`) groups
completed bins by zone and totals generation, consumption and signed net, with a
scale-free `imbalance_ratio`; `system_net_kwh`
(`crates/aggregator-logic/src/zone_balance.rs:98`) sums across zones. Pure and
sync, per Sync Core — it takes bins and returns numbers.

Three details that are easy to get wrong:

- **`net_deficit_kwh`** (`crates/aggregator-logic/src/aggregator.rs:83`) is the
  exact mirror of `net_surplus_kwh` and returns the deficit **positive**. Exactly
  one of the two is `Some` for a bin; both are `None` at exact net zero.
- **The zone is recorded on the bin** (`BillingBin.zone_index`,
  `crates/aggregator-logic/src/aggregator.rs:48`), written by the ingester from
  the *same* `get_zone_index` call that routed the reading
  (`crates/aggregator-api/src/ingester/zone_ingester.rs:675`). It is deliberately
  NOT re-derived at sweep time: the ingester hashes `zone_code` when a reading has
  one and only falls back to `meter_serial`, so a serial-only re-derivation would
  silently mis-attribute every zone-tagged meter. `serde(default)` keeps
  crash-restored bins deserializable.
- **Unzoned bins stay their own group** rather than being folded into zone 0 —
  attributing unzoned energy to a real zone would corrupt the very number this
  exists to measure.

> **A zone here is a hash partition, not a feeder.** `get_zone_index` hashes
> `zone_code` (or the serial) modulo `IOT_NUM_ZONES`, so these balances are per
> partition, not per electrical boundary. Any invariant that depends on a zone
> being one piece of grid needs zone identity to mean network topology first.

> **Expect a large diurnal swing, and do not read one window as steady state.**
> Measured 2026-07-30 on the 80-meter simulator fleet (20 solar prosumers,
> 200 kW installed, ~210 kW load): at simulated dawn generation ran ~0.5% of
> consumption (zones ~99% net import), and by mid-morning it had risen to ~31% as
> solar climbed 5.6% → 31% of capacity. A per-window balance invariant would
> therefore burn heavily all morning and approach balance near local noon; any
> such invariant has to hold over a daily cycle, not per 15-minute window.

> **SECURITY.** The gateway signs the mint envelope with this service's mTLS
> client key (P-256/ECDSA over canonical bytes) and attaches an `EnvelopeAuth`
> (cert PEM + signature), so the bridge binds the self-asserted
> `service_identity` to a CA-issued cert and rejects spoofed identities under
> `CHAIN_BRIDGE_REQUIRE_SIGNED_NATS`. The signing scheme is **no longer mirrored
> locally** — `EnvelopeSigner` and `canonical_mint_bytes` now come from the
> shared light `gridtokenx-blockchain-types` crate
> (`crates/aggregator-persistence/src/infra/mint.rs:26`), a single source of truth
> with the Chain Bridge verifier. When the client cert/key is absent (insecure
> dev) the signer is `None` and the envelope ships unsigned, accepted only while
> the bridge runs signing in log-only mode.

A resolved wallet that is not a parseable Solana address is declined
aggregator-side *before* the NATS publish (`wallet_is_valid`,
`crates/aggregator-persistence/src/infra/mint.rs:63`), so no round-trip is spent.

### 3.5 The mint outbox (durability)

The killer case is a settled surplus that **can't land on-chain yet** — Chain
Bridge or the validator down, a sim rejection, a lost reply. Rather than
fire-and-forget then evict, the settlement loop enqueues a `PendingMint`
(`crates/aggregator-logic/src/mint_outbox.rs:45`) into a Redis hash
`gridtokenx:billing:mint_outbox`, and a drain loop (`MINT_RETRY_INTERVAL_SECS`,
default 30, `src/main.rs:789`) retries until the mint is *confirmed on-chain*
(`MintOutbox`, `crates/aggregator-logic/src/mint_outbox.rs:95`).

The outbox survives restart, and the wallet is resolved **fresh per attempt** — so
a meter that registers *after* its window still mints on a later retry. Retries
are safe by the idempotency rules in §3.4.

**Retention is bounded, never silent.** An entry that hasn't landed after
`MINT_OUTBOX_MAX_AGE_SECS` (default 7 days; `0` = retry forever) is **parked** —
moved to `gridtokenx:billing:mint_outbox:parked`, out of the retry path, with a
loud `warn!` and a `parked/expired` metric. Re-enqueue manually to resume. No
Redis ⇒ falls back to the prior best-effort fire-and-forget mint.

### 3.6 Optional Postgres readings sink

`AGGREGATOR_PG_READINGS` enables an optional sink that batches `DeviceReading`s
into the IAM-owned `meter_readings` table, so the trading UI and meter-service can
list a meter's recent readings. Disabled by default; degrades safely when unset
or the pool is absent.

---

## 4. Dispatch (VPP flex)

Frequency-driven demand response. **The fleet itself is the frequency sensor** —
there is no external SCADA feed.

### 4.1 Self-sourced grid status

The zone ingester feeds each reading's `frequency`/`frequency_hz` metadata into a
rolling window (`FrequencyMonitor`,
`crates/aggregator-logic/src/grid_status.rs:19`; ingester hook at
`crates/aggregator-api/src/ingester/zone_ingester.rs:593`, extraction at `:766`).
Implausible samples (<40 / >70 Hz) are dropped. A publisher task turns the window
mean into `GridStatusEvent` JSON on the Kafka dispatch topic every
`GRID_STATUS_PUBLISH_SECS` (default 30, `src/main.rs:933`). Window length is
`GRID_FREQ_WINDOW_SECS` (default 60).

### 4.2 Dispatch engine

A Kafka listener (`src/main.rs:996`) feeds each grid-status frequency to
`DispatchEngine::evaluate_and_dispatch`
(`crates/aggregator-logic/src/dispatch/engine.rs:196`): below
`DISPATCH_FREQ_LOW_HZ` ⇒ FLEX_UP, above `DISPATCH_FREQ_HIGH_HZ` ⇒ FLEX_DOWN, at
capacity `DISPATCH_CAPACITY_KW`. Dispatch refuses to fire with zero completed
aggregation capacity — checked once for the whole fan-out
(`has_dispatch_capacity`, `crates/aggregator-logic/src/dispatch/engine.rs:315`), so
telemetry must flow before dispatch can.

Repeat suppression is tracked **per (action, adapter)**: re-dispatching the same
action to the same adapter waits out `DISPATCH_COOLDOWN_SECS` (default 900 = one
15-minute window), while a flipped action — or the same action to a *different*
adapter — fires immediately on its own independent timer. So an oscillating
frequency cannot reset the cooldown by alternating directions, and one fan-out
target cannot suppress another. A cooldown starts only on success
(`cooldown_allows`, `crates/aggregator-logic/src/dispatch/engine.rs:46`).

### 4.3 Adapters (single or fan-out)

`DISPATCH_ADAPTERS` (csv) dispatches each excursion to **every** listed adapter at
once — e.g. a downstream in-mesh VTN *and* a utility VTN — with per-(action,
adapter) cooldown and partial-failure isolation: one adapter erroring never stops
the others, and `evaluate_and_dispatch` returns `Err` only when *all* targets
fail. The legacy single-value `DISPATCH_ADAPTER` is honored as a one-element list;
with neither set the default is `openleadr` when configured, else `ieee`. Unknown
names are dropped loud. Selection is the pure `select_adapters_from`
(`crates/aggregator-logic/src/dispatch/engine.rs:130`).

| Adapter | Target |
| --- | --- |
| `grpc` | ConnectRPC to edge controllers (`DISPATCH_GRPC_URL`) |
| `ieee` | IEEE 2030.5 DERControl — **simulation stub**, logs only, no actuation |
| `openleadr` | OpenADR 3 business-logic side, against a VTN |

### 4.4 OpenADR 3 — BL side (outbound)

`OpenLeadrAdapter` (`crates/aggregator-logic/src/standards/openleadr.rs:24`) acts
as business logic against a VTN (`OPENLEADR_VTN_URL`). Each dispatch becomes an
OpenADR event with a signed-kW `DISPATCH_SETPOINT` payload — up = +, down = −
(`dispatch_event`, `crates/aggregator-logic/src/standards/openleadr.rs:92`). The
program is resolved by name before create, because a blind create 409s forever
after a restart.

### 4.5 OpenADR 3 — VEN side (inbound)

`OpenLeadrVenListener`
(`crates/aggregator-logic/src/standards/openleadr_ven.rs:47`) polls a
(typically utility-operated) VTN at `OPENLEADR_VEN_VTN_URL` for setpoint events —
both absolute `DISPATCH_SETPOINT` and relative `DISPATCH_SETPOINT_RELATIVE`, which
actuate the same signed-kW FLEX path with the relative case tagged
`executed_relative` in logs/metrics (`setpoint_kind`,
`crates/aggregator-logic/src/standards/openleadr_ven.rs:542`).

Execution goes through an injected adapter — `ieee` by default or `grpc`, **never**
`openleadr`, which would loop events back to a VTN. At startup the listener
self-registers a VEN object named `OPENLEADR_VEN_CLIENT_NAME`, best-effort
(`ensure_registered`,
`crates/aggregator-logic/src/standards/openleadr_ven.rs:194`).

Schedules are honored across **all** setpoint intervals (`decide`,
`crates/aggregator-logic/src/standards/openleadr_ven.rs:583`): each interval
executes as its window opens (deduped per interval), future windows wait, an event
is done only when no pending interval remains, an interval-level period wins over
the event-level default, and a period-less interval executes immediately.

Events dedupe on id + `modificationDateTime`, persisted to Redis
(`gridtokenx:openleadr:ven:executed`) so a restart does not re-execute
still-listed events; failed dispatches retry next poll, and entries for events the
VTN no longer lists are pruned after 7 days (`poll_once`,
`crates/aggregator-logic/src/standards/openleadr_ven.rs:229`). Optional
`OPENLEADR_VEN_TARGET` restricts polling to events carrying that target. An
executed event that vanishes from the VTN while still active is flagged loud
(cancellation visibility) — no automatic revert, by design.

Each executed dispatch is confirmed back to the VTN as an OpenADR report
(AGGREGATED_REPORT resource, SETPOINT payload), best-effort — a report failure
never fails or retries the dispatch (`post_execution_report`,
`crates/aggregator-logic/src/standards/openleadr_ven.rs:401`).

> **Two configuration hazards.** (1) `ieee` is a *simulation stub*, so a VEN using
> it posts execution reports attesting dispatch that never physically happened —
> `main.rs` warns loud. (2) When `OPENLEADR_VEN_VTN_URL` equals
> `OPENLEADR_VTN_URL` with no program/target filter, the VEN consumes the bridge's
> own outbound events — double actuation. Also warned.

### 4.6 Why the VEN polls rather than subscribing

OpenADR 3.1 defines push subscriptions, but the backing library `openleadr-rs`
v0.2.4 — the spec floor for 3.1; 3.0 ended at v0.1 — does **not** implement
real-time webhook subscriptions. (166/168 OpenADR Alliance tests pass; the 2
failures are intentional, from a differing permission model, not from the
unsupported subscriptions feature.)

So the listener falls back to a poll loop every `OPENLEADR_VEN_POLL_SECS`
(default 30, `crates/aggregator-logic/src/standards/openleadr_ven.rs:51`). **The
polling cadence is a dependency constraint, not a design preference** —
`poll_interval` bounds dispatch latency to one poll. Lower it for tighter
response, raise it to spare the VTN. Revisit if openleadr-rs ships subscriptions.

### 4.7 Local test loop

The superproject compose runs an `openleadr-vtn` service (upstream openleadr-rs
v0.2.4, host port 4031) with seeded dev OAuth clients. `just openadr-e2e` proves
the full loop: telemetry → frequency window → Kafka → dispatch → VTN event → VEN
execution.

---

## 5. Observability (Prometheus `/metrics`)

The Prometheus exporter is installed at startup and rendered on the IoT gateway
at `/metrics`, behind the same mTLS as ingest — a scraper presents a CA-issued
client cert (the superproject's prometheus job uses `reporting-service.crt`).

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `aggregator_settlement_path` | gauge | `path` = `nats` \| `grpc` \| `disabled` | Active surplus-mint path, set once at startup **after** the recorder is installed. Active = `1`, others = `0`. `disabled` = `MINT_VIA_CHAIN_BRIDGE` unset or NATS unreachable. |
| `aggregator_mint_total` | counter | `outcome`, `reason` (below) | One increment per completed billing bin in the settlement sweep. |

`aggregator_mint_total` outcomes, and what each is actually telling you:

| `outcome` / `reason` | Meaning |
| --- | --- |
| `no_surplus` | Net-consumption window — the denominator, not a problem. |
| `skipped/no_wallet` | Unregistered meter. Surfaces what would otherwise be a silent skip. |
| `skipped/invalid_wallet` | Resolved wallet isn't a parseable Solana address; declined before any NATS round-trip. |
| `skipped/in_flight` | Retry declined — an attempt for the same `{serial}:{window}` is still awaiting its reply in this process (`MintInFlight`). |
| `failed/reply_timeout` | Reply lost. Intent is durably queued in JetStream and **may have landed** — the outbox retry is dedup-safe. |
| `failed/mint_err` | Hard rejection, distinct from the above. |
| `lost/outbox_and_mint_failed` | Outbox enqueue failed twice *and* the last-resort immediate mint also failed. The bin's durable Redis entry is **retained** for manual recovery instead of evicted. |
| `parked/expired` | Aged past `MINT_OUTBOX_MAX_AGE_SECS` without landing; moved to `gridtokenx:billing:mint_outbox:parked`. Re-enqueue manually (`HSET` back into `gridtokenx:billing:mint_outbox` + `HDEL` from `:parked`). |

Alert rules live in the superproject `monitoring/prometheus_rules.yml`
(`AggregatorSurplusMintDisabled`, `AggregatorMintSkipSpike`, `AggregatorMintLost`,
`AggregatorMintParked`).

---

## 6. Failure policy per hop

End-to-end hop timing (steady state):

| Hop | Mechanism | Steady-state latency |
| --- | --- | --- |
| ingress → verify | in-proc Ed25519 / DLMS, Redis key fetch | sub-ms |
| verify → zone `XADD` | sync `XADD` to Redis Streams | sub-ms |
| stream → zone ingester | `XREAD` block 2000 ms, count 25 | 0–2 s |
| ingester → InfluxDB `energy` | fire-and-forget, batched flush (2 s / 500 pts) | ~0–2 s |
| completed bin → InfluxDB `billing` | flush loop 30 s + grace 120 s | ~120–150 s after window close |
| completed bin → surplus mint | spawned task, NATS req-reply 30 s timeout | seconds, off critical path |
| grid status → Kafka | publisher every 30 s | ~30 s |

**Fail-closed** (refuse) vs **fire-and-forget** (drop, keep going):

| Hop | Policy | Behavior when its backend is down |
| --- | --- | --- |
| Signature / DLMS-key verify | **fail-closed** | Redis-unreachable ⇒ loud `Err`, never silent `Ok(false)`; prod missing `enckey` ⇒ frame skipped (`crates/aggregator-persistence/src/infra/crypto.rs:165`) |
| Zone `XADD` | sync + **retry-once** | rebuild connection, retry once; persistent failure surfaces as back-pressure (`crates/aggregator-logic/src/router.rs:184`) |
| Durable bin write | **fire-and-forget** | degrade to memory-only + `warn!`; ingest never blocks on Redis |
| InfluxDB (`energy` / `billing`) | **fire-and-forget** | drop batch, `warn!`, ingest continues |
| Kafka (`meter.readings` / grid status) | async best-effort | publish error logged, message dropped |
| Surplus mint (NATS) | **durable outbox** | enqueue + retry until confirmed on-chain; no Redis ⇒ best-effort fire-and-forget |
| Meter registry (Postgres tier) | **degraded tiers** | PG down ⇒ Redis-only; neither configured ⇒ nil-user fallback (`crates/aggregator-persistence/src/infra/meter_registry.rs:52`) |
| API-key auth (IAM) | **degraded** | connection error ⇒ static `GRIDTOKENX_API_KEYS`; definitive reject ⇒ 401, no static retry |

---

## 7. Boundaries

Neither side below is implemented in this submodule. They are recorded here only
so the seams are explicit — do not treat this section as a description of code
that lives here.

**Upstream (field/edge).** Smart meters sign telemetry with Ed25519 and encrypt
DLMS payloads with a per-device AES-256 key; device identity is verified
cryptographically, not by network position. Telemetry ingresses **directly** to
this service's IoT gateway — there is no separate edge proxy, and this service
contains no MQTT broker integration. The frame format is
[PROTOCOL.md](PROTOCOL.md).

**Downstream (market/chain).** GRID / GRX / REC tokens are issued on Solana via
Chain Bridge, which is the only service that touches Solana RPC. P2P order-book
matching and REC minting live in the trading service. This service's sole
outbound chain interaction is the `chain.tx.mint` intent in §3.4.

**Sideways (metering DB).** `meters ⋈ users` is owned by meter-service; this
service is read-only on it. `METER_DATABASE_URL` is the DB-per-service Phase 2
seam — see [docs/db-split-phase2.md](docs/db-split-phase2.md). Migrations for the
shared metering DB are owned by a dedicated migrate job, never by two services'
boot runners racing one `_sqlx_migrations` ledger.

---

## 8. Testing

- Unit tests are inline (`#[cfg(test)] mod tests`, no `tests/` dirs).
  `cargo test --workspace` runs every crate — the root is a package, so a bare
  `cargo test` runs only the binary.
- Live-infra tests (Redis / Vault / Postgres / RabbitMQ / Kafka / gRPC) are
  `#[ignore]`-gated: `cargo test -- --ignored` with `just orb-up` running, or
  `cargo test -- --include-ignored` for both.
- The cross-service pytest e2e suite lives in the superproject (`tests/e2e/`);
  `20_oracle`, `30_settlement` and `90_golden_path` cover this service.
