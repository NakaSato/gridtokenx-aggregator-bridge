# VPP Four-Layer Architecture

## 1. Field Layer (Edge)
- **Primary Node:** Smart Meter (Radxa Cubie A7Z).
- **Security:** ATECC608B Secure Element (Root of Trust).
- **Software:** Rust-based sampling (1s) and aggregation (15s).

## 2. Communication Layer
- **MQTT:** EMQX 5.0 cluster (mTLS with device certificates).
- **Streaming:** Apache Kafka (KRaft mode).
- **Topics:**
  - `telemetry.raw`: 15-second intervals.

## 3. Aggregator Bridge Layer

The **central connection point for every device node** in the smart grid: it
verifies, aggregates, and disseminates inbound telemetry, and drives VPP flex
dispatch. It does **not** mint tokens or settle on-chain (the former Plonky2
ZK-rollup "Path B" was removed).

- **UTT Ingestion:** Verified entry point for all device telemetry.

### Ingestion pipeline

- **Signature verification (fail-closed, self-healing).** Ed25519 telemetry
  signatures verified against device pubkeys in Redis
  (`gridtokenx:devices:{meter_id}:pubkey`). The verifier holds a Redis URL and a
  lazily-rebuilt connection (`SignatureVerifier`, verified
  `crates/aggregator-persistence/src/infra/crypto.rs:19`); a transport error rebuilds
  the connection and retries once (`get_with_retry`, verified
  `crates/aggregator-persistence/src/infra/crypto.rs:81`) so a Redis restart no longer
  freezes verification. Redis-unreachable returns a loud `Err`, **not** a silent
  `Ok(false)` — fail-closed but observable.
- **Secure DLMS decryption (per-device key).** The v4 UTT-S+ binary frame is
  AES-256-GCM encrypted; its header (version, manuf ID, LDN, timestamp) is
  plaintext and precedes the ciphertext, so the resolve order is
  `parse_header → meter_id (LDN) → fetch enckey → parse(bytes, Some(key))`.
  The header-only parse runs CRC-32 + version check, no decrypt (`DlmsHeader` /
  `parse_header`, verified `crates/aggregator-stacks/src/binary_decoder.rs:32`,
  `:48`); full decrypt is `parse(payload, Some(key))`
  (`crates/aggregator-stacks/src/binary_decoder.rs:98`). The per-device AES-256
  key lives in Redis at `gridtokenx:devices:{meter_id}:enckey` (64-char hex, 32
  bytes), fetched by the self-healing `DeviceKeyRegistry` (mirrors the verifier).
  The gRPC ingest path resolves + decrypts in `decode_secure_frame`
  (`crates/aggregator-api/src/grpc/service.rs:55`); the branch policy is the pure
  `apply_dlms_key_policy` (`:94`). **Fail-closed:** under `ENVIRONMENT=production`
  a frame whose `enckey` is missing is skipped (never decoded plaintext); the
  dev/legacy plaintext fallback is gated behind `ALLOW_PLAINTEXT_DLMS=true` and
  logged loud. Redis-unreachable / malformed key ⇒ loud skip, never a silent
  plaintext decode.
- **REST `dlms-enc` envelope (the JSON counterpart of the binary frame).** The
  HTTP IoT gateway is mTLS; a meter POSTs `{"protocol":"dlms-enc","device_id":…,
  "payload":{"enc":{"counter":<i64>,"nonce":<b64 12B>,"ciphertext":<b64>,"kid":<i64?>}}}`.
  `decrypt_dlms_envelope` (`crates/aggregator-api/src/handlers.rs:178`) AES-256-GCM
  decrypts with AAD `device_id:counter`; the plaintext is the canonical
  (sorted-keys, no-space) OBIS JSON — the same object a plaintext `dlms` frame
  carries, including its inner Ed25519 signature — so post-decrypt the path is
  identical to plaintext (`payload.protocol` is rewritten to `dlms`). **Order
  matters:** authenticate (GCM) BEFORE touching the replay store, so a forged
  frame never advances the counter; only an authenticated frame bumps it via
  `check_and_bump_counter` (`handlers.rs:258`). Outcomes: decrypt/contract
  failure ⇒ `400`, replayed/non-increasing counter ⇒ `409`, and under secure
  mode a non-`dlms-enc` meter frame ⇒ `426` (`handlers.rs:314`).
- **Rotated (versioned) per-device key.** When the envelope carries a `kid`, the
  key is the Vault-Transit-wrapped GUEK at `gridtokenx:devices:{meter_id}:enckey:v{kid}`
  (absent `kid` ⇒ the legacy unversioned `enckey`). `get_device_aes_key_versioned`
  (`crates/aggregator-persistence/src/infra/crypto.rs:611`) reads the wrapped blob
  (`:624`), unwraps via Vault, and caches by `(meter_id, kid)`. The sender keeps a
  small **grace window** of prior versions live so frames signed under the previous
  key still decode across a rotation; the simulator reconciles its prune state from
  Redis on restart so old `enckey:v*` versions are bounded to the grace count
  rather than leaking across restarts.
- **Protocol resolution.** DLMS/COSEM is the only meter protocol. An ingest
  request with `protocol = "auto"` (or omitted) resolves to `dlms`; the only
  other accepted value is `simulator` (unsigned dev bypass). Wired into single
  ingest (`crates/aggregator-api/src/handlers.rs:180`) and per-item in batch
  ingest (`crates/aggregator-api/src/handlers.rs:361`); both dispatch to the lone
  `dlms_stack` (`crates/aggregator-api/src/handlers.rs:270`).
- **Secure mode (locked-down deployments).** `AGGREGATOR_REQUIRE_SECURE=true`
  (`secure_mode_enabled`, `crates/aggregator-api/src/handlers.rs`) hard-overrides
  every ingest bypass, fail-closed regardless of the dev env vars: the REST
  unverified-telemetry hatch (`signature_enforcement_disabled`) and the unsigned
  `simulator` bypass (`simulator_bypass_allowed`) on REST single + batch are both
  forced off, the REST meter path requires an authenticated `dlms-enc` frame
  (non-encrypted ⇒ `426 UPGRADE_REQUIRED`), and on gRPC the `SKIP_SIG_VERIFY` bulk
  bypass (`bulk_skip_verify_allowed`) and `ALLOW_PLAINTEXT_DLMS` fallback
  (`plaintext_dlms_allowed`) are forced off
  (`crates/aggregator-api/src/grpc/service.rs`). Default off so dev/e2e keep their
  bypasses.
- **Dissemination (self-healing).** Verified readings fan out to
  zone-partitioned Redis Streams; the publisher rebuilds its connection and
  retries the `XADD` once on transport error (`Router::disseminate`, verified
  `crates/aggregator-logic/src/router.rs:84`).

### Routing: latency & degraded behavior

End-to-end hop timing (steady state). The security path is synchronous and
fail-closed; every downstream sink is fire-and-forget so a sink outage never
stalls realtime ingest. Only the zone `XADD` can back-pressure the caller —
it is the operational spine.

| Hop | Mechanism | Steady-state latency |
| --- | --- | --- |
| ingress → verify | in-proc Ed25519 / DLMS, Redis key fetch | sub-ms |
| verify → zone `XADD` | sync `XADD` to Redis Streams | sub-ms |
| stream → zone ingester | `XREAD` block 2000ms, batch count 25 | 0–2 s |
| ingester → InfluxDB `energy` | fire-and-forget, batched flush | ~0–2 s |
| completed bin → InfluxDB `billing` | flush loop `BILLING_FLUSH_INTERVAL_SECS` (30) + `BILLING_FLUSH_GRACE_SECS` (120) | ~120–150 s after window close |
| completed bin → surplus mint (NATS → Chain Bridge) | spawned task, NATS req-reply 30 s timeout | seconds, off critical path |
| grid status → Kafka | publisher every `GRID_STATUS_PUBLISH_SECS` (30) | ~30 s |

Failure policy per hop — **fail-closed** (refuse) vs **fire-and-forget** (drop, keep going):

| Hop | Policy | Behavior when its backend is down |
| --- | --- | --- |
| Signature / DLMS-key verify | **fail-closed** | Redis-unreachable ⇒ loud `Err`, never silent `Ok(false)`; prod missing `enckey` ⇒ frame skipped (`crates/aggregator-persistence/src/infra/crypto.rs:81`) |
| Zone `XADD` | sync + **retry-once** | rebuild connection, retry once; persistent fail surfaces to caller as back-pressure (`crates/aggregator-logic/src/router.rs:84`) |
| InfluxDB (`energy` / `billing`) | **fire-and-forget** | drop batch, `warn!`, ingest continues — InfluxDB latency/outage never blocks Redis dissemination |
| Kafka (`meter.readings` / grid status) | async best-effort | publish error logged, message dropped |
| Surplus mint (NATS) | **fire-and-forget spawn** | missing wallet ⇒ skip + evict bin; bridge slow/down ⇒ bin still evicts (idempotency key backstops replay) |
| Meter registry (Postgres tier) | **degraded tiers** | PG down ⇒ Redis-only; neither configured ⇒ nil-user fallback (`crates/aggregator-persistence/src/infra/meter_registry.rs`) |
| API-key auth (IAM) | **degraded** | IAM connection error ⇒ static `GRIDTOKENX_API_KEYS`; a definitive IAM reject ⇒ 401 (no static retry) |

### Observability (Prometheus `/metrics`)

The Prometheus exporter is installed at startup (`src/main.rs`, step 7b) and
rendered on the IoT gateway at `/metrics` (behind the same mTLS as ingest — a
scraper presents a CA-issued client cert; the superproject's prometheus job uses
`reporting-service.crt`). Settlement-path observability:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `aggregator_settlement_path` | gauge | `path` = `nats` \| `grpc` \| `disabled` | The active surplus-mint path, set once at startup **after** the recorder is installed. Active path = `1`, others = `0`. `disabled` = `MINT_VIA_CHAIN_BRIDGE` unset or NATS unreachable. |
| `aggregator_mint_total` | counter | `outcome` = `settled` \| `skipped` \| `failed` \| `no_surplus`; `reason` = `ok` \| `no_wallet` \| `resolve_err` \| `mint_err` | One increment per completed billing bin in the settlement sweep. `skipped/no_wallet` surfaces the otherwise-silent unregistered-meter case; `no_surplus` is the net-consumption denominator. |

Alert rules live in the superproject `monitoring/prometheus_rules.yml`
(`AggregatorSurplusMintDisabled`, `AggregatorMintSkipSpike`).

**API-key auth cache.** `crates/aggregator-api/src/auth.rs` caches the IAM
`VerifyApiKey` verdict to keep sustained ingest from flooding IAM (each call is a
Redis event + DB write). Positive verdicts are trusted for
`API_KEY_POS_CACHE_TTL_SECS` (default 60); **definitive IAM rejects** for the
shorter `API_KEY_NEG_CACHE_TTL_SECS` (default 10) so a replayed bad/rotated key is
also bounded to one IAM round-trip per TTL. IAM *connection errors* are never
cached — they must still fall through to the static-key fallback.

### Dispatch layer (VPP flex)

Frequency-driven demand response. The fleet itself is the frequency sensor —
no external SCADA feed:

- **Self-sourced grid status.** The zone ingester feeds each reading's
  `frequency` / `frequency_hz` metadata into a rolling window
  (`FrequencyMonitor`, verified `crates/aggregator-logic/src/grid_status.rs:19`;
  ingester hook verified
  `crates/aggregator-api/src/ingester/zone_ingester.rs:466`, extraction `:574`).
  Implausible samples (<40 / >70 Hz) are dropped. A publisher task in `main`
  turns the window mean into `GridStatusEvent` JSON on the Kafka dispatch topic
  every `GRID_STATUS_PUBLISH_SECS` (default 30s; verified `src/main.rs:243`).
- **Dispatch engine.** A Kafka listener (verified `src/main.rs:316`) feeds each
  grid-status frequency to `DispatchEngine::evaluate_and_dispatch` (verified
  `crates/aggregator-logic/src/dispatch/engine.rs:133`): below
  `DISPATCH_FREQ_LOW_HZ` ⇒ FLEX_UP, above `DISPATCH_FREQ_HIGH_HZ` ⇒ FLEX_DOWN,
  capacity `DISPATCH_CAPACITY_KW`. Dispatch refuses to fire with zero completed
  aggregation capacity. Repeat suppression is tracked **per action**: a
  re-dispatch of the same action waits out `DISPATCH_COOLDOWN_SECS` (default
  900 = one 15-minute aggregation window); a flipped action fires immediately on its own
  independent timer, so an oscillating frequency cannot reset the cooldown by
  alternating directions; a cooldown starts only on success (`cooldown_allows`,
  verified `crates/aggregator-logic/src/dispatch/engine.rs:41`).
- **Adapters.** `DISPATCH_ADAPTER` picks `grpc` (ConnectRPC to edge
  controllers), `ieee` (IEEE 2030.5 DERControl), or `openleadr` (default when
  configured, else `ieee`).
- **OpenADR 3, BL side (outbound).** `OpenLeadrAdapter` (verified
  `crates/aggregator-logic/src/standards/openleadr.rs:25`) acts as business
  logic against a VTN (`OPENLEADR_VTN_URL`): each dispatch becomes an OpenADR
  event with a signed-kW `DISPATCH_SETPOINT` payload (up = +, down = −;
  `dispatch_event`, verified
  `crates/aggregator-logic/src/standards/openleadr.rs:92`). The program is
  resolved by name before create — blind create 409s forever after a restart.
- **OpenADR 3, VEN side (inbound).** `OpenLeadrVenListener` (verified
  `crates/aggregator-logic/src/standards/openleadr_ven.rs:47`) polls a
  (typically utility-operated) VTN (`OPENLEADR_VEN_VTN_URL`) for
  `DISPATCH_SETPOINT` events and executes them through an injected adapter —
  at startup it self-registers a VEN object named `OPENLEADR_VEN_CLIENT_NAME`
  on the VTN, best-effort (`ensure_registered`, verified
  `crates/aggregator-logic/src/standards/openleadr_ven.rs:191`) —
  `ieee` default or `grpc`, **never** `openleadr`, which would loop events back
  to a VTN. Event schedules are honored across **all** setpoint intervals
  (`decide`, verified
  `crates/aggregator-logic/src/standards/openleadr_ven.rs:552`): each interval
  executes as its window opens (deduped per interval), future windows wait,
  an event is done only when no pending interval remains, the interval-level
  period wins over the event-level default, and a period-less interval
  executes immediately. Events dedupe on id + `modificationDateTime` —
  persisted to Redis (`gridtokenx:openleadr:ven:executed`) so a restart does
  not re-execute still-listed events; failed dispatches retry next poll, and
  entries for events the VTN no longer lists are pruned after 7 days
  (`poll_once`, verified
  `crates/aggregator-logic/src/standards/openleadr_ven.rs:226`). Optional
  `OPENLEADR_VEN_TARGET` restricts polling to events carrying that target. An
  executed event that vanishes from the VTN while still active is flagged loud
  (cancellation visibility) — no automatic revert, by design. Each executed
  dispatch is confirmed back to the VTN as an OpenADR report (AGGREGATED_REPORT
  resource, SETPOINT payload; best-effort — a report failure never fails or
  retries the dispatch; `post_execution_report`, verified
  `crates/aggregator-logic/src/standards/openleadr_ven.rs:392`).
- **Local test loop.** The superproject compose runs an `openleadr-vtn` service
  (upstream openleadr-rs v0.2.3, host port 4031) + seeded dev OAuth clients;
  `just openadr-e2e` proves the full loop telemetry → frequency window → Kafka
  → dispatch → VTN event → VEN execution.

> **No settlement here.** On-chain generation-mint / settlement (the former
> "Path B": 15-min billing bins → Plonky2 ZK-rollup → Merkle root → HyperEVM)
> was removed from this service. The Aggregator Bridge produces verified,
> aggregated telemetry and drives flex dispatch only; downstream token issuance
> and settlement are the Market Layer's concern (below), reached through other
> platform services, not from this bridge.

## 4. Market Layer (downstream — external to this service)
- **Settlement:** HyperEVM.
- **Tokens:** ERC-1155 (Energy Tokens), veW2T (Governance).
- **Market Engines:** P2P Order Book, I-REC Minting, HIP-3 Derivatives.

## 5. Testing
- See [TEST.md](TEST.md) — unit-test inventory (per crate/file) + the superproject pytest e2e suite (`20_oracle`, `30_settlement`, `90_golden_path` cover this layer).
