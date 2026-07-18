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
dispatch.

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
| `aggregator_mint_total` | counter | `outcome` = `settled` \| `skipped` \| `failed` \| `no_surplus` \| `lost` \| `parked`; `reason` = `ok` \| `no_wallet` \| `invalid_wallet` \| `in_flight` \| `resolve_err` \| `mint_err` \| `reply_timeout` \| `outbox_and_mint_failed` \| `expired` | One increment per completed billing bin in the settlement sweep. `skipped/no_wallet` surfaces the otherwise-silent unregistered-meter case; `skipped/invalid_wallet` is a declined publish — the resolved wallet isn't a parseable Solana address (`wallet_is_valid`, `crates/aggregator-persistence/src/infra/mint.rs`), so no NATS round-trip is spent; `skipped/in_flight` is a retry declined because an attempt for the same `{serial}:{window}` key is still awaiting its reply in this process (`MintInFlight`); `no_surplus` is the net-consumption denominator. `failed/reply_timeout` is a lost mint reply (intent durably queued in JetStream, the mint may have landed — the outbox retry is dedup-safe), distinct from a hard `failed/mint_err` rejection. `lost/outbox_and_mint_failed` is the durable-outbox enqueue failing twice **and** the last-resort immediate mint also failing — the bin's durable Redis entry is retained for manual recovery instead of evicted (`src/main.rs`, `MintHandoff::Lost`). `parked/expired` is an outbox entry that aged past `MINT_OUTBOX_MAX_AGE_SECS` (default 7 days, `0` disables) without landing on-chain and was moved to `gridtokenx:billing:mint_outbox:parked`, out of the retry path — re-enqueue manually (`HSET` back into `gridtokenx:billing:mint_outbox` + `HDEL` from `:parked`) to resume retries. |

Alert rules live in the superproject `monitoring/prometheus_rules.yml`
(`AggregatorSurplusMintDisabled`, `AggregatorMintSkipSpike`, `AggregatorMintLost`,
`AggregatorMintParked`).

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
  `crates/aggregator-logic/src/dispatch/engine.rs:192`): below
  `DISPATCH_FREQ_LOW_HZ` ⇒ FLEX_UP, above `DISPATCH_FREQ_HIGH_HZ` ⇒ FLEX_DOWN,
  capacity `DISPATCH_CAPACITY_KW`. Dispatch refuses to fire with zero completed
  aggregation capacity (checked once for the whole fan-out, `has_dispatch_capacity`).
  Repeat suppression is tracked **per (action, adapter)**: a re-dispatch of the
  same action to the same adapter waits out `DISPATCH_COOLDOWN_SECS` (default
  900 = one 15-minute aggregation window); a flipped action — or the same action
  to a *different* adapter — fires immediately on its own independent timer, so
  an oscillating frequency cannot reset the cooldown by alternating directions
  and one fan-out target cannot suppress another; a cooldown starts only on
  success (`cooldown_allows`, verified
  `crates/aggregator-logic/src/dispatch/engine.rs:46`).
- **Adapters (single or fan-out).** `DISPATCH_ADAPTERS` (csv) dispatches each
  excursion to **every** listed adapter at once — e.g. a downstream in-mesh VTN
  **and** a utility VTN — with per-(action,adapter) cooldown and partial-failure
  isolation (one adapter erroring never stops the others; `evaluate_and_dispatch`
  returns `Err` only when *all* targets fail). The legacy single-value
  `DISPATCH_ADAPTER` is still honored as a one-element list (back-compat); when
  neither is set, the default is `openleadr` (when configured) else `ieee`.
  Adapter names: `grpc` (ConnectRPC to edge controllers), `ieee` (IEEE 2030.5
  DERControl), `openleadr` (OpenADR 3 BL). Unknown names are dropped loud;
  selection logic is the pure `select_adapters_from` (verified
  `crates/aggregator-logic/src/dispatch/engine.rs:126`).
- **OpenADR 3, BL side (outbound).** `OpenLeadrAdapter` (verified
  `crates/aggregator-logic/src/standards/openleadr.rs:25`) acts as business
  logic against a VTN (`OPENLEADR_VTN_URL`): each dispatch becomes an OpenADR
  event with a signed-kW `DISPATCH_SETPOINT` payload (up = +, down = −;
  `dispatch_event`, verified
  `crates/aggregator-logic/src/standards/openleadr.rs:92`). The program is
  resolved by name before create — blind create 409s forever after a restart.
- **OpenADR 3, VEN side (inbound).** `OpenLeadrVenListener` (verified
  `crates/aggregator-logic/src/standards/openleadr_ven.rs:47`) polls a
  (typically utility-operated) VTN (`OPENLEADR_VEN_VTN_URL`) for dispatch
  setpoint events — both absolute `DISPATCH_SETPOINT` and relative
  `DISPATCH_SETPOINT_RELATIVE` (`setpoint_kind`, both actuate the same signed-kW
  FLEX path, relative tagged only in logs/metrics `executed_relative`) — and
  executes them through an injected adapter —
  at startup it self-registers a VEN object named `OPENLEADR_VEN_CLIENT_NAME`
  on the VTN, best-effort (`ensure_registered`, verified
  `crates/aggregator-logic/src/standards/openleadr_ven.rs:194`) —
  `ieee` default or `grpc`, **never** `openleadr`, which would loop events back
  to a VTN. Event schedules are honored across **all** setpoint intervals
  (`decide`, verified
  `crates/aggregator-logic/src/standards/openleadr_ven.rs:583`): each interval
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
- **Why the VEN polls (not webhook).** OpenADR 3.1 defines push subscriptions,
  but the backing library (`openleadr-rs` v0.2.4 — the spec floor for 3.1;
  3.0 ended at v0.1) does **not** implement real-time webhook subscriptions
  (166/168 OpenADR Alliance tests pass; the 2 failures are intentional, from a
  differing permission model, not the unsupported subscriptions feature). So the
  listener falls back to a poll loop every
  `OPENLEADR_VEN_POLL_SECS` (default 30, verified
  `crates/aggregator-logic/src/standards/openleadr_ven.rs:51`). The polling
  cadence is a dependency constraint, not a design preference — `poll_interval`
  bounds dispatch latency to one poll; lower it for tighter response, raise it
  to spare the VTN. Revisit if openleadr-rs ships subscriptions.
- **Local test loop.** The superproject compose runs an `openleadr-vtn` service
  (upstream openleadr-rs v0.2.4, host port 4031) + seeded dev OAuth clients;
  `just openadr-e2e` proves the full loop telemetry → frequency window → Kafka
  → dispatch → VTN event → VEN execution.

> **Surplus minting (Chain Bridge over NATS, chain-light).** When a 15-min
> billing window closes with net surplus generation, the settlement sink mints
> it to the meter owner via Chain Bridge over NATS (`chain.tx.mint`). The
> service carries no Solana / blockchain-core dependency — it sends intent only.
> Disabled by default; gated on `MINT_VIA_CHAIN_BRIDGE` + `NATS_URL`.

## 4. Market Layer (downstream — external to this service)
- **Tokens:** GRID / GRX / REC, issued on Solana via Chain Bridge.
- **Market Engines:** P2P Order Book, REC minting.

## 5. Testing
- See [TEST.md](TEST.md) — unit-test inventory (per crate/file) + the superproject pytest e2e suite (`20_oracle`, `30_settlement`, `90_golden_path` cover this layer).
