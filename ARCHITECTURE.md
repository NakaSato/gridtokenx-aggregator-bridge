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
- **Protocol resolution.** DLMS/COSEM is the only meter protocol. An ingest
  request with `protocol = "auto"` (or omitted) resolves to `dlms`; the only
  other accepted value is `simulator` (unsigned dev bypass). Wired into single
  ingest (`crates/aggregator-api/src/handlers.rs:180`) and per-item in batch
  ingest (`crates/aggregator-api/src/handlers.rs:361`); both dispatch to the lone
  `dlms_stack` (`crates/aggregator-api/src/handlers.rs:270`).
- **Dissemination (self-healing).** Verified readings fan out to
  zone-partitioned Redis Streams; the publisher rebuilds its connection and
  retries the `XADD` once on transport error (`Router::disseminate`, verified
  `crates/aggregator-logic/src/router.rs:84`).

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
