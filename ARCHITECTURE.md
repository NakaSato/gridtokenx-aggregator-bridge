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
  - `attestation.signed`: 15-minute intervals.

## 3. Aggregator Bridge Layer
- **UTT Ingestion:** Verified entry point for both paths.
- **ZK-Prover:** Plonky2 (Goldilocks field, FRI-based).
- **Finality:** Merkle Root + Plonky2 Proof.

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
- **Protocol auto-detect.** When an ingest request sets `protocol = "auto"` (or
  omits it), the stack is chosen from the payload field set — dlms / ocpp /
  openadr / sunspec, defaulting to dlms (`detect_protocol`, verified
  `crates/aggregator-stacks/src/stacks/mod.rs:19`). Wired into single ingest
  (`crates/aggregator-api/src/handlers.rs:205`) and per-item in batch ingest
  (`crates/aggregator-api/src/handlers.rs:351`).
- **Dissemination (self-healing).** Verified readings fan out to
  zone-partitioned Redis Streams; the publisher rebuilds its connection and
  retries the `XADD` once on transport error (`Router::disseminate`, verified
  `crates/aggregator-logic/src/router.rs:84`).

## 4. Market Layer
- **Settlement:** HyperEVM.
- **Tokens:** ERC-1155 (Energy Tokens), veW2T (Governance).
- **Market Engines:** P2P Order Book, I-REC Minting, HIP-3 Derivatives.
