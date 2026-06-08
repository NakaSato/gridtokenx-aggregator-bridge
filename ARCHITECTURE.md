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

## 3. Oracle Bridge Layer
- **UTT Ingestion:** Verified entry point for both paths.
- **ZK-Prover:** Plonky2 (Goldilocks field, FRI-based).
- **Finality:** Merkle Root + Plonky2 Proof.

### Ingestion pipeline

- **Signature verification (fail-closed, self-healing).** Ed25519 telemetry
  signatures verified against device pubkeys in Redis
  (`gridtokenx:devices:{meter_id}:pubkey`). The verifier holds a Redis URL and a
  lazily-rebuilt connection (`SignatureVerifier`, verified
  `crates/oracle-persistence/src/infra/crypto.rs:19`); a transport error rebuilds
  the connection and retries once (`get_with_retry`, verified
  `crates/oracle-persistence/src/infra/crypto.rs:81`) so a Redis restart no longer
  freezes verification. Redis-unreachable returns a loud `Err`, **not** a silent
  `Ok(false)` — fail-closed but observable.
- **Protocol auto-detect.** When an ingest request sets `protocol = "auto"` (or
  omits it), the stack is chosen from the payload field set — dlms / ocpp /
  openadr / sunspec, defaulting to dlms (`detect_protocol`, verified
  `crates/oracle-stacks/src/stacks/mod.rs:19`). Wired into single ingest
  (`crates/oracle-api/src/handlers.rs:205`) and per-item in batch ingest
  (`crates/oracle-api/src/handlers.rs:351`).
- **Dissemination (self-healing).** Verified readings fan out to
  zone-partitioned Redis Streams; the publisher rebuilds its connection and
  retries the `XADD` once on transport error (`Router::disseminate`, verified
  `crates/oracle-logic/src/router.rs:84`).

## 4. Market Layer
- **Settlement:** HyperEVM.
- **Tokens:** ERC-1155 (Energy Tokens), veW2T (Governance).
- **Market Engines:** P2P Order Book, I-REC Minting, HIP-3 Derivatives.
