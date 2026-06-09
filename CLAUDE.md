# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> Scope: the **Aggregator Bridge** service — a git submodule of the `gridtokenx-coresystem` superproject.
> The superproject `../CLAUDE.md` holds platform-wide rules (sync-core/async-edges, error/logging/Axum conventions,
> blockchain-via-Chain-Bridge, the `code-review-graph` MCP graph). This file covers only what is specific to this crate.

## What this service is

High-throughput **ingestion + VPP convergence layer**. Decoupled from edge hardware; it verifies, aggregates, and
disseminates inbound telemetry. Two flows:

- **Path A (operational)**: real-time Ed25519-signed telemetry → verify → zone-partitioned Redis Streams → VPP forecasting/optimization.
- **Path B (settlement)**: 15-minute batched attestations → Plonky2 ZK-rollup → Merkle root → HyperEVM settlement.

See [ARCHITECTURE.md](ARCHITECTURE.md) (source of truth, the four-layer VPP map) and [PROTOCOL.md](PROTOCOL.md)
(the v4 UTT-S+ binary frame layout + TLV dictionary). Both are doc-lint gated — edit the doc next to the code you change.

## Build & test

Independent Cargo workspace — `cd` into this service before any `cargo`. **Do not** run cargo from the superproject root.

```bash
cargo check                       # fast feedback (whole workspace)
cargo check -p aggregator-logic   # single crate
cargo test  -p aggregator-stacks  # single crate's tests
cargo test test_name -- --nocapture
cargo build --release             # LTO + panic=abort + strip (slow; see [profile.release])
```

`cargo test` is the test path — tests live in `#[cfg(test)] mod tests` inline (no `tests/` dirs). The README references
`scripts/test-e2e.sh` / `scripts/register-edge-key.sh`, but those scripts are **not** in this submodule (provided by the superproject's tooling).

## Crate layout & dependency direction

Workspace of 6 crates under `crates/`, plus a thin binary at `src/main.rs`. Strict one-way dependency flow — never reverse:

```
core ← protocol ← stacks ← persistence ← logic ← api ← (src/main.rs binary)
```

| Crate | Role |
| --- | --- |
| `aggregator-core` | Domain models, numeric types. Zero internal deps. |
| `aggregator-protocol` | Generated ConnectRPC/prost types from `proto/{oracle,dispatch}.proto` via `build.rs` → `OUT_DIR`. Packages: `oracle::*`, `dispatch::*`, `identity::*`. |
| `aggregator-stacks` | DLMS/COSEM meter decoder (`dlms`) + `binary_decoder` (secure v4 frame). DLMS/COSEM is the only meter protocol; `protocol="auto"`/omitted resolves to `dlms`. |
| `aggregator-persistence` | Edges: Redis crypto verifier, Kafka, RabbitMQ, meter registry, circular-buffer/sync storage. |
| `aggregator-logic` | Aggregator, Router (dissemination), ZK prover (Plonky2 circuit), dispatch engine, IEEE 2030.5 standards. Depends on `gridtokenx-blockchain-core` (sibling submodule). |
| `aggregator-api` | HTTP handlers, gRPC service, auth, ingesters (zone, settlement, batcher), `AppState`. Depends on `gridtokenx-telemetry` (sibling submodule). |

`src/main.rs` is a wiring-only entrypoint: it re-imports everything through `aggregator_api::{...}` and runs the HTTP + gRPC servers. New business logic goes in the crate that matches the dependency rule, **not** in `main.rs`.

## Runtime shape (from src/main.rs)

- Two servers run concurrently: **HTTP IoT gateway** on `IOT_GATEWAY_PORT` (default `4010`) and **gRPC ingestion** on `GRPC_PORT` (default **5030**, the canonical mesh port — `50051` in `.env.example` is the simulator override).
- HTTP routes: `/health`, `/v1/private-network/ingest[/batch]`, `/v1/ingest/telemetry[/batch]`.
- **Degraded-mode by design**: Redis (3s timeout), NATS, Kafka, RabbitMQ, IAM gRPC, and the settlement signer all fall back to disabled/None on connect failure with a `warn!` — the process still starts. Don't "fix" these by making them fatal.
- Background tasks: zone ingester, Kafka dispatch listener, UTT settlement engine, gRPC server — all gated on a shared `CancellationToken` driven by SIGINT/SIGTERM.
- Env interpolation: values support `${VAR}` expansion via `expand_env`.

## Security-critical invariants (don't regress)

- **Fail-closed, loud verification.** Ed25519 signatures checked against device pubkeys in Redis at `gridtokenx:devices:{meter_id}:pubkey`. Redis-unreachable must return `Err`, **never** a silent `Ok(false)` (`crates/aggregator-persistence/src/infra/crypto.rs`).
- **Encrypted DLMS is wired (no longer a gap).** The secure v4 binary frame is AES-256-GCM; the per-device key lives at `gridtokenx:devices:{meter_id}:enckey` (64-char hex, 32 bytes) and is fetched by `DeviceKeyRegistry` (self-healing, mirrors the verifier). gRPC ingest resolves + decrypts in `decode_secure_frame`; the branch policy is the pure `apply_dlms_key_policy` (`crates/aggregator-api/src/grpc/service.rs`). Under `ENVIRONMENT=production`, a missing `enckey` ⇒ frame **skipped** (fail-closed, never silent plaintext). The dev plaintext fallback is gated behind `ALLOW_PLAINTEXT_DLMS=true` and logged loud — don't decode plaintext by default.
- **Self-healing connections.** The `SignatureVerifier` owns a Redis *URL* (not a one-shot connection) and rebuilds + retries once on transport error (`get_with_retry`). The `Router::disseminate` publisher does the same for `XADD`. This is why a Redis restart no longer freezes the bridge — preserve it.
- **Production enforcement.** `ENVIRONMENT=production` makes signature verification strict.
- **DLMS REST canonical sign-value.** The REST sign-target is `{device_id}:{value}:{timestamp_ms}` where `value` is protocol-native (`canonical_sign_value`, `crates/aggregator-api/src/handlers.rs`). For DLMS it resolves `kwh` → `energy_consumed` → `energy_generated` → OBIS active import `1.1.1.8.0.255` (Wh/1000) → OBIS export `1.1.2.8.0.255` (Wh/1000). A real OBIS-only meter must sign that derived kWh — don't drop the OBIS fallback or pure-OBIS payloads sign `:0:` and fail closed. (Binary gRPC path signs raw frame bytes; not affected.)
- Auth falls back to static `GRIDTOKENX_API_KEYS` (comma-separated) when the IAM gRPC client is unavailable.

## Config

Copy `.env.example` → `.env`. Key vars: `REDIS_URL`, `IOT_GATEWAY_PORT`, `GRPC_PORT`, `GRIDTOKENX_API_KEYS`,
`IAM_SERVICE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `RABBITMQ_URL`, `NATS_URL`, `AGGREGATOR_BRIDGE_SIGNING_KEY`,
`SETTLEMENT_API_URL`, `IOT_NUM_ZONES` (default 10), `ENVIRONMENT` (`production` ⇒ strict sig + DLMS
decryption), `ALLOW_PLAINTEXT_DLMS` (dev-only; allow plaintext v4 frames when a device has no `enckey`).
