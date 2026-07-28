# GridTokenX Aggregator Bridge & VPP Operation Service

The **GridTokenX Aggregator Bridge** is a **private, cloud-native convergence layer** that orchestrates
Virtual Power Plant (VPP) operations and provides cryptographic integrity for grid-scale energy assets. It
is the **central connection point for every device node** in the smart grid — the high-throughput ingestion
entry point for the GridTokenX VPP, bridging edge gateways to the optimization platform.

It is a git submodule of the `gridtokenx-coresystem` superproject and an **independent Cargo workspace**
(no root `Cargo.toml`; `cd` into this directory before any `cargo` command).

---

## Table of Contents

- [What This Service Does](#what-this-service-does)
- [Architectural Role](#architectural-role)
- [Data Flow](#data-flow)
- [Crate Layout](#crate-layout)
- [Runtime Shape](#runtime-shape)
- [Endpoints](#endpoints)
- [Build & Test](#build--test)
- [Configuration](#configuration)
- [Security-Critical Invariants](#security-critical-invariants)
- [Chain-Light Surplus Minting](#chain-light-surplus-minting)
- [Flex Dispatch (OpenADR / IEEE 2030.5)](#flex-dispatch-openadr--ieee-20305)
- [Tech Stack](#tech-stack)
- [Documentation Index](#documentation-index)

---

## What This Service Does

High-throughput **ingestion + VPP convergence layer**. Decoupled from edge hardware, it verifies,
aggregates, and disseminates inbound telemetry:

- **VPP Ingestion** — high-concurrency ingestion for normalized smart-meter, EV, and BESS data streams.
- **Secure Telemetry** — Ed25519 (Base58) signature verification against device pubkeys in Redis;
  AES-256-GCM decryption of secure DLMS/COSEM v4 binary frames.
- **Real-time Orchestration** — sub-100ms routing of verified readings to zone-partitioned Redis Streams
  feeding VPP forecasting and MILP optimization.
- **Independent Realtime History** — every disseminated reading is fire-and-forget written to this
  service's **own** InfluxDB v2 instance (never a shared one), so a slow/down InfluxDB never blocks ingest.
- **Billing Aggregation** — 15-minute billing bins with time-of-use (TOU) peak/off-peak split and
  max-demand tracking.
- **Flex Dispatch** — frequency-driven demand response via IEEE 2030.5 / OpenADR 3 (OpenLEADR), VTN and
  VEN sides.
- **Chain-Light Surplus Minting** — on a net-surplus window, sends a mint intent to Chain Bridge over NATS
  (`chain.tx.mint`). No Solana / blockchain-core dependency. Disabled by default.
- **Production Mode Enforcement** — strict signature verification and DLMS decryption when
  `ENVIRONMENT=production`.

---

## Architectural Role

The Aggregator Bridge is the **high-performance ingest layer** of the GridTokenX Infrastructure platform. It
is decoupled from the hardware (edge meters / oracles) and focuses on processing the inbound telemetry flow:
real-time, cryptographically signed telemetry — verified, aggregated, and synchronized with the VPP
platform, then driving flex dispatch.

It is **degraded-mode by design**: Redis, Kafka, RabbitMQ, InfluxDB, and IAM gRPC all fall back to
disabled/None on connect failure with a `warn!` — the process still starts. This is intentional, not a bug.

---

## Data Flow

```
                          Ed25519-signed telemetry
   Edge Gateways / Meters ─────────────────────────►  Aggregator Bridge
   (DLMS/COSEM, secure v4 binary frames)                    │
                                                             ▼
                                              ┌── Verify (Ed25519, fail-closed)
                                              ├── Decrypt (AES-256-GCM, secure DLMS)
                                              ├── Aggregate (15-min TOU billing bins)
                                              │
              ┌───────────────────────────────┼───────────────────────────────┐
              ▼                                ▼                                ▼
   Zone-partitioned Redis Streams   InfluxDB v2 (independent)      Kafka MeterReadingEvent
   (operational: VPP forecast,      (realtime history +            (downstream consumers)
    MILP optimize, dispatch)         rolled-up billing bins)
              │
              ▼
   Flex Dispatch (IEEE 2030.5 / OpenADR)    Surplus window ──► Chain Bridge (NATS chain.tx.mint)
```

---

## Crate Layout

Workspace of 6 crates under `crates/`, plus a thin wiring-only binary at `src/main.rs`. Strict one-way
dependency flow — never reverse:

```
core ← protocol ← stacks ← persistence ← logic ← api ← (src/main.rs binary)
```

| Crate | Role |
| --- | --- |
| `aggregator-core` | Domain models, numeric types. Zero internal deps. |
| `aggregator-protocol` | Generated ConnectRPC/prost types from `proto/{oracle,dispatch}.proto` via `build.rs`. Packages: `oracle::*`, `dispatch::*`, `identity::*`. |
| `aggregator-stacks` | DLMS/COSEM meter decoder (`dlms`) + `binary_decoder` (secure v4 frame). DLMS/COSEM is the only meter protocol. |
| `aggregator-persistence` | Edges: Redis crypto verifier, Kafka, RabbitMQ, 3-tier meter registry (cache → Redis → Postgres), circular-buffer/sync storage, independent InfluxDB v2 history sink. |
| `aggregator-logic` | Aggregator (15-min billing bins, TOU split, max-demand), Router (dissemination), billing sink, dispatch engine, IEEE 2030.5 / OpenADR. No blockchain deps. |
| `aggregator-api` | HTTP handlers, gRPC service, auth, ingesters (zone, batcher), `AppState`. Depends on `gridtokenx-telemetry` (sibling submodule). |

New business logic goes in the crate that matches the dependency rule, **not** in `src/main.rs`.

---

## Runtime Shape

Two servers run concurrently:

- **HTTP IoT gateway** on `IOT_GATEWAY_PORT` (default `4010`).
- **gRPC ingestion** on `GRPC_PORT` (default `5030`, the canonical mesh port).

Background tasks (all gated on a shared `CancellationToken` driven by SIGINT/SIGTERM):

- Zone ingester
- Kafka dispatch listener
- gRPC server
- Billing-sink flush loop (runs when InfluxDB **or** minting is enabled)
- pg_readings writer (only when `AGGREGATOR_PG_READINGS` enabled)
- Mint outbox drain loop (when minting enabled and Redis reachable)

Env interpolation: values support `${VAR}` expansion.

---

## Endpoints

### HTTP (`IOT_GATEWAY_PORT`, default 4010)

| Route | Auth | Purpose |
| --- | --- | --- |
| `GET /health` | exempt | Liveness |
| `GET /metrics` | exempt | Prometheus metrics |
| `POST /v1/private-network/ingest` | `api_key_auth` | Private-network telemetry ingest |
| `POST /v1/private-network/ingest/batch` | `api_key_auth` | Batch variant |
| `POST /v1/ingest/telemetry` | `api_key_auth` | Legacy telemetry ingest |
| `POST /v1/ingest/telemetry/batch` | `api_key_auth` | Legacy batch variant |

### gRPC (`GRPC_PORT`, default 5030)

Binary UTT-S+ v4 frame ingestion (single + bulk). Resolves per-device enckey and decrypts secure DLMS
frames. See [PROTOCOL.md](PROTOCOL.md) for the frame layout and TLV dictionary.

---

## Build & Test

Independent Cargo workspace — `cd` into this service first. **Do not** run `cargo` from the superproject root.

```bash
cargo check                       # fast feedback (whole workspace)
cargo check -p aggregator-logic   # single crate
cargo test  -p aggregator-stacks  # single crate's tests
cargo test test_name -- --nocapture
cargo build --release             # LTO + panic=abort (slow; see [profile.release])
```

Tests live in `#[cfg(test)] mod tests` inline (no `tests/` dirs). `cargo test --workspace` runs every
crate; live-infra tests are `#[ignore]`-gated (`cargo test -- --ignored`, needs `just orb-up`).

> Note: `strip=true` in the release profile corrupts `sqlx_macros` on some macOS toolchains. Workaround:
> `CARGO_PROFILE_RELEASE_STRIP=false cargo build --release`.

---

## Configuration

Copy `.env.example` → `.env`. Key variables:

### Core

| Var | Default | Purpose |
| --- | --- | --- |
| `REDIS_URL` | — | Device keys, zone streams, durable bins, mint outbox |
| `DATABASE_URL` | `postgresql://gridtokenx_user:...@localhost:7001/gridtokenx` | Meter registry source of truth, optional readings sink |
| `IOT_GATEWAY_PORT` | `4010` | HTTP ingest port |
| `GRPC_PORT` | `5030` | gRPC ingest port (mesh canonical) |
| `IOT_NUM_ZONES` | `10` | Zone-partitioned Redis Streams count |
| `ENVIRONMENT` | — | `production` ⇒ strict signature + DLMS decryption |
| `GRIDTOKENX_API_KEYS` | — | Comma-separated static API keys (fallback when IAM errors) |
| `IAM_SERVICE_URL` | — | IAM gRPC for API-key verification |

### Security & Caching

| Var | Default | Purpose |
| --- | --- | --- |
| `AGGREGATOR_REQUIRE_SECURE` | `false` | `true` ⇒ neutralizes every dev ingest bypass (fail-closed) |
| `ALLOW_PLAINTEXT_DLMS` | `false` | Dev-only: allow plaintext v4 frames when device has no enckey |
| `API_KEY_POS_CACHE_TTL_SECS` | `60` | Positive API-key verdict cache |
| `API_KEY_NEG_CACHE_TTL_SECS` | `10` | Definitive IAM-reject cache |
| `PUBKEY_CACHE_TTL_SECS` | `60` | Positive pubkey cache (**= revocation latency**) |
| `PUBKEY_NEG_CACHE_TTL_SECS` | `10` | Negative pubkey cache |
| `DEVICE_ENCKEY_CACHE_TTL_SECS` | `300` | Device enckey cache |

### Persistence Sinks

| Var | Default | Purpose |
| --- | --- | --- |
| `INFLUXDB_URL` | — | Enables independent InfluxDB v2 history sink (unset ⇒ disabled) |
| `INFLUXDB_ORG` / `INFLUXDB_BUCKET` / `INFLUXDB_TOKEN` | `gridtokenx` / `aggregator_telemetry` / — | InfluxDB config |
| `AGGREGATOR_PG_READINGS` | `false` | `true` ⇒ optional Postgres `meter_readings` sink (needs `DATABASE_URL`) |
| `METER_DATABASE_URL` | — | DB-per-service Phase 2 seam; does NOT migrate at boot |
| `DURABLE_BINS` | on (Redis up) | Write-through in-flight billing bins to Redis for crash recovery |

### Minting & Dispatch

| Var | Default | Purpose |
| --- | --- | --- |
| `MINT_VIA_CHAIN_BRIDGE` | `false` | Enable surplus minting via Chain Bridge over NATS |
| `NATS_URL` | — | JetStream endpoint for mint intents |
| `CHAIN_BRIDGE_SERVICE_IDENTITY` | — | Signed-envelope service identity |
| `BILLING_FLUSH_INTERVAL_SECS` / `BILLING_FLUSH_GRACE_SECS` | `30` / `120` | Settlement flush loop tunables |
| `MINT_RETRY_INTERVAL_SECS` / `MINT_OUTBOX_MAX_AGE_SECS` | `30` / `604800` | Mint outbox drain + park age |
| `OPENLEADR_VTN_URL` | — | Enables OpenLEADR dispatch adapter (VTN side) |
| `OPENLEADR_VEN_VTN_URL` | — | Enables OpenADR VEN polling listener |
| `DISPATCH_ADAPTERS` | — | CSV of `grpc`/`ieee`/`openleadr` for fan-out dispatch |

Full env reference: `.env.example` and [CLAUDE.md](CLAUDE.md).

---

## Security-Critical Invariants

Do **not** regress these:

- **Fail-closed, loud verification.** Ed25519 signatures checked against Redis pubkeys at
  `gridtokenx:devices:{meter_id}:pubkey`. Redis-unreachable returns `Err`, **never** silent `Ok(false)`.
  Only the (static) key fetch is cached — the signature is verified on every call. Positive TTL **is** the
  revocation latency; keep it short.
- **Encrypted DLMS wired.** Secure v4 frame is AES-256-GCM; per-device key at
  `gridtokenx:devices:{meter_id}:enckey`. Under `ENVIRONMENT=production`, a missing enckey ⇒ frame skipped
  (fail-closed, never silent plaintext).
- **Self-healing connections.** Verifier and dissemination publisher own a Redis URL (not a one-shot
  connection) and rebuild + retry once on transport error — a Redis restart no longer freezes the bridge.
- **Secure mode neutralizes every bypass.** `AGGREGATOR_REQUIRE_SECURE=true` hard-overrides all dev escape
  hatches, fail-closed, regardless of dev env vars.
- **Signed mint envelope.** The mint gateway signs with this service's mTLS client key (P-256/ECDSA) and
  attaches an `EnvelopeAuth`; the local layout must stay byte-identical to `gridtokenx-blockchain-core`'s
  `rpc::envelope_auth`.

Device public keys must be registered in Redis under `gridtokenx:devices:{meter_id}:pubkey`.

---

## Chain-Light Surplus Minting

When a 15-min billing window closes with net surplus generation, the settlement sink mints it to the meter
owner via **Chain Bridge over NATS** (`chain.tx.mint`). This service carries **no Solana / blockchain-core
dependency** — it sends intent only and mirrors the wire types locally.

- Disabled by default; gated on `MINT_VIA_CHAIN_BRIDGE` + `NATS_URL`.
- Idempotency key `mint:{serial}:{window_start_ms}`; on-chain `(meter_id, window_start_ms)` PDA is the
  backstop — retries never double-mint.
- **Durable mint outbox** (Redis hash `gridtokenx:billing:mint_outbox`) retries until confirmed on-chain,
  survives restart, and parks entries older than `MINT_OUTBOX_MAX_AGE_SECS`.
- JetStream emit awaits PubAck before the reply — closes the silent-drop gap when the consumer is
  momentarily absent.

---

## Flex Dispatch (OpenADR / IEEE 2030.5)

- **VTN side** — `OPENLEADR_VTN_URL` enables the OpenLEADR dispatch adapter, preferred over `ieee`.
  `DISPATCH_ADAPTERS` fans a frequency excursion out to every listed adapter at once (partial-failure
  isolated). Trigger: a Kafka `GridStatusEvent` on `KAFKA_TOPIC_GRID_STATUS`; requires at least one
  completed aggregation bin.
- **VEN side** — `OPENLEADR_VEN_VTN_URL` enables a polling listener consuming both absolute
  `DISPATCH_SETPOINT` and relative `DISPATCH_SETPOINT_RELATIVE` events, executed via
  `OPENLEADR_VEN_DISPATCH_ADAPTER`. Positive setpoint = FLEX_UP, negative = FLEX_DOWN.

---

## Tech Stack

- **Language**: Rust (Tokio / Axum)
- **Messaging**: Apache Kafka, Redis Streams, NATS JetStream, RabbitMQ
- **Cryptography**: Ed25519 (verification), AES-256-GCM (DLMS frame decryption), P-256/ECDSA (mint envelope)
- **API**: gRPC (Protobuf / prost / tonic), REST
- **Storage**: PostgreSQL (meter registry, optional `meter_readings` sink), InfluxDB v2 (independent
  realtime history), SQLite (circular buffer)
- **Meter Protocol**: DLMS/COSEM (secure v4 UTT-S+ binary frames)

---

## Documentation Index

- [ARCHITECTURE.md](ARCHITECTURE.md) — **Source of truth**: full technical system design (VPP map),
  components, telemetry verification, aggregation, dissemination, flex dispatch.
- [PROTOCOL.md](PROTOCOL.md) — v4 UTT-S+ binary frame layout + TLV dictionary.
- [CLAUDE.md](CLAUDE.md) — service-specific conventions, invariants, and full env reference.
- [DLMS_ENCRYPTION_PLAN.md](DLMS_ENCRYPTION_PLAN.md) — DLMS encryption design.
