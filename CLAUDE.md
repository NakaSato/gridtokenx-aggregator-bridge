# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> Scope: the **Aggregator Bridge** service — a git submodule of the `gridtokenx-coresystem` superproject.
> The superproject `../CLAUDE.md` holds platform-wide rules (sync-core/async-edges, error/logging/Axum conventions,
> blockchain-via-Chain-Bridge, the `code-review-graph` MCP graph). This file covers only what is specific to this crate.

## What this service is

High-throughput **ingestion + VPP convergence layer** — the central connection point for every device node in the
smart grid. Decoupled from edge hardware; it verifies, aggregates, and disseminates inbound telemetry:

- **Operational flow**: real-time Ed25519-signed telemetry → verify → zone-partitioned Redis Streams → VPP
  forecasting/optimization and flex dispatch (IEEE 2030.5 / OpenADR). Each disseminated reading is also
  written to an **independent InfluxDB v2 instance** (this service's own, shared with nobody) for realtime
  history — async fire-and-forget, so a slow/down InfluxDB never blocks ingest (`Router::disseminate` →
  `InfluxWriter`, `crates/aggregator-persistence/src/infra/influxdb.rs`).

> **No on-chain settlement here.** The former "Path B" (15-min batched attestations → Plonky2 ZK-rollup → Merkle
> root → HyperEVM mint/settlement) has been removed. This service does not mint GRID or talk to the chain.

See [ARCHITECTURE.md](ARCHITECTURE.md) (source of truth, the VPP map) and [PROTOCOL.md](PROTOCOL.md)
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
| `aggregator-persistence` | Edges: Redis crypto verifier, Kafka, RabbitMQ, meter registry, circular-buffer/sync storage, independent InfluxDB v2 history sink (`infra/influxdb.rs`). |
| `aggregator-logic` | Aggregator, Router (dissemination), dispatch engine, IEEE 2030.5 / OpenADR standards. No blockchain deps. |
| `aggregator-api` | HTTP handlers, gRPC service, auth, ingesters (zone, batcher), `AppState`. Depends on `gridtokenx-telemetry` (sibling submodule). |

`src/main.rs` is a wiring-only entrypoint: it re-imports everything through `aggregator_api::{...}` and runs the HTTP + gRPC servers. New business logic goes in the crate that matches the dependency rule, **not** in `main.rs`.

## Runtime shape (from src/main.rs)

- Two servers run concurrently: **HTTP IoT gateway** on `IOT_GATEWAY_PORT` (default `4010`) and **gRPC ingestion** on `GRPC_PORT` (default **5030**, the canonical mesh port — `50051` in `.env.example` is the simulator override).
- HTTP routes: `/health`, `/v1/private-network/ingest[/batch]`, `/v1/ingest/telemetry[/batch]`.
- **Degraded-mode by design**: Redis (3s timeout), NATS, Kafka, RabbitMQ, InfluxDB, and IAM gRPC all fall back to disabled/None on connect failure with a `warn!` — the process still starts. Don't "fix" these by making them fatal.
- Background tasks: zone ingester, Kafka dispatch listener, gRPC server — all gated on a shared `CancellationToken` driven by SIGINT/SIGTERM.
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
`IAM_SERVICE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `RABBITMQ_URL`, `NATS_URL`, `IOT_NUM_ZONES` (default 10),
`ENVIRONMENT` (`production` ⇒ strict sig + DLMS decryption), `ALLOW_PLAINTEXT_DLMS` (dev-only; allow
plaintext v4 frames when a device has no `enckey`).

Meter-service handoff (NATS): when `NATS_URL` is set, each verified smart-meter reading with mintable
surplus (`net_kwh > 0`) is forwarded to **meter-service** on `METER_SERVICE_NATS_SUBJECT` (default
`meter.reading`) as a `MintForwardReading` (`aggregator_core::models`, published by `Router::disseminate`,
`crates/aggregator-logic/src/router.rs`). That payload is the on-chain mint provenance, so it carries a
stable `reading_id` (idempotency key — meter-service uses it as the row PK so a redelivery never
double-ingests/mints) plus `meter_serial`, `energy_kwh`, `timestamp_ms`. The recipient wallet is **not**
on the wire — meter-service derives it from the registered meter owner, so an untrusted forward cannot
redirect minted tokens. meter-service owns the mint decision; this bridge never mints and has no
blockchain deps. Unset `NATS_URL` ⇒ forwarding disabled.

InfluxDB (independent realtime history): `INFLUXDB_URL` enables an InfluxDB v2 sink dedicated to this
service alone — point it at this service's **own** instance (the superproject's `aggregator-influxdb`
compose service), never a shared one. Optional: `INFLUXDB_ORG` (default `gridtokenx`), `INFLUXDB_BUCKET`
(default `aggregator_telemetry`), `INFLUXDB_TOKEN`. Unset `INFLUXDB_URL` ⇒ disabled; unreachable at boot ⇒
`warn!` + disabled. Writes are async fire-and-forget (batched), so InfluxDB latency/outage never blocks the
realtime Redis dissemination path. Measurements: `energy` / `ev_session` / `battery`; tags include
`device_id`, `device_type`, `serial_number`, `zone_code`.

OpenADR 3 dispatch (OpenLEADR): setting `OPENLEADR_VTN_URL` enables the `openleadr` dispatch adapter
(`crates/aggregator-logic/src/standards/openleadr.rs`), preferred over `ieee` in the dispatch engine.
Optional: `OPENLEADR_CLIENT_ID`/`OPENLEADR_CLIENT_SECRET` (OAuth pair), `OPENLEADR_PROGRAM_ID`,
`OPENLEADR_PROGRAM_NAME` (default `gridtokenx-flex-dispatch`), `OPENLEADR_TARGET`,
`OPENLEADR_EVENT_DURATION_HOURS` (default 1.0). A local VTN for testing runs as the superproject's
`openleadr-vtn` compose service (port 4031, upstream openleadr-rs v0.2.3 — same version as the
`openleadr-client`/`openleadr-wire` crates.io deps; dev credentials `bl-client`/`bl-client` are
seeded by the one-shot `openleadr-vtn-seed` service). The dispatch trigger is a Kafka
`GridStatusEvent` JSON message on `KAFKA_TOPIC_GRID_STATUS` (default
`gridtokenx.aggregator.grid_status`); dispatch also requires at least one completed aggregation
bin (capacity > 0), so ingest telemetry first.

OpenADR VEN side: `OPENLEADR_VEN_VTN_URL` enables a polling listener
(`crates/aggregator-logic/src/standards/openleadr_ven.rs`) that consumes `DISPATCH_SETPOINT` events
from a (utility) VTN and executes them via `OPENLEADR_VEN_DISPATCH_ADAPTER` (`ieee` default, `grpc`
→ `DISPATCH_GRPC_URL`) — never `openleadr`, or events would loop back to a VTN. Note: `ieee` is a
simulation stub (logs, no actuation) — main.rs warns loud when the VEN uses it because execution
reports would attest simulated dispatch. Positive setpoint = FLEX_UP, negative = FLEX_DOWN;
multi-interval events execute each interval as its window opens (deduped per interval); events are
deduped by id + modificationDateTime and retried next poll on dispatch failure. At startup the
listener self-registers a VEN object named `OPENLEADR_VEN_CLIENT_NAME` on the VTN (best-effort;
needs `write_vens_ven` scope; `OPENLEADR_VEN_REGISTER=false` disables). When
`OPENLEADR_VEN_VTN_URL` equals `OPENLEADR_VTN_URL` with no program/target filter, main.rs warns:
the VEN would consume the bridge's own outbound events (double actuation).

## Search Tooling

> **Use `rg` (ripgrep), never `grep`.** When shelling out to search files, run `rg` —
> it respects `.gitignore`, skips binaries, and is far faster than `grep`/`find -exec grep`.
> Reserve plain `grep` only for piping non-file streams.
