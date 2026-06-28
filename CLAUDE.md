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

> **Surplus minting lives here (Chain Bridge over NATS), chain-light.** When a 15-min billing window
> closes with net surplus generation, the
> settlement sink mints it to the meter owner via **Chain Bridge over NATS** (`chain.tx.mint`). The
> service carries **no Solana / blockchain-core dependency** — it sends intent only and mirrors the wire
> types locally (`crates/aggregator-persistence/src/infra/mint.rs`). Disabled by default; gated on
> `MINT_VIA_CHAIN_BRIDGE` + `NATS_URL`. Verification, aggregation, and telemetry dissemination
> (Redis Streams + InfluxDB) are unchanged.

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

See [TEST.md](TEST.md) for the full test inventory — unit tests (per crate/file) and the superproject pytest e2e suite.

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
| `aggregator-persistence` | Edges: Redis crypto verifier, Kafka, RabbitMQ, meter registry (3-tier owner resolver: local cache → Redis → Postgres source of truth, `infra/meter_registry.rs`), circular-buffer/sync storage, independent InfluxDB v2 history sink (`infra/influxdb.rs`). |
| `aggregator-logic` | Aggregator (15-min billing bins w/ TOU split + max-demand), Router (dissemination), `billing_sink` (bins → InfluxDB `billing`), dispatch engine, IEEE 2030.5 / OpenADR standards. No blockchain deps. |
| `aggregator-api` | HTTP handlers, gRPC service, auth, ingesters (zone, batcher), `AppState`. Depends on `gridtokenx-telemetry` (sibling submodule). |

`src/main.rs` is a wiring-only entrypoint: it re-imports everything through `aggregator_api::{...}` and runs the HTTP + gRPC servers. New business logic goes in the crate that matches the dependency rule, **not** in `main.rs`.

## Runtime shape (from src/main.rs)

- Two servers run concurrently: **HTTP IoT gateway** on `IOT_GATEWAY_PORT` (default `4010`) and **gRPC ingestion** on `GRPC_PORT` (default **5030**, the canonical mesh port — `50051` in `.env.example` is the simulator override).
- HTTP routes: `/health`, `/v1/private-network/ingest[/batch]`, `/v1/ingest/telemetry[/batch]`.
- **Degraded-mode by design**: Redis (3s timeout), Kafka, RabbitMQ, InfluxDB, and IAM gRPC all fall back to disabled/None on connect failure with a `warn!` — the process still starts. Don't "fix" these by making them fatal.
- Background tasks: zone ingester, Kafka dispatch listener, gRPC server, billing-sink flush loop (only when InfluxDB enabled) — all gated on a shared `CancellationToken` driven by SIGINT/SIGTERM.
- Env interpolation: values support `${VAR}` expansion via `expand_env`.

## Security-critical invariants (don't regress)

- **Fail-closed, loud verification.** Ed25519 signatures checked against device pubkeys in Redis at `gridtokenx:devices:{meter_id}:pubkey`. Redis-unreachable must return `Err`, **never** a silent `Ok(false)` (`crates/aggregator-persistence/src/infra/crypto.rs`). The pubkey *lookup* is cached (parsed `VerifyingKey`, positive TTL `PUBKEY_CACHE_TTL_SECS` default 60, negative `PUBKEY_NEG_CACHE_TTL_SECS` default 10) to stop per-reading Redis floods — but **the signature is still verified on every call**; only the (static) key fetch is cached. SECURITY TRADE-OFF: the positive TTL **is the revocation latency** (a key removed/rotated in Redis stays accepted ≤ TTL) — keep it short. Fail-closed is preserved: a cache *miss* with Redis down still `Err`s, absent keys reject, and malformed keys are never cached. Same caching pattern applies to the device `enckey` (`DEVICE_ENCKEY_CACHE_TTL_SECS`, default 300 — rotation goes through the *versioned* GUEK path, so a longer TTL is safe there).
- **Encrypted DLMS is wired (no longer a gap).** The secure v4 binary frame is AES-256-GCM; the per-device key lives at `gridtokenx:devices:{meter_id}:enckey` (64-char hex, 32 bytes) and is fetched by `DeviceKeyRegistry` (self-healing, mirrors the verifier). gRPC ingest resolves + decrypts in `decode_secure_frame`; the branch policy is the pure `apply_dlms_key_policy` (`crates/aggregator-api/src/grpc/service.rs`). Under `ENVIRONMENT=production`, a missing `enckey` ⇒ frame **skipped** (fail-closed, never silent plaintext). The dev plaintext fallback is gated behind `ALLOW_PLAINTEXT_DLMS=true` and logged loud — don't decode plaintext by default.
- **Self-healing connections.** The `SignatureVerifier` owns a Redis *URL* (not a one-shot connection) and rebuilds + retries once on transport error (`get_with_retry`). The `Router::disseminate` publisher does the same for `XADD`. This is why a Redis restart no longer freezes the bridge — preserve it.
- **Production enforcement.** `ENVIRONMENT=production` makes signature verification strict.
- **Secure mode neutralizes every ingest bypass.** `AGGREGATOR_REQUIRE_SECURE=true` (`secure_mode_enabled`, `crates/aggregator-api/src/handlers.rs`) hard-overrides all dev escape hatches, fail-closed regardless of the dev env vars: the REST unverified-telemetry hatch is ignored (`signature_enforcement_disabled` ⇒ `false`), the unsigned `simulator` bypass is refused on both REST single + batch (`simulator_bypass_allowed` ⇒ `false`), and the REST meter path requires an authenticated `dlms-enc` frame (non-encrypted ⇒ `426 UPGRADE_REQUIRED`, no plaintext downgrade). On the gRPC side it forces `SKIP_SIG_VERIFY` off (bulk path always verifies, via `bulk_skip_verify_allowed`) and `ALLOW_PLAINTEXT_DLMS` off (unkeyed frame skipped, via `plaintext_dlms_allowed`, `crates/aggregator-api/src/grpc/service.rs`). Default off so dev/e2e keep their bypasses — don't make it default-on.
- **DLMS REST canonical sign-value.** The REST sign-target is `{device_id}:{value}:{timestamp_ms}` where `value` is protocol-native (`canonical_sign_value`, `crates/aggregator-api/src/handlers.rs`). For DLMS it resolves `kwh` → `energy_consumed` → `energy_generated` → OBIS active import `1.1.1.8.0.255` (Wh/1000) → OBIS export `1.1.2.8.0.255` (Wh/1000). A real OBIS-only meter must sign that derived kWh — don't drop the OBIS fallback or pure-OBIS payloads sign `:0:` and fail closed. (Binary gRPC path signs raw frame bytes; not affected.)
- **OBIS register decode → metadata, but only the zone stream keeps it.** `DlmsStack.map_payload` (`crates/aggregator-stacks/src/stacks/dlms.rs`) decodes the residential register set into `DeviceReading.metadata`: active import/export totals drive `consumed/generated_kwh`; reactive, per-phase V/I, frequency, PF, sum active power (`1.1.16.7.0.255`, signed kW — the sim's net; `1.1.1.7.0.255` stays positive-only A+ for real meters), max demand (`1.1.1.6.0.255`, kW), DR status (`0.0.96.10.0.255`), active tariff (`0.0.96.14.0.255`) and TOU rate registers (`1.1.1.8.1/2`, `1.1.2.8.1/2`) land as metadata only — **not** double-counted into the energy. That metadata survives **only** on the zone Redis Streams (`router.disseminate` serializes the whole `DeviceReading`); `reading_to_point`→InfluxDB keeps just `generated/consumed/net_kwh`, the Kafka `MeterReadingEvent` cherry-picks `voltage_v`/`frequency_hz`/`power_factor`/`signature`, and settlement uses energy + `frequency` only. Unknown OBIS keys pass through the `_` fallback arm verbatim.
- Auth falls back to static `GRIDTOKENX_API_KEYS` (comma-separated) **only when the IAM gRPC client errors** (connection failure) — a definitive IAM *reject* returns 401 without trying static keys. The seeded IAM key (`api_keys` migration) is a placeholder (empty-string hash), so no usable dev key exists out of the box; register one via `ApiKeyService` (hash = `SHA-256(key + API_KEY_SECRET)`).
- **API-key auth is cached to bound IAM load** (`crates/aggregator-api/src/auth.rs`). Sustained ingest would otherwise call IAM `VerifyApiKey` per request (Redis event + DB write each), saturating IAM. Positive verdicts are trusted for `API_KEY_POS_CACHE_TTL_SECS` (default 60); **definitive IAM rejects** are cached for the shorter `API_KEY_NEG_CACHE_TTL_SECS` (default 10) so a replayed bad/rotated key is also bounded to one IAM round-trip per TTL. IAM *connection errors* are **never** cached — they must still reach the static-key fallback. Observability: `aggregator_settlement_path{path}` gauge + `aggregator_mint_total{outcome,reason}` counter on `/metrics` (the latter makes the silent unregistered-meter skip visible as `skipped/no_wallet`); see [ARCHITECTURE.md](ARCHITECTURE.md) → Observability.

## Config

Copy `.env.example` → `.env`. Key vars: `REDIS_URL`, `DATABASE_URL`, `IOT_GATEWAY_PORT`, `GRPC_PORT`,
`GRIDTOKENX_API_KEYS`, `API_KEY_POS_CACHE_TTL_SECS` (default 60), `API_KEY_NEG_CACHE_TTL_SECS`
(default 10), `IAM_SERVICE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `RABBITMQ_URL`, `IOT_NUM_ZONES`
(default 10), `ENVIRONMENT` (`production` ⇒ strict sig + DLMS decryption), `ALLOW_PLAINTEXT_DLMS`
(dev-only; allow plaintext v4 frames when a device has no `enckey`), `AGGREGATOR_REQUIRE_SECURE`
(`true` ⇒ locked-down: neutralizes every ingest bypass — see Security-critical invariants).

Meter-owner registry (`DATABASE_URL`): the **durable source of truth** for `meter_serial →
(user_id, owner wallet)` is the shared gridtokenx Postgres `meters` JOIN `users`, written by the
**meter-service** registration API (`POST /api/v1/meters`). `MeterRegistry`
(`crates/aggregator-persistence/src/infra/meter_registry.rs`) resolves in three tiers — local cache
→ Redis (`gridtokenx:meters:{serial}:user_id`/`:wallet`) → **Postgres** — and **backfills Redis +
local cache** on a Postgres hit, so Redis is a self-populating hot cache and a flush/restart never
loses ownership. Read-only; the bridge never writes `meters`. Degraded-safe: `DATABASE_URL`
unreachable at boot ⇒ `warn!` + DB tier disabled (Redis-only; unseeded meters stay unattributed).
When **neither** Redis nor Postgres is configured, `resolve_user_id` keeps the legacy nil-user
fallback for pure-local dev. Compose already sets `DATABASE_URL` (→ `pgdog:6432/gridtokenx`).

InfluxDB (independent realtime history): `INFLUXDB_URL` enables an InfluxDB v2 sink dedicated to this
service alone — point it at this service's **own** instance (the superproject's `aggregator-influxdb`
compose service), never a shared one. Optional: `INFLUXDB_ORG` (default `gridtokenx`), `INFLUXDB_BUCKET`
(default `aggregator_telemetry`), `INFLUXDB_TOKEN`. Unset `INFLUXDB_URL` ⇒ disabled; unreachable at boot ⇒
`warn!` + disabled. Writes are async fire-and-forget (batched), so InfluxDB latency/outage never blocks the
realtime Redis dissemination path. Measurements: `energy` / `ev_session` / `battery` (realtime per-reading,
from `Router::disseminate`) and `billing` (rolled-up bins, see below); tags include `device_id`,
`device_type`, `serial_number`, `zone_code`.

Settlement sink (durable home for completed bins + surplus mint): the binary spawns a flush loop
(`src/main.rs`) that periodically `peek_completed_bins(grace)` and for each completed bin: (1) converts via
`bin_to_billing_point` (`crates/aggregator-logic/src/billing_sink.rs`) to the InfluxDB `billing` measurement
(TOU peak/off-peak split + window `max_demand_kw`, tagged `user_id`, timestamped at `end_time`); (2) if
`BillingBin::net_surplus_kwh()` is `Some` (net generation > consumption), **mints** that surplus to the meter
owner via Chain Bridge — fire-and-forget in a spawned task so a slow bridge never stalls the sweep; (3)
**evicts** the bin — which bounds the otherwise-unbounded `active_bins` map. The loop now runs whenever
InfluxDB **or** minting is enabled (previously InfluxDB-only), so eviction no longer depends on InfluxDB.
Mint idempotency: `mint:{serial}:{window_start_ms}` (15-min window = the billing window) lets the bridge
dedup any replay (e.g. a crash before eviction); the on-chain `(meter_id, window_start_ms)` PDA is the
backstop. Recipient wallet is resolved from the meter registry (`gridtokenx:meters:{serial}:wallet`); a
missing wallet skips the mint (logged, bin still evicts). No on-chain confirmer (fire-and-forget).
Tunables: `BILLING_FLUSH_INTERVAL_SECS` (default 30), `BILLING_FLUSH_GRACE_SECS` (default 120). Mint config:
`MINT_VIA_CHAIN_BRIDGE`, `NATS_URL`, `CHAIN_BRIDGE_SERVICE_IDENTITY`. `BillingBin` carries `#[serde(default)]`
TOU/demand fields so bins persisted before these existed still deserialize on crash recovery.

> SECURITY: `NatsMintGateway` **signs** the mint envelope with this service's mTLS client key
> (P-256/ECDSA over the canonical bytes) and attaches an `EnvelopeAuth` (cert PEM + signature), so the
> bridge binds the self-asserted `service_identity` to a CA-issued cert and rejects spoofed identities
> under `CHAIN_BRIDGE_REQUIRE_SIGNED_NATS`. The signing scheme is mirrored locally
> (`crates/aggregator-persistence/src/infra/mint.rs`: `EnvelopeSigner` + `canonical_mint_bytes`,
> p256/base64 only) so the service stays chain-light — that layout MUST stay byte-identical to
> `gridtokenx-blockchain-core`'s `rpc::envelope_auth` or the bridge rejects every signature. When the
> client cert/key is absent (insecure dev), the signer is `None` and the envelope ships unsigned
> (accepted only while the bridge runs signing in log-only mode).

OpenADR 3 dispatch (OpenLEADR): setting `OPENLEADR_VTN_URL` enables the `openleadr` dispatch adapter
(`crates/aggregator-logic/src/standards/openleadr.rs`), preferred over `ieee` in the dispatch engine.
**Adapter selection / fan-out:** `DISPATCH_ADAPTERS` (csv of `grpc`/`ieee`/`openleadr`) dispatches each
frequency excursion to **every** listed adapter at once (e.g. a downstream in-mesh VTN **and** a utility
VTN) — partial-failure isolated (one adapter erroring never stops the others; `Err` only when all fail),
cooldown tracked per `(action, adapter)`. The legacy single-value `DISPATCH_ADAPTER` is still honored as a
one-element list when `DISPATCH_ADAPTERS` is unset (`select_adapters`, `crates/aggregator-logic/src/dispatch/engine.rs`).
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
