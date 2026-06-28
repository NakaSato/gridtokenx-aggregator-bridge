# TEST_COVERAGE.md

Test-coverage overview and **gap analysis** for the **Aggregator Bridge** service.

> Companion to [TEST.md](TEST.md) (the per-file test inventory + run commands). This file is the
> bird's-eye view: **what is covered, how strongly, and where the gaps are.** It deliberately does
> **not** add test cases — it documents the current state so the gaps are visible and triageable.
> Last reviewed: 2026-06-28.

---

## Headline numbers

| Metric | Value |
| --- | --- |
| Total inline unit tests | **240** (across 32 files) |
| Run by plain `cargo test` (+ `--workspace`) | **225** |
| `#[ignore]`-gated (need live infra) | **15** (8 files) |
| Source files with **no** inline tests | only `mod.rs`/`lib.rs`/`build.rs`/`telemetry.rs` (re-exports + codegen, no testable logic) |

Every substantial source file (>40 lines of logic) carries inline `#[cfg(test)] mod tests`. The
gaps below are **untested code paths inside otherwise-tested files**, not untested files.

---

## Coverage by layer

| Layer | Crate | Strength | Notes |
| --- | --- | --- | --- |
| Domain / numeric | `aggregator-core` | 🟢 strong | numeric conversions, `DeviceType` mapping, serde roundtrips — pure, fully covered |
| Meter decode | `aggregator-stacks` | 🟢 strong | DLMS/COSEM OBIS mapping, secure v4 binary frame parse (header, CRC, GCM auth, TLV) |
| Aggregation | `aggregator-logic/aggregator.rs` | 🟢 strong | 15-min windowing, accumulate, peek+grace, TOU split, max-demand |
| Mint envelope | `aggregator-persistence/mint.rs` | 🟢 strong | canonical-bytes golden vs blockchain-core, signed-envelope verify, wire shape, disabled gating |
| Crypto verify | `aggregator-persistence/crypto.rs` | 🟢 strong (unit) / 🟡 infra | fail-closed branches covered; real-Redis paths `#[ignore]` |
| Dispatch / standards | `aggregator-logic/standards/*` + `dispatch/engine.rs` | 🟢 strong | IEEE2030.5 + OpenADR VTN/VEN adapters, cooldown, setpoint mapping |
| Router self-heal | `aggregator-logic/router.rs` | 🟢 strong | zone hashing, OBIS promotion, **+ `ReconnectCache` self-heal state machine**; only the live XADD round-trip is e2e-only |
| Auth cache + policy | `aggregator-api/auth.rs` | 🟢 strong | cache store/evict/TTL **+ reject-vs-error policy** (`post_iam_action`/`cache_verdict_for`) covered; only the axum I/O wiring is e2e-only |
| REST ingest | `aggregator-api/handlers.rs` | 🟢 strong | sign-value ladder, secure-mode policy, **+ route status decisions** (`resolve_protocol`/`is_supported_protocol`/`secure_mode_gate`/`sig_failure_status`); only the axum I/O assembly is e2e-only |
| gRPC ingest | `aggregator-api/grpc/service.rs` | 🟢 strong | DLMS key policy matrix, secure-mode overrides, **+ bulk frame unpacking** (`split_bulk_frames`) **+ canonical sign-target** (`grpc_sign_target`); only the assembled service method is e2e-only |
| Server wiring | `src/main.rs` | 🟡 partial | `expand_env`, `parse_api_keys`, `build_mtls_server_config` error paths covered; the `main` async assembly + happy-path mTLS stay e2e |
| External edges | kafka/rabbitmq/vault/postgres/oracle | 🟡 infra | wire-shape covered; live behavior `#[ignore]`-gated |

---

## Gaps (untested code paths)

Ordered by risk. **G1–G4 are the security/correctness-critical ones.**

### G1 — REST ingest route status decisions (handlers.rs) — ✅ CLOSED
The status-mapping logic was extracted from `ingest_private_network` / `ingest_private_network_batch`
into pure helpers the handlers now call: `resolve_protocol` (empty/`auto`→`dlms`),
`is_supported_protocol` (→`400` on garbage), `secure_mode_gate` (→`426 UPGRADE_REQUIRED` for a
non-encrypted frame in secure mode), and `sig_failure_status` (fail-closed `403` invalid / `401`
verify-error, suppressed by the dev hatch). Tests: `resolve_protocol_maps_empty_and_auto_to_dlms`,
`supported_protocols_are_dlms_and_simulator_only`,
`secure_mode_gate_426s_only_unencrypted_in_secure_mode`,
`sig_failure_status_is_fail_closed_403_invalid_401_error`,
`sig_failure_status_hatch_open_accepts_unverified`. The refactor also de-duplicated the
single/batch protocol + secure-mode logic.
- **Still e2e-only**: the full request→decrypt→verify→`disseminate` assembly (needs Redis/router) —
  covered by superproject e2e `20_oracle`, `30_settlement`.

### G2 — gRPC ingest service method (grpc/service.rs) — ✅ CLOSED
Two more decision seams were extracted + tested on top of the existing DLMS key-policy matrix:
- **`split_bulk_frames`** — the `bulk_raw_ingest` wire unpacker (`[len][frame][64B sig]`), now pure.
  Tests cover per-entry frame↔signature pairing, truncated-tail drop (no partial entry), zero-length
  frame, missing-signature drop, empty payload — i.e. the security-relevant framing/bounds matrix
  (a frame can never be paired with the wrong/short signature).
- **`grpc_sign_target`** — the canonical `{meter_id}:{kwh}:{timestamp}` Ed25519 sign-target, now the
  single source used by both `ingest` and `ingest_batch`; tested for format/order so it can't drift
  from the REST canonical form.
- **Still e2e-only**: the assembled `decode_secure_frame` (needs Redis key registry) + dissemination —
  covered by e2e `test_dlms_secure_frame*.py`. The DLMS prod/dev/secure policy itself is fully unit-tested.

### G3 — API-key auth middleware (auth.rs) — ✅ CLOSED
The reject-vs-error decision was extracted from `api_key_auth` into pure helpers
(`post_iam_action`, `cache_verdict_for`) that the middleware now calls. Unit tests
(`iam_reject_denies_and_never_tries_static`, `iam_unavailable_falls_through_to_static`,
`iam_authorized_allows`, `reject_is_cached_but_transient_failure_is_not`) lock the security
invariant: only an `Unavailable` verdict (connection error *or* no client) reaches the static-key
fallback; a definitive IAM reject is final (401, cached negative), and a transient failure is never
cached.
- **Still e2e-only**: the axum header-extract → `next.run` plumbing (covered by superproject e2e).

### G4 — Billing/mint flush loop (src/main.rs) — ✅ CLOSED
The loop's two policies were made pure + tested:
- **Mint decision**: extracted to `billing_sink::plan_mint(bin, mint_enabled) -> MintDecision`
  (`Surplus(kwh)` / `NoSurplus` / `Disabled`), which the flush loop now calls. Tests:
  `plan_mint_surplus_yields_net_kwh`, `plan_mint_net_import_yields_no_surplus`,
  `plan_mint_net_zero_is_no_surplus_not_a_zero_mint`, `plan_mint_disabled_skips_regardless_of_surplus`.
- **Eviction (map-bounding)**: `aggregator::flush_drain_evicts_completed_bins_but_keeps_open_ones`
  mirrors the loop's `peek_completed_bins` → `remove_bins` drain — every completed bin is evicted,
  the open window survives and keeps accumulating.
- **Idempotency** `mint:{serial}:{window_start_ms}` already covered (`mint.rs:373`).
- **Still e2e-only**: the spawned fire-and-forget mint task itself (wallet resolve → bridge call) and
  grace-timer cadence — runtime glue, covered by e2e `test_surplus_mint.py`.

### G5 — Server startup & shutdown (src/main.rs) — ◑ PARTIALLY CLOSED
The testable config seams are now covered:
- **`parse_api_keys`** — comma-split static-fallback list; tests lock the empty-entry drop (a blank
  `GRIDTOKENX_API_KEYS` must yield *no* keys, so a blank `X-API-KEY` can't authorize), plus the
  no-trim behavior.
- **`build_mtls_server_config`** — error branches (missing cert, missing key, no parseable private
  key) all `Err` instead of panicking; a misconfigured mTLS gateway fails loud. Happy path needs a
  real CA chain (e2e).
- **Still e2e-only**: the `main` async assembly — concurrent HTTP+gRPC bring-up, degraded-mode
  fallbacks (Redis/Kafka/RabbitMQ/InfluxDB/IAM → disabled on connect fail), background-task spawn,
  and `CancellationToken` SIGINT/SIGTERM graceful shutdown. No pure seam to extract — genuinely
  integration territory.

### G6 — Stream-consume / batch worker loops — ◑ PARTIALLY CLOSED
The pure cores of the two flush paths are now extracted + tested:
- **InfluxDB flush** — `drain_to_data_points` (convert each point, skip malformed, **always drain** so
  a bad batch can't wedge the queue). Tests: keeps-valid/skips-malformed/empties, all-malformed still
  drains, plus a guard that a fields-less point really is unbuildable.
- **Batch XACK grouping** — `group_entry_ids_by_stream` (every entry id mapped under its own stream,
  none dropped — a mis-group leaves messages unacked/redelivered). Tests: multi-stream grouping +
  empty.
- **Still e2e-only**: the `tokio::select!` loop bodies themselves — `run_writer` (size/interval flush
  cadence), `BatchWorker::run` (accumulate-threshold + XACK round-trip), and the `zone_ingester` XREAD
  consume loop. These are infra-coupled runtime glue (Redis/InfluxDB); the zone-ingester *decode*
  branches are already fully covered.

### G7 — Router self-heal — ✅ CLOSED
The self-heal state machine behind `disseminate`'s XADD retry was extracted into a generic
`ReconnectCache<T>` that `Router` now uses for its connection. Tests
(`reconnect_cache_serves_seeded_handle_without_building`,
`reconnect_cache_rebuilds_exactly_once_after_invalidate_then_caches`,
`reconnect_cache_propagates_build_error_and_stays_empty_for_retry`) lock the core invariants without
Redis: a cached handle is reused (no spurious rebuild), `invalidate` forces exactly one rebuild which
is then cached, and a failed rebuild propagates without poisoning the cache (next call retries).
- **Live coverage**: an `#[ignore]` real-Redis test (`disseminate_and_self_heal_against_real_redis`)
  now exercises live XADD then `invalidate`→rebuild→XADD end-to-end (run with
  `cargo test -- --ignored`, needs Redis). The full `XADD → transport-error → retry-once` *inline*
  branch (mid-flight Redis drop) still only triggers under a real restart.

### G8 — Infra live behavior is opt-in (the 15 `#[ignore]` tests)
crypto (real Redis ×4), kafka (×2), rabbitmq self-heal (×2), vault (×1), meter_registry Postgres
tier (×1), platform/oracle client (×3), dispatch grpc_client (×1), router live XADD + self-heal (×1).
**Skipped by default** — a plain `cargo test` / CI run without `just orb-up` proves zero live-infra
behavior. Run with `cargo test -- --ignored`. (This session: infra was **down**, so these were not
executed — the router `#[ignore]` test compiles but is unrun pending a live Redis.)

---

## What backstops the gaps today

Most G1–G4 paths are exercised by the **superproject pytest e2e** (`tests/e2e/`, phases `20_oracle`,
`30_settlement`, `90_golden_path`) and the shell e2e in `scripts/` — but those need full infra
(`just orb-up`) and a Solana validator, so they do **not** run in this submodule's `cargo test`. Net:
**unit tests prove the pure logic; the wired flows are proven only by infra-dependent e2e.**

---

## Triage summary (no tests added here — backlog only)

| ID | Gap | Priority | Status |
| --- | --- | --- | --- |
| G3 | auth reject-vs-error fallback | high | ✅ closed — `post_iam_action`/`cache_verdict_for` + 4 tests |
| G4 | flush-loop evict + mint decision | high | ✅ closed — `plan_mint` + drain-eviction test (5 tests) |
| G1 | REST route status codes | med | ✅ closed — 4 status helpers + 5 tests |
| G2 | gRPC assembled ingest | med | ✅ closed — `split_bulk_frames` + `grpc_sign_target` (6 tests) |
| G7 | router XADD self-heal | med | ✅ closed — `ReconnectCache` state machine (3 tests) |
| G6 | consume/flush loops | low | ◑ cores closed — `drain_to_data_points` + `group_entry_ids_by_stream` (5 tests); loop bodies stay e2e |
| G5 | startup/shutdown | low | ◑ cores closed — `parse_api_keys` + `build_mtls` error paths (6 tests); `main` assembly stays e2e |
| G8 | infra live behavior | n/a | already covered, just opt-in (`--ignored`) |
