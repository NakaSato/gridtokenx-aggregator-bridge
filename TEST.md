# TEST.md

Test inventory for the **Aggregator Bridge** service.

> This submodule ships **inline unit tests** (`#[cfg(test)] mod tests`, no `tests/` dirs) plus a set of
> `#[ignore]`-gated **integration tests** that hit live infra (Redis/Vault/Postgres/RabbitMQ/Kafka/gRPC) —
> run on demand with `cargo test -- --ignored`. The cross-service **e2e suite** is **not** in this submodule;
> it lives in the **superproject** (`gridtokenx-coresystem/tests/e2e/`, pytest, run via `run.sh` / `just e2e`)
> and is listed below for reference only.

---

## Unit tests (this service — `cargo test`)

Inline `#[cfg(test)] mod tests`, no `tests/` dirs. Run from this submodule root.

> Some files carry `#[ignore]` tests that need live infra (Redis/Vault/RabbitMQ) — skipped by
> default, run with `cargo test -- --ignored` once the broker is up (e.g. `just orb-up`).
> Affected: `crypto.rs` (real Redis), `vault.rs` (real Vault), `meter_registry.rs` (real Postgres),
> `rabbitmq.rs` (real broker, self-heal), `kafka.rs` (real broker, produce→consume; default `localhost:29001`),
> `dispatch/grpc_client.rs` + `platform/client.rs` (live gRPC server, default `localhost:5030`),
> `router.rs` (real Redis: live XADD + `invalidate`→rebuild self-heal, default `redis://127.0.0.1:6379`).

| Crate | File | Covers |
| --- | --- | --- |
| aggregator-core | `src/numeric.rs` | `f64_to_decimal`/`to_positive_decimal`: finite convert, NaN/inf reject, negative reject, label in error |
| aggregator-core | `src/models.rs` | `DeviceType::target_stream` map, snake_case serde, tagged `DeviceMetrics` roundtrip |
| aggregator-persistence | `src/storage/circular_buffer.rs` | SQLite ring buffer: push/get_unsynced order+limit, mark_as_synced, JSON roundtrip (in-mem `:memory:`) |
| aggregator-persistence | `src/infra/rabbitmq.rs` | validation-job payload wire shape; `#[ignore]` real-broker connect + channel self-heal |
| aggregator-persistence | `src/infra/kafka.rs` | `MeterReadingEvent`/`GridStatusEvent` serde roundtrip + malformed/empty reject; `#[ignore]` real-broker produce→consume |
| aggregator-persistence | `src/infra/influxdb.rs` | `TelemetryPoint`→`DataPoint` map (zone some/none), disabled-when-no-URL, drop-on-closed-queue, `drain_to_data_points` (skip-malformed, always-drain) |
| aggregator-persistence | `src/storage/sync_manager.rs` | replay request shaping: `replay_url` endpoint path, `replay_body` contract shape (meter/ts RFC3339/payload) |
| aggregator-api | `src/ingester/batcher.rs` | `BatchHandle` wire contract: add/flush/shutdown → `BatchMessage`, channel-closed error; `group_entry_ids_by_stream` XACK grouping |
| aggregator-api | `src/state.rs` | `Metrics` atomics: zeroed-new, authorized/failed branches, latency accumulate + last |
| aggregator-persistence | `src/infra/platform/client.rs` | `#[ignore]` real OracleService connect + batch submit |
| aggregator-logic | `src/metrics/mod.rs` | recorder smoke (no-panic, all label sets, success+failure branches, HttpMetricsTimer lifecycle) |
| aggregator-logic | `src/dispatch/grpc_client.rs` | `DispatchClient::new` URI parse (valid/malformed); `#[ignore]` real-server dispatch |
| (binary) | `src/main.rs` | `expand_env` `${VAR}` interpolation; `parse_api_keys` (split + empty-drop + no-trim); `build_mtls_server_config` error paths (missing cert/key, no private key) |
| aggregator-stacks | `src/binary_decoder.rs` | secure v4 binary frame decode |
| aggregator-stacks | `src/stacks/dlms.rs` | DLMS/COSEM OBIS register → metadata mapping |
| aggregator-logic | `src/aggregator.rs` | 15-min billing bins, window/accumulate/peek, TOU split, max-demand, flush-drain eviction (peek→remove bounds `active_bins`) |
| aggregator-logic | `src/billing_sink.rs` | bin → InfluxDB `billing` point conversion; `plan_mint` flush-loop mint decision (surplus/no-surplus/disabled/net-zero) |
| aggregator-logic | `src/router.rs` | dissemination (Redis Streams + InfluxDB); zone hashing, OBIS promotion, `ReconnectCache` self-heal state machine (reuse/invalidate-rebuild-once/error-no-poison); `#[ignore]` real-Redis live XADD + self-heal rebuild |
| aggregator-logic | `src/grid_status.rs` | grid status event parse |
| aggregator-logic | `src/dispatch/engine.rs` | dispatch engine |
| aggregator-logic | `src/standards/ieee2030_5.rs` | DERControl mapping (FLEX_UP→ReducePower), `is_simulation`, stub dispatch Ok |
| aggregator-logic | `src/standards/openleadr.rs` | OpenADR 3 (VTN-side) dispatch adapter |
| aggregator-logic | `src/standards/openleadr_ven.rs` | OpenADR VEN-side polling listener |
| aggregator-persistence | `src/infra/crypto.rs` | Ed25519 verify (valid / wrong-key / bad-len), fail-closed |
| aggregator-persistence | `src/infra/meter_registry.rs` | meter/device key + wallet registry |
| aggregator-persistence | `src/infra/mint.rs` | surplus mint envelope (NATS `chain.tx.mint`) |
| aggregator-api | `src/auth.rs` | API-key cache (store/evict/TTL) + reject-vs-error policy (`post_iam_action`: IAM reject→deny, error/no-client→static fallback; transient never cached) |
| aggregator-api | `src/handlers.rs` | REST sig fallback ladder, canonical sign-value, route status decisions (`resolve_protocol`/`is_supported_protocol`/`secure_mode_gate`→426/`sig_failure_status`→403/401) |
| aggregator-api | `src/grpc/service.rs` | gRPC ingest, `apply_dlms_key_policy`, bulk frame unpacking (`split_bulk_frames`: pairing/bounds/truncation), canonical `grpc_sign_target` |
| aggregator-api | `src/ingester/zone_ingester.rs` | zone-partitioned ingest |

```bash
cargo test --workspace           # all crates (root is a package — bare `cargo test` runs the binary only)
cargo test -p aggregator-logic   # single crate
cargo test test_name -- --nocapture
cargo test -- --ignored          # integration only (needs live infra: just orb-up)
cargo test -- --include-ignored  # unit + integration
```

**Intentionally untested** (no unit-testable logic): `aggregator-protocol/build.rs` (prost codegen),
`aggregator-stacks/src/stacks/mod.rs` + `aggregator-logic/src/dispatch/mod.rs` are trait definitions
(only the `DispatchAdapter::is_simulation` default is covered). All other source files carry tests.

---

## E2E tests (superproject — pytest, phase-ordered)

Location: `gridtokenx-coresystem/tests/e2e/`. Needs infra up (`just orb-up`).

| Phase | File | Covers |
| --- | --- | --- |
| 00_harness | *(fixtures only)* | conftest, lib, proto setup |
| 10_iam | `test_iam_grpc.py` | IAM gRPC auth |
| 20_oracle | `test_dlms_secure_frame.py` | secure v4 DLMS decrypt |
| | `test_dlms_secure_frame_failclosed.py` | missing enckey → fail-closed |
| | `test_simulator_obis_contract.py` | OBIS register contract |
| | `test_telemetry.py` | telemetry ingest |
| 30_settlement | `test_surplus_mint.py` | net-surplus → Chain Bridge mint |
| | `test_unregistered_meter_rejected.py` | unknown meter reject |
| 40_trading | `test_trading.py` | matching / settlement |
| 50_chain_bridge | `test_chain_bridge.py` | gRPC reads |
| | `test_nats_tx.py` | NATS tx submit |
| 60_noti | `test_noti.py` | notification pipeline |
| 70_anchor | *(empty)* | — |
| 80_gateways | *(empty)* | — |
| 90_golden_path | `test_golden_path.py` | full cross-service flow |

**Aggregator-relevant phases**: `20_oracle`, `30_settlement`, `90_golden_path`.

```bash
cd ../                           # superproject root
bash tests/e2e/run.sh            # full suite (or: just e2e)
pytest tests/e2e/20_oracle/      # single phase
```

### Shell-script e2e (separate, `scripts/`)

| Script | Covers |
| --- | --- |
| `scripts/openleadr-e2e.sh` | OpenADR / OpenLEADR VTN↔VEN flow |
| `scripts/production-e2e.sh` | production-mode flow |
| `scripts/test-registration-e2e.sh` | IAM registration (register→verify→PDA) |
| `scripts/test-prosumer1.sh` | prosumer telemetry flow |
