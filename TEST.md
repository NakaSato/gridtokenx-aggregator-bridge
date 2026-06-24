# TEST.md

Test inventory for the **Aggregator Bridge** service.

> This submodule ships **unit tests only** — inline `#[cfg(test)] mod tests`, no `tests/` dirs,
> **no e2e or integration tests in-repo**. The e2e/integration suite is **not** part of this submodule;
> it lives in the **superproject** (`gridtokenx-coresystem/tests/e2e/`, pytest, run via `run.sh` / `just e2e`)
> and is listed below for reference only.

---

## Unit tests (this service — `cargo test`)

Inline `#[cfg(test)] mod tests`, no `tests/` dirs. Run from this submodule root.

| Crate | File | Covers |
| --- | --- | --- |
| aggregator-stacks | `src/binary_decoder.rs` | secure v4 binary frame decode |
| aggregator-stacks | `src/stacks/dlms.rs` | DLMS/COSEM OBIS register → metadata mapping |
| aggregator-logic | `src/aggregator.rs` | 15-min billing bins, window/accumulate/peek, TOU split, max-demand |
| aggregator-logic | `src/billing_sink.rs` | bin → InfluxDB `billing` point conversion |
| aggregator-logic | `src/router.rs` | dissemination (Redis Streams + InfluxDB) |
| aggregator-logic | `src/grid_status.rs` | grid status event parse |
| aggregator-logic | `src/dispatch/engine.rs` | dispatch engine |
| aggregator-logic | `src/standards/openleadr.rs` | OpenADR 3 (VTN-side) dispatch adapter |
| aggregator-logic | `src/standards/openleadr_ven.rs` | OpenADR VEN-side polling listener |
| aggregator-persistence | `src/infra/crypto.rs` | Ed25519 verify (valid / wrong-key / bad-len), fail-closed |
| aggregator-persistence | `src/infra/meter_registry.rs` | meter/device key + wallet registry |
| aggregator-persistence | `src/infra/mint.rs` | surplus mint envelope (NATS `chain.tx.mint`) |
| aggregator-api | `src/handlers.rs` | REST sig fallback ladder, canonical sign-value |
| aggregator-api | `src/grpc/service.rs` | gRPC ingest, `apply_dlms_key_policy` |
| aggregator-api | `src/ingester/zone_ingester.rs` | zone-partitioned ingest |

```bash
cargo test                       # whole workspace
cargo test -p aggregator-logic   # single crate
cargo test test_name -- --nocapture
```

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
