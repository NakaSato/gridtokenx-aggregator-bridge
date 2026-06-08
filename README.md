# GridTokenX Aggregator Bridge & VPP Operation Service

The **GridTokenX Aggregator Bridge** is a **Private Cloud-Native** Convergence Layer designed to orchestrate VPP operations and provide cryptographic integrity for grid-scale energy assets. It serves as the high-throughput ingestion entry point for the GridTokenX VPP, bridging edge gateways to the optimization platform and blockchain settlement.

## Features

- **VPP Ingestion Service** - High-concurrency ingestion for normalized smart meter, EV, and BESS data streams.
- **Secure Telemetry Ingestion (Path A Security)** - Cryptographically signed telemetry verification via Ed25519 (Base58).
- **Real-time Orchestration** - Sub-100ms telemetry routing to VPP forecasting and MILP optimization engines.
- **ZK-Aggregation (Path B)** - Cloud-side recursive ZK-Rollup (Plonky2) for PDPA-compliant aggregate proving.
- **Production Mode Enforcement** - Strict signature verification when `ENVIRONMENT=production`.
- **Performance Driven** - Optimized Rust/Tokio implementation with Redis/Kafka streaming.

---

## 🚀 Architectural Role

The Aggregator Bridge is the **high-performance ingest layer**. It is decoupled from the hardware (Oracle of Edge Meter) and focuses on processing the inbound telemetry and attestation flows.

1.  **Path A (Operational)**: Real-time, cryptographically signed telemetry synchronized with the VPP Platform.
2.  **Path B (Settlement)**: Batched attestation processing and ZK-rollup generation for HyperEVM.

---

## ⚡ Quick Start: Security Verification

The Aggregator Bridge requires all telemetry to be signed by registered edge devices. You can verify the security pipeline using the automated E2E suite:

```bash
# Verify the secure telemetry link (gRPC + REST)
./scripts/test-e2e.sh
```

> [!NOTE]
> Device public keys must be registered in Redis under `gridtokenx:devices:{meter_id}:pubkey`. Use `./scripts/register-edge-key.sh` for manual registration.

---

## 📂 Documentation Index

- [ARCHITECTURE.md](ARCHITECTURE.md) - **Source of Truth**: Full technical system design — components, telemetry verification, aggregation, and settlement dissemination.

---

## 🛠 Tech Stack

- **Language**: Rust (Tokio/Axum)
- **Streaming**: Apache Kafka / Redis Streams
- **Cryptography**: Plonky2 (ZK-Rollup), Ed25519 (Verification)
- **API**: gRPC (Protobuf), REST (HyperEVM Relayer)
- **Database**: TimescaleDB / SQLite (Circular Buffer)
