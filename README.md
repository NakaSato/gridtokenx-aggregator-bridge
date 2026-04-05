# GridTokenX Oracle Bridge & VPP Operation Service

The **GridTokenX Oracle Bridge** is a **Private Cloud-Native** Convergence Layer designed to orchestrate VPP operations and provide cryptographic integrity for grid-scale energy assets. It serves as the high-throughput ingestion entry point for the GridTokenX VPP, bridging edge gateways to the optimization platform and blockchain settlement.

## Features

- **VPP Ingestion Service** - High-concurrency ingestion for normalized smart meter, EV, and BESS data streams.
- **Real-time Orchestration** - Sub-100ms telemetry routing to VPP forecasting and MILP optimization engines.
- **ZK-Aggregation (Path B)** - Cloud-side recursive ZK-Rollup (Plonky2) for PDPA-compliant aggregate proving.
- **Service Mesh Ready** - Built for high-availability deployment with gRPC and mTLS identity support.
- **Performance Driven** - Optimized Rust/Tokio implementation with Redis/Kafka streaming.

---

## 🚀 Architectural Role

The Oracle Bridge is the **high-performance ingest layer**. It is decoupled from the hardware (Oracle of Edge Meter) and focuses on processing the inbound telemetry and attestation flows.

1.  **Path A (Operational)**: Real-time telemetry synchronized with the VPP Platform for sub-second dispatch.
2.  **Path B (Settlement)**: Batched attestation processing and ZK-rollup generation for HyperEVM.

---

## 📂 Documentation Index

- [implement.md](implement.md) - **Source of Truth**: Full technical system design (GridTokenX VPP).
- [core.md](docs/core.md) - **Architecture Matrix**: SVGs and component matrices.
- [DATA-FLOW.md](docs/DATA-FLOW.md) - **The Dual Path**: Operational vs. Settlement flows.
- [ORACLE-BRIDGE.md](docs/ORACLE-BRIDGE.md) - **Path B Deep Dive**: Cryptographic Trust & ZK-Rollup Settlement.
- [TELEMETRY.md](docs/TELEMETRY.md) - **Path A Deep Dive**: Real-time VPP Operations & Orchestration.

---

## 🛠 Tech Stack

- **Language**: Rust (Tokio/Axum)
- **Streaming**: Apache Kafka / Redis Streams
- **Cryptography**: Plonky2 (ZK-Rollup), Ed25519 (Verification)
- **API**: gRPC (Protobuf), REST (HyperEVM Relayer)
- **Database**: TimescaleDB / SQLite (Circular Buffer)
