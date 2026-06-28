# GridTokenX Aggregator Bridge & VPP Operation Service

The **GridTokenX Aggregator Bridge** is a **Private Cloud-Native** Convergence Layer designed to orchestrate VPP operations and provide cryptographic integrity for grid-scale energy assets. It is the **central connection point for every device node** in the smart grid — the high-throughput ingestion entry point for the GridTokenX VPP, bridging edge gateways to the optimization platform.

## Features

- **VPP Ingestion Service** - High-concurrency ingestion for normalized smart meter, EV, and BESS data streams.
- **Secure Telemetry Ingestion** - Cryptographically signed telemetry verification via Ed25519 (Base58).
- **Real-time Orchestration** - Sub-100ms telemetry routing to VPP forecasting and MILP optimization engines.
- **Flex Dispatch** - Frequency-driven demand response via IEEE 2030.5 / OpenADR 3 (OpenLEADR).
- **Production Mode Enforcement** - Strict signature verification when `ENVIRONMENT=production`.
- **Performance Driven** - Optimized Rust/Tokio implementation with Redis/Kafka streaming.

> **Chain-light surplus minting.** This service verifies, aggregates, disseminates telemetry, and drives dispatch. On a 15-min surplus window it sends a mint intent to Chain Bridge over NATS (`chain.tx.mint`) — no Solana / blockchain-core dependency. Disabled by default (`MINT_VIA_CHAIN_BRIDGE` + `NATS_URL`).

---

## 🚀 Architectural Role

The Aggregator Bridge is the **high-performance ingest layer**. It is decoupled from the hardware (Oracle of Edge Meter) and focuses on processing the inbound telemetry flow: real-time, cryptographically signed telemetry verified and synchronized with the VPP Platform, then driving flex dispatch.

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

- [ARCHITECTURE.md](ARCHITECTURE.md) - **Source of Truth**: Full technical system design — components, telemetry verification, aggregation, dissemination, and flex dispatch.

---

## 🛠 Tech Stack

- **Language**: Rust (Tokio/Axum)
- **Streaming**: Apache Kafka / Redis Streams
- **Cryptography**: Ed25519 (Verification), AES-256-GCM (DLMS frame decryption)
- **API**: gRPC (Protobuf), REST
- **Database**: TimescaleDB / SQLite (Circular Buffer)
