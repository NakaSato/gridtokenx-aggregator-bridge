# GridTokenX Oracle Bridge: VPP Operation & Trust Layer

## 1. Overview
The **GridTokenX Oracle Bridge** is a **Private Cloud-Native** service that orchestrates **VPP Operations** and ensures the cryptographic integrity of energy data. It serves as the high-throughput ingestion entry point for the GridTokenX ecosystem, implementing the **Unified Trusted Telemetry (UTT)** architecture.

> [!IMPORTANT]
> **Architectural Shift**: Starting from v4, the bridge has consolidated the real-time grid operations (Path A) and blockchain settlement (Path B) into a **single, high-integrity ingestion pipeline**. This reduces architectural complexity and ensures that every piece of data in the system is hardware-verified.

---

## 2. Unified Trusted Telemetry (UTT)
The Oracle Bridge enforces a "Trust-on-Entry" model, where a single gRPC call provides all data necessary for both immediate grid control and eventual on-chain finality.

### 2.1 Industrial Ingestion (UTT-H)
- **Unified Path**: Replaces split telemetry and attestation flows with a single `Ingest` RPC.
- **Hardware-Enforced Security**: Every reading is signed by the source device's Ed25519 key pair following the **UTT-H** (High Integrity) standard.
- **Hardened Anti-Replay**: Signatures are generated over a canonical string including millisecond precision and a monotonic sequence number.
- **Performance**: Capable of processing **33,000+ readings/sec** with end-to-end latency of **<50ms**.

### 2.2 Secure Binary Protocol (v4)
- **Privacy-by-Design**: High-fidelity metrics are encrypted using **AES-256-GCM** (Authenticated Encryption), ensuring full PDPA compliance.
- **Integrity**: Multi-layer protection using **CRC-32** checksums and GCM authentication tags.
- **Extensibility**: Uses **TLV** (Tag-Length-Value) encoding to allow future-proof expansion of meter metrics without firmware-backend misalignment.

---

## 3. Operational Logic

### Path A: Real-Time VPP Optimization
Validated telemetry is immediately fanned out to:
- **Kafka**: For forecasting and load balancing.
- **NATS**: For sub-second grid dispatch and direct blockchain minting triggers.
- **Redis**: For real-time monitoring and hot-state storage.

### Path B: On-Chain Settlement (Safe Sink)
The bridge automatically manages the settlement lifecycle:
- **Aggregation**: Data is accumulated in 15-minute billing windows (Bins).
- **Automated Settlement**: Upon window closure, the bridge signs the aggregate or triggers the **Plonky2 ZK-prover**.
- **Finality**: Signed artifacts are pushed to HyperEVM for P2P order clearance and I-REC minting.

---

## 4. Core RPC Interface (v4)
The bridge exposes a high-performance gRPC surface for edge gateways.

```protobuf
service OracleService {
  // Unified Ingestion (UTT)
  rpc Ingest (MeterReading) returns (IngestResponse);
  
  // High-Throughput Batch Ingestion
  rpc IngestBatch (MeterReadingBatchRequest) returns (MeterReadingBatchResponse);
}
```

---

## 5. Security Policy
- **Development**: Invalid/missing signatures result in warnings to facilitate rapid prototyping.
- **Production (`ENVIRONMENT=production`)**: Strict enforcement of **UTT-H** signing and **v4 Secure** encryption. Malformed or unauthorized telemetry results in immediate rejection.

---

## Related Documentation
- [INGESTION-API.md](INGESTION-API.md) - Field-level gRPC reference.
- [INGESTION-PROTOCOL-V4.md](INGESTION-PROTOCOL-V4.md) - Binary v4 (Secure) specification.
- [core.md](core.md) - Architecture Diagrams & Integration Matrix.
