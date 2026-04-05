# GridTokenX Oracle Bridge: VPP Operation & Trust Layer

## 1. Overview
The **GridTokenX Oracle Bridge** is a **Private Cloud-Native** service that orchestrates **VPP Operations** and ensures the cryptographic integrity of energy data. It acts as the high-throughput ingestion entry point for the GridTokenX VPP, handling real-time telemetry (Path A) and ZK-attestation aggregation (Path B).

> [!IMPORTANT]
> **Service Boundary**: This repository contains the **Platform-side Bridge Service**. It is decoupled from the physical **Oracle of Edge Meter** hardware. This service consumes cryptographically signed data via the **DLMS/COSEM (IEC 62056)** global standard and bridges it to the VPP Optimization engine and HyperEVM.

---

## 2. VPP Operations (Path A — Real-Time)
The Oracle Bridge is the primary orchestrator for real-time grid services, ensuring sub-second data availability for the VPP Platform.

### 2.1 Industrial Low-Latency Ingestion
- **Global Standard Compliance**: Exclusively uses **DLMS/COSEM (IEC 62056)** for all B2C and B2B telemetry, ensuring unified high-fidelity energy accounting.
- **Zero-Copy Performance**: Utilizing `buffa::view::OwnedView` for Path A ingestion, enabling sub-millisecond Protobuf processing without memory allocations.
- **Dynamic Dispatch**: Routes incoming telemetry to the forecasting and MILP optimization engines in `<50ms`.

### 2.2 In-Memory Aggregation
- **VPP Metrics**: Computes real-time feeder-level and zone-level load aggregates.
- **Prometheus Integration**: Exposes granular operational metrics per asset class (BESS SoC, PV Yield, EV Load).

---

## 3. Trust & Aggregation (Path B — Settlement)
The service performs the heavy computational lifting for recursive ZK-Rollups, enabling PDPA-compliant settlement.

### 3.1 ZK-Rollup Aggregator (Stage 3)
Instead of processing individual transactions on-chain, the bridge performs off-chain aggregation:
- **Batching**: Groups 15-minute attestation windows into zone-level batches.
- **Verification**: Validates the Ed25519 hardware signatures of all incoming attestations against the GridTokenX device registry.
- **ZK-Proving**: Generates a **Plonky2** recursive proof that certifies the aggregate production/consumption of the batch is valid and hardware-verified.

### 3.2 Privacy Boundary
Raw household-level data enters the bridge but is **never stored**. Only the ZK-proof and the Merkle Root are persisted, ensuring that prosumer privacy is structurally protected under PDPA.

---

## 4. Downstream Settlement Integration (HyperEVM)
The Oracle Bridge provides the final cryptographic artifacts required for on-chain finality.

- **Proof Relaying**: Submits the generated ZK-proofs to the `Plonky2Verifier.sol` contract on HyperEVM.
- **Settlement Triggers**: Verified batches automatically trigger P2P order clearance and I-REC minting on the settlement layer.

---

## 5. Service APIs (gRPC + Protobuf)
The bridge exposes a high-performance gRPC surface for internal platform services and authorized edge gateways.

```protobuf
service OracleService {
  // Professional Path A: High-frequency telemetry (Zero-Copy)
  rpc SubmitTelemetry (TelemetryRequest) returns (TelemetryResponse);
  rpc SubmitTelemetryBatch (TelemetryBatchRequest) returns (TelemetryBatchResponse);
  
  // Professional Path B: Settlement Attestations (IEC 62056)
  rpc SubmitAttestation (AttestationRequest) returns (AttestationResponse);
  rpc SubmitAttestationBatch (AttestationBatchRequest) returns (AttestationBatchResponse);
}
```

---

## 6. Performance Targets
| Metric | Target |
| :--- | :--- |
| **Ingestion Processing** | `<50ms` |
| **ZK Batch Proving** | `20-30s` per 5k attestations |
| **Telemetry Throughput** | 33,000 msg/sec |
| **VPP Loopback** | `<2s` end-to-end |

---

## Related Documentation
- [implement.md](../implement.md) - Full System Architecture
- [TELEMETRY.md](TELEMETRY.md) - Real-Time VPP Operations (Path A)
- [DATA-FLOW.md](DATA-FLOW.md) - Operational vs. Settlement Data Paths
- [core.md](core.md) - Architecture Diagrams & Integration Matrix
