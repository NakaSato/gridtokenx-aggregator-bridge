# gridtokenx VPP — Oracle Bridge & Edge Device Architecture

**Technical System Design Document**
**Version 1.0 — March 2026**
**Target context:** Thailand MEA/PEA/ERC grid, PDP 2024 alignment, PDPA compliance

---

## 1. Architecture overview

The gridtokenx VPP operates as a four-layer hybrid edge-cloud platform with the oracle bridge as a cross-cutting component that connects the physical grid (edge devices) to both the VPP optimization platform and the GridTokenX blockchain settlement layer on HyperEVM.

**Four layers (bottom to top):**

- **Field/edge layer** — all DER device classes: smart meters, BESS, solar PV, EV chargers (V2G), smart loads
- **Communication layer** — EMQX MQTT 5.0 broker cluster, protocol adapters, mTLS/TLS 1.3
- **Oracle bridge layer** — cryptographic signing, ZK-rollup aggregation, Merkle attestation trees, EVM verification
- **Platform/cloud layer** — DER registry, three-tier optimizer (MILP + RL + auction), forecasting engine, dual settlement
- **Market/blockchain layer** — GridTokenX (HyperEVM), EGAT VPPCC, MEA/PEA billing integration

**Critical design principle:** every edge device feeds two parallel data paths — (1) real-time telemetry via MQTT/Kafka to the VPP platform for optimization and forecasting, and (2) cryptographically signed attestations via the oracle bridge to HyperEVM for P2P settlement, REC tokenization, and energy derivatives. Raw household data never leaves the device — only signed summaries and ZK proofs reach the blockchain, structurally enforcing PDPA compliance.

---

## 2. Field/edge layer — all DER device classes

### 2.1 Edge ML smart meters (primary oracle node)

The smart meter is the universal oracle node — it measures its own consumption and serves as the metering and signing backbone for co-located BESS, EV chargers, and smart loads. Every site needs exactly one Radxa Cubie A7Z with ATECC608B, regardless of how many DER asset types are present.

**Hardware specification:**

- **SoC:** Rockchip RK3566, quad-core Cortex-A55 @ 1.8 GHz
- **NPU:** RKNN NPU, 1 TOPS INT8 inference throughput
- **Secure element:** Microchip ATECC608B — tamper-resistant key storage, Ed25519 signing, measured boot chain
- **Memory:** 2 GB LPDDR4 (sufficient for Sparse MoE model + 24-hour data buffer)
- **Storage:** 32 GB eMMC (SQLite circular buffer, model files, firmware)
- **Connectivity:** NB-IoT/LTE module (primary), Wi-Fi 802.11ac (fallback), RS-485 for Modbus peripherals

**ML inference pipeline:**

- **Architecture:** Sparse Mixture-of-Experts (MoE) with shared 1D-CNN trunk
- **Experts:** 8 total, top-2 activated per inference via noisy top-K gating
- **Specialization:** Each expert targets a load category — HVAC, water heater, EV charger, lighting, refrigeration, cooking, laundry, electronics, industrial motor, miscellaneous
- **Parameters:** ~50M total, ~12M active per forward pass
- **Quantization:** INT8 via RKNN-Toolkit2 for NPU deployment
- **Latency:** <10ms per disaggregation cycle
- **Dual output heads:** power regression (per-appliance wattage) and appliance state classification (on/off/transitioning)

**Data acquisition (Rust pipeline):**

- **Sampling rate:** 1 second (voltage, current, power factor, active/reactive power, harmonics up to 15th order)
- **Upstream aggregation:** 15-second intervals for platform telemetry (reduces NB-IoT bandwidth)
- **Local buffer:** 24-hour SQLite circular buffer (86,400 raw samples/day at 1-second resolution)
- **Reconnection protocol:** Last-known-sequence sync on connectivity restoration
- **Data format:** Protocol Buffers (Protobuf) — ~40 bytes per aggregated sample, bandwidth-optimized for NB-IoT

**ML output (published every 15 seconds):**

| Output                 | Description                                                                         | Consumer                           |
| ---------------------- | ----------------------------------------------------------------------------------- | ---------------------------------- |
| Appliance power vector | Per-appliance active power (W) for 10 classes                                       | VPP forecasting engine             |
| Anomaly flags          | Phase imbalance >5%, THD >8%, voltage sag/swell ±10%, meter tamper, equipment fault | Platform anomaly service           |
| Flexibility scores     | Per-appliance 0–1 score based on thermal inertia, user behavior, charging curves    | VPP optimizer (dispatch targeting) |
| Aggregate consumption  | Total household kW/kWh (15-second resolution)                                       | MEA/PEA billing settlement         |

**MQTT topic hierarchy:**

```
gridtokenx/{region}/{feeder}/{meter_id}/telemetry     — 15-sec aggregated data (QoS 1)
gridtokenx/{region}/{feeder}/{meter_id}/anomaly        — real-time anomaly alerts (QoS 1)
gridtokenx/{region}/{feeder}/{meter_id}/flexibility    — per-appliance flex scores (QoS 1)
gridtokenx/{region}/{feeder}/{meter_id}/dispatch       — incoming dispatch commands (QoS 2)
gridtokenx/{region}/{feeder}/{meter_id}/attestation    — signed oracle attestations (QoS 2)
gridtokenx/{region}/{feeder}/{meter_id}/fl_gradient    — federated learning updates (QoS 1)
```

**Oracle role:** The smart meter is the primary hardware oracle for GridTokenX. Every 15 minutes, the Rust pipeline aggregates energy production/consumption data, the RKNN NPU computes a NILM summary hash, and the ATECC608B secure element signs the attestation record with Ed25519. This signed attestation is the atomic unit of truth that flows into the oracle bridge.

**Federated learning cycle (weekly):**

1. Local training on 7 days × 86,400 samples at 1-second resolution
2. Gradient computation for Sparse MoE model parameters
3. Differential privacy noise injection (ε=1.0, δ=10⁻⁵)
4. Compressed gradient upload (~500 KB) via dedicated MQTT control channel
5. Central aggregator applies robust gradient filtering (trimmed mean, outlier exclusion)
6. Updated global model quantized to INT8 RKNN format
7. OTA model push with A/B testing on 5% of fleet before full rollout

### 2.2 BESS and solar PV inverters

**Primary protocol:** SunSpec Modbus TCP

- Inverter model registers: 40000–40069 (nameplate, status, power output)
- Storage model registers: 40121–40150 (SoC, charge/discharge power, temperature)
- Protocol adapter daemon runs on the co-located Radxa smart meter or a standalone OpenEMS Edge instance

**Fallback protocol:** IEC 61850 MMS/GOOSE for utility-scale assets (>1 MW)

- Dedicated edge gateway (industrial Linux SBC) translates MMS/GOOSE messages to internal gRPC protocol
- GOOSE messages for fast trip/protection signaling (<4ms)
- MMS for supervisory data and setpoint commands

**Telemetry data points:**

| Parameter                       | Resolution | Source                       |
| ------------------------------- | ---------- | ---------------------------- |
| Active/reactive power (kW/kVAR) | 1 second   | SunSpec register 40076-40077 |
| State of charge (%)             | 5 seconds  | SunSpec register 40130       |
| Battery temperature (°C)        | 15 seconds | SunSpec register 40135       |
| DC string currents (A)          | 5 seconds  | SunSpec register 40260+      |
| Grid frequency (Hz)             | 1 second   | SunSpec register 40085       |
| Inverter operating state        | On change  | SunSpec register 40070       |

**Dispatch interface:**

- MILP optimizer generates 96-period (15-minute) charge/discharge schedules
- Schedules translated to SunSpec StorCtl commands (registers 40149-40150)
- Ramp-rate constraints enforced per manufacturer specification (typically 10-25% rated power per second)
- Battery degradation model (cycle-depth-weighted, calibrated per chemistry — LFP or NMC) constrains maximum cycle depth

**PV health cross-reference:**

- Solar PV Thermal Inspection Platform provides asset-level defect data via REST API
- YOLOv8 defect detection classifies 13 defect types per IEC 62446-3 (hotspot, cell crack, bypass diode failure, PID, snail trail, etc.)
- Forecasting engine applies a degradation-adjusted capacity factor: PV systems with detected defects receive reduced forecast ceilings proportional to defect severity class
- Only PV systems with current IEC 62446-3 compliant inspection records qualify for REC minting via the oracle bridge

**Oracle role:** BESS and PV generation/consumption data is metered by the co-located smart meter. The smart meter's signed attestation includes both household consumption and co-located DER production/storage flows, creating a single attestation per site per 15-minute interval.

### 2.3 EV chargers (V2G-capable)

**Primary protocol:** OCPP 2.0.1 over WebSocket/TLS

- Charge Point Management Service (CPMS) microservice handles all OCPP connections
- OCPP 2.0.1 features used: ChargingProfiles (Smart Charging), TransactionEvent, StatusNotification, GetVariables
- WebSocket with TLS 1.3 provides persistent bidirectional communication

**Vehicle-charger protocol:** ISO 15118-20

- Bidirectional power negotiation (V2G discharge scheduling)
- Plug & Charge authentication via V2G-PKI certificate chain
- Dynamic power profile exchange between vehicle BMS and charger
- EXI (Efficient XML Interchange) encoding for low-latency message exchange

**Telemetry data points:**

| Parameter                  | Resolution      | Source                        |
| -------------------------- | --------------- | ----------------------------- |
| Session energy (kWh)       | Per transaction | OCPP TransactionEvent         |
| Active power (kW)          | 5 seconds       | OCPP MeterValues              |
| EV state of charge (%)     | 30 seconds      | ISO 15118-20 SessionSetup     |
| Departure time (estimated) | On connection   | User input or learned pattern |
| Connector status           | On change       | OCPP StatusNotification       |
| V2G discharge energy (kWh) | Per transaction | ISO 15118-20 + OCPP           |

**Constraints model:**

- **Minimum SoC at departure:** User-configurable (default 80%), hard constraint in optimization
- **Battery degradation limit:** Cycle depth × frequency budget per chemistry, limiting V2G discharge to preserve EV battery warranty
- **Thermal envelope:** Charger and battery temperature limits, derate power in high ambient temperature
- **Power limits:** Per-charger maximum (typically 7.4 kW AC, 50 kW DC), per-site transformer capacity

**Dispatch translation:**

1. VPP optimizer generates fleet-level power commands (e.g., "site A: discharge 15 kW from EV fleet for 30 minutes")
2. CPMS allocates across connected chargers based on individual EV constraints (SoC, departure, degradation budget)
3. CPMS translates to OCPP ChargingProfile per charger (TxProfile for active sessions)
4. ISO 15118-20 negotiates the profile with each vehicle's BMS
5. Actual delivery confirmed via OCPP MeterValues and reconciled with dispatch target

**Oracle role:** V2G discharge is metered by the co-located smart meter and classified as generation. The oracle bridge treats V2G energy identically to solar PV generation — signed attestations enable GridTokenX P2P trading of V2G energy and, potentially, V2G-derived REC issuance (pending ERC regulatory clarification).

### 2.4 Smart loads (HVAC, water heaters)

**Primary protocol:** CTA-2045 modular communication interface (where available)

- Standardized demand response interface for residential appliances
- Commands: shed, endshed, loadup, grid emergency
- Response: operating state, available capacity, estimated flexibility duration

**Fallback protocol:** BACnet/IP via Building Management System (BMS)

- For commercial HVAC systems with BACnet-enabled controllers
- Read: zone temperature, setpoint, fan speed, compressor state
- Write: setpoint adjustment, operating mode (occupied/unoccupied/standby)

**Tertiary fallback: NILM-inferred state**

- When no direct communication protocol is available, the co-located smart meter's NILM disaggregation infers load operating state
- Enables passive flexibility estimation without requiring load-specific hardware
- Dispatch commands are issued indirectly (e.g., "reduce household consumption by 1.5 kW" with NILM providing real-time verification)

**Flexibility model (thermal inertia-based):**

| Load type      | Flexibility action       | Duration      | Recovery           |
| -------------- | ------------------------ | ------------- | ------------------ |
| HVAC (cooling) | Increase setpoint +1–2°C | 15–30 minutes | 5-minute pre-cool  |
| HVAC (heating) | Decrease setpoint -1–2°C | 15–30 minutes | 5-minute pre-heat  |
| Water heater   | Defer heating cycle      | 2–4 hours     | 30-minute reheat   |
| Pool pump      | Shift runtime window     | 4–8 hours     | No recovery needed |
| Refrigerator   | Defer defrost cycle      | 1–2 hours     | Auto-recovery      |

**Dispatch advantage of NILM-aware targeting:**

Traditional VPP platforms send blanket DR signals ("reduce 2 kW") without knowledge of what loads are running. Edge NILM disaggregation enables targeted dispatch:

- Smart meter identifies: HVAC 1.5 kW running, EV charger 4.5 kW active, refrigerator 0.3 kW steady
- VPP sends targeted command: "curtail EV charging to 2 kW for 30 minutes" instead of generic shed
- Result: required 2.5 kW reduction achieved while preserving thermal comfort and food safety
- Impact: higher DR participation rates (less prosumer inconvenience) and improved dispatch precision (actual delivered flexibility matches committed flexibility)

**Oracle role:** Demand reduction events are metered by the smart meter's NILM pipeline. The oracle bridge generates DR performance attestations — signed proof that a specific load was curtailed for a specific duration, verified by before/after NILM disaggregation. These attestations enable GridTokenX settlement of DR incentive payments.

---

## 3. Communication layer

### 3.1 Three-tier message architecture

**Tier 1 — Device-to-edge (southbound):**

- **Broker:** EMQX 5.x cluster (5 nodes at scale, supporting 500,000+ concurrent MQTT sessions)
- **Protocol:** MQTT 5.0 with shared subscriptions for horizontal consumer scaling
- **QoS levels:** QoS 1 for telemetry and flexibility scores, QoS 2 for dispatch commands and attestations
- **Security:** TLS 1.3 with client certificates (mTLS) — every device authenticates with its ATECC608B-stored certificate
- **Rules engine:** EMQX rules engine performs first-pass data validation (range checks, schema conformance) and topic-based routing
- **Protocol adapters:** Dedicated adapter services handle protocol translation:
  - Modbus TCP/RTU → Protobuf (for BESS/PV inverters)
  - SunSpec → Protobuf (for SunSpec-certified inverters)
  - OCPP 2.0.1 → Protobuf (for EV chargers via CPMS)
  - DNP3 → Protobuf (for legacy SCADA integration with MEA/PEA)
  - IEC 60870-5-104 → Protobuf (for substation automation integration)

**Tier 2 — Edge-to-platform (core):**

- **Streaming backbone:** Apache Kafka (KRaft mode, no ZooKeeper) — 6 brokers at scale
- **Partitioning:** By asset_id (1,024 partitions for telemetry topics) ensuring per-device ordering
- **Key topics:**

| Topic                      | Purpose                                              | Retention                        |
| -------------------------- | ---------------------------------------------------- | -------------------------------- |
| `telemetry.raw`            | Raw 15-second meter data                             | 7 days (then tier to S3/Parquet) |
| `telemetry.processed`      | NILM-enriched data with appliance breakdown          | 30 days                          |
| `dispatch.commands`        | Optimizer → DER dispatch instructions                | 24 hours                         |
| `dispatch.acknowledgments` | DER → platform dispatch confirmation                 | 24 hours                         |
| `market.signals`           | EGAT DR events, TOU tariff updates                   | 90 days                          |
| `settlement.events`        | On-chain settlement confirmations from oracle bridge | Permanent                        |
| `anomaly.alerts`           | Edge-detected and platform-correlated anomalies      | 30 days                          |
| `attestation.signed`       | Signed meter attestations for oracle bridge          | Until ZK-proved                  |
| `fl.gradients`             | Federated learning gradient updates                  | 7 days                           |

- **Tiered storage:** Kafka data older than 7 days moves to Apache Parquet on S3-compatible object storage (MinIO for on-premise or AWS S3), queryable via DuckDB for analytics
- **Kafka Connect sinks:** TimescaleDB (time-series), ML feature store (Feast), PostgreSQL (DER registry updates)

**Tier 3 — Platform-to-market (northbound):**

- **REST/OpenAPI 3.0:** EGAT VPPCC integration, ERC regulatory reporting, utility partner portal
- **OpenADR 3.0:** DR event signaling (program discovery, event distribution, opt-out handling, telemetry reporting) between EGAT's DR Control Center and gridtokenx
- **gRPC:** Internal microservice communication (sub-millisecond latency, bidirectional streaming for telemetry)
- **GraphQL:** Operator dashboards and utility partner portals (flexible query patterns for diverse frontend needs)

### 3.2 Scale calculations

At 500,000 edge meters (2030 target):

- **MQTT sessions:** 500,000 concurrent (EMQX 5-node cluster capacity: 10M+)
- **Telemetry messages:** 500,000 meters × 4 readings/minute (15-sec intervals) = 2M rows/minute = ~33,000 rows/second to Kafka
- **Attestation messages:** 500,000 meters × 4 attestations/hour (15-min intervals) = ~556 attestations/second to oracle bridge
- **Dispatch commands:** ~5,000 commands/minute during DR events (targeting 1% of fleet per optimization cycle)
- **Kafka throughput:** 33,000 messages/second sustained, well within single-cluster capacity (Kafka benchmarks at 2M+ messages/second)

---

## 4. Oracle bridge layer — detailed architecture

The oracle bridge is the trust boundary between the physical grid and the blockchain. It transforms raw meter readings into cryptographically verified on-chain energy data while preserving individual privacy under PDPA.

### 4.1 Stage 1 — edge measurement and inference (on-device)

**Process flow:**

1. Rust data acquisition pipeline samples V/I/PF at 1-second resolution
2. RKNN NPU runs Sparse MoE NILM inference (<10ms per cycle)
3. Inference outputs: per-appliance power, anomaly flags, flexibility scores
4. Outputs published to MQTT for platform consumption (telemetry path)
5. Simultaneously, a 15-minute energy summary is prepared for attestation (oracle path)

**Data generated per 15-minute window:**

```
{
  "meter_id": "SGM-BKK-001-A7Z",
  "window_start": "2026-03-30T10:00:00+07:00",
  "window_end": "2026-03-30T10:15:00+07:00",
  "total_consumption_wh": 1250,
  "total_production_wh": 890,
  "net_export_wh": 0,
  "net_import_wh": 360,
  "nilm_summary_hash": "sha256:a3f8...",
  "anomaly_count": 0,
  "avg_power_factor": 0.95,
  "max_demand_w": 3200,
  "co_located_assets": [
    {"type": "pv", "production_wh": 890, "capacity_kw": 5.0},
    {"type": "bess", "charge_wh": 200, "discharge_wh": 0, "soc_pct": 65},
    {"type": "ev_charger", "consumption_wh": 450, "v2g_discharge_wh": 0}
  ]
}
```

### 4.2 Stage 2 — hardware oracle signing (on-device)

**Secure element: ATECC608B**

- **Key storage:** Ed25519 private key generated on-chip during device provisioning, never exported
- **Signing operation:** Attestation record serialized as Protobuf → SHA-256 hash → Ed25519 signature via ATECC608B I²C interface
- **Tamper resistance:** Active metal shield, voltage/frequency monitoring, DPA/SPA countermeasures
- **Certificate chain:** Device certificate → intermediate CA (gridtokenx PKI) → root CA
- **Key rotation:** Annual re-keying via secure OTA provisioning channel

**Signed attestation output:**

```
{
  "attestation": { ... },  // Protobuf-encoded energy data
  "signature": "ed25519:<base64-encoded-64-byte-signature>",
  "device_cert_fingerprint": "sha256:<cert-hash>",
  "sequence_number": 48392,
  "firmware_hash": "sha256:<measured-boot-hash>"
}
```

**Batch accumulation:**

- Signed attestations are accumulated in a local queue (SQLite)
- Every 15 minutes, the batch is published to MQTT topic `gridtokenx/.../attestation` with QoS 2
- If connectivity is lost, attestations queue locally and are transmitted in order upon reconnection
- Sequence numbers enable the ZK aggregator to detect gaps and request retransmissions

**Post-quantum migration path:**

- Current: Ed25519 (128-bit classical security)
- Migration target: ML-DSA (CRYSTALS-Dilithium, NIST FIPS 204) — lattice-based, quantum-resistant
- ATECC608B replacement: ATECC608C or equivalent with ML-DSA support (expected 2027–2028)
- Hybrid signing during transition: Ed25519 + ML-DSA dual signatures, verifiable by both classical and PQC verifiers

### 4.3 Stage 3 — ZK-rollup aggregation (off-device, cloud)

This stage runs on dedicated compute nodes in the gridtokenx Kubernetes cluster. It is the PDPA compliance boundary — individual meter data enters this stage but does not exit it. Only aggregated proofs leave.

**Batch collector service:**

- Consumes from Kafka topic `attestation.signed`
- Groups attestations by 15-minute window and geographic zone (MEA district / PEA area)
- Validates Ed25519 signatures against device certificate registry
- Rejects attestations with invalid signatures, out-of-sequence numbers, or failed firmware hash verification
- Produces verified batches of ~500–5,000 attestations per window per zone

**Merkle tree builder:**

- Constructs a SHA-256 Merkle tree over the verified attestation batch
- Leaf nodes: individual attestation hashes
- Tree depth: log₂(batch_size), typically 10–13 levels
- Merkle root: single 32-byte hash representing the entire batch
- Merkle proofs stored for each leaf, enabling individual attestation verification without revealing the batch

**Plonky2 ZK prover:**

The ZK proof circuit verifies the following statements without revealing individual meter data:

1. **Signature validity:** Every attestation in the batch has a valid Ed25519 signature from a registered device
2. **Temporal consistency:** All attestations fall within the declared 15-minute window
3. **Energy conservation:** Total production equals total consumption plus net export minus net import across the batch (Kirchhoff's law check)
4. **Statistical consistency:** No individual meter reports values >3σ from its historical mean (anomaly exclusion)
5. **Merkle inclusion:** The Merkle root correctly commits to exactly the set of verified attestations

**ZK proof characteristics:**

| Property           | Value                                            |
| ------------------ | ------------------------------------------------ |
| Proof system       | Plonky2 (Goldilocks field, FRI-based)            |
| Proof size         | ~45 KB per batch                                 |
| Proving time       | ~20–30 seconds per batch (on 32-core server)     |
| Verification time  | ~2ms on-chain (EVM)                              |
| Quantum resistance | Yes — hash-based (no elliptic curve assumptions) |
| Privacy            | Individual meter data not recoverable from proof |

**Output per 15-minute window per zone:**

```
{
  "zone_id": "MEA-BKK-RATBURANA",
  "window": "2026-03-30T10:00:00+07:00",
  "merkle_root": "0xa3f8...",
  "meter_count": 2347,
  "total_production_kwh": 1893.5,
  "total_consumption_kwh": 4721.2,
  "total_net_export_kwh": 312.8,
  "zk_proof": "<plonky2-proof-bytes>",
  "prover_version": "gridtokenx-plonky2-v1.2"
}
```

### 4.4 Stage 4 — HyperEVM on-chain verification and settlement

**Verifier contract (Solidity on HyperEVM):**

- Receives ZK proof + Merkle root + aggregated totals from the oracle bridge relayer service
- Calls the Plonky2 verifier precompile (or custom verifier contract) to validate the proof
- If valid: stores the Merkle root and aggregated data in the on-chain energy data registry
- If invalid: reverts transaction, emits alert event, triggers investigation in platform anomaly service
- Gas cost: ~200,000 gas per verification (~$0.02 at typical HyperEVM gas prices)

**Downstream settlement contracts (triggered by verified data):**

**P2P energy trading — ERC-1155 order book:**

- Each tokenId encodes delivery time slot (15-min) + price level (in $W2T)
- Prosumers mint sell-orders for surplus energy; consumers mint buy-orders
- Matcher contract clears matched orders atomically
- On match: buyer's $W2T → producer's yield vault; Principal Token (PT) minted for delivery right; Yield Token (YT) minted for revenue claim
- Physical delivery confirmed by oracle bridge attestation in the matching time slot

**REC minting — I-REC with EnergyTag extensions:**

- Triggered when oracle bridge confirms renewable generation from a qualified PV system
- Qualification check: asset must have current IEC 62446-3 inspection record (verified against PV inspection platform API)
- ERC-1155 token metadata embeds: generation timestamp (15-min granularity), generator DID, location (feeder/transformer), energy quantity (kWh), inspection status hash, I-REC serial number
- EGAT holds a validator role on the oracle committee for I-REC registry synchronization
- Enables 24/7 carbon-free energy matching for data centers and BOI-promoted industries

**Energy derivatives via HIP-3:**

- Perpetual futures deployed on Hyperliquid's HyperCore CLOB
- Markets: THPWR-USDC (Thai electricity price index), SREC-USDC (REC price index), CCX-USDC (carbon credit)
- HyperCore specifications: ~0.2 second finality, 200,000 orders/second throughput
- ERC-1155 green bond tranching packages VPP revenue streams (DR payments, arbitrage, REC sales) into senior/mezzanine/equity tranches

**Yield vaults:**

- Auto-compound returns from grid service revenues, P2P trading spreads, and REC sales
- Denominated in $AIC (GridTokenX utility token)
- Returns convertible to veW2T governance power (vote-escrowed $W2T)
- veW2T holders vote on VPP market parameters: maximum spread, minimum lot size, feeder congestion surcharges

---

## 5. Cross-layer data flow — complete cycle

The full lifecycle of a single energy measurement from physical grid to blockchain settlement:

```
1. Physical measurement (0ms)
   └─ CT/PT sensors sample V/I/PF at 1-second resolution

2. Edge inference (10ms)
   └─ RKNN NPU runs Sparse MoE NILM disaggregation
   └─ Outputs: appliance power, anomaly flags, flexibility scores

3. Dual-path publication (50ms)
   ├─ PATH A (Telemetry): MQTT QoS 1 → EMQX → Kafka → TimescaleDB
   │   └─ Consumed by: forecasting engine, optimizer, anomaly service
   └─ PATH B (Attestation): 15-min accumulation → Ed25519 sign → MQTT QoS 2

4. Oracle bridge aggregation (15 minutes + 30 seconds)
   └─ Batch collector groups attestations by zone/window
   └─ Merkle tree construction over verified batch
   └─ Plonky2 ZK proof generation (~20-30s)

5. On-chain verification (~0.2 seconds)
   └─ Relayer submits proof + Merkle root to HyperEVM verifier contract
   └─ Contract validates ZK proof → stores verified energy data

6. Settlement (atomic, same transaction or next block)
   ├─ P2P trades cleared against verified delivery data
   ├─ RECs minted for verified renewable generation
   └─ Derivative positions marked-to-market against price oracle

7. Value distribution (next epoch, ~1 hour)
   └─ Yield vaults compound accumulated revenues
   └─ veW2T governance power updated
```

**End-to-end latency:** ~10ms edge inference + 15-minute batch window + ~30s ZK proof + ~0.2s EVM finality = **~15 minutes 30 seconds** from measurement to on-chain settlement.

**Real-time VPP operations are not bottlenecked by blockchain settlement.** The telemetry path (PATH A) delivers data to the optimizer in <1 second. Dispatch commands execute in <100ms. Only financial settlement waits for the 15-minute oracle bridge cycle.

---

## 6. Security architecture

### 6.1 Zero-trust network segmentation

| Zone     | Components                        | Ingress control          | Egress control                  |
| -------- | --------------------------------- | ------------------------ | ------------------------------- |
| Field    | Edge meters, DERs                 | mTLS device certificates | Only MQTT to DMZ                |
| DMZ      | EMQX brokers, protocol adapters   | Allowlisted IPs only     | Kafka internal, MQTT from field |
| Platform | Kubernetes cluster (all services) | API gateway (Envoy)      | EGAT/MEA APIs, HyperEVM RPC     |
| Market   | External APIs, blockchain nodes   | OAuth 2.0 + JWT          | Restricted to partner endpoints |

### 6.2 Cryptographic stack

| Layer           | Algorithm                  | Purpose                                            |
| --------------- | -------------------------- | -------------------------------------------------- |
| Device identity | Ed25519 (ATECC608B)        | Attestation signing, device authentication         |
| Transport       | TLS 1.3 + X25519Kyber768   | Hybrid classical + post-quantum key exchange       |
| Service mesh    | Istio mTLS (ECDSA P-256)   | East-west microservice authentication              |
| ZK proofs       | Plonky2 (FRI/Goldilocks)   | Batch attestation verification (quantum-resistant) |
| Blockchain      | ECDSA secp256k1 (HyperEVM) | Transaction signing                                |
| API auth        | JWT (RS256, 15-min expiry) | Short-lived authorization tokens                   |

### 6.3 PDPA compliance architecture

- **Data residency:** Kubernetes cluster runs in Thai data centers (True IDC, CAT Telecom)
- **Consent management:** Dedicated consent service with granular permissions — meter data collection (required), DR participation (optional), P2P trading (optional), federated learning contribution (optional)
- **Data minimization:** Appliance-level NILM data remains on-device or in prosumer's encrypted personal data store; only aggregated fleet data flows to utility partners
- **Right to deletion:** Prosumer can revoke consent → meter stops publishing attestations → historical on-chain data is pseudonymous (DID-based, not PII-linked)
- **Cross-border restriction:** No meter data transmitted outside Thailand; ZK proofs on HyperEVM contain only aggregated statistical summaries

### 6.4 IEC 62351 compliance

- **62351-3:** TLS profiles for all power system protocol communications
- **62351-8:** Role-based access control for operator dashboard, utility partner portal, and API access
- **62351-14 (draft):** Security event logging and monitoring via centralized SIEM (Elasticsearch + Kibana)

---

## 7. Scalability targets

| Metric            | Phase 1 (2026–2027)                       | Phase 2 (2027–2029)                       | Phase 3 (2029–2032)                      |
| ----------------- | ----------------------------------------- | ----------------------------------------- | ---------------------------------------- |
| Edge meters       | 5,000                                     | 100,000                                   | 500,000+                                 |
| DER capacity      | 50 MW solar, 10 MWh BESS, 500 EV chargers | 1 GW solar, 200 MWh BESS, 20K EV chargers | 5 GW solar, 1 GWh BESS, 100K EV chargers |
| Coverage          | 1 MEA district (Rat Burana)               | MEA + 3 PEA provinces                     | MEA + PEA national                       |
| MQTT sessions     | 5,000                                     | 100,000                                   | 500,000+                                 |
| Kafka throughput  | 330 msg/sec                               | 6,600 msg/sec                             | 33,000 msg/sec                           |
| ZK proofs/hour    | 4 (1 zone)                                | 120 (30 zones)                            | 800+ (200 zones)                         |
| On-chain txns/day | ~96                                       | ~2,880                                    | ~19,200                                  |
| Kubernetes nodes  | 3                                         | 15                                        | 50+ (multi-region)                       |

### 7.1 Time-series data layer

- **Database:** TimescaleDB (PostgreSQL extension)
- **Partitioning:** Hypertable by meter_id and time
- **Continuous aggregates:** 1-second → 15-second → 15-minute → hourly rollups
- **Compression:** Columnar compression achieving >90% storage reduction
- **Tiering:** Data older than 30 days → Apache Parquet on S3, queryable via DuckDB

### 7.2 Kubernetes deployment topology

| Node pool        | Workload                          | Instance type      |
| ---------------- | --------------------------------- | ------------------ |
| CPU-optimized    | MILP optimization (Gurobi/HiGHS)  | 16-core, 32 GB RAM |
| GPU              | ML training (PyTorch), ZK proving | NVIDIA A10G / T4   |
| General-purpose  | Microservices, API gateway        | 8-core, 16 GB RAM  |
| Memory-optimized | TimescaleDB, Redis cache          | 8-core, 64 GB RAM  |

Geographic distribution: edge aggregation services in Bangkok (MEA) and regional hubs (PEA zones) for low-latency DER control.

---

## 8. API specifications

### 8.1 Internal APIs (gRPC + Protobuf)

```protobuf
service DERRegistry {
  rpc RegisterAsset (AssetRegistration) returns (AssetRecord);
  rpc GetAsset (AssetId) returns (AssetRecord);
  rpc UpdateAssetStatus (AssetStatusUpdate) returns (AssetRecord);
  rpc ListAssetsByFeeder (FeederQuery) returns (stream AssetRecord);
}

service DispatchService {
  rpc SendDispatch (DispatchCommand) returns (DispatchAck);
  rpc StreamDispatches (DispatchFilter) returns (stream DispatchCommand);
  rpc GetDispatchStatus (DispatchId) returns (DispatchStatus);
}

service OracleBridge {
  rpc SubmitAttestation (SignedAttestation) returns (AttestationReceipt);
  rpc GetMerkleProof (AttestationId) returns (MerkleProof);
  rpc GetBatchStatus (BatchId) returns (BatchStatus);
}

service ForecastService {
  rpc GetSolarForecast (ForecastRequest) returns (ForecastTimeSeries);
  rpc GetLoadForecast (ForecastRequest) returns (ForecastTimeSeries);
  rpc GetFlexibilityMap (FlexibilityQuery) returns (FlexibilityMap);
}
```

### 8.2 External APIs (REST/OpenAPI 3.0)

| Endpoint                           | Method    | Purpose                            |
| ---------------------------------- | --------- | ---------------------------------- |
| `/api/v1/assets`                   | GET, POST | DER registration and listing       |
| `/api/v1/assets/{id}/telemetry`    | GET, WS   | Real-time and historical telemetry |
| `/api/v1/dispatch`                 | POST      | Submit dispatch command            |
| `/api/v1/dr/events`                | GET       | List active and upcoming DR events |
| `/api/v1/dr/events/{id}/opt-out`   | POST      | Prosumer opt-out from DR event     |
| `/api/v1/settlement/positions`     | GET       | Current market positions           |
| `/api/v1/settlement/history`       | GET       | Historical settlement records      |
| `/api/v1/oracle/attestations`      | GET       | Verified attestation records       |
| `/api/v1/oracle/proofs/{batch_id}` | GET       | ZK proof for a specific batch      |
| `/api/v1/recs`                     | GET       | REC portfolio and history          |

### 8.3 OpenADR 3.0 integration

Implements the full program lifecycle for EGAT DR dispatch:

1. **Program discovery:** gridtokenx registers as a VEN (Virtual End Node) with EGAT's VTN (Virtual Top Node)
2. **Event distribution:** EGAT sends DR events specifying target reduction (MW), duration, and compensation rate
3. **Opt-out handling:** Prosumers can opt-out within a configurable window; gridtokenx re-optimizes remaining fleet
4. **Telemetry reporting:** gridtokenx reports actual delivered reduction via OpenADR telemetry, backed by oracle bridge attestations

---

## 9. Deployment roadmap

**Phase 1 — ERC Sandbox Pilot (2026–2027):**
Deploy 5,000 edge ML smart meters across MEA Rat Burana district. Integrate 50 MW rooftop solar, 10 MWh BESS, 500 V2G-capable EV chargers. Demonstrate peak shaving and renewable firming for MEA. Launch GridTokenX P2P trading within sandbox community. Core platform: 3-node Kubernetes cluster, single-region deployment. Highest-risk component: ZK-rollup oracle bridge (TRL 6–7), mitigated through initial deployment with trusted oracle committee before full ZK verification.

**Phase 2 — Utility Partnership Scaling (2027–2029):**
Expand to 100,000 meters across MEA and 3 PEA provinces. Integrate with EGAT VPPCC for DR dispatch via OpenADR 3.0. Launch REC tokenization with EGAT as I-REC validator. Introduce energy derivatives on HyperEVM. Scale to multi-region Kubernetes with TimescaleDB clustering. Transition from trusted oracle committee to full Plonky2 ZK verification.

**Phase 3 — National VPP Platform (2029–2032):**
Target 500,000+ endpoints spanning MEA + PEA national. Full grid services portfolio including frequency regulation. Federated learning fleet-wide optimization. Cross-border REC trading within ASEAN. Position as VPP infrastructure provider for TPA-enabled competitive retail market when regulations mature. Post-quantum cryptography migration (Ed25519 → ML-DSA).

---

## 10. Key design decisions summary

| Decision                                  | Rationale                                                                                    |
| ----------------------------------------- | -------------------------------------------------------------------------------------------- |
| Smart meter as universal oracle node      | Single hardware per site regardless of DER count; ATECC608B provides hardware root of trust  |
| Decomposed optimization (Tesla pattern)   | Scales linearly with sites — no central MILP re-solve needed as fleet grows                  |
| ZK-rollup for oracle bridge               | PDPA compliance by construction; individual data never on-chain; quantum-resistant           |
| Dual data paths (telemetry + attestation) | Decouples real-time VPP operations (<1s) from financial settlement (15-min)                  |
| NILM-aware targeted dispatch              | Higher DR participation, better dispatch precision, prosumer comfort preserved               |
| HyperEVM for settlement                   | Sub-second finality, institutional-grade CLOB via HIP-3, EVM compatibility                   |
| Federated learning for model updates      | Fleet-wide NILM improvement without centralizing raw energy data                             |
| OpenADR 3.0 for EGAT integration          | Industry standard for utility DR programs; program-based lifecycle matches ERC framework     |
| Protocol-agnostic communication layer     | Supports Thailand's heterogeneous AMI landscape (Modbus, SunSpec, OCPP, CTA-2045, IEC 61850) |

---

_Document prepared as part of the gridtokenx VPP technical design phase. Architecture integrates with GridTokenX DePIN protocol, Sparse MoE edge ML smart meters, and Solar PV Thermal Inspection Platform per existing system specifications._
