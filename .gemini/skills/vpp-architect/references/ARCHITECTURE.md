# VPP Four-Layer Architecture

## 1. Field Layer (Edge)
- **Primary Node:** Smart Meter (Radxa Cubie A7Z).
- **Security:** ATECC608B Secure Element (Root of Trust).
- **Software:** Rust-based sampling (1s) and aggregation (15s).

## 2. Communication Layer
- **MQTT:** EMQX 5.0 cluster (mTLS with device certificates).
- **Streaming:** Apache Kafka (KRaft mode).
- **Topics:**
  - `telemetry.raw`: 15-second intervals.
  - `attestation.signed`: 15-minute intervals.

## 3. Aggregator Bridge Layer
- **UTT Ingestion:** Verified entry point for both paths.
- **ZK-Prover:** Plonky2 (Goldilocks field, FRI-based).
- **Finality:** Merkle Root + Plonky2 Proof.

## 4. Market Layer
- **Settlement:** HyperEVM.
- **Tokens:** ERC-1155 (Energy Tokens), veW2T (Governance).
- **Market Engines:** P2P Order Book, I-REC Minting, HIP-3 Derivatives.
