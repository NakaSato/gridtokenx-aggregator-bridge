# GridTokenX Oracle Bridge & IoT Gateway — Technical Documentation

## Overview

The Oracle Bridge + IoT Gateway is a high-performance Rust microservice that serves two primary functions:
1. **IoT Ingestion Gateway**: Provides a unified HTTP API for heterogeneous IoT devices (Smart Meters, EV Chargers, Batteries) and disseminates normalized data to Redis Streams.
2. **Oracle Bridge**: A stateful relay that consumes events from Redis Streams and synchronizes them to the Solana blockchain in real-time.

```
IoT Devices / Simulators
        │ (HTTP/JSON)
        ▼
   IoT Gateway (Port 4010)
        │
   Redis Streams (gridtokenx:events:v1, gridtokenx:ev:v1, ...)
        │
   ┌──────────────────────────┐
   │      Oracle Bridge       │
   │  ┌────────────────────┐  │
   │  │   EventIngester    │  │  ← Multi-stream Consumer
   │  │        │           │  │
   │  │  ┌─────▼─────┐     │  │
   │  │  │Blockchain │     │  │  ← Solana Transaction Submitter
   │  │  └─────┬─────┘     │  │
   │  └────────│───────────┘  │
   └───────────┼──────────────┘
               ▼
        Solana Blockchain
```

---

## Architecture

### Module Structure

| Module | File | Responsibility |
|---|---|---|
| `main` | `src/main.rs` | Bootstrap, Axum server setup, and Service orchestration |
| `handlers` | `src/handlers.rs` | HTTP endpoints for device ingestion and health checks |
| `protocol` | `src/protocol/*` | Normalization adapters for different device types (OCPP, DLMS-lite) |
| `router` | `src/router.rs` | Routing normalized events to specific Redis Streams |
| `ingester` | `src/ingester/mod.rs` | Redis Stream consumer & multi-device event routing |
| `blockchain` | `src/blockchain/mod.rs` | Solana transaction construction, signing, and RPC submission |

---

## IoT Gateway API

Accepts POST requests with device-specific payloads and returns a unique `reading_id`.

| Endpoint | Device Type | Payload Model |
|---|---|---|
| `/api/v1/ingest/smart-meter` | Smart Meter | `SmartMeterPayload` |
| `/api/v1/ingest/ev-charger` | EV Charger | `EvChargerPayload` |
| `/api/v1/ingest/battery` | Battery | `BatteryPayload` |
| `/api/v1/ingest` | Auto-detect | `GenericIngestPayload` |

### Typical Payload (Smart Meter)
```json
{
  "device_id": "meter-001",
  "energy_generated": 10.5,
  "energy_consumed": 2.1,
  "timestamp": "2024-03-25T10:00:00Z"
}
```

---

## Oracle Bridge (On-Chain Sync)

The `EventIngester` subscribes to multiple streams and maps IoT events to the Solana Oracle program.

- **Unified Mapping**:
  - **Smart Meter**: `energy_generated` → `produced`, `energy_consumed` → `consumed`.
  - **EV Charger**: `energy_delivered_kwh` → `consumed`.
  - **Battery**: `power_kw` (charge/discharge) → mapped to `produced` or `consumed` based on mode.

- **PDA Derivation**:
  - `oracle_data` PDA: `seeds = [b"oracle_data"]`
  - `meter_state` PDA: `seeds = [b"meter", <device_serial_no_hyphens>]`

---

## Configuration

| Env Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection string |
| `SOLANA_RPC_URL` | `http://127.0.0.1:8899` | Solana RPC endpoint |
| `IOT_GATEWAY_PORT` | `4010` | Port for the HTTP ingestion API |
| `AUTHORITY_WALLET_PATH` | `gridtokenx-api/dev-wallet.json` | Path to transaction signer keypair |

---

## Verification

### Local Testing
```bash
# Start the service
cargo run --package gridtokenx-oracle-bridge

# Ingest a meter reading
curl -X POST http://localhost:4010/api/v1/ingest/smart-meter \
     -H "Content-Type: application/json" \
     -d '{"device_id": "M101", "energy_generated": 5.0, "energy_consumed": 1.0}'
```

---

## Security Considerations

1. **Private Key**: Loaded from file (`dev-wallet.json`). In production, use a Secret Manager (e.g., AWS Secrets Manager, HashiCorp Vault).
2. **RPC Protocol**: Currently HTTP. Production should use HTTPS with a private RPC provider.
3. **Network Isolation**: No HTTP ports exposed. Communicates only via Redis (inbound) and Solana RPC (outbound).
4. **Hot Wallet**: The authority wallet should hold minimal SOL for transaction fees only.
