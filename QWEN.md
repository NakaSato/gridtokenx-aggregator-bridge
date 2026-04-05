# GridTokenX Oracle Bridge - Project Context

## Project Overview

The **GridTokenX Oracle Bridge** is a high-performance Rust-based ingestion and convergence layer for the GridTokenX VPP (Virtual Power Plant) platform. It serves as the critical trust boundary between physical grid assets (smart meters, EV chargers, BESS, solar PV) and the blockchain settlement layer on HyperEVM.

### Core Responsibilities

1. **Path A (Operational Telemetry)**: Real-time ingestion and routing of energy telemetry to the VPP optimization platform for sub-second dispatch and forecasting.
2. **Path B (Settlement Attestation)**: Cryptographic verification of hardware-signed attestations, ZK-rollup aggregation (Plonky2), and relay to HyperEVM for trustless settlement.

### Key Features

- **Zone-Based Parallel Processing**: 10 zone partitions for horizontal scaling via Redis Streams consumer groups
- **Batch Forwarding**: Batches 50 telemetry readings per gRPC call to reduce overhead (target: 2,000+ req/s throughput)
- **Multi-Protocol Support**: DLMS/COSEM, OCPP 2.0.1, SunSpec Modbus, OpenADR via protocol adapter stacks
- **Hardware Root-of-Trust**: Ed25519 signature verification for ATECC608B secure element attestations
- **Privacy-by-Design**: ZK-rollup aggregation ensures only proofs (not raw data) reach the blockchain (PDPA compliant)

---

## Architecture

### High-Level Data Flow

```
Edge Devices → IoT Gateway (Port 4010) → Redis Streams (Zone-Partitioned)
                                           ↓
                              ZoneEventIngester (10 parallel workers)
                                           ↓
                              BatchForwarder (50 readings / 100ms)
                                           ↓
                              ConnectRPC → API Gateway → VPP Platform / HyperEVM
```

### Source Structure

```
src/
├── main.rs                    # Entry point, service orchestration
├── handlers.rs                # Axum HTTP handlers for IoT Gateway
├── router.rs                  # Device-type routing logic
├── state.rs                   # Shared AppState (Arc-injected)
├── auth.rs                    # API key / IAM gRPC authentication
├── models.rs                  # Device payload models and serialization
│
├── ingester/
│   ├── zone_ingester.rs       # Zone-based parallel processing (10 zones)
│   ├── batcher.rs             # Batch forwarding logic (50 readings / 100ms)
│   └── mod.rs                 # Event models (MeterReadingPayload, Event enum)
│
├── aggregator/
│   ├── mod.rs                 # Local statistics aggregation
│   ├── attestation.rs         # OracleSigner for Path B
│   └── attestation_service.rs # Background attestation processing
│
├── protocol/
│   ├── smart_meter.rs         # DLMS/COSEM adapter
│   ├── ev_charger.rs          # OCPP 2.0.1 adapter
│   ├── battery.rs             # SunSpec/BMS adapter
│   └── stacks/
│       ├── ocpp.rs            # OCPP protocol stack
│       ├── sunspec.rs         # SunSpec Modbus stack
│       ├── dlms.rs            # DLMS/COSEM stack
│       └── openadr.rs         # OpenADR 3.0 stack
│
├── infra/
│   └── platform/
│       ├── client.rs          # ConnectRPC client (OracleServiceClient)
│       └── mod.rs
│
├── nilm/                      # Non-Intrusive Load Monitoring (edge ML)
│   ├── engine.rs
│   ├── models.rs
│   └── mod.rs
│
├── storage/                   # SQLite circular buffer (offline resilience)
│   ├── circular_buffer.rs
│   ├── sync_manager.rs
│   └── mod.rs
│
└── metrics/                   # Prometheus metrics
    └── mod.rs
```

### Key Components

| Component | Role | Configuration |
|-----------|------|---------------|
| `ZoneEventIngester` | Parallel zone-based stream processing | `NUM_ZONES = 10`, `ZONE_SEMAPHORE_SIZE = 50` |
| `BatchForwarder` | Batches telemetry before gRPC submission | `FORWARD_BATCH_SIZE = 50`, `BATCH_TIMEOUT_MS = 100` |
| `PlatformClient` | ConnectRPC client to API Gateway | Uses `OracleServiceClient` |
| `OracleSigner` | Path B attestation signing | Ed25519, Plonky2 ZK-proofs |

---

## Building and Running

### Prerequisites

- Rust 1.88+ (edition 2021)
- Protobuf compiler (`protobuf-compiler`)
- Redis 7+ (for streaming)
- Access to API Gateway (gridtokenx-api) and IAM Service (gridtokenx-iam-service)
- **OrbStack** (required Docker runtime for GridTokenX development)

### Build Commands

```bash
# Build (generates protobuf code via build.rs)
cargo build

# Build release
cargo build --release

# Run with environment
cp .env.example .env  # Configure Redis, API Gateway, IAM URLs
cargo run

# Run with logging
RUST_LOG=gridtokenx_oracle_bridge=debug cargo run
```

### Docker Deployment

```bash
# Build image (multi-stage, ~50MB final)
docker build -t gridtokenx-oracle-bridge .

# Run with environment variables
docker run -d \
  -p 4010:4010 \
  -e REDIS_URL=redis://redis:6379 \
  -e API_GATEWAY_URL=http://api-gateway:4000 \
  -e IAM_SERVICE_URL=http://iam:50051 \
  -e GRIDTOKENX_API_KEYS=key1,key2,key3 \
  gridtokenx-oracle-bridge
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL |
| `API_GATEWAY_URL` | `http://127.0.0.1:4000` | API Gateway ConnectRPC endpoint |
| `IAM_SERVICE_URL` | `http://127.0.0.1:50051` | IAM gRPC service for auth |
| `IOT_GATEWAY_PORT` | `4010` | HTTP port for device ingestion |
| `GRIDTOKENX_API_KEYS` | (empty) | Comma-separated API keys for device auth |
| `RUST_LOG` | `info` | Log level (trace/debug/info/warn/error) |

---

## API Endpoints

### IoT Gateway (HTTP/JSON)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |
| `/api/v1/ingest/smart-meter` | POST | Smart meter telemetry |
| `/api/v1/ingest/ev-charger` | POST | EV charger session data |
| `/api/v1/ingest/battery` | POST | BESS state updates |
| `/api/v1/ingest` | POST | Auto-detect device type |
| `/api/v1/private-network/ingest` | POST | Protocol stack (OCPP/SunSpec/DLMS) |

### Example Payload (Smart Meter)

```json
{
  "device_id": "MTR-001",
  "serial_number": "SN-123456",
  "zone_id": 3,
  "energy_generated": 10.5,
  "energy_consumed": 2.1,
  "voltage": 230.5,
  "current": 12.3,
  "timestamp": 1711900800
}
```

---

## Testing

### Test Commands

```bash
# Run unit tests
cargo test

# Run with logging
RUST_LOG=debug cargo test -- --nocapture

# Load test (requires Python + smartmeter-simulator)
cd ../gridtokenx-smartmeter-simulator
uv run python test_oracle_bridge_load.py
```

### Performance Targets

| Metric | Baseline | Target | Current |
|--------|----------|--------|---------|
| Throughput | 280 req/s | 2,000+ req/s | Zone+Batch enabled |
| P95 Latency | 240ms | <100ms | - |
| P99 Latency | - | <500ms | - |
| Batch Efficiency | 1 reading/call | 50 readings/call | ✅ |
| Memory (load) | 200MB | <500MB | - |

### Test Scenarios

See [TEST_PLAN.md](TEST_PLAN.md) for detailed test scenarios:
1. Zone Distribution Test
2. Batch Efficiency Test
3. Throughput Stress Test (200k readings)
4. Latency Distribution Test
5. Batch Timeout Test
6. Error Recovery Test
7. Memory Leak Test (500k readings)

---

## Development Conventions

### Code Style

- **Error Handling**: Use `anyhow::Result` for application logic, `thiserror` for library errors
- **Async**: Tokio runtime with `#[tokio::main]`, `Arc<T>` for shared state
- **Logging**: `tracing` crate with structured JSON logs
- **Serialization**: `serde` with `rust_decimal` for precise energy values

### Architecture Patterns

1. **Dependency Injection**: All services injected via `AppState` (Arc-cloned into handlers)
2. **Zone Partitioning**: Readings hashed to zones for parallel processing
3. **Reliable Delivery**: Redis Streams with consumer groups + XACK after successful gRPC
4. **Batching**: Auto-flush on batch full (50) OR timeout (100ms)

### Metrics

Prometheus metrics exported at `/metrics`:
- `total_requests`: Total ingestion requests
- `authorized_requests`: Successfully authenticated
- `failed_requests`: Auth/validation failures
- `on_chain_syncs`: Successful blockchain submissions
- `batch_forward_*`: Batch size, latency, timeout flushes

---

## Related Documentation

| Document | Description |
|----------|-------------|
| [implement.md](implement.md) | Full technical system design (Source of Truth) |
| [plan.md](plan.md) | Edge device architecture & ZK-rollup specs |
| [ZONE_BASED_ARCHITECTURE.md](ZONE_BASED_ARCHITECTURE.md) | Zone partitioning design |
| [BATCH_FORWARDING_IMPLEMENTATION.md](BATCH_FORWARDING_IMPLEMENTATION.md) | Batch forwarding implementation details |
| [TEST_PLAN.md](TEST_PLAN.md) | Comprehensive test scenarios |
| [docs/core.md](docs/core.md) | Architecture diagrams & matrices |
| [docs/DATA-FLOW.md](docs/DATA-FLOW.md) | Path A (Telemetry) & Path B (Settlement) flows |

---

## Common Issues & Troubleshooting

### Redis Connection Failures

```
⚠️ Redis connection attempt 1 failed. Retrying...
```

**Solution**: Ensure Redis is running and accessible at `REDIS_URL`. Check network connectivity.

### gRPC Connection to API Gateway

```
❌ Platform telemetry ingestion failed: RPC error
```

**Solution**: Verify `API_GATEWAY_URL` is correct and API Gateway is running with ConnectRPC endpoints enabled.

### Consumer Group Conflicts

```
BUSYGROUP Consumer Group name already exists
```

**Solution**: This is expected on restart. The code handles this gracefully. To reset, delete the stream: `redis-cli DEL gridtokenx:events:zone_0`

### Batch Timeout Flushes

```
⏰ Background flush task started (interval: 100ms)
```

**Solution**: Normal behavior. Ensures low-latency delivery even under low load.

---

## Git & Workspace Context

This service is part of the larger GridTokenX platform monorepo:

```
gridtokenx-platform-infa/
├── gridtokenx-api/              # Primary API Gateway (Rust/Axum)
├── gridtokenx-iam-service/      # Identity & Access (gRPC)
├── gridtokenx-oracle-bridge/    # This service
├── gridtokenx-trading-service/  # Trading engine
├── gridtokenx-smartmeter-simulator/  # Test tooling
└── ...
```

Proto files reference sibling services:
- `../gridtokenx-iam-service/proto/identity.proto` (IAM gRPC)
- `proto/oracle.proto` (Oracle Bridge service definition)
