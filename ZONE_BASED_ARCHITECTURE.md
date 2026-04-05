# Oracle Bridge Zone-Based Microgrid Architecture

## Problem

Current Oracle Bridge throughput: **~200-280 req/s**

The bottleneck is:
1. **Serial Redis stream processing** - Single consumer group processes all readings sequentially
2. **Per-reading gRPC forwarding** - Each meter reading triggers an individual gRPC call to API Gateway
3. **No zone partitioning** - All readings compete for the same Redis stream and consumer

## Solution: Zone-Based Microgrid Parallelization

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│              IoT Gateway (Port 4010)                            │
│  HTTP Ingestion → Zone Router → Zone Streams                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │ Zone 0      │  │ Zone 1      │  │ Zone N      │
   │ Stream      │  │ Stream      │  │ Stream      │
   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
          │                │                │
          ▼                ▼                ▼
   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │ Worker 0    │  │ Worker 1    │  │ Worker N    │
   │ (Parallel)  │  │ (Parallel)  │  │ (Parallel)  │
   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
          │                │                │
          └────────────────┼────────────────┘
                           ▼
                  ┌─────────────────┐
                  │  Batch Forward  │
                  │  to API Gateway │
                  └────────┬────────┘
                           ▼
                  ┌─────────────────┐
                  │  API Gateway    │
                  │  (Port 4000)    │
                  └─────────────────┘
```

### Key Improvements

#### 1. Zone-Partitioned Redis Streams

**Before:**
```
gridtokenx:events:v1  ← All readings
```

**After:**
```
gridtokenx:events:zone_0  ← Zone 0 meters
gridtokenx:events:zone_1  ← Zone 1 meters
...
gridtokenx:events:zone_9  ← Zone 9 meters
```

**Routing Logic:**
```rust
fn get_zone_index(zone_id: Option<i32>, meter_serial: &str) -> usize {
    match zone_id {
        Some(zid) if zid >= 0 && (zid as usize) < NUM_ZONES => zid as usize,
        _ => hash(meter_serial) % NUM_ZONES  // Consistent hashing
    }
}
```

#### 2. Parallel Zone Workers

**Before:**
```rust
// Single ingester processes all readings serially
loop {
    let messages = read_from_stream();
    for msg in messages {
        process(msg).await;  // Sequential
    }
}
```

**After:**
```rust
// Spawn worker per zone
for (idx, stream) in zone_streams.iter().enumerate() {
    tokio::spawn(async move {
        process_zone_stream(stream, idx).await
    });
}
```

**Expected Throughput:** 10 zones × 200 req/s = **2,000 req/s**

#### 3. Batch Forwarding to API Gateway

**Before:**
```rust
// One gRPC call per reading
for reading in readings {
    forward_to_gateway(reading).await;  // 1 gRPC call
}
```

**After:**
```rust
// Batch 50 readings per gRPC call
let mut batch = Vec::with_capacity(BATCH_SIZE);
for reading in readings {
    batch.push(reading);
    if batch.len() >= BATCH_SIZE {
        forward_batch_to_gateway(batch).await;  // 1 gRPC call for 50 readings
        batch.clear();
    }
}
```

**Expected Improvement:** 50× reduction in gRPC overhead

### Implementation Files

#### `src/ingester/zone_ingester.rs` (New)
```rust
pub struct ZoneEventIngester {
    connection_manager: ConnectionManager,
    platform_client: PlatformClient,
    metrics: Arc<Metrics>,
    zone_streams: Vec<String>,  // ["gridtokenx:events:zone_0", ...]
    group_name: String,
    consumer_name: String,
}

impl ZoneEventIngester {
    pub async fn new(...) -> Result<Self>;
    pub async fn run(self: Arc<Self>) -> Result<()>;
    async fn process_zone_stream(...) -> Result<()>;
}
```

#### `src/router.rs` (Modified)
```rust
// Route to zone-specific stream instead of device-type stream
pub async fn disseminate(&self, reading: &DeviceReading) -> Result<String> {
    let zone_idx = Self::get_zone_index(reading);
    let stream_name = format!("gridtokenx:events:zone_{}", zone_idx);
    // ... publish to stream
}
```

#### `src/main.rs` (Modified)
```rust
// Initialize zone-based ingester
let zone_ingester = Arc::new(ZoneEventIngester::new(
    &redis_url,
    &api_gateway_url,
    metrics.clone()
).await?);

// Run with IoT gateway
tokio::select! {
    result = zone_ingester.run() => { ... }
    result = axum::serve(listener, app) => { ... }
}
```

### Performance Targets

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| Throughput | 280 req/s | 2,000+ req/s | **7×** |
| gRPC Calls/sec | 280 | 40 (with batching) | **7× reduction** |
| P95 Latency | 240 ms | <100 ms | **2.4× faster** |
| Redis Stream Contention | High | None (partitioned) | **Eliminated** |
| **Batch Efficiency** | 1 reading/call | 50 readings/call | **50× fewer RPC calls** |

### Configuration

```bash
# Number of zone partitions (default: 10)
NUM_ZONES=10

# Batch size for forwarding (default: 50)
FORWARD_BATCH_SIZE=50

# Batch timeout in ms (default: 100)
BATCH_TIMEOUT_MS=100

# Max concurrent processing per zone (default: 50)
ZONE_SEMAPHORE_SIZE=50
```

### Batch Forwarding Implementation

The `BatchForwarder` struct manages automatic batching:

```rust
pub struct BatchForwarder {
    platform_client: PlatformClient,
    batch: Vec<TelemetryRequest>,
    batch_size: usize,        // FORWARD_BATCH_SIZE = 50
    last_flush: Instant,
    timeout: Duration,        // BATCH_TIMEOUT_MS = 100ms
    metrics: Arc<Metrics>,
}

impl BatchForwarder {
    /// Add a reading to the batch, auto-flush if full or timeout
    pub async fn add(&mut self, req: TelemetryRequest) -> Result<()> {
        self.batch.push(req);
        
        if self.batch.len() >= self.batch_size || 
           self.last_flush.elapsed() >= self.timeout {
            self.flush().await?;
        }
        Ok(())
    }
}
```

**Benefits:**
- **Reduced gRPC overhead**: 50 readings in 1 call instead of 50 calls
- **Lower API Gateway load**: Fewer HTTP/2 streams to manage
- **Better throughput**: Amortized connection setup cost
- **Bounded latency**: 100ms timeout ensures readings aren't delayed

### Monitoring

New Grafana dashboard panels:

1. **Throughput by Zone** - Show req/s per zone partition
2. **Worker Utilization** - Parallel worker activity
3. **Batch Efficiency** - Average batch size vs target
4. **Zone Distribution** - Readings per zone (load balancing)

### Migration Path

1. **Phase 1:** Deploy zone-partitioned router (backward compatible)
   - New readings go to zone streams
   - Old ingester reads from all zone streams + legacy stream

2. **Phase 2:** Deploy parallel zone workers
   - Each worker processes one zone stream
   - Monitor throughput improvement

3. **Phase 3:** Enable batch forwarding
   - Collect readings into batches before forwarding
   - Tune batch size for optimal latency/throughput

### Rollback Plan

If issues occur:
```bash
# Revert to single stream processing
export USE_ZONE_INGESTER=false
docker-compose restart oracle-bridge
```

## Conclusion

Zone-based microgrid parallelization provides a **7× throughput improvement** (280 → 2,000+ req/s) by:
- Eliminating Redis stream contention through partitioning
- Enabling true parallel processing with zone workers
- Reducing gRPC overhead with intelligent batching

This architecture scales linearly - adding more zones (e.g., 20 instead of 10) would theoretically double throughput to 4,000+ req/s.
