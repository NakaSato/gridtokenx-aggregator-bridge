# Aggregator Bridge — Ingest Sig-Verify Benchmarks

Canonical performance results for the **off-chain telemetry ingest path**:
`POST /v1/private-network/ingest` → mTLS terminate → AES-256-GCM decrypt →
Ed25519 signature verify → per-meter replay-counter check-and-bump → zone Redis
Stream `XADD`. This is the "no data loss" path behind the platform's AMI claims.

> Distinct from the anchor repo's `BENCHMARKS.md §9` (Oracle
> `submit_meter_reading`, **on-chain**). That path has no per-reading Ed25519
> verify — it is gateway-payer-authorized. This doc is the **off-chain** verify path.

Provenance: Apple M2, 8 cores, 16 GiB; bridge `864bc7b` (main); localnet single
node (bridge + Redis + Vault + IAM in Docker/OrbStack, load-gen native on same
host); 2026-07-07.

---

## 1. Secure-mode ingest contract (how a reading reaches 202)

Bridge runs `AGGREGATOR_REQUIRE_SECURE=true`. A reading is **202 Accepted**
(`handlers.rs:1126`) — meaning signature verified (`handlers.rs:196,479`) and
disseminated — only when every layer below passes. Each was root-caused in order:

| Layer | Failure if absent | Fix |
|:--|:--|:--|
| **mTLS** (host `:4030` → container `:4010`) | TLS alert / handshake fail | present client cert + dev CA (`infra/certs/clients/smartmeter-simulator.{crt,key}`, `ca.crt`) |
| **Encrypted frame** `protocol="dlms-enc"` | **426** Upgrade Required (`handlers.rs:83,898`) — secure mode refuses plaintext `dlms` | AES-256-GCM envelope `{counter,nonce,ciphertext}`, AAD=`device_id:counter` (`handlers.rs:230`) |
| **enckey in Redis** `gridtokenx:devices:{id}:enckey` | **400** "no AES key" (`handlers.rs:289`) — bridge READS the key, does not re-derive | seed via `register_enckeys_redis` |
| **X-API-KEY** header | **401** | `AGGREGATOR_API_KEY` |
| **Strictly-increasing counter** per meter (Redis high-water, persists across runs) | **409** — replay guard (`crates/aggregator-persistence/src/infra/crypto.rs:752,1138`) | base counters on epoch-ms; **1 frame/meter** when firing concurrently (concurrent multi-counter/meter reorders → 409 storm) |

> The repo's `just auto-meter-send` / `just bench-ingest` recipes point at
> `http://` and predate secure+mTLS mode → they 426 against this bridge. The
> benchmarks below use standalone drivers that satisfy the full contract.

---

## 2. Results (all 0 loss except host oversubscription)

### 2a. Closed-loop fleet sweep — throughput vs fleet size

Concurrent signed `dlms-enc` sends (semaphore 64), interval=0 (max offered).

| meters | readings | throughput r/s | loss |
|-------:|---------:|---------------:|-----:|
| 80 | 3,120 | 203.6 | 0 |
| 160 | 3,040 | 202.0 | 0 |
| 320 | 3,200 | 198.6 | 0 |
| 640 | 3,200 | 206.9 | 0 |
| 1,280 | 3,840 | 211.0 | 0 |
| 2,000 | 4,000 | 186.2 | 0 |
| 4,000 | 4,000 | 195.7 | 0 |
| 8,000 | 8,000 | 193.1 | 0 |

**Throughput is flat ~200 r/s across a 100× fleet range** — independent of meter
count (per-meter PDAs/keys are disjoint; the limit is elsewhere).

### 2b. Isolating the ceiling — 6 harnesses converge on two limits

| harness | result | conclusion |
|:--|:--|:--|
| open-loop, 1 proc, ramp → 3200 r/s | caps ~200 r/s; backlog → latency, **0 loss** | 1 process cannot offer >~200 r/s |
| pre-signed, 1 proc (crypto pre-paid) | 203 r/s | ~200 r/s is **not** crypto — it is per-process event-loop/mTLS |
| multi-proc **live-crypto** K=8 | 399 r/s | ≥2 procs needed to pass 200 |
| multi-proc **pre-signed** K=8 | 390 r/s | same as live → client crypto **not** the shared limit → **bridge-bound** |
| K=16 (either) | 133 r/s + **2.8% loss** | oversubscription thrash (16 procs > 8 cores); losses are connection-level (ConnectError/timeout/RemoteProtocolError), **never** bridge 5xx |

**Two ceilings, both pinned:**
1. **~200 verified r/s per client process** — event-loop + mTLS record handling,
   crypto-independent (pre-signed hits the same wall).
2. **~400 verified r/s bridge-bound** on this host — reached at K≥2, unchanged by
   lighter clients. The bridge's own per-reading cost dominates: mTLS terminate +
   Ed25519 verify + AES-GCM decrypt + **atomic Redis counter check-bump**
   (`crypto.rs:752` — a single-threaded-Redis serialization point) + `XADD`.

---

## 3. Reading

- **0 loss under every realistic condition.** The only losses observed were K=16
  host oversubscription, and they were connection-level, not bridge rejections.
- **Fleet size is not the limiter** — flat ~200 r/s from 80 to 8,000 meters
  (mirrors, on the off-chain side, the on-chain settlement's global-write
  serialization: throughput flat despite disjoint hot-path state).
- **Enormous headroom vs real AMI cadence:** 80 meters at 15 s = 5.3 r/s; even
  ~3,000 meters at 15 s ≈ 200 r/s. The bench's original 80-meter compression
  scenario runs ~75× under the bridge ceiling.
- **The true bridge ceiling above ~400 r/s is not measurable co-located** — the
  native load generators and the bridge share one 8-core host, so pushing past
  ~400 r/s only oversubscribes cores. Isolating it needs a second load-gen host,
  or a multi-core / horizontally-scaled bridge deployment. Stated as a measurement
  limit, not a code limit.
- To exceed ~400 r/s: more bridge CPU, pipeline the Redis counter guard, or run
  multiple bridge instances behind the IoT gateway — none is a correctness change.

---

## 4. Reproduce

Infra up (`just orb-up` — bridge + Redis + Vault; validator NOT required — it only
feeds the downstream surplus-mint outbox, not ingest verify). Drivers satisfy the
§1 contract (mTLS certs, `dlms-enc`, `register_enckeys_redis`, monotonic counter).
Harnesses (session scratchpad, not committed): `enc_stream.py` (closed-loop),
`openloop.py` (open-loop + shard), `precomp.py` (pre-signed + shard), with
`sweep.sh` / `mp.sh` / `mp_precomp.sh` wrappers. Bridge-side truth = count of
`✅ Telemetry signature verified (REST)` log lines (`handlers.rs:196,479`); Redis
`XLEN` under-counts (zone streams cap at `REDIS_STREAM_MAXLEN`).
