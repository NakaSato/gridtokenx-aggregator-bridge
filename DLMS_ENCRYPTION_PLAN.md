# Plan: Wire AES-256-GCM Decryption into the Binary DLMS Ingest Path

## The gap

The secure UTT-S+ v4 binary frame ([PROTOCOL.md](PROTOCOL.md)) is **AES-256-GCM encrypted**, and
`DlmsBinaryFrame::parse(payload, encryption_key)` already implements decryption
(`crates/aggregator-stacks/src/binary_decoder.rs:79`). But every production caller passes `None`:

| Call site | Current |
| --- | --- |
| `crates/aggregator-api/src/grpc/service.rs:73` (`bulk_raw_ingest`) | `parse(frame_bytes, None)` |
| `crates/aggregator-api/src/grpc/service.rs:230` (`ingest`) | `parse(&request.raw_payload, None)` |
| `crates/aggregator-api/src/grpc/service.rs:332` (`ingest_stream` / telemetry) | `parse(&tel.raw_payload, None)` |

`None` ⇒ plaintext fallback (`binary_decoder.rs:92`). **Encrypted frames cannot be decoded in
production.** The crypto exists but is unreachable — no per-device key is ever plumbed in.

## Design

Frame header (version, manuf ID, LDN, timestamp) is **plaintext** and precedes the encrypted TLV block
(`binary_decoder.rs:63-75`). So the resolve order is:

```
parse_header(bytes) → meter_id (LDN) → fetch device AES key from Redis → parse(bytes, Some(key))
```

This keeps the key lookup OUT of `aggregator-stacks` (which has no `persistence` dep — must stay that way
per the `core ← protocol ← stacks ← persistence ← logic ← api` direction). The decoder stays pure; the
Redis-backed key registry lives in `aggregator-persistence`, mirroring the existing `SignatureVerifier`.

### Key storage convention

Symmetric per-device key in Redis alongside the existing pubkey:

```
gridtokenx:devices:{meter_id}:pubkey   # existing — Ed25519 verify key
gridtokenx:devices:{meter_id}:enckey   # NEW — AES-256 key, 64-char hex (32 bytes)
```

### Production enforcement

Mirror signature policy: when `ENVIRONMENT=production`, a frame whose `enckey` is missing must be
**rejected loud** (fail-closed), not silently decoded as plaintext. Dev/legacy keeps the plaintext
fallback. Gate dev bypass behind the existing `SKIP_SIG_VERIFY`-style flag or a new `ALLOW_PLAINTEXT_DLMS`.

---

## Implementation checklist

### 1. Decoder — expose a header-only parse (`aggregator-stacks`) — ✅ DONE (commit `74d5943`)
- [x] Add `pub struct DlmsHeader { version, manufacturer_id, logical_device_name, timestamp }`.
- [x] Add `DlmsBinaryFrame::parse_header(payload: &[u8]) -> Result<DlmsHeader>` — runs CRC-32 + version
      check + plaintext header extraction only, **no decryption**. Note: CRC-32 is over the *whole* frame
      (`binary_decoder.rs:44-52`), so `parse_header` still validates the full payload, then reads only the
      plaintext header bytes (`:63-75`) and returns before the decrypt step.
- [x] **Min-size floor differs from `parse`.** `parse_header` floor = 25B (`ver1+totlen1+manuf3+ldn8+ts8 =
      21B` + 4B CRC). Latent bug fixed: the unconditional `len < 41` plaintext-reject guard removed from
      `parse` — plaintext frames no longer wrongly rejected.
- [x] Refactor `parse()` to call `parse_header` internally (no duplicated CRC/header logic). `parse` now
      destructures `parse_header(payload)?` then re-reads RAW bytes for the GCM nonce.
- [x] Keep `parse(payload, None)` plaintext behavior unchanged (back-compat for dev).

### 2. Key registry — fetch device AES key (`aggregator-persistence`) — ✅ DONE (commit `0bca93e`)
- [x] Add `DeviceKeyRegistry` in `infra/crypto.rs` holding a self-healing Redis connection — mirrors the
      `conn` / `invalidate` / `get_with_retry` / `mget_with_retry` pattern (separate struct, no regression
      risk to `SignatureVerifier`).
- [x] Method `get_device_aes_key(meter_id) -> Result<Option<[u8; 32]>>` reading
      `gridtokenx:devices:{meter_id}:enckey`, hex-decoding to 32 bytes; loud `Err` on Redis-unreachable
      (fail-closed, **not** silent `None`), `Ok(None)` only on genuinely-absent key.
- [x] Reject malformed key length with `Err` (`decode_aes_key_hex` — never truncate/pad).
- [x] **Bulk path: one MGET.** `get_device_aes_keys(meter_ids) -> Result<Vec<Option<[u8;32]>>>` via
      `mget_with_retry`, same round-trip shape as batch sig-verify. A malformed key in the batch ⇒ `None`
      for that entry (logged), so one bad key can't fail the whole batch; Redis-unreachable still fails loud.
      (Pubkey+enckey sharing a *single* MGET deferred to step 3 wiring — registries stay decoupled for now.)

### 3. Wire into gRPC ingest (`aggregator-api`) — ✅ DONE
- [x] Add `device_key_registry: Arc<DeviceKeyRegistry>` field to `AppState` next to
      `signature_verifier` (`state.rs`).
- [x] Construct it from `redis_url` next to `SignatureVerifier::new` (`src/main.rs`), and pass it
      into the `AppState { … }` literal next to `signature_verifier` (`src/main.rs`).
- [x] `service.rs` `bulk_raw_ingest`: now `self.decode_secure_frame(frame_bytes)` (`if let Some`).
- [x] `service.rs` `ingest`: `match self.decode_secure_frame(&request.raw_payload)` — `None` arm
      falls back to standard signed fields (sig gate already passed; same as legacy parse-error path).
- [x] `service.rs` `ingest_batch`: `self.decode_secure_frame(&tel.raw_payload)` (`if let Some`).
- [x] Factored into one private helper `AggregatorServiceImpl::decode_secure_frame` — all 3 sites
      share the resolve-then-decrypt + policy (no drift). Per-frame `get_device_aes_key` (one GET per
      frame, self-healing) — shared-MGET batching deferred (correctness first).

### 4. Production policy enforcement — ✅ DONE (folded into `decode_secure_frame`)
- [x] Reads `ENVIRONMENT` once per frame; production + missing/invalid `enckey` ⇒ `None` + `warn!`
      (frame skipped, fail-closed — never plaintext). Redis-unreachable / malformed key ⇒ `error!` +
      `None` (fail-closed-loud, the helper's `get_device_aes_key` `Err` arm).
- [x] Dev plaintext fallback gated behind new `ALLOW_PLAINTEXT_DLMS=true`; logged loud when used.

### 5. Docs
- [ ] [ARCHITECTURE.md](ARCHITECTURE.md) §3 ingestion pipeline: add the decryption step + key convention,
      with `binary_decoder.rs:line` citations.
- [ ] [CLAUDE.md](CLAUDE.md) "Real gap" note: flip to "encrypted DLMS wired; key at `…:enckey`".
- [ ] Update `.env.example` if a new flag (`ALLOW_PLAINTEXT_DLMS`) is added.

---

## Test checklist

### Decoder (`aggregator-stacks`, inline `#[cfg(test)]`) — ✅ DONE (11 tests green, commit `74d5943`)
- [x] `parse_header` extracts version/manuf/LDN/timestamp from a valid frame **without** a key.
      (`parse_header_extracts_fields_without_key`)
- [x] `parse_header` fails on CRC mismatch. (`parse_header_fails_on_crc_mismatch`)
- [x] `parse_header` fails on wrong version byte (≠ `0x04`). (`parse_header_fails_on_wrong_version`)
- [x] `parse(frame, Some(key))` round-trips an AES-256-GCM encrypted TLV frame.
      (`test_parse_v4_encrypted_frame`, `parse_decodes_all_tlv_fields`)
- [x] `parse(frame, Some(wrong_key))` ⇒ `Err` (GCM auth-tag failure), not garbage TLVs.
      (`parse_with_wrong_key_fails_gcm_auth`)
- [x] `parse(frame, None)` on a plaintext frame still decodes (back-compat).
      (`parse_plaintext_frame_back_compat`)
- [x] Header values from `parse_header` equal those from full `parse` for the same frame.
      (`parse_header_matches_full_parse`)
- [x] Extra guards: `parse_header_fails_when_too_small`,
      `parse_header_fails_when_declared_length_exceeds_payload`, `parse_header_trims_null_padded_ldn`.

### Key registry (`aggregator-persistence`, inline) — ✅ DONE (8 unit + 3 live = 11 green, commits `0bca93e`, `e51865a`)
- [x] `get_device_aes_key` with no Redis URL ⇒ loud `Err` (`get_aes_key_errors_loud_when_no_redis_url`);
      batch path too (`get_aes_keys_batch_errors_loud_when_no_redis_url`).
- [x] Malformed hex / wrong-length key ⇒ `Err` (`decode_aes_key_rejects_bad_hex`,
      `decode_aes_key_rejects_wrong_length`); 64-char hex ⇒ 32 bytes (`decode_aes_key_accepts_32_bytes`).
- [x] `#[ignore]` live test (verified against `gridtokenx-redis:7010`): missing key ⇒ `Ok(None)`, seeded
      key ⇒ `Ok(Some(32 bytes))` (`get_aes_key_against_real_redis`).
- [x] `#[ignore]` live batch: mixed seeded/malformed/absent ⇒ `[Some(32B), None, None]`
      (`get_aes_keys_batch_mixed_against_real_redis`).

### gRPC ingest integration (`aggregator-api`)
- [ ] Encrypted frame + seeded `enckey` ⇒ decrypted, disseminated, `processed_count == 1`.
- [ ] Encrypted frame + **missing** key under `ENVIRONMENT=production` ⇒ frame skipped, not processed.
- [ ] Encrypted frame + missing key in dev with plaintext flag off ⇒ skipped + warn.
- [ ] Plaintext frame in dev with flag on ⇒ processed.
- [ ] Bulk path: mixed batch (some keyed, some not) processes only the decryptable+verified frames.
- [ ] Decryption failure does **not** bypass the existing Ed25519 signature gate (both must pass).

### Regression guards
- [ ] Existing `verify_errors_loud_when_no_redis_url` / `_no_manager` still pass (no crypto coupling break).
- [ ] `cargo check` + `cargo test` green across all 6 crates.

---

## Open questions
1. Key provisioning — who writes `…:enckey` to Redis? (IAM device-registration flow? out of scope here.)
2. ~~AES key + pubkey share one MGET round-trip for bulk?~~ **Resolved: yes.** `mget_with_retry():100`
   already supports it — see step 2. Both keys fetched per meter in one batch round-trip.
3. Key rotation — any TTL / versioning on `…:enckey`, or static until re-provisioned?
