# GridTokenX Technical Specification: Protocol v4 (UTT-S+)

## 1. Overview
The **Unified Trusted Telemetry - Security Plus (UTT-S+)** protocol is an industrial-grade binary specification for secure energy data transmission. It is designed to be transport-agnostic, future-proof, and PDPA-compliant.

## 2. Frame Architecture
A Protocol v4 frame consists of a plaintext header, an authenticated encrypted block, and an integrity trailer.

### 2.1 Frame Map
| Field | Offset | Length | Format | Visibility |
| :--- | :--- | :--- | :--- | :--- |
| **Version** | 0 | 1 byte | `0x04` | Plaintext |
| **Total Length** | 1 | 1 byte | `u8` | Plaintext |
| **Manufacturer ID** | 2 | 3 bytes | ASCII | Plaintext |
| **Logical Device Name** | 5 | 8 bytes | ASCII (Null-padded) | Plaintext |
| **Timestamp** | 13 | 8 bytes | `u64 BE` (Unix s) | Plaintext |
| **Ciphertext** | 21 | Variable | **AES-256-GCM** | Encrypted |
| **GCM Auth Tag** | Variable| 16 bytes | Cryptographic MAC | Plaintext |
| **CRC-32 Checksum** | -4 | 4 bytes | `u32 BE` | Plaintext |

---

## 3. Cryptography & Privacy

### 3.1 Encryption (AES-256-GCM)
Metrics are protected using **AES-256-GCM** authenticated encryption to ensure both confidentiality (PDPA) and authenticity.

- **Key:** Unique 256-bit key per device.
- **Nonce (96-bit):** Derived deterministically from header fields to prevent reuse:
  `[Manuf ID (3b)] + [Timestamp (8b)] + [Version (1b)]`
- **AAD (Additional Authenticated Data):** The plaintext header (Bytes 0-21) is used as AAD for the GCM cipher.

### 3.2 Hardware Signing (Ed25519)
The outer gRPC wrapper contains an **Ed25519** signature generated over the canonical string:
`"{meter_id}:{kwh}:{timestamp_ms}:{sequence}"`

---

## 4. Decrypted Data: TLV Dictionary
Once decrypted, the payload contains a stream of **Tag-Length-Value** blocks. Parsers MUST skip unknown tags using the `Length` field.

| Tag | Name | Length | Unit / Format |
| :--- | :--- | :--- | :--- |
| `0x01` | Active Energy Import (+A) | 8 bytes | `u64 BE` (Watt-hours) |
| `0x02` | Active Energy Export (-A) | 8 bytes | `u64 BE` (Watt-hours) |
| `0x03` | L1 Voltage | 4 bytes | `u32 BE` (Centi-volts, 0.01V) |
| `0x04` | L1 Current | 4 bytes | `u32 BE` (Milli-amps, 1mA) |
| `0x05` | Battery SoC | 4 bytes | `u32 BE` (Basis Points, 0.01%) |

---

## 5. Integrity & Validation (Step-by-Step)

1. **Checksum Check:** Calculate **CRC-32** of the entire frame (excluding the last 4 bytes). If mismatches, discard.
2. **Framing:** Use **Total Length** to identify frame boundaries in stream-based buffers.
3. **Decryption:** Extract **Manuf ID**, **Timestamp**, and **Version** to reconstruct the **Nonce**. Decrypt the ciphertext using the device key and verify the **Auth Tag**.
4. **Parsing:** Iterate through the decrypted **TLV** stream to extract operational metrics.

---

## 6. Implementation Example (Pseudocode)
```python
# Nonce Construction
nonce = frame[2:5] + frame[13:21] + frame[0:1]

# Decryption
plaintext = aes_gcm_decrypt(
    key=device_key, 
    nonce=nonce, 
    ciphertext=frame[21:-20], # Excludes Tag(16b) and CRC(4b)
    tag=frame[-20:-4],
    aad=frame[0:21]
)
```
