# GridTokenX Ingestion API Reference (UTT v4)

This document provides a detailed reference for Edge Gateway developers to securely ingest telemetry into the GridTokenX Oracle Bridge using the **Unified Trusted Telemetry (UTT)** model.

## 1. Authentication & Security Policy (UTT-H)

The Oracle Bridge implements a **Zero-Trust Ingestion** model. Every reading must be cryptographically signed by the source device using **Ed25519** and follow the **UTT-H** integrity standards.

### 1.1 Signature Standard
- **Algorithm**: Ed25519
- **Encoding**: Base58
- **Identity Storage**: Public keys must be pre-registered in the platform's Redis registry (`gridtokenx:devices:{meter_id}:pubkey`).

### 1.2 Canonical Signing Format (Hardened)
To prevent replay attacks, the signature must be generated over a hardened canonical string:

**Format:** `{meter_id}:{kwh}:{timestamp_ms}:{sequence}`

- `meter_id`: The unique device identifier (UUID).
- `kwh`: The energy value formatted as a string (e.g., `99.99`).
- `timestamp_ms`: Unix timestamp in milliseconds.
- `sequence`: An incrementing counter managed by the device.

---

## 2. gRPC Ingestion (Unified Path)

Standard ingestion path for both real-time grid operations (Path A) and blockchain settlement (Path B).

### Endpoints
- Port: `50051` (Default)
- Service: `OracleService`
- Method: `Ingest`

### Message: `MeterReading`
```protobuf
message MeterReading {
  string reading_id = 1;      // UUID
  string meter_id = 2;        // Unique Device ID (UUID)
  string meter_serial = 3;    // Serial Number
  string kwh = 7;             // Reading Value (string format)
  int64 timestamp = 14;       // Unix seconds
  bytes raw_payload = 15;     // Secure DLMS-lite v4 (Encrypted + CRC)
  optional string signature = 16; // Base58 Signature (UTT-H)
}
```

---

## 3. Binary Payload (Secure DLMS-lite v4)
Operational data should be packed into the `raw_payload` field. 
- **Encryption**: AES-256-GCM.
- **Integrity**: CRC-32 checksum.
- **Framing**: Total Length prefix.

Refer to `INGESTION-PROTOCOL-V4.md` for the full binary specification.

---

## 4. Implementation Example (Python)

```python
import base58
from ed25519 import SigningKey

def sign_telemetry(private_key_hex, meter_id, kwh, timestamp_ms, sequence):
    # Construct Hardened Canonical Payload
    message = f"{meter_id}:{kwh}:{timestamp_ms}:{sequence}".encode('utf-8')
    
    # Sign
    sk = SigningKey(bytes.fromhex(private_key_hex))
    signature = sk.sign(message)
    
    return base58.b58encode(signature).decode('utf-8')
```

## 5. Troubleshooting

- **PERMISSION_DENIED**: Invalid UTT-H signature or expired/replayed sequence number.
- **INVALID_ARGUMENT**: Missing signature in production or malformed payload version.
- **UNAVAILABLE**: Bridge scaling or network timeout.
