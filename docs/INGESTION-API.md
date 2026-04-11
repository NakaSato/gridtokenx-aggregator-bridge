# GridTokenX Ingestion API Reference

This document provides a detailed reference for Edge Gateway developers to securely ingest telemetry into the GridTokenX Oracle Bridge.

## 1. Authentication & Security Policy

The Oracle Bridge implements a **Zero-Trust Ingestion** model. Every reading must be cryptographically signed by the source device using **Ed25519**.

### 1.1 Signature Standard
- **Algorithm**: Ed25519
- **Encoding**: Base58 (for the signature string)
- **Identity Storage**: Public keys must be pre-registered in the platform's Redis registry (`gridtokenx:devices:{meter_id}:pubkey`).

### 1.2 Canonical Signing Format
To ensure consistency across languages, the signature must be generated over a canonical string representation of the reading:

**Format:** `{meter_id}:{kwh}:{timestamp_ms}`

- `meter_id`: The unique device identifier (e.g., `0x0001`).
- `kwh`: The energy value formatted as a string (e.g., `99.99`). For multi-variable payloads, use `energy_consumed` or `energy_generated`.
- `timestamp_ms`: Unix timestamp in milliseconds.

---

## 2. gRPC Ingestion (Standard)

Standard ingestion path for low-latency, high-throughput telemetry.

### Endpoints
- Port: `50051` (Default)
- Service: `OracleService`

### Message: `TelemetryRequest`
```protobuf
message TelemetryRequest {
  string reading_id = 1;      // UUID
  string meter_id = 2;        // Unique Device ID
  string meter_serial = 3;    // Serial Number
  string kwh = 7;             // Reading Value (string format)
  int64 timestamp = 14;       // Unix MS
  optional string signature = 16; // Base58 Signature
}
```

---

## 3. REST Ingestion (Fallback)

Used for environments behind restrictive firewalls or for simpler IoT device integrations.

### Single Ingestion
- **POST** `/v1/private-network/ingest`
- **Port**: `4010`

**Request Body:**
```json
{
  "protocol": "dlms",
  "device_id": "0x0001",
  "payload": {
    "device_id": "0x0001",
    "timestamp": "2024-04-08T16:00:00.000Z",
    "energy_consumed": 123.45,
    "signature": "3y9S..."
  }
}
```

### Batch Ingestion (Optimized)
- **POST** `/v1/private-network/ingest/batch`

**Request Body:**
```json
{
  "protocol": "dlms",
  "readings": [
    {
      "device_id": "0x0001",
      "timestamp": "2024-04-08T16:00:00.000Z",
      "energy_consumed": 123.45,
      "signature": "3y9S..."
    },
    ...
  ]
}
```

---

## 4. Implementation Example (Python)

```python
import base58
from ed25519 import SigningKey

def sign_telemetry(private_key_hex, meter_id, kwh, timestamp_ms):
    # Construct Canonical Payload
    message = f"{meter_id}:{kwh}:{timestamp_ms}".encode('utf-8')
    
    # Sign
    sk = SigningKey(bytes.fromhex(private_key_hex))
    signature = sk.sign(message)
    
    return base58.b58encode(signature).decode('utf-8')
```

## 5. Troubleshooting

- **401 Unauthorized**: Public key not found for the `meter_id` in Redis. Verify registration.
- **403 Forbidden**: Signature mismatch. Check the canonical format and timestamp precision (milliseconds).
- **Latency**: Ensure connection pooling is enabled on the client side for gRPC.
