# Protocol v4 (UTT-S+) Binary Map

## Frame Structure
| Offset | Size | Field | Description |
| :--- | :--- | :--- | :--- |
| 0 | 1b | **Version** | `0x04` for Secure UTT-S+ |
| 1 | 1b | **Total Length** | Bytes following this field (Header + Ciphertext + CRC) |
| 2 | 3b | **Manuf ID** | Registered manufacturer code (e.g., `INC`) |
| 5 | 8b | **Device Name** | Unique hardware LDN (Null-padded) |
| 13 | 8b | **Timestamp** | Unix Epoch seconds (Big-Endian) |
| 21 | Var | **Encrypted Data** | AES-256-GCM ciphertext (TLVs + 16b Auth Tag) |
| -4 | 4b | **Checksum** | CRC-32 of all preceding bytes |

## Nonce Derivation
`Nonce = [Manuf ID (3b)] + [Timestamp (8b)] + [Version (1b)]`

## TLV Dictionary
| Tag | Len | Metric | Unit |
| :--- | :--- | :--- | :--- |
| `0x01` | 8b | Active Energy Import | Watt-hours (Wh) |
| `0x02` | 8b | Active Energy Export | Watt-hours (Wh) |
| `0x03` | 4b | L1 Voltage | Centi-volts (0.01V) |
| `0x04` | 4b | L1 Current | Milli-amps (1mA) |
| `0x05` | 4b | Battery SoC | Basis Points (0.01%) |
