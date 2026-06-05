use anyhow::{bail, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use crc32fast::Hasher;
use aes_gcm::{Aes256Gcm, Key, Nonce, KeyInit, aead::Aead};

/// Current Protocol Version (UTT-H Security+)
pub const PROTOCOL_VERSION_V4: u8 = 0x04;

/// Parsed results from a Secure DLMS-lite v4 binary payload (Encrypted + TLV + CRC)
#[derive(Debug, Clone)]
pub struct DlmsBinaryFrame {
    pub version: u8,
    pub manufacturer_id: String,
    pub logical_device_name: String,
    pub timestamp: DateTime<Utc>,

    // Extracted values
    pub active_energy_import_wh: Option<u64>,
    pub active_energy_export_wh: Option<u64>,
    pub voltage_cv: Option<u32>,
    pub current_ma: Option<u32>,
    pub battery_soc_bps: Option<u32>,
}

impl DlmsBinaryFrame {
    /// Parses Secure DLMS-lite v4 format
    ///
    /// Format:
    /// [Version: 1 byte]
    /// [Total Length: 1 byte]
    /// [Header: Manuf ID (3b) + LDN (8b) + TS (8b)]
    /// [Encrypted Block: TLVs... + Auth Tag (16b)]
    /// [Checksum: 4 bytes (CRC-32)]
    ///
    /// Note: AES-GCM requires a 96-bit (12-byte) nonce. 
    /// We derive the nonce from: [Manufacturer (3b)] + [Timestamp (8b)] + [Version (1b)].
    pub fn parse(payload: &[u8], encryption_key: Option<&[u8]>) -> Result<Self> {
        let len = payload.len();
        if len < 41 { // Minimal size with Encryption (Version + Header + AuthTag + CRC)
            bail!("Payload too small for v4 secure frame");
        }

        // 1. Integrity Check (CRC-32)
        let total_length = payload[1] as usize;
        let frame_data = &payload[..total_length + 2];
        let (data_to_checksum, checksum_bytes) = frame_data.split_at(frame_data.len() - 4);
        let expected_crc = u32::from_be_bytes(checksum_bytes.try_into().unwrap());
        
        let mut hasher = Hasher::new();
        hasher.update(data_to_checksum);
        if expected_crc != hasher.finalize() {
            bail!("CRC-32 integrity check failed");
        }

        // 2. Version Check
        let version = payload[0];
        if version != PROTOCOL_VERSION_V4 {
            bail!("Unsupported protocol version: 0x{:02x}", version);
        }

        let mut cursor = 2;

        // 3. Header (Plaintext for Nonce derivation)
        let manuf_bytes = &payload[cursor..cursor + 3];
        let manufacturer_id = String::from_utf8_lossy(manuf_bytes).to_string();
        cursor += 3;

        let ldn_bytes = &payload[cursor..cursor + 8];
        let logical_device_name = String::from_utf8_lossy(ldn_bytes).trim_matches('\0').to_string();
        cursor += 8;

        let ts_bytes: [u8; 8] = payload[cursor..cursor + 8].try_into().unwrap();
        let ts_seconds = u64::from_be_bytes(ts_bytes);
        let timestamp = Utc.timestamp_opt(ts_seconds as i64, 0).single().context("Invalid TS")?;
        cursor += 8;

        // 4. Decryption (AES-256-GCM)
        let encrypted_data = &payload[cursor..total_length + 2 - 4];
        let decrypted_data = if let Some(key_bytes) = encryption_key {
            let key = Key::<Aes256Gcm>::from_slice(key_bytes);
            let cipher = Aes256Gcm::new(key);
            
            // Nonce (12 bytes): Manuf (3) + TS (8) + Version (1)
            let mut nonce_bytes = [0u8; 12];
            nonce_bytes[0..3].copy_from_slice(manuf_bytes);
            nonce_bytes[3..11].copy_from_slice(&ts_bytes);
            nonce_bytes[11] = version;
            let nonce = Nonce::from_slice(&nonce_bytes);

            cipher.decrypt(nonce, encrypted_data)
                .map_err(|e| anyhow::anyhow!("Decryption failed: {}", e))?
        } else {
            // If no key provided, assume plaintext (for development/legacy)
            encrypted_data.to_vec()
        };

        // 5. TLV Parsing (on Decrypted Data)
        let mut frame = DlmsBinaryFrame {
            version,
            manufacturer_id,
            logical_device_name,
            timestamp,
            active_energy_import_wh: None,
            active_energy_export_wh: None,
            voltage_cv: None,
            current_ma: None,
            battery_soc_bps: None,
        };

        let mut tlv_cursor = 0;
        while tlv_cursor < decrypted_data.len() {
            let tag = decrypted_data[tlv_cursor];
            tlv_cursor += 1;
            if tlv_cursor >= decrypted_data.len() { break; }
            let tag_len = decrypted_data[tlv_cursor] as usize;
            tlv_cursor += 1;

            if tlv_cursor + tag_len > decrypted_data.len() { break; }
            let val = &decrypted_data[tlv_cursor..tlv_cursor + tag_len];

            match tag {
                1 if tag_len == 8 => frame.active_energy_import_wh = Some(u64::from_be_bytes(val.try_into().unwrap())),
                2 if tag_len == 8 => frame.active_energy_export_wh = Some(u64::from_be_bytes(val.try_into().unwrap())),
                3 if tag_len == 4 => frame.voltage_cv = Some(u32::from_be_bytes(val.try_into().unwrap())),
                4 if tag_len == 4 => frame.current_ma = Some(u32::from_be_bytes(val.try_into().unwrap())),
                5 if tag_len == 4 => frame.battery_soc_bps = Some(u32::from_be_bytes(val.try_into().unwrap())),
                _ => {}
            }
            tlv_cursor += tag_len;
        }

        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_v4_encrypted_frame() {
        let key_bytes = [0u8; 32]; // 256-bit key
        let mut tlv_data = Vec::new();
        tlv_data.push(1); tlv_data.push(8); tlv_data.extend_from_slice(&5000_u64.to_be_bytes());

        let manuf = b"INC";
        let ts_sec = 1700000000_u64;
        let ts_bytes = ts_sec.to_be_bytes();

        // Encrypt
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        let cipher = Aes256Gcm::new(key);
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[0..3].copy_from_slice(manuf);
        nonce_bytes[3..11].copy_from_slice(&ts_bytes);
        nonce_bytes[11] = PROTOCOL_VERSION_V4;
        let nonce = Nonce::from_slice(&nonce_bytes);
        let encrypted = cipher.encrypt(nonce, tlv_data.as_slice()).unwrap();

        let mut payload = Vec::new();
        payload.push(PROTOCOL_VERSION_V4);
        payload.push(0); // Placeholder
        payload.extend_from_slice(manuf);
        payload.extend_from_slice(b"DEVICE1\0");
        payload.extend_from_slice(&ts_bytes);
        payload.extend_from_slice(&encrypted);

        payload[1] = (payload.len() - 2 + 4) as u8;
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();
        payload.extend_from_slice(&crc.to_be_bytes());

        let frame = DlmsBinaryFrame::parse(&payload, Some(&key_bytes)).unwrap();
        assert_eq!(frame.active_energy_import_wh, Some(5000));
    }
}
