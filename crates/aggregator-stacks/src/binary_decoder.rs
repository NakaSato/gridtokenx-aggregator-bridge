use aes_gcm::{aead::Aead, Aes256Gcm, Key, KeyInit, Nonce};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use crc32fast::Hasher;

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

/// Plaintext header of a Secure DLMS-lite v4 frame.
///
/// The header (version, manufacturer ID, logical device name, timestamp) precedes the
/// encrypted TLV block and is needed *before* decryption to resolve the per-device key:
/// `logical_device_name` is the meter identity used for the Redis key lookup. Extracted by
/// [`DlmsBinaryFrame::parse_header`] without touching the ciphertext.
#[derive(Debug, Clone)]
pub struct DlmsHeader {
    pub version: u8,
    pub manufacturer_id: String,
    pub logical_device_name: String,
    pub timestamp: DateTime<Utc>,
}

impl DlmsBinaryFrame {
    /// Validates integrity + version and extracts the plaintext header **without** decrypting.
    ///
    /// CRC-32 covers the whole frame, so the full payload is still validated here; only the
    /// header bytes are then read, returning before the AES-GCM step. Use this to resolve the
    /// device identity (LDN) and fetch its key, then call [`parse`](Self::parse) with the key.
    ///
    /// Min size is the header floor (`ver+totlen+manuf+ldn+ts+CRC = 25B`), **not** the encrypted
    /// floor `parse` enforces — a caller resolving the key must not be forced to assume ciphertext.
    pub fn parse_header(payload: &[u8]) -> Result<DlmsHeader> {
        if payload.len() < 25 {
            bail!("Payload too small for v4 header");
        }

        // 1. Integrity Check (CRC-32) over the whole frame.
        let total_length = payload[1] as usize;
        if payload.len() < total_length + 2 {
            bail!("Declared frame length exceeds payload");
        }
        let frame_data = &payload[..total_length + 2];
        let (data_to_checksum, checksum_bytes) = frame_data.split_at(frame_data.len() - 4);
        let expected_crc = u32::from_be_bytes(checksum_bytes.try_into().unwrap());
        let mut hasher = Hasher::new();
        hasher.update(data_to_checksum);
        if expected_crc != hasher.finalize() {
            bail!("CRC-32 integrity check failed");
        }

        // 2. Version Check.
        let version = payload[0];
        if version != PROTOCOL_VERSION_V4 {
            bail!("Unsupported protocol version: 0x{:02x}", version);
        }

        // 3. Plaintext header (Manuf 3b, LDN 8b, TS 8b).
        let mut cursor = 2;
        let manufacturer_id = String::from_utf8_lossy(&payload[cursor..cursor + 3]).to_string();
        cursor += 3;
        let logical_device_name = String::from_utf8_lossy(&payload[cursor..cursor + 8])
            .trim_matches('\0')
            .to_string();
        cursor += 8;
        let ts_bytes: [u8; 8] = payload[cursor..cursor + 8].try_into().unwrap();
        let ts_seconds = u64::from_be_bytes(ts_bytes);
        let timestamp = Utc
            .timestamp_opt(ts_seconds as i64, 0)
            .single()
            .context("Invalid TS")?;

        Ok(DlmsHeader {
            version,
            manufacturer_id,
            logical_device_name,
            timestamp,
        })
    }

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
        // 1-3. Integrity (CRC-32) + version + plaintext header — validated once by parse_header.
        // parse_header enforces the 25B header floor + total_length bounds; the encrypted path
        // additionally fails GCM auth if the ciphertext region is too short, so no separate
        // "minimum encrypted size" guard is needed (and one would wrongly reject plaintext frames).
        let DlmsHeader {
            version,
            manufacturer_id,
            logical_device_name,
            timestamp,
        } = Self::parse_header(payload)?;
        let total_length = payload[1] as usize;

        // Nonce derivation needs the *raw* header bytes (UTF-8-lossy strings can't round-trip).
        // Fixed offsets after the 2-byte preamble: Manuf [2..5], LDN [5..13], TS [13..21].
        let manuf_bytes = &payload[2..5];
        let ts_bytes: [u8; 8] = payload[13..21].try_into().unwrap();

        // 4. Decryption (AES-256-GCM)
        let encrypted_data = &payload[21..total_length + 2 - 4];
        let decrypted_data = if let Some(key_bytes) = encryption_key {
            let key = Key::<Aes256Gcm>::from_slice(key_bytes);
            let cipher = Aes256Gcm::new(key);

            // Nonce (12 bytes): Manuf (3) + TS (8) + Version (1)
            let mut nonce_bytes = [0u8; 12];
            nonce_bytes[0..3].copy_from_slice(manuf_bytes);
            nonce_bytes[3..11].copy_from_slice(&ts_bytes);
            nonce_bytes[11] = version;
            let nonce = Nonce::from_slice(&nonce_bytes);

            cipher
                .decrypt(nonce, encrypted_data)
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
            if tlv_cursor >= decrypted_data.len() {
                break;
            }
            let tag_len = decrypted_data[tlv_cursor] as usize;
            tlv_cursor += 1;

            if tlv_cursor + tag_len > decrypted_data.len() {
                break;
            }
            let val = &decrypted_data[tlv_cursor..tlv_cursor + tag_len];

            match tag {
                1 if tag_len == 8 => {
                    frame.active_energy_import_wh =
                        Some(u64::from_be_bytes(val.try_into().unwrap()))
                }
                2 if tag_len == 8 => {
                    frame.active_energy_export_wh =
                        Some(u64::from_be_bytes(val.try_into().unwrap()))
                }
                3 if tag_len == 4 => {
                    frame.voltage_cv = Some(u32::from_be_bytes(val.try_into().unwrap()))
                }
                4 if tag_len == 4 => {
                    frame.current_ma = Some(u32::from_be_bytes(val.try_into().unwrap()))
                }
                5 if tag_len == 4 => {
                    frame.battery_soc_bps = Some(u32::from_be_bytes(val.try_into().unwrap()))
                }
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
        tlv_data.push(1);
        tlv_data.push(8);
        tlv_data.extend_from_slice(&5000_u64.to_be_bytes());

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

    /// Build a well-formed v4 frame carrying a single import-energy TLV (tag 1 = 5000 Wh).
    /// `key = Some` ⇒ AES-256-GCM encrypted block; `key = None` ⇒ plaintext TLV block (dev/legacy).
    /// LDN is null-padded/truncated to the fixed 8 bytes. Returns the full framed payload.
    fn build_v4_frame(key: Option<&[u8; 32]>, ldn: &str) -> Vec<u8> {
        let mut tlv = Vec::new();
        tlv.push(1);
        tlv.push(8);
        tlv.extend_from_slice(&5000_u64.to_be_bytes());
        frame_from_tlv(key, ldn, tlv)
    }

    /// Like [`build_v4_frame`] but with a caller-supplied TLV block (for multi-field cases).
    fn frame_from_tlv(key: Option<&[u8; 32]>, ldn: &str, tlv: Vec<u8>) -> Vec<u8> {
        let manuf = b"INC";
        let ts_bytes = 1700000000_u64.to_be_bytes();

        let block = match key {
            Some(k) => {
                let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(k));
                let mut nonce_bytes = [0u8; 12];
                nonce_bytes[0..3].copy_from_slice(manuf);
                nonce_bytes[3..11].copy_from_slice(&ts_bytes);
                nonce_bytes[11] = PROTOCOL_VERSION_V4;
                cipher
                    .encrypt(Nonce::from_slice(&nonce_bytes), tlv.as_slice())
                    .unwrap()
            }
            None => tlv,
        };

        let mut ldn_bytes = [b'\0'; 8];
        let src = ldn.as_bytes();
        let n = src.len().min(8);
        ldn_bytes[..n].copy_from_slice(&src[..n]);

        let mut payload = Vec::new();
        payload.push(PROTOCOL_VERSION_V4);
        payload.push(0); // total-length placeholder
        payload.extend_from_slice(manuf);
        payload.extend_from_slice(&ldn_bytes);
        payload.extend_from_slice(&ts_bytes);
        payload.extend_from_slice(&block);

        payload[1] = (payload.len() - 2 + 4) as u8;
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        payload.extend_from_slice(&hasher.finalize().to_be_bytes());
        payload
    }

    #[test]
    fn parse_header_extracts_fields_without_key() {
        let payload = build_v4_frame(Some(&[0u8; 32]), "METER042");
        let h = DlmsBinaryFrame::parse_header(&payload).unwrap();
        assert_eq!(h.version, PROTOCOL_VERSION_V4);
        assert_eq!(h.manufacturer_id, "INC");
        assert_eq!(h.logical_device_name, "METER042");
        assert_eq!(h.timestamp.timestamp(), 1700000000);
    }

    #[test]
    fn parse_header_fails_on_crc_mismatch() {
        let mut payload = build_v4_frame(Some(&[0u8; 32]), "METER042");
        let last = payload.len() - 1;
        payload[last] ^= 0xFF; // corrupt CRC
        assert!(DlmsBinaryFrame::parse_header(&payload).is_err());
    }

    #[test]
    fn parse_header_fails_on_wrong_version() {
        let mut payload = build_v4_frame(Some(&[0u8; 32]), "METER042");
        payload[0] = 0x03; // unsupported version (CRC still validates first; both must pass)
        let mut hasher = Hasher::new();
        let total_length = payload[1] as usize;
        hasher.update(&payload[..total_length + 2 - 4]);
        let crc = hasher.finalize().to_be_bytes();
        let n = payload.len();
        payload[n - 4..].copy_from_slice(&crc);
        assert!(DlmsBinaryFrame::parse_header(&payload).is_err());
    }

    #[test]
    fn parse_with_wrong_key_fails_gcm_auth() {
        let payload = build_v4_frame(Some(&[0u8; 32]), "METER042");
        let wrong = [1u8; 32];
        assert!(DlmsBinaryFrame::parse(&payload, Some(&wrong)).is_err());
    }

    #[test]
    fn parse_plaintext_frame_back_compat() {
        let payload = build_v4_frame(None, "METER042");
        let frame = DlmsBinaryFrame::parse(&payload, None).unwrap();
        assert_eq!(frame.active_energy_import_wh, Some(5000));
    }

    #[test]
    fn parse_header_matches_full_parse() {
        let key = [0u8; 32];
        let payload = build_v4_frame(Some(&key), "METER042");
        let h = DlmsBinaryFrame::parse_header(&payload).unwrap();
        let full = DlmsBinaryFrame::parse(&payload, Some(&key)).unwrap();
        assert_eq!(h.version, full.version);
        assert_eq!(h.manufacturer_id, full.manufacturer_id);
        assert_eq!(h.logical_device_name, full.logical_device_name);
        assert_eq!(h.timestamp, full.timestamp);
    }

    #[test]
    fn parse_header_fails_when_too_small() {
        // Below the 25B header floor — must bail, not slice-panic.
        assert!(DlmsBinaryFrame::parse_header(&[0x04, 0x00, 0x01]).is_err());
    }

    #[test]
    fn parse_header_fails_when_declared_length_exceeds_payload() {
        // Malformed/truncated frame claiming far more bytes than present must bail before any
        // out-of-bounds header/CRC slicing (guards against panic on hostile input).
        let mut payload = build_v4_frame(Some(&[0u8; 32]), "METER042");
        payload[1] = 250; // total_length far beyond actual length
        assert!(DlmsBinaryFrame::parse_header(&payload).is_err());
    }

    #[test]
    fn parse_header_trims_null_padded_ldn() {
        let payload = build_v4_frame(Some(&[0u8; 32]), "M1"); // null-padded to 8 bytes
        let h = DlmsBinaryFrame::parse_header(&payload).unwrap();
        assert_eq!(h.logical_device_name, "M1");
    }

    #[test]
    fn parse_decodes_all_tlv_fields() {
        let key = [0u8; 32];
        let mut tlv = Vec::new();
        tlv.push(1);
        tlv.push(8);
        tlv.extend_from_slice(&5000_u64.to_be_bytes()); // import_wh
        tlv.push(2);
        tlv.push(8);
        tlv.extend_from_slice(&1200_u64.to_be_bytes()); // export_wh
        tlv.push(3);
        tlv.push(4);
        tlv.extend_from_slice(&23010_u32.to_be_bytes()); // voltage_cv
        tlv.push(4);
        tlv.push(4);
        tlv.extend_from_slice(&8500_u32.to_be_bytes()); // current_ma
        tlv.push(5);
        tlv.push(4);
        tlv.extend_from_slice(&7600_u32.to_be_bytes()); // battery_soc_bps
        let payload = frame_from_tlv(Some(&key), "METER042", tlv);

        let f = DlmsBinaryFrame::parse(&payload, Some(&key)).unwrap();
        assert_eq!(f.active_energy_import_wh, Some(5000));
        assert_eq!(f.active_energy_export_wh, Some(1200));
        assert_eq!(f.voltage_cv, Some(23010));
        assert_eq!(f.current_ma, Some(8500));
        assert_eq!(f.battery_soc_bps, Some(7600));
    }
}
