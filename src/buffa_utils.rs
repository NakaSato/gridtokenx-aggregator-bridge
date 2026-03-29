//! Buffa Protobuf Utilities
//!
//! This module provides helper functions for working with buffa protobuf messages,
//! including JSON serialization (with the `json` feature), binary encoding, and safe decoding with limits.
//!
//! # Features
//!
//! - **JSON Serialization**: Convert protobuf messages to/from JSON using serde
//! - **Safe Decoding**: Configurable recursion limits and message size limits
//! - **Size Validation**: Encode/decode functions with explicit size limits
//!
//! # Example
//!
//! ```rust,no_run
//! use crate::buffa_utils;
//! use crate::state::identity::ApiKeyRequest;
//!
//! // Encode to binary
//! let request = ApiKeyRequest { key: "my-key".to_string(), ..Default::default() };
//! let bytes = buffa_utils::encode_to_bytes(&request).unwrap();
//!
//! // Convert to JSON
//! let json = buffa_utils::to_json(&request).unwrap();
//!
//! // Decode from binary
//! let decoded: ApiKeyRequest = buffa_utils::decode_from_slice(&bytes).unwrap();
//!
//! // Parse from JSON
//! let from_json: ApiKeyRequest = buffa_utils::from_json(&json).unwrap();
//! ```

#![allow(dead_code)]

use buffa::{Message, DecodeOptions};
use serde::{Deserialize, Serialize};

/// Maximum message size for security (10 MB)
const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Recursion limit for nested protobuf messages
const RECURSION_LIMIT: u32 = 100;

/// Error type for buffa operations
#[derive(Debug, thiserror::Error)]
pub enum BuffaError {
    #[error("Decode error: {0}")]
    Decode(#[from] buffa::DecodeError),
    
    #[error("Encode error: {0}")]
    Encode(String),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("Size limit exceeded: {0}")]
    SizeLimit(String),
}

pub type Result<T> = std::result::Result<T, BuffaError>;

/// Encode a protobuf message to bytes
pub fn encode_to_bytes<T: Message>(msg: &T) -> Result<Vec<u8>> {
    Ok(msg.encode_to_vec())
}

/// Decode a protobuf message from bytes with security limits
pub fn decode_from_slice<T: Message + Default>(bytes: &[u8]) -> Result<T> {
    let options = DecodeOptions::new()
        .with_recursion_limit(RECURSION_LIMIT)
        .with_max_message_size(MAX_MESSAGE_SIZE);
    
    options.decode_from_slice::<T>(bytes).map_err(BuffaError::from)
}

/// Convert protobuf message to JSON (Proto3 JSON format)
pub fn to_json<T: Message + Serialize>(msg: &T) -> Result<String> {
    serde_json::to_string(msg).map_err(BuffaError::Json)
}

/// Convert protobuf message to pretty-printed JSON
pub fn to_json_pretty<T: Message + Serialize>(msg: &T) -> Result<String> {
    serde_json::to_string_pretty(msg).map_err(BuffaError::Json)
}

/// Parse JSON to protobuf message (Proto3 JSON format)
pub fn from_json<T: Message + Default + for<'de> Deserialize<'de>>(json: &str) -> Result<T> {
    serde_json::from_str(json).map_err(BuffaError::Json)
}

/// Parse JSON bytes to protobuf message
pub fn from_json_bytes<T: Message + Default + for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(BuffaError::Json)
}

/// Safe binary encoding and decoding with size validation
pub mod safe_codec {
    use super::*;
    
    /// Encode with size check
    pub fn encode_with_limit<T: Message>(msg: &T, max_size: usize) -> Result<Vec<u8>> {
        let encoded = msg.encode_to_vec();
        if encoded.len() > max_size {
            return Err(BuffaError::SizeLimit(
                format!("Encoded message size {} exceeds limit {}", encoded.len(), max_size)
            ));
        }
        Ok(encoded)
    }
    
    /// Decode with strict size validation
    pub fn decode_with_limit<T: Message + Default>(bytes: &[u8], max_size: usize) -> Result<T> {
        if bytes.len() > max_size {
            return Err(BuffaError::SizeLimit(
                format!("Input size {} exceeds maximum {}", bytes.len(), max_size)
            ));
        }
        decode_from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert!(MAX_MESSAGE_SIZE > 0);
        assert!(RECURSION_LIMIT > 0);
    }
}
