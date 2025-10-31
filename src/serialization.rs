//! Serialization layer for NGDB
//!
//! This module provides the serialization abstraction layer that allows
//! pluggable codecs for encoding/decoding values.

use crate::{Error, Result};
use bincode::{config, Decode, Encode};

// Type alias for bincode decode context - bincode 2.0 uses () as the context type
type BincodeConfig = ();

/// Trait for encoding and decoding values to/from bytes.
///
/// This abstraction allows for different serialization formats to be used
/// with NGDB, though bincode is the default and recommended choice for
/// performance and compatibility.
pub trait Codec: Send + Sync + 'static {
    /// Serialize a value to bytes
    fn encode<T: Encode>(&self, value: &T) -> Result<Vec<u8>>;

    /// Deserialize bytes to a value
    fn decode<T: Decode<BincodeConfig>>(&self, bytes: &[u8]) -> Result<T>;
}

/// Bincode-based codec implementation (default)
///
/// Bincode is a compact, fast binary serialization format that works
/// excellently for database storage.
#[derive(Debug, Clone, Copy, Default)]
pub struct BincodeCodec;

impl BincodeCodec {
    /// Create a new BincodeCodec instance
    pub fn new() -> Self {
        Self
    }
}

impl Codec for BincodeCodec {
    fn encode<T: Encode>(&self, value: &T) -> Result<Vec<u8>> {
        bincode::encode_to_vec(value, config::standard())
            .map_err(|e| Error::Serialization(format!("Failed to serialize value: {}", e)))
    }

    fn decode<T: Decode<BincodeConfig>>(&self, bytes: &[u8]) -> Result<T> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _len)| value)
            .map_err(|e| Error::Deserialization(format!("Failed to deserialize value: {}", e)))
    }
}

/// Helper functions for direct serialization/deserialization
pub mod helpers {
    use super::*;

    /// Serialize a value using bincode
    #[inline]
    pub fn serialize<T: Encode>(value: &T) -> Result<Vec<u8>> {
        bincode::encode_to_vec(value, config::standard())
            .map_err(|e| Error::Serialization(format!("Serialization failed: {}", e)))
    }

    /// Deserialize a value using bincode
    #[inline]
    pub fn deserialize<T: Decode<BincodeConfig>>(bytes: &[u8]) -> Result<T> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _len)| value)
            .map_err(|e| Error::Deserialization(format!("Deserialization failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Encode, Decode)]
    struct TestStruct {
        id: u64,
        name: String,
        tags: Vec<String>,
    }

    #[test]
    fn test_bincode_codec() {
        let codec = BincodeCodec::new();

        let original = TestStruct {
            id: 123,
            name: "test".to_string(),
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        };

        let encoded = codec.encode(&original).unwrap();
        let decoded: TestStruct = codec.decode(&encoded).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_codec_with_primitives() {
        let codec = BincodeCodec::new();

        let num: u64 = 42;
        let encoded = codec.encode(&num).unwrap();
        let decoded: u64 = codec.decode(&encoded).unwrap();
        assert_eq!(num, decoded);

        let text = "Hello, NGDB!".to_string();
        let encoded = codec.encode(&text).unwrap();
        let decoded: String = codec.decode(&encoded).unwrap();
        assert_eq!(text, decoded);
    }

    #[test]
    fn test_helpers() {
        let original = vec![1u64, 2, 3, 4, 5];
        let serialized = helpers::serialize(&original).unwrap();
        let deserialized: Vec<u64> = helpers::deserialize(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }
}
