//! Serialization layer for NGDB
//!
//! This module provides the serialization abstraction layer that allows
//! pluggable codecs for encoding/decoding values.

use crate::{Error, Result};
use borsh::{BorshDeserialize, BorshSerialize};

/// Trait for encoding and decoding values to/from bytes.
///
/// This abstraction allows for different serialization formats to be used
/// with NGDB, though Borsh is the default and recommended choice for
/// performance and compatibility.
pub trait Codec: Send + Sync + 'static {
    /// Serialize a value to bytes
    fn encode<T: BorshSerialize>(&self, value: &T) -> Result<Vec<u8>>;

    /// Deserialize bytes to a value
    fn decode<T: BorshDeserialize>(&self, bytes: &[u8]) -> Result<T>;
}

/// Borsh-based codec implementation (default)
///
/// Borsh is a compact, fast binary serialization format that works
/// excellently for database storage.
///
/// Note: This struct is named `BincodeCodec` for backward compatibility,
/// but it uses Borsh serialization internally.
#[derive(Debug, Clone, Copy, Default)]
pub struct BincodeCodec;

impl BincodeCodec {
    /// Create a new BincodeCodec instance (uses Borsh serialization)
    pub fn new() -> Self {
        Self
    }
}

impl Codec for BincodeCodec {
    fn encode<T: BorshSerialize>(&self, value: &T) -> Result<Vec<u8>> {
        borsh::to_vec(value)
            .map_err(|e| Error::Serialization(format!("Failed to serialize value: {}", e)))
    }

    fn decode<T: BorshDeserialize>(&self, bytes: &[u8]) -> Result<T> {
        borsh::from_slice(bytes)
            .map_err(|e| Error::Deserialization(format!("Failed to deserialize value: {}", e)))
    }
}

/// Helper functions for direct serialization/deserialization
pub mod helpers {
    use super::*;

    /// Serialize a value using borsh
    #[inline]
    pub fn serialize<T: BorshSerialize>(value: &T) -> Result<Vec<u8>> {
        borsh::to_vec(value)
            .map_err(|e| Error::Serialization(format!("Serialization failed: {}", e)))
    }

    /// Deserialize a value using borsh
    #[inline]
    pub fn deserialize<T: BorshDeserialize>(bytes: &[u8]) -> Result<T> {
        borsh::from_slice(bytes)
            .map_err(|e| Error::Deserialization(format!("Deserialization failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsh::{BorshDeserialize, BorshSerialize};

    #[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize)]
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
