//! Serialization layer for NGDB
//!
//! This module provides the serialization abstraction layer that allows
//! pluggable codecs for encoding/decoding values.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// Trait for encoding and decoding values to/from bytes.
///
/// This abstraction allows for different serialization formats to be used
/// with NGDB, though bincode is the default and recommended choice for
/// performance and compatibility.
pub trait Codec: Send + Sync + 'static {
    /// Serialize a value to bytes
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>>;

    /// Deserialize bytes to a value
    fn decode<T: for<'de> Deserialize<'de>>(&self, bytes: &[u8]) -> Result<T>;
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
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        bincode::serialize(value)
            .map_err(|e| Error::Serialization(format!("Failed to serialize value: {}", e)))
    }

    fn decode<T: for<'de> Deserialize<'de>>(&self, bytes: &[u8]) -> Result<T> {
        bincode::deserialize(bytes)
            .map_err(|e| Error::Deserialization(format!("Failed to deserialize value: {}", e)))
    }
}

/// Helper functions for direct serialization/deserialization
pub mod helpers {
    use super::*;

    /// Serialize a value using bincode
    #[inline]
    pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
        bincode::serialize(value).map_err(Into::into)
    }

    /// Deserialize a value using bincode
    #[inline]
    pub fn deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
        bincode::deserialize(bytes).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
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
