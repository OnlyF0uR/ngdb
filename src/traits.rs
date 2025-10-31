//! Core traits for NGDB
//!
//! This module defines the fundamental traits that users implement to make their types
//! work seamlessly with NGDB.

use crate::{Error, Result};
use bincode::{config, Decode, Encode};

// Type alias for bincode decode context - bincode 2.0 uses () as the context type
type BincodeConfig = ();

/// The core trait that types must implement to be stored in NGDB.
///
/// This trait allows you to define how your custom types map to database keys
/// and enables automatic serialization/deserialization.
///
/// # Examples
///
/// ```rust
/// use ngdb::Storable;
/// use bincode::{Decode, Encode};
///
/// #[derive(Encode, Decode)]
/// struct User {
///     id: u64,
///     name: String,
///     email: String,
/// }
///
/// impl Storable for User {
///     type Key = u64;
///
///     fn key(&self) -> Self::Key {
///         self.id
///     }
/// }
/// ```
pub trait Storable: Encode + Decode<BincodeConfig> + Sized {
    /// The key type for this storable type.
    /// Common types: u64, String, (u64, String), etc.
    type Key: KeyType;

    /// Extract the key from this instance.
    fn key(&self) -> Self::Key;

    /// Optional: Validate the instance before storing.
    /// Returns `Ok(())` if valid, or an error if invalid.
    fn validate(&self) -> Result<()> {
        Ok(())
    }

    /// Optional: Called after the instance is successfully stored.
    /// Useful for logging or triggering side effects.
    fn on_stored(&self) {}

    /// Optional: Called after the instance is successfully deleted.
    fn on_deleted(&self) {}
}

/// Trait for types that can be used as database keys.
///
/// This trait is automatically implemented for common key types.
pub trait KeyType:
    Encode + Decode<BincodeConfig> + Clone + Send + Sync + std::fmt::Debug + 'static
{
    /// Convert the key to bytes for storage.
    fn to_bytes(&self) -> Result<Vec<u8>>;

    /// Reconstruct the key from bytes.
    #[allow(dead_code)]
    fn from_bytes(bytes: &[u8]) -> Result<Self>;
}

// Implementations for common key types

impl KeyType for u8 {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl KeyType for u16 {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl KeyType for u32 {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl KeyType for u64 {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl KeyType for u128 {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl KeyType for i8 {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl KeyType for i16 {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl KeyType for i32 {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl KeyType for i64 {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl KeyType for i128 {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl KeyType for String {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| Error::InvalidKey(format!("Invalid UTF-8 in key: {}", e)))
    }
}

impl KeyType for Vec<u8> {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.clone())
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bytes.to_vec())
    }
}

// Tuple implementations for composite keys
impl<A, B> KeyType for (A, B)
where
    A: KeyType,
    B: KeyType,
{
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl<A, B, C> KeyType for (A, B, C)
where
    A: KeyType,
    B: KeyType,
    C: KeyType,
{
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

impl<A, B, C, D> KeyType for (A, B, C, D)
where
    A: KeyType,
    B: KeyType,
    C: KeyType,
    D: KeyType,
{
    fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(self, config::standard()).map_err(Into::into)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::decode_from_slice(bytes, config::standard())
            .map(|(value, _)| value)
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_type_u64() {
        let key: u64 = 42;
        let bytes = key.to_bytes().unwrap();
        let recovered = u64::from_bytes(&bytes).unwrap();
        assert_eq!(key, recovered);
    }

    #[test]
    fn test_key_type_string() {
        let key = "test_key".to_string();
        let bytes = key.to_bytes().unwrap();
        let recovered = String::from_bytes(&bytes).unwrap();
        assert_eq!(key, recovered);
    }

    #[test]
    fn test_key_type_tuple() {
        let key: (u64, String) = (123, "test".to_string());
        let bytes = key.to_bytes().unwrap();
        let recovered = <(u64, String)>::from_bytes(&bytes).unwrap();
        assert_eq!(key, recovered);
    }
}
