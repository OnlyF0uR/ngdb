//! Error types for NGDB
//!
//! This module provides comprehensive error handling without external dependencies.
//! All errors are manually implemented for maximum control and minimal dependencies.

use std::fmt;
use std::io;

/// Result type alias for NGDB operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for all NGDB operations
#[derive(Debug)]
pub enum Error {
    /// Error occurred during database operations
    Database(String),

    /// Error occurred during serialization
    Serialization(String),

    /// Serialization error (alias for backward compatibility)
    SerializationError(String),

    /// Error occurred during deserialization
    Deserialization(String),

    /// I/O error
    Io(io::Error),

    /// Key not found in the database
    NotFound,

    /// Database is not properly initialized
    NotInitialized,

    /// Invalid configuration provided
    InvalidConfig(String),

    /// Replication error (future use)
    Replication(String),

    /// Batch operation error
    BatchError(String),

    /// Iterator error
    IteratorError(String),

    /// Snapshot error
    SnapshotError(String),

    /// Thread pool/async runtime error
    RuntimeError(String),

    /// Invalid key provided
    InvalidKey(String),

    /// Invalid value provided
    InvalidValue(String),

    /// Invalid data that failed validation
    InvalidData(String),

    /// Database already exists when it shouldn't
    AlreadyExists(String),

    /// Operation not supported
    Unsupported(String),

    /// Lock poisoning error (thread panicked while holding a lock)
    LockPoisoned(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Database(msg) => write!(f, "Database error: {}", msg),
            Error::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Error::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            Error::Deserialization(msg) => write!(f, "Deserialization error: {}", msg),
            Error::Io(err) => write!(f, "I/O error: {}", err),
            Error::NotFound => write!(f, "Key not found in database"),
            Error::NotInitialized => write!(f, "Database not properly initialized"),
            Error::InvalidConfig(msg) => write!(f, "Invalid configuration: {}", msg),
            Error::Replication(msg) => write!(f, "Replication error: {}", msg),
            Error::BatchError(msg) => write!(f, "Batch operation error: {}", msg),
            Error::IteratorError(msg) => write!(f, "Iterator error: {}", msg),
            Error::SnapshotError(msg) => write!(f, "Snapshot error: {}", msg),
            Error::RuntimeError(msg) => write!(f, "Runtime error: {}", msg),
            Error::InvalidKey(msg) => write!(f, "Invalid key: {}", msg),
            Error::InvalidValue(msg) => write!(f, "Invalid value: {}", msg),
            Error::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            Error::AlreadyExists(msg) => write!(f, "Already exists: {}", msg),
            Error::Unsupported(msg) => write!(f, "Unsupported operation: {}", msg),
            Error::LockPoisoned(msg) => write!(f, "Lock poisoned: {}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

// Convert from rocksdb::Error to our Error type
impl From<rocksdb::Error> for Error {
    fn from(err: rocksdb::Error) -> Self {
        Error::Database(err.to_string())
    }
}

// Convert from io::Error to our Error type
impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

// Convert from bincode 2.0 encode error to our Error type
impl From<bincode::error::EncodeError> for Error {
    fn from(err: bincode::error::EncodeError) -> Self {
        Error::Serialization(format!("Bincode encode error: {}", err))
    }
}

// Convert from bincode 2.0 decode error to our Error type
impl From<bincode::error::DecodeError> for Error {
    fn from(err: bincode::error::DecodeError) -> Self {
        Error::Deserialization(format!("Bincode decode error: {}", err))
    }
}

// Helper macro for creating errors with context
#[macro_export]
macro_rules! db_error {
    ($msg:expr) => {
        $crate::Error::Database($msg.to_string())
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::Error::Database(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! serialization_error {
    ($msg:expr) => {
        $crate::Error::Serialization($msg.to_string())
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::Error::Serialization(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! replication_error {
    ($msg:expr) => {
        $crate::Error::Replication($msg.to_string())
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::Error::Replication(format!($fmt, $($arg)*))
    };
}

impl Error {
    /// Check if error is a NotFound error
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound)
    }

    /// Check if error is an I/O error
    pub fn is_io_error(&self) -> bool {
        matches!(self, Error::Io(_))
    }

    /// Check if error is a serialization-related error
    pub fn is_serialization_error(&self) -> bool {
        matches!(self, Error::Serialization(_) | Error::Deserialization(_))
    }

    /// Check if error is a database error
    pub fn is_database_error(&self) -> bool {
        matches!(self, Error::Database(_))
    }

    /// Get the error message as a string slice
    pub fn as_str(&self) -> &str {
        match self {
            Error::Database(msg)
            | Error::Serialization(msg)
            | Error::SerializationError(msg)
            | Error::Deserialization(msg)
            | Error::InvalidConfig(msg)
            | Error::Replication(msg)
            | Error::BatchError(msg)
            | Error::IteratorError(msg)
            | Error::SnapshotError(msg)
            | Error::RuntimeError(msg)
            | Error::InvalidKey(msg)
            | Error::InvalidValue(msg)
            | Error::InvalidData(msg)
            | Error::AlreadyExists(msg)
            | Error::Unsupported(msg)
            | Error::LockPoisoned(msg) => msg,
            Error::Io(_err) => {
                // We can't return a reference to a temporary string,
                // so we return a static string for I/O errors
                "I/O error occurred"
            }
            Error::NotFound => "Not found",
            Error::NotInitialized => "Not initialized",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::Database("test error".to_string());
        assert_eq!(err.to_string(), "Database error: test error");

        let err = Error::NotFound;
        assert_eq!(err.to_string(), "Key not found in database");
    }

    #[test]
    fn test_error_checks() {
        let err = Error::NotFound;
        assert!(err.is_not_found());
        assert!(!err.is_io_error());

        let err = Error::Serialization("test".to_string());
        assert!(err.is_serialization_error());
        assert!(!err.is_database_error());
    }

    #[test]
    fn test_error_macros() {
        let err = db_error!("test");
        assert!(matches!(err, Error::Database(_)));

        let err = serialization_error!("test {}", 123);
        assert!(matches!(err, Error::Serialization(_)));
    }
}
