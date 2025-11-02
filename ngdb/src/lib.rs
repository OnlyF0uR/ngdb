//! # NGDB - High-Performance RocksDB Wrapper
//!
//! NGDB provides a clean, idiomatic Rust interface to RocksDB with zero async overhead
//! and built-in thread-safety.
//!
//! ## Features
//!
//! - **Synchronous API**: All operations are fast and synchronous - no async overhead
//! - **Type-safe**: Generic over key/value types with trait-based serialization
//! - **Zero RocksDB exposure**: Users never deal with RocksDB types directly
//! - **Thread-safe**: Built on `Arc` with multi-threaded column family support
//! - **Column Families**: Store multiple types in one database using collections
//! - **Efficient**: Multi-get operations, batching, transactions, and snapshots
//! - **Replication**: Built-in support for multi-node replication with conflict resolution
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use ngdb::{DatabaseConfig, Storable, ngdb};
//!
//! #[ngdb("users")]
//! struct User {
//!     id: u64,
//!     name: String,
//!     email: String,
//! }
//!
//! impl Storable for User {
//!     type Key = u64;
//!     fn key(&self) -> Self::Key {
//!         self.id
//!     }
//! }
//!
//! fn main() -> Result<(), ngdb::Error> {
//!     let db = DatabaseConfig::new("./data")
//!         .create_if_missing(true)
//!         .add_column_family("users")
//!         .open()?;
//!
//!     let user = User {
//!         id: 1,
//!         name: "Alice".to_string(),
//!         email: "alice@example.com".to_string(),
//!     };
//!     user.save(&db)?;
//!
//!     let users = User::collection(&db)?;
//!     let retrieved: Option<User> = users.get(&1)?;
//!     println!("Retrieved: {:?}", retrieved);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Batch Operations
//!
//! ```rust,no_run
//! # use ngdb::{Database, Storable};
//! # use borsh::{BorshSerialize, BorshDeserialize};
//! # #[derive(BorshSerialize, BorshDeserialize)]
//! # struct User { id: u64, name: String }
//! # impl Storable for User {
//! #     type Key = u64;
//! #     fn key(&self) -> u64 { self.id }
//! # }
//! # fn example(db: Database) -> Result<(), ngdb::Error> {
//! let users = db.collection::<User>("users")?;
//!
//! let mut batch = users.batch();
//! for i in 0..1000 {
//!     batch.put(&User { id: i, name: format!("User {}", i) })?;
//! }
//! batch.commit()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Transactions
//!
//! ```rust,no_run
//! # use ngdb::{Database, Storable};
//! # use borsh::{BorshSerialize, BorshDeserialize};
//! # #[derive(BorshSerialize, BorshDeserialize)]
//! # struct Account { id: u64, balance: i64 }
//! # impl Storable for Account {
//! #     type Key = u64;
//! #     fn key(&self) -> u64 { self.id }
//! # }
//! # fn example(db: Database) -> Result<(), ngdb::Error> {
//! let txn = db.transaction()?;
//! let accounts = txn.collection::<Account>("accounts")?;
//!
//! accounts.put(&Account { id: 1, balance: 100 })?;
//! accounts.put(&Account { id: 2, balance: 200 })?;
//!
//! txn.commit()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Replication
//!
//! NGDB provides production-ready replication support for multi-node deployments:
//!
//! ```rust,no_run
//! # use ngdb::{DatabaseConfig, ReplicationConfig, ReplicationManager, ReplicationLog, ReplicationOperation};
//! # fn example() -> Result<(), ngdb::Error> {
//! // Setup replica node
//! let db = DatabaseConfig::new("./data/replica")
//!     .create_if_missing(true)
//!     .add_column_family("users")
//!     .open()?;
//!
//! // Configure replication
//! let config = ReplicationConfig::new("replica-1")
//!     .enable()
//!     .with_peers(vec!["primary-1".to_string()]);
//!
//! let manager = ReplicationManager::new(db, config)?;
//!
//! // Apply replication logs from primary (received via network)
//! let log = ReplicationLog::new(
//!     "primary-1".to_string(),
//!     ReplicationOperation::Put {
//!         collection: "users".to_string(),
//!         key: vec![1, 2, 3],
//!         value: vec![4, 5, 6],
//!     },
//! ).with_checksum();
//!
//! manager.apply_replication(log)?;
//! # Ok(())
//! # }
//! ```
//!
//! Features:
//! - **Idempotent**: Safe to apply the same operation multiple times
//! - **Conflict Resolution**: LastWriteWins, FirstWriteWins, or Custom strategies
//! - **Checksums**: Optional data integrity verification
//! - **Hooks**: Extensible hook system for custom replication logic
//! - **Batching**: Efficient batch operation replication
//!
//! See the `replication` module and examples for more details.

mod config;
mod db;
mod error;
mod refs;
pub mod replication;
mod serialization;
mod traits;

// Public API exports
pub use config::{DatabaseConfig, OpenOptions};
pub use db::{
    BackupInfo, Batch, Collection, Database, IterationStatus, Iterator, Snapshot, Transaction,
    TransactionCollection,
};
pub use error::{Error, Result};
pub use refs::{Ref, Referable};
pub use replication::{
    BatchOp, ConflictResolution, ReplicationConfig, ReplicationHook, ReplicationLog,
    ReplicationManager, ReplicationOperation, ReplicationStats,
};
pub use serialization::{BincodeCodec, Codec};
pub use traits::{KeyType, Storable};

// Re-export attribute macro
pub use ngdb_macros::ngdb;

/// Re-export commonly used types
pub mod prelude {
    pub use crate::{
        BackupInfo, Collection, ConflictResolution, Database, DatabaseConfig, Error,
        IterationStatus, Ref, Referable, ReplicationConfig, ReplicationLog, ReplicationManager,
        ReplicationOperation, Result, Storable, Transaction,
    };

    pub use ngdb_macros::ngdb;
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsh::{BorshDeserialize, BorshSerialize};

    #[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
    struct TestData {
        id: u64,
        value: String,
    }

    impl Storable for TestData {
        type Key = u64;

        fn key(&self) -> Self::Key {
            self.id
        }
    }

    #[test]
    fn test_basic_operations() {
        let temp_dir = std::env::temp_dir().join(format!(
            "ngdb_test_basic_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&temp_dir);

        let db = DatabaseConfig::new(&temp_dir)
            .create_if_missing(true)
            .add_column_family("test")
            .open()
            .expect("Failed to open database");

        let collection = db
            .collection::<TestData>("test")
            .expect("Failed to get collection");

        let data = TestData {
            id: 1,
            value: "test".to_string(),
        };

        collection.put(&data).expect("Failed to put data");

        let retrieved: Option<TestData> = collection.get(&1).expect("Failed to get data");
        assert_eq!(Some(data.clone()), retrieved);

        collection.delete(&1).expect("Failed to delete data");

        let retrieved: Option<TestData> = collection.get(&1).expect("Failed to get after delete");
        assert_eq!(None, retrieved);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
