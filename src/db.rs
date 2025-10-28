//! Core database implementation for NGDB
//!
//! This module provides a high-performance, thread-safe RocksDB wrapper with zero async overhead.
//! All operations are synchronous and leverage RocksDB's internal thread-safety.

use crate::{serialization::helpers, traits::KeyType, Error, Result, Storable};
use rocksdb::{BoundColumnFamily, WriteBatch as RocksWriteBatch};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info, instrument, warn};

/// The main database handle
///
/// This is a thread-safe, cloneable handle to the underlying RocksDB instance.
/// All operations are synchronous and fast - no async overhead.
///
/// # Thread Safety
///
/// The database uses RocksDB's multi-threaded column family mode, making it safe
/// to use concurrently from multiple threads without additional synchronization.
///
/// # Examples
///
/// ```rust,no_run
/// use ngdb::{Database, DatabaseConfig, Storable};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct User {
///     id: u64,
///     name: String,
/// }
///
/// impl Storable for User {
///     type Key = u64;
///     fn key(&self) -> Self::Key {
///         self.id
///     }
/// }
///
/// fn main() -> Result<(), ngdb::Error> {
///     let db = DatabaseConfig::new("./data")
///         .create_if_missing(true)
///         .open()?;
///
///     let users = db.collection::<User>("users")?;
///     let user = User { id: 1, name: "Alice".to_string() };
///     users.put(&user)?;
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct Database {
    pub(crate) inner: Arc<DatabaseInner>,
}

pub(crate) struct DatabaseInner {
    pub(crate) db: Arc<rocksdb::DB>,
    // RwLock for shutdown: read locks allow operations, write lock for shutdown
    shutdown: Arc<std::sync::RwLock<bool>>,
}

impl Database {
    /// Create a new database handle (internal use only)
    pub(crate) fn new(db: rocksdb::DB) -> Self {
        Self {
            inner: Arc::new(DatabaseInner {
                db: Arc::new(db),
                shutdown: Arc::new(std::sync::RwLock::new(false)),
            }),
        }
    }

    /// Get a typed collection for storing and retrieving values
    ///
    /// Collections are backed by RocksDB column families and provide type-safe
    /// access to your data.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the column family
    ///
    /// # Errors
    ///
    /// Returns an error if the column family doesn't exist. Make sure to declare
    /// all column families in `DatabaseConfig::add_column_family()` before opening.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ngdb::{Database, Storable};
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct User { id: u64 }
    /// # impl Storable for User {
    /// #     type Key = u64;
    /// #     fn key(&self) -> u64 { self.id }
    /// # }
    /// # fn example(db: Database) -> Result<(), ngdb::Error> {
    /// let users = db.collection::<User>("users")?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self))]
    pub fn collection<T: Storable>(&self, name: &str) -> Result<Collection<T>> {
        // Acquire read lock to prevent shutdown during this operation
        let shutdown = self
            .inner
            .shutdown
            .read()
            .map_err(|e| Error::LockPoisoned(format!("Shutdown lock poisoned: {:?}", e)))?;

        if *shutdown {
            return Err(Error::Database("Database has been shut down".to_string()));
        }

        // Verify column family exists
        self.inner.db.cf_handle(name).ok_or_else(|| {
            error!("Column family '{}' not found", name);
            Error::Database(format!(
                "Column family '{}' does not exist. Ensure it was declared in DatabaseConfig::add_column_family() before opening the database.",
                name
            ))
        })?;

        debug!("Created collection for column family '{}'", name);
        Ok(Collection::new(
            Arc::clone(&self.inner.db),
            name,
            Arc::clone(&self.inner.shutdown),
        ))
    }

    /// List all column families in the database
    pub fn list_collections(&self) -> Vec<String> {
        rocksdb::DB::list_cf(&rocksdb::Options::default(), self.inner.db.path())
            .unwrap_or_else(|_| vec!["default".to_string()])
    }

    /// Flush all memtables to disk
    ///
    /// This forces all in-memory data to be written to SST files.
    #[instrument(skip(self))]
    pub fn flush(&self) -> Result<()> {
        info!("Flushing database");
        self.inner.db.flush().map_err(|e| {
            error!("Flush failed: {}", e);
            Error::Database(format!("Flush failed: {}", e))
        })
    }

    /// Compact all data in the database
    ///
    /// This will trigger compaction across all column families.
    #[instrument(skip(self))]
    pub fn compact_all(&self) -> Result<()> {
        info!("Compacting entire database");
        self.inner.db.compact_range::<&[u8], &[u8]>(None, None);
        Ok(())
    }

    /// Create a backup of the database
    ///
    /// # Arguments
    ///
    /// * `backup_path` - Directory where the backup will be stored
    #[instrument(skip(self, backup_path))]
    pub fn backup<P: AsRef<Path>>(&self, backup_path: P) -> Result<()> {
        use rocksdb::backup::{BackupEngine, BackupEngineOptions};

        let path = backup_path.as_ref();
        info!("Creating backup at {:?}", path);

        let backup_opts = BackupEngineOptions::new(path).map_err(|e| {
            error!("Failed to create backup options: {}", e);
            Error::Database(format!("Failed to create backup options: {}", e))
        })?;

        let mut backup_engine =
            BackupEngine::open(&backup_opts, &rocksdb::Env::new()?).map_err(|e| {
                error!("Failed to open backup engine: {}", e);
                Error::Database(format!("Failed to open backup engine: {}", e))
            })?;

        backup_engine
            .create_new_backup(&self.inner.db)
            .map_err(|e| {
                error!("Failed to create backup: {}", e);
                Error::Database(format!("Failed to create backup: {}", e))
            })?;

        info!("Backup created successfully");
        Ok(())
    }

    /// Restore database from a backup
    ///
    /// # Arguments
    ///
    /// * `backup_path` - Directory containing the backup
    /// * `restore_path` - Directory where the database will be restored
    pub fn restore_from_backup<P: AsRef<Path>>(backup_path: P, restore_path: P) -> Result<()> {
        use rocksdb::backup::{BackupEngine, BackupEngineOptions, RestoreOptions};

        let backup_path = backup_path.as_ref();
        let restore_path = restore_path.as_ref();

        info!(
            "Restoring from backup {:?} to {:?}",
            backup_path, restore_path
        );

        let backup_opts = BackupEngineOptions::new(backup_path).map_err(|e| {
            error!("Failed to create backup options: {}", e);
            Error::Database(format!("Failed to create backup options: {}", e))
        })?;

        let mut backup_engine =
            BackupEngine::open(&backup_opts, &rocksdb::Env::new()?).map_err(|e| {
                error!("Failed to open backup engine: {}", e);
                Error::Database(format!("Failed to open backup engine: {}", e))
            })?;

        let restore_opts = RestoreOptions::default();
        backup_engine
            .restore_from_latest_backup(restore_path, restore_path, &restore_opts)
            .map_err(|e| {
                error!("Failed to restore backup: {}", e);
                Error::Database(format!("Failed to restore backup: {}", e))
            })?;

        info!("Backup restored successfully");
        Ok(())
    }

    /// List all available backups
    pub fn list_backups<P: AsRef<Path>>(backup_path: P) -> Result<Vec<BackupInfo>> {
        use rocksdb::backup::{BackupEngine, BackupEngineOptions};

        let path = backup_path.as_ref();
        let backup_opts = BackupEngineOptions::new(path)
            .map_err(|e| Error::Database(format!("Failed to create backup options: {}", e)))?;

        let backup_engine = BackupEngine::open(&backup_opts, &rocksdb::Env::new()?)
            .map_err(|e| Error::Database(format!("Failed to open backup engine: {}", e)))?;

        let infos = backup_engine.get_backup_info();
        Ok(infos
            .iter()
            .map(|info| BackupInfo {
                backup_id: info.backup_id,
                timestamp: info.timestamp,
                size: info.size,
            })
            .collect())
    }

    /// Create a new transaction for atomic operations
    ///
    /// Transactions allow you to group multiple operations and commit them atomically.
    /// All writes in a transaction are buffered in memory until commit.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ngdb::{Database, Storable};
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Account { id: u64, balance: i64 }
    /// # impl Storable for Account {
    /// #     type Key = u64;
    /// #     fn key(&self) -> u64 { self.id }
    /// # }
    /// # fn example(db: Database) -> Result<(), ngdb::Error> {
    /// let txn = db.transaction()?;
    /// let accounts = txn.collection::<Account>("accounts")?;
    ///
    /// accounts.put(&Account { id: 1, balance: 100 })?;
    /// accounts.put(&Account { id: 2, balance: 200 })?;
    ///
    /// txn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self))]
    pub fn transaction(&self) -> Result<Transaction> {
        // Acquire read lock to prevent shutdown during this operation
        let shutdown = self
            .inner
            .shutdown
            .read()
            .map_err(|e| Error::LockPoisoned(format!("Shutdown lock poisoned: {:?}", e)))?;

        if *shutdown {
            return Err(Error::Database("Database has been shut down".to_string()));
        }

        Ok(Transaction::new(
            Arc::clone(&self.inner.db),
            Arc::clone(&self.inner.shutdown),
        ))
    }

    /// Gracefully shut down the database
    ///
    /// Flushes all memtables, marks the database as shut down, and prevents new operations.
    /// After calling this, all subsequent operations will fail.
    ///
    /// This acquires a write lock, which will block until all ongoing operations
    /// (which hold read locks) complete. This eliminates the TOCTOU race condition.
    #[instrument(skip(self))]
    pub fn shutdown(&self) -> Result<()> {
        info!("Shutting down database");

        // Acquire write lock - blocks until all read locks (operations) complete
        let mut shutdown = self
            .inner
            .shutdown
            .write()
            .map_err(|e| Error::LockPoisoned(format!("Shutdown lock poisoned: {:?}", e)))?;

        // Mark as shut down to prevent new operations
        *shutdown = true;

        // Flush all data
        self.flush()?;

        info!("Database shutdown complete");
        Ok(())
    }
}

// SAFETY: Database can be safely sent between threads and shared across threads because:
// 1. RocksDB guarantees thread-safety when opened with multi-threaded column family mode (the default)
// 2. All internal state (Arc<rocksdb::DB>, Arc<AtomicBool>) is Send + Sync
// 3. RocksDB documentation confirms that DB instances can be safely shared across threads
//    See: https://github.com/facebook/rocksdb/wiki/Basic-Operations#thread-safety
// 4. The Arc wrapper ensures the DB outlives all references
unsafe impl Send for Database {}
unsafe impl Sync for Database {}

/// Information about a database backup
#[derive(Debug, Clone)]
pub struct BackupInfo {
    /// Unique backup identifier
    pub backup_id: u32,
    /// Unix timestamp when backup was created
    pub timestamp: i64,
    /// Size of the backup in bytes
    pub size: u64,
}

/// A typed collection for storing and retrieving values
///
/// Collections are backed by RocksDB column families and provide type-safe
/// access to stored data. All operations are synchronous and thread-safe.
pub struct Collection<T: Storable> {
    db: Arc<rocksdb::DB>,
    cf_name: String,
    shutdown: Arc<std::sync::RwLock<bool>>,
    _phantom: PhantomData<T>,
}

impl<T: Storable> Collection<T> {
    fn new(db: Arc<rocksdb::DB>, name: &str, shutdown: Arc<std::sync::RwLock<bool>>) -> Self {
        Self {
            db,
            cf_name: name.to_string(),
            shutdown,
            _phantom: PhantomData,
        }
    }

    fn cf<'a>(&'a self) -> Result<Arc<BoundColumnFamily<'a>>> {
        // Just fetch the handle - RocksDB makes this very fast (simple hashmap lookup)
        self.db
            .cf_handle(&self.cf_name)
            .ok_or_else(|| Error::Database(format!("Column family '{}' not found", self.cf_name)))
    }

    #[inline]
    fn check_shutdown(&self) -> Result<std::sync::RwLockReadGuard<'_, bool>> {
        // Acquire read lock to prevent shutdown during the operation
        // This eliminates the TOCTOU race condition
        let guard = self
            .shutdown
            .read()
            .map_err(|e| Error::LockPoisoned(format!("Shutdown lock poisoned: {:?}", e)))?;

        if *guard {
            return Err(Error::Database("Database has been shut down".to_string()));
        }

        Ok(guard)
    }

    /// Store a value in the collection
    ///
    /// The value will be validated, serialized, and written to disk.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to store
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails, serialization fails, or the write fails
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ngdb::{Collection, Storable};
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct User { id: u64, name: String }
    /// # impl Storable for User {
    /// #     type Key = u64;
    /// #     fn key(&self) -> u64 { self.id }
    /// # }
    /// # fn example(collection: Collection<User>) -> Result<(), ngdb::Error> {
    /// let user = User { id: 1, name: "Alice".to_string() };
    /// collection.put(&user)?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self, value))]
    pub fn put(&self, value: &T) -> Result<()> {
        let _guard = self.check_shutdown()?;

        // Validate first
        value.validate()?;

        let key = value.key();
        let key_bytes = key.to_bytes()?;
        let value_bytes = helpers::serialize(value)?;

        debug!("Putting value in collection '{}'", self.cf_name);

        let cf = self.cf()?;
        self.db.put_cf(&cf, key_bytes, value_bytes).map_err(|e| {
            error!("Failed to put value: {}", e);
            Error::Database(format!("Failed to put value: {}", e))
        })?;

        value.on_stored();
        Ok(())
    }

    /// Retrieve a value from the collection by key
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up
    ///
    /// # Returns
    ///
    /// * `Some(T)` if the key exists
    /// * `None` if the key doesn't exist
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails or there's a database error
    #[instrument(skip(self))]
    pub fn get(&self, key: &T::Key) -> Result<Option<T>> {
        let _guard = self.check_shutdown()?;

        let key_bytes = key.to_bytes()?;
        let cf = self.cf()?;

        match self.db.get_cf(&cf, key_bytes)? {
            Some(value_bytes) => {
                let value: T = helpers::deserialize(&value_bytes)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Retrieve multiple values at once using optimized multi_get
    ///
    /// This is significantly faster than calling `get()` multiple times
    /// as it performs a single batched operation.
    ///
    /// # Arguments
    ///
    /// * `keys` - Slice of keys to retrieve
    ///
    /// # Returns
    ///
    /// A vector of optional values in the same order as the input keys
    #[instrument(skip(self, keys))]
    pub fn get_many(&self, keys: &[T::Key]) -> Result<Vec<Option<T>>> {
        let _guard = self.check_shutdown()?;

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // Convert all keys to bytes
        let key_bytes: Result<Vec<Vec<u8>>> = keys.iter().map(|k| k.to_bytes()).collect();
        let key_bytes = key_bytes?;

        // Prepare column family references
        let cf = self.cf()?;
        let cf_refs: Vec<_> = key_bytes.iter().map(|k| (&cf, k.as_slice())).collect();

        // Perform multi_get
        let results = self.db.multi_get_cf(cf_refs);

        // Process results
        let mut output = Vec::with_capacity(keys.len());
        for result in results {
            match result {
                Ok(Some(value_bytes)) => {
                    let value: T = helpers::deserialize(&value_bytes)?;
                    output.push(Some(value));
                }
                Ok(None) => output.push(None),
                Err(e) => {
                    return Err(Error::Database(format!("Multi-get failed: {}", e)));
                }
            }
        }

        Ok(output)
    }

    /// Delete a value from the collection by key
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete
    ///
    /// # Errors
    ///
    /// Returns an error if the delete operation fails
    #[instrument(skip(self))]
    pub fn delete(&self, key: &T::Key) -> Result<()> {
        let _guard = self.check_shutdown()?;

        let key_bytes = key.to_bytes()?;

        debug!("Deleting key from collection '{}'", self.cf_name);

        let cf = self.cf()?;
        self.db.delete_cf(&cf, key_bytes).map_err(|e| {
            error!("Failed to delete: {}", e);
            Error::Database(format!("Failed to delete: {}", e))
        })
    }

    /// Check if a key exists in the collection
    #[instrument(skip(self))]
    pub fn exists(&self, key: &T::Key) -> Result<bool> {
        let _guard = self.check_shutdown()?;
        Ok(self.get(key)?.is_some())
    }

    /// Create a batch for multiple write operations
    ///
    /// Batches allow you to group multiple writes together for better performance.
    /// All operations in a batch are applied atomically.
    pub fn batch(&self) -> Batch<T> {
        Batch::new(Arc::clone(&self.db), self.cf_name.clone())
    }

    /// Create a snapshot for consistent reads
    ///
    /// Snapshots provide a consistent view of the database at a point in time.
    pub fn snapshot(&self) -> Snapshot<T> {
        Snapshot::new(Arc::clone(&self.db), self.cf_name.clone())
    }

    /// Create an iterator over all items in the collection
    ///
    /// The iterator holds an Arc to the database, keeping it alive for the duration
    /// of the iteration. Dropping the iterator won't invalidate the database.
    pub fn iter(&self) -> Iterator<T> {
        Iterator::new(
            Arc::clone(&self.db),
            self.cf_name.clone(),
            IteratorMode::Start,
        )
    }

    /// Create an iterator starting from a specific key
    pub fn iter_from(&self, key: &T::Key) -> Result<Iterator<T>> {
        let key_bytes = key.to_bytes()?;
        Ok(Iterator::new(
            Arc::clone(&self.db),
            self.cf_name.clone(),
            IteratorMode::From(key_bytes),
        ))
    }

    /// Estimate the number of keys in the collection
    ///
    /// This uses RocksDB's internal statistics and may not be exact.
    pub fn estimate_num_keys(&self) -> Result<u64> {
        let cf = self.cf()?;
        self.db
            .property_int_value_cf(&cf, "rocksdb.estimate-num-keys")
            .map(|v| v.unwrap_or(0))
            .map_err(|e| Error::Database(format!("Failed to get estimate: {}", e)))
    }

    /// Flush this collection's memtable to disk
    #[instrument(skip(self))]
    pub fn flush(&self) -> Result<()> {
        info!("Flushing collection '{}'", self.cf_name);
        let cf = self.cf()?;
        self.db.flush_cf(&cf).map_err(|e| {
            error!("Flush failed: {}", e);
            Error::Database(format!("Flush failed: {}", e))
        })
    }

    /// Compact a range of keys in this collection
    #[instrument(skip(self, start, end))]
    pub fn compact_range(&self, start: Option<&T::Key>, end: Option<&T::Key>) -> Result<()> {
        let start_bytes = start.map(|k| k.to_bytes()).transpose()?;
        let end_bytes = end.map(|k| k.to_bytes()).transpose()?;

        info!("Compacting range in collection '{}'", self.cf_name);

        let cf = self.cf()?;
        self.db
            .compact_range_cf(&cf, start_bytes.as_deref(), end_bytes.as_deref());
        Ok(())
    }

    /// Get the name of this collection
    pub fn name(&self) -> &str {
        &self.cf_name
    }
}

// SAFETY: Collection can be safely sent between threads and shared across threads because:
// 1. All internal state is Send + Sync (Arc<DB>, String, Arc<AtomicBool>)
// 2. T: Storable which requires T: Send + Sync (see traits.rs)
// 3. RocksDB column family operations are thread-safe
unsafe impl<T: Storable> Send for Collection<T> {}
unsafe impl<T: Storable> Sync for Collection<T> {}

/// A batch of write operations
///
/// Batches allow multiple writes to be applied atomically and efficiently.
pub struct Batch<T: Storable> {
    db: Arc<rocksdb::DB>,
    cf_name: String,
    batch: RocksWriteBatch,
    _phantom: PhantomData<T>,
}

impl<T: Storable> Batch<T> {
    fn new(db: Arc<rocksdb::DB>, cf_name: String) -> Self {
        Self {
            db,
            cf_name,
            batch: RocksWriteBatch::default(),
            _phantom: PhantomData,
        }
    }

    /// Add a put operation to the batch
    pub fn put(&mut self, value: &T) -> Result<()> {
        value.validate()?;

        let key = value.key();
        let key_bytes = key.to_bytes()?;
        let value_bytes = helpers::serialize(value)?;

        let cf = self.db.cf_handle(&self.cf_name).ok_or_else(|| {
            Error::Database(format!("Column family '{}' not found", self.cf_name))
        })?;
        self.batch.put_cf(&cf, &key_bytes, &value_bytes);
        Ok(())
    }

    /// Add a delete operation to the batch
    pub fn delete(&mut self, key: &T::Key) -> Result<()> {
        let key_bytes = key.to_bytes()?;

        let cf = self.db.cf_handle(&self.cf_name).ok_or_else(|| {
            Error::Database(format!("Column family '{}' not found", self.cf_name))
        })?;
        self.batch.delete_cf(&cf, &key_bytes);
        Ok(())
    }

    /// Clear all operations from the batch
    pub fn clear(&mut self) {
        self.batch.clear();
    }

    /// Get the number of operations in the batch
    pub fn len(&self) -> usize {
        self.batch.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Commit all operations in the batch atomically
    #[instrument(skip(self))]
    pub fn commit(self) -> Result<()> {
        let op_count = self.batch.len();
        debug!(
            "Committing batch with {} operations to '{}'",
            op_count, self.cf_name
        );

        self.db.write(self.batch).map_err(|e| {
            error!("Batch commit failed: {}", e);
            Error::Database(format!("Batch commit failed: {}", e))
        })
    }
}

/// A consistent snapshot of the database
///
/// Snapshots provide a point-in-time view of the data. This holds a single
/// RocksDB snapshot that is reused for all operations, ensuring all reads
/// see the same consistent state.
///
/// The snapshot is stored in a Box to pin its location in memory, allowing
/// us to safely tie its lifetime to the DB reference we hold via Arc.
pub struct Snapshot<T: Storable> {
    _db: Arc<rocksdb::DB>,
    snapshot: Box<rocksdb::SnapshotWithThreadMode<'static, rocksdb::DB>>,
    cf_name: String,
    _phantom: PhantomData<T>,
}

impl<T: Storable> Snapshot<T> {
    fn new(db: Arc<rocksdb::DB>, cf_name: String) -> Self {
        // Create a snapshot and store it
        let snapshot = db.snapshot();
        // SAFETY: We extend the lifetime to 'static because:
        // 1. We hold an Arc<DB> which keeps the database alive
        // 2. The snapshot is valid as long as the DB exists
        // 3. We Box the snapshot to pin it in memory
        // 4. The Snapshot struct owns both the DB Arc and the snapshot
        // 5. Rust's drop order guarantees snapshot is dropped before _db
        let static_snapshot = unsafe {
            std::mem::transmute::<
                rocksdb::SnapshotWithThreadMode<'_, rocksdb::DB>,
                rocksdb::SnapshotWithThreadMode<'static, rocksdb::DB>,
            >(snapshot)
        };
        Self {
            _db: db,
            snapshot: Box::new(static_snapshot),
            cf_name,
            _phantom: PhantomData,
        }
    }

    fn cf<'a>(&'a self) -> Result<Arc<BoundColumnFamily<'a>>> {
        self._db
            .cf_handle(&self.cf_name)
            .ok_or_else(|| Error::Database(format!("Column family '{}' not found", self.cf_name)))
    }

    /// Get a value from the snapshot
    ///
    /// Reads from the consistent point-in-time snapshot held by this struct.
    pub fn get(&self, key: &T::Key) -> Result<Option<T>> {
        let key_bytes = key.to_bytes()?;
        let cf = self.cf()?;

        match self.snapshot.get_cf(&cf, key_bytes)? {
            Some(value_bytes) => {
                let value: T = helpers::deserialize(&value_bytes)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Check if a key exists in the snapshot
    pub fn exists(&self, key: &T::Key) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }
}

/// Iterator mode
enum IteratorMode {
    Start,
    From(Vec<u8>),
}

/// Result of an iteration operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IterationStatus {
    /// Iteration completed successfully - all items were processed
    Completed,
    /// Iteration stopped early because the callback returned false
    StoppedEarly,
}

/// Iterator over collection items
///
/// Provides methods to iterate through all items in a collection.
pub struct Iterator<T: Storable> {
    db: Arc<rocksdb::DB>,
    cf_name: String,
    mode: IteratorMode,
    _phantom: PhantomData<T>,
}

impl<T: Storable> Iterator<T> {
    fn new(db: Arc<rocksdb::DB>, cf_name: String, mode: IteratorMode) -> Self {
        Self {
            db,
            cf_name,
            mode,
            _phantom: PhantomData,
        }
    }

    fn cf<'a>(&'a self) -> Result<Arc<BoundColumnFamily<'a>>> {
        self.db
            .cf_handle(&self.cf_name)
            .ok_or_else(|| Error::Database(format!("Column family '{}' not found", self.cf_name)))
    }

    /// Collect all items into a vector
    ///
    /// # Warning
    ///
    /// This will load all items into memory. Use with caution on large collections.
    pub fn collect_all(&self) -> Result<Vec<T>> {
        let mut results = Vec::new();
        let cf = self.cf()?;
        let iter = match &self.mode {
            IteratorMode::Start => self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start),
            IteratorMode::From(key) => self.db.iterator_cf(
                &cf,
                rocksdb::IteratorMode::From(key, rocksdb::Direction::Forward),
            ),
        };

        for item in iter {
            let (_key, value_bytes) =
                item.map_err(|e| Error::IteratorError(format!("Iterator error: {}", e)))?;

            let value: T = helpers::deserialize(&value_bytes)?;
            results.push(value);
        }

        Ok(results)
    }

    /// Iterate and apply a function to each item
    ///
    /// This is more memory-efficient than `collect_all()` for large datasets.
    ///
    /// # Arguments
    ///
    /// * `f` - Function to apply to each item. Return `false` to stop iteration early.
    ///
    /// # Returns
    ///
    /// Returns `Ok(IterationStatus)` where the status indicates whether iteration
    /// completed or was stopped early by the callback.
    pub fn for_each<F>(&self, mut f: F) -> Result<IterationStatus>
    where
        F: FnMut(T) -> bool,
    {
        let cf = self.cf()?;
        let iter = match &self.mode {
            IteratorMode::Start => self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start),
            IteratorMode::From(key) => self.db.iterator_cf(
                &cf,
                rocksdb::IteratorMode::From(key, rocksdb::Direction::Forward),
            ),
        };

        for item in iter {
            let (_key, value_bytes) =
                item.map_err(|e| Error::IteratorError(format!("Iterator error: {}", e)))?;

            let value: T = helpers::deserialize(&value_bytes)?;

            if !f(value) {
                return Ok(IterationStatus::StoppedEarly);
            }
        }

        Ok(IterationStatus::Completed)
    }

    /// Count the total number of items
    pub fn count(&self) -> Result<usize> {
        let cf = self.cf()?;
        let iter = match &self.mode {
            IteratorMode::Start => self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start),
            IteratorMode::From(key) => self.db.iterator_cf(
                &cf,
                rocksdb::IteratorMode::From(key, rocksdb::Direction::Forward),
            ),
        };

        let mut count = 0;
        for item in iter {
            item.map_err(|e| Error::IteratorError(format!("Iterator error: {}", e)))?;
            count += 1;
        }

        Ok(count)
    }
}

/// A database transaction for atomic operations
///
/// Transactions buffer all writes in memory and apply them atomically on commit.
/// This implementation is thread-safe and can be shared across threads.
///
/// # Cloning
///
/// Transaction intentionally does NOT implement Clone. Cloning a transaction would be
/// confusing and error-prone because:
/// - It's unclear whether clones share state or have independent caches
/// - Multiple clones committing could lead to unexpected behavior
/// - The transaction cache is meant to provide isolation for a single logical transaction
///
/// If you need to share a transaction across threads, use `Arc<Transaction>` instead.
///
/// # Examples
///
/// ```rust,no_run
/// # use ngdb::{Database, Storable};
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct Account { id: u64, balance: i64 }
/// # impl Storable for Account {
/// #     type Key = u64;
/// #     fn key(&self) -> u64 { self.id }
/// # }
/// # fn example(db: Database) -> Result<(), ngdb::Error> {
/// let txn = db.transaction()?;
/// let accounts = txn.collection::<Account>("accounts")?;
///
/// accounts.put(&Account { id: 1, balance: 100 })?;
/// accounts.put(&Account { id: 2, balance: 200 })?;
///
/// txn.commit()?;
/// # Ok(())
/// # }
/// ```
pub struct Transaction {
    db: Arc<rocksdb::DB>,
    batch: Mutex<RocksWriteBatch>,
    // Cache for read isolation: (cf_name, key_bytes) -> Option<value_bytes>
    // None means deleted in this transaction
    // WARNING: Large transactions can consume significant memory without any built-in limit.
    // Keep transactions small and commit frequently. Consider the following limits:
    // - Number of operations: Keep under 10,000 for best performance
    // - Total data size: Keep under 100MB to avoid excessive memory usage
    // If you need larger batch operations, use Batch instead of Transaction.
    cache: Mutex<HashMap<(String, Vec<u8>), Option<Vec<u8>>>>,
    shutdown: Arc<std::sync::RwLock<bool>>,
}

impl Transaction {
    fn new(db: Arc<rocksdb::DB>, shutdown: Arc<std::sync::RwLock<bool>>) -> Self {
        Self {
            db,
            batch: Mutex::new(RocksWriteBatch::default()),
            cache: Mutex::new(HashMap::new()),
            shutdown,
        }
    }

    #[inline]
    fn check_shutdown(&self) -> Result<std::sync::RwLockReadGuard<'_, bool>> {
        let guard = self
            .shutdown
            .read()
            .map_err(|e| Error::LockPoisoned(format!("Shutdown lock poisoned: {:?}", e)))?;

        if *guard {
            return Err(Error::Database("Database has been shut down".to_string()));
        }

        Ok(guard)
    }

    /// Get a typed collection within this transaction
    #[instrument(skip(self))]
    pub fn collection<'txn, T: Storable>(
        &'txn self,
        name: &str,
    ) -> Result<TransactionCollection<'txn, T>> {
        let _guard = self.check_shutdown()?;

        // Verify column family exists
        self.db.cf_handle(name).ok_or_else(|| {
            error!("Column family '{}' not found", name);
            Error::Database(format!("Column family '{}' not found", name))
        })?;

        debug!("Created transaction collection for '{}'", name);
        Ok(TransactionCollection::new(
            Arc::clone(&self.db),
            name.to_string(),
            &self.batch,
            &self.cache,
        ))
    }

    /// Commit the transaction
    ///
    /// All operations are applied atomically. If this fails, all changes are rolled back.
    #[instrument(skip(self))]
    pub fn commit(self) -> Result<()> {
        let guard = self.check_shutdown()?;
        drop(guard); // Drop guard before moving self's fields

        let db = self.db;
        let batch = self.batch.into_inner().map_err(|e| {
            error!("Failed to acquire batch lock during commit: {:?}", e);
            Error::LockPoisoned(format!("Batch lock poisoned during commit: {:?}", e))
        })?;
        let op_count = batch.len();

        info!("Committing transaction with {} operations", op_count);

        db.write(batch).map_err(|e| {
            error!("Failed to commit transaction: {}", e);
            Error::Database(format!("Failed to commit transaction: {}", e))
        })
    }

    /// Rollback the transaction
    ///
    /// All operations are discarded. This is done automatically by dropping the transaction.
    #[instrument(skip(self))]
    pub fn rollback(self) -> Result<()> {
        let op_count = self
            .batch
            .lock()
            .map_err(|e| {
                error!("Failed to acquire batch lock during rollback: {:?}", e);
                Error::LockPoisoned(format!("Batch lock poisoned during rollback: {:?}", e))
            })?
            .len();
        warn!("Rolling back transaction with {} operations", op_count);
        Ok(())
    }

    /// Clear all operations from the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the internal locks are poisoned (a thread panicked while holding the lock)
    pub fn clear(&self) -> Result<()> {
        self.batch
            .lock()
            .map_err(|e| {
                error!("Failed to acquire batch lock: {:?}", e);
                Error::LockPoisoned(format!("Batch lock poisoned during clear: {:?}", e))
            })?
            .clear();
        self.cache
            .lock()
            .map_err(|e| {
                error!("Failed to acquire cache lock: {:?}", e);
                Error::LockPoisoned(format!("Cache lock poisoned during clear: {:?}", e))
            })?
            .clear();
        Ok(())
    }

    /// Get the number of operations in the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the internal lock is poisoned (a thread panicked while holding the lock)
    pub fn len(&self) -> Result<usize> {
        Ok(self
            .batch
            .lock()
            .map_err(|e| {
                error!("Failed to acquire batch lock: {:?}", e);
                Error::LockPoisoned(format!("Batch lock poisoned during len: {:?}", e))
            })?
            .len())
    }

    /// Check if the transaction is empty
    ///
    /// # Errors
    ///
    /// Returns an error if the internal lock is poisoned (a thread panicked while holding the lock)
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self
            .batch
            .lock()
            .map_err(|e| {
                error!("Failed to acquire batch lock: {:?}", e);
                Error::LockPoisoned(format!("Batch lock poisoned during is_empty: {:?}", e))
            })?
            .is_empty())
    }
}

// SAFETY: Transaction can be safely sent between threads and shared across threads because:
// 1. All internal state is Send + Sync (Arc<DB>, Mutex<WriteBatch>, Mutex<HashMap>, Arc<AtomicBool>)
// 2. Mutex provides interior mutability with proper synchronization
// 3. The transaction cache is protected by Mutex, preventing data races
// 4. RocksDB WriteBatch is thread-safe when properly synchronized (which we do with Mutex)
unsafe impl Send for Transaction {}
unsafe impl Sync for Transaction {}

/// A typed collection view within a transaction
///
/// Provides read isolation - reads will see uncommitted writes made
/// within the same transaction.
pub struct TransactionCollection<'txn, T: Storable> {
    db: Arc<rocksdb::DB>,
    cf_name: String,
    batch: &'txn Mutex<RocksWriteBatch>,
    cache: &'txn Mutex<HashMap<(String, Vec<u8>), Option<Vec<u8>>>>,
    _phantom: PhantomData<T>,
}

impl<'txn, T: Storable> TransactionCollection<'txn, T> {
    fn new(
        db: Arc<rocksdb::DB>,
        cf_name: String,
        batch: &'txn Mutex<RocksWriteBatch>,
        cache: &'txn Mutex<HashMap<(String, Vec<u8>), Option<Vec<u8>>>>,
    ) -> Self {
        Self {
            db,
            cf_name,
            batch,
            cache,
            _phantom: PhantomData,
        }
    }

    fn cf<'a>(&'a self) -> Result<Arc<BoundColumnFamily<'a>>> {
        self.db
            .cf_handle(&self.cf_name)
            .ok_or_else(|| Error::Database(format!("Column family '{}' not found", self.cf_name)))
    }

    /// Store a value in the transaction
    ///
    /// The value is cached locally and will be visible to subsequent reads within
    /// the same transaction.
    #[instrument(skip(self, value))]
    pub fn put(&self, value: &T) -> Result<()> {
        // Validate first
        value.validate()?;

        let key = value.key();
        let key_bytes = key.to_bytes()?;
        let value_bytes = helpers::serialize(value)?;

        debug!("Transaction put in collection '{}'", self.cf_name);

        // Add to batch for commit
        let cf = self.cf()?;
        self.batch
            .lock()
            .map_err(|e| {
                error!("Failed to acquire batch lock: {:?}", e);
                Error::LockPoisoned(format!("Batch lock poisoned: {:?}", e))
            })?
            .put_cf(&cf, &key_bytes, &value_bytes);

        // Add to cache for read isolation
        self.cache
            .lock()
            .map_err(|e| {
                error!("Failed to acquire cache lock: {:?}", e);
                Error::LockPoisoned(format!("Cache lock poisoned: {:?}", e))
            })?
            .insert((self.cf_name.clone(), key_bytes), Some(value_bytes));

        value.on_stored();
        Ok(())
    }

    /// Get a value from the transaction
    ///
    /// This provides proper isolation - reads will see uncommitted writes made
    /// in the same transaction.
    #[instrument(skip(self))]
    pub fn get(&self, key: &T::Key) -> Result<Option<T>> {
        let key_bytes = key.to_bytes()?;

        // Check cache first (uncommitted writes)
        let cache_key = (self.cf_name.clone(), key_bytes.clone());
        let cached_value = self
            .cache
            .lock()
            .map_err(|e| {
                error!("Failed to acquire cache lock: {:?}", e);
                Error::LockPoisoned(format!("Cache lock poisoned: {:?}", e))
            })?
            .get(&cache_key)
            .cloned();

        if let Some(cached) = cached_value {
            debug!("Transaction cache hit for key in '{}'", self.cf_name);
            return match cached {
                Some(value_bytes) => {
                    let value: T = helpers::deserialize(&value_bytes)?;
                    Ok(Some(value))
                }
                None => Ok(None), // Deleted in transaction
            };
        }

        // Not in cache, read from committed state
        let cf = self.cf()?;
        match self.db.get_cf(&cf, key_bytes)? {
            Some(value_bytes) => {
                let value: T = helpers::deserialize(&value_bytes)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Delete a value in the transaction
    ///
    /// The deletion is cached locally and subsequent reads will return None.
    #[instrument(skip(self))]
    pub fn delete(&self, key: &T::Key) -> Result<()> {
        let key_bytes = key.to_bytes()?;

        debug!("Transaction delete in collection '{}'", self.cf_name);

        // Add to batch for commit
        let cf = self.cf()?;
        self.batch
            .lock()
            .map_err(|e| {
                error!("Failed to acquire batch lock: {:?}", e);
                Error::LockPoisoned(format!("Batch lock poisoned: {:?}", e))
            })?
            .delete_cf(&cf, &key_bytes);

        // Add to cache as deleted (None value)
        self.cache
            .lock()
            .map_err(|e| {
                error!("Failed to acquire cache lock: {:?}", e);
                Error::LockPoisoned(format!("Cache lock poisoned: {:?}", e))
            })?
            .insert((self.cf_name.clone(), key_bytes), None);

        Ok(())
    }

    /// Check if a key exists
    pub fn exists(&self, key: &T::Key) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Retrieve multiple values at once using optimized multi_get
    ///
    /// This provides proper transaction isolation - reads will see uncommitted writes
    /// made in the same transaction. This is significantly faster than calling `get()`
    /// multiple times for keys not in the transaction cache.
    ///
    /// # Arguments
    ///
    /// * `keys` - Slice of keys to retrieve
    ///
    /// # Returns
    ///
    /// A vector of optional values in the same order as the input keys
    #[instrument(skip(self, keys))]
    pub fn get_many(&self, keys: &[T::Key]) -> Result<Vec<Option<T>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // Pre-allocate results with exact size
        let mut results: Vec<Option<T>> = (0..keys.len()).map(|_| None).collect();
        let mut uncached_indices = Vec::new();
        let mut uncached_keys = Vec::new();

        // First pass: check cache for all keys
        // We intentionally drop the lock before DB access to avoid holding it during I/O
        let cache = self.cache.lock().map_err(|e| {
            error!("Failed to acquire cache lock: {:?}", e);
            Error::LockPoisoned(format!("Cache lock poisoned: {:?}", e))
        })?;

        for (i, key) in keys.iter().enumerate() {
            let key_bytes = key.to_bytes()?;
            let cache_key = (self.cf_name.clone(), key_bytes.clone());

            if let Some(cached) = cache.get(&cache_key) {
                // In cache - resolve immediately
                results[i] = match cached {
                    Some(value_bytes) => Some(helpers::deserialize(value_bytes)?),
                    None => None, // Deleted in transaction
                };
            } else {
                // Not in cache - need to fetch from DB
                uncached_indices.push(i);
                uncached_keys.push(key_bytes);
            }
        }

        drop(cache); // Release lock before DB access

        // Second pass: batch fetch uncached keys from DB
        if !uncached_keys.is_empty() {
            let cf = self.cf()?;
            let cf_refs: Vec<_> = uncached_keys.iter().map(|k| (&cf, k.as_slice())).collect();
            let db_results = self.db.multi_get_cf(cf_refs);

            for (result_idx, db_result) in db_results.into_iter().enumerate() {
                let original_idx = uncached_indices[result_idx];
                results[original_idx] = match db_result {
                    Ok(Some(value_bytes)) => Some(helpers::deserialize(&value_bytes)?),
                    Ok(None) => None,
                    Err(e) => return Err(Error::Database(format!("Multi-get failed: {}", e))),
                };
            }
        }

        Ok(results)
    }
}

// SAFETY: TransactionCollection can be safely sent between threads and shared across threads because:
// 1. All internal state is Send + Sync (Arc<DB>, String, &'txn Mutex<...>)
// 2. T: Storable which requires T: Send + Sync
// 3. The 'txn lifetime ensures the Transaction outlives this collection
// 4. All mutations go through the parent Transaction's Mutex-protected state
unsafe impl<'txn, T: Storable> Send for TransactionCollection<'txn, T> {}
unsafe impl<'txn, T: Storable> Sync for TransactionCollection<'txn, T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DatabaseConfig;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestItem {
        id: u64,
        data: String,
    }

    impl Storable for TestItem {
        type Key = u64;
        fn key(&self) -> Self::Key {
            self.id
        }
    }

    fn create_test_db() -> Database {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!("ngdb_test_{}", id));
        let _ = std::fs::remove_dir_all(&path);

        DatabaseConfig::new(&path)
            .create_if_missing(true)
            .add_column_family("test")
            .open()
            .expect("Failed to create test database")
    }

    #[test]
    fn test_collection_put_and_get() {
        let db = create_test_db();
        let collection = db.collection::<TestItem>("test").unwrap();

        let item = TestItem {
            id: 1,
            data: "test".to_string(),
        };

        collection.put(&item).unwrap();
        let retrieved = collection.get(&1).unwrap();

        assert_eq!(Some(item), retrieved);
    }

    #[test]
    fn test_collection_delete() {
        let db = create_test_db();
        let collection = db.collection::<TestItem>("test").unwrap();

        let item = TestItem {
            id: 1,
            data: "test".to_string(),
        };

        collection.put(&item).unwrap();
        collection.delete(&1).unwrap();

        assert_eq!(None, collection.get(&1).unwrap());
    }

    #[test]
    fn test_batch() {
        let db = create_test_db();
        let collection = db.collection::<TestItem>("test").unwrap();

        let mut batch = collection.batch();
        for i in 0..10 {
            batch
                .put(&TestItem {
                    id: i,
                    data: format!("item_{}", i),
                })
                .unwrap();
        }
        batch.commit().unwrap();

        for i in 0..10 {
            let item = collection.get(&i).unwrap().unwrap();
            assert_eq!(i, item.id);
        }
    }

    #[test]
    fn test_iterator() {
        let db = create_test_db();
        let collection = db.collection::<TestItem>("test").unwrap();

        for i in 0..5 {
            collection
                .put(&TestItem {
                    id: i,
                    data: format!("item_{}", i),
                })
                .unwrap();
        }

        let items = collection.iter().collect_all().unwrap();
        assert_eq!(5, items.len());
    }

    #[test]
    fn test_get_many() {
        let db = create_test_db();
        let collection = db.collection::<TestItem>("test").unwrap();

        // Insert test data
        for i in 0..10 {
            collection
                .put(&TestItem {
                    id: i,
                    data: format!("item_{}", i),
                })
                .unwrap();
        }

        // Test multi-get
        let keys = vec![1, 3, 5, 99]; // 99 doesn't exist
        let results = collection.get_many(&keys).unwrap();

        assert_eq!(4, results.len());
        assert!(results[0].is_some());
        assert_eq!(1, results[0].as_ref().unwrap().id);
        assert!(results[1].is_some());
        assert_eq!(3, results[1].as_ref().unwrap().id);
        assert!(results[2].is_some());
        assert_eq!(5, results[2].as_ref().unwrap().id);
        assert!(results[3].is_none());
    }

    #[test]
    fn test_transaction() {
        let db = create_test_db();
        let txn = db.transaction().unwrap();
        let collection = txn.collection::<TestItem>("test").unwrap();

        collection
            .put(&TestItem {
                id: 1,
                data: "test".to_string(),
            })
            .unwrap();

        // Should see uncommitted write
        assert!(collection.get(&1).unwrap().is_some());

        txn.commit().unwrap();

        // Should see committed write
        let regular_collection = db.collection::<TestItem>("test").unwrap();
        assert!(regular_collection.get(&1).unwrap().is_some());
    }

    #[test]
    fn test_transaction_get_many() {
        let db = create_test_db();
        let collection = db.collection::<TestItem>("test").unwrap();

        // Pre-populate some data
        collection
            .put(&TestItem {
                id: 1,
                data: "one".to_string(),
            })
            .unwrap();
        collection
            .put(&TestItem {
                id: 2,
                data: "two".to_string(),
            })
            .unwrap();
        collection
            .put(&TestItem {
                id: 5,
                data: "five".to_string(),
            })
            .unwrap();

        let txn = db.transaction().unwrap();
        let txn_collection = txn.collection::<TestItem>("test").unwrap();

        // Write new items in transaction
        txn_collection
            .put(&TestItem {
                id: 3,
                data: "three".to_string(),
            })
            .unwrap();
        txn_collection
            .put(&TestItem {
                id: 4,
                data: "four".to_string(),
            })
            .unwrap();

        // Delete an existing item
        txn_collection.delete(&5).unwrap();

        // Test get_many with mix of: committed, uncommitted, deleted, non-existent
        let keys = vec![1, 2, 3, 4, 5, 6];
        let results = txn_collection.get_many(&keys).unwrap();

        // Verify results
        assert!(results[0].is_some()); // 1: exists (committed)
        assert_eq!(results[0].as_ref().unwrap().data, "one");

        assert!(results[1].is_some()); // 2: exists (committed)
        assert_eq!(results[1].as_ref().unwrap().data, "two");

        assert!(results[2].is_some()); // 3: exists (uncommitted in txn)
        assert_eq!(results[2].as_ref().unwrap().data, "three");

        assert!(results[3].is_some()); // 4: exists (uncommitted in txn)
        assert_eq!(results[3].as_ref().unwrap().data, "four");

        assert!(results[4].is_none()); // 5: deleted in transaction
        assert!(results[5].is_none()); // 6: never existed

        // Verify transaction hasn't affected committed state
        let committed_results = collection.get_many(&keys).unwrap();
        assert!(committed_results[2].is_none()); // 3 doesn't exist yet
        assert!(committed_results[3].is_none()); // 4 doesn't exist yet
        assert!(committed_results[4].is_some()); // 5 still exists

        // Commit and verify
        txn.commit().unwrap();

        let final_results = collection.get_many(&keys).unwrap();
        assert!(final_results[2].is_some()); // 3 now exists
        assert!(final_results[3].is_some()); // 4 now exists
        assert!(final_results[4].is_none()); // 5 now deleted
    }
}
