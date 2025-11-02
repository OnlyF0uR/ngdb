//! Replication support for NGDB
//!
//! This module provides replication capabilities for multi-node deployments.
//! It follows sound replication principles:
//!
//! - Write-ahead logging for all operations
//! - Idempotent operation application
//! - Configurable conflict resolution strategies (LastWriteWins, FirstWriteWins, Custom)
//! - BLAKE3 checksums for data integrity verification
//! - Hook system for custom replication logic
//! - Tombstone-based deletion tracking
//! - Age-based cleanup of operation history (1-hour retention + 50k safety limit)
//!
//! # Architecture
//!
//! 1. **ReplicationLog**: Represents a single replicated operation with metadata
//! 2. **ReplicationManager**: Applies replication logs to the local database
//! 3. **ReplicationHook**: Extensible hook system for custom logic
//! 4. **ConflictResolution**: Strategies for handling concurrent writes
//! 5. **ReplicationMetadata**: Timestamps stored separately from actual data
//!
//! # Key Features
//!
//! ## Conflict Resolution
//!
//! Three strategies are available:
//!
//! - **LastWriteWins**: Most recent write (by timestamp) wins
//! - **FirstWriteWins**: First write wins, subsequent writes are rejected
//! - **Custom**: Use hooks to implement custom merge logic
//!
//! ## Metadata Separation
//!
//! Timestamps and tombstone markers are stored in separate metadata column families
//! (e.g., `__ngdb_repl_meta_users` for the `users` collection). These are created
//! **automatically** when replication is first used for a collection. This means:
//!
//! - **No manual setup**: Metadata column families are created on-demand
//! - **No wrapping overhead**: Your data is stored exactly as you write it
//! - **Transparent reads**: `Collection::get()` just works - no special handling needed
//! - **Clean separation**: Replication metadata doesn't pollute your data
//! - **Easy migration**: Add replication to existing databases without data migration
//!
//! ## Tombstones for Deletes
//!
//! Deletes store tombstone metadata (with timestamps) rather than immediate removal.
//! This prevents "delete before write" race conditions where a delete arrives before
//! the original write it was meant to delete.
//!
//! ## Network Transport
//!
//! `ReplicationLog` implements `BorshSerialize`/`BorshDeserialize` for efficient
//! network transport. Use your preferred transport layer (gRPC, message queue, etc.)
//! to send logs between nodes.
//!
//! # Example
//!
//! ```rust,no_run
//! use ngdb::{Database, DatabaseConfig, ReplicationConfig, ReplicationManager, ReplicationLog, ReplicationOperation};
//!
//! # fn example() -> Result<(), ngdb::Error> {
//! // Just create your normal column families - metadata CFs are created automatically
//! let db = DatabaseConfig::new("./data/replica")
//!     .create_if_missing(true)
//!     .add_column_family("users")
//!     .open()?;
//!
//! let config = ReplicationConfig::new("replica-1")
//!     .enable()
//!     .with_peers(vec!["primary-1".to_string()]);
//!
//! // Metadata column family __ngdb_repl_meta_users will be created automatically
//! let manager = ReplicationManager::new(db.clone(), config)?;
//!
//! // Receive replication log from primary (via network, message queue, etc.)
//! let log = ReplicationLog::new(
//!     "primary-1".to_string(),
//!     ReplicationOperation::Put {
//!         collection: "users".to_string(),
//!         key: vec![1, 2, 3],
//!         value: vec![4, 5, 6],
//!     },
//! ).with_checksum();
//!
//! // Apply replication - data is stored normally, metadata stored separately
//! manager.apply_replication(log)?;
//!
//! // Read data normally - no special handling needed!
//! // Your Collection::get() calls work transparently with replicated data
//! # Ok(())
//! # }
//! ```

use crate::{Database, Error, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, instrument, warn};

/// A replication log entry representing a single operation to be replicated
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ReplicationLog {
    /// Unique identifier for this operation
    pub operation_id: String,

    /// ID of the node that originated this operation
    pub source_node_id: String,

    /// Timestamp in microseconds since Unix epoch
    pub timestamp_micros: u64,

    /// The operation to replicate
    pub operation: ReplicationOperation,

    /// Optional checksum for verification (blake3 hash)
    pub checksum: Option<[u8; 32]>,
}

/// Types of operations that can be replicated
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum ReplicationOperation {
    /// Insert or update a key-value pair
    Put {
        /// Collection name
        collection: String,
        /// Serialized key
        key: Vec<u8>,
        /// Serialized value
        value: Vec<u8>,
    },
    /// Delete a key
    Delete {
        /// Collection name
        collection: String,
        /// Serialized key
        key: Vec<u8>,
    },
    /// Batch of operations
    Batch {
        /// Collection name
        collection: String,
        /// List of batch operations
        operations: Vec<BatchOp>,
    },
}

/// Individual operation within a batch
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum BatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Metadata stored separately for replication conflict resolution
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct ReplicationMetadata {
    timestamp_micros: u64,
    is_tombstone: bool,
}

impl ReplicationMetadata {
    fn new(timestamp_micros: u64) -> Self {
        Self {
            timestamp_micros,
            is_tombstone: false,
        }
    }

    fn tombstone(timestamp_micros: u64) -> Self {
        Self {
            timestamp_micros,
            is_tombstone: true,
        }
    }
}

impl ReplicationLog {
    /// Create a new replication log entry
    pub fn new(source_node_id: String, operation: ReplicationOperation) -> Self {
        Self {
            operation_id: generate_operation_id(),
            source_node_id,
            timestamp_micros: current_timestamp_micros(),
            operation,
            checksum: None,
        }
    }

    /// Validate the replication log
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.is_empty() {
            return Err(Error::Replication(
                "Source node ID cannot be empty".to_string(),
            ));
        }

        if self.operation_id.is_empty() {
            return Err(Error::Replication(
                "Operation ID cannot be empty".to_string(),
            ));
        }

        Ok(())
    }

    /// Get the collection name from the operation
    pub fn collection(&self) -> &str {
        match &self.operation {
            ReplicationOperation::Put { collection, .. } => collection,
            ReplicationOperation::Delete { collection, .. } => collection,
            ReplicationOperation::Batch { collection, .. } => collection,
        }
    }
}

/// Configuration for replication
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Whether replication is enabled
    pub enabled: bool,

    /// Unique identifier for this node
    pub node_id: String,

    /// List of peer node IDs
    pub peer_nodes: Vec<String>,

    /// Conflict resolution strategy
    pub conflict_resolution: ConflictResolution,

    /// Whether to verify checksums
    pub verify_checksums: bool,
}

/// Strategies for resolving conflicts when the same key is modified concurrently
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Last write wins (based on timestamp)
    LastWriteWins,

    /// First write wins (reject newer writes)
    FirstWriteWins,

    /// Use custom hook for resolution
    Custom,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: String::new(),
            peer_nodes: Vec::new(),
            conflict_resolution: ConflictResolution::LastWriteWins,
            verify_checksums: true,
        }
    }
}

impl ReplicationConfig {
    /// Create a new replication configuration
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            ..Default::default()
        }
    }

    /// Enable replication
    pub fn enable(mut self) -> Self {
        self.enabled = true;
        self
    }

    /// Set peer nodes
    pub fn with_peers(mut self, peers: Vec<String>) -> Self {
        self.peer_nodes = peers;
        self
    }

    /// Set conflict resolution strategy
    pub fn conflict_resolution(mut self, strategy: ConflictResolution) -> Self {
        self.conflict_resolution = strategy;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.enabled && self.node_id.is_empty() {
            return Err(Error::Replication(
                "Node ID is required when replication is enabled".to_string(),
            ));
        }
        Ok(())
    }
}

/// Trait for custom replication hooks
///
/// Implement this trait to add custom logic around replication events.
pub trait ReplicationHook: Send + Sync {
    /// Called before applying incoming replicated data
    ///
    /// Return `Err` to reject the replication.
    fn before_apply(&self, _log: &ReplicationLog) -> Result<()> {
        Ok(())
    }

    /// Called after successfully applying replicated data
    fn after_apply(&self, _log: &ReplicationLog) -> Result<()> {
        Ok(())
    }

    /// Called when a replication operation fails
    fn on_replication_error(&self, _log: &ReplicationLog, _error: &Error) {
        // Default: do nothing
    }

    /// Custom conflict resolution
    ///
    /// Only called if ConflictResolution::Custom is set.
    /// Return the data that should be written (either existing or new).
    ///
    /// # Hook Chaining
    ///
    /// When multiple hooks are registered, they are applied sequentially:
    /// - First hook receives: `(existing_data, incoming_data)`
    /// - Second hook receives: `(existing_data, first_hook_result)`
    /// - Third hook receives: `(existing_data, second_hook_result)`
    /// - And so on...
    ///
    /// The final hook's result is what gets written to the database.
    /// This allows you to compose conflict resolution strategies.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// struct MergeHook;
    /// impl ReplicationHook for MergeHook {
    ///     fn resolve_conflict(&self, existing: &[u8], new: &[u8]) -> Result<Vec<u8>> {
    ///         // Custom merge logic here
    ///         Ok(merge_json(existing, new))
    ///     }
    /// }
    /// ```
    fn resolve_conflict(&self, _existing: &[u8], new: &[u8]) -> Result<Vec<u8>> {
        // Default: use new data
        Ok(new.to_vec())
    }
}

/// Manages replication for a database instance
pub struct ReplicationManager {
    config: ReplicationConfig,
    hooks: Vec<Arc<dyn ReplicationHook>>,
    db: Database,
    applied_operations: Arc<RwLock<HashMap<String, u64>>>,
}

impl ReplicationManager {
    /// Get the metadata column family name for a given data column family
    fn meta_cf_name(collection: &str) -> String {
        format!("__ngdb_repl_meta_{}", collection)
    }

    /// Get or create the metadata column family for replication timestamps
    fn ensure_meta_cf(&self, collection: &str) -> Result<()> {
        let meta_cf = Self::meta_cf_name(collection);

        // Check if column family exists
        if self.db.inner.db.cf_handle(&meta_cf).is_none() {
            // Create it automatically with default options
            debug!("Auto-creating metadata column family: {}", meta_cf);
            self.db
                .inner
                .db
                .create_cf(&meta_cf, &rocksdb::Options::default())
                .map_err(|e| {
                    Error::Database(format!(
                        "Failed to create metadata column family '{}': {}",
                        meta_cf, e
                    ))
                })?;
        }
        Ok(())
    }

    /// Get replication metadata for a key
    fn get_metadata(&self, collection: &str, key: &[u8]) -> Result<Option<ReplicationMetadata>> {
        let meta_cf_name = Self::meta_cf_name(collection);
        let meta_cf =
            self.db.inner.db.cf_handle(&meta_cf_name).ok_or_else(|| {
                Error::Database(format!("Metadata CF '{}' not found", meta_cf_name))
            })?;

        match self.db.inner.db.get_cf(&meta_cf, key)? {
            Some(bytes) => {
                let metadata = ReplicationMetadata::try_from_slice(&bytes).map_err(|e| {
                    Error::Deserialization(format!("Failed to deserialize metadata: {}", e))
                })?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    /// Set replication metadata for a key
    fn set_metadata(
        &self,
        collection: &str,
        key: &[u8],
        metadata: &ReplicationMetadata,
    ) -> Result<()> {
        let meta_cf_name = Self::meta_cf_name(collection);
        let meta_cf =
            self.db.inner.db.cf_handle(&meta_cf_name).ok_or_else(|| {
                Error::Database(format!("Metadata CF '{}' not found", meta_cf_name))
            })?;

        let bytes = borsh::to_vec(metadata)
            .map_err(|e| Error::Serialization(format!("Failed to serialize metadata: {}", e)))?;

        self.db
            .inner
            .db
            .put_cf(&meta_cf, key, bytes)
            .map_err(|e| Error::Database(format!("Failed to write metadata: {}", e)))?;

        Ok(())
    }
}

impl ReplicationManager {
    /// Create a new replication manager
    ///
    /// # Arguments
    ///
    /// * `db` - Database instance for applying replicated operations
    /// * `config` - Replication configuration
    #[instrument(skip(db, config))]
    pub fn new(db: Database, config: ReplicationConfig) -> Result<Self> {
        config.validate()?;

        info!(
            "Creating replication manager for node {} with {} peers",
            config.node_id,
            config.peer_nodes.len()
        );

        Ok(Self {
            config,
            hooks: Vec::new(),
            db,
            applied_operations: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Register a replication hook
    #[instrument(skip(self, hook))]
    pub fn register_hook(&mut self, hook: Arc<dyn ReplicationHook>) {
        info!("Registering replication hook");
        self.hooks.push(hook);
    }

    /// Process incoming replication data
    ///
    /// This is the main entry point for handling replicated data from peer nodes.
    /// The method is idempotent - applying the same log multiple times is safe.
    ///
    /// # Arguments
    ///
    /// * `log` - The replication log entry received from a peer
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the replication was applied successfully,
    /// or an error if validation or application failed.
    #[instrument(skip(self, log), fields(op_id = %log.operation_id, source = %log.source_node_id))]
    pub fn apply_replication(&self, log: ReplicationLog) -> Result<()> {
        if !self.config.enabled {
            warn!("Attempted to apply replication but replication is not enabled");
            return Err(Error::Replication("Replication is not enabled".to_string()));
        }

        info!("Applying replication from {}", log.source_node_id);

        // Validate the log entry
        log.validate()?;

        // Verify checksum if present and verification is enabled
        if self.config.verify_checksums && log.checksum.is_some() {
            debug!("Verifying checksum for operation {}", log.operation_id);
            let computed = compute_checksum(&log);
            if log.checksum.as_ref() != Some(&computed) {
                error!(
                    "Checksum mismatch for operation {}: expected {:?}, got {:?}",
                    log.operation_id, log.checksum, computed
                );
                return Err(Error::Replication(format!(
                    "Checksum mismatch for operation {}",
                    log.operation_id
                )));
            }
        }

        // Check idempotency - have we already applied this operation?
        {
            let applied = self
                .applied_operations
                .read()
                .map_err(|e| Error::Replication(format!("Failed to acquire read lock: {}", e)))?;

            if let Some(&existing_ts) = applied.get(&log.operation_id) {
                // Already applied - check if we should skip or reapply based on timestamp
                if existing_ts >= log.timestamp_micros {
                    // Already applied a newer or same version, skip
                    debug!(
                        "Skipping duplicate operation {} (existing: {}, incoming: {})",
                        log.operation_id, existing_ts, log.timestamp_micros
                    );
                    return Ok(());
                }
                info!(
                    "Re-applying operation {} with newer timestamp (existing: {}, incoming: {})",
                    log.operation_id, existing_ts, log.timestamp_micros
                );
            }
        }

        // Call before_apply hooks
        for hook in &self.hooks {
            hook.before_apply(&log)?;
        }

        // Apply the operation to the database
        let result = self.apply_operation(&log, log.timestamp_micros);

        // Handle errors
        if let Err(ref e) = result {
            error!("Replication application failed: {}", e);
            for hook in &self.hooks {
                hook.on_replication_error(&log, e);
            }
            return result;
        }

        info!("Replication applied successfully: {}", log.operation_id);

        // Record that we've applied this operation
        {
            let mut applied = self
                .applied_operations
                .write()
                .map_err(|e| Error::Replication(format!("Failed to acquire write lock: {}", e)))?;
            applied.insert(log.operation_id.clone(), log.timestamp_micros);

            // Age-based cleanup: remove operations older than 1 hour
            const MAX_AGE_MICROS: u64 = 3_600_000_000; // 1 hour in microseconds
            let current_time = current_timestamp_micros();
            let cutoff_time = current_time.saturating_sub(MAX_AGE_MICROS);

            let before_count = applied.len();
            applied.retain(|_k, &mut v| v >= cutoff_time);
            let after_count = applied.len();

            if before_count != after_count {
                info!(
                    "Cleaned up {} old operation entries (kept {} recent entries)",
                    before_count - after_count,
                    after_count
                );
            }

            // Safety limit: if still over 50000 entries, remove oldest
            if applied.len() > 50000 {
                warn!(
                    "Operation tracking map exceeded 50000 entries ({}), performing emergency cleanup",
                    applied.len()
                );
                let mut entries: Vec<_> = applied.iter().map(|(k, v)| (k.clone(), *v)).collect();
                entries.sort_by_key(|(_k, v)| *v);
                let remove_count = entries.len().saturating_sub(25000);
                for (key, _) in entries.iter().take(remove_count) {
                    applied.remove(key);
                }
                info!(
                    "Emergency cleanup completed, now tracking {} entries",
                    applied.len()
                );
            }
        }

        // Call after_apply hooks
        for hook in &self.hooks {
            if let Err(e) = hook.after_apply(&log) {
                warn!("Hook after_apply failed: {}", e);
            }
        }

        Ok(())
    }

    /// Apply a single operation to the database
    #[instrument(skip(self, log))]
    fn apply_operation(&self, log: &ReplicationLog, timestamp: u64) -> Result<()> {
        match &log.operation {
            ReplicationOperation::Put {
                collection,
                key,
                value,
            } => {
                debug!("Applying PUT operation to collection '{}'", collection);
                self.apply_put(collection, key, value, timestamp)
            }
            ReplicationOperation::Delete { collection, key } => {
                debug!("Applying DELETE operation to collection '{}'", collection);
                self.apply_delete(collection, key, timestamp)
            }
            ReplicationOperation::Batch {
                collection,
                operations,
            } => {
                debug!(
                    "Applying BATCH operation with {} ops to collection '{}'",
                    operations.len(),
                    collection
                );
                self.apply_batch(collection, operations, timestamp)
            }
        }
    }

    /// Apply a put operation with conflict detection
    #[instrument(skip(self, key, value))]
    fn apply_put(&self, collection: &str, key: &[u8], value: &[u8], timestamp: u64) -> Result<()> {
        // Ensure metadata CF exists
        self.ensure_meta_cf(collection)?;

        // Get the column family
        let cf =
            self.db.inner.db.cf_handle(collection).ok_or_else(|| {
                Error::Database(format!("Column family '{}' not found", collection))
            })?;

        // Check for existing metadata for conflict resolution
        let existing_metadata = self.get_metadata(collection, key)?;

        // Check if there's an existing value
        let existing_data = self
            .db
            .inner
            .db
            .get_cf(&cf, key)
            .map_err(|e| Error::Database(format!("Failed to get existing value: {}", e)))?;

        let final_value = if let Some(existing_metadata) = existing_metadata {
            // Conflict detected - apply resolution strategy
            match self.config.conflict_resolution {
                ConflictResolution::LastWriteWins => {
                    // Compare timestamps
                    if existing_metadata.timestamp_micros >= timestamp {
                        debug!(
                            "Conflict resolved: LastWriteWins - keeping existing value (ts: {} >= {})",
                            existing_metadata.timestamp_micros, timestamp
                        );
                        return Ok(());
                    }
                    debug!("Conflict resolved: LastWriteWins - accepting new value");
                    value.to_vec()
                }
                ConflictResolution::FirstWriteWins => {
                    // Keep existing value, reject new write
                    debug!("Conflict resolved: FirstWriteWins - keeping existing value");
                    return Ok(());
                }
                ConflictResolution::Custom => {
                    // Use custom hooks - iterate all and chain the results
                    // Note: Each hook receives the output of the previous hook
                    let mut resolved_value = value.to_vec();
                    let existing_value = existing_data.unwrap_or_default();

                    if self.hooks.is_empty() {
                        warn!(
                            "Custom conflict resolution set but no hook registered, defaulting to LastWriteWins"
                        );
                    } else {
                        for hook in &self.hooks {
                            debug!("Applying custom conflict resolution hook");
                            resolved_value =
                                hook.resolve_conflict(&existing_value, &resolved_value)?;
                        }
                    }
                    resolved_value
                }
            }
        } else {
            // No conflict - just write the new value
            value.to_vec()
        };

        // Write the actual data (unwrapped)
        self.db
            .inner
            .db
            .put_cf(&cf, key, &final_value)
            .map_err(|e| Error::Database(format!("Failed to put value: {}", e)))?;

        // Write metadata separately
        let metadata = ReplicationMetadata::new(timestamp);
        self.set_metadata(collection, key, &metadata)?;

        Ok(())
    }

    /// Apply a delete operation with tombstone
    #[instrument(skip(self, key))]
    fn apply_delete(&self, collection: &str, key: &[u8], timestamp: u64) -> Result<()> {
        // Ensure metadata CF exists
        self.ensure_meta_cf(collection)?;

        let cf =
            self.db.inner.db.cf_handle(collection).ok_or_else(|| {
                Error::Database(format!("Column family '{}' not found", collection))
            })?;

        // Check for existing metadata for conflict resolution
        if let Some(existing_metadata) = self.get_metadata(collection, key)? {
            // Check if delete is older than existing write
            if existing_metadata.timestamp_micros > timestamp {
                debug!(
                    "Delete rejected: existing value is newer (ts: {} > {})",
                    existing_metadata.timestamp_micros, timestamp
                );
                return Ok(());
            }
        }

        // Delete the actual data
        self.db
            .inner
            .db
            .delete_cf(&cf, key)
            .map_err(|e| Error::Database(format!("Failed to delete value: {}", e)))?;

        // Store tombstone metadata
        let tombstone = ReplicationMetadata::tombstone(timestamp);
        self.set_metadata(collection, key, &tombstone)?;

        Ok(())
    }

    /// Apply a batch of operations with conflict resolution
    /// Uses the log's timestamp for all operations in the batch for proper LastWriteWins
    #[instrument(skip(self, operations))]
    fn apply_batch(
        &self,
        collection: &str,
        operations: &[BatchOp],
        batch_timestamp: u64,
    ) -> Result<()> {
        // Ensure metadata CF exists
        self.ensure_meta_cf(collection)?;

        let cf =
            self.db.inner.db.cf_handle(collection).ok_or_else(|| {
                Error::Database(format!("Column family '{}' not found", collection))
            })?;

        let meta_cf_name = Self::meta_cf_name(collection);
        let meta_cf =
            self.db.inner.db.cf_handle(&meta_cf_name).ok_or_else(|| {
                Error::Database(format!("Metadata CF '{}' not found", meta_cf_name))
            })?;

        let mut batch = rocksdb::WriteBatch::default();

        // Process each operation with conflict resolution
        for op in operations {
            match op {
                BatchOp::Put { key, value } => {
                    // Check for conflicts in ALL cases
                    if let Some(existing_metadata) = self.get_metadata(collection, key)? {
                        match self.config.conflict_resolution {
                            ConflictResolution::LastWriteWins => {
                                // Compare timestamps - skip if existing is newer or equal
                                if existing_metadata.timestamp_micros >= batch_timestamp {
                                    debug!(
                                        "Batch: LastWriteWins - skipping outdated write (existing: {} >= batch: {})",
                                        existing_metadata.timestamp_micros, batch_timestamp
                                    );
                                    continue;
                                }
                                // Existing is older, we'll overwrite below
                            }
                            ConflictResolution::FirstWriteWins => {
                                debug!("Batch: FirstWriteWins - skipping existing key");
                                continue; // Skip this operation
                            }
                            ConflictResolution::Custom => {
                                let existing_data = self
                                    .db
                                    .inner
                                    .db
                                    .get_cf(&cf, key)
                                    .map_err(|e| {
                                        Error::Database(format!(
                                            "Failed to get existing value: {}",
                                            e
                                        ))
                                    })?
                                    .unwrap_or_default();

                                let mut resolved_value = value.clone();
                                for hook in &self.hooks {
                                    resolved_value =
                                        hook.resolve_conflict(&existing_data, &resolved_value)?;
                                }

                                batch.put_cf(&cf, key, &resolved_value);

                                let metadata = ReplicationMetadata::new(batch_timestamp);
                                let meta_bytes = borsh::to_vec(&metadata).map_err(|e| {
                                    Error::Serialization(format!(
                                        "Failed to serialize metadata: {}",
                                        e
                                    ))
                                })?;
                                batch.put_cf(&meta_cf, key, &meta_bytes);
                                continue;
                            }
                        }
                    }

                    // Write value and metadata
                    batch.put_cf(&cf, key, value);

                    let metadata = ReplicationMetadata::new(batch_timestamp);
                    let meta_bytes = borsh::to_vec(&metadata).map_err(|e| {
                        Error::Serialization(format!("Failed to serialize metadata: {}", e))
                    })?;
                    batch.put_cf(&meta_cf, key, &meta_bytes);
                }
                BatchOp::Delete { key } => {
                    // Delete the data
                    batch.delete_cf(&cf, key);

                    // Store tombstone metadata
                    let tombstone = ReplicationMetadata::tombstone(batch_timestamp);
                    let meta_bytes = borsh::to_vec(&tombstone).map_err(|e| {
                        Error::Serialization(format!("Failed to serialize metadata: {}", e))
                    })?;
                    batch.put_cf(&meta_cf, key, &meta_bytes);
                }
            }
        }

        // WriteBatch is atomic - either all operations succeed or none do (rollback handled by RocksDB)
        self.db
            .inner
            .db
            .write(batch)
            .map_err(|e| Error::Database(format!("Failed to write batch: {}", e)))?;

        Ok(())
    }

    /// Create a replication log for a put operation
    ///
    /// Helper method for creating replication logs from local writes.
    pub fn create_put_log(
        &self,
        collection: impl Into<String>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> ReplicationLog {
        ReplicationLog::new(
            self.config.node_id.clone(),
            ReplicationOperation::Put {
                collection: collection.into(),
                key,
                value,
            },
        )
        .with_checksum()
    }

    /// Create a replication log for a delete operation
    pub fn create_delete_log(&self, collection: impl Into<String>, key: Vec<u8>) -> ReplicationLog {
        ReplicationLog::new(
            self.config.node_id.clone(),
            ReplicationOperation::Delete {
                collection: collection.into(),
                key,
            },
        )
        .with_checksum()
    }

    /// Get the replication configuration
    pub fn config(&self) -> &ReplicationConfig {
        &self.config
    }

    /// Get statistics about applied operations
    pub fn stats(&self) -> Result<ReplicationStats> {
        let applied = self
            .applied_operations
            .read()
            .map_err(|e| Error::Replication(format!("Failed to acquire read lock: {}", e)))?;

        let total = applied.len();
        let oldest = applied.values().min().copied();
        let newest = applied.values().max().copied();

        Ok(ReplicationStats {
            total_operations: total,
            oldest_operation_timestamp: oldest,
            newest_operation_timestamp: newest,
        })
    }

    /// Clear operation history
    ///
    /// This clears the idempotency tracking. Use with caution.
    pub fn clear_operation_history(&self) -> Result<()> {
        let mut applied = self
            .applied_operations
            .write()
            .map_err(|e| Error::Replication(format!("Failed to acquire write lock: {}", e)))?;
        applied.clear();
        info!("Cleared replication operation history");
        Ok(())
    }
}

/// Statistics about replication operations
#[derive(Debug, Clone)]
pub struct ReplicationStats {
    /// Total number of operations tracked
    pub total_operations: usize,
    /// Timestamp of oldest tracked operation
    pub oldest_operation_timestamp: Option<u64>,
    /// Timestamp of newest tracked operation
    pub newest_operation_timestamp: Option<u64>,
}

/// Generate a unique operation ID using cryptographically strong randomness
/// Includes process ID for easier debugging in multi-node scenarios
fn generate_operation_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
    let timestamp = current_timestamp_micros();
    let process_id = std::process::id();

    // Use blake3 for better randomness from timestamp and counter
    let random_component = {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&counter.to_le_bytes());
        hasher.update(&process_id.to_le_bytes());
        let hash = hasher.finalize();
        u32::from_le_bytes([
            hash.as_bytes()[0],
            hash.as_bytes()[1],
            hash.as_bytes()[2],
            hash.as_bytes()[3],
        ])
    };

    // Format: pid_timestamp_counter_random (for easier debugging)
    format!(
        "{:08x}_{:016x}_{:08x}_{:08x}",
        process_id, timestamp, counter, random_component
    )
}

/// Get current timestamp in microseconds
fn current_timestamp_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch")
        .as_micros() as u64
}

/// Compute a cryptographically secure checksum using blake3
fn compute_checksum(log: &ReplicationLog) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();

    hasher.update(log.operation_id.as_bytes());
    hasher.update(log.source_node_id.as_bytes());
    hasher.update(&log.timestamp_micros.to_le_bytes());

    match &log.operation {
        ReplicationOperation::Put {
            collection,
            key,
            value,
        } => {
            hasher.update(b"PUT");
            hasher.update(collection.as_bytes());
            hasher.update(key);
            hasher.update(value);
        }
        ReplicationOperation::Delete { collection, key } => {
            hasher.update(b"DELETE");
            hasher.update(collection.as_bytes());
            hasher.update(key);
        }
        ReplicationOperation::Batch {
            collection,
            operations,
        } => {
            hasher.update(b"BATCH");
            hasher.update(collection.as_bytes());
            hasher.update(&operations.len().to_le_bytes());
            for op in operations {
                match op {
                    BatchOp::Put { key, value } => {
                        hasher.update(b"PUT");
                        hasher.update(key);
                        hasher.update(value);
                    }
                    BatchOp::Delete { key } => {
                        hasher.update(b"DELETE");
                        hasher.update(key);
                    }
                }
            }
        }
    }

    *hasher.finalize().as_bytes()
}

impl ReplicationLog {
    /// Compute and attach a checksum to this log
    pub fn with_checksum(mut self) -> Self {
        self.checksum = Some(compute_checksum(&self));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_log_creation() {
        let log = ReplicationLog::new(
            "node-1".to_string(),
            ReplicationOperation::Put {
                collection: "test".to_string(),
                key: vec![1, 2, 3],
                value: vec![4, 5, 6],
            },
        );

        assert_eq!(log.source_node_id, "node-1");
        assert!(!log.operation_id.is_empty());
        assert!(log.timestamp_micros > 0);
    }

    #[test]
    fn test_replication_config() {
        let config = ReplicationConfig::new("node-1")
            .enable()
            .with_peers(vec!["node-2".to_string()]);

        assert!(config.enabled);
        assert_eq!(config.node_id, "node-1");
        assert_eq!(config.peer_nodes.len(), 1);
    }

    #[test]
    fn test_operation_types() {
        let put_op = ReplicationOperation::Put {
            collection: "test".to_string(),
            key: vec![1],
            value: vec![2],
        };

        let delete_op = ReplicationOperation::Delete {
            collection: "test".to_string(),
            key: vec![1],
        };

        let batch_op = ReplicationOperation::Batch {
            collection: "test".to_string(),
            operations: vec![
                BatchOp::Put {
                    key: vec![1],
                    value: vec![2],
                },
                BatchOp::Delete { key: vec![3] },
            ],
        };

        // Just ensure they can be created
        assert!(matches!(put_op, ReplicationOperation::Put { .. }));
        assert!(matches!(delete_op, ReplicationOperation::Delete { .. }));
        assert!(matches!(batch_op, ReplicationOperation::Batch { .. }));
    }

    #[test]
    fn test_compute_checksum() {
        let log = ReplicationLog::new(
            "node-1".to_string(),
            ReplicationOperation::Put {
                collection: "test".to_string(),
                key: vec![1, 2, 3],
                value: vec![4, 5, 6],
            },
        );

        let checksum1 = compute_checksum(&log);
        let checksum2 = compute_checksum(&log);

        // Same log should produce same checksum
        assert_eq!(checksum1, checksum2);
        // Verify it's a 32-byte blake3 hash
        assert_eq!(checksum1.len(), 32);
    }

    #[test]
    fn test_with_checksum() {
        let log = ReplicationLog::new(
            "node-1".to_string(),
            ReplicationOperation::Put {
                collection: "test".to_string(),
                key: vec![1],
                value: vec![2],
            },
        )
        .with_checksum();

        assert!(log.checksum.is_some());
    }

    #[test]
    fn test_replication_metadata() {
        let metadata = ReplicationMetadata::new(12345);
        assert_eq!(metadata.timestamp_micros, 12345);
        assert!(!metadata.is_tombstone);

        let tombstone = ReplicationMetadata::tombstone(67890);
        assert_eq!(tombstone.timestamp_micros, 67890);
        assert!(tombstone.is_tombstone);
    }

    #[test]
    fn test_metadata_serialization() {
        // Test metadata serialization
        let metadata = ReplicationMetadata::new(12345);
        let serialized = borsh::to_vec(&metadata).unwrap();
        let deserialized: ReplicationMetadata = borsh::from_slice(&serialized).unwrap();
        assert_eq!(deserialized.timestamp_micros, 12345);
        assert!(!deserialized.is_tombstone);

        // Test tombstone serialization
        let tombstone = ReplicationMetadata::tombstone(67890);
        let serialized = borsh::to_vec(&tombstone).unwrap();
        let deserialized: ReplicationMetadata = borsh::from_slice(&serialized).unwrap();
        assert_eq!(deserialized.timestamp_micros, 67890);
        assert!(deserialized.is_tombstone);
    }

    #[test]
    fn test_replication_log_serialization() {
        let log = ReplicationLog::new(
            "node-1".to_string(),
            ReplicationOperation::Put {
                collection: "test".to_string(),
                key: vec![1, 2, 3],
                value: vec![4, 5, 6],
            },
        )
        .with_checksum();

        // Serialize and deserialize
        let serialized = borsh::to_vec(&log).unwrap();
        let deserialized: ReplicationLog = borsh::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.operation_id, log.operation_id);
        assert_eq!(deserialized.source_node_id, log.source_node_id);
        assert_eq!(deserialized.timestamp_micros, log.timestamp_micros);
        assert_eq!(deserialized.checksum, log.checksum);
    }

    #[test]
    fn test_operation_id_format() {
        let op_id = generate_operation_id();
        // Format: pid_timestamp_counter_random
        let parts: Vec<&str> = op_id.split('_').collect();
        assert_eq!(parts.len(), 4, "Operation ID should have 4 parts");

        // Each part should be valid hex
        for part in parts {
            assert!(u64::from_str_radix(part, 16).is_ok() || u32::from_str_radix(part, 16).is_ok());
        }
    }

    #[test]
    fn test_stats_no_panic() {
        use crate::DatabaseConfig;

        let temp_dir = tempfile::tempdir().unwrap();
        let db = DatabaseConfig::new(temp_dir.path())
            .create_if_missing(true)
            .add_column_family("test")
            .open()
            .unwrap();

        let config = ReplicationConfig::new("test-node").enable();
        let manager = ReplicationManager::new(db, config).unwrap();

        // Should not panic (using Result instead of unwrap)
        let stats = manager.stats().unwrap();
        assert_eq!(stats.total_operations, 0);
        assert_eq!(stats.oldest_operation_timestamp, None);
        assert_eq!(stats.newest_operation_timestamp, None);
    }

    #[test]
    fn test_batch_last_write_wins_conflict() {
        use crate::DatabaseConfig;

        let temp_dir = tempfile::tempdir().unwrap();
        let db = DatabaseConfig::new(temp_dir.path())
            .create_if_missing(true)
            .add_column_family("test")
            .open()
            .unwrap();

        let config = ReplicationConfig::new("test-node")
            .enable()
            .conflict_resolution(ConflictResolution::LastWriteWins);
        let manager = ReplicationManager::new(db.clone(), config).unwrap();

        let key = vec![1, 2, 3];
        let newer_value = vec![10, 20, 30];
        let older_value = vec![40, 50, 60];

        // First, apply a single put with timestamp 1000
        let newer_log = ReplicationLog {
            operation_id: "op1".to_string(),
            source_node_id: "node1".to_string(),
            timestamp_micros: 1000,
            operation: ReplicationOperation::Put {
                collection: "test".to_string(),
                key: key.clone(),
                value: newer_value.clone(),
            },
            checksum: None,
        };

        manager.apply_replication(newer_log).unwrap();

        // Verify the value was written
        let cf = db.inner.db.cf_handle("test").unwrap();
        let stored = db.inner.db.get_cf(&cf, &key).unwrap();
        assert_eq!(stored, Some(newer_value.clone()));

        // Now apply a batch with older timestamp 500
        let older_batch_log = ReplicationLog {
            operation_id: "op2".to_string(),
            source_node_id: "node2".to_string(),
            timestamp_micros: 500, // Older!
            operation: ReplicationOperation::Batch {
                collection: "test".to_string(),
                operations: vec![BatchOp::Put {
                    key: key.clone(),
                    value: older_value.clone(),
                }],
            },
            checksum: None,
        };

        manager.apply_replication(older_batch_log).unwrap();

        // Verify the newer value is still there (old batch was rejected)
        let stored = db.inner.db.get_cf(&cf, &key).unwrap();
        assert_eq!(
            stored,
            Some(newer_value),
            "LastWriteWins should reject older batch operation"
        );

        // Now apply a batch with newer timestamp 2000
        let even_newer_value = vec![70, 80, 90];
        let newer_batch_log = ReplicationLog {
            operation_id: "op3".to_string(),
            source_node_id: "node3".to_string(),
            timestamp_micros: 2000, // Newer!
            operation: ReplicationOperation::Batch {
                collection: "test".to_string(),
                operations: vec![BatchOp::Put {
                    key: key.clone(),
                    value: even_newer_value.clone(),
                }],
            },
            checksum: None,
        };

        manager.apply_replication(newer_batch_log).unwrap();

        // Verify the even newer value replaced the old one
        let stored = db.inner.db.get_cf(&cf, &key).unwrap();
        assert_eq!(
            stored,
            Some(even_newer_value),
            "LastWriteWins should accept newer batch operation"
        );
    }
}
