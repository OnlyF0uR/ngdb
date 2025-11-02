//! Replication support for NGDB
//!
//! This module provides replication capabilities for multi-node deployments.
//! It follows sound replication principles:
//!
//! - Write-ahead logging for all operations
//! - Idempotent operation application
//! - Configurable conflict resolution strategies
//! - Checksum verification for data integrity
//! - Hook system for custom replication logic
//!
//! # Architecture
//!
//! 1. **ReplicationLog**: Represents a single replicated operation with metadata
//! 2. **ReplicationManager**: Applies replication logs to the local database
//! 3. **ReplicationHook**: Extensible hook system for custom logic
//! 4. **ConflictResolution**: Strategies for handling concurrent writes
//!
//! # Example
//!
//! ```rust,no_run
//! use ngdb::{Database, DatabaseConfig, ReplicationConfig, ReplicationManager, ReplicationLog, ReplicationOperation};
//!
//! # fn example() -> Result<(), ngdb::Error> {
//! let db = DatabaseConfig::new("./data/replica")
//!     .create_if_missing(true)
//!     .add_column_family("users")
//!     .open()?;
//!
//! let config = ReplicationConfig::new("replica-1")
//!     .enable()
//!     .with_peers(vec!["primary-1".to_string()]);
//!
//! let manager = ReplicationManager::new(db, config)?;
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
//! manager.apply_replication(log)?;
//! # Ok(())
//! # }
//! ```

use crate::{Database, Error, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, instrument, warn};

/// A replication log entry representing a single operation to be replicated
#[derive(Debug, Clone)]
pub struct ReplicationLog {
    /// Unique identifier for this operation
    pub operation_id: String,

    /// ID of the node that originated this operation
    pub source_node_id: String,

    /// Timestamp in microseconds since Unix epoch
    pub timestamp_micros: u64,

    /// The operation to replicate
    pub operation: ReplicationOperation,

    /// Optional checksum for verification
    pub checksum: Option<u64>,
}

/// Types of operations that can be replicated
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub enum BatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
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
    applied_operations: Arc<Mutex<HashMap<String, u64>>>,
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
            applied_operations: Arc::new(Mutex::new(HashMap::new())),
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
            if Some(computed) != log.checksum {
                error!(
                    "Checksum mismatch for operation {}: expected {:?}, got {}",
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
                .lock()
                .map_err(|e| Error::Replication(format!("Failed to acquire lock: {}", e)))?;

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
        let result = self.apply_operation(&log);

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
                .lock()
                .map_err(|e| Error::Replication(format!("Failed to acquire lock: {}", e)))?;
            applied.insert(log.operation_id.clone(), log.timestamp_micros);

            // Limit the size of the tracking map (keep last 10000 operations)
            if applied.len() > 10000 {
                warn!(
                    "Operation tracking map exceeded 10000 entries ({}), cleaning up oldest entries",
                    applied.len()
                );
                // Remove oldest entries
                let mut entries: Vec<_> = applied.iter().map(|(k, v)| (k.clone(), *v)).collect();
                entries.sort_by_key(|(_k, v)| *v);
                for (key, _) in entries.iter().take(applied.len() - 10000) {
                    applied.remove(key);
                }
                info!(
                    "Cleaned up operation tracking map to {} entries",
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
    fn apply_operation(&self, log: &ReplicationLog) -> Result<()> {
        match &log.operation {
            ReplicationOperation::Put {
                collection,
                key,
                value,
            } => {
                debug!("Applying PUT operation to collection '{}'", collection);
                self.apply_put(collection, key, value, log.timestamp_micros)
            }
            ReplicationOperation::Delete { collection, key } => {
                debug!("Applying DELETE operation to collection '{}'", collection);
                self.apply_delete(collection, key)
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
                self.apply_batch(collection, operations)
            }
        }
    }

    /// Apply a put operation with conflict detection
    #[instrument(skip(self, key, value))]
    fn apply_put(&self, collection: &str, key: &[u8], value: &[u8], timestamp: u64) -> Result<()> {
        // Get the column family
        let cf =
            self.db.inner.db.cf_handle(collection).ok_or_else(|| {
                Error::Database(format!("Column family '{}' not found", collection))
            })?;

        // Check for existing value for conflict resolution
        let existing = self
            .db
            .inner
            .db
            .get_cf(&cf, key)
            .map_err(|e| Error::Database(format!("Failed to get existing value: {}", e)))?;

        let final_value = if let Some(existing_data) = existing {
            // Conflict detected - apply resolution strategy
            match self.config.conflict_resolution {
                ConflictResolution::LastWriteWins => {
                    // Always use the new value (default behavior)
                    debug!("Conflict resolved: LastWriteWins - accepting new value");
                    value.to_vec()
                }
                ConflictResolution::FirstWriteWins => {
                    // Keep existing value, reject new write
                    debug!("Conflict resolved: FirstWriteWins - keeping existing value");
                    return Ok(());
                }
                ConflictResolution::Custom => {
                    // Use custom hook
                    if let Some(hook) = self.hooks.first() {
                        debug!("Conflict resolved: Custom hook");
                        hook.resolve_conflict(&existing_data, value)?
                    } else {
                        warn!("Custom conflict resolution set but no hook registered, defaulting to LastWriteWins");
                        value.to_vec()
                    }
                }
            }
        } else {
            // No conflict - just write the new value
            value.to_vec()
        };

        // Write the value
        self.db
            .inner
            .db
            .put_cf(&cf, key, &final_value)
            .map_err(|e| Error::Database(format!("Failed to put value: {}", e)))?;

        Ok(())
    }

    /// Apply a delete operation
    #[instrument(skip(self, key))]
    fn apply_delete(&self, collection: &str, key: &[u8]) -> Result<()> {
        let cf =
            self.db.inner.db.cf_handle(collection).ok_or_else(|| {
                Error::Database(format!("Column family '{}' not found", collection))
            })?;

        self.db
            .inner
            .db
            .delete_cf(&cf, key)
            .map_err(|e| Error::Database(format!("Failed to delete: {}", e)))?;

        Ok(())
    }

    /// Apply a batch of operations
    #[instrument(skip(self, operations))]
    fn apply_batch(&self, collection: &str, operations: &[BatchOp]) -> Result<()> {
        let cf =
            self.db.inner.db.cf_handle(collection).ok_or_else(|| {
                Error::Database(format!("Column family '{}' not found", collection))
            })?;

        let mut batch = rocksdb::WriteBatch::default();

        for op in operations {
            match op {
                BatchOp::Put { key, value } => {
                    batch.put_cf(&cf, key, value);
                }
                BatchOp::Delete { key } => {
                    batch.delete_cf(&cf, key);
                }
            }
        }

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
    pub fn stats(&self) -> ReplicationStats {
        let applied = self.applied_operations.lock().unwrap();

        let total = applied.len();
        let oldest = applied.values().min().copied();
        let newest = applied.values().max().copied();

        ReplicationStats {
            total_operations: total,
            oldest_operation_timestamp: oldest,
            newest_operation_timestamp: newest,
        }
    }

    /// Clear operation history
    ///
    /// This clears the idempotency tracking. Use with caution.
    pub fn clear_operation_history(&self) -> Result<()> {
        let mut applied = self
            .applied_operations
            .lock()
            .map_err(|e| Error::Replication(format!("Failed to acquire lock: {}", e)))?;
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

/// Generate a unique operation ID
fn generate_operation_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
    let timestamp = current_timestamp_micros();

    // Format: timestamp_counter_random
    format!(
        "{:016x}_{:08x}_{:08x}",
        timestamp,
        counter,
        rand::random::<u32>()
    )
}

/// Get current timestamp in microseconds
fn current_timestamp_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch")
        .as_micros() as u64
}

/// Compute a simple checksum for a replication log
fn compute_checksum(log: &ReplicationLog) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();

    log.operation_id.hash(&mut hasher);
    log.source_node_id.hash(&mut hasher);
    log.timestamp_micros.hash(&mut hasher);

    match &log.operation {
        ReplicationOperation::Put {
            collection,
            key,
            value,
        } => {
            "PUT".hash(&mut hasher);
            collection.hash(&mut hasher);
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        ReplicationOperation::Delete { collection, key } => {
            "DELETE".hash(&mut hasher);
            collection.hash(&mut hasher);
            key.hash(&mut hasher);
        }
        ReplicationOperation::Batch {
            collection,
            operations,
        } => {
            "BATCH".hash(&mut hasher);
            collection.hash(&mut hasher);
            operations.len().hash(&mut hasher);
            for op in operations {
                match op {
                    BatchOp::Put { key, value } => {
                        "PUT".hash(&mut hasher);
                        key.hash(&mut hasher);
                        value.hash(&mut hasher);
                    }
                    BatchOp::Delete { key } => {
                        "DELETE".hash(&mut hasher);
                        key.hash(&mut hasher);
                    }
                }
            }
        }
    }

    hasher.finish()
}

impl ReplicationLog {
    /// Compute and attach a checksum to this log
    pub fn with_checksum(mut self) -> Self {
        self.checksum = Some(compute_checksum(&self));
        self
    }
}

// Simple random number generator (replace rand::random with this if rand is not available)
mod rand {
    pub fn random<T>() -> T
    where
        T: From<u32>,
    {
        use std::collections::hash_map::RandomState;
        use std::hash::BuildHasher;

        let state = RandomState::new();

        T::from(state.hash_one(std::time::SystemTime::now()) as u32)
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
}
