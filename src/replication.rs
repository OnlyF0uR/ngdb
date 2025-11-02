//! Replication module for NGDB
//!
//! This module provides infrastructure for distributed replication.
//!
//! # Architecture Overview
//!
//! ## Outbound Replication (Local Writes)
//! When you write to your local database, NGDB can optionally capture replication logs
//! that you can send to peer nodes:
//!
//! ```rust,ignore
//! let users = db.collection::<User>("users")?;
//!
//! // Enable replication logging
//! let log_entry = users.put_with_replication(&user).await?;
//!
//! // Send log_entry to your peers via HTTP/gRPC/etc
//! send_to_peers(&log_entry).await?;
//! ```
//!
//! ## Inbound Replication (Receiving from Peers)
//! When your server receives replicated data from a peer, process it:
//!
//! ```rust,ignore
//! // In your HTTP handler
//! async fn handle_replication(data: ReplicationLog) -> Result<()> {
//!     // NGDB processes the incoming data
//!     db.apply_replication(data).await?;
//!     Ok(())
//! }
//! ```

use crate::{Database, Error, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info, instrument, warn};

/// A replication log entry representing a database operation
///
/// This is what you send to peer nodes when replicating data.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ReplicationLog {
    /// Unique identifier for this operation
    pub operation_id: String,

    /// Node that originated this operation
    pub source_node_id: String,

    /// Timestamp in microseconds (for conflict resolution)
    pub timestamp_micros: u64,

    /// The actual operation to replicate
    pub operation: ReplicationOperation,

    /// Optional checksum for data integrity
    pub checksum: Option<u64>,
}

/// Types of operations that can be replicated
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum ReplicationOperation {
    /// Put operation
    Put {
        /// Collection (column family) name
        collection: String,
        /// Serialized key
        key: Vec<u8>,
        /// Serialized value
        value: Vec<u8>,
    },

    /// Delete operation
    Delete {
        /// Collection (column family) name
        collection: String,
        /// Serialized key
        key: Vec<u8>,
    },

    /// Batch of operations (atomic)
    Batch {
        /// Collection (column family) name
        collection: String,
        /// List of operations in the batch
        operations: Vec<BatchOp>,
    },
}

/// Individual operation within a batch
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
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
            checksum: None, // TODO: Implement checksumming
        }
    }

    /// Validate the replication log entry
    #[instrument(skip(self))]
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.is_empty() {
            error!("Validation failed: source node ID is empty");
            return Err(Error::Replication(
                "Source node ID cannot be empty".to_string(),
            ));
        }

        if self.operation_id.is_empty() {
            error!("Validation failed: operation ID is empty");
            return Err(Error::Replication(
                "Operation ID cannot be empty".to_string(),
            ));
        }

        debug!("Replication log validated: op={}", self.operation_id);
        Ok(())
    }

    /// Get the collection this operation applies to
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
    /// Enable replication
    pub enabled: bool,

    /// This node's unique identifier
    pub node_id: String,

    /// List of peer nodes (e.g., ["http://node1:8080", "http://node2:8080"])
    pub peer_nodes: Vec<String>,

    /// Conflict resolution strategy
    pub conflict_resolution: ConflictResolution,

    /// Whether to verify checksums
    pub verify_checksums: bool,
}

/// Strategy for resolving conflicts when same key is written by multiple nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Last write wins (based on timestamp)
    LastWriteWins,

    /// First write wins (reject later writes to same key)
    FirstWriteWins,

    /// Custom (user must implement conflict resolution)
    Custom,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: "node-0".to_string(),
            peer_nodes: Vec::new(),
            conflict_resolution: ConflictResolution::LastWriteWins,
            verify_checksums: true,
        }
    }
}

impl ReplicationConfig {
    /// Create a new replication configuration
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            ..Default::default()
        }
    }

    /// Enable or disable replication
    pub fn enable(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the peer nodes
    pub fn with_peers(mut self, peers: Vec<String>) -> Self {
        self.peer_nodes = peers;
        self
    }

    /// Set the conflict resolution strategy
    pub fn conflict_resolution(mut self, strategy: ConflictResolution) -> Self {
        self.conflict_resolution = strategy;
        self
    }

    /// Validate the configuration
    #[instrument(skip(self))]
    pub fn validate(&self) -> Result<()> {
        if self.enabled {
            if self.node_id.is_empty() {
                error!("Replication config validation failed: node ID is empty");
                return Err(Error::InvalidConfig("Node ID cannot be empty".to_string()));
            }
        }
        debug!("Replication config validated for node {}", self.node_id);
        Ok(())
    }
}

/// Hook trait for custom replication behavior
///
/// Implement this to add custom logic around replication events.
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
    /// Return true to accept the incoming write, false to reject it.
    fn resolve_conflict(&self, _incoming: &ReplicationLog, _existing_timestamp: u64) -> bool {
        true // Default: accept
    }
}

/// Manager for replication operations
pub struct ReplicationManager {
    config: ReplicationConfig,
    hooks: Vec<Arc<dyn ReplicationHook>>,
    db: Database,
    // Track applied operations for idempotency (operation_id -> timestamp)
    applied_operations: Arc<Mutex<HashMap<String, u64>>>,
}

impl ReplicationManager {
    /// Create a new replication manager
    ///
    /// # Arguments
    ///
    /// * `config` - Replication configuration
    /// * `db` - Database instance for applying replicated operations
    #[instrument(skip(config, db))]
    pub fn new(config: ReplicationConfig, db: Database) -> Result<Self> {
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
    pub fn register_hook<H: ReplicationHook + 'static>(&mut self, hook: H) {
        info!("Registering replication hook");
        self.hooks.push(Arc::new(hook));
    }

    /// Process incoming replication data
    ///
    /// This is the main entry point for handling replicated data from peer nodes.
    ///
    /// # Arguments
    ///
    /// * `log` - The replication log entry received from a peer
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the replication was applied successfully,
    /// or an error if validation or application failed.
    ///
    #[instrument(skip(self, log), fields(op_id = %log.operation_id, source = %log.source_node_id))]
    pub async fn apply_replication(&self, log: ReplicationLog) -> Result<()> {
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
        let result = self.apply_operation(&log).await;

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
                // Remove oldest entries (simple approach - in production, use a better strategy)
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
                eprintln!("Hook after_apply failed: {}", e);
            }
        }

        Ok(())
    }

    /// Apply a single operation to the database
    #[instrument(skip(self, log))]
    async fn apply_operation(&self, log: &ReplicationLog) -> Result<()> {
        match &log.operation {
            ReplicationOperation::Put {
                collection,
                key,
                value,
            } => {
                debug!("Applying PUT operation to collection '{}'", collection);
                self.apply_put(collection, key, value, log.timestamp_micros)
                    .await
            }
            ReplicationOperation::Delete { collection, key } => {
                debug!("Applying DELETE operation to collection '{}'", collection);
                self.apply_delete(collection, key).await
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
                self.apply_batch(collection, operations).await
            }
        }
    }

    /// Apply a put operation with conflict detection
    #[instrument(skip(self, key, value))]
    async fn apply_put(
        &self,
        collection: &str,
        key: &[u8],
        value: &[u8],
        timestamp: u64,
    ) -> Result<()> {
        // Get the column family handle
        let cf = self.db.inner.db.cf_handle(collection).ok_or_else(|| {
            error!(
                "Column family '{}' not found during replication",
                collection
            );
            Error::Database(format!("Column family '{}' not found", collection))
        })?;

        // Check for conflicts based on strategy
        match self.config.conflict_resolution {
            ConflictResolution::LastWriteWins => {
                // Always apply - last write wins
                debug!("Applying put with LastWriteWins strategy");
                self.db.inner.db.put_cf(cf, key, value).map_err(|e| {
                    error!("Failed to apply put: {}", e);
                    Error::Database(format!("Failed to apply put: {}", e))
                })?;
            }
            ConflictResolution::FirstWriteWins => {
                // Only apply if key doesn't exist
                debug!("Applying put with FirstWriteWins strategy");
                if self
                    .db
                    .inner
                    .db
                    .get_cf(cf, key)
                    .map_err(|e| {
                        error!("Failed to check key existence: {}", e);
                        Error::Database(format!("Failed to check existence: {}", e))
                    })?
                    .is_none()
                {
                    debug!("Key doesn't exist, applying put");
                    self.db.inner.db.put_cf(cf, key, value).map_err(|e| {
                        error!("Failed to apply put: {}", e);
                        Error::Database(format!("Failed to apply put: {}", e))
                    })?;
                } else {
                    debug!("Key exists, skipping put (FirstWriteWins)");
                }
            }
            ConflictResolution::Custom => {
                // Check with hooks for custom resolution
                debug!("Applying put with Custom conflict resolution strategy");
                let existing = self.db.inner.db.get_cf(cf, key).map_err(|e| {
                    error!("Failed to get existing value: {}", e);
                    Error::Database(format!("Failed to get existing value: {}", e))
                })?;

                if existing.is_some() {
                    // There's an existing value - let hooks decide
                    info!("Conflict detected, consulting hooks for resolution");
                    let mut should_apply = true;
                    for hook in &self.hooks {
                        if !hook.resolve_conflict(
                            &ReplicationLog {
                                operation_id: format!("conflict_check_{}", timestamp),
                                source_node_id: self.config.node_id.clone(),
                                timestamp_micros: timestamp,
                                operation: ReplicationOperation::Put {
                                    collection: collection.to_string(),
                                    key: key.to_vec(),
                                    value: value.to_vec(),
                                },
                                checksum: None,
                            },
                            timestamp,
                        ) {
                            info!("Hook rejected the conflicting write");
                            should_apply = false;
                            break;
                        }
                    }

                    if should_apply {
                        debug!("Hooks approved conflicting write, applying");
                        self.db.inner.db.put_cf(cf, key, value).map_err(|e| {
                            error!("Failed to apply put: {}", e);
                            Error::Database(format!("Failed to apply put: {}", e))
                        })?;
                    } else {
                        info!("Conflicting write rejected by custom resolution");
                    }
                } else {
                    // No conflict, apply directly
                    debug!("No conflict detected, applying put directly");
                    self.db.inner.db.put_cf(cf, key, value).map_err(|e| {
                        error!("Failed to apply put: {}", e);
                        Error::Database(format!("Failed to apply put: {}", e))
                    })?;
                }
            }
        }

        Ok(())
    }

    /// Apply a delete operation
    #[instrument(skip(self, key))]
    async fn apply_delete(&self, collection: &str, key: &[u8]) -> Result<()> {
        let cf = self.db.inner.db.cf_handle(collection).ok_or_else(|| {
            error!("Column family '{}' not found during delete", collection);
            Error::Database(format!("Column family '{}' not found", collection))
        })?;

        self.db.inner.db.delete_cf(cf, key).map_err(|e| {
            error!("Failed to apply delete: {}", e);
            Error::Database(format!("Failed to apply delete: {}", e))
        })?;

        debug!("Delete operation applied successfully");
        Ok(())
    }

    /// Apply a batch of operations atomically
    #[instrument(skip(self, operations))]
    async fn apply_batch(&self, collection: &str, operations: &[BatchOp]) -> Result<()> {
        let cf = self.db.inner.db.cf_handle(collection).ok_or_else(|| {
            error!(
                "Column family '{}' not found during batch apply",
                collection
            );
            Error::Database(format!("Column family '{}' not found", collection))
        })?;

        let mut batch = rocksdb::WriteBatch::default();

        for op in operations {
            match op {
                BatchOp::Put { key, value } => {
                    batch.put_cf(cf, key, value);
                }
                BatchOp::Delete { key } => {
                    batch.delete_cf(cf, key);
                }
            }
        }

        self.db.inner.db.write(batch).map_err(|e| {
            error!("Failed to apply batch: {}", e);
            Error::Database(format!("Failed to apply batch: {}", e))
        })?;

        info!(
            "Batch of {} operations applied successfully",
            operations.len()
        );
        Ok(())
    }

    /// Create a replication log for a put operation
    ///
    /// Call this when you want to replicate a local write to peers.
    #[instrument(skip(self, value))]
    pub fn create_put_log(
        &self,
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> ReplicationLog {
        debug!(
            "Creating PUT replication log for collection '{}'",
            collection
        );
        ReplicationLog::new(
            self.config.node_id.clone(),
            ReplicationOperation::Put {
                collection,
                key,
                value,
            },
        )
    }

    /// Create a replication log for a delete operation
    #[instrument(skip(self))]
    pub fn create_delete_log(&self, collection: String, key: Vec<u8>) -> ReplicationLog {
        debug!(
            "Creating DELETE replication log for collection '{}'",
            collection
        );
        ReplicationLog::new(
            self.config.node_id.clone(),
            ReplicationOperation::Delete { collection, key },
        )
    }

    /// Get the current configuration
    pub fn config(&self) -> &ReplicationConfig {
        &self.config
    }

    /// Initialize replication system
    ///
    /// This would set up connections to peers, start heartbeat, etc.
    ///
    /// This sets up the replication system and prepares for sending/receiving operations.
    /// In a production system, this would establish network connections to peer nodes.
    ///
    /// # Note
    ///
    /// Network layer implementation is left to the application. This library provides
    /// the data structures and application logic, while you provide the transport
    /// (HTTP, gRPC, custom protocol, etc.).
    #[instrument(skip(self))]
    pub async fn initialize(&self) -> Result<()> {
        if !self.config.enabled {
            info!("Replication not enabled, skipping initialization");
            return Ok(());
        }

        info!(
            "Initializing replication for node {} with {} peers",
            self.config.node_id,
            self.config.peer_nodes.len()
        );

        // Validate peer configuration
        if self.config.peer_nodes.is_empty() {
            error!("No peer nodes configured for replication");
            return Err(Error::Replication(
                "No peer nodes configured for replication".to_string(),
            ));
        }

        // In a production implementation, you would:
        // 1. Establish connections to each peer node
        // 2. Perform version/capability handshake
        // 3. Start heartbeat tasks
        // 4. Begin syncing any missed operations
        //
        // This is intentionally left as a framework - the actual network
        // implementation depends on your transport choice (HTTP, gRPC, etc.)

        info!("Replication initialization complete");
        Ok(())
    }

    /// Get statistics about applied operations
    pub fn stats(&self) -> Result<ReplicationStats> {
        let applied = self
            .applied_operations
            .lock()
            .map_err(|e| Error::Replication(format!("Failed to acquire lock: {}", e)))?;

        Ok(ReplicationStats {
            total_operations: applied.len(),
            oldest_operation_timestamp: applied.values().min().copied(),
            newest_operation_timestamp: applied.values().max().copied(),
        })
    }

    /// Clear operation history (use with caution)
    ///
    /// This removes all tracked operation IDs, which will cause previously-applied
    /// operations to be reapplied if received again. Only use this if you're certain
    /// all peers are synchronized.
    pub fn clear_operation_history(&self) -> Result<()> {
        let mut applied = self
            .applied_operations
            .lock()
            .map_err(|e| Error::Replication(format!("Failed to acquire lock: {}", e)))?;
        applied.clear();
        Ok(())
    }
}

/// Statistics about replication operations
#[derive(Debug, Clone)]
pub struct ReplicationStats {
    /// Total number of tracked operations
    pub total_operations: usize,
    /// Timestamp of oldest tracked operation
    pub oldest_operation_timestamp: Option<u64>,
    /// Timestamp of newest tracked operation
    pub newest_operation_timestamp: Option<u64>,
}

/// Helper function to generate a unique operation ID
fn generate_operation_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| {
            // Fallback: use a fixed value if system time is before UNIX epoch
            // This should never happen in practice on modern systems
            std::time::Duration::from_nanos(1)
        })
        .as_nanos();

    let counter = COUNTER.fetch_add(1, Ordering::SeqCst);

    // ID format: timestamp + counter for uniqueness even in same nanosecond
    format!("op_{:x}_{:x}", nanos, counter)
}

/// Helper function to get current timestamp in microseconds
fn current_timestamp_micros() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| {
            // Fallback: return 0 if system time is before UNIX epoch
            // This should never happen in practice on modern systems
            std::time::Duration::from_secs(0)
        })
        .as_micros() as u64
}

/// Compute a simple checksum for a replication log
///
/// This is a basic checksum implementation. In production, you might want
/// to use a more robust hashing algorithm like SHA-256.
fn compute_checksum(log: &ReplicationLog) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();

    // Hash the critical fields
    log.operation_id.hash(&mut hasher);
    log.source_node_id.hash(&mut hasher);
    log.timestamp_micros.hash(&mut hasher);

    match &log.operation {
        ReplicationOperation::Put {
            collection,
            key,
            value,
        } => {
            "put".hash(&mut hasher);
            collection.hash(&mut hasher);
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        ReplicationOperation::Delete { collection, key } => {
            "delete".hash(&mut hasher);
            collection.hash(&mut hasher);
            key.hash(&mut hasher);
        }
        ReplicationOperation::Batch {
            collection,
            operations,
        } => {
            "batch".hash(&mut hasher);
            collection.hash(&mut hasher);
            for op in operations {
                match op {
                    BatchOp::Put { key, value } => {
                        "put".hash(&mut hasher);
                        key.hash(&mut hasher);
                        value.hash(&mut hasher);
                    }
                    BatchOp::Delete { key } => {
                        "delete".hash(&mut hasher);
                        key.hash(&mut hasher);
                    }
                }
            }
        }
    }

    hasher.finish()
}

impl ReplicationLog {
    /// Add a checksum to this log entry
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
                collection: "users".to_string(),
                key: vec![1, 2, 3],
                value: vec![4, 5, 6],
            },
        );

        assert_eq!(log.source_node_id, "node-1");
        assert_eq!(log.collection(), "users");
        assert!(log.timestamp_micros > 0);
        assert!(log.validate().is_ok());
    }

    #[test]
    fn test_replication_config() {
        let config = ReplicationConfig::new("node-1".to_string())
            .enable(true)
            .with_peers(vec!["http://node2:8080".to_string()])
            .conflict_resolution(ConflictResolution::LastWriteWins);

        assert_eq!(config.node_id, "node-1");
        assert!(config.enabled);
        assert_eq!(config.peer_nodes.len(), 1);
        assert_eq!(
            config.conflict_resolution,
            ConflictResolution::LastWriteWins
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_operation_types() {
        let put_op = ReplicationOperation::Put {
            collection: "users".to_string(),
            key: vec![1],
            value: vec![2],
        };

        let delete_op = ReplicationOperation::Delete {
            collection: "users".to_string(),
            key: vec![1],
        };

        let batch_op = ReplicationOperation::Batch {
            collection: "users".to_string(),
            operations: vec![
                BatchOp::Put {
                    key: vec![1],
                    value: vec![2],
                },
                BatchOp::Delete { key: vec![3] },
            ],
        };

        // Just verify they can be created
        assert!(matches!(put_op, ReplicationOperation::Put { .. }));
        assert!(matches!(delete_op, ReplicationOperation::Delete { .. }));
        assert!(matches!(batch_op, ReplicationOperation::Batch { .. }));
    }

    #[test]
    fn test_compute_checksum() {
        let log = ReplicationLog::new(
            "node-1".to_string(),
            ReplicationOperation::Put {
                collection: "users".to_string(),
                key: vec![1, 2, 3],
                value: vec![4, 5, 6],
            },
        );

        let checksum1 = compute_checksum(&log);
        let checksum2 = compute_checksum(&log);

        // Same log should produce same checksum
        assert_eq!(checksum1, checksum2);

        // Different log should produce different checksum
        let log2 = ReplicationLog::new(
            "node-1".to_string(),
            ReplicationOperation::Put {
                collection: "users".to_string(),
                key: vec![1, 2, 3],
                value: vec![7, 8, 9], // Different value
            },
        );

        let checksum3 = compute_checksum(&log2);
        assert_ne!(checksum1, checksum3);
    }

    #[test]
    fn test_with_checksum() {
        let log = ReplicationLog::new(
            "node-1".to_string(),
            ReplicationOperation::Put {
                collection: "users".to_string(),
                key: vec![1, 2, 3],
                value: vec![4, 5, 6],
            },
        )
        .with_checksum();

        assert!(log.checksum.is_some());
        assert_eq!(log.checksum.unwrap(), compute_checksum(&log));
    }
}
