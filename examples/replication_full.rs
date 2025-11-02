//! Comprehensive replication example demonstrating advanced patterns
//!
//! This example covers:
//! - Multi-node replication setup
//! - Conflict resolution strategies (LastWriteWins, FirstWriteWins, Custom)
//! - Custom replication hooks
//! - Batch operations across nodes
//! - Checksum verification
//! - Proper production replication patterns

use borsh::{BorshDeserialize, BorshSerialize};
use ngdb::{
    ConflictResolution, DatabaseConfig, ReplicationConfig, ReplicationHook, ReplicationLog,
    ReplicationManager, ReplicationOperation, Result, Storable,
};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
struct Record {
    id: u64,
    value: String,
    version: u64,
    timestamp: u64,
}

impl Storable for Record {
    type Key = u64;

    fn key(&self) -> Self::Key {
        self.id
    }

    fn validate(&self) -> Result<()> {
        if self.value.is_empty() {
            return Err(ngdb::Error::InvalidData(
                "Value cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

struct AuditHook {
    node_id: String,
}

impl ReplicationHook for AuditHook {
    fn before_apply(&self, log: &ReplicationLog) -> Result<()> {
        println!(
            "[{}] Receiving operation {} from {}",
            self.node_id, log.operation_id, log.source_node_id
        );
        Ok(())
    }

    fn after_apply(&self, log: &ReplicationLog) -> Result<()> {
        println!("[{}] Applied operation {}", self.node_id, log.operation_id);
        Ok(())
    }

    fn on_replication_error(&self, log: &ReplicationLog, error: &ngdb::Error) {
        eprintln!(
            "[{}] Error applying operation {}: {:?}",
            self.node_id, log.operation_id, error
        );
    }

    fn resolve_conflict(&self, existing: &[u8], new: &[u8]) -> Result<Vec<u8>> {
        let existing_record: Record = borsh::from_slice(existing)
            .map_err(|e| ngdb::Error::SerializationError(e.to_string()))?;
        let new_record: Record =
            borsh::from_slice(new).map_err(|e| ngdb::Error::SerializationError(e.to_string()))?;

        let winner = if new_record.version > existing_record.version {
            println!(
                "[{}] Conflict: New v{} > Existing v{}, accepting new",
                self.node_id, new_record.version, existing_record.version
            );
            new_record
        } else if new_record.version == existing_record.version {
            if new_record.timestamp > existing_record.timestamp {
                println!(
                    "[{}] Conflict: Same version, newer timestamp, accepting new",
                    self.node_id
                );
                new_record
            } else {
                println!(
                    "[{}] Conflict: Same version, older timestamp, keeping existing",
                    self.node_id
                );
                existing_record
            }
        } else {
            println!(
                "[{}] Conflict: New v{} < Existing v{}, keeping existing",
                self.node_id, new_record.version, existing_record.version
            );
            existing_record
        };

        borsh::to_vec(&winner).map_err(|e| ngdb::Error::SerializationError(e.to_string()))
    }
}

fn main() -> Result<()> {
    let temp_dir = std::env::temp_dir();
    let node1_path = temp_dir.join("ngdb_full_repl_node1");
    let node2_path = temp_dir.join("ngdb_full_repl_node2");
    let node3_path = temp_dir.join("ngdb_full_repl_node3");

    let _ = std::fs::remove_dir_all(&node1_path);
    let _ = std::fs::remove_dir_all(&node2_path);
    let _ = std::fs::remove_dir_all(&node3_path);

    // Setup three-node replication cluster
    {
        let node1_db = DatabaseConfig::new(node1_path.to_str().unwrap())
            .create_if_missing(true)
            .add_column_family("records")
            .open()?;

        let node2_db = DatabaseConfig::new(node2_path.to_str().unwrap())
            .create_if_missing(true)
            .add_column_family("records")
            .open()?;

        let node3_db = DatabaseConfig::new(node3_path.to_str().unwrap())
            .create_if_missing(true)
            .add_column_family("records")
            .open()?;

        let config1 = ReplicationConfig::new("node-1")
            .enable()
            .with_peers(vec!["node-2".to_string(), "node-3".to_string()])
            .conflict_resolution(ConflictResolution::LastWriteWins);

        let config2 = ReplicationConfig::new("node-2")
            .enable()
            .with_peers(vec!["node-1".to_string(), "node-3".to_string()])
            .conflict_resolution(ConflictResolution::LastWriteWins);

        let config3 = ReplicationConfig::new("node-3")
            .enable()
            .with_peers(vec!["node-1".to_string(), "node-2".to_string()])
            .conflict_resolution(ConflictResolution::Custom);

        let mut manager1 = ReplicationManager::new(node1_db.clone(), config1)?;
        let mut manager2 = ReplicationManager::new(node2_db.clone(), config2)?;
        let mut manager3 = ReplicationManager::new(node3_db.clone(), config3)?;

        manager1.register_hook(Arc::new(AuditHook {
            node_id: "node-1".to_string(),
        }));
        manager2.register_hook(Arc::new(AuditHook {
            node_id: "node-2".to_string(),
        }));
        manager3.register_hook(Arc::new(AuditHook {
            node_id: "node-3".to_string(),
        }));

        println!("Initialized 3-node cluster\n");

        // Primary write with full replication
        {
            let record = Record {
                id: 1,
                value: "Initial record from Node 1".to_string(),
                version: 1,
                timestamp: current_timestamp_micros(),
            };

            let node1_records = node1_db.collection::<Record>("records")?;
            node1_records.put(&record)?;
            println!("Node 1: Written record {}", record.id);

            let log = ReplicationLog::new(
                "node-1".to_string(),
                ReplicationOperation::Put {
                    collection: "records".to_string(),
                    key: borsh::to_vec(&record.id)?,
                    value: borsh::to_vec(&record)?,
                },
            )
            .with_checksum();

            manager2.apply_replication(log.clone())?;
            manager3.apply_replication(log)?;
            println!("Replicated to all nodes\n");
        }

        // Conflict resolution - LastWriteWins
        {
            let record_v2 = Record {
                id: 1,
                value: "Updated by Node 2".to_string(),
                version: 2,
                timestamp: current_timestamp_micros(),
            };

            let node2_records = node2_db.collection::<Record>("records")?;
            node2_records.put(&record_v2)?;
            println!("Node 2: Updated to v{}", record_v2.version);

            let log2 = ReplicationLog::new(
                "node-2".to_string(),
                ReplicationOperation::Put {
                    collection: "records".to_string(),
                    key: borsh::to_vec(&record_v2.id)?,
                    value: borsh::to_vec(&record_v2)?,
                },
            )
            .with_checksum();

            std::thread::sleep(std::time::Duration::from_millis(10));

            let record_v2_conflict = Record {
                id: 1,
                value: "Updated by Node 3".to_string(),
                version: 2,
                timestamp: current_timestamp_micros(),
            };

            let node3_records = node3_db.collection::<Record>("records")?;
            node3_records.put(&record_v2_conflict)?;
            println!(
                "Node 3: Updated to v{} (conflicting)",
                record_v2_conflict.version
            );

            let log3 = ReplicationLog::new(
                "node-3".to_string(),
                ReplicationOperation::Put {
                    collection: "records".to_string(),
                    key: borsh::to_vec(&record_v2_conflict.id)?,
                    value: borsh::to_vec(&record_v2_conflict)?,
                },
            )
            .with_checksum();

            manager1.apply_replication(log2)?;
            manager1.apply_replication(log3)?;

            let node1_records = node1_db.collection::<Record>("records")?;
            let final_record = node1_records.get(&1)?.expect("Record should exist");
            println!(
                "Node 1 final state: v{}, value: {}\n",
                final_record.version, final_record.value
            );
        }

        // Batch replication
        {
            let node1_records = node1_db.collection::<Record>("records")?;
            let mut batch = node1_records.batch();
            let mut batch_ops = Vec::new();

            let timestamp = current_timestamp_micros();

            for i in 10..15 {
                let record = Record {
                    id: i,
                    value: format!("Batch record {}", i),
                    version: 1,
                    timestamp,
                };
                batch.put(&record)?;

                batch_ops.push(ngdb::BatchOp::Put {
                    key: borsh::to_vec(&record.id)?,
                    value: borsh::to_vec(&record)?,
                });
            }

            batch.commit()?;
            println!("Node 1: Committed batch of 5 records");

            let batch_log = ReplicationLog::new(
                "node-1".to_string(),
                ReplicationOperation::Batch {
                    collection: "records".to_string(),
                    operations: batch_ops,
                },
            )
            .with_checksum();

            manager2.apply_replication(batch_log.clone())?;
            manager3.apply_replication(batch_log)?;
            println!("Batch replicated to all nodes\n");
        }

        // Delete operation replication
        {
            let delete_id = 14u64;

            let node1_records = node1_db.collection::<Record>("records")?;
            node1_records.delete(&delete_id)?;
            println!("Node 1: Deleted record {}", delete_id);

            let delete_log = ReplicationLog::new(
                "node-1".to_string(),
                ReplicationOperation::Delete {
                    collection: "records".to_string(),
                    key: borsh::to_vec(&delete_id)?,
                },
            )
            .with_checksum();

            manager2.apply_replication(delete_log.clone())?;
            manager3.apply_replication(delete_log)?;
            println!("Delete replicated to all nodes\n");
        }

        // FirstWriteWins strategy
        {
            let config2_fww = ReplicationConfig::new("node-2")
                .enable()
                .with_peers(vec!["node-1".to_string(), "node-3".to_string()])
                .conflict_resolution(ConflictResolution::FirstWriteWins);

            let manager2_fww = ReplicationManager::new(node2_db.clone(), config2_fww)?;

            let initial_record = Record {
                id: 100,
                value: "First write".to_string(),
                version: 1,
                timestamp: current_timestamp_micros(),
            };

            let node2_records = node2_db.collection::<Record>("records")?;
            node2_records.put(&initial_record)?;
            println!("Node 2: Written initial record {}", initial_record.id);

            let update_record = Record {
                id: 100,
                value: "Second write (should be rejected)".to_string(),
                version: 2,
                timestamp: current_timestamp_micros(),
            };

            let update_log = ReplicationLog::new(
                "node-1".to_string(),
                ReplicationOperation::Put {
                    collection: "records".to_string(),
                    key: borsh::to_vec(&update_record.id)?,
                    value: borsh::to_vec(&update_record)?,
                },
            )
            .with_checksum();

            manager2_fww.apply_replication(update_log)?;

            let final_record = node2_records.get(&100)?.expect("Record should exist");
            println!("Node 2 kept: '{}'\n", final_record.value);
        }

        // Custom conflict resolution with version checking
        {
            let record_v1 = Record {
                id: 200,
                value: "Version 1".to_string(),
                version: 1,
                timestamp: current_timestamp_micros(),
            };

            let node3_records = node3_db.collection::<Record>("records")?;
            node3_records.put(&record_v1)?;
            println!("Node 3: Written v{}", record_v1.version);

            let record_v2 = Record {
                id: 200,
                value: "Version 2".to_string(),
                version: 2,
                timestamp: current_timestamp_micros(),
            };

            let log_v2 = ReplicationLog::new(
                "node-1".to_string(),
                ReplicationOperation::Put {
                    collection: "records".to_string(),
                    key: borsh::to_vec(&record_v2.id)?,
                    value: borsh::to_vec(&record_v2)?,
                },
            )
            .with_checksum();

            manager3.apply_replication(log_v2)?;

            let final_record = node3_records.get(&200)?.expect("Record should exist");
            println!("Node 3 accepted: v{}\n", final_record.version);
        }

        // Consistency verification across all nodes
        {
            let node1_records = node1_db.collection::<Record>("records")?;
            let node2_records = node2_db.collection::<Record>("records")?;
            let node3_records = node3_db.collection::<Record>("records")?;

            let count1 = node1_records.iter()?.count()?;
            let count2 = node2_records.iter()?.count()?;
            let count3 = node3_records.iter()?.count()?;

            println!("Node 1: {} records", count1);
            println!("Node 2: {} records", count2);
            println!("Node 3: {} records\n", count3);
        }

        // Replication statistics
        {
            let stats1 = manager1.stats().unwrap();
            let stats2 = manager2.stats().unwrap();
            let stats3 = manager3.stats().unwrap();

            println!("Node 1: {} operations applied", stats1.total_operations);
            println!("Node 2: {} operations applied", stats2.total_operations);
            println!("Node 3: {} operations applied\n", stats3.total_operations);
        }

        // Checksum verification
        {
            let record = Record {
                id: 300,
                value: "Checksum test".to_string(),
                version: 1,
                timestamp: current_timestamp_micros(),
            };

            let log_with_checksum = ReplicationLog::new(
                "node-1".to_string(),
                ReplicationOperation::Put {
                    collection: "records".to_string(),
                    key: borsh::to_vec(&record.id)?,
                    value: borsh::to_vec(&record)?,
                },
            )
            .with_checksum();

            println!(
                "Created log with checksum: {:?}",
                log_with_checksum.checksum
            );
            manager2.apply_replication(log_with_checksum)?;
        }

        node1_db.shutdown()?;
        node2_db.shutdown()?;
        node3_db.shutdown()?;
    }

    let _ = std::fs::remove_dir_all(&node1_path);
    let _ = std::fs::remove_dir_all(&node2_path);
    let _ = std::fs::remove_dir_all(&node3_path);

    Ok(())
}

fn current_timestamp_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("System time before Unix epoch")
        .as_micros() as u64
}
