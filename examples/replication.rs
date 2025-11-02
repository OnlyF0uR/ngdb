//! Basic replication example demonstrating ReplicationManager usage
//!
//! This example shows how to:
//! - Set up a replication manager
//! - Create replication logs for operations
//! - Apply replication logs to replica nodes
//! - Verify data consistency across nodes

use borsh::{BorshDeserialize, BorshSerialize};
use ngdb::{
    DatabaseConfig, ReplicationConfig, ReplicationLog, ReplicationManager, ReplicationOperation,
    Result, Storable,
};

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
struct Document {
    id: u64,
    title: String,
    content: String,
    version: u64,
}

impl Storable for Document {
    type Key = u64;

    fn key(&self) -> Self::Key {
        self.id
    }

    fn validate(&self) -> Result<()> {
        if self.title.is_empty() {
            return Err(ngdb::Error::InvalidData(
                "Title cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    // Setup primary node
    let primary_db = DatabaseConfig::new("./data/replication_primary")
        .create_if_missing(true)
        .add_column_family("documents")
        .open()?;

    let primary_docs = primary_db.collection::<Document>("documents")?;

    // Setup replica node with replication manager
    let replica_db = DatabaseConfig::new("./data/replication_replica")
        .create_if_missing(true)
        .add_column_family("documents")
        .open()?;

    let replication_config = ReplicationConfig::new("replica-1")
        .enable()
        .with_peers(vec!["primary-1".to_string()]);

    let replica_manager = ReplicationManager::new(replica_db.clone(), replication_config)?;

    // Single write with replication
    {
        let doc = Document {
            id: 1,
            title: "Introduction to NGDB".to_string(),
            content: "NGDB is a high-performance RocksDB wrapper".to_string(),
            version: 1,
        };

        primary_docs.put(&doc)?;
        println!("Primary: Written document {}", doc.id);

        let replication_log = ReplicationLog::new(
            "primary-1".to_string(),
            ReplicationOperation::Put {
                collection: "documents".to_string(),
                key: borsh::to_vec(&doc.id)?,
                value: borsh::to_vec(&doc)?,
            },
        )
        .with_checksum();

        replica_manager.apply_replication(replication_log)?;
        println!("Replica: Applied replication log");

        let replica_docs = replica_db.collection::<Document>("documents")?;
        let replica_doc = replica_docs.get(&1)?;
        if replica_doc == Some(doc.clone()) {
            println!("Data consistent across nodes\n");
        }
    }

    // Batch operations
    {
        let mut batch = primary_docs.batch();
        let mut batch_ops = Vec::new();

        for i in 2..=5 {
            let doc = Document {
                id: i,
                title: format!("Document {}", i),
                content: format!("Content for document {}", i),
                version: 1,
            };
            batch.put(&doc)?;

            batch_ops.push(ngdb::BatchOp::Put {
                key: borsh::to_vec(&doc.id)?,
                value: borsh::to_vec(&doc)?,
            });
        }

        batch.commit()?;
        println!("Primary: Committed batch of 4 documents");

        let batch_log = ReplicationLog::new(
            "primary-1".to_string(),
            ReplicationOperation::Batch {
                collection: "documents".to_string(),
                operations: batch_ops,
            },
        )
        .with_checksum();

        replica_manager.apply_replication(batch_log)?;
        println!("Replica: Applied batch replication");

        let replica_docs = replica_db.collection::<Document>("documents")?;
        let count = replica_docs.iter()?.count()?;
        println!("Replica has {} documents\n", count);
    }

    // Delete operation
    {
        let delete_id = 5u64;
        primary_docs.delete(&delete_id)?;
        println!("Primary: Deleted document {}", delete_id);

        let delete_log = ReplicationLog::new(
            "primary-1".to_string(),
            ReplicationOperation::Delete {
                collection: "documents".to_string(),
                key: borsh::to_vec(&delete_id)?,
            },
        )
        .with_checksum();

        replica_manager.apply_replication(delete_log)?;
        println!("Replica: Applied delete operation\n");
    }

    // Update with version increment
    {
        let mut doc = primary_docs.get(&1)?.expect("Document should exist");
        doc.content = "Updated content".to_string();
        doc.version = 2;

        primary_docs.put(&doc)?;
        println!("Primary: Updated document {} to v{}", doc.id, doc.version);

        let update_log = ReplicationLog::new(
            "primary-1".to_string(),
            ReplicationOperation::Put {
                collection: "documents".to_string(),
                key: borsh::to_vec(&doc.id)?,
                value: borsh::to_vec(&doc)?,
            },
        )
        .with_checksum();

        replica_manager.apply_replication(update_log)?;

        let replica_docs = replica_db.collection::<Document>("documents")?;
        let replica_doc = replica_docs.get(&1)?;
        println!(
            "Replica: Document version is now v{}\n",
            replica_doc.as_ref().unwrap().version
        );
    }

    // Using helper methods
    {
        let doc = Document {
            id: 10,
            title: "Helper Test".to_string(),
            content: "Testing helper methods".to_string(),
            version: 1,
        };

        primary_docs.put(&doc)?;

        let log = replica_manager.create_put_log(
            "documents",
            borsh::to_vec(&doc.id)?,
            borsh::to_vec(&doc)?,
        );

        replica_manager.apply_replication(log)?;
        println!("Applied using helper method\n");
    }

    // Idempotency - applying same operation twice
    {
        let doc = Document {
            id: 20,
            title: "Idempotency Test".to_string(),
            content: "Testing duplicate application".to_string(),
            version: 1,
        };

        primary_docs.put(&doc)?;

        let log = ReplicationLog::new(
            "primary-1".to_string(),
            ReplicationOperation::Put {
                collection: "documents".to_string(),
                key: borsh::to_vec(&doc.id)?,
                value: borsh::to_vec(&doc)?,
            },
        )
        .with_checksum();

        replica_manager.apply_replication(log.clone())?;
        replica_manager.apply_replication(log)?;
        println!("Applied same operation twice (idempotent)\n");
    }

    // Replication statistics
    {
        let stats = replica_manager.stats().unwrap();
        println!("Total operations applied: {}", stats.total_operations);
        if let Some(oldest) = stats.oldest_operation_timestamp {
            println!("Oldest operation timestamp: {} us", oldest);
        }
        if let Some(newest) = stats.newest_operation_timestamp {
            println!("Newest operation timestamp: {} us", newest);
        }
    }

    // Final verification
    {
        let replica_docs = replica_db.collection::<Document>("documents")?;
        let primary_count = primary_docs.iter()?.count()?;
        let replica_count = replica_docs.iter()?.count()?;

        println!("Primary: {} documents", primary_count);
        println!("Replica: {} documents", replica_count);
    }

    primary_db.shutdown()?;
    replica_db.shutdown()?;

    Ok(())
}
