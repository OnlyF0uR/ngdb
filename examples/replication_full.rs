//! Comprehensive replication example demonstrating conflict resolution and node coordination

use ngdb::{DatabaseConfig, Result, Storable};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    id: u64,
    value: String,
    version: u64,
    node_id: String,
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

fn setup_node(db_path: &str) -> Result<ngdb::Database> {
    DatabaseConfig::new(db_path)
        .create_if_missing(true)
        .add_column_family("records")
        .open()
}

fn main() -> Result<()> {
    let temp_dir = std::env::temp_dir();
    let node1_path = temp_dir.join("ngdb_node1");
    let node2_path = temp_dir.join("ngdb_node2");
    let node3_path = temp_dir.join("ngdb_node3");

    // Cleanup
    let _ = std::fs::remove_dir_all(&node1_path);
    let _ = std::fs::remove_dir_all(&node2_path);
    let _ = std::fs::remove_dir_all(&node3_path);

    // Example 1: Multi-node setup
    println!("Example 1: Setting up nodes");
    let node1 = setup_node(node1_path.to_str().unwrap())?;
    let node2 = setup_node(node2_path.to_str().unwrap())?;
    let node3 = setup_node(node3_path.to_str().unwrap())?;

    let node1_records = node1.collection::<Record>("records")?;
    let node2_records = node2.collection::<Record>("records")?;
    let node3_records = node3.collection::<Record>("records")?;
    println!("Initialized 3 nodes\n");

    // Example 2: Write to node 1 and replicate
    println!("Example 2: Primary write and replication");
    {
        let record = Record {
            id: 1,
            value: "Data from Node 1".to_string(),
            version: 1,
            node_id: "node-1".to_string(),
        };

        node1_records.put(&record)?;
        println!("Node 1: Written record {}", record.id);

        // Simulate replication to peers
        node2_records.put(&record)?;
        node3_records.put(&record)?;
        println!("Replicated to Node 2 and Node 3\n");
    }

    // Example 3: Concurrent writes (conflict scenario)
    println!("Example 3: Handling concurrent writes");
    {
        // Node 2 writes
        let record_v2 = Record {
            id: 1,
            value: "Updated by Node 2".to_string(),
            version: 2,
            node_id: "node-2".to_string(),
        };
        node2_records.put(&record_v2)?;
        println!(
            "Node 2: Updated record {} to v{}",
            record_v2.id, record_v2.version
        );

        // Node 3 writes conflicting update
        let record_v2_conflict = Record {
            id: 1,
            value: "Updated by Node 3".to_string(),
            version: 2,
            node_id: "node-3".to_string(),
        };
        node3_records.put(&record_v2_conflict)?;
        println!(
            "Node 3: Updated record {} to v{}",
            record_v2_conflict.id, record_v2_conflict.version
        );

        // Last-write-wins: Node 3's write takes precedence
        node1_records.put(&record_v2_conflict)?;
        node2_records.put(&record_v2_conflict)?;
        println!("Conflict resolved: Last-write-wins applied\n");
    }

    // Example 4: Batch replication
    println!("Example 4: Batch replication");
    {
        let mut batch = node1_records.batch();

        for i in 10..15 {
            batch.put(&Record {
                id: i,
                value: format!("Batch record {}", i),
                version: 1,
                node_id: "node-1".to_string(),
            })?;
        }

        batch.commit()?;
        println!("Node 1: Committed batch of 5 records");

        // Replicate batch to other nodes
        for i in 10..15 {
            if let Some(record) = node1_records.get(&i)? {
                node2_records.put(&record)?;
                node3_records.put(&record)?;
            }
        }
        println!("Batch replicated to all nodes\n");
    }

    // Example 5: Verify consistency
    println!("Example 5: Consistency verification");
    {
        let node1_count = node1_records.iter().count()?;
        let node2_count = node2_records.iter().count()?;
        let node3_count = node3_records.iter().count()?;

        println!("Node 1 records: {}", node1_count);
        println!("Node 2 records: {}", node2_count);
        println!("Node 3 records: {}", node3_count);

        let consistent = node1_count == node2_count && node2_count == node3_count;
        println!("All nodes consistent: {}\n", consistent);
    }

    // Example 6: Snapshot isolation during replication
    println!("Example 6: Snapshot isolation");
    {
        let snapshot = node1_records.snapshot();

        // Modify data after snapshot
        node1_records.put(&Record {
            id: 100,
            value: "New record".to_string(),
            version: 1,
            node_id: "node-1".to_string(),
        })?;

        // Snapshot sees old state
        let snapshot_result = snapshot.get(&100)?;
        let current_result = node1_records.get(&100)?;

        println!("Snapshot sees record 100: {}", snapshot_result.is_some());
        println!("Current sees record 100: {}\n", current_result.is_some());
    }

    // Example 7: Transaction-based replication
    println!("Example 7: Transaction replication");
    {
        let txn = node1.transaction()?;
        let txn_records = txn.collection::<Record>("records")?;

        txn_records.put(&Record {
            id: 200,
            value: "Transactional record".to_string(),
            version: 1,
            node_id: "node-1".to_string(),
        })?;

        txn_records.put(&Record {
            id: 201,
            value: "Another transactional record".to_string(),
            version: 1,
            node_id: "node-1".to_string(),
        })?;

        txn.commit()?;
        println!("Node 1: Committed transaction with 2 records");

        // Replicate transaction results
        if let Some(r1) = node1_records.get(&200)? {
            node2_records.put(&r1)?;
            node3_records.put(&r1)?;
        }
        if let Some(r2) = node1_records.get(&201)? {
            node2_records.put(&r2)?;
            node3_records.put(&r2)?;
        }
        println!("Transaction results replicated\n");
    }

    // Example 8: Multi-get for efficient replication
    println!("Example 8: Multi-get replication");
    {
        let keys = vec![1, 10, 11, 12, 200];
        let records = node1_records.get_many(&keys)?;

        println!("Retrieved {} records via multi-get", records.len());

        // Replicate retrieved records
        for record in records.into_iter().flatten() {
            node2_records.put(&record)?;
        }
        println!("Multi-get results replicated\n");
    }

    // Final state
    println!("Final state:");
    let final_count = node1_records.iter().count()?;
    println!("Total records across cluster: {}", final_count);

    // Cleanup
    node1.shutdown()?;
    node2.shutdown()?;
    node3.shutdown()?;

    let _ = std::fs::remove_dir_all(&node1_path);
    let _ = std::fs::remove_dir_all(&node2_path);
    let _ = std::fs::remove_dir_all(&node3_path);

    Ok(())
}
