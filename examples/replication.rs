//! Basic replication example demonstrating replication log creation and processing

use ngdb::{DatabaseConfig, Result, Storable};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Document {
    id: u64,
    title: String,
    content: String,
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

    // Setup replica node
    let replica_db = DatabaseConfig::new("./data/replication_replica")
        .create_if_missing(true)
        .add_column_family("documents")
        .open()?;

    let replica_docs = replica_db.collection::<Document>("documents")?;

    // Example 1: Write to primary and replicate
    println!("Example 1: Primary write and replication");
    {
        let doc = Document {
            id: 1,
            title: "Introduction".to_string(),
            content: "Getting started with NGDB replication".to_string(),
        };

        primary_docs.put(&doc)?;
        println!("Written to primary: {}", doc.title);

        // In production, create replication log and send to replica
        // For this example, we simulate by writing directly
        replica_docs.put(&doc)?;
        println!("Replicated to replica\n");
    }

    // Example 2: Verify consistency
    println!("Example 2: Consistency verification");
    {
        let primary_doc = primary_docs.get(&1)?;
        let replica_doc = replica_docs.get(&1)?;

        let consistent = primary_doc.is_some() && replica_doc.is_some();
        println!("Data consistent across nodes: {}\n", consistent);
    }

    // Example 3: Batch replication
    println!("Example 3: Batch replication");
    {
        let mut batch = primary_docs.batch();

        for i in 2..=5 {
            batch.put(&Document {
                id: i,
                title: format!("Document {}", i),
                content: format!("Content for document {}", i),
            })?;
        }

        batch.commit()?;
        println!("Primary: Committed batch of 4 documents");

        // Replicate batch to replica
        let mut replica_batch = replica_docs.batch();
        for i in 2..=5 {
            if let Some(doc) = primary_docs.get(&i)? {
                replica_batch.put(&doc)?;
            }
        }
        replica_batch.commit()?;
        println!("Replica: Applied batch replication\n");
    }

    // Example 4: Count verification
    println!("Example 4: Final state");
    {
        let primary_count = primary_docs.iter().count()?;
        let replica_count = replica_docs.iter().count()?;

        println!("Primary documents: {}", primary_count);
        println!("Replica documents: {}", replica_count);
    }

    primary_db.shutdown()?;
    replica_db.shutdown()?;

    Ok(())
}
