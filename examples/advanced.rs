//! Advanced example demonstrating batches, snapshots, and iterations

use bincode::{Decode, Encode};
use ngdb::{DatabaseConfig, IterationStatus, Result, Storable};

#[derive(Debug, Clone, Encode, Decode)]
struct Record {
    id: u64,
    value: String,
}

impl Storable for Record {
    type Key = u64;
    fn key(&self) -> Self::Key {
        self.id
    }
}

fn main() -> Result<()> {
    let db = DatabaseConfig::new("./data/advanced")
        .create_if_missing(true)
        .add_column_family("records")
        .open()?;

    let records = db.collection::<Record>("records")?;

    // Batch operations - efficient bulk writes
    println!("Batch operations:");
    let mut batch = records.batch();
    for i in 0..100 {
        batch.put(&Record {
            id: i,
            value: format!("record_{}", i),
        })?;
    }
    println!("  Added {} operations to batch", batch.len());
    batch.commit()?;
    println!("  Batch committed");

    // Multi-get - efficient bulk reads
    println!("\nMulti-get:");
    let keys: Vec<u64> = vec![0, 10, 20, 30];
    let results = records.get_many(&keys)?;
    println!("  Retrieved {} records", results.len());

    // Snapshots - consistent point-in-time view
    println!("\nSnapshot:");
    let snapshot = records.snapshot();

    // Modify data after snapshot
    records.put(&Record {
        id: 0,
        value: "modified".to_string(),
    })?;

    // Snapshot still sees old data
    let snapshot_value = snapshot.get(&0)?;
    let current_value = records.get(&0)?;
    println!(
        "  Snapshot sees: {:?}",
        snapshot_value.as_ref().map(|r| &r.value)
    );
    println!(
        "  Current sees: {:?}",
        current_value.as_ref().map(|r| &r.value)
    );

    // Iteration
    println!("\nIteration:");
    let count = records.iter()?.count()?;
    println!("  Total records: {}", count);

    // Collect all into vector
    let all = records.iter()?.collect_all()?;
    println!("  Collected {} records", all.len());

    // Iterate with callback
    let mut found = 0;
    let status = records.iter()?.for_each(|record| {
        if record.id % 10 == 0 {
            found += 1;
        }
        record.id < 50 // Stop at 50
    })?;

    match status {
        IterationStatus::Completed => println!("  Iteration completed"),
        IterationStatus::StoppedEarly => println!("  Iteration stopped early"),
    }
    println!("  Found {} records divisible by 10", found);

    // Iterate from a starting point
    println!("\nIterate from key 50:");
    let from_50 = records.iter_from(&50)?.count()?;
    println!("  Records from 50: {}", from_50);

    // Range compaction
    println!("\nRange operations:");
    records.compact_range(Some(&0), Some(&50))?;
    println!("  Compacted range 0-50");

    // Statistics
    let estimated = records.estimate_num_keys()?;
    println!("  Estimated keys: {}", estimated);

    // Cleanup
    records.flush()?;
    db.shutdown()?;
    Ok(())
}
