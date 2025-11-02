//! Advanced example demonstrating batches, snapshots, and iterations

use ngdb::{DatabaseConfig, IterationStatus, Result, Storable, ngdb};

#[ngdb("records")]
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

    let records = Record::collection(&db)?;

    // Batch operations - efficient bulk writes
    let mut batch = records.batch();
    for i in 0..100 {
        batch.put(&Record {
            id: i,
            value: format!("record_{}", i),
        })?;
    }
    batch.commit()?;

    // Multi-get - efficient bulk reads
    let keys: Vec<u64> = vec![0, 10, 20, 30];
    let results = records.get_many(&keys)?;
    assert_eq!(results.len(), 4);

    // Snapshots - consistent point-in-time view
    let snapshot = records.snapshot();

    // Modify data after snapshot
    records.put(&Record {
        id: 0,
        value: "modified".to_string(),
    })?;

    // Snapshot still sees old data
    let snapshot_value = snapshot.get(&0)?;
    let current_value = records.get(&0)?;
    assert_ne!(
        snapshot_value.as_ref().map(|r| &r.value),
        current_value.as_ref().map(|r| &r.value)
    );

    // Iteration
    let count = records.iter()?.count()?;
    assert_eq!(count, 100);

    // Collect all into vector
    let all = records.iter()?.collect_all()?;
    assert_eq!(all.len(), 100);

    // Iterate with callback
    let mut found = 0;
    let status = records.iter()?.for_each(|record| {
        if record.id % 10 == 0 {
            found += 1;
        }
        record.id < 50 // Stop at 50
    })?;

    assert!(matches!(status, IterationStatus::StoppedEarly));
    assert!(found > 0);

    // Iterate from a starting point
    let from_50 = records.iter_from(&50)?.count()?;
    assert_eq!(from_50, 50);

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
