//! Thread-safe transaction example demonstrating concurrent access patterns

use bincode::{Decode, Encode};
use ngdb::{DatabaseConfig, Result, Storable};
use std::sync::Arc;
use std::thread;

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
struct Counter {
    id: String,
    value: i64,
}

impl Storable for Counter {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.id.clone()
    }
}

fn main() -> Result<()> {
    let db = DatabaseConfig::new("./data/thread_safe")
        .create_if_missing(true)
        .add_column_family("counters")
        .open()?;

    let counters = db.collection::<Counter>("counters")?;

    // Example 1: Shared transaction across threads
    println!("Example 1: Shared transaction");
    {
        let txn = Arc::new(db.transaction()?);
        let mut handles = vec![];

        for i in 0..3 {
            let txn_clone = Arc::clone(&txn);
            let handle = thread::spawn(move || {
                let collection = txn_clone
                    .collection::<Counter>("counters")
                    .expect("Failed to get collection");

                collection
                    .put(&Counter {
                        id: format!("thread_{}", i),
                        value: i as i64 * 100,
                    })
                    .expect("Failed to put counter");
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        match Arc::try_unwrap(txn) {
            Ok(transaction) => transaction.commit()?,
            Err(_) => panic!("Failed to unwrap Arc"),
        }
    }
    println!("Committed shared transaction\n");

    // Example 2: Transaction read isolation
    println!("Example 2: Read isolation");
    {
        let txn = db.transaction()?;
        let txn_counters = txn.collection::<Counter>("counters")?;

        txn_counters.put(&Counter {
            id: "isolated".to_string(),
            value: 999,
        })?;

        // Read uncommitted write within transaction
        let read_back = txn_counters.get(&"isolated".to_string())?;
        assert_eq!(read_back.as_ref().map(|c| c.value), Some(999));

        txn.rollback()?;

        // Verify rollback
        let after_rollback = counters.get(&"isolated".to_string())?;
        assert!(after_rollback.is_none());
    }
    println!("Verified transaction isolation\n");

    // Example 3: Concurrent independent transactions
    println!("Example 3: Concurrent transactions");
    {
        let mut handles = vec![];

        for i in 0..3 {
            let db_clone = db.clone();
            let handle = thread::spawn(move || {
                let txn = db_clone
                    .transaction()
                    .expect("Failed to create transaction");
                let collection = txn
                    .collection::<Counter>("counters")
                    .expect("Failed to get collection");

                collection
                    .put(&Counter {
                        id: format!("concurrent_{}", i),
                        value: i as i64 * 1000,
                    })
                    .expect("Failed to put");

                txn.commit().expect("Failed to commit");
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
    println!("Completed concurrent transactions\n");

    // Example 4: Iteration status tracking
    println!("Example 4: Iteration");
    let status = counters.iter().for_each(|counter| {
        // Stop iteration early at value >= 1000
        counter.value < 1000
    })?;

    match status {
        ngdb::IterationStatus::Completed => println!("Iteration completed"),
        ngdb::IterationStatus::StoppedEarly => println!("Iteration stopped early"),
    }

    // Final state
    let count = counters.iter().count()?;
    println!("\nTotal counters: {}", count);

    db.shutdown()?;
    Ok(())
}
