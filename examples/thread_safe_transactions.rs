//! Thread-safe transaction example demonstrating concurrent access patterns

use ngdb::{DatabaseConfig, Result, Storable, ngdb};
use std::sync::Arc;
use std::thread;

#[ngdb("counters")]
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

    let counters = Counter::collection(&db)?;

    // Shared transaction across threads
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
    println!("Committed shared transaction across 3 threads");

    // Transaction read isolation
    {
        let txn = db.transaction()?;
        let txn_counters = txn.collection::<Counter>("counters")?;

        txn_counters.put(&Counter {
            id: "isolated".to_string(),
            value: 999,
        })?;

        let read_back = txn_counters.get(&"isolated".to_string())?;
        println!(
            "Within transaction, value: {:?}",
            read_back.as_ref().map(|c| c.value)
        );

        txn.rollback()?;

        let after_rollback = counters.get(&"isolated".to_string())?;
        println!("After rollback, value: {:?}", after_rollback);
    }

    // Concurrent independent transactions
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
    println!("Completed 3 concurrent independent transactions");

    // Iteration status tracking
    let status = counters.iter()?.for_each(|counter| counter.value < 1000)?;

    match status {
        ngdb::IterationStatus::Completed => println!("Iteration completed all records"),
        ngdb::IterationStatus::StoppedEarly => println!("Iteration stopped early"),
    }

    let count = counters.iter()?.count()?;
    println!("Total counters: {}", count);

    db.shutdown()?;
    Ok(())
}
