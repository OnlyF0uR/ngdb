//! Transaction example demonstrating atomic operations across multiple writes

use bincode::{Decode, Encode};
use ngdb::{DatabaseConfig, Result, Storable};

#[derive(Debug, Clone, Encode, Decode)]
struct Account {
    id: u64,
    name: String,
    balance: i64,
}

impl Storable for Account {
    type Key = u64;

    fn key(&self) -> Self::Key {
        self.id
    }

    fn validate(&self) -> Result<()> {
        if self.balance < 0 {
            return Err(ngdb::Error::InvalidData(
                "Account balance cannot be negative".to_string(),
            ));
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    // Setup database
    let db = DatabaseConfig::new("./data/transactions")
        .create_if_missing(true)
        .add_column_family("accounts")
        .open()?;

    let accounts = db.collection::<Account>("accounts")?;

    // Create initial accounts
    accounts.put(&Account {
        id: 1,
        name: "Alice".to_string(),
        balance: 1000,
    })?;

    accounts.put(&Account {
        id: 2,
        name: "Bob".to_string(),
        balance: 500,
    })?;

    println!("Initial - Alice: 1000, Bob: 500");

    // Example 1: Successful transfer with transaction
    {
        let txn = db.transaction()?;
        let txn_accounts = txn.collection::<Account>("accounts")?;

        // Read current values
        let mut alice = txn_accounts.get(&1)?.unwrap();
        let mut bob = txn_accounts.get(&2)?.unwrap();

        // Perform transfer
        alice.balance -= 200;
        bob.balance += 200;

        // Write updated values
        txn_accounts.put(&alice)?;
        txn_accounts.put(&bob)?;

        // Read isolation - can see uncommitted writes
        let alice_check = txn_accounts.get(&1)?.unwrap();
        assert_eq!(alice_check.balance, 800);

        // Commit atomically
        txn.commit()?;
    }

    println!("After transfer - Alice: 800, Bob: 700");

    // Example 2: Transaction with validation failure
    let result = {
        let txn = db.transaction()?;
        let txn_accounts = txn.collection::<Account>("accounts")?;

        let mut alice = txn_accounts.get(&1)?.unwrap();
        alice.balance -= 1000; // Would make balance negative

        // This will fail validation
        txn_accounts.put(&alice)
    };

    match result {
        Ok(_) => println!("Unexpected success"),
        Err(e) => println!("Validation failed as expected: {}", e),
    }

    // Balances unchanged due to validation failure
    println!("After failed transaction - Alice: 800, Bob: 700");

    // Example 3: Explicit rollback
    {
        let txn = db.transaction()?;
        let txn_accounts = txn.collection::<Account>("accounts")?;

        let mut alice = txn_accounts.get(&1)?.unwrap();
        alice.balance += 500;
        txn_accounts.put(&alice)?;

        // Rollback instead of commit
        txn.rollback()?;
    }

    println!("After rollback - Alice: 800, Bob: 700");

    // Example 4: Transaction state management
    {
        let txn = db.transaction()?;
        let txn_accounts = txn.collection::<Account>("accounts")?;

        println!("Transaction is empty: {}", txn.is_empty()?);

        let mut alice = txn_accounts.get(&1)?.unwrap();
        alice.balance += 100;
        txn_accounts.put(&alice)?;

        println!("Transaction operations: {}", txn.len()?);

        // Clear and start over
        txn.clear()?;
        println!("After clear: {}", txn.len()?);

        alice.balance += 50;
        txn_accounts.put(&alice)?;
        txn.commit()?;
    }

    println!("After state management - Alice: 850, Bob: 700");

    db.shutdown()?;
    Ok(())
}
