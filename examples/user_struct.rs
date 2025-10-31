//! User struct example demonstrating custom types with validation

use bincode::{Decode, Encode};
use ngdb::{DatabaseConfig, Result, Storable};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct User {
    id: u64,
    username: String,
    email: String,
    age: u32,
    active: bool,
}

impl Storable for User {
    type Key = u64;

    fn key(&self) -> Self::Key {
        self.id
    }

    // Validate data before storing
    fn validate(&self) -> Result<()> {
        if self.username.is_empty() {
            return Err(ngdb::Error::InvalidValue(
                "Username cannot be empty".to_string(),
            ));
        }
        if self.username.len() < 3 {
            return Err(ngdb::Error::InvalidValue(
                "Username must be at least 3 characters".to_string(),
            ));
        }
        if !self.email.contains('@') {
            return Err(ngdb::Error::InvalidValue(
                "Email must contain @".to_string(),
            ));
        }
        if self.age < 13 {
            return Err(ngdb::Error::InvalidValue(
                "User must be at least 13 years old".to_string(),
            ));
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    // Open database
    let db = DatabaseConfig::new("./data/users_example")
        .create_if_missing(true)
        .add_column_family("users")
        .open()?;

    let users = db.collection::<User>("users")?;

    // Create and store users
    let alice = User {
        id: 1,
        username: "alice".to_string(),
        email: "alice@example.com".to_string(),
        age: 28,
        active: true,
    };
    users.put(&alice)?;

    let bob = User {
        id: 2,
        username: "bob".to_string(),
        email: "bob@example.com".to_string(),
        age: 35,
        active: true,
    };
    users.put(&bob)?;

    // Retrieve a user
    if let Some(user) = users.get(&1)? {
        println!("Found user: {} ({})", user.username, user.email);
    }

    // Check existence
    for id in 1..=3 {
        if users.exists(&id)? {
            println!("User {} exists", id);
        }
    }

    // Multi-get for batch retrieval
    let ids = vec![1, 2, 999];
    let results = users.get_many(&ids)?;
    for (id, result) in ids.iter().zip(results.iter()) {
        match result {
            Some(user) => println!("ID {}: {}", id, user.username),
            None => println!("ID {}: not found", id),
        }
    }

    // Iterate over all users
    println!("All users:");
    users.iter().for_each(|user| {
        let status = if user.active { "active" } else { "inactive" };
        println!("  {} - {} ({})", user.username, user.email, status);
        true
    })?;

    // Update a user
    let mut updated = users.get(&2)?.unwrap();
    updated.email = "bob.new@example.com".to_string();
    users.put(&updated)?;

    // Test validation
    let invalid = User {
        id: 3,
        username: "ab".to_string(), // Too short
        email: "invalid".to_string(),
        age: 10, // Too young
        active: true,
    };

    match users.put(&invalid) {
        Ok(_) => println!("Error: invalid user was accepted"),
        Err(e) => println!("Validation rejected: {}", e),
    }

    // Delete a user
    users.delete(&1)?;
    println!("Deleted user 1");

    // Statistics
    let count = users.iter().count()?;
    println!("Final user count: {}", count);

    db.shutdown()?;
    Ok(())
}
