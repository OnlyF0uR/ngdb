//! Backup and restore example demonstrating disaster recovery capabilities

use borsh::{BorshDeserialize, BorshSerialize};
use ngdb::{Database, DatabaseConfig, Result, Storable};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct User {
    id: u64,
    name: String,
    email: String,
}

impl Storable for User {
    type Key = u64;

    fn key(&self) -> Self::Key {
        self.id
    }
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct Post {
    id: u64,
    user_id: u64,
    title: String,
    content: String,
}

impl Storable for Post {
    type Key = u64;

    fn key(&self) -> Self::Key {
        self.id
    }
}

fn create_sample_data(db: &Database) -> Result<()> {
    let users = db.collection::<User>("users")?;
    let posts = db.collection::<Post>("posts")?;

    // Add users
    for i in 1..=5 {
        users.put(&User {
            id: i,
            name: format!("User {}", i),
            email: format!("user{}@example.com", i),
        })?;
    }

    // Add posts
    for i in 1..=10 {
        posts.put(&Post {
            id: i,
            user_id: (i % 5) + 1,
            title: format!("Post {}", i),
            content: format!("Content for post {}", i),
        })?;
    }

    Ok(())
}

fn print_counts(db: &Database) -> Result<()> {
    let users = db.collection::<User>("users")?;
    let posts = db.collection::<Post>("posts")?;

    let user_count = users.iter()?.count()?;
    let post_count = posts.iter()?.count()?;

    println!("Database: {} users, {} posts", user_count, post_count);
    Ok(())
}

fn main() -> Result<()> {
    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join("ngdb_backup_example");
    let backup_path = temp_dir.join("ngdb_backups");

    // Clean up from previous runs
    let _ = std::fs::remove_dir_all(&db_path);
    let _ = std::fs::remove_dir_all(&backup_path);

    // Example 1: Create initial backup
    println!("Example 1: Creating backup");
    {
        let db = DatabaseConfig::new(&db_path)
            .create_if_missing(true)
            .add_column_family("users")
            .add_column_family("posts")
            .open()?;

        create_sample_data(&db)?;
        print_counts(&db)?;

        db.backup(&backup_path)?;
        println!("Backup created\n");
    }

    // Example 2: List available backups
    println!("Example 2: Listing backups");
    let backups = Database::list_backups(&backup_path)?;
    println!("Found {} backup(s)\n", backups.len());

    // Example 3: Multiple backups
    println!("Example 3: Creating second backup");
    {
        let db = DatabaseConfig::new(&db_path)
            .create_if_missing(false)
            .add_column_family("users")
            .add_column_family("posts")
            .open()?;

        let users = db.collection::<User>("users")?;

        // Modify data
        users.put(&User {
            id: 6,
            name: "New User".to_string(),
            email: "newuser@example.com".to_string(),
        })?;
        users.delete(&1)?;

        db.backup(&backup_path)?;
        println!("Second backup created");

        let backups = Database::list_backups(&backup_path)?;
        println!("Total backups: {}\n", backups.len());
    }

    // Example 4: Restore from backup
    println!("Example 4: Restoring from backup");
    {
        let restore_path = temp_dir.join("ngdb_restored");
        let _ = std::fs::remove_dir_all(&restore_path);

        Database::restore_from_backup(&backup_path, &restore_path)?;

        let restored_db = DatabaseConfig::new(&restore_path)
            .create_if_missing(false)
            .add_column_family("users")
            .add_column_family("posts")
            .open()?;

        print_counts(&restored_db)?;
        println!("Restore completed\n");

        let _ = std::fs::remove_dir_all(&restore_path);
    }

    // Example 5: Backup validation
    println!("Example 5: Backup verification");
    {
        let restore_path = temp_dir.join("ngdb_verified");
        let _ = std::fs::remove_dir_all(&restore_path);

        Database::restore_from_backup(&backup_path, &restore_path)?;

        let verified_db = DatabaseConfig::new(&restore_path)
            .create_if_missing(false)
            .add_column_family("users")
            .add_column_family("posts")
            .open()?;

        let users = verified_db.collection::<User>("users")?;
        let posts = verified_db.collection::<Post>("posts")?;

        let user6_exists = users.exists(&6)?;
        let user1_exists = users.exists(&1)?;
        let post_count = posts.iter()?.count()?;

        println!("User 6 exists: {}", user6_exists);
        println!("User 1 exists: {}", user1_exists);
        println!("Post count: {}", post_count);

        let _ = std::fs::remove_dir_all(&restore_path);
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&db_path);
    let _ = std::fs::remove_dir_all(&backup_path);

    Ok(())
}
