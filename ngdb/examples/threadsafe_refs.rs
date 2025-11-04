//! Thread-safe references example for NGDB
//!
//! Demonstrates that Ref<T> can now be safely shared across threads
//! thanks to parking_lot::RwLock replacing RefCell.

use ngdb::{DatabaseConfig, Ref, Result, Storable, ngdb};
use std::sync::Arc;
use std::thread;

#[ngdb("users")]
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

#[ngdb("posts")]
struct Post {
    id: u64,
    title: String,
    content: String,
    author: Ref<User>,
}

impl Storable for Post {
    type Key = u64;
    fn key(&self) -> Self::Key {
        self.id
    }
}

fn main() -> Result<()> {
    let db = Arc::new(
        DatabaseConfig::new("./data/threadsafe_refs_example")
            .create_if_missing(true)
            .add_column_family("users")
            .add_column_family("posts")
            .open()?,
    );

    // Create and save users
    let alice = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    alice.save(&db)?;

    let bob = User {
        id: 2,
        name: "Bob".to_string(),
        email: "bob@example.com".to_string(),
    };
    bob.save(&db)?;

    // Create posts
    let post1 = Post {
        id: 1,
        title: "Introduction to NGDB".to_string(),
        content: "NGDB is a high-performance RocksDB wrapper...".to_string(),
        author: Ref::new(1),
    };
    post1.save(&db)?;

    let post2 = Post {
        id: 2,
        title: "Advanced NGDB Features".to_string(),
        content: "Learn about transactions, replication...".to_string(),
        author: Ref::new(2),
    };
    post2.save(&db)?;

    // Example 1
    let mut handles = vec![];

    for i in 1..=2 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || -> Result<()> {
            let posts = Post::collection(&db_clone)?;
            let post = posts.get(&i)?.unwrap();

            // Ref<T> is now thread-safe! This works across threads
            let author = post.author.get(&db_clone)?;

            println!(
                "[Thread {:?}] Post {}: '{}' by {}",
                thread::current().id(),
                i,
                post.title,
                author.name
            );

            Ok(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap()?;
    }

    // Example 2
    let posts = Post::collection(&db)?;
    let post = posts.get(&1)?.unwrap();

    // Pre-resolve the reference
    let _ = post.author.get(&db)?;

    // Share the post across multiple threads - they'll all use the cached value
    let post_arc = Arc::new(post);
    let mut handles = vec![];

    for i in 0..5 {
        let db_clone = Arc::clone(&db);
        let post_clone = Arc::clone(&post_arc);

        let handle = thread::spawn(move || -> Result<()> {
            // All threads can read the cached reference concurrently
            let author = post_clone.author.get(&db_clone)?;
            println!("[Thread {}] Author: {}", i, author.name);
            Ok(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap()?;
    }

    // Example 3
    let post_ids = vec![1, 2];
    let chunk_size = 1;
    let mut handles = vec![];

    for chunk in post_ids.chunks(chunk_size) {
        let db_clone = Arc::clone(&db);
        let chunk = chunk.to_vec();

        let handle = thread::spawn(move || -> Result<Vec<String>> {
            let posts = Post::collection(&db_clone)?;
            let mut results = Vec::new();

            for id in chunk {
                if let Some(post) = posts.get(&id)? {
                    let author = post.author.get(&db_clone)?;
                    let summary = format!("{} by {}", post.title, author.name);
                    results.push(summary);
                }
            }

            Ok(results)
        });
        handles.push(handle);
    }

    for handle in handles {
        let results = handle.join().unwrap()?;
        for summary in results {
            println!("  - {}", summary);
        }
    }

    use std::sync::mpsc;

    let (tx, rx) = mpsc::channel();
    let num_workers = 3;
    let mut workers = vec![];

    // Spawn worker threads
    for worker_id in 0..num_workers {
        let db_clone = Arc::clone(&db);
        let tx_clone = tx.clone();

        let worker = thread::spawn(move || {
            let posts = Post::collection(&db_clone).unwrap();

            // Each worker processes posts assigned to it
            for post_id in 1..=2 {
                if post_id % num_workers == worker_id {
                    if let Ok(Some(post)) = posts.get(&post_id) {
                        // Ref<T> safely crosses thread boundaries
                        let author = post.author.get(&db_clone).unwrap();
                        let result = format!(
                            "[Worker {}] Processed: '{}', Author: {}",
                            worker_id, post.title, author.name
                        );
                        tx_clone.send(result).unwrap();
                    }
                }
            }
        });
        workers.push(worker);
    }

    drop(tx); // Close sender so rx knows when to stop

    for received in rx {
        println!("  {}", received);
    }

    for worker in workers {
        worker.join().unwrap();
    }

    Ok(())
}
