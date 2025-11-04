//! Nested references example for NGDB
//!
//! Demonstrates relationships between objects with automatic reference resolution
//! using the new immutable API with interior mutability.

use ngdb::{DatabaseConfig, Ref, Result, Storable, ngdb};

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

#[ngdb("comments")]
struct Comment {
    id: u64,
    text: String,
    author: Ref<User>,
    post: Ref<Post>,
}

impl Storable for Comment {
    type Key = u64;
    fn key(&self) -> Self::Key {
        self.id
    }
}

fn main() -> Result<()> {
    let db = DatabaseConfig::new("./data/nested_refs_example")
        .create_if_missing(true)
        .add_column_family("users")
        .add_column_family("posts")
        .add_column_family("comments")
        .open()?;

    // Create and save a user
    let alice = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    alice.save(&db)?;

    // Create and save a post with a reference to the user
    let post = Post {
        id: 1,
        title: "Introduction to NGDB".to_string(),
        content: "NGDB is a high-performance RocksDB wrapper...".to_string(),
        author: Ref::from_value(alice.clone()),
    };
    post.save(&db)?;

    // Create and save comments
    Comment {
        id: 1,
        text: "Great article!".to_string(),
        author: Ref::new(1),
        post: Ref::new(1),
    }
    .save(&db)?;

    Comment {
        id: 2,
        text: "Very helpful, thanks!".to_string(),
        author: Ref::new(1),
        post: Ref::new(1),
    }
    .save(&db)?;

    Comment {
        id: 3,
        text: "Looking forward to more.".to_string(),
        author: Ref::new(1),
        post: Ref::new(1),
    }
    .save(&db)?;

    println!("=== Example 1: Manual reference resolution (no mut needed!) ===\n");

    let comments = Comment::collection(&db)?;

    // Notice: No mut needed! The new API uses interior mutability
    let comment1 = comments.get(&1)?.unwrap();
    println!("Comment: '{}'", comment1.text);

    // First call: Resolves from DB and caches
    let author = comment1.author.get(&db)?;
    println!("Author: {} ({})", author.name, author.email);
    drop(author); // Drop the borrow

    // Access the post reference - also auto-resolves
    let post = comment1.post.get(&db)?;
    println!("Post: '{}'", post.title);

    // Nested references work seamlessly
    let post_author = post.author.get(&db)?;
    println!("Post Author: {}", post_author.name);

    println!("\n=== Example 2: Multiple accesses use cached value ===\n");

    let comment2 = comments.get(&2)?.unwrap();

    // Check resolution status
    println!("Is author resolved? {}", comment2.author.is_resolved());

    // First access - resolves from DB
    let author = comment2.author.get(&db)?;
    println!("First access: {}", author.name);
    drop(author);

    // Now it's resolved and cached
    println!("Is author resolved? {}", comment2.author.is_resolved());

    // Second access - uses cached value, no DB query!
    let author_name = comment2.author.get(&db)?.name.clone();
    let author_email = comment2.author.get(&db)?.email.clone();
    println!("Cached access: {} <{}>", author_name, author_email);

    println!("\n=== Example 3: get_with_refs() resolves everything upfront ===\n");

    // This resolves ALL references recursively
    let resolved = comments.get_with_refs(&3, &db)?.unwrap();
    println!("Comment: '{}'", resolved.text);

    // All references are already resolved, so these are instant
    println!(
        "Author: {} ({})",
        resolved.author.get(&db)?.name,
        resolved.author.get(&db)?.email
    );

    println!("Post: '{}'", resolved.post.get(&db)?.title);
    println!(
        "Post Author: {}",
        resolved.post.get(&db)?.author.get(&db)?.name
    );

    println!("\n=== Example 4: Batch retrieval ===\n");

    let ids = vec![1, 2, 3];
    let all_comments = comments.get_many_with_refs(&ids, &db)?;

    println!("All comments (batch loaded with references resolved):");
    for comment in all_comments.into_iter().flatten() {
        println!("  '{}' by {}", comment.text, comment.author.get(&db)?.name);
    }

    println!("\n=== Example 5: Mutable access ===\n");

    let comment = comments.get(&1)?.unwrap();

    // Get mutable reference - still auto-resolves!
    let mut author = comment.author.get_mut(&db)?;
    let old_name = author.name.clone();
    author.name = "Alice Smith".to_string();
    println!(
        "Changed author name from '{}' to '{}'",
        old_name, author.name
    );

    // Note: This doesn't persist to DB unless you save
    // Just demonstrating mutable access works

    println!("\n✅ All examples completed successfully!");
    println!("Key takeaways:");
    println!("  • No 'mut' needed on parent structs");
    println!("  • First .get() resolves from DB, subsequent calls use cache");
    println!("  • No more get_unchecked() - just use .get() everywhere");
    println!("  • Cleaner, more intuitive API!");

    Ok(())
}
