//! Nested references example for NGDB
//!
//! Demonstrates how to use `Ref<T>` to create relationships between objects
//! with automatic reference resolution.

use borsh::{BorshDeserialize, BorshSerialize};
use ngdb::{Database, DatabaseConfig, Ref, Referable, Result, Storable};

// ============================================================================
// Define our data models
// ============================================================================

/// A user in the system
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

// User has no references, so resolve_refs does nothing
impl Referable for User {
    fn resolve_refs(&mut self, _db: &Database) -> Result<()> {
        Ok(())
    }
}

/// A blog post that references a User as the author
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct Post {
    id: u64,
    title: String,
    content: String,
    author: Ref<User>, // Only stores the user's ID
}

impl Storable for Post {
    type Key = u64;
    fn key(&self) -> Self::Key {
        self.id
    }
}

// Post has a reference to User, so we resolve it
impl Referable for Post {
    fn resolve_refs(&mut self, db: &Database) -> Result<()> {
        // Resolve the author reference by fetching from the "users" collection
        self.author.resolve_from_db(db, "users")?;
        Ok(())
    }
}

/// A comment that references both a User and a Post
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
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

// Comment has references to both User and Post (which itself has a Ref<User>)
impl Referable for Comment {
    fn resolve_refs(&mut self, db: &Database) -> Result<()> {
        // Resolve direct references
        self.author.resolve_from_db(db, "users")?;
        self.post.resolve_from_db(db, "posts")?;

        // Also resolve nested references within Post
        if let Ok(post) = self.post.get_mut() {
            post.resolve_refs(db)?;
        }
        Ok(())
    }
}

// ============================================================================
// Example
// ============================================================================

fn main() -> Result<()> {
    println!("=== NGDB Nested References Example ===\n");

    // Setup database
    let db = DatabaseConfig::new("./data/nested_refs_example")
        .create_if_missing(true)
        .add_column_family("users")
        .add_column_family("posts")
        .add_column_family("comments")
        .open()?;

    let users = db.collection::<User>("users")?;
    let posts = db.collection::<Post>("posts")?;
    let comments = db.collection::<Comment>("comments")?;

    // Create and store a user
    let alice = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    users.put(&alice)?;
    println!("✓ Created user: {}", alice.name);

    // Create and store a post with a reference to the user
    let post = Post {
        id: 1,
        title: "Introduction to NGDB".to_string(),
        content: "NGDB is a high-performance RocksDB wrapper...".to_string(),
        author: Ref::from_value(alice.clone()),
    };
    posts.put(&post)?;
    println!("✓ Created post: '{}'", post.title);

    // Create and store a comment with references to both user and post
    let comment = Comment {
        id: 1,
        text: "Great article!".to_string(),
        author: Ref::new(1), // Reference by key
        post: Ref::new(1),   // Reference by key
    };
    comments.put(&comment)?;
    println!("✓ Created comment\n");

    // ========================================================================
    // Demonstration 1: Regular get() - references are NOT resolved
    // ========================================================================
    println!("1. Using regular get() - references contain only keys:");
    let comment_unresolved = comments.get(&1)?.unwrap();
    println!("   Comment text: '{}'", comment_unresolved.text);
    println!("   ⚠ Trying to access .author.name would PANIC!");

    // Safe access using .get() method
    match comment_unresolved.author.get() {
        Ok(author) => println!("   Author: {}", author.name),
        Err(_) => println!("   ✓ Safe access with .get() returned an error (no panic!)"),
    }
    println!();

    // ========================================================================
    // Demonstration 2: get_with_refs() - automatic resolution
    // ========================================================================
    println!("2. Using get_with_refs() - all references resolved:");
    let comment_resolved = comments.get_with_refs(&1, &db)?.unwrap();
    println!("   Comment: '{}'", comment_resolved.text);

    // Transparent access via Deref - no need for .get() or .unwrap()
    println!(
        "   Comment by: {} ({})",
        comment_resolved.author.name, comment_resolved.author.email
    );
    println!("   On post: '{}'", comment_resolved.post.title);

    // Nested references are also resolved!
    println!(
        "   Post by: {} ({})",
        comment_resolved.post.author.name, comment_resolved.post.author.email
    );
    println!("   ✓ All references (including nested) resolved automatically!\n");

    // ========================================================================
    // Demonstration 3: Batch retrieval with references
    // ========================================================================
    println!("3. Batch retrieval with get_many_with_refs():");

    // Create more comments
    comments.put(&Comment {
        id: 2,
        text: "Very helpful, thanks!".to_string(),
        author: Ref::new(1),
        post: Ref::new(1),
    })?;

    comments.put(&Comment {
        id: 3,
        text: "Looking forward to more.".to_string(),
        author: Ref::new(1),
        post: Ref::new(1),
    })?;

    let ids = vec![1, 2, 3];
    let resolved_comments = comments.get_many_with_refs(&ids, &db)?;

    for comment in resolved_comments.into_iter().flatten() {
        println!("   - '{}' by {}", comment.text, comment.author.name);
    }

    println!("\n=== Example completed successfully! ===");
    Ok(())
}
