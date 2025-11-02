//! Nested references example for NGDB
//!
//! Demonstrates how to use `Ref<T>` to create relationships between objects
//! with automatic reference resolution.

use borsh::{BorshDeserialize, BorshSerialize};
use ngdb::{Database, DatabaseConfig, Ref, Referable, Result, Storable};

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

impl Referable for User {
    fn resolve_refs(&mut self, _db: &Database) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
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

impl Referable for Post {
    fn resolve_refs(&mut self, db: &Database) -> Result<()> {
        self.author.resolve_from_db(db, "users")?;
        Ok(())
    }
}

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

impl Referable for Comment {
    fn resolve_refs(&mut self, db: &Database) -> Result<()> {
        self.author.resolve_from_db(db, "users")?;
        self.post.resolve_from_db(db, "posts")?;

        if let Ok(post) = self.post.get_mut() {
            post.resolve_refs(db)?;
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let db = DatabaseConfig::new("./data/nested_refs_example")
        .create_if_missing(true)
        .add_column_family("users")
        .add_column_family("posts")
        .add_column_family("comments")
        .open()?;

    let users = db.collection::<User>("users")?;
    let posts = db.collection::<Post>("posts")?;
    let comments = db.collection::<Comment>("comments")?;

    let alice = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    users.put(&alice)?;
    println!("Created user: {}", alice.name);

    let post = Post {
        id: 1,
        title: "Introduction to NGDB".to_string(),
        content: "NGDB is a high-performance RocksDB wrapper...".to_string(),
        author: Ref::from_value(alice.clone()),
    };
    posts.put(&post)?;
    println!("Created post: '{}'", post.title);

    let comment = Comment {
        id: 1,
        text: "Great article!".to_string(),
        author: Ref::new(1),
        post: Ref::new(1),
    };
    comments.put(&comment)?;
    println!("Created comment\n");

    // Regular get() - references are NOT resolved
    let comment_unresolved = comments.get(&1)?.unwrap();
    println!("Unresolved comment text: '{}'", comment_unresolved.text);

    match comment_unresolved.author.get() {
        Ok(author) => println!("Author: {}", author.name),
        Err(_) => println!("Author reference not resolved (expected)"),
    }
    println!();

    // get_with_refs() - automatic resolution
    let comment_resolved = comments.get_with_refs(&1, &db)?.unwrap();
    println!("Resolved comment: '{}'", comment_resolved.text);
    println!(
        "Comment by: {} ({})",
        comment_resolved.author.name, comment_resolved.author.email
    );
    println!("On post: '{}'", comment_resolved.post.title);
    println!(
        "Post by: {} ({})",
        comment_resolved.post.author.name, comment_resolved.post.author.email
    );
    println!();

    // Batch retrieval with references
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
    println!(
        "Retrieved {} comments with resolved references:",
        resolved_comments.len()
    );

    for comment in resolved_comments.into_iter().flatten() {
        println!("  '{}' by {}", comment.text, comment.author.name);
    }

    Ok(())
}
