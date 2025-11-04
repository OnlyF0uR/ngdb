//! Nested references example for NGDB
//!
//! Demonstrates relationships between objects with automatic reference resolution.

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

    let alice = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    alice.save(&db)?;

    let post = Post {
        id: 1,
        title: "Introduction to NGDB".to_string(),
        content: "NGDB is a high-performance RocksDB wrapper...".to_string(),
        author: Ref::from_value(alice.clone()),
    };
    post.save(&db)?;

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

    let comments = Comment::collection(&db)?;

    // Example 1: Using get() - references are NOT resolved automatically
    // You must call .get(&db) on each Ref field to resolve it
    let mut comment1 = comments.get(&1)?.unwrap();
    println!("Comment: '{}'", comment1.text);

    // Call .get(&db) to automatically resolve the author reference
    let author = comment1.author.get(&db)?;
    println!("Author: {} ({})", author.name, author.email);

    // Call .get_mut(&db) to automatically resolve the post reference
    let post = comment1.post.get_mut(&db)?;
    println!("Post: '{}'", post.title);

    // Nested references are also auto-resolved
    let post_author = post.author.get(&db)?;
    println!("Post Author: {}", post_author.name);

    // Example 2: Using get_with_refs() - ALL references are resolved automatically
    // You can use .get_unchecked() to access already-resolved references
    let resolved = comments.get_with_refs(&1, &db)?.unwrap();
    println!("Comment: '{}'", resolved.text);
    println!(
        "Author: {} ({})",
        resolved.author.get_unchecked()?.name,
        resolved.author.get_unchecked()?.email
    );
    println!("Post: '{}'", resolved.post.get_unchecked()?.title);
    println!(
        "Post Author: {}",
        resolved.post.get_unchecked()?.author.get_unchecked()?.name
    );

    // Batch retrieval with get_many_with_refs()
    let ids = vec![1, 2, 3];
    let all_comments = comments.get_many_with_refs(&ids, &db)?;

    println!("\nAll comments (with get_many_with_refs):");
    for comment in all_comments.into_iter().flatten() {
        println!(
            "'{}' by {}",
            comment.text,
            comment.author.get_unchecked()?.name
        );
    }

    Ok(())
}
