//! Example demonstrating the NGDB attribute macro
//!
//! Shows how #[ngdb] eliminates boilerplate with automatic derives,
//! collection methods, and reference resolution.

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
    let db = DatabaseConfig::new("./data/derive_example")
        .create_if_missing(true)
        .add_column_family("users")
        .add_column_family("posts")
        .add_column_family("comments")
        .open()?;

    let user = User {
        id: 1,
        name: "Bob".to_string(),
        email: "bob@example.com".to_string(),
    };
    user.save(&db)?;

    let post = Post {
        id: 1,
        title: "Hello NGDB!".to_string(),
        content: "Learning about the macro...".to_string(),
        author: Ref::from_value(user.clone()),
    };
    post.save(&db)?;

    let comment = Comment {
        id: 1,
        text: "Great post!".to_string(),
        author: Ref::new(1),
        post: Ref::new(1),
    };
    comment.save(&db)?;

    let comments = Comment::collection(&db)?;

    // Example 1: Manual reference resolution (no mut needed!)
    // Notice: No mut needed! The new API uses interior mutability
    let comment1 = comments.get(&1)?.unwrap();
    println!("Comment: '{}'", comment1.text);

    // Call .get(&db) to automatically resolve and access the author
    let author = comment1.author.get(&db)?;
    println!("Author: {} ({})", author.name, author.email);
    drop(author); // Drop the borrow

    // Access the post reference - also auto-resolves
    let post = comment1.post.get(&db)?;
    println!("Post: '{}'", post.title);

    // Nested references work seamlessly
    let post_author = post.author.get(&db)?;
    println!("Post Author: {}", post_author.name);

    // Example 2: Using get_with_refs() - ALL references are resolved upfront
    let resolved = comments.get_with_refs(&1, &db)?.unwrap();
    println!("\nWith get_with_refs():");
    println!("Comment: '{}'", resolved.text);

    // All references are already resolved, just use .get(&db) as usual
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

    Ok(())
}
