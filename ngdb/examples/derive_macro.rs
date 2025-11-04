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

    Ok(())
}
