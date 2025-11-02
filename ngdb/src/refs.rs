//! Reference system for nested object relationships
//!
//! This module provides the `Ref<T>` type that allows storing references to other
//! `Storable` objects. When a struct contains `Ref<T>`, only the key is stored,
//! and the referenced object is automatically fetched during retrieval.
//!
//! After resolution, you can access the referenced value via `.get()`:
//! `post.author.get()?.name` for safe access with error propagation
//!
//! # Key Features
//!
//! - **Space Efficient**: Only stores keys, not full objects
//! - **Safe Access**: Use `.get()` to access resolved references with error handling
//! - **Automatic Resolution**: Call `get_with_refs()` to fetch and populate all references
//! - **Type Safe**: Compile-time verification of reference relationships
//! - **Nested Support**: References can contain other references
//! - **Attribute Macro**: Use `#[ngdb("name")]` to automatically generate all boilerplate
//!
//! # Quick Start with NGDB Attribute (Recommended)
//!
//! The easiest way to use references is with the `#[ngdb("name")]` attribute macro:
//!
//! ```rust,ignore
//! use ngdb::{Storable, Ref, Database, Result};
//!
//! // Define a User type
//! #[ngdb("users")]
//! struct User {
//!     id: u64,
//!     name: String,
//! }
//!
//! impl Storable for User {
//!     type Key = u64;
//!     fn key(&self) -> Self::Key { self.id }
//! }
//!
//! // Define a Post that references a User
//! #[ngdb("posts")]
//! struct Post {
//!     id: u64,
//!     title: String,
//!     author: Ref<User>,  // Only stores user_id internally
//! }
//!
//! impl Storable for Post {
//!     type Key = u64;
//!     fn key(&self) -> Self::Key { self.id }
//! }
//!
//! // The #[ngdb] attribute automatically:
//! // - Adds derives: BorshSerialize, BorshDeserialize, Clone, Debug
//! // - Generates Post::collection_name() -> "posts"
//! // - Generates Post::collection(&db) -> Collection<Post>
//! // - Generates Post::save(&self, db) -> Result<()>
//! // - Implements Referable that resolves all Ref<T> fields
//!
//! // Store objects (no need to manually get collections!)
//! let user = User { id: 1, name: "Alice".to_string() };
//! user.save(&db)?;
//!
//! let post = Post {
//!     id: 1,
//!     title: "Hello World".to_string(),
//!     author: Ref::from_value(user),
//! };
//! post.save(&db)?;
//!
//! // Retrieve with automatic reference resolution
//! let posts = Post::collection(&db)?;
//! let post = posts.get_with_refs(&1, &db)?.unwrap();
//! println!("Author: {}", post.author.get()?.name);
//! ```
//!
//! # Nested References
//!
//! References can be nested - a referenced object can itself contain references.
//! The attribute macro automatically handles this:
//!
//! ```rust,ignore
//! #[ngdb("comments")]
//! struct Comment {
//!     id: u64,
//!     text: String,
//!     author: Ref<User>,
//!     post: Ref<Post>,  // Post also has a Ref<User>
//! }
//!
//! // The #[ngdb] attribute generates resolve_all() that:
//! // 1. Resolves self.author from "users" collection
//! // 2. Resolves self.post from "posts" collection
//! // 3. Calls post.resolve_all() to resolve nested references
//! ```
//!
//! # Manual Implementation (Advanced)
//!
//! If you need more control, you can manually implement `Referable`:
//!
//! ```rust,ignore
//! impl Referable for Post {
//!     fn resolve_all(&mut self, db: &Database) -> Result<()> {
//!         // Manually resolve individual references
//!         self.author.resolve(db, User::collection_name())?;
//!
//!         // Optional: resolve nested references
//!         if let Ok(author) = self.author.get_mut() {
//!             author.resolve_all(db)?;
//!         }
//!         Ok(())
//!     }
//! }
//! ```
//!
//! # Important Notes
//!
//! - **Circular references are NOT supported**: Don't create cycles (A -> B -> A)
//! - **Serialization**: Only the key is serialized, not the full object
//! - **Error Handling**: Always use `.get()` or `.get_mut()` to access resolved references safely
//! - **Performance**: Each reference resolution requires a database lookup
//! - **Attribute Macro**: Use `#[ngdb("name")]` to eliminate boilerplate and auto-add derives

use crate::{Database, Error, Result, Storable};
use borsh::{BorshDeserialize, BorshSerialize};
use std::io::{Read, Write};

/// A reference to another `Storable` object.
///
/// `Ref<T>` stores only the key of the referenced object during serialization.
/// After calling `get_with_refs()`, the referenced object is automatically fetched
/// and you can access it safely via `.get()` or `.get_mut()`.
///
/// # Examples
///
/// ```rust,ignore
/// use ngdb::{Storable, Ref, Referable};
/// use borsh::{BorshSerialize, BorshDeserialize};
///
/// #[derive(Debug, BorshSerialize, BorshDeserialize)]
/// struct Post {
///     id: u64,
///     title: String,
///     author: Ref<User>,
/// }
///
/// // After get_with_refs():
/// let post = posts.get_with_refs(&1, &db)?.unwrap();
/// println!("Author: {}", post.author.get()?.name);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ref<T: Storable> {
    key: T::Key,
    value: Option<Box<T>>,
}

impl<T: Storable> Ref<T> {
    /// Create a new reference from a key.
    ///
    /// The referenced value will be `None` until resolved.
    pub fn new(key: T::Key) -> Self {
        Self { key, value: None }
    }

    /// Create a new reference from an object.
    ///
    /// This stores the key and caches the value.
    pub fn from_value(value: T) -> Self {
        let key = value.key();
        Self {
            key,
            value: Some(Box::new(value)),
        }
    }

    /// Get the key of the referenced object.
    pub fn key(&self) -> &T::Key {
        &self.key
    }

    /// Get the resolved value.
    ///
    /// Returns an error if the reference hasn't been resolved yet.
    /// Use this for safe access without panicking.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Safe access with error handling
    /// match comment.author.get() {
    ///     Ok(author) => println!("Author: {}", author.name),
    ///     Err(_) => println!("Author not loaded"),
    /// }
    ///
    /// // Or with ?
    /// let author = comment.author.get()?;
    /// println!("Author: {}", author.name);
    /// ```
    pub fn get(&self) -> Result<&T> {
        self.value.as_ref().map(|b| b.as_ref()).ok_or_else(|| {
            Error::InvalidValue(
                "Reference not resolved. Use get_with_refs() to resolve references.".to_string(),
            )
        })
    }

    /// Get a mutable reference to the resolved value.
    ///
    /// Returns an error if the reference hasn't been resolved yet.
    pub fn get_mut(&mut self) -> Result<&mut T> {
        self.value.as_mut().map(|b| b.as_mut()).ok_or_else(|| {
            Error::InvalidValue(
                "Reference not resolved. Use get_with_refs() to resolve references.".to_string(),
            )
        })
    }

    /// Resolve this reference by fetching from the database.
    ///
    /// This is used during automatic resolution.
    pub fn resolve(&mut self, db: &Database, collection_name: &str) -> Result<()>
    where
        T: Referable,
    {
        if self.value.is_some() {
            return Ok(());
        }

        let collection = db.collection::<T>(collection_name)?;
        match collection.get(&self.key)? {
            Some(value) => {
                self.value = Some(Box::new(value));
                Ok(())
            }
            None => Err(Error::InvalidValue(format!(
                "Referenced object with key {:?} not found in collection '{}'",
                self.key, collection_name
            ))),
        }
    }

    /// Consume the Ref and return the inner value if resolved.
    pub fn into_inner(self) -> Option<T> {
        self.value.map(|b| *b)
    }
}

// Custom BorshSerialize implementation - only serialize the key
impl<T: Storable> BorshSerialize for Ref<T> {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.key.serialize(writer)
    }
}

// Custom BorshDeserialize implementation - only deserialize the key
impl<T: Storable> BorshDeserialize for Ref<T> {
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        // Deserialize only the key, value remains None
        let key = T::Key::deserialize_reader(reader)?;
        Ok(Ref { key, value: None })
    }
}

/// Trait for types that can be referenced by `Ref<T>` and contain references themselves.
///
/// Implementing this trait allows a type to:
/// 1. Be used in `Ref<T>` fields
/// 2. Have its references automatically resolved when retrieved with `get_with_refs()`
///
/// # Examples
///
/// ```rust,ignore
/// use ngdb::{Storable, Referable, Ref, Database, Result};
/// use borsh::{BorshSerialize, BorshDeserialize};
///
/// #[derive(BorshSerialize, BorshDeserialize)]
/// struct User {
///     id: u64,
///     name: String,
/// }
///
/// impl Storable for User {
///     type Key = u64;
///     fn key(&self) -> Self::Key { self.id }
/// }
///
/// // User has no references, so resolve_all does nothing
/// impl Referable for User {
///     fn resolve_all(&mut self, _db: &Database) -> Result<()> {
///         Ok(())
///     }
/// }
///
/// #[derive(BorshSerialize, BorshDeserialize)]
/// struct Post {
///     id: u64,
///     title: String,
///     author: Ref<User>,
/// }
///
/// impl Storable for Post {
///     type Key = u64;
///     fn key(&self) -> Self::Key { self.id }
/// }
///
/// // Post has a reference to User, so we resolve it
/// impl Referable for Post {
///     fn resolve_all(&mut self, db: &Database) -> Result<()> {
///         self.author.resolve(db, "users")?;
///         Ok(())
///     }
/// }
/// ```
pub trait Referable: Storable {
    /// Resolve all references in this object.
    ///
    /// For types without references, this should just return `Ok(())`.
    /// For types with `Ref<T>` fields, call `resolve()` on each field.
    /// For nested references, also call `resolve_all()` on the resolved objects.
    fn resolve_all(&mut self, db: &Database) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsh::{BorshDeserialize, BorshSerialize};

    #[test]
    fn test_ref_creation() {
        #[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
        struct TestType {
            id: u64,
            value: String,
        }

        impl Storable for TestType {
            type Key = u64;
            fn key(&self) -> Self::Key {
                self.id
            }
        }

        let key = 42u64;
        let reference = Ref::<TestType>::new(key);
        assert_eq!(reference.key(), &42);
        // Reference is not resolved, so .get() should return an error
        assert!(reference.get().is_err());
    }

    #[test]
    fn test_ref_from_value() {
        #[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
        struct TestType {
            id: u64,
            value: String,
        }

        impl Storable for TestType {
            type Key = u64;
            fn key(&self) -> Self::Key {
                self.id
            }
        }

        let value = TestType {
            id: 42,
            value: "test".to_string(),
        };
        let reference = Ref::from_value(value.clone());
        assert_eq!(reference.key(), &42);
        assert_eq!(reference.get().ok(), Some(&value));
    }

    #[test]
    fn test_ref_access_via_get() {
        #[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
        struct TestType {
            id: u64,
            value: String,
        }

        impl Storable for TestType {
            type Key = u64;
            fn key(&self) -> Self::Key {
                self.id
            }
        }

        let value = TestType {
            id: 42,
            value: "test".to_string(),
        };
        let reference = Ref::from_value(value.clone());

        // Test safe access via get()
        let accessed = reference.get().unwrap();
        assert_eq!(accessed.value, "test");
        assert_eq!(accessed.id, 42);
    }

    #[test]
    fn test_ref_encode_decode() {
        #[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
        struct TestType {
            id: u64,
            value: String,
        }

        impl Storable for TestType {
            type Key = u64;
            fn key(&self) -> Self::Key {
                self.id
            }
        }

        let reference = Ref::<TestType>::new(42);
        let encoded = borsh::to_vec(&reference).unwrap();
        let decoded: Ref<TestType> = borsh::from_slice(&encoded).unwrap();

        assert_eq!(reference.key(), decoded.key());
        // Decoded reference is not resolved, so .get() should return an error
        assert!(decoded.get().is_err());
    }

    #[test]
    fn test_ref_get_returns_error_when_unresolved() {
        #[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
        struct TestType {
            id: u64,
            value: String,
        }

        impl Storable for TestType {
            type Key = u64;
            fn key(&self) -> Self::Key {
                self.id
            }
        }

        let reference = Ref::<TestType>::new(42);
        assert!(reference.get().is_err()); // Should return error, not panic
    }
}
