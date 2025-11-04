//! Reference system for nested object relationships
//!
//! This module provides the `Ref<T>` type that allows storing references to other
//! `Storable` objects. When a struct contains `Ref<T>`, only the key is stored,
//! and the referenced object is automatically fetched during retrieval.
//!
//! You can access the referenced value via `.get(&db)` which automatically resolves
//! the reference on-demand without requiring mutable access to the parent struct.
//!
//! # Key Features
//!
//! - **Space Efficient**: Only stores keys, not full objects
//! - **Immutable API**: No `&mut` needed - uses interior mutability for lazy loading
//! - **Automatic Resolution**: References are resolved on-demand when you call `.get(&db)`
//! - **Cached**: First call resolves from DB, subsequent calls use cached value
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
//! // Retrieve post - no mut needed!
//! let posts = Post::collection(&db)?;
//! let post = posts.get(&1)?.unwrap();  // Immutable!
//!
//! // Call .get(&db) to automatically resolve and access the author
//! let author = post.author.get(&db)?;  // Works without mut!
//! println!("Author: {}", author.name);
//!
//! // Multiple accesses use cached value - no DB queries
//! let name = post.author.get(&db)?.name;
//! let email = post.author.get(&db)?.email;
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
//!     fn resolve_all(&self, db: &Database) -> Result<()> {
//!         // Manually resolve individual references
//!         self.author.resolve(db, User::collection_name())?;
//!
//!         // Nested references are also resolved
//!         if let Ok(author) = self.author.get(db) {
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
//! - **Caching**: First `.get()` resolves from DB, subsequent calls use cached value
//! - **Performance**: Each reference resolution requires a database lookup (on first access)
//! - **Thread Safety**: Uses `RefCell` (not thread-safe), don't share across threads
//! - **Attribute Macro**: Use `#[ngdb("name")]` to eliminate boilerplate and auto-add derives

use crate::{Database, Error, Result, Storable};
use borsh::{BorshDeserialize, BorshSerialize};
use std::cell::{Ref as CellRef, RefCell, RefMut as CellRefMut};
use std::io::{Read, Write};

/// A reference to another `Storable` object.
///
/// `Ref<T>` stores only the key of the referenced object during serialization.
/// Uses interior mutability to enable lazy loading without requiring `&mut` access.
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
/// // Retrieve and access without mut:
/// let post = posts.get(&1)?.unwrap();  // No mut!
/// let author = post.author.get(&db)?;  // Auto-resolves on first call
/// println!("Author: {}", author.name);
///
/// // Subsequent calls use cached value
/// let email = post.author.get(&db)?.email;  // No DB query
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ref<T: Storable> {
    key: T::Key,
    value: RefCell<Option<Box<T>>>,
}

impl<T: Storable> Ref<T> {
    /// Create a new reference from a key.
    ///
    /// The referenced value will be `None` until resolved.
    pub fn new(key: T::Key) -> Self {
        Self {
            key,
            value: RefCell::new(None),
        }
    }

    /// Create a new reference from an object.
    ///
    /// This stores the key and caches the value.
    pub fn from_value(value: T) -> Self {
        let key = value.key();
        Self {
            key,
            value: RefCell::new(Some(Box::new(value))),
        }
    }

    /// Get the key of the referenced object.
    pub fn key(&self) -> &T::Key {
        &self.key
    }

    /// Check if the reference has been resolved.
    ///
    /// Returns `true` if the value is cached, `false` if it needs to be fetched.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let post = posts.get(&1)?.unwrap();
    /// assert!(!post.author.is_resolved());  // Not yet resolved
    ///
    /// let _ = post.author.get(&db)?;
    /// assert!(post.author.is_resolved());   // Now resolved
    /// ```
    pub fn is_resolved(&self) -> bool {
        self.value.borrow().is_some()
    }

    /// Get the resolved value, automatically resolving if needed.
    ///
    /// This method uses interior mutability to resolve the reference on first access.
    /// Subsequent calls return the cached value without querying the database.
    ///
    /// Returns a `Ref` smart pointer (from `std::cell`) that ensures borrowing rules.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // No mut needed!
    /// let post = posts.get(&1)?.unwrap();
    ///
    /// // First call: resolves from DB and caches
    /// let author = post.author.get(&db)?;
    /// println!("Author: {}", author.name);
    ///
    /// // Second call: uses cached value, no DB query
    /// let email = post.author.get(&db)?.email;
    ///
    /// // Multiple immutable accesses work fine
    /// let name = post.author.get(&db)?.name;
    /// let id = post.author.get(&db)?.id;
    /// ```
    pub fn get(&self, db: &Database) -> Result<CellRef<'_, T>>
    where
        T: Referable + HasCollectionName,
    {
        // Resolve if not already cached
        if self.value.borrow().is_none() {
            self.resolve(db, T::collection_name())?;
        }

        // Map the RefCell borrow to extract the inner value
        Ok(CellRef::map(self.value.borrow(), |opt| {
            opt.as_ref().unwrap().as_ref()
        }))
    }

    /// Get a mutable reference to the resolved value, automatically resolving if needed.
    ///
    /// Returns a `RefMut` smart pointer (from `std::cell`) that ensures borrowing rules.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let post = posts.get(&1)?.unwrap();
    ///
    /// // Get mutable access - auto-resolves if needed
    /// let mut author = post.author.get_mut(&db)?;
    /// author.name = "New Name".to_string();
    /// ```
    pub fn get_mut(&self, db: &Database) -> Result<CellRefMut<'_, T>>
    where
        T: Referable + HasCollectionName,
    {
        // Resolve if not already cached
        if self.value.borrow().is_none() {
            self.resolve(db, T::collection_name())?;
        }

        // Map the RefCell borrow to extract the inner value
        Ok(CellRefMut::map(self.value.borrow_mut(), |opt| {
            opt.as_mut().unwrap().as_mut()
        }))
    }

    /// Resolve this reference by fetching from the database.
    ///
    /// This is called automatically by `get()` and `get_mut()`. You typically
    /// don't need to call this directly unless you're implementing custom logic.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Manual resolution (usually not needed)
    /// post.author.resolve(&db, User::collection_name())?;
    /// assert!(post.author.is_resolved());
    /// ```
    pub fn resolve(&self, db: &Database, collection_name: &str) -> Result<()>
    where
        T: Referable,
    {
        // Check if already resolved
        if self.value.borrow().is_some() {
            return Ok(());
        }

        // Fetch from database
        let collection = db.collection::<T>(collection_name)?;
        match collection.get(&self.key)? {
            Some(value) => {
                *self.value.borrow_mut() = Some(Box::new(value));
                Ok(())
            }
            None => Err(Error::InvalidValue(format!(
                "Referenced object with key {:?} not found in collection '{}'",
                self.key, collection_name
            ))),
        }
    }

    /// Consume the Ref and return the inner value if resolved.
    ///
    /// Returns `None` if the reference hasn't been resolved yet.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let post = posts.get(&1)?.unwrap();
    /// let _ = post.author.get(&db)?;  // Resolve first
    ///
    /// let author_opt = post.author.into_inner();
    /// assert!(author_opt.is_some());
    /// ```
    pub fn into_inner(self) -> Option<T> {
        self.value.into_inner().map(|b| *b)
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
        Ok(Ref {
            key,
            value: RefCell::new(None),
        })
    }
}

/// Trait for providing collection name for a type.
///
/// This trait is automatically implemented by the `#[ngdb]` macro.
/// It allows generic code to access the collection name.
pub trait HasCollectionName {
    /// Returns the collection name for this type.
    fn collection_name() -> &'static str;
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
///     fn resolve_all(&self, _db: &Database) -> Result<()> {
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
///     fn resolve_all(&self, db: &Database) -> Result<()> {
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
    ///
    /// This method takes `&self` instead of `&mut self` because references use
    /// interior mutability (`RefCell`) for lazy loading.
    fn resolve_all(&self, db: &Database) -> Result<()>;
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
        assert!(!reference.is_resolved());
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
        assert!(reference.is_resolved());

        let inner = reference.into_inner();
        assert!(inner.is_some());
        assert_eq!(inner.unwrap(), value);
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
        // Decoded reference is not resolved (value is None)
        assert!(!decoded.is_resolved());
    }

    #[test]
    fn test_ref_is_resolved() {
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

        let unresolved = Ref::<TestType>::new(42);
        assert!(!unresolved.is_resolved());

        let value = TestType {
            id: 42,
            value: "test".to_string(),
        };
        let resolved = Ref::from_value(value);
        assert!(resolved.is_resolved());
    }
}
