# NGDB - High-Performance RocksDB Wrapper

[![Crates.io](https://img.shields.io/crates/v/ngdb.svg)](https://crates.io/crates/ngdb)
[![Documentation](https://docs.rs/ngdb/badge.svg)](https://docs.rs/ngdb)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

A thread-safe, synchronous RocksDB wrapper for Rust that provides a clean, type-safe API with zero async overhead. NGDB abstracts away RocksDB complexity, offering ACID transactions, automatic backups, and distributed replication infrastructure.

## Features

- **🚀 Zero Async Overhead** - Pure synchronous API leveraging RocksDB's native performance
- **🔒 ACID Transactions** - Full transaction support with commit, rollback, and isolation
- **🔐 Type-Safe Collections** - Generic key-value storage with compile-time type checking
- **📦 Column Families** - Organize multiple typed collections in a single database
- **⚡ Thread-Safe** - Safe concurrent access using RocksDB's multi-threaded column families
- **💾 Backup & Restore** - Built-in point-in-time backup and disaster recovery
- **🌐 Replication Infrastructure** - Framework for distributed replication (network layer not included)
- **🔗 Reference System** - `Ref<T>` type for efficient object relationships
- **🎯 Zero RocksDB Exposure** - Complete abstraction—users never interact with RocksDB types
- **📊 Rich Operations** - Batch writes, snapshots, multi-get, and functional iteration

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
ngdb = "1.0"
borsh = { version = "1.5", features = ["derive"] }
```

## Quick Start

```rust
use ngdb::{Database, DatabaseConfig, Storable};
use borsh::{BorshSerialize, BorshDeserialize};

#[derive(Debug, BorshSerialize, BorshDeserialize)]
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

fn main() -> Result<(), ngdb::Error> {
    // Open database with column family
    let db = DatabaseConfig::new("./data")
        .create_if_missing(true)
        .add_column_family("users")
        .open()?;

    // Get typed collection
    let users = db.collection::<User>("users")?;

    // Store data
    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    users.put(&user)?;

    // Retrieve data
    if let Some(user) = users.get(&1)? {
        println!("Found: {} <{}>", user.name, user.email);
    }

    Ok(())
}
```

## Core Concepts

### Collections

Collections provide type-safe access to column families:

```rust
let products = db.collection::<Product>("products")?;

// Single operations
products.put(&product)?;
let item = products.get(&id)?;
products.delete(&id)?;

// Batch operations
let items = products.get_many(&[1, 2, 3])?;

// Iteration
products.iter()?.for_each(|product| {
    println!("{}", product.name);
    true // continue iteration
})?;
```

### Transactions

ACID transactions ensure atomic operations:

```rust
let txn = db.transaction()?;
let accounts = txn.collection::<Account>("accounts")?;

// All operations are isolated
let mut alice = accounts.get(&1)?.unwrap();
let mut bob = accounts.get(&2)?.unwrap();

alice.balance -= 100;
bob.balance += 100;

accounts.put(&alice)?;
accounts.put(&bob)?;

// Commit atomically or rollback on error
txn.commit()?;
```

### Batch Operations

Efficient bulk writes:

```rust
let mut batch = users.batch();
for i in 0..1000 {
    batch.put(&User { id: i, name: format!("User {}", i) })?;
}
batch.commit()?;
```

### Snapshots

Point-in-time consistent reads:

```rust
let snapshot = db.snapshot()?;
let users = snapshot.collection::<User>("users")?;

// Read from snapshot while database continues to change
let user = users.get(&1)?;
```

### Backup & Restore

```rust
// Create backup
db.backup("./backups")?;

// List backups
let backups = Database::list_backups("./backups")?;

// Restore from backup
Database::restore_from_backup("./backups", "./restored")?;
```

### References

Efficiently store object relationships:

```rust
use ngdb::{Ref, Referable};

#[derive(BorshSerialize, BorshDeserialize)]
struct Post {
    id: u64,
    title: String,
    author: Ref<User>, // Only stores user ID
}

impl Referable for Post {
    fn resolve_refs(&mut self, db: &Database) -> Result<()> {
        self.author.resolve_from_db(db, "users")?;
        Ok(())
    }
}

// Retrieve with automatic reference resolution
let post = posts.get_with_refs(&1, &db)?.unwrap();
println!("Author: {}", post.author.name); // Transparent access
```

## Distributed Replication

NGDB provides the infrastructure for building distributed systems with eventual consistency. The library handles operation logging and conflict resolution, but **you must implement the network layer** (HTTP, gRPC, WebSocket, etc.) between nodes.

### Architecture

**Outbound Replication** - Capture local writes as replication logs:
```rust
// Your code creates replication logs for writes
let log = create_replication_log(&operation);

// Send to peers via your network layer
send_to_peers(log).await?;
```

**Inbound Replication** - Process logs received from peers:
```rust
// In your network handler
async fn handle_replication(log: ReplicationLog) -> Result<()> {
    manager.apply_replication(log)?;
    Ok(())
}
```

NGDB handles:
- Operation serialization
- Timestamp-based conflict resolution
- Atomic batch replication
- Data integrity verification

You implement:
- Network transport (TCP, HTTP, gRPC, etc.)
- Peer discovery and routing
- Authentication and authorization
- Network failure handling

## Examples

The repository includes comprehensive examples:

- `basic_usage.rs` - CRUD operations
- `user_struct.rs` - Working with custom structs
- `advanced.rs` - Advanced features and patterns
- `transactions.rs` - ACID transactions with rollback
- `thread_safe_transactions.rs` - Concurrent access patterns
- `backup_restore.rs` - Disaster recovery
- `replication.rs` - Basic replication setup
- `replication_full.rs` - Complete replication system
- `nested_refs.rs` - Object relationships with `Ref<T>`

Run an example:
```bash
cargo run --example basic_usage
```

## Performance

NGDB is designed for performance:
- **Synchronous operations** - No async runtime overhead
- **Zero-copy** where possible
- **RocksDB optimizations** - LZ4 compression, jemalloc (non-Windows)
- **Efficient batching** - Minimize write amplification
- **Multi-threaded** - Safe concurrent access to column families

## License

Dual-licensed under MIT or Apache-2.0.

## Acknowledgments

Built on [RocksDB](https://rocksdb.org/), a high-performance embedded key-value store originally developed at Facebook.

Serialization powered by [Borsh](https://borsh.io/), a binary serialization format optimized for speed and consistency.

## Contributing

Contributions are welcome! Please ensure:
- Code passes `cargo test`
- Code is formatted with `cargo fmt`
- Code passes `cargo clippy` with no warnings
- New features include tests and documentation
- Follow existing code style
