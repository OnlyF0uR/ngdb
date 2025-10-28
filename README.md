# NGDB - Production-Grade Async RocksDB Wrapper

A RocksDB wrapper for Rust with first-class async support, ACID transactions, automatic backups, and distributed replication.

## Features

- 🔒 **ACID Transactions** - Atomic batch operations with rollback support
- 💾 **Backup & Restore** - Built-in backup functionality compatible with replication
- 🌐 **Distributed Replication** - Replication with conflict resolution
- 🎯 **Type-Safe** - Generic over key/value types with trait-based serialization
- 🔐 **Full RocksDB Abstraction** - RocksDB usage is abstracted away, users never deal with RocksDB types directly
- ⚡ **Thread-Safe** - Built for safe concurrent access
- 📦 **Column Families** - Store multiple types in one database using collections
- 🧪 **Tested** - Extensive, ever-expanding, test suite

## Quick Start

```toml
[dependencies]
ngdb = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

```rust
use ngdb::{Database, DatabaseConfig, Storable};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
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

#[tokio::main]
async fn main() -> Result<(), ngdb::Error> {
    // Open database with column families
    let db = DatabaseConfig::new("./data")
        .create_if_missing(true)
        .add_column_family("users")
        .open()?;

    // Get a typed collection
    let users = db.collection::<User>("users")?;

    // Write data (async, non-blocking)
    users.put(&User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    }).await?;

    // Read data (sync, fast)
    if let Some(user) = users.get(&1)? {
        println!("Found user: {}", user.name);
    }

    Ok(())
}
```

## Core Concepts

### Collections

Collections are typed views into column families. Each collection stores one type:

```rust
let db = DatabaseConfig::new("./data")
    .add_column_family("users")
    .add_column_family("posts")
    .open()?;

let users = db.collection::<User>("users")?;
let posts = db.collection::<Post>("posts")?;
```

### Storable Trait

Implement `Storable` to make your types database-ready:

```rust
impl Storable for User {
    type Key = u64;

    fn key(&self) -> Self::Key {
        self.id
    }

    // Optional: validation before storing
    // This will be called everytime before the put object
    // to ensure data integrity
    fn validate(&self) -> Result<()> {
        if self.email.is_empty() {
            return Err(Error::InvalidData("Email required".into()));
        }
        Ok(())
    }

    // Optional: hooks
    fn on_stored(&self) {
        println!("Stored user {}", self.id);
    }
}
```

### Supported Key Types

NGDB supports various key types out of the box:

```rust
// Primitive types
type Key = u64;      // or u32, i64, etc.
type Key = String;
type Key = Vec<u8>;

// Tuples for composite keys
type Key = (u64, String);
type Key = (u32, u32, String);
```

## Transactions

ACID-compliant atomic operations using WriteBatch:

```rust
// Transfer money atomically
let txn = db.transaction();
let accounts = txn.collection::<Account>("accounts")?;

let mut alice = accounts.get(&1)?.unwrap();
let mut bob = accounts.get(&2)?.unwrap();

alice.balance -= 100;
bob.balance += 100;

accounts.put(&alice)?;
accounts.put(&bob)?;

// Commit atomically - either both succeed or both fail
txn.commit().await?;
```

### Transaction Features

- ✅ **Atomicity** - All operations succeed or all fail
- ✅ **Consistency** - Validation ensures valid state
- ✅ **Durability** - Committed changes persist
- ✅ **Multiple Collections** - Cross-collection transactions
- ✅ **Error Handling** - Automatic rollback on failure

See `examples/transactions.rs` for comprehensive examples.

## Backup & Restore

Built-in backup functionality for disaster recovery:

```rust
// Create a consistent backup
db.backup("./backups/backup-2024-01-01").await?;

// List available backups
let backups = Database::list_backups("./backups").await?;
for backup in backups {
    println!("Backup {}: {} bytes", backup.backup_id, backup.size);
}

// Restore from backup
Database::restore_from_backup(
    "./backups/backup-2024-01-01",
    "./data-restored",
    None,
).await?;
```

### Backup Features

- ✅ **Consistent Snapshots** - Safe to create during active writes
- ✅ **Replication Compatible** - Restored nodes can continue receiving replicated writes
- ✅ **Multiple Versions** - Keep multiple backup versions
- ✅ **Hot Backups** - No downtime required

See `examples/backup_restore.rs` for detailed examples.

## Distributed Replication

Production-grade replication for distributed systems:

```rust
// Setup replication
let replication_config = ReplicationConfig::new("node-1".to_string())
    .enable(true)
    .with_peers(vec!["http://node2:8080".to_string()])
    .conflict_resolution(ConflictResolution::LastWriteWins);

let mut replication_manager = ReplicationManager::new(
    replication_config,
    db.clone(),
)?;

// Register hooks for monitoring
replication_manager.register_hook(MyReplicationHook);

// Outbound: Create replication log when writing locally
let log = replication_manager
    .create_put_log("users".to_string(), key_bytes, value_bytes)
    .with_checksum();

// Send log to peers via your transport (HTTP, gRPC, etc.)
send_to_peers(log).await?;

// Inbound: Apply replication from peers
replication_manager.apply_replication(received_log).await?;
```

### Replication Features

- ✅ **Conflict Resolution** - LastWriteWins, FirstWriteWins, or Custom
- ✅ **Idempotency** - Duplicate operations automatically detected and skipped
- ✅ **Checksums** - Optional data integrity verification
- ✅ **Hooks** - Custom validation and monitoring
- ✅ **Batch Operations** - Replicate multiple operations atomically
- ✅ **Statistics** - Track applied operations and replication lag

### Conflict Resolution Strategies

```rust
// Last Write Wins (based on timestamp)
ConflictResolution::LastWriteWins

// First Write Wins (reject later writes)
ConflictResolution::FirstWriteWins

// Custom (implement your own logic)
ConflictResolution::Custom
```

### Replication Hooks

Implement custom logic for replication events:

```rust
struct MyHook;

impl ReplicationHook for MyHook {
    fn before_apply(&self, log: &ReplicationLog) -> Result<()> {
        // Validate incoming replication
        println!("Applying op: {}", log.operation_id);
        Ok(())
    }

    fn after_apply(&self, log: &ReplicationLog) -> Result<()> {
        // Post-processing, metrics, etc.
        Ok(())
    }

    fn on_replication_error(&self, log: &ReplicationLog, error: &Error) {
        // Handle errors, alert, retry, etc.
        eprintln!("Replication failed: {}", error);
    }

    fn resolve_conflict(&self, incoming: &ReplicationLog, _existing_ts: u64) -> bool {
        // Custom conflict resolution
        true // Accept or reject
    }
}
```

See `examples/replication_full.rs` for a complete production example.

## Advanced Features

### Batch Operations

Atomic batch writes:

```rust
let mut batch = collection.batch();

for user in users {
    batch.put(&user)?;
}

batch.commit().await?;  // All or nothing
```

### Iterators

Efficient iteration over collections:

```rust
// Collect all items (use carefully on large collections)
let all_users: Vec<User> = collection.iter().collect_all()?;

// Iterate with callback (memory efficient)
collection.iter().for_each(|user| {
    println!("{}", user.name);
    true  // Continue iteration
})?;

// Count items
let count = collection.iter().count()?;

// Iterate from a specific key
let recent = collection.iter_from(&last_id).collect_all()?;
```

### Snapshots

Point-in-time consistent reads:

```rust
let snapshot = collection.snapshot();

// Read from snapshot (sees consistent state)
let user = snapshot.get(&1)?;
let exists = snapshot.exists(&2)?;
```

### Database Configuration

```rust
let db = DatabaseConfig::new("./data")
    .create_if_missing(true)
    .compression(CompressionType::Lz4)
    .increase_parallelism(4)
    .optimize_for_point_lookup(1024)
    .set_max_open_files(1000)
    .add_column_family("users")
    .add_column_family("posts")
    .open()?;
```

## Production Deployment

### Best Practices

1. **Error Handling** - All operations return `Result`, no panics in production
2. **Backups** - Schedule regular backups to separate storage
3. **Monitoring** - Use replication hooks to track metrics
4. **Testing** - Test failure scenarios (network partitions, node crashes)
5. **Capacity Planning** - Monitor disk usage and backup sizes
6. **Replication Lag** - Track lag between nodes

### Performance Tips

1. **Batch Writes** - Use transactions/batches for multiple operations
2. **Column Families** - Separate different data types
3. **Compression** - Enable compression for large values
4. **Bloom Filters** - Enable for faster lookups
5. **Parallelism** - Tune parallelism based on CPU cores

### Safety Guarantees

NGDB is production-safe:

- ✅ **No `.expect()` calls** - All errors properly handled
- ✅ **Thread-Safe** - All types implement `Send + Sync` with safety justifications
- ✅ **Memory-Safe** - No unsafe code except where justified with comments
- ✅ **Panic-Free** - Graceful error handling throughout

### Replication Network Layer

NGDB provides data structures and application logic. You implement the transport:

```rust
// Example HTTP endpoint for receiving replication
async fn receive_replication(log: ReplicationLog) -> Result<()> {
    replication_manager.apply_replication(log).await?;
    Ok(())
}

// Example: sending to peers
async fn send_to_peers(log: ReplicationLog) -> Result<()> {
    for peer in peers {
        reqwest::post(format!("{}/replicate", peer))
            .json(&log)
            .send()
            .await?;
    }
    Ok(())
}
```

## Examples

Run examples to see NGDB in action:

```bash
# Basic usage
cargo run --example basic_usage

# User struct example
cargo run --example user_struct

# Advanced features
cargo run --example advanced

# Transactions (NEW!)
cargo run --example transactions

# Backup and restore (NEW!)
cargo run --example backup_restore

# Production replication (NEW!)
cargo run --example replication_full
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Application                     │
└─────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│              NGDB Public API                     │
│  Database, Collection<T>, Transaction, Backup   │
└─────────────────────────────────────────────────┘
                      │
         ┌────────────┼────────────┐
         ▼            ▼            ▼
┌──────────────┐ ┌──────────┐ ┌────────────────┐
│ Transactions │ │  Backup  │ │  Replication   │
│  (WriteBatch)│ │  Engine  │ │   Manager      │
└──────────────┘ └──────────┘ └────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │   RocksDB (Internal)   │
         │   Never Exposed        │
         └────────────────────────┘
```

## Error Handling

All operations return `Result<T, Error>`:

```rust
match collection.get(&1) {
    Ok(Some(user)) => println!("Found: {}", user.name),
    Ok(None) => println!("Not found"),
    Err(e) => eprintln!("Error: {}", e),
}

// Or use `?` operator
let user = collection.get(&1)?;
```

Error types:
- `Database` - RocksDB errors
- `Serialization`/`Deserialization` - Encoding errors
- `Replication` - Replication-specific errors
- `InvalidData` - Validation failures
- `Io` - I/O errors
- And more...

## Testing

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_transactions
```

## License

Dual license MIT/Apache-2.0.

## Acknowledgments

Built on top of [RocksDB](https://rocksdb.org/).

---

**Note**: This library provides the core database and replication logic. You are responsible for implementing the network layer (HTTP, gRPC, etc.) between nodes.
