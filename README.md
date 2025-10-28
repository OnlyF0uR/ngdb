# NGDB - Next Generation Database

A RocksDB wrapper for Rust with ACID transactions, automatic backups, and distributed replication.

## Features

- 🔒 **ACID Transactions** - Atomic batch operations with rollback support
- 💾 **Backup & Restore** - Built-in backup functionality compatible with replication
- 🌐 **Distributed Replication** - Replication with conflict resolution
- 🎯 **Type-Safe** - Generic over key/value types with trait-based serialization
- 🔐 **Full RocksDB Abstraction** - RocksDB usage is abstracted away, users never deal with RocksDB types directly
- ⚡ **Thread-Safe** - Built for safe concurrent access
- 📦 **Column Families** - Store multiple types in one database using collections
- 🧪 **Tested** - Extensive, ever-expanding, test suite

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
