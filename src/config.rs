//! Configuration and builder for NGDB
//!
//! This module provides a fluent builder API for configuring and opening databases.

use crate::{db::Database, Error, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Configuration builder for opening a database
///
/// # Examples
///
/// ```rust,no_run
/// use ngdb::DatabaseConfig;
///
/// let db = DatabaseConfig::new("./my_database")
///     .create_if_missing(true)
///     .increase_parallelism(4)
///     .set_max_open_files(1000)
///     .open()
///     .expect("Failed to open database");
/// ```
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    path: PathBuf,
    create_if_missing: bool,
    error_if_exists: bool,
    max_open_files: Option<i32>,
    parallelism: Option<i32>,
    write_buffer_size: Option<usize>,
    max_write_buffer_number: Option<i32>,
    enable_statistics: bool,
    optimize_for_point_lookup: Option<u64>,
    compression_type: CompressionType,
    column_families: HashSet<String>,
}

/// Compression type for database
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum CompressionType {
    None,
    Snappy,
    Zlib,
    Bz2,
    #[default]
    Lz4,
    Lz4hc,
    Zstd,
}


impl CompressionType {
    fn to_rocksdb(&self) -> rocksdb::DBCompressionType {
        match self {
            CompressionType::None => rocksdb::DBCompressionType::None,
            CompressionType::Snappy => rocksdb::DBCompressionType::Snappy,
            CompressionType::Zlib => rocksdb::DBCompressionType::Zlib,
            CompressionType::Bz2 => rocksdb::DBCompressionType::Bz2,
            CompressionType::Lz4 => rocksdb::DBCompressionType::Lz4,
            CompressionType::Lz4hc => rocksdb::DBCompressionType::Lz4hc,
            CompressionType::Zstd => rocksdb::DBCompressionType::Zstd,
        }
    }
}

impl DatabaseConfig {
    /// Create a new database configuration with the given path
    ///
    /// # Arguments
    ///
    /// * `path` - The filesystem path where the database will be stored
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let mut column_families = HashSet::new();
        // Default column family is always required by RocksDB
        column_families.insert("default".to_string());

        Self {
            path: path.as_ref().to_path_buf(),
            create_if_missing: false,
            error_if_exists: false,
            max_open_files: None,
            parallelism: None,
            write_buffer_size: None,
            max_write_buffer_number: None,
            enable_statistics: false,
            optimize_for_point_lookup: None,
            compression_type: CompressionType::default(),
            column_families,
        }
    }

    /// Create the database if it doesn't exist (default: false)
    pub fn create_if_missing(mut self, create: bool) -> Self {
        self.create_if_missing = create;
        self
    }

    /// Return an error if the database already exists (default: false)
    pub fn error_if_exists(mut self, error: bool) -> Self {
        self.error_if_exists = error;
        self
    }

    /// Set the maximum number of open files the database can use
    ///
    /// Default: -1 (unlimited)
    pub fn set_max_open_files(mut self, max: i32) -> Self {
        self.max_open_files = Some(max);
        self
    }

    /// Set the level of parallelism for background threads
    ///
    /// This controls compaction and flush operations
    pub fn increase_parallelism(mut self, parallelism: i32) -> Self {
        self.parallelism = Some(parallelism);
        self
    }

    /// Set the write buffer size in bytes
    ///
    /// Larger values increase memory usage but may improve performance
    pub fn set_write_buffer_size(mut self, size: usize) -> Self {
        self.write_buffer_size = Some(size);
        self
    }

    /// Set the maximum number of write buffers
    pub fn set_max_write_buffer_number(mut self, num: i32) -> Self {
        self.max_write_buffer_number = Some(num);
        self
    }

    /// Enable statistics collection (useful for monitoring)
    pub fn enable_statistics(mut self, enable: bool) -> Self {
        self.enable_statistics = enable;
        self
    }

    /// Optimize for point lookup with the given block cache size
    ///
    /// This is useful when the workload is primarily Get() operations
    pub fn optimize_for_point_lookup(mut self, block_cache_size_mb: u64) -> Self {
        self.optimize_for_point_lookup = Some(block_cache_size_mb);
        self
    }

    /// Set the compression type for the database
    pub fn set_compression_type(mut self, compression: CompressionType) -> Self {
        self.compression_type = compression;
        self
    }

    /// Add a column family (collection) that should be created if it doesn't exist
    ///
    /// Use this to pre-declare collections before opening the database.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use ngdb::DatabaseConfig;
    ///
    /// let db = DatabaseConfig::new("./data")
    ///     .add_column_family("users")
    ///     .add_column_family("posts")
    ///     .add_column_family("sessions")
    ///     .open()
    ///     .expect("Failed to open database");
    /// ```
    pub fn add_column_family(mut self, name: &str) -> Self {
        self.column_families.insert(name.to_string());
        self
    }

    /// Add multiple column families at once
    pub fn add_column_families<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for name in names {
            self.column_families.insert(name.into());
        }
        self
    }

    /// Open the database with the current configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database path is invalid
    /// - The database cannot be created/opened
    /// - File system permissions are insufficient
    pub fn open(self) -> Result<Database> {
        self.open_internal()
    }

    fn open_internal(self) -> Result<Database> {
        // Validate path
        if self.path.as_os_str().is_empty() {
            return Err(Error::InvalidConfig(
                "Database path cannot be empty".to_string(),
            ));
        }

        // Build RocksDB options
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(self.create_if_missing);
        opts.set_error_if_exists(self.error_if_exists);
        opts.create_missing_column_families(true);

        if let Some(max) = self.max_open_files {
            opts.set_max_open_files(max);
        }

        if let Some(parallelism) = self.parallelism {
            opts.increase_parallelism(parallelism);
        }

        if let Some(size) = self.write_buffer_size {
            opts.set_write_buffer_size(size);
        }

        if let Some(num) = self.max_write_buffer_number {
            opts.set_max_write_buffer_number(num);
        }

        if self.enable_statistics {
            opts.enable_statistics();
        }

        if let Some(block_cache_size_mb) = self.optimize_for_point_lookup {
            opts.optimize_for_point_lookup(block_cache_size_mb);
        }

        opts.set_compression_type(self.compression_type.to_rocksdb());

        // Check if database exists
        let db_exists = self.path.exists();

        let all_cfs: HashSet<String> = if db_exists {
            // Database exists, list existing column families and merge
            let existing_cfs = rocksdb::DB::list_cf(&opts, &self.path)
                .unwrap_or_else(|_| vec!["default".to_string()]);
            let mut cfs: HashSet<String> = existing_cfs.into_iter().collect();
            cfs.extend(self.column_families.clone());
            cfs
        } else {
            // New database, use configured column families
            self.column_families.clone()
        };

        // Open the database with all column families
        let cf_descriptors: Vec<_> = all_cfs
            .iter()
            .map(|name| {
                let mut cf_opts = rocksdb::Options::default();
                cf_opts.set_compression_type(self.compression_type.to_rocksdb());
                rocksdb::ColumnFamilyDescriptor::new(name, cf_opts)
            })
            .collect();

        let db = rocksdb::DB::open_cf_descriptors(&opts, &self.path, cf_descriptors)
            .map_err(|e| Error::Database(format!("Failed to open database: {}", e)))?;

        Ok(Database::new(db))
    }
}

/// Additional options for advanced database configuration
#[derive(Debug, Clone)]
#[derive(Default)]
pub struct OpenOptions {
    /// List of peer nodes for future replication
    pub peer_nodes: Vec<String>,

    /// Enable replication mode
    pub enable_replication: bool,

    /// Node identifier for this instance
    pub node_id: Option<String>,
}


impl OpenOptions {
    /// Create new open options
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the peer nodes for replication
    ///
    /// # Arguments
    ///
    /// * `nodes` - Vector of node addresses (e.g., ["http://node1:8080", "http://node2:8080"])
    ///
    /// # Note
    ///
    /// This is for future replication support. Currently not fully implemented.
    pub fn with_peer_nodes(mut self, nodes: Vec<String>) -> Self {
        self.peer_nodes = nodes;
        self
    }

    /// Enable replication mode
    ///
    /// # Note
    ///
    /// This is for future replication support. Currently not fully implemented.
    pub fn enable_replication(mut self, enable: bool) -> Self {
        self.enable_replication = enable;
        self
    }

    /// Set the node identifier
    pub fn with_node_id(mut self, id: String) -> Self {
        self.node_id = Some(id);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = DatabaseConfig::new("/tmp/test")
            .create_if_missing(true)
            .set_max_open_files(500)
            .increase_parallelism(2);

        assert_eq!(config.path, PathBuf::from("/tmp/test"));
        assert!(config.create_if_missing);
        assert_eq!(config.max_open_files, Some(500));
        assert_eq!(config.parallelism, Some(2));
    }

    #[test]
    fn test_compression_types() {
        let config = DatabaseConfig::new("/tmp/test").set_compression_type(CompressionType::Zstd);

        assert_eq!(config.compression_type, CompressionType::Zstd);
    }

    #[test]
    fn test_open_options() {
        let opts = OpenOptions::new()
            .with_peer_nodes(vec![
                "http://node1:8080".to_string(),
                "http://node2:8080".to_string(),
            ])
            .enable_replication(true)
            .with_node_id("node-1".to_string());

        assert_eq!(opts.peer_nodes.len(), 2);
        assert!(opts.enable_replication);
        assert_eq!(opts.node_id, Some("node-1".to_string()));
    }
}
