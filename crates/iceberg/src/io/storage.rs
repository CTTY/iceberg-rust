// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Storage interfaces of Iceberg
//!
//! This module defines the core storage traits that enable pluggable storage backends.
//! The actual storage implementations are provided by separate crates:
//!
//! - `iceberg-storage-opendal`: OpenDAL-based storage (S3, GCS, Azure, filesystem)
//! - `iceberg` crate: `MemoryStorage` for testing (always available)
//!
//! # Architecture
//!
//! The storage system uses a factory pattern:
//!
//! - [`Storage`]: Trait defining storage operations (read, write, delete, etc.)
//! - [`StorageFactory`]: Trait for creating `Storage` instances from configuration
//! - [`StorageConfig`]: Configuration struct containing scheme and properties
//!
//! # Example
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use iceberg::io::{FileIO, StorageConfig, StorageFactory, MemoryStorageFactory};
//!
//! // Use memory storage for testing
//! let file_io = FileIO::new_with_memory();
//!
//! // Or use a custom storage factory
//! let factory: Arc<dyn StorageFactory> = Arc::new(MemoryStorageFactory);
//! let config = StorageConfig::new("memory", Default::default());
//! let storage = factory.build(&config)?;
//! ```

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use super::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile, StorageConfig};
use crate::Result;

/// Trait for storage operations in Iceberg.
///
/// The trait supports serialization via `typetag`, allowing storage instances to be
/// serialized and deserialized across process boundaries.
///
/// Third-party implementations can implement this trait to provide custom storage backends.
///
/// # Implementing Custom Storage
///
/// To implement a custom storage backend:
///
/// 1. Create a struct that implements this trait
/// 2. Add `#[typetag::serde]` attribute for serialization support
/// 3. Implement all required methods
///
/// # Example
///
/// ```rust,ignore
/// use async_trait::async_trait;
/// use bytes::Bytes;
/// use iceberg::io::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage};
/// use iceberg::Result;
///
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// struct MyStorage {
///     // custom fields
/// }
///
/// #[async_trait]
/// #[typetag::serde]
/// impl Storage for MyStorage {
///     async fn exists(&self, path: &str) -> Result<bool> {
///         // implementation
///         todo!()
///     }
///     // ... implement other methods
/// }
/// ```
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Storage: Debug + Send + Sync {
    /// Check if a file exists at the given path
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Get metadata from an input path
    async fn metadata(&self, path: &str) -> Result<FileMetadata>;

    /// Read bytes from a path
    async fn read(&self, path: &str) -> Result<Bytes>;

    /// Get FileRead from a path
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>>;

    /// Write bytes to an output path
    async fn write(&self, path: &str, bs: Bytes) -> Result<()>;

    /// Get FileWrite from a path
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>>;

    /// Delete a file at the given path
    async fn delete(&self, path: &str) -> Result<()>;

    /// Delete all files with the given prefix
    async fn delete_prefix(&self, path: &str) -> Result<()>;

    /// Create a new input file for reading
    fn new_input(&self, path: &str) -> Result<InputFile>;

    /// Create a new output file for writing
    fn new_output(&self, path: &str) -> Result<OutputFile>;
}

/// Factory for creating Storage instances from configuration.
///
/// Implement this trait to provide custom storage backends. The factory pattern
/// allows for lazy initialization of storage instances and enables users to
/// inject custom storage implementations into catalogs.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use iceberg::io::{StorageConfig, StorageFactory, Storage};
/// use iceberg::Result;
///
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// struct MyCustomStorageFactory {
///     // custom configuration
/// }
///
/// #[typetag::serde]
/// impl StorageFactory for MyCustomStorageFactory {
///     fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
///         // Create and return custom storage implementation
///         todo!()
///     }
/// }
/// ```
#[typetag::serde(tag = "type")]
pub trait StorageFactory: Debug + Send + Sync {
    /// Build a new Storage instance from the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration containing scheme and properties
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Arc<dyn Storage>` on success, or an error
    /// if the storage could not be created.
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>>;
}
