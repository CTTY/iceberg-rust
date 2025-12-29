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

//! File IO implementation.
//!
//! # Overview
//!
//! This module provides the core file I/O abstractions for Apache Iceberg. The storage
//! system is designed to be pluggable, allowing different storage backends to be used
//! without changing application code.
//!
//! # Storage Architecture
//!
//! The storage system consists of:
//!
//! - [`Storage`]: Trait defining storage operations (read, write, delete, etc.)
//! - [`StorageFactory`]: Trait for creating `Storage` instances from configuration
//! - [`StorageConfig`]: Configuration struct containing scheme and properties
//! - [`FileIO`]: Main entry point for file operations
//!
//! # Built-in Storage
//!
//! The `iceberg` crate includes `MemoryStorage` for testing purposes. For production
//! use with cloud storage (S3, GCS, Azure) or local filesystem, use the
//! `iceberg-storage-opendal` crate.
//!
//! # How to build `FileIO`
//!
//! ## Using Memory Storage (for testing)
//!
//! ```rust
//! use iceberg::io::FileIO;
//!
//! // Create a memory-backed FileIO for testing
//! let file_io = FileIO::new_with_memory();
//! ```
//!
//! ## Using Custom Storage Factory
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use iceberg::io::{FileIO, StorageConfig};
//! use iceberg_storage_opendal::OpenDalStorageFactory;
//!
//! // Use OpenDAL storage factory for cloud storage
//! let factory = Arc::new(OpenDalStorageFactory);
//! let file_io = FileIO::from_path("s3://bucket/path")?
//!     .with_storage_factory(factory);
//! ```
//!
//! # How to use `FileIO`
//!
//! Currently `FileIO` provides simple methods for file operations:
//!
//! - `delete`: Delete file.
//! - `exists`: Check if file exists.
//! - `new_input`: Create input file for reading.
//! - `new_output`: Create output file for writing.

mod file_io;
mod local_fs;
mod memory;
mod storage;
mod storage_config;

pub use file_io::*;
pub use local_fs::{LocalFsFileRead, LocalFsFileWrite, LocalFsStorage, LocalFsStorageFactory};
pub use memory::{MemoryFileRead, MemoryFileWrite, MemoryStorage, MemoryStorageFactory};
pub use storage::{Storage, StorageFactory};
pub use storage_config::StorageConfig;
pub(crate) mod object_cache;

/// Helper function to check if a string value is truthy.
///
/// Returns `true` if the value is one of: "true", "t", "1", "on" (case-insensitive).
pub fn is_truthy(value: &str) -> bool {
    ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str())
}
