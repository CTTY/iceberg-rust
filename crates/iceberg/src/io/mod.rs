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
//! use iceberg::io::{FileIOBuilder, StorageConfig};
//! use iceberg_storage_opendal::OpenDalStorageFactory;
//!
//! // Use OpenDAL storage factory for cloud storage
//! let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3))
//!     .with_prop("s3.region", "us-east-1")
//!     .build()?;
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

mod config;
mod file_io;
mod local_fs;
mod memory;
mod storage;

pub use config::{
    ADLS_ACCOUNT_KEY,
    ADLS_ACCOUNT_NAME,
    ADLS_AUTHORITY_HOST,
    ADLS_CLIENT_ID,
    ADLS_CLIENT_SECRET,
    ADLS_CONNECTION_STRING,
    ADLS_SAS_TOKEN,
    ADLS_TENANT_ID,
    // Azure ADLS config
    AzdlsConfig,
    CLIENT_REGION,
    GCS_ALLOW_ANONYMOUS,
    GCS_CREDENTIALS_JSON,
    GCS_DISABLE_CONFIG_LOAD,
    GCS_DISABLE_VM_METADATA,
    GCS_NO_AUTH,
    GCS_PROJECT_ID,
    GCS_SERVICE_PATH,
    GCS_TOKEN,
    GCS_USER_PROJECT,
    // GCS config
    GcsConfig,
    OSS_ACCESS_KEY_ID,
    OSS_ACCESS_KEY_SECRET,
    OSS_ENDPOINT,
    // OSS config
    OssConfig,
    S3_ACCESS_KEY_ID,
    S3_ALLOW_ANONYMOUS,
    S3_ASSUME_ROLE_ARN,
    S3_ASSUME_ROLE_EXTERNAL_ID,
    S3_ASSUME_ROLE_SESSION_NAME,
    S3_DISABLE_CONFIG_LOAD,
    S3_DISABLE_EC2_METADATA,
    S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
    S3_SSE_KEY,
    S3_SSE_MD5,
    S3_SSE_TYPE,
    // S3 config
    S3Config,
    StorageConfig,
};
pub use file_io::*;
pub use local_fs::{LocalFsFileRead, LocalFsFileWrite, LocalFsStorage, LocalFsStorageFactory};
pub use memory::{MemoryFileRead, MemoryFileWrite, MemoryStorage, MemoryStorageFactory};
pub use storage::{Storage, StorageFactory};
pub(crate) mod object_cache;

/// Helper function to check if a string value is truthy.
///
/// Returns `true` if the value is one of: "true", "t", "1", "on" (case-insensitive).
pub fn is_truthy(value: &str) -> bool {
    ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str())
}
