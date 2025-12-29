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

//! OpenDAL-based storage implementation for Apache Iceberg.
//!
//! This crate provides storage backends using [OpenDAL](https://opendal.apache.org/),
//! supporting various cloud storage services including:
//!
//! - Amazon S3 (feature: `storage-s3`)
//! - Google Cloud Storage (feature: `storage-gcs`)
//! - Alibaba Cloud OSS (feature: `storage-oss`)
//! - Azure Data Lake Storage (feature: `storage-azdls`)
//! - Local filesystem (feature: `storage-fs`)
//!
//! # Usage
//!
//! Add this crate to your `Cargo.toml` with the desired storage features:
//!
//! ```toml
//! [dependencies]
//! iceberg-storage-opendal = { version = "0.8.0", features = ["storage-s3"] }
//! ```
//!
//! Then use the `OpenDalStorageFactory` to create storage instances:
//!
//! ```rust,ignore
//! use std::collections::HashMap;
//! use iceberg::io::{FileIO, StorageConfig};
//! use iceberg_storage_opendal::OpenDalStorageFactory;
//!
//! let factory = OpenDalStorageFactory;
//! let file_io = FileIO::from_path("s3://bucket/path")?
//!     .with_storage_factory(std::sync::Arc::new(factory));
//! ```

mod storage;

#[cfg(feature = "storage-azdls")]
mod storage_azdls;
#[cfg(feature = "storage-fs")]
mod storage_fs;
#[cfg(feature = "storage-gcs")]
mod storage_gcs;
#[cfg(feature = "storage-oss")]
mod storage_oss;
#[cfg(feature = "storage-s3")]
mod storage_s3;

// Re-export the main types
// Re-export traits from iceberg crate for convenience
pub use iceberg::io::{Storage, StorageConfig, StorageFactory};
pub use storage::{OpenDalStorage, OpenDalStorageFactory};
// Re-export storage-specific configuration constants
#[cfg(feature = "storage-azdls")]
pub use storage_azdls::*;
#[cfg(feature = "storage-gcs")]
pub use storage_gcs::*;
#[cfg(feature = "storage-oss")]
pub use storage_oss::*;
#[cfg(feature = "storage-s3")]
pub use storage_s3::*;

/// Helper function to check if a string value is truthy.
pub(crate) fn is_truthy(value: &str) -> bool {
    ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str())
}
