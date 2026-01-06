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

//! Storage utilities for Apache Iceberg.
//!
//! This crate provides a default storage factory based on enabled features.
//! It serves as a convenience layer for catalog implementations that need
//! to construct `FileIO` without being tied to a specific storage implementation.
//!
//! # Features
//!
//! - `storage-s3` (default): Enables S3 storage backend
//! - `storage-gcs`: Enables Google Cloud Storage backend
//! - `storage-oss`: Enables Alibaba Cloud OSS backend
//! - `storage-azdls`: Enables Azure Data Lake Storage backend
//! - `storage-fs`: Enables local filesystem backend
//! - `storage-all`: Enables all storage backends
//!
//! # Usage
//!
//! ```rust,ignore
//! use iceberg::io::FileIO;
//! use iceberg_storage_utils::default_storage_factory;
//!
//! // Create FileIO with the default storage factory
//! let file_io = FileIO::new(default_storage_factory())
//!     .with_prop("s3.region", "us-east-1");
//! ```
//!
//! # For Catalog Implementers
//!
//! Catalog crates should depend on this crate and use `default_storage_factory()`
//! when constructing `FileIO` without an explicitly injected factory:
//!
//! ```rust,ignore
//! use iceberg_storage_utils::default_storage_factory;
//!
//! let file_io = match self.file_io {
//!     Some(io) => io,
//!     None => {
//!         let factory = self.storage_factory
//!             .unwrap_or_else(default_storage_factory);
//!         FileIO::new(factory)
//!             .with_props(props)
//!     }
//! };
//! ```

use std::sync::Arc;

// Re-export traits from iceberg crate for convenience
pub use iceberg::io::{LocalFsStorageFactory, Storage, StorageConfig, StorageFactory};

// Re-export OpenDalStorageFactory when any storage feature is enabled
#[cfg(any(
    feature = "storage-s3",
    feature = "storage-gcs",
    feature = "storage-oss",
    feature = "storage-azdls",
    feature = "storage-fs"
))]
pub use iceberg_storage_opendal::OpenDalStorageFactory;

/// Returns the default storage factory based on enabled features.
///
/// This function returns the first available storage factory based on
/// the enabled feature flags. For catalog implementations that need
/// to support multiple backends, use the specific factory variant directly.
///
/// # Feature Priority
///
/// When multiple features are enabled, the priority is:
/// 1. S3 (most common cloud storage)
/// 2. GCS
/// 3. OSS
/// 4. Azure
/// 5. Filesystem (OpenDAL)
/// 6. LocalFsStorageFactory (fallback when no storage features enabled)
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::io::FileIO;
/// use iceberg_storage_utils::default_storage_factory;
///
/// let file_io = FileIO::new(default_storage_factory())
///     .with_prop("s3.region", "us-east-1");
/// ```
pub fn default_storage_factory() -> Arc<dyn StorageFactory> {
    #[cfg(feature = "storage-s3")]
    {
        return Arc::new(OpenDalStorageFactory::S3);
    }
    #[cfg(feature = "storage-gcs")]
    {
        return Arc::new(OpenDalStorageFactory::Gcs);
    }
    #[cfg(feature = "storage-oss")]
    {
        return Arc::new(OpenDalStorageFactory::Oss);
    }
    #[cfg(feature = "storage-azdls")]
    {
        return Arc::new(OpenDalStorageFactory::Azdls);
    }
    #[cfg(feature = "storage-fs")]
    {
        return Arc::new(OpenDalStorageFactory::Fs);
    }
    #[allow(unreachable_code)]
    Arc::new(LocalFsStorageFactory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "storage-s3")]
    fn test_default_storage_factory_returns_s3() {
        let factory = default_storage_factory();
        // Verify we got a valid factory instance
        assert!(format!("{:?}", factory).contains("S3"));
    }

    #[test]
    #[cfg(feature = "storage-s3")]
    fn test_default_storage_factory_builds_storage() {
        let factory = default_storage_factory();
        let config = StorageConfig::new();
        let result = factory.build(&config);
        // Should succeed - S3 factory doesn't require any config
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "storage-s3")]
    fn test_storage_factory_trait_object() {
        let factory: Arc<dyn StorageFactory> = default_storage_factory();
        assert!(format!("{:?}", factory).contains("S3"));
    }
}
