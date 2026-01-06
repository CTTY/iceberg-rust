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
//! - `opendal` (default): Enables OpenDAL-based storage backends
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
pub use iceberg::io::{Storage, StorageConfig, StorageFactory};
#[cfg(feature = "opendal")]
pub use iceberg_storage_opendal::OpenDalStorageFactory;

/// Returns the default storage factory based on enabled features.
///
/// When the `opendal` feature is enabled (default), this returns an S3 factory
/// since S3 is the most commonly used cloud storage backend.
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
#[cfg(feature = "opendal")]
pub fn default_storage_factory() -> Arc<dyn StorageFactory> {
    // Default to S3 factory since it's the most common cloud storage
    // The opendal feature enables storage-s3 by default
    Arc::new(OpenDalStorageFactory::S3)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "opendal")]
    fn test_default_storage_factory_returns_factory() {
        let factory = default_storage_factory();
        // Verify we got a valid factory instance
        assert!(format!("{:?}", factory).contains("S3"));
    }

    #[test]
    #[cfg(feature = "opendal")]
    fn test_default_storage_factory_builds_storage() {
        let factory = default_storage_factory();
        let config = StorageConfig::new();
        let result = factory.build(&config);
        // Should succeed - S3 factory doesn't require any config
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "opendal")]
    fn test_storage_factory_trait_object() {
        let factory: Arc<dyn StorageFactory> = default_storage_factory();
        assert!(format!("{:?}", factory).contains("S3"));
    }
}
