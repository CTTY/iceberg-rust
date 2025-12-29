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
//! This crate provides a default storage factory that can delegate to multiple
//! storage backends based on enabled features. It serves as a convenience layer
//! for catalog implementations that need to construct `FileIO` without being
//! tied to a specific storage implementation.
//!
//! # Features
//!
//! - `opendal` (default): Enables OpenDAL-based storage backends (S3, GCS, Azure, filesystem)
//!
//! # Usage
//!
//! ```rust,ignore
//! use iceberg::io::FileIO;
//! use iceberg_storage_utils::default_storage_factory;
//!
//! // Create FileIO with the default storage factory
//! let file_io = FileIO::from_path("s3://bucket/path")?
//!     .with_storage_factory(default_storage_factory());
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
//!         FileIO::from_path(&warehouse)?
//!             .with_storage_factory(factory)
//!     }
//! };
//! ```

use std::sync::Arc;

use iceberg::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};

#[cfg(feature = "opendal")]
use iceberg_storage_opendal::OpenDalStorageFactory;

// Re-export traits from iceberg crate for convenience
pub use iceberg::io::{Storage, StorageConfig, StorageFactory};

/// Returns the default storage factory based on enabled features.
///
/// This factory can create storage instances for all schemes supported
/// by the enabled storage backends.
///
/// # Supported Schemes
///
/// With the `opendal` feature enabled (default):
/// - `s3`, `s3a`: Amazon S3
/// - `gs`, `gcs`: Google Cloud Storage
/// - `oss`: Alibaba Cloud OSS
/// - `abfss`, `abfs`, `wasbs`, `wasb`: Azure Data Lake Storage
/// - `file`, ``: Local filesystem
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::io::FileIO;
/// use iceberg_storage_utils::default_storage_factory;
///
/// let factory = default_storage_factory();
/// let file_io = FileIO::from_path("s3://bucket/path")?
///     .with_storage_factory(factory);
/// ```
pub fn default_storage_factory() -> Arc<dyn StorageFactory> {
    Arc::new(ResolvingStorageFactory::new())
}

/// A composite storage factory that delegates to the appropriate
/// backend based on the scheme.
///
/// This factory checks enabled features and delegates to the appropriate
/// storage backend. If no backend supports the requested scheme, it returns
/// an error.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use iceberg::io::{StorageConfig, StorageFactory};
/// use iceberg_storage_utils::ResolvingStorageFactory;
///
/// let factory = ResolvingStorageFactory::new();
/// let config = StorageConfig::new("s3", Default::default());
/// let storage = factory.build(&config)?;
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResolvingStorageFactory {
    #[cfg(feature = "opendal")]
    #[serde(skip)]
    opendal: OpenDalStorageFactory,
}

impl ResolvingStorageFactory {
    /// Create a new ResolvingStorageFactory.
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "opendal")]
            opendal: OpenDalStorageFactory,
        }
    }

    /// Check if this factory supports the given scheme.
    ///
    /// Returns `true` if any enabled backend supports the scheme.
    pub fn supports_scheme(&self, scheme: &str) -> bool {
        #[cfg(feature = "opendal")]
        if self.opendal.supports_scheme(scheme) {
            return true;
        }

        // Memory scheme is always supported via iceberg crate
        scheme == "memory"
    }
}

#[typetag::serde]
impl StorageFactory for ResolvingStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let scheme = config.scheme();

        // Try OpenDAL first (if enabled)
        #[cfg(feature = "opendal")]
        {
            if self.opendal.supports_scheme(scheme) {
                return self.opendal.build(config);
            }
        }

        // Memory scheme is handled by MemoryStorageFactory in iceberg crate
        // Users should use FileIO::new_with_memory() for memory storage
        if scheme == "memory" {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Memory storage should be created via FileIO::new_with_memory()",
            ));
        }

        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "No storage backend available for scheme '{}'. \
                 Enable the appropriate feature (e.g., 'opendal') or use a custom StorageFactory.",
                scheme
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_storage_factory_returns_resolving_factory() {
        let factory = default_storage_factory();
        // Verify we got a valid factory instance
        assert!(format!("{:?}", factory).contains("ResolvingStorageFactory"));
    }

    #[test]
    fn test_resolving_factory_new() {
        let factory = ResolvingStorageFactory::new();
        assert!(format!("{:?}", factory).contains("ResolvingStorageFactory"));
    }

    #[test]
    #[cfg(feature = "opendal")]
    fn test_supports_scheme_with_opendal() {
        let factory = ResolvingStorageFactory::new();

        // OpenDAL schemes
        assert!(factory.supports_scheme("s3"));
        assert!(factory.supports_scheme("s3a"));
        assert!(factory.supports_scheme("gs"));
        assert!(factory.supports_scheme("gcs"));
        assert!(factory.supports_scheme("file"));
        assert!(factory.supports_scheme("oss"));
        assert!(factory.supports_scheme("abfss"));

        // Memory is always supported
        assert!(factory.supports_scheme("memory"));

        // Unknown schemes
        assert!(!factory.supports_scheme("unknown"));
    }

    #[test]
    fn test_supports_scheme_memory() {
        let factory = ResolvingStorageFactory::new();
        assert!(factory.supports_scheme("memory"));
    }

    #[test]
    #[cfg(feature = "opendal")]
    fn test_build_with_opendal_fs() {
        use std::collections::HashMap;

        let factory = ResolvingStorageFactory::new();
        let config = StorageConfig::new("file", HashMap::new());
        let storage = factory.build(&config);

        assert!(storage.is_ok());
        assert!(format!("{:?}", storage.unwrap()).contains("LocalFs"));
    }

    #[test]
    fn test_build_memory_returns_error() {
        use std::collections::HashMap;

        let factory = ResolvingStorageFactory::new();
        let config = StorageConfig::new("memory", HashMap::new());
        let result = factory.build(&config);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("FileIO::new_with_memory()"));
    }

    #[test]
    fn test_build_unknown_scheme_returns_error() {
        use std::collections::HashMap;

        let factory = ResolvingStorageFactory::new();
        let config = StorageConfig::new("unknown", HashMap::new());
        let result = factory.build(&config);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No storage backend available"));
    }

    #[test]
    fn test_resolving_factory_serialization() {
        let factory = ResolvingStorageFactory::new();

        // Serialize
        let serialized = serde_json::to_string(&factory).unwrap();

        // Deserialize
        let deserialized: ResolvingStorageFactory = serde_json::from_str(&serialized).unwrap();

        // Verify the deserialized factory works
        assert!(deserialized.supports_scheme("memory"));
    }

    #[test]
    fn test_storage_factory_trait_object() {
        // Test that StorageFactory can be used as a trait object
        let factory: Arc<dyn StorageFactory> = default_storage_factory();
        assert!(format!("{:?}", factory).contains("ResolvingStorageFactory"));
    }
}
