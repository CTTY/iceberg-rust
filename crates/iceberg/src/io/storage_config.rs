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

//! Storage configuration for storage backends.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{Error, ErrorKind, Result};

/// Configuration for storage backends.
///
/// Contains all settings needed to create a Storage instance, including
/// the URL scheme (e.g., "s3", "gs", "file") and configuration properties.
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
///
/// use iceberg::io::StorageConfig;
///
/// // Create a new StorageConfig for S3
/// let config = StorageConfig::new("s3", HashMap::new())
///     .with_prop("region", "us-east-1")
///     .with_prop("bucket", "my-bucket");
///
/// assert_eq!(config.scheme(), "s3");
/// assert_eq!(config.get("region"), Some(&"us-east-1".to_string()));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    /// URL scheme (e.g., "s3", "gs", "file", "memory")
    scheme: String,
    /// Configuration properties for the storage backend
    props: HashMap<String, String>,
}

impl StorageConfig {
    /// Create a new StorageConfig with the given scheme and properties.
    ///
    /// # Arguments
    ///
    /// * `scheme` - The URL scheme for the storage backend (e.g., "s3", "gs", "file")
    /// * `props` - Configuration properties for the storage backend
    pub fn new(scheme: impl Into<String>, props: HashMap<String, String>) -> Self {
        Self {
            scheme: scheme.into(),
            props,
        }
    }

    /// Create a StorageConfig from a path, inferring the scheme.
    ///
    /// # Arguments
    ///
    /// * `path` - A path or URL from which to infer the scheme
    ///
    /// # Returns
    ///
    /// A `Result` containing the `StorageConfig` with the inferred scheme,
    /// or an error if the path is invalid.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::io::StorageConfig;
    ///
    /// let config = StorageConfig::from_path("s3://bucket/path").unwrap();
    /// assert_eq!(config.scheme(), "s3");
    ///
    /// let config = StorageConfig::from_path("/local/path").unwrap();
    /// assert_eq!(config.scheme(), "file");
    /// ```
    pub fn from_path(path: impl AsRef<str>) -> Result<Self> {
        let path_str = path.as_ref();
        let url = Url::parse(path_str).map_err(Error::from).or_else(|e| {
            Url::from_file_path(path_str).map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Input is neither a valid url nor path",
                )
                .with_context("input", path_str.to_string())
                .with_source(e)
            })
        })?;

        Ok(Self::new(url.scheme(), HashMap::new()))
    }

    /// Get the URL scheme.
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Get all configuration properties.
    pub fn props(&self) -> &HashMap<String, String> {
        &self.props
    }

    /// Get a specific configuration property by key.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to look up
    ///
    /// # Returns
    ///
    /// An `Option` containing a reference to the property value if it exists.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.props.get(key)
    }

    /// Add a configuration property.
    ///
    /// This is a builder-style method that returns `self` for chaining.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key
    /// * `value` - The property value
    pub fn with_prop(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.props.insert(key.into(), value.into());
        self
    }

    /// Add multiple configuration properties.
    ///
    /// This is a builder-style method that returns `self` for chaining.
    ///
    /// # Arguments
    ///
    /// * `props` - An iterator of key-value pairs to add
    pub fn with_props(
        mut self,
        props: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.props
            .extend(props.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_config_new() {
        let props = HashMap::from([
            ("region".to_string(), "us-east-1".to_string()),
            ("bucket".to_string(), "my-bucket".to_string()),
        ]);
        let config = StorageConfig::new("s3", props.clone());

        assert_eq!(config.scheme(), "s3");
        assert_eq!(config.props(), &props);
    }

    #[test]
    fn test_storage_config_from_path_s3() {
        let config = StorageConfig::from_path("s3://bucket/path").unwrap();
        assert_eq!(config.scheme(), "s3");
        assert!(config.props().is_empty());
    }

    #[test]
    fn test_storage_config_from_path_gs() {
        let config = StorageConfig::from_path("gs://bucket/path").unwrap();
        assert_eq!(config.scheme(), "gs");
    }

    #[test]
    fn test_storage_config_from_path_file() {
        let config = StorageConfig::from_path("file:///tmp/path").unwrap();
        assert_eq!(config.scheme(), "file");
    }

    #[test]
    fn test_storage_config_from_path_local() {
        let config = StorageConfig::from_path("/tmp/local/path").unwrap();
        assert_eq!(config.scheme(), "file");
    }

    #[test]
    fn test_storage_config_from_path_memory() {
        let config = StorageConfig::from_path("memory:///path").unwrap();
        assert_eq!(config.scheme(), "memory");
    }

    #[test]
    fn test_storage_config_from_path_invalid() {
        let result = StorageConfig::from_path("invalid||path");
        assert!(result.is_err());
    }

    #[test]
    fn test_storage_config_get() {
        let config = StorageConfig::new("s3", HashMap::new()).with_prop("region", "us-east-1");

        assert_eq!(config.get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(config.get("nonexistent"), None);
    }

    #[test]
    fn test_storage_config_with_prop() {
        let config = StorageConfig::new("s3", HashMap::new())
            .with_prop("region", "us-east-1")
            .with_prop("bucket", "my-bucket");

        assert_eq!(config.get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(config.get("bucket"), Some(&"my-bucket".to_string()));
    }

    #[test]
    fn test_storage_config_with_props() {
        let additional_props = vec![("key1", "value1"), ("key2", "value2")];
        let config = StorageConfig::new("s3", HashMap::new()).with_props(additional_props);

        assert_eq!(config.get("key1"), Some(&"value1".to_string()));
        assert_eq!(config.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_storage_config_clone() {
        let config = StorageConfig::new("s3", HashMap::new()).with_prop("region", "us-east-1");
        let cloned = config.clone();

        assert_eq!(config, cloned);
        assert_eq!(cloned.scheme(), "s3");
        assert_eq!(cloned.get("region"), Some(&"us-east-1".to_string()));
    }

    #[test]
    fn test_storage_config_serialization_roundtrip() {
        let config = StorageConfig::new("s3", HashMap::new())
            .with_prop("region", "us-east-1")
            .with_prop("bucket", "my-bucket");

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: StorageConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_storage_config_clone_independence() {
        let original = StorageConfig::new("s3", HashMap::new()).with_prop("region", "us-east-1");
        let mut cloned = original.clone();

        // Modify the clone
        cloned = cloned.with_prop("region", "eu-west-1");
        cloned = cloned.with_prop("new_key", "new_value");

        // Original should be unchanged
        assert_eq!(original.get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(original.get("new_key"), None);

        // Clone should have the new values
        assert_eq!(cloned.get("region"), Some(&"eu-west-1".to_string()));
        assert_eq!(cloned.get("new_key"), Some(&"new_value".to_string()));
    }
}
