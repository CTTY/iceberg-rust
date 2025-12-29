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

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use opendal::layers::RetryLayer;
#[cfg(feature = "storage-azdls")]
use opendal::services::AzdlsConfig;
#[cfg(feature = "storage-gcs")]
use opendal::services::GcsConfig;
#[cfg(feature = "storage-oss")]
use opendal::services::OssConfig;
#[cfg(feature = "storage-s3")]
use opendal::services::S3Config;
use opendal::{Operator, Scheme};
use serde::{Deserialize, Serialize};

use super::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile, StorageConfig};
use crate::{Error, ErrorKind, Result};

/// Trait for storage operations in Iceberg.
///
/// The trait supports serialization via `typetag`, allowing storage instances to be
/// serialized and deserialized across process boundaries.
///
/// Third-party implementations can implement this trait to provide custom storage backends.
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

/// Default storage factory using OpenDAL.
///
/// This factory creates `OpenDalStorage` instances based on the provided
/// `StorageConfig`. It supports all storage backends that OpenDAL supports,
/// including S3, GCS, Azure, local filesystem, and in-memory storage.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use iceberg::io::{StorageConfig, StorageFactory, OpenDalStorageFactory};
///
/// let factory = OpenDalStorageFactory;
/// let config = StorageConfig::new("memory", Default::default());
/// let storage = factory.build(&config)?;
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OpenDalStorageFactory;

#[typetag::serde]
impl StorageFactory for OpenDalStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let storage = OpenDalStorage::build_from_config(config)?;
        Ok(Arc::new(storage))
    }
}

/// Unified OpenDAL-based storage implementation.
///
/// This storage handles all supported schemes (S3, GCS, Azure, filesystem, memory)
/// through OpenDAL, creating operators on-demand based on the path scheme.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OpenDalStorage {
    /// In-memory storage, useful for testing
    #[cfg(feature = "storage-memory")]
    Memory(#[serde(skip, default = "default_memory_op")] Operator),
    /// Local filesystem storage
    #[cfg(feature = "storage-fs")]
    LocalFs,
    /// Amazon S3 storage
    /// Expects paths of the form `s3[a]://<bucket>/<path>`.
    #[cfg(feature = "storage-s3")]
    S3 {
        /// s3 storage could have `s3://` and `s3a://`.
        /// Storing the scheme string here to return the correct path.
        configured_scheme: String,
        /// S3 configuration
        config: Arc<S3Config>,
        /// Optional custom credential loader
        #[serde(skip)]
        customized_credential_load: Option<super::CustomAwsCredentialLoader>,
    },
    /// Google Cloud Storage
    #[cfg(feature = "storage-gcs")]
    Gcs {
        /// GCS configuration
        config: Arc<GcsConfig>,
    },
    /// Alibaba Cloud OSS
    #[cfg(feature = "storage-oss")]
    Oss {
        /// OSS configuration
        config: Arc<OssConfig>,
    },
    /// Azure Data Lake Storage
    /// Expects paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    #[cfg(feature = "storage-azdls")]
    Azdls {
        /// Because Azdls accepts multiple possible schemes, we store the full
        /// passed scheme here to later validate schemes passed via paths.
        configured_scheme: super::AzureStorageScheme,
        /// Azure DLS configuration
        config: Arc<AzdlsConfig>,
    },
}

#[cfg(feature = "storage-memory")]
fn default_memory_op() -> Operator {
    super::memory_config_build().expect("Failed to build memory operator")
}

impl OpenDalStorage {
    /// Build storage from StorageConfig.
    ///
    /// This method creates an OpenDalStorage instance from a StorageConfig,
    /// which contains the scheme and properties needed to configure the storage backend.
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration containing scheme and properties
    ///
    /// # Returns
    ///
    /// A `Result` containing the `OpenDalStorage` on success, or an error
    /// if the storage could not be created.
    pub fn build_from_config(config: &StorageConfig) -> Result<Self> {
        let scheme_str = config.scheme();
        let props = config.props().clone();
        let scheme = Self::parse_scheme(scheme_str)?;

        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory(super::memory_config_build()?)),

            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs),

            #[cfg(feature = "storage-s3")]
            Scheme::S3 => Ok(Self::S3 {
                configured_scheme: scheme_str.to_string(),
                config: super::s3_config_parse(props)?.into(),
                // Note: Custom credential loaders are not supported via StorageConfig.
                // Users needing custom credentials should implement their own StorageFactory.
                customized_credential_load: None,
            }),

            #[cfg(feature = "storage-gcs")]
            Scheme::Gcs => Ok(Self::Gcs {
                config: super::gcs_config_parse(props)?.into(),
            }),

            #[cfg(feature = "storage-oss")]
            Scheme::Oss => Ok(Self::Oss {
                config: super::oss_config_parse(props)?.into(),
            }),

            #[cfg(feature = "storage-azdls")]
            Scheme::Azdls => {
                let configured_scheme = scheme_str.parse::<super::AzureStorageScheme>()?;
                Ok(Self::Azdls {
                    configured_scheme,
                    config: super::azdls_config_parse(props)?.into(),
                })
            }
            // Update doc on [`FileIO`] when adding new schemes.
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Constructing file io from scheme: {scheme} not supported now",),
            )),
        }
    }

    /// Creates operator from path.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    ///
    /// # Returns
    ///
    /// The return value consists of two parts:
    ///
    /// * An [`opendal::Operator`] instance used to operate on file.
    /// * Relative path to the root uri of [`opendal::Operator`].
    fn create_operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "storage-memory")]
            Self::Memory(op) => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    Ok::<_, crate::Error>((op.clone(), stripped))
                } else {
                    Ok::<_, crate::Error>((op.clone(), &path[1..]))
                }
            }

            #[cfg(feature = "storage-fs")]
            Self::LocalFs => {
                let op = super::fs_config_build()?;

                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok::<_, crate::Error>((op, stripped))
                } else {
                    Ok::<_, crate::Error>((op, &path[1..]))
                }
            }

            #[cfg(feature = "storage-s3")]
            Self::S3 {
                configured_scheme,
                config,
                customized_credential_load,
            } => {
                let op = super::s3_config_build(config, customized_credential_load, path)?;
                let op_info = op.info();

                // Check prefix of s3 path.
                let prefix = format!("{}://{}/", configured_scheme, op_info.name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ))
                }
            }

            #[cfg(feature = "storage-gcs")]
            Self::Gcs { config } => {
                let operator = super::gcs_config_build(config, path)?;
                let prefix = format!("gs://{}/", operator.info().name());
                if path.starts_with(&prefix) {
                    Ok((operator, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, should start with {prefix}"),
                    ))
                }
            }

            #[cfg(feature = "storage-oss")]
            Self::Oss { config } => {
                let op = super::oss_config_build(config, path)?;
                // Check prefix of oss path.
                let prefix = format!("oss://{}/", op.info().name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, should start with {prefix}"),
                    ))
                }
            }

            #[cfg(feature = "storage-azdls")]
            Self::Azdls {
                configured_scheme,
                config,
            } => super::azdls_create_operator(path, config, configured_scheme),
            #[cfg(all(
                not(feature = "storage-s3"),
                not(feature = "storage-fs"),
                not(feature = "storage-gcs"),
                not(feature = "storage-oss"),
                not(feature = "storage-azdls"),
            ))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }?;

        // Transient errors are common for object stores; however there's no
        // harm in retrying temporary failures for other storage backends as well.
        let operator = operator.layer(RetryLayer::new());

        Ok((operator, relative_path))
    }

    /// Parse scheme.
    fn parse_scheme(scheme: &str) -> crate::Result<Scheme> {
        match scheme {
            "memory" => Ok(Scheme::Memory),
            "file" | "" => Ok(Scheme::Fs),
            "s3" | "s3a" => Ok(Scheme::S3),
            "gs" | "gcs" => Ok(Scheme::Gcs),
            "oss" => Ok(Scheme::Oss),
            "abfss" | "abfs" | "wasbs" | "wasb" => Ok(Scheme::Azdls),
            s => Ok(s.parse::<Scheme>()?),
        }
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for OpenDalStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(op.exists(relative_path).await?)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, relative_path) = self.create_operator(path)?;
        let meta = op.stat(relative_path).await?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(op.read(relative_path).await?.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(Box::new(op.reader(relative_path).await?))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let mut writer = self.writer(path).await?;
        writer.write(bs).await?;
        writer.close().await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(Box::new(op.writer(relative_path).await?))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(op.delete(relative_path).await?)
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(path)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(op.remove_all(&path).await?)
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_opendal_storage_memory() {
        let config = StorageConfig::new("memory", HashMap::new());
        let storage = OpenDalStorage::build_from_config(&config).unwrap();
        assert!(matches!(storage, OpenDalStorage::Memory { .. }));
    }

    #[test]
    #[cfg(feature = "storage-fs")]
    fn test_opendal_storage_fs() {
        let config = StorageConfig::new("file", HashMap::new());
        let storage = OpenDalStorage::build_from_config(&config).unwrap();
        assert!(matches!(storage, OpenDalStorage::LocalFs));
    }

    #[test]
    #[cfg(feature = "storage-s3")]
    fn test_opendal_storage_s3() {
        let config = StorageConfig::new("s3", HashMap::new());
        let storage = OpenDalStorage::build_from_config(&config).unwrap();
        assert!(matches!(storage, OpenDalStorage::S3 { .. }));
    }

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_storage_serialization() {
        let config = StorageConfig::new("memory", HashMap::new());
        let storage = OpenDalStorage::build_from_config(&config).unwrap();

        // Serialize
        let serialized = serde_json::to_string(&storage).unwrap();

        // Deserialize
        let deserialized: OpenDalStorage = serde_json::from_str(&serialized).unwrap();

        assert!(matches!(deserialized, OpenDalStorage::Memory { .. }));
    }

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_opendal_storage_factory_memory() {
        let factory = OpenDalStorageFactory;
        let config = StorageConfig::new("memory", HashMap::new());
        let storage = factory.build(&config).unwrap();

        // Verify we got a valid storage instance
        assert!(format!("{:?}", storage).contains("Memory"));
    }

    #[test]
    #[cfg(feature = "storage-fs")]
    fn test_opendal_storage_factory_fs() {
        let factory = OpenDalStorageFactory;
        let config = StorageConfig::new("file", HashMap::new());
        let storage = factory.build(&config).unwrap();

        // Verify we got a valid storage instance
        assert!(format!("{:?}", storage).contains("LocalFs"));
    }

    #[test]
    #[cfg(feature = "storage-s3")]
    fn test_opendal_storage_factory_s3() {
        let factory = OpenDalStorageFactory;
        let config = StorageConfig::new("s3", HashMap::new());
        let storage = factory.build(&config).unwrap();

        // Verify we got a valid storage instance
        assert!(format!("{:?}", storage).contains("S3"));
    }

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_opendal_storage_factory_serialization() {
        let factory = OpenDalStorageFactory;

        // Serialize
        let serialized = serde_json::to_string(&factory).unwrap();

        // Deserialize
        let deserialized: OpenDalStorageFactory = serde_json::from_str(&serialized).unwrap();

        // Verify the deserialized factory works
        let config = StorageConfig::new("memory", HashMap::new());
        let storage = deserialized.build(&config).unwrap();
        assert!(format!("{:?}", storage).contains("Memory"));
    }

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_storage_factory_trait_object() {
        // Test that StorageFactory can be used as a trait object
        let factory: Arc<dyn StorageFactory> = Arc::new(OpenDalStorageFactory);
        let config = StorageConfig::new("memory", HashMap::new());
        let storage = factory.build(&config).unwrap();

        assert!(format!("{:?}", storage).contains("Memory"));
    }

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_build_from_config_memory() {
        let config = StorageConfig::new("memory", HashMap::new());
        let storage = OpenDalStorage::build_from_config(&config).unwrap();
        assert!(matches!(storage, OpenDalStorage::Memory { .. }));
    }

    #[test]
    #[cfg(feature = "storage-fs")]
    fn test_build_from_config_fs() {
        let config = StorageConfig::new("file", HashMap::new());
        let storage = OpenDalStorage::build_from_config(&config).unwrap();
        assert!(matches!(storage, OpenDalStorage::LocalFs));
    }

    #[test]
    #[cfg(feature = "storage-s3")]
    fn test_build_from_config_s3() {
        let config = StorageConfig::new("s3", HashMap::new());
        let storage = OpenDalStorage::build_from_config(&config).unwrap();
        assert!(matches!(storage, OpenDalStorage::S3 { .. }));
    }

    #[test]
    #[cfg(feature = "storage-s3")]
    fn test_build_from_config_s3_with_props() {
        let config = StorageConfig::new("s3", HashMap::new()).with_prop("region", "us-east-1");
        let storage = OpenDalStorage::build_from_config(&config).unwrap();
        assert!(matches!(storage, OpenDalStorage::S3 { .. }));
    }
}
