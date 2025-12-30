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

//! OpenDAL-based storage implementation.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
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

/// Default storage factory using OpenDAL.
///
/// This factory creates `OpenDalStorage` instances based on the provided
/// `StorageConfig`. It supports all storage backends that OpenDAL supports,
/// including S3, GCS, Azure, local filesystem.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use iceberg::io::{StorageConfig, StorageFactory};
/// use iceberg_storage_opendal::OpenDalStorageFactory;
///
/// let factory = OpenDalStorageFactory;
/// let config = StorageConfig::new("s3", Default::default());
/// let storage = factory.build(&config)?;
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OpenDalStorageFactory;

impl OpenDalStorageFactory {
    /// Check if this factory supports the given scheme.
    ///
    /// This is useful for composite factories that delegate to multiple backends.
    pub fn supports_scheme(&self, scheme: &str) -> bool {
        matches!(
            scheme,
            "file" | "" | "s3" | "s3a" | "gs" | "gcs" | "oss" | "abfss" | "abfs" | "wasbs" | "wasb"
        )
    }
}

#[typetag::serde]
impl StorageFactory for OpenDalStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let storage = OpenDalStorage::build_from_config(config)?;
        Ok(Arc::new(storage))
    }
}

/// Unified OpenDAL-based storage implementation.
///
/// This storage handles all supported schemes (S3, GCS, Azure, filesystem)
/// through OpenDAL, creating operators on-demand based on the path scheme.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpenDalStorage {
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
        customized_credential_load: Option<super::storage_s3::CustomAwsCredentialLoader>,
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
        configured_scheme: super::storage_azdls::AzureStorageScheme,
        /// Azure DLS configuration
        config: Arc<AzdlsConfig>,
    },
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
        let scheme = Self::parse_scheme(scheme_str)?;

        match scheme {
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs),

            #[cfg(feature = "storage-s3")]
            Scheme::S3 => {
                let iceberg_s3_config = iceberg::io::S3Config::from(config);
                let opendal_s3_config = super::storage_s3::s3_config_to_opendal(&iceberg_s3_config);
                Ok(Self::S3 {
                    configured_scheme: scheme_str.to_string(),
                    config: opendal_s3_config.into(),
                    // Note: Custom credential loaders are not supported via StorageConfig.
                    // Users needing custom credentials should implement their own StorageFactory.
                    customized_credential_load: None,
                })
            }

            #[cfg(feature = "storage-gcs")]
            Scheme::Gcs => {
                let iceberg_gcs_config = iceberg::io::GcsConfig::from(config);
                let opendal_gcs_config =
                    super::storage_gcs::gcs_config_to_opendal(&iceberg_gcs_config);
                Ok(Self::Gcs {
                    config: opendal_gcs_config.into(),
                })
            }

            #[cfg(feature = "storage-oss")]
            Scheme::Oss => {
                let iceberg_oss_config = iceberg::io::OssConfig::from(config);
                let opendal_oss_config =
                    super::storage_oss::oss_config_to_opendal(&iceberg_oss_config);
                Ok(Self::Oss {
                    config: opendal_oss_config.into(),
                })
            }

            #[cfg(feature = "storage-azdls")]
            Scheme::Azdls => {
                let configured_scheme =
                    scheme_str.parse::<super::storage_azdls::AzureStorageScheme>()?;
                let iceberg_azdls_config = iceberg::io::AzdlsConfig::from(config);
                let opendal_azdls_config =
                    super::storage_azdls::azdls_config_to_opendal(&iceberg_azdls_config)?;
                Ok(Self::Azdls {
                    configured_scheme,
                    config: opendal_azdls_config.into(),
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
            #[cfg(feature = "storage-fs")]
            Self::LocalFs => {
                let op = super::storage_fs::fs_config_build()?;

                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok::<_, iceberg::Error>((op, stripped))
                } else {
                    Ok::<_, iceberg::Error>((op, &path[1..]))
                }
            }

            #[cfg(feature = "storage-s3")]
            Self::S3 {
                configured_scheme,
                config,
                customized_credential_load,
            } => {
                let op =
                    super::storage_s3::s3_config_build(config, customized_credential_load, path)?;
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
                let operator = super::storage_gcs::gcs_config_build(config, path)?;
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
                let op = super::storage_oss::oss_config_build(config, path)?;
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
            } => super::storage_azdls::azdls_create_operator(path, config, configured_scheme),
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
    fn parse_scheme(scheme: &str) -> iceberg::Result<Scheme> {
        match scheme {
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
    #[cfg(feature = "storage-fs")]
    fn test_opendal_storage_factory_serialization() {
        let factory = OpenDalStorageFactory;

        // Serialize
        let serialized = serde_json::to_string(&factory).unwrap();

        // Deserialize
        let deserialized: OpenDalStorageFactory = serde_json::from_str(&serialized).unwrap();

        // Verify the deserialized factory works
        let config = StorageConfig::new("file", HashMap::new());
        let storage = deserialized.build(&config).unwrap();
        assert!(format!("{:?}", storage).contains("LocalFs"));
    }

    #[test]
    #[cfg(feature = "storage-fs")]
    fn test_storage_factory_trait_object() {
        // Test that StorageFactory can be used as a trait object
        let factory: Arc<dyn StorageFactory> = Arc::new(OpenDalStorageFactory);
        let config = StorageConfig::new("file", HashMap::new());
        let storage = factory.build(&config).unwrap();

        assert!(format!("{:?}", storage).contains("LocalFs"));
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

    #[test]
    fn test_supports_scheme() {
        let factory = OpenDalStorageFactory;
        assert!(factory.supports_scheme("s3"));
        assert!(factory.supports_scheme("s3a"));
        assert!(factory.supports_scheme("gs"));
        assert!(factory.supports_scheme("gcs"));
        assert!(factory.supports_scheme("file"));
        assert!(factory.supports_scheme("oss"));
        assert!(factory.supports_scheme("abfss"));
        assert!(factory.supports_scheme("abfs"));
        assert!(factory.supports_scheme("wasbs"));
        assert!(factory.supports_scheme("wasb"));
        assert!(!factory.supports_scheme("memory"));
        assert!(!factory.supports_scheme("unknown"));
    }

    // Local filesystem integration tests
    #[cfg(feature = "storage-fs")]
    mod fs_tests {
        use std::fs::{File, create_dir_all};
        use std::io::Write;
        use std::path::Path;

        use futures::AsyncReadExt;
        use futures::io::AllowStdIo;
        use iceberg::io::FileIO;
        use tempfile::TempDir;

        use super::*;

        fn create_local_file_io() -> FileIO {
            FileIO::from_path("file:///tmp")
                .unwrap()
                .with_storage_factory(Arc::new(OpenDalStorageFactory))
        }

        fn write_to_file<P: AsRef<Path>>(s: &str, path: P) {
            create_dir_all(path.as_ref().parent().unwrap()).unwrap();
            let mut f = File::create(path).unwrap();
            write!(f, "{s}").unwrap();
        }

        async fn read_from_file<P: AsRef<Path>>(path: P) -> String {
            let mut f = AllowStdIo::new(File::open(path).unwrap());
            let mut s = String::new();
            f.read_to_string(&mut s).await.unwrap();
            s
        }

        #[tokio::test]
        async fn test_local_input_file() {
            let tmp_dir = TempDir::new().unwrap();

            let file_name = "a.txt";
            let content = "Iceberg loves rust.";

            let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);
            write_to_file(content, &full_path);

            let file_io = create_local_file_io();
            let input_file = file_io.new_input(&full_path).unwrap();

            assert!(input_file.exists().await.unwrap());
            assert_eq!(&full_path, input_file.location());
            let read_content = read_from_file(full_path).await;

            assert_eq!(content, &read_content);
        }

        #[tokio::test]
        async fn test_delete_local_file() {
            let tmp_dir = TempDir::new().unwrap();

            let a_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), "a.txt");
            let sub_dir_path = format!("{}/sub", tmp_dir.path().to_str().unwrap());
            let b_path = format!("{}/{}", sub_dir_path, "b.txt");
            let c_path = format!("{}/{}", sub_dir_path, "c.txt");
            write_to_file("Iceberg loves rust.", &a_path);
            write_to_file("Iceberg loves rust.", &b_path);
            write_to_file("Iceberg loves rust.", &c_path);

            let file_io = create_local_file_io();
            assert!(file_io.exists(&a_path).await.unwrap());

            // Remove a file should be no-op.
            file_io.delete_prefix(&a_path).await.unwrap();
            assert!(file_io.exists(&a_path).await.unwrap());

            // Remove a not exist dir should be no-op.
            file_io.delete_prefix("not_exists/").await.unwrap();

            // Remove a dir should remove all files in it.
            file_io.delete_prefix(&sub_dir_path).await.unwrap();
            assert!(!file_io.exists(&b_path).await.unwrap());
            assert!(!file_io.exists(&c_path).await.unwrap());
            assert!(file_io.exists(&a_path).await.unwrap());

            file_io.delete(&a_path).await.unwrap();
            assert!(!file_io.exists(&a_path).await.unwrap());
        }

        #[tokio::test]
        async fn test_delete_non_exist_file() {
            let tmp_dir = TempDir::new().unwrap();

            let file_name = "a.txt";
            let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

            let file_io = create_local_file_io();
            assert!(!file_io.exists(&full_path).await.unwrap());
            assert!(file_io.delete(&full_path).await.is_ok());
            assert!(file_io.delete_prefix(&full_path).await.is_ok());
        }

        #[tokio::test]
        async fn test_local_output_file() {
            let tmp_dir = TempDir::new().unwrap();

            let file_name = "a.txt";
            let content = "Iceberg loves rust.";

            let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

            let file_io = create_local_file_io();
            let output_file = file_io.new_output(&full_path).unwrap();

            assert!(!output_file.exists().await.unwrap());
            {
                output_file.write(content.into()).await.unwrap();
            }

            assert_eq!(&full_path, output_file.location());

            let read_content = read_from_file(full_path).await;

            assert_eq!(content, &read_content);
        }
    }
}
