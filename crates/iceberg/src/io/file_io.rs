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

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

pub use super::storage::Storage;
use super::{LocalFsStorageFactory, MemoryStorageFactory, StorageConfig, StorageFactory};
use crate::{Error, ErrorKind, Result};

/// FileIO implementation, used to manipulate files in underlying storage.
///
/// # Note
///
/// All path passed to `FileIO` must be absolute path starting with scheme string used to construct `FileIO`.
/// For example, if you construct `FileIO` with `s3a` scheme, then all path passed to `FileIO` must start with `s3a://`.
///
/// Supported storages:
///
/// | Storage            | Feature Flag      | Expected Path Format             | Schemes                       |
/// |--------------------|-------------------|----------------------------------| ------------------------------|
/// | Local file system  | `storage-fs`      | `file`                           | `file://path/to/file`         |
/// | Memory             | `storage-memory`  | `memory`                         | `memory://path/to/file`       |
/// | S3                 | `storage-s3`      | `s3`, `s3a`                      | `s3://<bucket>/path/to/file`  |
/// | GCS                | `storage-gcs`     | `gs`, `gcs`                      | `gs://<bucket>/path/to/file`  |
/// | OSS                | `storage-oss`     | `oss`                            | `oss://<bucket>/path/to/file` |
/// | Azure Datalake     | `storage-azdls`   | `abfs`, `abfss`, `wasb`, `wasbs` | `abfs://<filesystem>@<account>.dfs.core.windows.net/path/to/file` or `wasb://<container>@<account>.blob.core.windows.net/path/to/file` |
#[derive(Clone)]
pub struct FileIO {
    /// Storage configuration containing scheme and properties
    config: StorageConfig,
    /// Factory for creating storage instances
    factory: Arc<dyn StorageFactory>,
    /// Cached storage instance (lazily initialized)
    storage: Arc<OnceCell<Arc<dyn Storage>>>,
}

/// Default storage factory that supports "memory" and "file" schemes.
///
/// This factory is used by default when creating a `FileIO` without specifying
/// a custom storage factory. It supports:
/// - `memory` scheme: In-memory storage for testing
/// - `file` scheme: Local filesystem storage
///
/// For cloud storage (S3, GCS, Azure, etc.), use `iceberg-storage-opendal` crate
/// and provide a custom storage factory via `FileIO::with_storage_factory()`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DefaultStorageFactory;

#[typetag::serde]
impl StorageFactory for DefaultStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        match config.scheme() {
            "memory" => MemoryStorageFactory.build(config),
            "file" => LocalFsStorageFactory.build(config),
            scheme => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "DefaultStorageFactory only supports 'memory' and 'file' schemes, got '{}'. \
                     For cloud storage (S3, GCS, Azure, etc.), use iceberg-storage-opendal crate.",
                    scheme
                ),
            )),
        }
    }
}

impl Debug for FileIO {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileIO")
            .field("config", &self.config)
            .field("factory", &self.factory)
            .field("storage_initialized", &self.storage.get().is_some())
            .finish()
    }
}

impl FileIO {
    /// Create a new FileIO with the given configuration.
    ///
    /// The storage instance is lazily initialized on first access.
    ///
    /// # Note
    ///
    /// By default, this uses `DefaultStorageFactory` which supports "memory" and "file" schemes.
    /// For cloud storage (S3, GCS, Azure, etc.), you must provide a custom storage factory
    /// using `with_storage_factory()`. The `iceberg-storage-opendal` crate provides
    /// `OpenDalStorageFactory` for cloud storage support.
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration containing scheme and properties
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::collections::HashMap;
    ///
    /// use iceberg::io::{FileIO, StorageConfig};
    ///
    /// let config = StorageConfig::new("memory", HashMap::new());
    /// let file_io = FileIO::new(config);
    /// ```
    pub fn new(config: StorageConfig) -> Self {
        Self {
            config,
            factory: Arc::new(DefaultStorageFactory),
            storage: Arc::new(OnceCell::new()),
        }
    }

    /// Create a new FileIO backed by in-memory storage.
    ///
    /// This is a convenience method for testing and scenarios where
    /// persistent storage is not needed. The returned FileIO uses
    /// `MemoryStorage` which stores all data in a thread-safe HashMap.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::io::FileIO;
    ///
    /// let file_io = FileIO::new_with_memory();
    /// ```
    pub fn new_with_memory() -> Self {
        Self {
            config: StorageConfig::new("memory", HashMap::new()),
            factory: Arc::new(MemoryStorageFactory),
            storage: Arc::new(OnceCell::new()),
        }
    }

    /// Create FileIO from a path, inferring the scheme.
    ///
    /// # Arguments
    ///
    /// * `path` - A path or URL from which to infer the scheme
    ///
    /// # Returns
    ///
    /// A `Result` containing the `FileIO` with the inferred scheme,
    /// or an error if the path is invalid.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use iceberg::io::FileIO;
    ///
    /// let file_io = FileIO::from_path("s3://bucket/path")?
    ///     .with_prop("region", "us-east-1");
    /// ```
    pub fn from_path(path: impl AsRef<str>) -> Result<Self> {
        let config = StorageConfig::from_path(path)?;
        Ok(Self::new(config))
    }

    /// Set a custom storage factory.
    ///
    /// This allows users to provide custom storage implementations.
    ///
    /// # Arguments
    ///
    /// * `factory` - The storage factory to use for creating storage instances
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    /// use iceberg::io::{FileIO, StorageFactory};
    ///
    /// let custom_factory: Arc<dyn StorageFactory> = /* ... */;
    /// let file_io = FileIO::from_path("s3://bucket/path")?
    ///     .with_storage_factory(custom_factory);
    /// ```
    pub fn with_storage_factory(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.factory = factory;
        self
    }

    /// Add a configuration property.
    ///
    /// This is a builder-style method that returns `self` for chaining.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key
    /// * `value` - The property value
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use iceberg::io::FileIO;
    ///
    /// let file_io = FileIO::from_path("s3://bucket/path")?
    ///     .with_prop("region", "us-east-1")
    ///     .with_prop("access_key_id", "my-key");
    /// ```
    pub fn with_prop(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config = self.config.with_prop(key, value);
        self
    }

    /// Add multiple configuration properties.
    ///
    /// This is a builder-style method that returns `self` for chaining.
    ///
    /// # Arguments
    ///
    /// * `props` - An iterator of key-value pairs to add
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use iceberg::io::FileIO;
    ///
    /// let props = vec![
    ///     ("region", "us-east-1"),
    ///     ("access_key_id", "my-key"),
    /// ];
    /// let file_io = FileIO::from_path("s3://bucket/path")?
    ///     .with_props(props);
    /// ```
    pub fn with_props(
        mut self,
        props: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.config = self.config.with_props(props);
        self
    }

    /// Get the storage configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Get or create the storage instance.
    ///
    /// This method lazily initializes the storage on first access and caches
    /// the result for subsequent calls.
    fn get_storage(&self) -> Result<&Arc<dyn Storage>> {
        self.storage
            .get_or_try_init(|| self.factory.build(&self.config))
    }

    /// Deletes file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        self.get_storage()?.delete(path.as_ref()).await
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    ///
    /// # Behavior
    ///
    /// - If the path is a file or not exist, this function will be no-op.
    /// - If the path is a empty directory, this function will remove the directory itself.
    /// - If the path is a non-empty directory, this function will remove the directory and all nested files and directories.
    pub async fn delete_prefix(&self, path: impl AsRef<str>) -> Result<()> {
        self.get_storage()?.delete_prefix(path.as_ref()).await
    }

    /// Check file exists.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        self.get_storage()?.exists(path.as_ref()).await
    }

    /// Creates input file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        self.get_storage()?.new_input(path.as_ref())
    }

    /// Creates output file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        self.get_storage()?.new_output(path.as_ref())
    }
}

/// The struct the represents the metadata of a file.
///
/// TODO: we can add last modified time, content type, etc. in the future.
pub struct FileMetadata {
    /// The size of the file.
    pub size: u64,
}

/// Trait for reading file.
///
/// # TODO
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileRead: Send + Sync + Unpin + 'static {
    /// Read file content with given range.
    ///
    /// TODO: we can support reading non-contiguous bytes in the future.
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes>;
}

#[async_trait::async_trait]
impl FileRead for opendal::Reader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(self, range).await?.to_bytes())
    }
}

/// Input file is used for reading from files.
#[derive(Debug)]
pub struct InputFile {
    storage: Arc<dyn Storage>,
    path: String,
}

impl InputFile {
    /// Creates a new input file.
    ///
    /// # Arguments
    ///
    /// * `storage` - The storage backend to use
    /// * `path` - Absolute path to the file
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self {
        Self { storage, path }
    }

    /// Absolute path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Check if file exists.
    pub async fn exists(&self) -> Result<bool> {
        self.storage.exists(&self.path).await
    }

    /// Fetch and returns metadata of file.
    pub async fn metadata(&self) -> Result<FileMetadata> {
        self.storage.metadata(&self.path).await
    }

    /// Read and returns whole content of file.
    ///
    /// For continuous reading, use [`Self::reader`] instead.
    pub async fn read(&self) -> Result<Bytes> {
        self.storage.read(&self.path).await
    }

    /// Creates [`FileRead`] for continuous reading.
    ///
    /// For one-time reading, use [`Self::read`] instead.
    pub async fn reader(&self) -> Result<Box<dyn FileRead>> {
        self.storage.reader(&self.path).await
    }
}

/// Trait for writing file.
///
/// # TODO
///
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileWrite: Send + Sync + Unpin + 'static {
    /// Write bytes to file.
    ///
    /// TODO: we can support writing non-contiguous bytes in the future.
    async fn write(&mut self, bs: Bytes) -> Result<()>;

    /// Close file.
    ///
    /// Calling close on closed file will generate an error.
    async fn close(&mut self) -> Result<()>;
}

#[async_trait::async_trait]
impl FileWrite for opendal::Writer {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        Ok(opendal::Writer::write(self, bs).await?)
    }

    async fn close(&mut self) -> crate::Result<()> {
        let _ = opendal::Writer::close(self).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl FileWrite for Box<dyn FileWrite> {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        self.as_mut().write(bs).await
    }

    async fn close(&mut self) -> crate::Result<()> {
        self.as_mut().close().await
    }
}

/// Output file is used for writing to files.
#[derive(Debug)]
pub struct OutputFile {
    storage: Arc<dyn Storage>,
    path: String,
}

impl OutputFile {
    /// Creates a new output file.
    ///
    /// # Arguments
    ///
    /// * `storage` - The storage backend to use
    /// * `path` - Absolute path to the file
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self {
        Self { storage, path }
    }

    /// Relative path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Checks if file exists.
    pub async fn exists(&self) -> Result<bool> {
        self.storage.exists(&self.path).await
    }

    /// Deletes file.
    ///
    /// If the file does not exist, it will not return error.
    pub async fn delete(&self) -> Result<()> {
        self.storage.delete(&self.path).await
    }

    /// Converts into [`InputFile`].
    pub fn to_input_file(self) -> InputFile {
        InputFile::new(self.storage, self.path)
    }

    /// Create a new output file with given bytes.
    ///
    /// # Notes
    ///
    /// Calling `write` will overwrite the file if it exists.
    /// For continuous writing, use [`Self::writer`].
    pub async fn write(&self, bs: Bytes) -> crate::Result<()> {
        self.storage.write(self.path.as_str(), bs).await
    }

    /// Creates output file for continuous writing.
    ///
    /// # Notes
    ///
    /// For one-time writing, use [`Self::write`] instead.
    pub async fn writer(&self) -> crate::Result<Box<dyn FileWrite>> {
        Ok(Box::new(self.storage.writer(&self.path).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::FileIO;

    #[test]
    fn test_from_path() {
        let io = FileIO::from_path("/tmp/a").unwrap();
        assert_eq!("file", io.config().scheme());

        let io = FileIO::from_path("file:/tmp/b").unwrap();
        assert_eq!("file", io.config().scheme());

        let io = FileIO::from_path("file:///tmp/c").unwrap();
        assert_eq!("file", io.config().scheme());

        let io = FileIO::from_path("s3://bucket/a").unwrap();
        assert_eq!("s3", io.config().scheme());

        let io = FileIO::from_path("tmp/||c");
        assert!(io.is_err());
    }

    #[test]
    fn test_with_prop() {
        let io = FileIO::from_path("s3://bucket/path")
            .unwrap()
            .with_prop("region", "us-east-1")
            .with_prop("access_key_id", "my-key");

        assert_eq!(io.config().get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(
            io.config().get("access_key_id"),
            Some(&"my-key".to_string())
        );
    }

    #[test]
    fn test_with_props() {
        let props = vec![("region", "us-east-1"), ("access_key_id", "my-key")];
        let io = FileIO::from_path("s3://bucket/path")
            .unwrap()
            .with_props(props);

        assert_eq!(io.config().get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(
            io.config().get("access_key_id"),
            Some(&"my-key".to_string())
        );
    }

    #[test]
    fn test_new_with_memory() {
        let io = FileIO::new_with_memory();
        assert_eq!("memory", io.config().scheme());
    }

    #[tokio::test]
    async fn test_new_with_memory_write_read() {
        let file_io = FileIO::new_with_memory();
        let path = "memory://test/file.txt";
        let content = "Hello, World!";

        // Write
        let output_file = file_io.new_output(path).unwrap();
        output_file.write(content.into()).await.unwrap();

        // Read
        let input_file = file_io.new_input(path).unwrap();
        let read_content = input_file.read().await.unwrap();
        assert_eq!(read_content, bytes::Bytes::from(content));
    }

    #[tokio::test]
    async fn test_new_with_memory_exists_delete() {
        let file_io = FileIO::new_with_memory();
        let path = "memory://test/file.txt";

        // File doesn't exist initially
        assert!(!file_io.exists(path).await.unwrap());

        // Write file
        let output_file = file_io.new_output(path).unwrap();
        output_file.write("test".into()).await.unwrap();

        // File exists now
        assert!(file_io.exists(path).await.unwrap());

        // Delete file
        file_io.delete(path).await.unwrap();
        assert!(!file_io.exists(path).await.unwrap());
    }
}
