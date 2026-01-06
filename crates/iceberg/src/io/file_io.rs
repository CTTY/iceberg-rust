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

use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use once_cell::sync::OnceCell;

pub use super::storage::Storage;
use super::{LocalFsStorageFactory, MemoryStorageFactory, StorageConfig, StorageFactory};
use crate::Result;

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
    /// Storage configuration containing properties
    config: StorageConfig,
    /// Factory for creating storage instances
    factory: Arc<dyn StorageFactory>,
    /// Cached storage instance (lazily initialized)
    storage: Arc<OnceCell<Arc<dyn Storage>>>,
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
    /// Convert this FileIO into a FileIOBuilder for modification.
    ///
    /// This allows creating a new FileIO with modified configuration
    /// while preserving the storage factory.
    pub fn into_builder(self) -> FileIOBuilder {
        FileIOBuilder {
            factory: self.factory,
            config: self.config,
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
            config: StorageConfig::new(),
            factory: Arc::new(MemoryStorageFactory),
            storage: Arc::new(OnceCell::new()),
        }
    }

    /// Create a new FileIO backed by local filesystem storage.
    ///
    /// This is a convenience method for local filesystem scenarios.
    /// This uses the built-in `LocalFsStorageFactory` from the iceberg crate,
    /// which is always available without any feature flags.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::io::FileIO;
    ///
    /// let file_io = FileIO::new_with_fs();
    /// ```
    pub fn new_with_fs() -> Self {
        Self {
            config: StorageConfig::new(),
            factory: Arc::new(LocalFsStorageFactory),
            storage: Arc::new(OnceCell::new()),
        }
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

/// Builder for creating FileIO instances.
///
/// This builder allows configuring a FileIO with a storage factory and
/// configuration properties before building the final instance.
pub struct FileIOBuilder {
    factory: Arc<dyn StorageFactory>,
    config: StorageConfig,
}

impl FileIOBuilder {
    /// Create a new FileIOBuilder with the given storage factory.
    ///
    /// # Arguments
    ///
    /// * `factory` - The storage factory to use for creating storage instances
    pub fn new(factory: Arc<dyn StorageFactory>) -> Self {
        Self {
            factory,
            config: StorageConfig::new(),
        }
    }

    /// Add a configuration property.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key
    /// * `value` - The property value
    pub fn with_prop(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config = self.config.with_prop(key, value);
        self
    }

    /// Add multiple configuration properties.
    ///
    /// # Arguments
    ///
    /// * `props` - An iterator of key-value pairs to add
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

    /// Build the FileIO instance.
    ///
    /// This creates a new FileIO with the configured factory and properties.
    /// The storage instance is lazily initialized on first access.
    pub fn build(self) -> Result<FileIO> {
        Ok(FileIO {
            config: self.config,
            factory: self.factory,
            storage: Arc::new(OnceCell::new()),
        })
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
    use std::sync::Arc;

    use super::{FileIO, FileIOBuilder};
    use crate::io::MemoryStorageFactory;

    #[test]
    fn test_builder_with_explicit_factory() {
        let factory = Arc::new(MemoryStorageFactory);
        let builder = FileIOBuilder::new(factory);
        assert!(builder.config().props().is_empty());
    }

    #[test]
    fn test_builder_with_explicit_factory_and_props() {
        let factory = Arc::new(MemoryStorageFactory);
        let builder = FileIOBuilder::new(factory)
            .with_prop("key1", "value1")
            .with_prop("key2", "value2");

        assert_eq!(builder.config().get("key1"), Some(&"value1".to_string()));
        assert_eq!(builder.config().get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_new_with_memory() {
        let io = FileIO::new_with_memory();
        // FileIO::new_with_memory() creates a FileIO with empty config
        // We can verify it works by using it
        assert!(io.into_builder().config().props().is_empty());
    }

    #[test]
    fn test_new_with_fs() {
        let io = FileIO::new_with_fs();
        // FileIO::new_with_fs() creates a FileIO with empty config
        // We can verify it works by using it
        assert!(io.into_builder().config().props().is_empty());
    }

    #[test]
    fn test_into_builder() {
        let factory = Arc::new(MemoryStorageFactory);
        let io = FileIOBuilder::new(factory)
            .with_prop("key1", "value1")
            .build()
            .unwrap();
        let builder = io.into_builder().with_prop("key2", "value2");

        assert_eq!(builder.config().get("key1"), Some(&"value1".to_string()));
        assert_eq!(builder.config().get("key2"), Some(&"value2".to_string()));
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

    #[tokio::test]
    async fn test_new_with_fs_write_read() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let path = tmp_dir.path().join("test.txt");
        let path_str = path.to_str().unwrap();
        let content = "Hello from FileIO::new_with_fs!";

        // Write
        let output_file = file_io.new_output(path_str).unwrap();
        output_file.write(content.into()).await.unwrap();

        // Read
        let input_file = file_io.new_input(path_str).unwrap();
        let read_content = input_file.read().await.unwrap();
        assert_eq!(read_content, bytes::Bytes::from(content));
    }

    #[tokio::test]
    async fn test_builder_write_read() {
        let factory = Arc::new(MemoryStorageFactory);
        let file_io = FileIOBuilder::new(factory).build().unwrap();
        let path = "memory://explicit/test.txt";
        let content = "Hello from explicit factory!";

        // Write
        let output_file = file_io.new_output(path).unwrap();
        output_file.write(content.into()).await.unwrap();

        // Read
        let input_file = file_io.new_input(path).unwrap();
        let read_content = input_file.read().await.unwrap();
        assert_eq!(read_content, bytes::Bytes::from(content));
    }
}
