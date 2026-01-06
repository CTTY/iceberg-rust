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
//! use std::sync::Arc;
//! use iceberg::io::{FileIOBuilder, StorageConfig};
//! use iceberg_storage_opendal::OpenDalStorageFactory;
//!
//! let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3))
//!     .with_prop("s3.region", "us-east-1")
//!     .build()?;
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
// Re-export storage-specific configuration constants from iceberg crate
// These are commonly used by catalog implementations
pub use iceberg::io::{
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
};
pub use iceberg::io::{Storage, StorageConfig, StorageFactory};
pub use storage::{OpenDalStorage, OpenDalStorageFactory};
// Re-export Azure storage scheme
#[cfg(feature = "storage-azdls")]
pub use storage_azdls::AzureStorageScheme;
// Re-export S3 credential types
#[cfg(feature = "storage-s3")]
pub use storage_s3::{AwsCredential, AwsCredentialLoad, CustomAwsCredentialLoader};
