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

//! Alibaba Cloud OSS storage configuration.
//!
//! This module provides configuration constants and types for Alibaba Cloud OSS storage.

use serde::{Deserialize, Serialize};

use super::StorageConfig;

/// Aliyun OSS endpoint.
pub const OSS_ENDPOINT: &str = "oss.endpoint";
/// Aliyun OSS access key ID.
pub const OSS_ACCESS_KEY_ID: &str = "oss.access-key-id";
/// Aliyun OSS access key secret.
pub const OSS_ACCESS_KEY_SECRET: &str = "oss.access-key-secret";

/// Alibaba Cloud OSS storage configuration.
///
/// This struct contains all the configuration options for connecting to Alibaba Cloud OSS.
/// It can be created from a [`StorageConfig`] using the `From` trait.
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
///
/// use iceberg::io::{OssConfig, StorageConfig};
///
/// let storage_config = StorageConfig::new("oss", HashMap::new())
///     .with_prop("oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com")
///     .with_prop("oss.access-key-id", "my-access-key")
///     .with_prop("oss.access-key-secret", "my-secret-key");
///
/// let oss_config = OssConfig::from(&storage_config);
/// assert_eq!(
///     oss_config.endpoint,
///     Some("https://oss-cn-hangzhou.aliyuncs.com".to_string())
/// );
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OssConfig {
    /// OSS endpoint URL.
    pub endpoint: Option<String>,
    /// OSS access key ID.
    pub access_key_id: Option<String>,
    /// OSS access key secret.
    pub access_key_secret: Option<String>,
}

impl From<&StorageConfig> for OssConfig {
    fn from(config: &StorageConfig) -> Self {
        let props = config.props();
        let mut oss_config = OssConfig::default();

        if let Some(endpoint) = props.get(OSS_ENDPOINT) {
            oss_config.endpoint = Some(endpoint.clone());
        }
        if let Some(access_key_id) = props.get(OSS_ACCESS_KEY_ID) {
            oss_config.access_key_id = Some(access_key_id.clone());
        }
        if let Some(access_key_secret) = props.get(OSS_ACCESS_KEY_SECRET) {
            oss_config.access_key_secret = Some(access_key_secret.clone());
        }

        oss_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oss_config_from_storage_config() {
        let storage_config = StorageConfig::new("oss", HashMap::new())
            .with_prop(OSS_ENDPOINT, "https://oss-cn-hangzhou.aliyuncs.com")
            .with_prop(OSS_ACCESS_KEY_ID, "my-access-key")
            .with_prop(OSS_ACCESS_KEY_SECRET, "my-secret-key");

        let oss_config = OssConfig::from(&storage_config);

        assert_eq!(
            oss_config.endpoint,
            Some("https://oss-cn-hangzhou.aliyuncs.com".to_string())
        );
        assert_eq!(oss_config.access_key_id, Some("my-access-key".to_string()));
        assert_eq!(
            oss_config.access_key_secret,
            Some("my-secret-key".to_string())
        );
    }

    #[test]
    fn test_oss_config_empty() {
        let storage_config = StorageConfig::new("oss", HashMap::new());

        let oss_config = OssConfig::from(&storage_config);

        assert_eq!(oss_config.endpoint, None);
        assert_eq!(oss_config.access_key_id, None);
        assert_eq!(oss_config.access_key_secret, None);
    }
}
