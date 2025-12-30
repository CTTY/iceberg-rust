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

//! Amazon S3 storage configuration.
//!
//! This module provides configuration constants and types for Amazon S3 storage.
//! These are based on the [Iceberg S3 FileIO configuration](https://py.iceberg.apache.org/configuration/#s3).

use serde::{Deserialize, Serialize};

use super::StorageConfig;
use crate::io::is_truthy;

/// S3 endpoint URL.
pub const S3_ENDPOINT: &str = "s3.endpoint";
/// S3 access key ID.
pub const S3_ACCESS_KEY_ID: &str = "s3.access-key-id";
/// S3 secret access key.
pub const S3_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
/// S3 session token (required when using temporary credentials).
pub const S3_SESSION_TOKEN: &str = "s3.session-token";
/// S3 region.
pub const S3_REGION: &str = "s3.region";
/// Region to use for the S3 client (takes precedence over [`S3_REGION`]).
pub const CLIENT_REGION: &str = "client.region";
/// S3 Path Style Access.
pub const S3_PATH_STYLE_ACCESS: &str = "s3.path-style-access";
/// S3 Server Side Encryption Type.
pub const S3_SSE_TYPE: &str = "s3.sse.type";
/// S3 Server Side Encryption Key.
/// If S3 encryption type is kms, input is a KMS Key ID.
/// In case this property is not set, default key "aws/s3" is used.
/// If encryption type is custom, input is a custom base-64 AES256 symmetric key.
pub const S3_SSE_KEY: &str = "s3.sse.key";
/// S3 Server Side Encryption MD5.
pub const S3_SSE_MD5: &str = "s3.sse.md5";
/// If set, all AWS clients will assume a role of the given ARN, instead of using the default
/// credential chain.
pub const S3_ASSUME_ROLE_ARN: &str = "client.assume-role.arn";
/// Optional external ID used to assume an IAM role.
pub const S3_ASSUME_ROLE_EXTERNAL_ID: &str = "client.assume-role.external-id";
/// Optional session name used to assume an IAM role.
pub const S3_ASSUME_ROLE_SESSION_NAME: &str = "client.assume-role.session-name";
/// Option to skip signing requests (e.g. for public buckets/folders).
pub const S3_ALLOW_ANONYMOUS: &str = "s3.allow-anonymous";
/// Option to skip loading the credential from EC2 metadata (typically used in conjunction with
/// `S3_ALLOW_ANONYMOUS`).
pub const S3_DISABLE_EC2_METADATA: &str = "s3.disable-ec2-metadata";
/// Option to skip loading configuration from config file and the env.
pub const S3_DISABLE_CONFIG_LOAD: &str = "s3.disable-config-load";

/// Amazon S3 storage configuration.
///
/// This struct contains all the configuration options for connecting to Amazon S3.
/// It can be created from a [`StorageConfig`] using the `From` trait.
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
///
/// use iceberg::io::{S3Config, StorageConfig};
///
/// let storage_config = StorageConfig::new("s3", HashMap::new())
///     .with_prop("s3.region", "us-east-1")
///     .with_prop("s3.access-key-id", "my-access-key")
///     .with_prop("s3.secret-access-key", "my-secret-key");
///
/// let s3_config = S3Config::from(&storage_config);
/// assert_eq!(s3_config.region, Some("us-east-1".to_string()));
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 endpoint URL.
    pub endpoint: Option<String>,
    /// S3 access key ID.
    pub access_key_id: Option<String>,
    /// S3 secret access key.
    pub secret_access_key: Option<String>,
    /// S3 session token.
    pub session_token: Option<String>,
    /// S3 region.
    pub region: Option<String>,
    /// Enable virtual host style (opposite of path style access).
    pub enable_virtual_host_style: bool,
    /// Server side encryption type.
    pub server_side_encryption: Option<String>,
    /// Server side encryption AWS KMS key ID.
    pub server_side_encryption_aws_kms_key_id: Option<String>,
    /// Server side encryption customer algorithm.
    pub server_side_encryption_customer_algorithm: Option<String>,
    /// Server side encryption customer key.
    pub server_side_encryption_customer_key: Option<String>,
    /// Server side encryption customer key MD5.
    pub server_side_encryption_customer_key_md5: Option<String>,
    /// Role ARN for assuming a role.
    pub role_arn: Option<String>,
    /// External ID for assuming a role.
    pub external_id: Option<String>,
    /// Session name for assuming a role.
    pub role_session_name: Option<String>,
    /// Allow anonymous access.
    pub allow_anonymous: bool,
    /// Disable EC2 metadata.
    pub disable_ec2_metadata: bool,
    /// Disable config load.
    pub disable_config_load: bool,
}

impl From<&StorageConfig> for S3Config {
    fn from(config: &StorageConfig) -> Self {
        let props = config.props();
        let mut s3_config = S3Config::default();

        if let Some(endpoint) = props.get(S3_ENDPOINT) {
            s3_config.endpoint = Some(endpoint.clone());
        }
        if let Some(access_key_id) = props.get(S3_ACCESS_KEY_ID) {
            s3_config.access_key_id = Some(access_key_id.clone());
        }
        if let Some(secret_access_key) = props.get(S3_SECRET_ACCESS_KEY) {
            s3_config.secret_access_key = Some(secret_access_key.clone());
        }
        if let Some(session_token) = props.get(S3_SESSION_TOKEN) {
            s3_config.session_token = Some(session_token.clone());
        }
        if let Some(region) = props.get(S3_REGION) {
            s3_config.region = Some(region.clone());
        }
        // CLIENT_REGION takes precedence over S3_REGION
        if let Some(region) = props.get(CLIENT_REGION) {
            s3_config.region = Some(region.clone());
        }
        if let Some(path_style_access) = props.get(S3_PATH_STYLE_ACCESS) {
            s3_config.enable_virtual_host_style =
                !is_truthy(path_style_access.to_lowercase().as_str());
        }
        if let Some(arn) = props.get(S3_ASSUME_ROLE_ARN) {
            s3_config.role_arn = Some(arn.clone());
        }
        if let Some(external_id) = props.get(S3_ASSUME_ROLE_EXTERNAL_ID) {
            s3_config.external_id = Some(external_id.clone());
        }
        if let Some(session_name) = props.get(S3_ASSUME_ROLE_SESSION_NAME) {
            s3_config.role_session_name = Some(session_name.clone());
        }

        // Handle SSE configuration
        let s3_sse_key = props.get(S3_SSE_KEY).cloned();
        if let Some(sse_type) = props.get(S3_SSE_TYPE) {
            match sse_type.to_lowercase().as_str() {
                "none" => {}
                "s3" => {
                    s3_config.server_side_encryption = Some("AES256".to_string());
                }
                "kms" => {
                    s3_config.server_side_encryption = Some("aws:kms".to_string());
                    s3_config.server_side_encryption_aws_kms_key_id = s3_sse_key;
                }
                "custom" => {
                    s3_config.server_side_encryption_customer_algorithm =
                        Some("AES256".to_string());
                    s3_config.server_side_encryption_customer_key = s3_sse_key;
                    s3_config.server_side_encryption_customer_key_md5 =
                        props.get(S3_SSE_MD5).cloned();
                }
                _ => {}
            }
        }

        if let Some(allow_anonymous) = props.get(S3_ALLOW_ANONYMOUS) {
            if is_truthy(allow_anonymous.to_lowercase().as_str()) {
                s3_config.allow_anonymous = true;
            }
        }
        if let Some(disable_ec2_metadata) = props.get(S3_DISABLE_EC2_METADATA) {
            if is_truthy(disable_ec2_metadata.to_lowercase().as_str()) {
                s3_config.disable_ec2_metadata = true;
            }
        }
        if let Some(disable_config_load) = props.get(S3_DISABLE_CONFIG_LOAD) {
            if is_truthy(disable_config_load.to_lowercase().as_str()) {
                s3_config.disable_config_load = true;
            }
        }

        s3_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_from_storage_config() {
        let storage_config = StorageConfig::new("s3", HashMap::new())
            .with_prop(S3_REGION, "us-east-1")
            .with_prop(S3_ACCESS_KEY_ID, "my-access-key")
            .with_prop(S3_SECRET_ACCESS_KEY, "my-secret-key")
            .with_prop(S3_ENDPOINT, "http://localhost:9000");

        let s3_config = S3Config::from(&storage_config);

        assert_eq!(s3_config.region, Some("us-east-1".to_string()));
        assert_eq!(s3_config.access_key_id, Some("my-access-key".to_string()));
        assert_eq!(
            s3_config.secret_access_key,
            Some("my-secret-key".to_string())
        );
        assert_eq!(
            s3_config.endpoint,
            Some("http://localhost:9000".to_string())
        );
    }

    #[test]
    fn test_s3_config_client_region_precedence() {
        let storage_config = StorageConfig::new("s3", HashMap::new())
            .with_prop(S3_REGION, "us-east-1")
            .with_prop(CLIENT_REGION, "eu-west-1");

        let s3_config = S3Config::from(&storage_config);

        // CLIENT_REGION should take precedence
        assert_eq!(s3_config.region, Some("eu-west-1".to_string()));
    }

    #[test]
    fn test_s3_config_path_style_access() {
        let storage_config =
            StorageConfig::new("s3", HashMap::new()).with_prop(S3_PATH_STYLE_ACCESS, "true");

        let s3_config = S3Config::from(&storage_config);

        // path style access = true means virtual host style = false
        assert!(!s3_config.enable_virtual_host_style);
    }

    #[test]
    fn test_s3_config_sse_kms() {
        let storage_config = StorageConfig::new("s3", HashMap::new())
            .with_prop(S3_SSE_TYPE, "kms")
            .with_prop(S3_SSE_KEY, "my-kms-key-id");

        let s3_config = S3Config::from(&storage_config);

        assert_eq!(
            s3_config.server_side_encryption,
            Some("aws:kms".to_string())
        );
        assert_eq!(
            s3_config.server_side_encryption_aws_kms_key_id,
            Some("my-kms-key-id".to_string())
        );
    }

    #[test]
    fn test_s3_config_allow_anonymous() {
        let storage_config =
            StorageConfig::new("s3", HashMap::new()).with_prop(S3_ALLOW_ANONYMOUS, "true");

        let s3_config = S3Config::from(&storage_config);

        assert!(s3_config.allow_anonymous);
    }
}
