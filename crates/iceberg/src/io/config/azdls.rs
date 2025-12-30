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

//! Azure Data Lake Storage configuration.
//!
//! This module provides configuration constants and types for Azure Data Lake Storage.

use serde::{Deserialize, Serialize};

use super::StorageConfig;

/// A connection string.
///
/// Note, this string is parsed first, and any other passed adls.* properties
/// will override values from the connection string.
pub const ADLS_CONNECTION_STRING: &str = "adls.connection-string";
/// The account that you want to connect to.
pub const ADLS_ACCOUNT_NAME: &str = "adls.account-name";
/// The key to authentication against the account.
pub const ADLS_ACCOUNT_KEY: &str = "adls.account-key";
/// The shared access signature.
pub const ADLS_SAS_TOKEN: &str = "adls.sas-token";
/// The tenant-id.
pub const ADLS_TENANT_ID: &str = "adls.tenant-id";
/// The client-id.
pub const ADLS_CLIENT_ID: &str = "adls.client-id";
/// The client-secret.
pub const ADLS_CLIENT_SECRET: &str = "adls.client-secret";
/// The authority host of the service principal.
/// - required for client_credentials authentication
/// - default value: `https://login.microsoftonline.com`
pub const ADLS_AUTHORITY_HOST: &str = "adls.authority-host";

/// Azure Data Lake Storage configuration.
///
/// This struct contains all the configuration options for connecting to Azure Data Lake Storage.
/// It can be created from a [`StorageConfig`] using the `From` trait.
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
///
/// use iceberg::io::{AzdlsConfig, StorageConfig};
///
/// let storage_config = StorageConfig::new("abfss", HashMap::new())
///     .with_prop("adls.account-name", "myaccount")
///     .with_prop("adls.account-key", "my-account-key");
///
/// let azdls_config = AzdlsConfig::from(&storage_config);
/// assert_eq!(azdls_config.account_name, Some("myaccount".to_string()));
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AzdlsConfig {
    /// Connection string.
    pub connection_string: Option<String>,
    /// Account name.
    pub account_name: Option<String>,
    /// Account key.
    pub account_key: Option<String>,
    /// SAS token.
    pub sas_token: Option<String>,
    /// Tenant ID.
    pub tenant_id: Option<String>,
    /// Client ID.
    pub client_id: Option<String>,
    /// Client secret.
    pub client_secret: Option<String>,
    /// Authority host.
    pub authority_host: Option<String>,
    /// Endpoint URL.
    pub endpoint: Option<String>,
    /// Filesystem name.
    pub filesystem: String,
}

impl From<&StorageConfig> for AzdlsConfig {
    fn from(config: &StorageConfig) -> Self {
        let props = config.props();
        let mut azdls_config = AzdlsConfig::default();

        if let Some(connection_string) = props.get(ADLS_CONNECTION_STRING) {
            azdls_config.connection_string = Some(connection_string.clone());
        }
        if let Some(account_name) = props.get(ADLS_ACCOUNT_NAME) {
            azdls_config.account_name = Some(account_name.clone());
        }
        if let Some(account_key) = props.get(ADLS_ACCOUNT_KEY) {
            azdls_config.account_key = Some(account_key.clone());
        }
        if let Some(sas_token) = props.get(ADLS_SAS_TOKEN) {
            azdls_config.sas_token = Some(sas_token.clone());
        }
        if let Some(tenant_id) = props.get(ADLS_TENANT_ID) {
            azdls_config.tenant_id = Some(tenant_id.clone());
        }
        if let Some(client_id) = props.get(ADLS_CLIENT_ID) {
            azdls_config.client_id = Some(client_id.clone());
        }
        if let Some(client_secret) = props.get(ADLS_CLIENT_SECRET) {
            azdls_config.client_secret = Some(client_secret.clone());
        }
        if let Some(authority_host) = props.get(ADLS_AUTHORITY_HOST) {
            azdls_config.authority_host = Some(authority_host.clone());
        }

        azdls_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_azdls_config_from_storage_config() {
        let storage_config = StorageConfig::new("abfss", HashMap::new())
            .with_prop(ADLS_ACCOUNT_NAME, "myaccount")
            .with_prop(ADLS_ACCOUNT_KEY, "my-account-key");

        let azdls_config = AzdlsConfig::from(&storage_config);

        assert_eq!(azdls_config.account_name, Some("myaccount".to_string()));
        assert_eq!(azdls_config.account_key, Some("my-account-key".to_string()));
    }

    #[test]
    fn test_azdls_config_with_sas_token() {
        let storage_config = StorageConfig::new("abfss", HashMap::new())
            .with_prop(ADLS_ACCOUNT_NAME, "myaccount")
            .with_prop(ADLS_SAS_TOKEN, "my-sas-token");

        let azdls_config = AzdlsConfig::from(&storage_config);

        assert_eq!(azdls_config.account_name, Some("myaccount".to_string()));
        assert_eq!(azdls_config.sas_token, Some("my-sas-token".to_string()));
    }

    #[test]
    fn test_azdls_config_with_client_credentials() {
        let storage_config = StorageConfig::new("abfss", HashMap::new())
            .with_prop(ADLS_ACCOUNT_NAME, "myaccount")
            .with_prop(ADLS_TENANT_ID, "my-tenant")
            .with_prop(ADLS_CLIENT_ID, "my-client")
            .with_prop(ADLS_CLIENT_SECRET, "my-secret");

        let azdls_config = AzdlsConfig::from(&storage_config);

        assert_eq!(azdls_config.account_name, Some("myaccount".to_string()));
        assert_eq!(azdls_config.tenant_id, Some("my-tenant".to_string()));
        assert_eq!(azdls_config.client_id, Some("my-client".to_string()));
        assert_eq!(azdls_config.client_secret, Some("my-secret".to_string()));
    }
}
