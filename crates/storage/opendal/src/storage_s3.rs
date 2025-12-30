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

use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::S3Config as IcebergS3Config;
use iceberg::{Error, ErrorKind, Result};
use opendal::services::S3Config;
use opendal::{Configurator, Operator};
pub use reqsign::{AwsCredential, AwsCredentialLoad};
use reqwest::Client;
use url::Url;

/// Convert iceberg S3Config to opendal S3Config.
pub(crate) fn s3_config_to_opendal(iceberg_config: &IcebergS3Config) -> S3Config {
    let mut cfg = S3Config::default();

    cfg.endpoint = iceberg_config.endpoint.clone();
    cfg.access_key_id = iceberg_config.access_key_id.clone();
    cfg.secret_access_key = iceberg_config.secret_access_key.clone();
    cfg.session_token = iceberg_config.session_token.clone();
    cfg.region = iceberg_config.region.clone();
    cfg.enable_virtual_host_style = iceberg_config.enable_virtual_host_style;
    cfg.server_side_encryption = iceberg_config.server_side_encryption.clone();
    cfg.server_side_encryption_aws_kms_key_id =
        iceberg_config.server_side_encryption_aws_kms_key_id.clone();
    cfg.server_side_encryption_customer_algorithm = iceberg_config
        .server_side_encryption_customer_algorithm
        .clone();
    cfg.server_side_encryption_customer_key =
        iceberg_config.server_side_encryption_customer_key.clone();
    cfg.server_side_encryption_customer_key_md5 = iceberg_config
        .server_side_encryption_customer_key_md5
        .clone();
    cfg.role_arn = iceberg_config.role_arn.clone();
    cfg.external_id = iceberg_config.external_id.clone();
    cfg.role_session_name = iceberg_config.role_session_name.clone();
    cfg.allow_anonymous = iceberg_config.allow_anonymous;
    cfg.disable_ec2_metadata = iceberg_config.disable_ec2_metadata;
    cfg.disable_config_load = iceberg_config.disable_config_load;

    cfg
}

/// Build new opendal operator from give path.
pub(crate) fn s3_config_build(
    cfg: &S3Config,
    customized_credential_load: &Option<CustomAwsCredentialLoader>,
    path: &str,
) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid s3 url: {path}, missing bucket"),
        )
    })?;

    let mut builder = cfg
        .clone()
        .into_builder()
        // Set bucket name.
        .bucket(bucket);

    if let Some(customized_credential_load) = customized_credential_load {
        builder = builder
            .customized_credential_load(customized_credential_load.clone().into_opendal_loader());
    }

    Ok(Operator::new(builder)?.finish())
}

/// Custom AWS credential loader.
/// This can be used to load credentials from a custom source, such as the AWS SDK.
///
/// This should be used within a custom `StorageFactory` implementation.
#[derive(Clone)]
pub struct CustomAwsCredentialLoader(Arc<dyn AwsCredentialLoad>);

impl std::fmt::Debug for CustomAwsCredentialLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomAwsCredentialLoader")
            .finish_non_exhaustive()
    }
}

impl CustomAwsCredentialLoader {
    /// Create a new custom AWS credential loader.
    pub fn new(loader: Arc<dyn AwsCredentialLoad>) -> Self {
        Self(loader)
    }

    /// Convert this loader into an opendal compatible loader for customized AWS credentials.
    pub fn into_opendal_loader(self) -> Box<dyn AwsCredentialLoad> {
        Box::new(self)
    }
}

#[async_trait]
impl AwsCredentialLoad for CustomAwsCredentialLoader {
    async fn load_credential(&self, client: Client) -> anyhow::Result<Option<AwsCredential>> {
        self.0.load_credential(client).await
    }
}
