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
//! Google Cloud Storage properties

use iceberg::io::GcsConfig as IcebergGcsConfig;
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::services::GcsConfig;
use url::Url;

/// Convert iceberg GcsConfig to opendal GcsConfig.
pub(crate) fn gcs_config_to_opendal(iceberg_config: &IcebergGcsConfig) -> GcsConfig {
    let mut cfg = GcsConfig::default();

    cfg.credential = iceberg_config.credential().map(|s| s.to_string());
    cfg.token = iceberg_config.token().map(|s| s.to_string());
    cfg.endpoint = iceberg_config.endpoint().map(|s| s.to_string());
    cfg.allow_anonymous = iceberg_config.allow_anonymous();
    cfg.disable_vm_metadata = iceberg_config.disable_vm_metadata();
    cfg.disable_config_load = iceberg_config.disable_config_load();

    cfg
}

/// Build a new OpenDAL [`Operator`] based on a provided [`GcsConfig`].
pub(crate) fn gcs_config_build(cfg: &GcsConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid gcs url: {path}, bucket is required"),
        )
    })?;

    let mut cfg = cfg.clone();
    cfg.bucket = bucket.to_string();
    Ok(Operator::from_config(cfg)?.finish())
}
