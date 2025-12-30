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

use iceberg::io::OssConfig as IcebergOssConfig;
use iceberg::{Error, ErrorKind, Result};
use opendal::services::OssConfig;
use opendal::{Configurator, Operator};
use url::Url;

/// Convert iceberg OssConfig to opendal OssConfig.
pub(crate) fn oss_config_to_opendal(iceberg_config: &IcebergOssConfig) -> OssConfig {
    let mut cfg = OssConfig::default();

    cfg.endpoint = iceberg_config.endpoint.clone();
    cfg.access_key_id = iceberg_config.access_key_id.clone();
    cfg.access_key_secret = iceberg_config.access_key_secret.clone();

    cfg
}

/// Build new opendal operator from give path.
pub(crate) fn oss_config_build(cfg: &OssConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid oss url: {path}, missing bucket"),
        )
    })?;

    let builder = cfg.clone().into_builder().bucket(bucket);

    Ok(Operator::new(builder)?.finish())
}
