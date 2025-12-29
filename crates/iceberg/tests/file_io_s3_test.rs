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

//! Integration tests for FileIO S3.
#[cfg(all(test, feature = "storage-s3"))]
mod tests {
    use std::net::{IpAddr, SocketAddr};
    use std::sync::RwLock;

    use ctor::{ctor, dtor};
    use iceberg::io::{FileIO, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
    use iceberg_test_utils::docker::DockerCompose;
    use iceberg_test_utils::{normalize_test_name, set_up};

    const MINIO_PORT: u16 = 9000;
    static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);

    #[ctor]
    fn before_all() {
        let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
        let docker_compose = DockerCompose::new(
            normalize_test_name(module_path!()),
            format!("{}/testdata/file_io_s3", env!("CARGO_MANIFEST_DIR")),
        );
        docker_compose.up();
        guard.replace(docker_compose);
    }

    #[dtor]
    fn after_all() {
        let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
        guard.take();
    }

    async fn get_file_io() -> FileIO {
        set_up();

        let container_ip = get_container_ip("minio");
        let minio_socket_addr = SocketAddr::new(container_ip, MINIO_PORT);

        FileIO::from_path("s3://bucket1/").unwrap().with_props(vec![
            (S3_ENDPOINT, format!("http://{minio_socket_addr}")),
            (S3_ACCESS_KEY_ID, "admin".to_string()),
            (S3_SECRET_ACCESS_KEY, "password".to_string()),
            (S3_REGION, "us-east-1".to_string()),
        ])
    }

    fn get_container_ip(service_name: &str) -> IpAddr {
        let guard = DOCKER_COMPOSE_ENV.read().unwrap();
        let docker_compose = guard.as_ref().unwrap();
        docker_compose.get_container_ip(service_name)
    }

    #[tokio::test]
    async fn test_file_io_s3_exists() {
        let file_io = get_file_io().await;
        assert!(!file_io.exists("s3://bucket2/any").await.unwrap());
        assert!(file_io.exists("s3://bucket1/").await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_s3_output() {
        let file_io = get_file_io().await;
        assert!(!file_io.exists("s3://bucket1/test_output").await.unwrap());
        let output_file = file_io.new_output("s3://bucket1/test_output").unwrap();
        {
            output_file.write("123".into()).await.unwrap();
        }
        assert!(file_io.exists("s3://bucket1/test_output").await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_s3_input() {
        let file_io = get_file_io().await;
        let output_file = file_io.new_output("s3://bucket1/test_input").unwrap();
        {
            output_file.write("test_input".into()).await.unwrap();
        }

        let input_file = file_io.new_input("s3://bucket1/test_input").unwrap();

        {
            let buffer = input_file.read().await.unwrap();
            assert_eq!(buffer, "test_input".as_bytes());
        }
    }

    // Note: Tests for custom credential loaders via Extensions have been removed.
    // Custom credential loaders should now be implemented via custom StorageFactory implementations.
    // See the StorageFactory trait for details on how to implement custom storage backends.
}
