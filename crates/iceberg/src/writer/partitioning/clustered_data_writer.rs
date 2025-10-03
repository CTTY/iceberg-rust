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

//! This module provides the `ClusteredWriter` implementation.

use std::collections::HashSet;
use arrow_array::RecordBatch;
use async_trait::async_trait;

use crate::spec::{DataFile, PartitionKey, Struct};
use crate::writer::partitioning::PartitioningWriter;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// A writer that writes data to a single partition at a time.
/// TODO maybe DefaultClusteredWriter is a better name?
#[derive(Clone)]
pub struct ClusteredDataWriter<B: IcebergWriterBuilder> {
    inner_builder: B,
    current_writer: Option<B::R>,
    current_partition: Option<Struct>,
    closed_partitions: HashSet<Struct>,
    output: Vec<DataFile>,
}

impl<B: IcebergWriterBuilder> ClusteredDataWriter<B> {
    /// Create a new `ClusteredWriter`.
    pub fn new(inner_builder: B) -> Self {
        Self {
            inner_builder,
            current_writer: None,
            current_partition: None,
            closed_partitions: HashSet::new(),
            output: Vec::new(),
        }
    }

    /// Closes the current writer if it exists, flushes the written data to output, and record closed partition.
    async fn close_current_writer(&mut self) -> Result<()> {
        if let Some(mut writer) = self.current_writer.take() {
            self.output.extend(writer.close().await?);

            // Add the current partition to the set of closed partitions
            if let Some(current_partition) = self.current_partition.take() {
                self.closed_partitions.insert(current_partition);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<B: IcebergWriterBuilder> PartitioningWriter for ClusteredDataWriter<B> {
    async fn write(&mut self, partition_key: Option<PartitionKey>, input: RecordBatch) -> Result<()> {
        if let Some(partition_key) = partition_key {
            let partition_value = partition_key.data();

            // Check if this partition has been closed already
            if self.closed_partitions.contains(partition_value) {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "The input is not sorted! Cannot write to partition that was previously closed: {:?}",
                        partition_key
                    ),
                ));
            }

            // Check if we need to switch to a new partition
            let need_new_writer = match &self.current_partition {
                Some(current) => current != partition_value,
                None => true,
            };

            if need_new_writer {
                self.close_current_writer().await?;

                // Create a new writer for the new partition
                self.current_writer = Some(self.inner_builder.clone().build().await?);
                self.current_partition = Some(partition_value.clone());
            }
        } else {
            // No partition key provided - create writer if it doesn't exist
            if self.current_writer.is_none() {
                self.current_writer = Some(self.inner_builder.clone().build().await?);
            }
        }

        // do write
        if let Some(writer) = &mut self.current_writer {
            writer.write(input).await?;
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Writer is not initialized!",
            ))
        }
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        self.close_current_writer().await?;

        // Return all collected data files
        Ok(std::mem::take(&mut self.output))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataFileFormat, NestedField, PrimitiveType, Type};
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    #[tokio::test]
    async fn test_clustered_writer_basic() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema
        let schema = Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()?,
        );

        // Create writer builder
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        // Create rolling file writer builder
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Create data file writer builder
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder, None);

        // Create clustered writer
        let mut writer = ClusteredDataWriter::new(data_file_writer_builder);

        // Create test data with proper field ID metadata
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec!["Dave", "Eve"])),
            ],
        )?;

        // Write data without partitioning (pass None for partition_key)
        writer.write(None, batch1).await?;
        writer.write(None, batch2).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify at least one file was created
        assert!(
            !data_files.is_empty(),
            "Expected at least one data file to be created"
        );

        Ok(())
    }
}
