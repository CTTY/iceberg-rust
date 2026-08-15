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

//! End-to-end tests for `RewriteFilesAction` against a filesystem-backed
//! `MemoryCatalog`: append small files across snapshots, rewrite them into a
//! merged file, and verify the committed `Replace` snapshot serves ONLY the
//! merged file.
//!
//! The regression these tests pin: committing a compaction as a plain append
//! leaves the source files live in the new snapshot and every rewritten row is
//! served twice. A `RewriteFiles` commit must atomically add the merged file
//! AND remove its sources.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use futures::TryStreamExt;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

use crate::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use crate::spec::{DataFile, ManifestStatus, NestedField, Operation, PrimitiveType, Schema, Type};
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use crate::writer::file_writer::ParquetWriterBuilder;
use crate::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent};

/// A catalog + table with schema `(id: long)`, warehoused in a fresh `TempDir`.
async fn fs_catalog_and_table() -> (Arc<dyn Catalog>, crate::table::Table, TempDir) {
    let tmp = TempDir::new().unwrap();
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                format!("file://{}", tmp.path().to_str().unwrap()),
            )]),
        )
        .await
        .unwrap();

    let ns = NamespaceIdent::new("compaction".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap();

    let table = catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("c12_regression".to_string())
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();

    (Arc::new(catalog) as Arc<dyn Catalog>, table, tmp)
}

/// Write `ids` into a single Parquet data file for `table` and return its `DataFile`.
async fn write_data_file(table: &crate::table::Table, ids: &[i64]) -> DataFile {
    let location_gen = DefaultLocationGenerator::new(table.metadata()).unwrap();
    let file_name_gen = DefaultFileNameGenerator::new(
        "e2e".to_string(),
        Some(uuid::Uuid::now_v7().to_string()),
        crate::spec::DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );
    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(None)
        .await
        .unwrap();

    let arrow_schema =
        Arc::new(crate::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap());
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef
    ])
    .unwrap();
    writer.write(batch).await.unwrap();
    let mut files = writer.close().await.unwrap();
    assert_eq!(files.len(), 1, "expected exactly one data file");
    files.pop().unwrap()
}

/// Append `file` to `table` in its own snapshot and return the refreshed table.
async fn append_one(
    catalog: &Arc<dyn Catalog>,
    table: crate::table::Table,
    file: DataFile,
) -> crate::table::Table {
    let tx = Transaction::new(&table);
    let append = tx.fast_append().add_data_files(vec![file]);
    let tx = append.apply(tx).unwrap();
    tx.commit(catalog.as_ref()).await.unwrap()
}

/// All live (non-deleted) data file paths reachable from the current snapshot.
async fn live_file_paths(table: &crate::table::Table) -> Vec<String> {
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
    let mut paths = Vec::new();
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
        for entry in manifest.entries() {
            if entry.status() != ManifestStatus::Deleted {
                paths.push(entry.file_path().to_string());
            }
        }
    }
    paths.sort();
    paths
}

/// Total rows served by scanning the current table state.
async fn scan_row_count(table: &crate::table::Table) -> usize {
    let batches: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn rewrite_commit_replaces_sources_with_merged_file() {
    let (catalog, table, _tmp) = fs_catalog_and_table().await;

    // Two append snapshots of two rows each — the "small files" of a compaction.
    let file_a = write_data_file(&table, &[1, 2]).await;
    let file_b = write_data_file(&table, &[3, 4]).await;
    let path_a = file_a.file_path().to_string();
    let path_b = file_b.file_path().to_string();
    let table = append_one(&catalog, table, file_a).await;
    let table = append_one(&catalog, table, file_b).await;
    assert_eq!(scan_row_count(&table).await, 4);

    // The rewrite: one merged file carrying the union of the source rows.
    let merged = write_data_file(&table, &[1, 2, 3, 4]).await;
    let merged_path = merged.file_path().to_string();

    // Removal set: the two source files, re-read from the current snapshot the
    // way a real compactor discovers them.
    let sources: Vec<DataFile> = {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut found = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.status() != ManifestStatus::Deleted {
                    found.push(entry.data_file().clone());
                }
            }
        }
        found
    };
    assert_eq!(sources.len(), 2);

    let tx = Transaction::new(&table);
    let rewrite = tx
        .rewrite_files()
        .add_files(vec![merged])
        .unwrap()
        .delete_files(sources)
        .unwrap();
    let tx = rewrite.apply(tx).unwrap();
    let table = tx.commit(catalog.as_ref()).await.unwrap();

    // The committed snapshot is a REPLACE...
    let snapshot = table.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.summary().operation, Operation::Replace);

    // ...that serves ONLY the merged file. The c12 regression (fast_append-as-
    // compaction) would leave path_a/path_b live here and double every row.
    let live = live_file_paths(&table).await;
    assert_eq!(live, vec![merged_path.clone()]);
    assert!(!live.contains(&path_a));
    assert!(!live.contains(&path_b));

    // Row count is preserved, not doubled.
    assert_eq!(scan_row_count(&table).await, 4);

    // The summary's totals must subtract the removed files — caught live: a
    // rewrite that only fed added files into the collector reported
    // total-data-files=5 / total-records=400 for this exact shape.
    let props = &snapshot.summary().additional_properties;
    assert_eq!(props.get("total-data-files").map(String::as_str), Some("1"));
    assert_eq!(props.get("total-records").map(String::as_str), Some("4"));
    assert_eq!(
        props.get("deleted-data-files").map(String::as_str),
        Some("2")
    );
    assert_eq!(props.get("deleted-records").map(String::as_str), Some("4"));
}

#[tokio::test]
async fn rewrite_commit_fails_when_a_source_file_is_missing() {
    let (catalog, table, _tmp) = fs_catalog_and_table().await;

    let file_a = write_data_file(&table, &[1, 2]).await;
    let table = append_one(&catalog, table, file_a).await;

    // The removal names a file that is not in the table (e.g. already rewritten
    // by a concurrent compaction). Committing the ADD half anyway would
    // duplicate rows — the commit must refuse.
    let merged = write_data_file(&table, &[1, 2]).await;
    let phantom = write_data_file(&table, &[9]).await; // written but never appended

    let tx = Transaction::new(&table);
    let rewrite = tx
        .rewrite_files()
        .add_files(vec![merged])
        .unwrap()
        .delete_files(vec![phantom])
        .unwrap();
    let tx = rewrite.apply(tx).unwrap();
    let err = tx.commit(catalog.as_ref()).await.unwrap_err();

    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert!(err.to_string().contains("Missing required files to delete"));

    // And the table is untouched: still exactly one live file, two rows.
    let table = catalog
        .load_table(&TableIdent::from_strs(["compaction", "c12_regression"]).unwrap())
        .await
        .unwrap();
    assert_eq!(live_file_paths(&table).await.len(), 1);
    assert_eq!(scan_row_count(&table).await, 2);
}

#[tokio::test]
async fn rewrite_with_nothing_to_delete_is_rejected() {
    let (catalog, table, _tmp) = fs_catalog_and_table().await;
    let file_a = write_data_file(&table, &[1]).await;
    let table = append_one(&catalog, table, file_a).await;

    // A "rewrite" that only adds is an append wearing a costume — reject it.
    let extra = write_data_file(&table, &[2]).await;
    let tx = Transaction::new(&table);
    let rewrite = tx.rewrite_files().add_files(vec![extra]).unwrap();
    let tx = rewrite.apply(tx).unwrap();
    let err = tx.commit(catalog.as_ref()).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::DataInvalid);
}
