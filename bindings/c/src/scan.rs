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

//! Planning a table scan: which data files to read, with which projection,
//! partition constants and delete files.
//!
//! # Protocol
//!
//! 1. `iceberg_scan_builder_new(table)` returns an `Exclusive<ScanBuilder>`:
//!    a plain configuration record (snapshot, selected field ids, case
//!    sensitivity, predicate). iceberg-rust's own builder borrows the table,
//!    so nothing is bound until build time.
//! 2. `iceberg_scan_builder_build(runtime, builder)` consumes the builder,
//!    binds the configuration against the chosen snapshot's schema, plans
//!    **eagerly** (every file task is collected before the call returns) and
//!    returns a `Shared<Scan>`. Planning is the only async step, hence the
//!    runtime argument; it must be the runtime the table was loaded with.
//! 3. The scan exposes its tasks by index, sorted by data file path so the
//!    order is stable across runs. `iceberg_scan_file_task` delivers one task
//!    as an [`IcebergFileTaskInfo`] plus a `Shared<FileTask>` handle the
//!    engine owns and frees with `iceberg_file_task_free`.
//!
//! A predicate that does not bind (unknown column, literal of the wrong type
//! or scale) fails `build` with
//! [`IcebergErrorCode::PredicateBindFailed`] so the engine can retry without
//! a predicate; the result is a superset of files, which the engine's own
//! filters reduce.

use std::ffi::c_void;
use std::sync::Arc;
use std::time::Instant;

use futures::TryStreamExt;
use iceberg::expr::{Bind, Predicate};
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataContentType, DataFileFormat, Schema, SchemaRef};

use crate::error::{
    AllocateErrorFn, FfiError, FfiResult, IcebergErrorCode, IcebergResultScan,
    IcebergResultScanBuilder, IcebergResultUnit,
};
use crate::guard::ErrorSource;
use crate::handle::{Exclusive, Shared};
use crate::literal::{IcebergLiteral, from_datum};
use crate::optional::{IcebergOptionalI64, IcebergOptionalU64};
use crate::predicate::{IcebergPredicateState, IcebergPredicateVisitorFn};
use crate::runtime::Runtime;
use crate::strings::StringSlice;
use crate::table::Table;

/// Configuration behind an `Exclusive<ScanBuilder>` handle.
pub struct ScanBuilder {
    table: Arc<Table>,
    snapshot_id: Option<i64>,
    field_ids: Option<Vec<i32>>,
    case_sensitive: bool,
    predicate: Option<Predicate>,
    alloc_error: AllocateErrorFn,
}

impl ScanBuilder {
    /// Resolves a handle to a mutable builder, reporting null handles.
    ///
    /// # Safety
    ///
    /// See [`Exclusive::borrow_mut`].
    pub(crate) unsafe fn from_handle_mut(
        handle: &mut Exclusive<ScanBuilder>,
    ) -> FfiResult<&mut ScanBuilder> {
        // SAFETY: forwarded to the caller.
        unsafe { handle.borrow_mut() }.ok_or_else(|| FfiError::invalid_handle("scan builder"))
    }
}

impl ErrorSource for ScanBuilder {
    fn alloc_error(&self) -> AllocateErrorFn {
        self.alloc_error
    }
}

/// A planned scan behind a `Shared<Scan>` handle.
///
/// Holds the table (and through it the engine file system) alive, the
/// planned tasks in data-file-path order, and the union of equality delete
/// field ids across all tasks.
pub struct Scan {
    _table: Arc<Table>,
    snapshot_id: Option<i64>,
    tasks: Vec<Arc<FileTask>>,
    equality_field_ids: Vec<i32>,
    alloc_error: AllocateErrorFn,
}

impl Scan {
    /// Resolves a handle to a scan reference, reporting null handles.
    ///
    /// # Safety
    ///
    /// See [`Shared::borrow`].
    pub(crate) unsafe fn from_handle(handle: &Shared<Scan>) -> FfiResult<&Scan> {
        // SAFETY: forwarded to the caller.
        unsafe { handle.borrow() }.ok_or_else(|| FfiError::invalid_handle("scan"))
    }
}

impl ErrorSource for Scan {
    fn alloc_error(&self) -> AllocateErrorFn {
        self.alloc_error
    }
}

/// One data file to read, behind a `Shared<FileTask>` handle.
///
/// Keeps the table alive for as long as the engine holds the handle, so a
/// task may outlive the scan and the engine's table handle.
pub struct FileTask {
    _table: Arc<Table>,
    task: FileScanTask,
    alloc_error: AllocateErrorFn,
}

impl FileTask {
    /// Resolves a handle to a task reference, reporting null handles.
    ///
    /// # Safety
    ///
    /// See [`Shared::borrow`].
    pub(crate) unsafe fn from_handle(handle: &Shared<FileTask>) -> FfiResult<&FileTask> {
        // SAFETY: forwarded to the caller.
        unsafe { handle.borrow() }.ok_or_else(|| FfiError::invalid_handle("file task"))
    }

    /// The fixed-size description of the task.
    fn info(&self) -> FfiResult<IcebergFileTaskInfo> {
        let t = &self.task;
        let file_size = t.file_size_in_bytes();
        if t.start() != 0 || t.length() != file_size {
            return Err(FfiError::new(
                IcebergErrorCode::FeatureUnsupported,
                format!(
                    "file task covers bytes {}..{} of {file_size} in {}; only whole files are supported",
                    t.start(),
                    t.start() + t.length(),
                    t.data_file_path()
                ),
            ));
        }
        let record_count = t.record_count().ok_or_else(|| {
            FfiError::new(
                IcebergErrorCode::DataInvalid,
                format!("file task for {} has no record count", t.data_file_path()),
            )
        })?;
        let partition_spec_id = t.partition_spec().map(|s| s.spec_id()).ok_or_else(|| {
            FfiError::new(
                IcebergErrorCode::DataInvalid,
                format!("file task for {} has no partition spec", t.data_file_path()),
            )
        })?;
        let mut n_positional_deletes: u32 = 0;
        let mut n_equality_deletes: u32 = 0;
        for d in t.deletes() {
            match d.file_type() {
                DataContentType::PositionDeletes => n_positional_deletes += 1,
                DataContentType::EqualityDeletes => n_equality_deletes += 1,
                DataContentType::Data => {}
            }
        }
        Ok(IcebergFileTaskInfo {
            path: StringSlice::borrowed(t.data_file_path()),
            file_size,
            start: t.start(),
            length: t.length(),
            record_count,
            data_sequence_number: t.data_sequence_number().into(),
            first_row_id: t.first_row_id().into(),
            partition_spec_id,
            n_positional_deletes,
            n_equality_deletes,
            format: t.data_file_format().into(),
        })
    }
}

impl ErrorSource for FileTask {
    fn alloc_error(&self) -> AllocateErrorFn {
        self.alloc_error
    }
}

// Scan and task handles may be used from any engine thread.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ScanBuilder>();
    assert_send_sync::<Scan>();
    assert_send_sync::<FileTask>();
};

/// File format of a data or delete file.
///
/// Values follow iceberg-rust's `DataFileFormat` declaration order.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergDataFileFormat {
    /// Avro.
    Avro = 0,
    /// ORC.
    Orc = 1,
    /// Parquet.
    Parquet = 2,
    /// Puffin (deletion vectors and statistics).
    Puffin = 3,
}

impl From<DataFileFormat> for IcebergDataFileFormat {
    fn from(f: DataFileFormat) -> Self {
        match f {
            DataFileFormat::Avro => Self::Avro,
            DataFileFormat::Orc => Self::Orc,
            DataFileFormat::Parquet => Self::Parquet,
            DataFileFormat::Puffin => Self::Puffin,
        }
    }
}

/// Content of a delete file; values are the Iceberg manifest `content` codes.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergDeleteContent {
    /// A data file (never reported by `iceberg_file_task_visit_deletes`).
    Data = 0,
    /// Position deletes (`file_path`, `pos` rows, or a deletion vector).
    PositionDeletes = 1,
    /// Equality deletes.
    EqualityDeletes = 2,
}

impl From<DataContentType> for IcebergDeleteContent {
    fn from(c: DataContentType) -> Self {
        match c {
            DataContentType::Data => Self::Data,
            DataContentType::PositionDeletes => Self::PositionDeletes,
            DataContentType::EqualityDeletes => Self::EqualityDeletes,
        }
    }
}

/// Fixed-size description of one file task, delivered by
/// `iceberg_scan_file_task`.
///
/// `path` is valid only during the callback. `start` is always 0 and `length`
/// always equals `file_size` (a task covering part of a file is rejected
/// before the callback runs).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IcebergFileTaskInfo {
    /// Data file path as recorded in the manifest.
    pub path: StringSlice,
    /// Size of the data file in bytes.
    pub file_size: u64,
    /// First byte to read; always 0.
    pub start: u64,
    /// Number of bytes to read; always `file_size`.
    pub length: u64,
    /// Rows in the file, from the manifest entry.
    pub record_count: u64,
    /// Data sequence number of the manifest entry, if recorded.
    pub data_sequence_number: IcebergOptionalI64,
    /// First row id of the file (row lineage), if recorded.
    pub first_row_id: IcebergOptionalI64,
    /// Partition spec the file was written with.
    pub partition_spec_id: i32,
    /// Number of position delete files that apply to this file.
    pub n_positional_deletes: u32,
    /// Number of equality delete files that apply to this file.
    pub n_equality_deletes: u32,
    /// File format.
    pub format: IcebergDataFileFormat,
}

/// Receives one task from `iceberg_scan_file_task`.
///
/// Arguments: `ctx`, the task description (valid only during the callback)
/// and a task handle the engine now owns and must release with
/// `iceberg_file_task_free`. Must not unwind.
pub type IcebergFileTaskCallbackFn =
    extern "C" fn(*mut c_void, *const IcebergFileTaskInfo, Shared<FileTask>);

/// Receives one identity partition constant from
/// `iceberg_file_task_visit_partition_constants`.
///
/// Arguments: `ctx`, the *source* field id (the schema column the partition
/// field reads) and its value, valid only during the callback. Must not
/// unwind.
pub type IcebergPartitionConstantCallbackFn =
    extern "C" fn(*mut c_void, i32, *const IcebergLiteral);

/// Receives one delete file from `iceberg_file_task_visit_deletes`.
///
/// Arguments: `ctx`, `path`, `content`, `format`, `file_size`,
/// `referenced_data_file` (empty when the delete file is not bound to one
/// data file), `content_offset` and `content_size` (set for deletion vectors
/// in Puffin files), `equality_ids` and `n_equality_ids` (null and 0 unless
/// `content` is equality deletes). Slices and arrays are valid only during
/// the callback. Must not unwind.
pub type IcebergDeleteFileCallbackFn = extern "C" fn(
    *mut c_void,
    StringSlice,
    IcebergDeleteContent,
    IcebergDataFileFormat,
    u64,
    StringSlice,
    IcebergOptionalU64,
    IcebergOptionalU64,
    *const i32,
    usize,
);

/// Maps selected field ids to column names for `TableScanBuilder::select`.
///
/// Only top-level columns can be selected: a nested field id is
/// `FeatureUnsupported`, an unknown id `InvalidArgument`.
fn field_names(schema: &Schema, ids: &[i32]) -> FfiResult<Vec<String>> {
    ids.iter()
        .map(|&id| {
            if schema.as_struct().field_by_id(id).is_some() {
                schema
                    .name_by_field_id(id)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        FfiError::new(
                            IcebergErrorCode::Unexpected,
                            format!("field id {id} has no name in the schema"),
                        )
                    })
            } else if let Some(field) = schema.field_by_id(id) {
                Err(FfiError::new(
                    IcebergErrorCode::FeatureUnsupported,
                    format!(
                        "field id {id} (`{}`) is nested; only top-level columns can be selected",
                        schema.name_by_field_id(id).unwrap_or(&field.name)
                    ),
                ))
            } else {
                Err(FfiError::new(
                    IcebergErrorCode::InvalidArgument,
                    format!("field id {id} is not in the schema"),
                ))
            }
        })
        .collect()
}

/// Microseconds since `start`, saturating.
fn elapsed_us(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX)
}

/// Binds the configuration, plans every file task and wraps the result.
fn plan(rt: &Runtime, builder: ScanBuilder) -> FfiResult<Shared<Scan>> {
    let ScanBuilder {
        table,
        snapshot_id,
        field_ids,
        case_sensitive,
        predicate,
        alloc_error,
    } = builder;
    let metadata = table.metadata();
    // The schema `TableScanBuilder::build` will bind against: the snapshot's
    // own schema under time travel, the current one otherwise.
    let schema: SchemaRef = match snapshot_id {
        Some(id) => metadata
            .snapshot_by_id(id)
            .ok_or_else(|| {
                FfiError::new(
                    IcebergErrorCode::InvalidArgument,
                    format!("unknown snapshot id {id}"),
                )
            })?
            .schema(&metadata)?,
        None => metadata.current_schema().clone(),
    };
    let names = field_ids
        .as_deref()
        .map(|ids| field_names(&schema, ids))
        .transpose()?;
    // iceberg-rust reports bind failures as plain DataInvalid errors, the
    // same kind as a missing snapshot, so bind here first to classify them.
    let build_start = Instant::now();
    if let Some(p) = &predicate {
        p.clone()
            .rewrite_not()
            .bind(schema.clone(), case_sensitive)
            .map_err(|e| FfiError::new(IcebergErrorCode::PredicateBindFailed, e.to_string()))?;
    }
    let predicate_text = predicate
        .as_ref()
        .map_or_else(|| "<none>".to_owned(), ToString::to_string);

    let mut b = table.scan().with_case_sensitive(case_sensitive);
    if let Some(id) = snapshot_id {
        b = b.snapshot_id(id);
    }
    if let Some(names) = names {
        b = b.select(names);
    }
    if let Some(p) = predicate {
        b = b.with_filter(p);
    }
    let scan = b.build()?;
    let build_us = elapsed_us(build_start);
    let planned_snapshot = scan.snapshot().map(|s| s.snapshot_id());

    // Both events sit inside `block_on`, the only place the engine's log
    // sink is installed; anything emitted after it returns is dropped. The
    // timing fields follow `tasks` so engines matching on the prefix of the
    // rendered message keep working.
    let mut tasks: Vec<FileScanTask> = rt.block_on(async {
        tracing::debug!(
            target: "iceberg_ffi::scan",
            snapshot = ?planned_snapshot,
            predicate = %predicate_text,
            "planning files"
        );
        let plan_start = Instant::now();
        let tasks: Vec<FileScanTask> = scan.plan_files().await?.try_collect().await?;
        tracing::debug!(
            target: "iceberg_ffi::scan",
            tasks = tasks.len(),
            build_us,
            plan_files_us = elapsed_us(plan_start),
            "planned files"
        );
        Ok::<_, iceberg::Error>(tasks)
    })??;
    tasks.sort_by(|a, b| a.data_file_path().cmp(b.data_file_path()));

    let mut equality_field_ids: Vec<i32> = tasks
        .iter()
        .flat_map(|t| t.deletes())
        .filter(|d| d.file_type() == DataContentType::EqualityDeletes)
        .flat_map(|d| d.equality_ids().unwrap_or(&[]).iter().copied())
        .collect();
    equality_field_ids.sort_unstable();
    equality_field_ids.dedup();

    let tasks = tasks
        .into_iter()
        .map(|task| {
            Arc::new(FileTask {
                _table: Arc::clone(&table),
                task,
                alloc_error,
            })
        })
        .collect();
    Ok(Shared::from_arc(Arc::new(Scan {
        _table: table,
        snapshot_id: planned_snapshot,
        tasks,
        equality_field_ids,
        alloc_error,
    })))
}

/// Copies up to `cap` ids into `out` and returns how many there are in
/// total; call with `cap == 0` to size the buffer. A null `out` is treated
/// as `cap == 0`: nothing is copied and the total is still returned.
fn copy_ids(ids: &[i32], out: *mut i32, cap: usize) -> usize {
    let cap = if out.is_null() { 0 } else { cap };
    let n = ids.len().min(cap);
    if n > 0 {
        // SAFETY: the engine provides `cap` writable `i32` slots at `out`.
        unsafe { std::ptr::copy_nonoverlapping(ids.as_ptr(), out, n) };
    }
    ids.len()
}

// The pointer arguments below are engine arguments with the same contract as
// every handle in this crate (valid for the call, checked for null);
// `unsafe` on an `extern "C"` signature would not change anything for the C
// caller.

/// Creates a scan builder for `table` with the defaults: current snapshot,
/// every column, case-sensitive names, no predicate.
///
/// The engine owns the returned handle until it passes it to
/// `iceberg_scan_builder_build` or `iceberg_scan_builder_free`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_builder_new(table: Shared<Table>) -> IcebergResultScanBuilder {
    with_table!(table, IcebergResultScanBuilder, |t| {
        // SAFETY: `t` was just resolved from `table`, so the handle is live.
        let arc = unsafe { table.clone_arc() }.ok_or_else(|| FfiError::invalid_handle("table"))?;
        Ok(Exclusive::from_box(Box::new(ScanBuilder {
            table: arc,
            snapshot_id: None,
            field_ids: None,
            case_sensitive: true,
            predicate: None,
            alloc_error: t.alloc_error(),
        })))
    })
}

/// Releases a scan builder that was not built. A null handle is ignored.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_builder_free(builder: Exclusive<ScanBuilder>) {
    ffi_guard!(|_msg| (), {
        // SAFETY: the engine gives up ownership by calling free.
        drop(unsafe { builder.into_box() });
    })
}

/// Scans snapshot `snapshot_id` instead of the current one. The snapshot's
/// own schema is used for binding and projection; an unknown id is reported
/// by `iceberg_scan_builder_build` as `IcebergErrorCodeInvalidArgument`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_builder_snapshot_id(
    builder: Exclusive<ScanBuilder>,
    snapshot_id: i64,
) -> IcebergResultUnit {
    with_scan_builder!(builder, IcebergResultUnit, |b| {
        b.snapshot_id = Some(snapshot_id);
        Ok(())
    })
}

/// Projects only the `n` top-level columns with these field ids, in this
/// order; `n == 0` projects no columns. Replaces an earlier selection. Ids
/// are validated at build time: a nested id is
/// `IcebergErrorCodeFeatureUnsupported`, an unknown id
/// `IcebergErrorCodeInvalidArgument`.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_builder_select_field_ids(
    builder: Exclusive<ScanBuilder>,
    field_ids: *const i32,
    n: usize,
) -> IcebergResultUnit {
    with_scan_builder!(builder, IcebergResultUnit, |b| {
        if n > 0 && field_ids.is_null() {
            return Err(FfiError::new(
                IcebergErrorCode::InvalidArgument,
                format!("field_ids pointer is null but {n} ids were given"),
            ));
        }
        let ids = if n == 0 {
            Vec::new()
        } else {
            // SAFETY: non-null and the engine provides `n` readable ids.
            unsafe { std::slice::from_raw_parts(field_ids, n) }.to_vec()
        };
        b.field_ids = Some(ids);
        Ok(())
    })
}

/// Whether column names in the predicate are matched case-sensitively
/// (default true).
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_builder_case_sensitive(
    builder: Exclusive<ScanBuilder>,
    case_sensitive: bool,
) -> IcebergResultUnit {
    with_scan_builder!(builder, IcebergResultUnit, |b| {
        b.case_sensitive = case_sensitive;
        Ok(())
    })
}

/// Builds the predicate by calling `visitor(ctx, state)` immediately, on the
/// calling thread; see the `predicate` module for the arena protocol.
///
/// The visitor returns the root node id, or `ICEBERG_PRED_NONE` to clear the
/// predicate. A failed state, or a root that is not a predicate node, is
/// `IcebergErrorCodeInvalidArgument` and leaves the builder's previous
/// predicate in place.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_builder_set_predicate(
    builder: Exclusive<ScanBuilder>,
    ctx: *mut c_void,
    visitor: IcebergPredicateVisitorFn,
) -> IcebergResultUnit {
    with_scan_builder!(builder, IcebergResultUnit, |b| {
        let mut state = IcebergPredicateState::new();
        let root = visitor(ctx, &mut state);
        b.predicate = state.finish(root)?;
        Ok(())
    })
}

/// Consumes the builder, plans the scan and returns it.
///
/// The builder is freed on every path, including a null runtime. `runtime`
/// must be the runtime the table was loaded with. Planning reads the
/// snapshot's manifest list and manifests through the table's file system
/// and collects every file task before returning.
///
/// Errors: a predicate that cannot be bound to the snapshot schema is
/// `IcebergErrorCodePredicateBindFailed` (retry without a predicate); see
/// `iceberg_scan_builder_snapshot_id` and
/// `iceberg_scan_builder_select_field_ids` for theirs.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_builder_build(
    runtime: Shared<Runtime>,
    builder: Exclusive<ScanBuilder>,
) -> IcebergResultScan {
    // Taking the builder back is an infallible move, so it happens before the
    // guard; from here on it is dropped on every exit path.
    // SAFETY: the engine hands over ownership by calling build.
    let builder = unsafe { builder.into_box() };
    with_runtime!(runtime, IcebergResultScan, |rt| {
        let builder = builder.ok_or_else(|| FfiError::invalid_handle("scan builder"))?;
        plan(rt, *builder)
    })
}

/// Releases the engine's reference to a scan. Task handles already handed
/// out stay valid. A null handle is ignored.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_free(scan: Shared<Scan>) {
    ffi_guard!(|_msg| (), {
        // SAFETY: the engine gives up its reference by calling free.
        drop(unsafe { scan.into_arc() });
    })
}

/// Number of file tasks in the scan; 0 for a null handle.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_file_task_count(scan: Shared<Scan>) -> usize {
    ffi_guard!(|_msg| 0, {
        // SAFETY: the engine passes a handle from `iceberg_scan_builder_build`.
        match unsafe { Scan::from_handle(&scan) } {
            Ok(s) => s.tasks.len(),
            Err(_) => 0,
        }
    })
}

/// The snapshot the scan was planned from; absent when the table had no
/// snapshot (the scan is then empty) or the handle is null.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_snapshot_id(scan: Shared<Scan>) -> IcebergOptionalI64 {
    ffi_guard!(|_msg| IcebergOptionalI64::NONE, {
        // SAFETY: the engine passes a handle from `iceberg_scan_builder_build`.
        match unsafe { Scan::from_handle(&scan) } {
            Ok(s) => s.snapshot_id.into(),
            Err(_) => IcebergOptionalI64::NONE,
        }
    })
}

/// Union of the equality field ids of every equality delete file in the
/// scan, sorted and without duplicates.
///
/// Copies at most `cap` ids into `out` and returns the total count, so call
/// once with `cap == 0` to size the buffer and once more to fill it. A null
/// `out` is treated as `cap == 0` whatever `cap` says. Returns 0 for a null
/// handle.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_equality_field_ids(
    scan: Shared<Scan>,
    out: *mut i32,
    cap: usize,
) -> usize {
    ffi_guard!(|_msg| 0, {
        // SAFETY: the engine passes a handle from `iceberg_scan_builder_build`.
        match unsafe { Scan::from_handle(&scan) } {
            Ok(s) => copy_ids(&s.equality_field_ids, out, cap),
            Err(_) => 0,
        }
    })
}

/// Delivers task `index` (0-based, in data file path order) to `cb` as an
/// [`IcebergFileTaskInfo`] plus a task handle the engine owns.
///
/// `index >= iceberg_scan_file_task_count` is
/// `IcebergErrorCodeInvalidArgument`. A task that does not cover its whole
/// file is `IcebergErrorCodeFeatureUnsupported`; one without a record count
/// or partition spec is `IcebergErrorCodeDataInvalid`; `cb` is not called in
/// those cases.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_scan_file_task(
    scan: Shared<Scan>,
    index: usize,
    ctx: *mut c_void,
    cb: IcebergFileTaskCallbackFn,
) -> IcebergResultUnit {
    with_scan!(scan, IcebergResultUnit, |s| {
        let task = s.tasks.get(index).ok_or_else(|| {
            FfiError::new(
                IcebergErrorCode::InvalidArgument,
                format!(
                    "file task index {index} is out of range; the scan has {} tasks",
                    s.tasks.len()
                ),
            )
        })?;
        let info = task.info()?;
        cb(ctx, &info, Shared::from_arc(Arc::clone(task)));
        Ok(())
    })
}

/// Releases the engine's reference to a file task. A null handle is ignored.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_file_task_free(task: Shared<FileTask>) {
    ffi_guard!(|_msg| (), {
        // SAFETY: the engine gives up its reference by calling free.
        drop(unsafe { task.into_arc() });
    })
}

/// The field ids the task projects, in selection order (every top-level
/// column when nothing was selected).
///
/// Same two-call pattern as `iceberg_scan_equality_field_ids`; returns 0 for
/// a null handle.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_file_task_project_field_ids(
    task: Shared<FileTask>,
    out: *mut i32,
    cap: usize,
) -> usize {
    ffi_guard!(|_msg| 0, {
        // SAFETY: the engine passes a handle from `iceberg_scan_file_task`.
        match unsafe { FileTask::from_handle(&task) } {
            Ok(t) => copy_ids(t.task.project_field_ids(), out, cap),
            Err(_) => 0,
        }
    })
}

/// Calls `cb(ctx, source_field_id, value)` for every identity-transformed
/// partition field of the task's file, in ascending source field id order.
///
/// Only identity transforms contribute; bucket, truncate and time transforms
/// cannot reconstruct the column and are skipped. A null partition value is
/// skipped as well, so the engine reads the column (or NULL) from the data
/// file. Unpartitioned files produce no calls.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_file_task_visit_partition_constants(
    task: Shared<FileTask>,
    ctx: *mut c_void,
    cb: IcebergPartitionConstantCallbackFn,
) -> IcebergResultUnit {
    with_file_task!(task, IcebergResultUnit, |t| {
        let mut constants: Vec<_> = t.task.identity_partition_constants()?.into_iter().collect();
        constants.sort_by_key(|(source_id, _)| *source_id);
        for (source_id, datum) in &constants {
            let mut uuid_buf = [0u8; 16];
            let literal = from_datum(datum, &mut uuid_buf)?;
            cb(ctx, *source_id, &literal);
        }
        Ok(())
    })
}

/// Calls `cb` once per delete file that applies to the task's data file, in
/// manifest order; see [`IcebergDeleteFileCallbackFn`] for the arguments.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_file_task_visit_deletes(
    task: Shared<FileTask>,
    ctx: *mut c_void,
    cb: IcebergDeleteFileCallbackFn,
) -> IcebergResultUnit {
    with_file_task!(task, IcebergResultUnit, |t| {
        for d in t.task.deletes() {
            let equality_ids = d.equality_ids().unwrap_or(&[]);
            cb(
                ctx,
                StringSlice::borrowed(d.file_path()),
                d.file_type().into(),
                d.file_format().into(),
                d.file_size_in_bytes(),
                d.referenced_data_file()
                    .map_or(StringSlice::empty(), StringSlice::borrowed),
                d.content_offset().into(),
                d.content_size_in_bytes().into(),
                if equality_ids.is_empty() {
                    std::ptr::null()
                } else {
                    equality_ids.as_ptr()
                },
                equality_ids.len(),
            );
        }
        Ok(())
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use iceberg::TableIdent;
    use iceberg::expr::Reference;
    use iceberg::spec::Datum;
    use iceberg::table::StaticTable;

    use super::*;
    use crate::error::IcebergResultTag;
    use crate::error::test_support::{TestError, take};
    use crate::literal::IcebergLiteralKind;
    use crate::logging::IcebergLogLevel;
    use crate::logging::test_support::{CAPTURED, TEST_CTX};
    use crate::predicate::{
        ICEBERG_PRED_NONE, iceberg_pred_eq, iceberg_pred_ge, iceberg_pred_is_null,
        iceberg_pred_not, iceberg_pred_not_null, iceberg_pred_starts_with, test_support as pred,
    };
    use crate::runtime::iceberg_runtime_free;
    use crate::runtime::test_support::new_test_runtime;
    use crate::storage::test_support::{Counters, new_std_fs_vtable};
    use crate::storage::{DuckDbStorage, file_io};
    use crate::table::iceberg_table_free;
    use crate::table::test_support::{
        fixture, fixture_relocation, load_fixture_relocated, snapshots,
    };

    const SIMPLE: (&str, &str) = ("simple_v2", "v3.metadata.json");
    const IDENTITY: (&str, &str) = ("partitioned_identity_v2", "v4.metadata.json");
    const TRANSFORMS: (&str, &str) = ("partitioned_transforms_v2", "v3.metadata.json");
    const EVOLUTION: (&str, &str) = ("schema_evolution_v2", "v5.metadata.json");
    const TYPES: (&str, &str) = ("types_v2", "v2.metadata.json");
    const SIMPLE_V1: (&str, &str) = ("simple_v1", "v3.metadata.json");

    /// 2024-03-03T00:00:00 in microseconds since the epoch (day 19785).
    const MARCH_3_2024_MICROS: i64 = 1_709_424_000_000_000;

    fn error_of(err: *mut crate::error::EngineError) -> TestError {
        take(err).unwrap_or(TestError {
            code: IcebergErrorCode::Ok,
            message: "<no error object>".to_owned(),
        })
    }

    fn ok(r: IcebergResultUnit) {
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
    }

    fn load(rt: Shared<Runtime>, table: (&str, &str)) -> (Shared<Table>, Arc<Counters>) {
        load_fixture_relocated(rt, table.0, table.1)
    }

    /// A predicate visitor driven by a Rust closure stored behind `ctx`.
    type PredFn<'a> = &'a dyn Fn(&mut IcebergPredicateState) -> usize;

    extern "C" fn closure_visitor(ctx: *mut c_void, state: *mut IcebergPredicateState) -> usize {
        // SAFETY: `ctx` points at the `PredFn` the test built in
        // `set_predicate`, alive for the duration of the call; `state` is the
        // non-null arena the builder passed.
        let f = unsafe { &*ctx.cast::<PredFn<'_>>() };
        let state = unsafe { &mut *state };
        f(state)
    }

    fn set_predicate(
        builder: Exclusive<ScanBuilder>,
        f: &dyn Fn(&mut IcebergPredicateState) -> usize,
    ) -> IcebergResultUnit {
        let f: PredFn<'_> = f;
        iceberg_scan_builder_set_predicate(
            builder,
            (&f as *const PredFn<'_>).cast_mut().cast(),
            closure_visitor,
        )
    }

    /// Creates a builder, lets `configure` adjust it, builds it.
    fn build(
        rt: Shared<Runtime>,
        table: Shared<Table>,
        configure: impl FnOnce(Exclusive<ScanBuilder>),
    ) -> Result<Shared<Scan>, TestError> {
        let r = iceberg_scan_builder_new(table);
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        assert!(!r.ok.is_null());
        configure(r.ok);
        let r = iceberg_scan_builder_build(rt, r.ok);
        match r.tag {
            IcebergResultTag::Ok => {
                assert!(!r.ok.is_null());
                Ok(r.ok)
            }
            IcebergResultTag::Err => {
                assert!(r.ok.is_null());
                Err(error_of(r.err))
            }
        }
    }

    /// Builds, counts the tasks and frees the scan.
    fn plan_count(
        rt: Shared<Runtime>,
        table: Shared<Table>,
        configure: impl FnOnce(Exclusive<ScanBuilder>),
    ) -> usize {
        let scan = build(rt, table, configure).expect("build");
        let n = iceberg_scan_file_task_count(scan);
        iceberg_scan_free(scan);
        n
    }

    /// Plans with a predicate built by `f`.
    fn pred_count(
        rt: Shared<Runtime>,
        table: Shared<Table>,
        f: &dyn Fn(&mut IcebergPredicateState) -> usize,
    ) -> usize {
        plan_count(rt, table, |b| ok(set_predicate(b, f)))
    }

    /// Plans with a predicate built by `f` and expects an error.
    fn pred_error(
        rt: Shared<Runtime>,
        table: Shared<Table>,
        f: &dyn Fn(&mut IcebergPredicateState) -> usize,
    ) -> TestError {
        build(rt, table, |b| ok(set_predicate(b, f))).expect_err("build must fail")
    }

    /// Reference count: iceberg-rust's own `plan_files`, bypassing `scan.rs`
    /// and `predicate.rs`, over the same std::fs vtable and relocation.
    fn direct_plan_count(
        rt: Shared<Runtime>,
        table: (&str, &str),
        snapshot: Option<i64>,
        predicate: Option<Predicate>,
    ) -> usize {
        let (vtable, _counters) = new_std_fs_vtable();
        let (from, to) = fixture_relocation(table.0);
        let storage = DuckDbStorage::new(vtable, Some((from, to)));
        let location = fixture(&format!("{}/metadata/{}", table.0, table.1));
        // SAFETY: fresh non-null handle from `new_test_runtime`.
        let runtime = unsafe { Runtime::from_handle(&rt) }.expect("runtime");
        let tasks: Vec<FileScanTask> = runtime
            .block_on(async move {
                let ident = TableIdent::from_strs(["direct", "table"])?;
                let table =
                    StaticTable::from_metadata_file(&location, ident, file_io(storage)).await?;
                let mut b = table.scan();
                if let Some(id) = snapshot {
                    b = b.snapshot_id(id);
                }
                if let Some(p) = predicate {
                    b = b.with_filter(p);
                }
                let scan = b.build()?;
                scan.plan_files().await?.try_collect().await
            })
            .expect("block_on")
            .expect("direct plan");
        tasks.len()
    }

    /// One `iceberg_scan_file_task` delivery.
    #[derive(Debug, Clone)]
    struct TaskRow {
        path: String,
        file_size: u64,
        start: u64,
        length: u64,
        record_count: u64,
        data_sequence_number: Option<i64>,
        first_row_id: Option<i64>,
        partition_spec_id: i32,
        n_positional_deletes: u32,
        n_equality_deletes: u32,
        format: IcebergDataFileFormat,
        handle: Shared<FileTask>,
    }

    extern "C" fn collect_task(
        ctx: *mut c_void,
        info: *const IcebergFileTaskInfo,
        task: Shared<FileTask>,
    ) {
        // SAFETY: `ctx` is the `Mutex<Vec<TaskRow>>` the test passed; `info`
        // is valid for the duration of the callback.
        let rows = unsafe { &*ctx.cast::<Mutex<Vec<TaskRow>>>() };
        let info = unsafe { &*info };
        let path = unsafe { info.path.as_str() }
            .expect("utf-8 path")
            .to_owned();
        rows.lock().expect("rows").push(TaskRow {
            path,
            file_size: info.file_size,
            start: info.start,
            length: info.length,
            record_count: info.record_count,
            data_sequence_number: info.data_sequence_number.get(),
            first_row_id: info.first_row_id.get(),
            partition_spec_id: info.partition_spec_id,
            n_positional_deletes: info.n_positional_deletes,
            n_equality_deletes: info.n_equality_deletes,
            format: info.format,
            handle: task,
        });
    }

    /// Every task of `scan`, in index order; the caller frees the handles.
    fn tasks_of(scan: Shared<Scan>) -> Vec<TaskRow> {
        let rows = Mutex::new(Vec::new());
        let ctx = (&rows as *const Mutex<Vec<TaskRow>>).cast_mut().cast();
        for index in 0..iceberg_scan_file_task_count(scan) {
            ok(iceberg_scan_file_task(scan, index, ctx, collect_task));
        }
        rows.into_inner().expect("rows")
    }

    fn free_tasks(rows: Vec<TaskRow>) {
        for row in rows {
            iceberg_file_task_free(row.handle);
        }
    }

    fn project_field_ids(task: Shared<FileTask>) -> Vec<i32> {
        let n = iceberg_file_task_project_field_ids(task, std::ptr::null_mut(), 0);
        let mut out = vec![0i32; n];
        assert_eq!(
            iceberg_file_task_project_field_ids(task, out.as_mut_ptr(), out.len()),
            n
        );
        out
    }

    fn equality_field_ids(scan: Shared<Scan>) -> Vec<i32> {
        let n = iceberg_scan_equality_field_ids(scan, std::ptr::null_mut(), 0);
        let mut out = vec![0i32; n];
        assert_eq!(
            iceberg_scan_equality_field_ids(scan, out.as_mut_ptr(), out.len()),
            n
        );
        out
    }

    /// One identity partition constant: source id, kind, is_null, integer.
    type ConstantRow = (i32, IcebergLiteralKind, bool, i64);

    extern "C" fn collect_constant(ctx: *mut c_void, source_id: i32, value: *const IcebergLiteral) {
        // SAFETY: `ctx` is the `Mutex<Vec<ConstantRow>>` the test passed;
        // `value` is valid for the duration of the callback.
        let rows = unsafe { &*ctx.cast::<Mutex<Vec<ConstantRow>>>() };
        let lit = unsafe { &*value };
        rows.lock()
            .expect("rows")
            .push((source_id, lit.kind, lit.is_null, lit.i));
    }

    fn partition_constants(task: Shared<FileTask>) -> Vec<ConstantRow> {
        let rows = Mutex::new(Vec::new());
        ok(iceberg_file_task_visit_partition_constants(
            task,
            (&rows as *const Mutex<Vec<ConstantRow>>).cast_mut().cast(),
            collect_constant,
        ));
        rows.into_inner().expect("rows")
    }

    #[allow(clippy::too_many_arguments)]
    extern "C" fn count_delete(
        ctx: *mut c_void,
        _path: StringSlice,
        _content: IcebergDeleteContent,
        _format: IcebergDataFileFormat,
        _file_size: u64,
        _referenced: StringSlice,
        _offset: IcebergOptionalU64,
        _size: IcebergOptionalU64,
        _equality_ids: *const i32,
        _n_equality_ids: usize,
    ) {
        // SAFETY: `ctx` is the `AtomicUsize` the test passed.
        unsafe { &*ctx.cast::<AtomicUsize>() }.fetch_add(1, Ordering::SeqCst);
    }

    fn delete_count(task: Shared<FileTask>) -> usize {
        let n = AtomicUsize::new(0);
        ok(iceberg_file_task_visit_deletes(
            task,
            (&n as *const AtomicUsize).cast_mut().cast(),
            count_delete,
        ));
        n.into_inner()
    }

    // ----------------------------------------------------------------------
    // Planning without a predicate, time travel
    // ----------------------------------------------------------------------

    #[test]
    fn plan_without_predicate_lists_every_file() {
        let rt = new_test_runtime();
        for (table, expected) in [(SIMPLE, 2), (IDENTITY, 7), (TRANSFORMS, 12)] {
            let (t, _c) = load(rt, table);
            assert_eq!(plan_count(rt, t, |_| ()), expected, "{}", table.0);
            assert_eq!(direct_plan_count(rt, table, None, None), expected);
            iceberg_table_free(t);
        }
        iceberg_runtime_free(rt);
    }

    #[test]
    fn snapshot_id_time_travel_changes_the_task_set() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        let snaps = snapshots(t);
        assert_eq!(snaps.len(), 3);
        for (snap, expected) in snaps.iter().zip([2usize, 5, 7]) {
            let id = snap.snapshot_id;
            assert_eq!(
                plan_count(rt, t, |b| ok(iceberg_scan_builder_snapshot_id(b, id))),
                expected
            );
            assert_eq!(direct_plan_count(rt, IDENTITY, Some(id), None), expected);
        }
        let e = build(rt, t, |b| ok(iceberg_scan_builder_snapshot_id(b, 42)))
            .expect_err("unknown snapshot");
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
        assert!(e.message.contains("42"), "{}", e.message);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn scan_snapshot_id_is_the_planned_snapshot() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        let snaps = snapshots(t);
        let first = snaps[0].snapshot_id;
        let current = snaps[2].snapshot_id;

        let scan = build(rt, t, |_| ()).expect("build");
        assert_eq!(iceberg_scan_snapshot_id(scan).get(), Some(current));
        iceberg_scan_free(scan);

        let scan = build(rt, t, |b| ok(iceberg_scan_builder_snapshot_id(b, first))).expect("build");
        assert_eq!(iceberg_scan_snapshot_id(scan).get(), Some(first));
        iceberg_scan_free(scan);

        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn planning_is_logged_at_debug() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        let n = pred_count(rt, t, &|s| {
            let c = pred::column(s, "region");
            let v = pred::int(s, 3);
            iceberg_pred_eq(s, c, v)
        });
        assert_eq!(n, 2);
        let captured = CAPTURED.lock().map(|v| v.clone()).unwrap_or_default();
        assert!(
            captured.iter().any(|(c, l, target, m)| *c == TEST_CTX
                && *l == IcebergLogLevel::Debug
                && target == "iceberg_ffi::scan"
                && m.starts_with("planning files")
                && m.contains("predicate=region = 3")),
            "{captured:?}"
        );
        // The count is emitted inside `block_on` so it reaches the sink too;
        // the timings follow it so prefix matches on `tasks=` keep working.
        assert!(
            captured.iter().any(|(c, l, target, m)| *c == TEST_CTX
                && *l == IcebergLogLevel::Debug
                && target == "iceberg_ffi::scan"
                && m.starts_with("planned files tasks=2 build_us=")
                && m.contains(" plan_files_us=")),
            "{captured:?}"
        );
        assert!(
            captured.iter().any(|(c, l, target, m)| *c == TEST_CTX
                && *l == IcebergLogLevel::Debug
                && target == "iceberg_ffi::table"
                && m.starts_with("loaded table metadata elapsed_us=")
                && m.contains(" snapshots=3")),
            "{captured:?}"
        );
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    // ----------------------------------------------------------------------
    // Predicates
    // ----------------------------------------------------------------------

    #[test]
    fn identity_predicate_prunes_files() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        for (region, expected) in [(1, 2usize), (2, 3), (3, 2)] {
            let n = pred_count(rt, t, &|s| {
                let c = pred::column(s, "region");
                let v = pred::int(s, region);
                iceberg_pred_eq(s, c, v)
            });
            assert_eq!(n, expected, "region = {region}");
            let direct = Reference::new("region").equal_to(Datum::int(region));
            assert_eq!(
                direct_plan_count(rt, IDENTITY, None, Some(direct)),
                expected
            );
        }
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn bucket_predicate_prunes_files() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, TRANSFORMS);
        let n = pred_count(rt, t, &|s| {
            let c = pred::column(s, "id");
            let v = pred::long(s, 1);
            iceberg_pred_eq(s, c, v)
        });
        let direct = Reference::new("id").equal_to(Datum::long(1));
        assert_eq!(n, direct_plan_count(rt, TRANSFORMS, None, Some(direct)));
        assert!(n > 0 && n < 12, "{n}");
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn truncate_predicate_prunes_files() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, TRANSFORMS);
        let n = pred_count(rt, t, &|s| {
            let c = pred::column(s, "category");
            let v = pred::string(s, "beta");
            iceberg_pred_eq(s, c, v)
        });
        // Files with category_trunc = "be".
        assert_eq!(n, 3);
        let direct = Reference::new("category").equal_to(Datum::string("beta"));
        assert_eq!(direct_plan_count(rt, TRANSFORMS, None, Some(direct)), 3);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn day_predicate_prunes_files() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, TRANSFORMS);
        let n = pred_count(rt, t, &|s| {
            let c = pred::column(s, "ts");
            let v = pred::timestamp(s, MARCH_3_2024_MICROS);
            iceberg_pred_ge(s, c, v)
        });
        // Files with ts_day = 19785 (2024-03-03).
        assert_eq!(n, 3);
        let direct = Reference::new("ts")
            .greater_than_or_equal_to(Datum::timestamp_micros(MARCH_3_2024_MICROS));
        assert_eq!(direct_plan_count(rt, TRANSFORMS, None, Some(direct)), 3);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn in_predicate() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        let n = pred_count(rt, t, &|s| {
            let c = pred::column(s, "region");
            let one = pred::int(s, 1);
            let three = pred::int(s, 3);
            pred::is_in(s, c, &[one, three])
        });
        assert_eq!(n, 4);
        let direct = Reference::new("region").is_in([Datum::int(1), Datum::int(3)]);
        assert_eq!(direct_plan_count(rt, IDENTITY, None, Some(direct)), 4);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn and_predicate() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        let n = pred_count(rt, t, &|s| {
            let region = pred::column(s, "region");
            let one = pred::int(s, 1);
            let user = pred::column(s, "user_id");
            let hundred = pred::long(s, 100);
            let p = iceberg_pred_eq(s, region, one);
            let q = iceberg_pred_eq(s, user, hundred);
            pred::and(s, &[p, q])
        });
        // Every region=1 row has user_id 100, so bucket pruning keeps both files.
        assert_eq!(n, 2);
        let direct = Reference::new("region")
            .equal_to(Datum::int(1))
            .and(Reference::new("user_id").equal_to(Datum::long(100)));
        assert_eq!(direct_plan_count(rt, IDENTITY, None, Some(direct)), 2);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn or_predicate() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        let n = pred_count(rt, t, &|s| {
            let region = pred::column(s, "region");
            let one = pred::int(s, 1);
            let three = pred::int(s, 3);
            let p = iceberg_pred_eq(s, region, one);
            let q = iceberg_pred_eq(s, region, three);
            pred::or(s, &[p, q])
        });
        assert_eq!(n, 4);
        let direct = Reference::new("region")
            .equal_to(Datum::int(1))
            .or(Reference::new("region").equal_to(Datum::int(3)));
        assert_eq!(direct_plan_count(rt, IDENTITY, None, Some(direct)), 4);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn not_predicate() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        let n = pred_count(rt, t, &|s| {
            let region = pred::column(s, "region");
            let one = pred::int(s, 1);
            let p = iceberg_pred_eq(s, region, one);
            iceberg_pred_not(s, p)
        });
        assert_eq!(n, 5);
        let direct = Reference::new("region").equal_to(Datum::int(1)).negate();
        assert_eq!(direct_plan_count(rt, IDENTITY, None, Some(direct)), 5);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn is_null_predicate() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        // `region` is required: nothing can match.
        let n = pred_count(rt, t, &|s| {
            let c = pred::column(s, "region");
            iceberg_pred_is_null(s, c)
        });
        assert_eq!(n, 0);
        assert_eq!(
            direct_plan_count(rt, IDENTITY, None, Some(Reference::new("region").is_null())),
            0
        );
        // `payload` is null for ids 5, 10 and 15, all in region=2 files.
        let n = pred_count(rt, t, &|s| {
            let c = pred::column(s, "payload");
            iceberg_pred_is_null(s, c)
        });
        let direct = direct_plan_count(
            rt,
            IDENTITY,
            None,
            Some(Reference::new("payload").is_null()),
        );
        assert_eq!(n, direct);
        assert_eq!(n, 3);
        // And the complement keeps every file.
        let n = pred_count(rt, t, &|s| {
            let c = pred::column(s, "payload");
            iceberg_pred_not_null(s, c)
        });
        assert_eq!(n, 7);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn starts_with_predicate() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        let n = pred_count(rt, t, &|s| {
            let c = pred::column(s, "payload");
            let v = pred::string(s, "payload-1");
            iceberg_pred_starts_with(s, c, v)
        });
        let direct = Reference::new("payload").starts_with(Datum::string("payload-1"));
        assert_eq!(n, direct_plan_count(rt, IDENTITY, None, Some(direct)));
        // payload-1 and payload-1x live in four of the seven files.
        assert_eq!(n, 4);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn unknown_column_is_predicate_bind_failed() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        let e = pred_error(rt, t, &|s| {
            let c = pred::column(s, "no_such_column");
            let v = pred::int(s, 1);
            iceberg_pred_eq(s, c, v)
        });
        assert_eq!(e.code, IcebergErrorCode::PredicateBindFailed);
        assert!(e.message.contains("no_such_column"), "{}", e.message);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn decimal_scale_mismatch_is_predicate_bind_failed() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, TRANSFORMS);
        // `amount` is decimal(10, 2); the literal has scale 3.
        let e = pred_error(rt, t, &|s| {
            let c = pred::column(s, "amount");
            let v = pred::decimal(s, 1234, 10, 3);
            iceberg_pred_eq(s, c, v)
        });
        assert_eq!(e.code, IcebergErrorCode::PredicateBindFailed);
        assert!(e.message.contains("scale"), "{}", e.message);
        // The matching scale binds and plans.
        let n = pred_count(rt, t, &|s| {
            let c = pred::column(s, "amount");
            let v = pred::decimal(s, 1234, 10, 2);
            iceberg_pred_eq(s, c, v)
        });
        let direct = Reference::new("amount")
            .equal_to(Datum::decimal_from_mantissa(1234, 10, 2).expect("decimal"));
        assert_eq!(n, direct_plan_count(rt, TRANSFORMS, None, Some(direct)));
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn set_predicate_failure_is_reported() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);

        // Root is a column node, not a predicate.
        let r = iceberg_scan_builder_new(t);
        assert_eq!(r.tag, IcebergResultTag::Ok);
        let b = r.ok;
        let res = set_predicate(b, &|s| pred::column(s, "region"));
        assert_eq!(res.tag, IcebergResultTag::Err);
        assert_eq!(error_of(res.err).code, IcebergErrorCode::InvalidArgument);
        // The builder is still usable and has no predicate.
        let r = iceberg_scan_builder_build(rt, b);
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        assert_eq!(iceberg_scan_file_task_count(r.ok), 7);
        iceberg_scan_free(r.ok);

        // The sentinel on a healthy state means "no predicate".
        assert_eq!(pred_count(rt, t, &|_| ICEBERG_PRED_NONE), 7);

        // A failure recorded earlier wins over the sentinel: set_predicate
        // itself fails and the builder keeps planning without a predicate.
        let r = iceberg_scan_builder_new(t);
        assert_eq!(r.tag, IcebergResultTag::Ok);
        let b = r.ok;
        let res = set_predicate(b, &|s| {
            let _ = iceberg_pred_is_null(s, 99);
            ICEBERG_PRED_NONE
        });
        assert_eq!(res.tag, IcebergResultTag::Err);
        let e = error_of(res.err);
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
        assert!(e.message.contains("99"), "{}", e.message);
        let r = iceberg_scan_builder_build(rt, b);
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        assert_eq!(iceberg_scan_file_task_count(r.ok), 7);
        iceberg_scan_free(r.ok);

        // Setting the sentinel clears an earlier predicate.
        let n = plan_count(rt, t, |b| {
            ok(set_predicate(b, &|s| {
                let c = pred::column(s, "region");
                let v = pred::int(s, 1);
                iceberg_pred_eq(s, c, v)
            }));
            ok(set_predicate(b, &|_| ICEBERG_PRED_NONE));
        });
        assert_eq!(n, 7);

        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn case_insensitive_names_bind() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        let upper = |s: &mut IcebergPredicateState| {
            let c = pred::column(s, "REGION");
            let v = pred::int(s, 1);
            iceberg_pred_eq(s, c, v)
        };
        let e = pred_error(rt, t, &upper);
        assert_eq!(e.code, IcebergErrorCode::PredicateBindFailed);
        let n = plan_count(rt, t, |b| {
            ok(iceberg_scan_builder_case_sensitive(b, false));
            ok(set_predicate(b, &upper));
        });
        assert_eq!(n, 2);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    // ----------------------------------------------------------------------
    // Projection
    // ----------------------------------------------------------------------

    #[test]
    fn nested_id_in_select_is_feature_unsupported() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, SIMPLE);

        // 16 is `point.x`.
        let nested = [16i32];
        let e = build(rt, t, |b| {
            ok(iceberg_scan_builder_select_field_ids(b, nested.as_ptr(), 1));
        })
        .expect_err("nested");
        assert_eq!(e.code, IcebergErrorCode::FeatureUnsupported);
        assert!(e.message.contains("point.x"), "{}", e.message);

        let unknown = [99i32];
        let e = build(rt, t, |b| {
            ok(iceberg_scan_builder_select_field_ids(
                b,
                unknown.as_ptr(),
                1,
            ));
        })
        .expect_err("unknown");
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
        assert!(e.message.contains("99"), "{}", e.message);

        let top = [1i32, 10];
        let scan = build(rt, t, |b| {
            ok(iceberg_scan_builder_select_field_ids(b, top.as_ptr(), 2));
        })
        .expect("top-level");
        let rows = tasks_of(scan);
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(project_field_ids(row.handle), vec![1, 10]);
        }
        free_tasks(rows);
        iceberg_scan_free(scan);

        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn project_field_ids_round_trip() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, SIMPLE);

        // Default: every top-level column.
        let scan = build(rt, t, |_| ()).expect("build");
        let rows = tasks_of(scan);
        assert_eq!(
            project_field_ids(rows[0].handle),
            (1..=15).collect::<Vec<_>>()
        );
        // A short buffer still reports the full length and fills what fits.
        let mut short = [0i32; 3];
        assert_eq!(
            iceberg_file_task_project_field_ids(rows[0].handle, short.as_mut_ptr(), 3),
            15
        );
        assert_eq!(short, [1, 2, 3]);
        // A null `out` is a sizing call whatever `cap` says.
        assert_eq!(
            iceberg_file_task_project_field_ids(rows[0].handle, std::ptr::null_mut(), 3),
            15
        );
        free_tasks(rows);
        iceberg_scan_free(scan);

        // Selection order is preserved.
        let ids = [4i32, 1];
        let scan = build(rt, t, |b| {
            ok(iceberg_scan_builder_select_field_ids(b, ids.as_ptr(), 2));
        })
        .expect("build");
        let rows = tasks_of(scan);
        for row in &rows {
            assert_eq!(project_field_ids(row.handle), vec![4, 1]);
        }
        free_tasks(rows);
        iceberg_scan_free(scan);

        // A null pointer with n > 0 is rejected.
        let r = iceberg_scan_builder_new(t);
        let res = iceberg_scan_builder_select_field_ids(r.ok, std::ptr::null(), 2);
        assert_eq!(res.tag, IcebergResultTag::Err);
        assert_eq!(error_of(res.err).code, IcebergErrorCode::InvalidArgument);
        iceberg_scan_builder_free(r.ok);

        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn zero_selected_fields_projects_nothing() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, SIMPLE);
        let scan = build(rt, t, |b| {
            ok(iceberg_scan_builder_select_field_ids(
                b,
                std::ptr::null(),
                0,
            ));
        })
        .expect("build");
        let rows = tasks_of(scan);
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert!(project_field_ids(row.handle).is_empty());
        }
        free_tasks(rows);
        iceberg_scan_free(scan);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    // ----------------------------------------------------------------------
    // Task accessors
    // ----------------------------------------------------------------------

    #[test]
    fn file_task_info_matches_the_manifest() {
        let rt = new_test_runtime();
        let (t, _c) = load(rt, IDENTITY);
        let scan = build(rt, t, |_| ()).expect("build");
        let rows = tasks_of(scan);
        assert_eq!(rows.len(), 7);

        let paths: Vec<&str> = rows.iter().map(|r| r.path.as_str()).collect();
        let mut sorted = paths.clone();
        sorted.sort_unstable();
        assert_eq!(paths, sorted, "tasks are ordered by path");
        let mut total_records = 0;
        for row in &rows {
            assert!(
                row.path
                    .starts_with("test/data/generated/partitioned_identity_v2/data/"),
                "{}",
                row.path
            );
            assert_eq!(row.start, 0);
            assert_eq!(row.length, row.file_size);
            assert!(row.file_size > 0);
            assert!(row.record_count == 2 || row.record_count == 3, "{row:?}");
            total_records += row.record_count;
            assert!(matches!(row.data_sequence_number, Some(1..=3)), "{row:?}");
            assert_eq!(row.first_row_id, None);
            assert_eq!(row.partition_spec_id, 0);
            assert_eq!(row.n_positional_deletes, 0);
            assert_eq!(row.n_equality_deletes, 0);
            assert_eq!(row.format, IcebergDataFileFormat::Parquet);
        }
        assert_eq!(total_records, 18);

        let r = iceberg_scan_file_task(scan, rows.len(), std::ptr::null_mut(), collect_task);
        assert_eq!(r.tag, IcebergResultTag::Err);
        let e = error_of(r.err);
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
        assert!(e.message.contains("out of range"), "{}", e.message);

        free_tasks(rows);
        iceberg_scan_free(scan);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn partition_constants_are_identity_only() {
        let rt = new_test_runtime();

        // partitioned_identity_v2: identity(region) + bucket[4](user_id).
        let (t, _c) = load(rt, IDENTITY);
        let scan = build(rt, t, |_| ()).expect("build");
        let rows = tasks_of(scan);
        for row in &rows {
            let region: i64 = if row.path.contains("region=1/") {
                1
            } else if row.path.contains("region=2/") {
                2
            } else {
                3
            };
            assert_eq!(
                partition_constants(row.handle),
                vec![(2, IcebergLiteralKind::Int, false, region)],
                "{}",
                row.path
            );
        }
        free_tasks(rows);
        iceberg_scan_free(scan);
        iceberg_table_free(t);

        // Unpartitioned and transform-only tables yield nothing.
        for table in [SIMPLE, TRANSFORMS] {
            let (t, _c) = load(rt, table);
            let scan = build(rt, t, |_| ()).expect("build");
            let rows = tasks_of(scan);
            for row in &rows {
                assert!(partition_constants(row.handle).is_empty(), "{}", row.path);
            }
            free_tasks(rows);
            iceberg_scan_free(scan);
            iceberg_table_free(t);
        }
        iceberg_runtime_free(rt);
    }

    #[test]
    fn partition_constants_skip_null() {
        use iceberg::spec::{
            Literal, NestedField, PartitionSpec, PrimitiveType, Struct, Transform, Type,
        };
        let rt = new_test_runtime();
        let (t, _c) = load(rt, SIMPLE);
        // SAFETY: fresh non-null handle.
        let table_arc = unsafe { t.clone_arc() }.expect("table");

        let schema = Arc::new(
            Schema::builder()
                .with_fields([
                    NestedField::optional(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("a", "a", Transform::Identity)
            .expect("a")
            .add_partition_field("b", "b", Transform::Identity)
            .expect("b")
            .build()
            .expect("spec");
        let partition = Struct::from_iter([None, Some(Literal::int(7))]);
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(1)
            .with_start(0)
            .with_length(1)
            .with_data_file_path("x.parquet".to_owned())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1, 2])
            .with_partition(Some(partition))
            .with_partition_spec(Some(Arc::new(spec)))
            .with_case_sensitive(true)
            .build()
            .expect("task");
        let handle = Shared::from_arc(Arc::new(FileTask {
            _table: table_arc,
            task,
            alloc_error: crate::error::test_support::allocate_error,
        }));
        assert_eq!(partition_constants(handle), vec![(
            2,
            IcebergLiteralKind::Int,
            false,
            7
        )]);
        iceberg_file_task_free(handle);
        iceberg_table_free(t);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn equality_field_ids_are_empty_for_fixtures() {
        let rt = new_test_runtime();
        for table in [SIMPLE, IDENTITY, TRANSFORMS, EVOLUTION, TYPES, SIMPLE_V1] {
            let (t, _c) = load(rt, table);
            let scan = build(rt, t, |_| ()).expect(table.0);
            assert!(iceberg_scan_file_task_count(scan) > 0, "{}", table.0);
            assert_eq!(
                iceberg_scan_equality_field_ids(scan, std::ptr::null_mut(), 0),
                0,
                "{}",
                table.0
            );
            assert!(equality_field_ids(scan).is_empty());
            iceberg_scan_free(scan);
            iceberg_table_free(t);
        }
        iceberg_runtime_free(rt);
    }

    #[test]
    fn visit_deletes_is_empty_for_fixtures() {
        assert_eq!(IcebergDeleteContent::Data as i32, 0);
        assert_eq!(IcebergDeleteContent::PositionDeletes as i32, 1);
        assert_eq!(IcebergDeleteContent::EqualityDeletes as i32, 2);
        assert_eq!(IcebergDataFileFormat::Parquet as i32, 2);
        let rt = new_test_runtime();
        for table in [IDENTITY, SIMPLE_V1] {
            let (t, _c) = load(rt, table);
            let scan = build(rt, t, |_| ()).expect(table.0);
            let rows = tasks_of(scan);
            for row in &rows {
                assert_eq!(delete_count(row.handle), 0, "{}", row.path);
            }
            free_tasks(rows);
            iceberg_scan_free(scan);
            iceberg_table_free(t);
        }
        iceberg_runtime_free(rt);
    }

    #[test]
    fn file_task_handle_outlives_scan() {
        let rt = new_test_runtime();
        let (t, counters) = load(rt, SIMPLE);
        let scan = build(rt, t, |_| ()).expect("build");
        let mut rows = tasks_of(scan);
        let kept = rows.remove(0).handle;
        free_tasks(rows);
        iceberg_scan_free(scan);
        iceberg_table_free(t);
        // The task keeps the table, and with it the file system, alive.
        assert!(!counters.released.load(Ordering::SeqCst));
        assert_eq!(project_field_ids(kept), (1..=15).collect::<Vec<_>>());
        iceberg_file_task_free(kept);
        assert!(counters.released.load(Ordering::SeqCst));
        iceberg_runtime_free(rt);
    }

    // ----------------------------------------------------------------------
    // Null handles
    // ----------------------------------------------------------------------

    #[test]
    fn null_handles_are_reported() {
        let rt = new_test_runtime();
        let null_table = Shared::<Table>::null();
        let r = iceberg_scan_builder_new(null_table);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.ok.is_null());
        assert!(r.err.is_null());

        let null_builder = Exclusive::<ScanBuilder>::null();
        let r = iceberg_scan_builder_snapshot_id(null_builder, 1);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.err.is_null());
        let r = iceberg_scan_builder_case_sensitive(null_builder, false);
        assert_eq!(r.tag, IcebergResultTag::Err);
        let r = iceberg_scan_builder_select_field_ids(null_builder, std::ptr::null(), 0);
        assert_eq!(r.tag, IcebergResultTag::Err);
        let r = set_predicate(null_builder, &|_| ICEBERG_PRED_NONE);
        assert_eq!(r.tag, IcebergResultTag::Err);
        iceberg_scan_builder_free(null_builder);

        // Building a null builder on a live runtime reports through the
        // runtime's allocator.
        let r = iceberg_scan_builder_build(rt, null_builder);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.ok.is_null());
        assert_eq!(error_of(r.err).code, IcebergErrorCode::InvalidHandle);

        // A null runtime still consumes (frees) the builder.
        let (t, _c) = load(rt, SIMPLE);
        let b = iceberg_scan_builder_new(t);
        assert_eq!(b.tag, IcebergResultTag::Ok);
        let r = iceberg_scan_builder_build(Shared::null(), b.ok);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.ok.is_null());
        assert!(r.err.is_null());
        iceberg_table_free(t);

        let null_scan = Shared::<Scan>::null();
        assert_eq!(iceberg_scan_file_task_count(null_scan), 0);
        assert_eq!(iceberg_scan_snapshot_id(null_scan).get(), None);
        assert_eq!(
            iceberg_scan_equality_field_ids(null_scan, std::ptr::null_mut(), 0),
            0
        );
        let r = iceberg_scan_file_task(null_scan, 0, std::ptr::null_mut(), collect_task);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.err.is_null());
        iceberg_scan_free(null_scan);

        let null_task = Shared::<FileTask>::null();
        assert_eq!(
            iceberg_file_task_project_field_ids(null_task, std::ptr::null_mut(), 0),
            0
        );
        let r = iceberg_file_task_visit_partition_constants(
            null_task,
            std::ptr::null_mut(),
            collect_constant,
        );
        assert_eq!(r.tag, IcebergResultTag::Err);
        let r = iceberg_file_task_visit_deletes(null_task, std::ptr::null_mut(), count_delete);
        assert_eq!(r.tag, IcebergResultTag::Err);
        iceberg_file_task_free(null_task);

        iceberg_runtime_free(rt);
    }

    // ----------------------------------------------------------------------
    // Planning benchmark driver (ignored; see iceberg-ffi/README.md)
    // ----------------------------------------------------------------------

    /// Plans the table at `ICEBERG_FFI_BENCH_METADATA` `ICEBERG_FFI_BENCH_ITERS`
    /// times (default 20) through the `std::fs` vtable and prints the time
    /// and vtable calls per iteration, so a profiler attached to this test
    /// binary sees nothing but planning. By default every iteration loads a
    /// fresh table, as the engine does for every query; with
    /// `ICEBERG_FFI_BENCH_RELOAD=0` one table is reused and iceberg-rust's
    /// per-table manifest cache serves the later iterations. Skips when the
    /// metadata variable is unset.
    #[test]
    #[ignore = "benchmark driver; needs ICEBERG_FFI_BENCH_METADATA"]
    fn plan_bench() {
        use crate::error::test_support::allocate_error;
        use crate::runtime::iceberg_runtime_new;
        use crate::table::test_support::load_with;

        let Ok(metadata) = std::env::var("ICEBERG_FFI_BENCH_METADATA") else {
            eprintln!("plan_bench: ICEBERG_FFI_BENCH_METADATA is not set; skipping");
            return;
        };
        let iters: usize = std::env::var("ICEBERG_FFI_BENCH_ITERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20);
        let reload = std::env::var("ICEBERG_FFI_BENCH_RELOAD").map_or(true, |v| v != "0");
        // Production sizing (4 workers, 64 blocking threads), not the test
        // runtime's 2/2, so the numbers match the engine.
        let r = iceberg_runtime_new(
            4,
            64,
            allocate_error as AllocateErrorFn,
            None,
            std::ptr::null_mut(),
        );
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        let rt = r.ok;
        let load = |label: &str| {
            let start = Instant::now();
            let (result, counters) = load_with(rt, &metadata, None);
            assert_eq!(result.tag, IcebergResultTag::Ok, "{:?}", take(result.err));
            let micros = start.elapsed().as_micros();
            eprintln!(
                "plan_bench: {label} loaded {metadata} in {micros} us ({} opens, {} reads)",
                counters.opens.load(Ordering::SeqCst),
                counters.reads.load(Ordering::SeqCst)
            );
            (result.ok, counters, micros)
        };
        let mut shared = if reload { None } else { Some(load("once")) };

        let mut plan_micros = Vec::with_capacity(iters);
        let mut load_micros = Vec::with_capacity(iters);
        for i in 0..iters {
            let (t, counters, loaded_us) = match shared.take() {
                Some(loaded) => loaded,
                None => load("iter"),
            };
            let (opens, reads, closes) = (
                counters.opens.load(Ordering::SeqCst),
                counters.reads.load(Ordering::SeqCst),
                counters.closes.load(Ordering::SeqCst),
            );
            let start = Instant::now();
            let scan = build(rt, t, |_| ()).expect("build");
            let elapsed = start.elapsed().as_micros();
            let tasks = iceberg_scan_file_task_count(scan);
            iceberg_scan_free(scan);
            plan_micros.push(elapsed);
            load_micros.push(loaded_us);
            eprintln!(
                "plan_bench: iter {i:>3} plan {elapsed:>9} us tasks={tasks} opens={} reads={} closes={}",
                counters.opens.load(Ordering::SeqCst) - opens,
                counters.reads.load(Ordering::SeqCst) - reads,
                counters.closes.load(Ordering::SeqCst) - closes,
            );
            if reload {
                iceberg_table_free(t);
            } else {
                shared = Some((t, counters, 0));
            }
        }
        if let Some((t, _, _)) = shared.take() {
            iceberg_table_free(t);
        }
        plan_micros.sort_unstable();
        load_micros.sort_unstable();
        eprintln!(
            "plan_bench: {iters} iterations, plan min {} us, median {} us, max {} us; load median {} us",
            plan_micros[0],
            plan_micros[plan_micros.len() / 2],
            plan_micros[plan_micros.len() - 1],
            load_micros[load_micros.len() / 2]
        );
        iceberg_runtime_free(rt);
    }
}
