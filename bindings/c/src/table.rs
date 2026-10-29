//! Loading a table from its metadata file and reading its metadata.
//!
//! `iceberg_table_load` reads a metadata JSON file through the engine's
//! [`IcebergFileSystemVTable`] and returns a `Shared<Table>` handle. The
//! accessors in this module are synchronous and only need the table handle;
//! the engine's error allocator is copied from the runtime at load time so
//! that errors can still be reported without a runtime handle.
//!
//! Collections (snapshots, partition fields, schema fields) are exposed
//! through callbacks rather than arrays, so no Rust memory is ever owned by
//! the engine.

use std::ffi::c_void;
use std::sync::Arc;

use iceberg::TableIdent;
use iceberg::spec::TableMetadataRef;
use iceberg::table::StaticTable;

use crate::error::{
    AllocateErrorFn, FfiError, FfiResult, IcebergErrorCode, IcebergResultOptionalI32,
    IcebergResultOptionalI64, IcebergResultTable, IcebergResultUnit,
};
use crate::guard::ErrorSource;
use crate::handle::Shared;
use crate::optional::{IcebergOptionalI32, IcebergOptionalI64};
use crate::runtime::Runtime;
use crate::storage::{DuckDbStorage, FsHandle, IcebergFileSystemVTable, file_io};
use crate::strings::{AllocateStringFn, StringSlice};

/// Namespace and name given to tables loaded from a metadata file; no
/// catalog is involved, so the identifier is purely informational.
const TABLE_IDENT: [&str; 2] = ["iceberg_rs", "table"];

/// State behind a `Shared<Table>` handle.
pub struct Table {
    inner: StaticTable,
    alloc_error: AllocateErrorFn,
}

// A `Shared<Table>` may be used from any engine thread, so the payload must be
// `Send + Sync`; this fails to compile otherwise.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Table>();
};

impl Table {
    /// The table metadata (an `Arc`, cheap to clone).
    pub(crate) fn metadata(&self) -> TableMetadataRef {
        self.inner.metadata()
    }

    /// Resolves a handle to a table reference, reporting null handles.
    ///
    /// # Safety
    ///
    /// See [`Shared::borrow`].
    pub(crate) unsafe fn from_handle(handle: &Shared<Table>) -> FfiResult<&Table> {
        // SAFETY: forwarded to the caller.
        unsafe { handle.borrow() }.ok_or_else(|| FfiError::invalid_handle("table"))
    }
}

impl ErrorSource for Table {
    fn alloc_error(&self) -> AllocateErrorFn {
        self.alloc_error
    }
}

/// Decodes a borrowed argument slice, reporting invalid UTF-8 as
/// [`IcebergErrorCode::InvalidArgument`].
///
/// # Safety
///
/// See [`StringSlice::as_str`].
unsafe fn argument<'a>(slice: &StringSlice, what: &str) -> FfiResult<&'a str> {
    // SAFETY: forwarded to the caller.
    unsafe { slice.as_str() }.map_err(|e| {
        FfiError::new(
            IcebergErrorCode::InvalidArgument,
            format!("{what} is not valid UTF-8: {e}"),
        )
    })
}

/// Combines the two relocation arguments: both empty means no relocation,
/// both set means relocate, anything else is a caller error.
fn relocation(from: &str, to: &str) -> FfiResult<Option<(String, String)>> {
    match (from.is_empty(), to.is_empty()) {
        (true, true) => Ok(None),
        (false, false) => Ok(Some((from.to_owned(), to.to_owned()))),
        _ => Err(FfiError::new(
            IcebergErrorCode::InvalidArgument,
            "relocate_from and relocate_to must be both empty or both set",
        )),
    }
}

/// Reads `metadata_location` through `fs` and builds the table.
fn load(
    rt: &Runtime,
    fs: FsHandle,
    metadata_location: &StringSlice,
    relocate_from: &StringSlice,
    relocate_to: &StringSlice,
) -> FfiResult<Shared<Table>> {
    // SAFETY: the engine keeps the argument slices alive for the duration of
    // the call.
    let location = unsafe { argument(metadata_location, "metadata_location") }?;
    // SAFETY: as above.
    let from = unsafe { argument(relocate_from, "relocate_from") }?;
    // SAFETY: as above.
    let to = unsafe { argument(relocate_to, "relocate_to") }?;
    if location.is_empty() {
        return Err(FfiError::new(
            IcebergErrorCode::InvalidArgument,
            "metadata_location must not be empty",
        ));
    }
    let storage = DuckDbStorage::from_fs(fs, relocation(from, to)?);
    let ident = TableIdent::from_strs(TABLE_IDENT)?;
    let inner = rt.block_on(StaticTable::from_metadata_file(
        location,
        ident,
        file_io(storage),
    ))??;
    Ok(Shared::from_arc(Arc::new(Table {
        inner,
        alloc_error: rt.alloc_error(),
    })))
}

/// Loads a table from a metadata JSON file.
///
/// `fs` is the engine file system every read goes through; this call takes
/// ownership of `fs.ctx` and `release` runs exactly once, either from
/// `iceberg_table_free` or, when loading fails (including a null runtime),
/// before this function returns. `metadata_location` is the path of the
/// `*.metadata.json` file, as the engine's file system understands it; gzip
/// compressed metadata is detected from the content.
///
/// `relocate_from` and `relocate_to` are either both empty (no relocation)
/// or both set, in which case every path recorded in the table that starts
/// with `relocate_from` is rewritten to start with `relocate_to` before it
/// reaches `fs`. One empty and one set is `IcebergErrorCodeInvalidArgument`.
///
/// On success the engine owns the returned handle and must release it with
/// `iceberg_table_free`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_table_load(
    runtime: Shared<Runtime>,
    fs: IcebergFileSystemVTable,
    metadata_location: StringSlice,
    relocate_from: StringSlice,
    relocate_to: StringSlice,
) -> IcebergResultTable {
    // Owning the vtable is an infallible move, so it happens before the guard;
    // from here on `release` runs on every exit path.
    let fs = FsHandle::new(fs);
    with_runtime!(runtime, IcebergResultTable, |rt| {
        load(rt, fs, &metadata_location, &relocate_from, &relocate_to)
    })
}

/// Releases the engine's reference to a table.
///
/// When this was the last reference, every open file is closed and the file
/// system vtable's `release` is called. A null handle is ignored.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_table_free(table: Shared<Table>) {
    ffi_guard!(|_msg| (), {
        // SAFETY: the engine gives up its reference by calling free.
        drop(unsafe { table.into_arc() });
    })
}

/// The table's format version (1, 2 or 3); 0 for a null handle.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_table_format_version(table: Shared<Table>) -> i32 {
    ffi_guard!(|_msg| 0, {
        // SAFETY: the engine passes a handle from `iceberg_table_load`.
        match unsafe { Table::from_handle(&table) } {
            Ok(t) => t.metadata().format_version() as i32,
            Err(_) => 0,
        }
    })
}

/// The table's root location as recorded in its metadata, through `alloc`.
///
/// Returns the pointer `alloc` produced, or null for a null handle.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_table_location(
    table: Shared<Table>,
    alloc: AllocateStringFn,
) -> *mut c_void {
    ffi_guard!(|_msg| std::ptr::null_mut(), {
        // SAFETY: the engine passes a handle from `iceberg_table_load`.
        match unsafe { Table::from_handle(&table) } {
            Ok(t) => alloc(StringSlice::borrowed(t.metadata().location())),
            Err(_) => std::ptr::null_mut(),
        }
    })
}

/// The value of table property `key`, through `alloc`.
///
/// Returns null when the handle is null, `key` is not valid UTF-8, or the
/// property is absent.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_table_property(
    table: Shared<Table>,
    key: StringSlice,
    alloc: AllocateStringFn,
) -> *mut c_void {
    ffi_guard!(|_msg| std::ptr::null_mut(), {
        // SAFETY: the engine passes a handle from `iceberg_table_load`.
        let resolved = unsafe { Table::from_handle(&table) };
        let Ok(t) = resolved else {
            return std::ptr::null_mut();
        };
        // SAFETY: the engine keeps `key` alive for the duration of the call.
        let decoded = unsafe { key.as_str() };
        let Ok(key) = decoded else {
            return std::ptr::null_mut();
        };
        match t.metadata().properties().get(key) {
            Some(value) => alloc(StringSlice::borrowed(value)),
            None => std::ptr::null_mut(),
        }
    })
}

/// The current snapshot id; absent when the table has no snapshot or the
/// handle is null.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_table_current_snapshot_id(table: Shared<Table>) -> IcebergOptionalI64 {
    ffi_guard!(|_msg| IcebergOptionalI64::NONE, {
        // SAFETY: the engine passes a handle from `iceberg_table_load`.
        match unsafe { Table::from_handle(&table) } {
            Ok(t) => t.metadata().current_snapshot_id().into(),
            Err(_) => IcebergOptionalI64::NONE,
        }
    })
}

/// The snapshot that was current at `timestamp_ms` (milliseconds since the
/// Unix epoch), from the table's snapshot log.
///
/// Among log entries with `timestamp_ms <= ts` the latest one wins (ties go
/// to the later entry). Absent when no entry qualifies.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_table_snapshot_id_as_of_ms(
    table: Shared<Table>,
    timestamp_ms: i64,
) -> IcebergResultOptionalI64 {
    with_table!(table, IcebergResultOptionalI64, |t| {
        let metadata = t.metadata();
        let found = metadata
            .history()
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.timestamp_ms <= timestamp_ms)
            .max_by_key(|(position, entry)| (entry.timestamp_ms, *position))
            .map(|(_, entry)| entry.snapshot_id);
        Ok(found.into())
    })
}

/// The schema id recorded on snapshot `snapshot_id`.
///
/// Absent when the snapshot predates schema ids (format version 1 tables).
/// An unknown snapshot id is `IcebergErrorCodeInvalidArgument`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_table_snapshot_schema_id(
    table: Shared<Table>,
    snapshot_id: i64,
) -> IcebergResultOptionalI32 {
    with_table!(table, IcebergResultOptionalI32, |t| {
        let metadata = t.metadata();
        let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
            FfiError::new(
                IcebergErrorCode::InvalidArgument,
                format!("unknown snapshot id {snapshot_id}"),
            )
        })?;
        Ok(snapshot.schema_id().into())
    })
}

/// Receives one snapshot from `iceberg_table_visit_snapshots`.
///
/// Arguments: `ctx`, `snapshot_id`, `parent_id`, `sequence_number`,
/// `timestamp_ms`, `manifest_list`, `operation` (`append`, `replace`,
/// `overwrite` or `delete`), `schema_id`. The string slices are valid only
/// during the callback. Must not unwind.
pub type IcebergSnapshotCallbackFn = extern "C" fn(
    *mut c_void,
    i64,
    IcebergOptionalI64,
    i64,
    i64,
    StringSlice,
    StringSlice,
    IcebergOptionalI32,
);

/// Calls `cb` once per snapshot, ordered by `(sequence_number, timestamp_ms,
/// snapshot_id)`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_table_visit_snapshots(
    table: Shared<Table>,
    ctx: *mut c_void,
    cb: IcebergSnapshotCallbackFn,
) -> IcebergResultUnit {
    with_table!(table, IcebergResultUnit, |t| {
        let metadata = t.metadata();
        let mut snapshots: Vec<_> = metadata.snapshots().collect();
        snapshots.sort_by_key(|s| (s.sequence_number(), s.timestamp_ms(), s.snapshot_id()));
        for snapshot in snapshots {
            cb(
                ctx,
                snapshot.snapshot_id(),
                snapshot.parent_snapshot_id().into(),
                snapshot.sequence_number(),
                snapshot.timestamp_ms(),
                StringSlice::borrowed(snapshot.manifest_list()),
                StringSlice::borrowed(snapshot.summary().operation.as_str()),
                snapshot.schema_id().into(),
            );
        }
        Ok(())
    })
}

/// Receives one partition field from `iceberg_table_visit_partition_spec`.
///
/// Arguments: `ctx`, `field_id`, `source_id` (the schema field the transform
/// reads), `name`, `transform` in Iceberg's textual form (`identity`,
/// `bucket[4]`, `day`, ...). The string slices are valid only during the
/// callback. Must not unwind.
pub type IcebergPartitionFieldCallbackFn =
    extern "C" fn(*mut c_void, i32, i32, StringSlice, StringSlice);

/// Calls `cb` once per field of partition spec `spec_id`, in spec order.
///
/// An unknown spec id is `IcebergErrorCodeInvalidArgument`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_table_visit_partition_spec(
    table: Shared<Table>,
    spec_id: i32,
    ctx: *mut c_void,
    cb: IcebergPartitionFieldCallbackFn,
) -> IcebergResultUnit {
    with_table!(table, IcebergResultUnit, |t| {
        let metadata = t.metadata();
        let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
            FfiError::new(
                IcebergErrorCode::InvalidArgument,
                format!("unknown partition spec id {spec_id}"),
            )
        })?;
        for field in spec.fields() {
            let transform = field.transform.to_string();
            cb(
                ctx,
                field.field_id,
                field.source_id,
                StringSlice::borrowed(&field.name),
                StringSlice::borrowed(&transform),
            );
        }
        Ok(())
    })
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::error::IcebergResultTag;
    use crate::error::test_support::take;
    use crate::storage::test_support::{Counters, new_std_fs_vtable};

    /// Absolute path of a file under `test/data/generated/`.
    pub(crate) fn fixture(relative: &str) -> String {
        format!(
            "{}/../test/data/generated/{relative}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    /// Loads `metadata_location` through a fresh `std::fs` vtable.
    pub(crate) fn load_with(
        runtime: Shared<Runtime>,
        metadata_location: &str,
        relocate: Option<(&str, &str)>,
    ) -> (IcebergResultTable, Arc<Counters>) {
        let (vtable, counters) = new_std_fs_vtable();
        let (from, to) = relocate.unwrap_or(("", ""));
        let result = iceberg_table_load(
            runtime,
            vtable,
            StringSlice::borrowed(metadata_location),
            StringSlice::borrowed(from),
            StringSlice::borrowed(to),
        );
        (result, counters)
    }

    /// Loads a fixture and asserts success.
    pub(crate) fn load_fixture(
        runtime: Shared<Runtime>,
        relative: &str,
    ) -> (Shared<Table>, Arc<Counters>) {
        let (result, counters) = load_with(runtime, &fixture(relative), None);
        assert_eq!(result.tag, IcebergResultTag::Ok, "{:?}", take(result.err));
        assert!(!result.ok.is_null());
        (result.ok, counters)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;

    use super::test_support::{fixture, load_fixture, load_with};
    use super::*;
    use crate::error::IcebergResultTag;
    use crate::error::test_support::{TestError, take};
    use crate::runtime::iceberg_runtime_free;
    use crate::runtime::test_support::new_test_runtime;
    use crate::strings::test_support::{allocate_string, take as take_string};

    const SIMPLE_V3: &str = "simple_v2/metadata/v3.metadata.json";
    const SIMPLE_LOCATION: &str = "test/data/generated/simple_v2";

    /// One row from `iceberg_table_visit_snapshots`.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SnapshotRow {
        snapshot_id: i64,
        parent_id: Option<i64>,
        sequence_number: i64,
        timestamp_ms: i64,
        manifest_list: String,
        operation: String,
        schema_id: Option<i32>,
    }

    extern "C" fn collect_snapshot(
        ctx: *mut c_void,
        snapshot_id: i64,
        parent_id: IcebergOptionalI64,
        sequence_number: i64,
        timestamp_ms: i64,
        manifest_list: StringSlice,
        operation: StringSlice,
        schema_id: IcebergOptionalI32,
    ) {
        // SAFETY: `ctx` is the `Mutex<Vec<SnapshotRow>>` the test passed, and
        // the slices are valid for the duration of the callback.
        let rows = unsafe { &*ctx.cast::<Mutex<Vec<SnapshotRow>>>() };
        let manifest_list = unsafe { manifest_list.as_str() }.unwrap_or("?").to_owned();
        let operation = unsafe { operation.as_str() }.unwrap_or("?").to_owned();
        if let Ok(mut rows) = rows.lock() {
            rows.push(SnapshotRow {
                snapshot_id,
                parent_id: parent_id.get(),
                sequence_number,
                timestamp_ms,
                manifest_list,
                operation,
                schema_id: schema_id.get(),
            });
        }
    }

    fn snapshots(table: Shared<Table>) -> Vec<SnapshotRow> {
        let rows = Mutex::new(Vec::new());
        let r = iceberg_table_visit_snapshots(
            table,
            (&rows as *const Mutex<Vec<SnapshotRow>>).cast_mut().cast(),
            collect_snapshot,
        );
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        rows.into_inner().unwrap_or_default()
    }

    fn property(table: Shared<Table>, key: &str) -> Option<String> {
        take_string(iceberg_table_property(
            table,
            StringSlice::borrowed(key),
            allocate_string,
        ))
    }

    fn error_of(err: *mut crate::error::EngineError) -> TestError {
        match take(err) {
            Some(e) => e,
            None => TestError {
                code: IcebergErrorCode::Ok,
                message: String::from("<no error object>"),
            },
        }
    }

    #[test]
    fn load_exposes_metadata() {
        let rt = new_test_runtime();
        let (table, counters) = load_fixture(rt, SIMPLE_V3);

        assert_eq!(iceberg_table_format_version(table), 2);
        assert_eq!(
            take_string(iceberg_table_location(table, allocate_string)),
            Some(SIMPLE_LOCATION.to_owned())
        );
        assert_eq!(property(table, "owner"), Some("iceberg_rs".to_owned()));
        assert_eq!(property(table, "no-such-property"), None);

        let rows = snapshots(table);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            iceberg_table_current_snapshot_id(table).get(),
            Some(rows[1].snapshot_id)
        );

        iceberg_table_free(table);
        assert!(counters.released.load(Ordering::SeqCst));
        assert_eq!(
            counters.opens.load(Ordering::SeqCst),
            counters.closes.load(Ordering::SeqCst)
        );
        iceberg_runtime_free(rt);
    }

    #[test]
    fn visit_snapshots_is_ordered_and_complete() {
        let rt = new_test_runtime();
        let (table, _counters) = load_fixture(rt, SIMPLE_V3);
        let rows = snapshots(table);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].sequence_number, 1);
        assert_eq!(rows[1].sequence_number, 2);
        assert_eq!(rows[0].parent_id, None);
        assert_eq!(rows[1].parent_id, Some(rows[0].snapshot_id));
        assert!(rows[0].timestamp_ms <= rows[1].timestamp_ms);
        for row in &rows {
            assert_eq!(row.operation, "append");
            assert!(
                row.manifest_list
                    .starts_with("test/data/generated/simple_v2/metadata/snap-"),
                "{}",
                row.manifest_list
            );
            assert_eq!(row.schema_id, Some(0));
        }

        iceberg_table_free(table);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn snapshot_id_as_of_ms_follows_the_log() {
        let rt = new_test_runtime();
        let (table, _counters) = load_fixture(rt, SIMPLE_V3);
        let rows = snapshots(table);
        let first = &rows[0];
        let last = &rows[rows.len() - 1];

        let as_of = |ts: i64| {
            let r = iceberg_table_snapshot_id_as_of_ms(table, ts);
            assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
            r.ok.get()
        };
        assert_eq!(as_of(first.timestamp_ms - 1), None);
        assert_eq!(as_of(first.timestamp_ms), Some(first.snapshot_id));
        assert_eq!(as_of(i64::MAX), Some(last.snapshot_id));

        iceberg_table_free(table);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn snapshot_schema_id_checks_the_snapshot() {
        let rt = new_test_runtime();
        let (table, _counters) = load_fixture(rt, SIMPLE_V3);
        let rows = snapshots(table);

        let r = iceberg_table_snapshot_schema_id(table, rows[0].snapshot_id);
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        assert_eq!(r.ok.get(), Some(0));

        let r = iceberg_table_snapshot_schema_id(table, 42);
        assert_eq!(r.tag, IcebergResultTag::Err);
        let e = error_of(r.err);
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
        assert!(e.message.contains("42"), "{}", e.message);

        iceberg_table_free(table);
        iceberg_runtime_free(rt);
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct PartitionRow {
        field_id: i32,
        source_id: i32,
        name: String,
        transform: String,
    }

    extern "C" fn collect_partition_field(
        ctx: *mut c_void,
        field_id: i32,
        source_id: i32,
        name: StringSlice,
        transform: StringSlice,
    ) {
        // SAFETY: `ctx` is the `Mutex<Vec<PartitionRow>>` the test passed, and
        // the slices are valid for the duration of the callback.
        let rows = unsafe { &*ctx.cast::<Mutex<Vec<PartitionRow>>>() };
        let name = unsafe { name.as_str() }.unwrap_or("?").to_owned();
        let transform = unsafe { transform.as_str() }.unwrap_or("?").to_owned();
        if let Ok(mut rows) = rows.lock() {
            rows.push(PartitionRow {
                field_id,
                source_id,
                name,
                transform,
            });
        }
    }

    #[test]
    fn visit_partition_spec_lists_fields_in_order() {
        let rt = new_test_runtime();
        let (table, _counters) =
            load_fixture(rt, "partitioned_identity_v2/metadata/v4.metadata.json");

        let rows = Mutex::new(Vec::new());
        let r = iceberg_table_visit_partition_spec(
            table,
            0,
            (&rows as *const Mutex<Vec<PartitionRow>>).cast_mut().cast(),
            collect_partition_field,
        );
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        let rows = rows.into_inner().unwrap_or_default();
        assert_eq!(
            rows,
            vec![
                PartitionRow {
                    field_id: 1000,
                    source_id: 2,
                    name: "region".to_owned(),
                    transform: "identity".to_owned(),
                },
                PartitionRow {
                    field_id: 1001,
                    source_id: 3,
                    name: "user_id_bucket".to_owned(),
                    transform: "bucket[4]".to_owned(),
                },
            ]
        );

        let r = iceberg_table_visit_partition_spec(
            table,
            7,
            std::ptr::null_mut(),
            collect_partition_field,
        );
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert_eq!(error_of(r.err).code, IcebergErrorCode::InvalidArgument);

        iceberg_table_free(table);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn relocation_rewrites_paths_before_the_engine_sees_them() {
        let rt = new_test_runtime();
        let repo_relative = format!("{SIMPLE_LOCATION}/metadata/v3.metadata.json");
        let moved_root = fixture("simple_v2_moved");

        let (r, counters) = load_with(
            rt,
            &repo_relative,
            Some((SIMPLE_LOCATION, moved_root.as_str())),
        );
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        let opened = counters
            .opened_paths
            .lock()
            .map(|p| p.clone())
            .unwrap_or_default();
        assert_eq!(
            opened,
            vec![format!("{moved_root}/metadata/v3.metadata.json")]
        );
        // The logical location is untouched.
        assert_eq!(
            take_string(iceberg_table_location(r.ok, allocate_string)),
            Some(SIMPLE_LOCATION.to_owned())
        );
        iceberg_table_free(r.ok);
        assert!(counters.released.load(Ordering::SeqCst));

        // The same repo-relative path does not exist from the crate directory.
        let (r, counters) = load_with(rt, &repo_relative, None);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.ok.is_null());
        let e = error_of(r.err);
        assert_eq!(e.code, IcebergErrorCode::DataInvalid);
        assert!(e.message.contains("does not exist"), "{}", e.message);
        assert!(counters.released.load(Ordering::SeqCst));

        iceberg_runtime_free(rt);
    }

    #[test]
    fn half_specified_relocation_is_rejected() {
        let rt = new_test_runtime();
        let (r, counters) = load_with(rt, &fixture(SIMPLE_V3), Some(("", "/elsewhere")));
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert_eq!(error_of(r.err).code, IcebergErrorCode::InvalidArgument);
        assert!(counters.released.load(Ordering::SeqCst));
        iceberg_runtime_free(rt);
    }

    #[test]
    fn non_table_json_is_data_invalid() {
        let rt = new_test_runtime();
        let (r, counters) = load_with(rt, &fixture("not_iceberg/metadata/v1.metadata.json"), None);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert_eq!(error_of(r.err).code, IcebergErrorCode::DataInvalid);
        assert!(counters.released.load(Ordering::SeqCst));
        iceberg_runtime_free(rt);
    }

    #[test]
    fn gzip_metadata_is_detected_from_content() {
        let rt = new_test_runtime();
        let (table, _counters) = load_fixture(rt, "simple_v2_gzip/metadata/v3.gz.metadata.json");
        assert_eq!(snapshots(table).len(), 2);
        iceberg_table_free(table);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn null_runtime_releases_the_vtable() {
        let (r, counters) = load_with(Shared::null(), &fixture(SIMPLE_V3), None);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.ok.is_null());
        // No runtime means no error allocator, so the InvalidHandle error has
        // no object to travel in.
        assert!(r.err.is_null());
        assert!(counters.released.load(Ordering::SeqCst));
        assert_eq!(counters.opens.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn null_table_handle_is_harmless() {
        let null = Shared::<Table>::null();
        assert_eq!(iceberg_table_format_version(null), 0);
        assert!(iceberg_table_location(null, allocate_string).is_null());
        assert!(
            iceberg_table_property(null, StringSlice::borrowed("owner"), allocate_string).is_null()
        );
        assert_eq!(iceberg_table_current_snapshot_id(null).get(), None);
        let r = iceberg_table_snapshot_id_as_of_ms(null, 0);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.err.is_null());
        let r = iceberg_table_visit_snapshots(null, std::ptr::null_mut(), collect_snapshot);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.err.is_null());
        iceberg_table_free(null);
    }
}
