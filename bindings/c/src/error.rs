//! Error codes, engine-owned error objects, and per-type result structs.
//!
//! Rust never allocates error memory. When a call fails, the crate invokes the
//! [`AllocateErrorFn`] the engine registered with the runtime, passing a code
//! and a message; the engine allocates whatever it wants to throw later and
//! returns an opaque pointer, which travels back inside the result struct.
//!
//! Result structs are written out once per payload type so the generated header
//! contains plain C structs and no templates.

use crate::handle::{Exclusive, Shared};
use crate::optional::{IcebergOptionalI32, IcebergOptionalI64};
use crate::runtime::Runtime;
use crate::scan::{Scan, ScanBuilder};
use crate::strings::StringSlice;
use crate::table::Table;

/// Error classification returned to the engine.
///
/// The first group mirrors `iceberg::ErrorKind`; the second group is specific
/// to the FFI layer. Discriminants are contiguous from zero so the engine can
/// index a string table by code; [`ICEBERG_ERROR_CODE_COUNT`] gives the table
/// size, and a test in this module keeps it in step with the variant list.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergErrorCode {
    /// No error.
    Ok = 0,
    /// Unexpected failure inside iceberg-rust (I/O, serialization, ...).
    Unexpected = 1,
    /// Metadata or data that violates the Iceberg specification.
    DataInvalid = 2,
    /// A spec feature iceberg-rust does not implement.
    FeatureUnsupported = 3,
    /// A caller-visible precondition failed.
    PreconditionFailed = 4,
    /// A namespace or table does not exist.
    NotFound = 5,
    /// A namespace or table already exists.
    AlreadyExists = 6,
    /// A catalog commit conflicted with a concurrent commit.
    CommitConflict = 7,
    /// A predicate could not be bound to the table schema.
    PredicateBindFailed = 8,
    /// A null or otherwise invalid handle was passed.
    InvalidHandle = 9,
    /// A scalar argument was out of range.
    InvalidArgument = 10,
    /// A Rust panic was caught at the boundary.
    Panic = 11,
}

/// Number of [`IcebergErrorCode`] variants; the error-code string table on the
/// engine side must have exactly this many entries.
pub const ICEBERG_ERROR_CODE_COUNT: usize = 12;

impl IcebergErrorCode {
    /// Maps an iceberg-rust error kind to the FFI code.
    pub fn from_kind(kind: iceberg::ErrorKind) -> Self {
        use iceberg::ErrorKind as K;
        match kind {
            K::Unexpected => Self::Unexpected,
            K::DataInvalid => Self::DataInvalid,
            K::FeatureUnsupported => Self::FeatureUnsupported,
            K::PreconditionFailed => Self::PreconditionFailed,
            K::NamespaceNotFound | K::TableNotFound => Self::NotFound,
            K::NamespaceAlreadyExists | K::TableAlreadyExists => Self::AlreadyExists,
            K::CatalogCommitConflicts => Self::CommitConflict,
            // ErrorKind is #[non_exhaustive]; new kinds surface as Unexpected
            // until they get a code of their own.
            _ => Self::Unexpected,
        }
    }
}

/// Opaque engine-owned error object.
///
/// Only ever handled by pointer. The engine defines the real type behind it
/// and owns every instance; Rust creates instances through
/// [`AllocateErrorFn`] and never reads or frees them.
pub struct EngineError {
    _private: [u8; 0],
}

/// Engine callback that allocates an [`EngineError`] for a code and message.
///
/// Must not be null: the type is a plain function pointer, not an optional
/// one, and passing a null pointer where it is expected is undefined
/// behaviour. The callback must not unwind. The message slice is only valid
/// during the callback. The engine owns the returned object and must free it
/// after unpacking the result that carries it; it may return null when it
/// cannot allocate, in which case the result carries a null `err`.
pub type AllocateErrorFn = extern "C" fn(IcebergErrorCode, StringSlice) -> *mut EngineError;

/// Which arm of a result struct is populated.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergResultTag {
    /// `ok` holds the payload; `err` is null.
    Ok = 0,
    /// `err` holds the engine error; `ok` is a placeholder. `err` is null only
    /// when no allocator was reachable (for example, a null runtime handle).
    Err = 1,
}

/// Internal error carried between the function body and the result struct.
#[derive(Debug)]
pub struct FfiError {
    /// Classification.
    pub code: IcebergErrorCode,
    /// Human-readable message.
    pub message: String,
}

impl FfiError {
    /// Builds an error from a code and message.
    pub fn new(code: IcebergErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// Error for a null or dangling handle.
    pub fn invalid_handle(what: &str) -> Self {
        Self::new(
            IcebergErrorCode::InvalidHandle,
            format!("{what} handle is null"),
        )
    }

    /// Error for a caught panic.
    pub fn panic(message: String) -> Self {
        Self::new(IcebergErrorCode::Panic, message)
    }

    /// Hands the error to the engine allocator, if one is available.
    pub fn allocate(&self, alloc: Option<AllocateErrorFn>) -> *mut EngineError {
        match alloc {
            Some(f) => f(self.code, StringSlice::borrowed(&self.message)),
            None => std::ptr::null_mut(),
        }
    }
}

impl From<iceberg::Error> for FfiError {
    fn from(e: iceberg::Error) -> Self {
        Self::new(IcebergErrorCode::from_kind(e.kind()), e.to_string())
    }
}

/// Convenience alias used inside function bodies.
pub type FfiResult<T> = Result<T, FfiError>;

/// Implements the constructors shared by every result struct.
///
/// The struct itself is declared by hand so cbindgen, which does not expand
/// macros, sees it; this macro only adds the `ok`/`err`/`from_result`
/// helpers. `$placeholder` is the value stored in `ok` on the error arm.
macro_rules! impl_iceberg_result {
    ($name:ident, $ok:ty, $placeholder:expr) => {
        impl $name {
            /// Success result.
            pub fn ok(value: $ok) -> Self {
                Self {
                    tag: IcebergResultTag::Ok,
                    ok: value,
                    err: std::ptr::null_mut(),
                }
            }

            /// Error result carrying an engine error (possibly null).
            pub fn err(err: *mut EngineError) -> Self {
                Self {
                    tag: IcebergResultTag::Err,
                    ok: $placeholder,
                    err,
                }
            }

            /// Converts a body result, allocating the error through `alloc`.
            pub fn from_result(result: FfiResult<$ok>, alloc: Option<AllocateErrorFn>) -> Self {
                match result {
                    Ok(v) => Self::ok(v),
                    Err(e) => Self::err(e.allocate(alloc)),
                }
            }
        }
    };
}

/// Result of `iceberg_runtime_new`.
#[repr(C)]
pub struct IcebergResultRuntime {
    /// Which arm is populated.
    pub tag: IcebergResultTag,
    /// The runtime handle on success; null on error.
    pub ok: Shared<Runtime>,
    /// The engine error on failure; null on success.
    pub err: *mut EngineError,
}
impl_iceberg_result!(IcebergResultRuntime, Shared<Runtime>, Shared::null());

/// Result carrying a `uint64_t`.
#[repr(C)]
pub struct IcebergResultU64 {
    /// Which arm is populated.
    pub tag: IcebergResultTag,
    /// The value on success; zero on error.
    pub ok: u64,
    /// The engine error on failure; null on success.
    pub err: *mut EngineError,
}
impl_iceberg_result!(IcebergResultU64, u64, 0);

/// Result carrying an `int32_t`.
#[repr(C)]
pub struct IcebergResultI32 {
    /// Which arm is populated.
    pub tag: IcebergResultTag,
    /// The value on success; zero on error.
    pub ok: i32,
    /// The engine error on failure; null on success.
    pub err: *mut EngineError,
}
impl_iceberg_result!(IcebergResultI32, i32, 0);

/// Result of `iceberg_table_load`.
#[repr(C)]
pub struct IcebergResultTable {
    /// Which arm is populated.
    pub tag: IcebergResultTag,
    /// The table handle on success; null on error.
    pub ok: Shared<Table>,
    /// The engine error on failure; null on success.
    pub err: *mut EngineError,
}
impl_iceberg_result!(IcebergResultTable, Shared<Table>, Shared::null());

/// Result of `iceberg_scan_builder_new`.
#[repr(C)]
pub struct IcebergResultScanBuilder {
    /// Which arm is populated.
    pub tag: IcebergResultTag,
    /// The scan builder handle on success; null on error.
    pub ok: Exclusive<ScanBuilder>,
    /// The engine error on failure; null on success.
    pub err: *mut EngineError,
}
impl_iceberg_result!(
    IcebergResultScanBuilder,
    Exclusive<ScanBuilder>,
    Exclusive::null()
);

/// Result of `iceberg_scan_builder_build`.
#[repr(C)]
pub struct IcebergResultScan {
    /// Which arm is populated.
    pub tag: IcebergResultTag,
    /// The scan handle on success; null on error.
    pub ok: Shared<Scan>,
    /// The engine error on failure; null on success.
    pub err: *mut EngineError,
}
impl_iceberg_result!(IcebergResultScan, Shared<Scan>, Shared::null());

/// Result carrying an optional `int64_t`.
#[repr(C)]
pub struct IcebergResultOptionalI64 {
    /// Which arm is populated.
    pub tag: IcebergResultTag,
    /// The value on success; absent on error.
    pub ok: IcebergOptionalI64,
    /// The engine error on failure; null on success.
    pub err: *mut EngineError,
}
impl_iceberg_result!(
    IcebergResultOptionalI64,
    IcebergOptionalI64,
    IcebergOptionalI64::NONE
);

/// Result carrying an optional `int32_t`.
#[repr(C)]
pub struct IcebergResultOptionalI32 {
    /// Which arm is populated.
    pub tag: IcebergResultTag,
    /// The value on success; absent on error.
    pub ok: IcebergOptionalI32,
    /// The engine error on failure; null on success.
    pub err: *mut EngineError,
}
impl_iceberg_result!(
    IcebergResultOptionalI32,
    IcebergOptionalI32,
    IcebergOptionalI32::NONE
);

/// Result carrying a `size_t`.
#[repr(C)]
pub struct IcebergResultUsize {
    /// Which arm is populated.
    pub tag: IcebergResultTag,
    /// The value on success; zero on error.
    pub ok: usize,
    /// The engine error on failure; null on success.
    pub err: *mut EngineError,
}
impl_iceberg_result!(IcebergResultUsize, usize, 0);

/// Result with no payload.
///
/// C has no unit type, so this struct carries only the tag and the error.
#[repr(C)]
pub struct IcebergResultUnit {
    /// Which arm is populated.
    pub tag: IcebergResultTag,
    /// The engine error on failure; null on success.
    pub err: *mut EngineError,
}

impl IcebergResultUnit {
    /// Success result.
    pub fn ok() -> Self {
        Self {
            tag: IcebergResultTag::Ok,
            err: std::ptr::null_mut(),
        }
    }

    /// Error result carrying an engine error (possibly null).
    pub fn err(err: *mut EngineError) -> Self {
        Self {
            tag: IcebergResultTag::Err,
            err,
        }
    }

    /// Converts a body result, allocating the error through `alloc`.
    pub fn from_result(result: FfiResult<()>, alloc: Option<AllocateErrorFn>) -> Self {
        match result {
            Ok(()) => Self::ok(),
            Err(e) => Self::err(e.allocate(alloc)),
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// Test-side error object: just the code and message.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct TestError {
        pub(crate) code: IcebergErrorCode,
        pub(crate) message: String,
    }

    /// Test allocator: boxes a [`TestError`] behind the opaque pointer.
    pub(crate) extern "C" fn allocate_error(
        code: IcebergErrorCode,
        msg: StringSlice,
    ) -> *mut EngineError {
        // SAFETY: the slice is valid for the duration of the callback.
        let message = unsafe { msg.as_str() }
            .unwrap_or("<invalid utf-8>")
            .to_owned();
        Box::into_raw(Box::new(TestError { code, message })).cast::<EngineError>()
    }

    /// Reclaims a pointer produced by [`allocate_error`].
    pub(crate) fn take(ptr: *mut EngineError) -> Option<TestError> {
        if ptr.is_null() {
            return None;
        }
        // SAFETY: `ptr` came from `Box::into_raw` in `allocate_error`.
        Some(*unsafe { Box::from_raw(ptr.cast::<TestError>()) })
    }

    #[test]
    fn error_code_count_matches_variants() {
        // Exhaustive match: adding a variant without updating the count or
        // this list fails to compile or fails this test.
        let all = [
            IcebergErrorCode::Ok,
            IcebergErrorCode::Unexpected,
            IcebergErrorCode::DataInvalid,
            IcebergErrorCode::FeatureUnsupported,
            IcebergErrorCode::PreconditionFailed,
            IcebergErrorCode::NotFound,
            IcebergErrorCode::AlreadyExists,
            IcebergErrorCode::CommitConflict,
            IcebergErrorCode::PredicateBindFailed,
            IcebergErrorCode::InvalidHandle,
            IcebergErrorCode::InvalidArgument,
            IcebergErrorCode::Panic,
        ];
        assert_eq!(all.len(), ICEBERG_ERROR_CODE_COUNT);
        for (i, code) in all.iter().enumerate() {
            assert_eq!(*code as usize, i, "discriminants must be contiguous");
            let _: () = match code {
                IcebergErrorCode::Ok
                | IcebergErrorCode::Unexpected
                | IcebergErrorCode::DataInvalid
                | IcebergErrorCode::FeatureUnsupported
                | IcebergErrorCode::PreconditionFailed
                | IcebergErrorCode::NotFound
                | IcebergErrorCode::AlreadyExists
                | IcebergErrorCode::CommitConflict
                | IcebergErrorCode::PredicateBindFailed
                | IcebergErrorCode::InvalidHandle
                | IcebergErrorCode::InvalidArgument
                | IcebergErrorCode::Panic => (),
            };
        }
    }

    #[test]
    fn iceberg_error_maps_to_code() {
        let e = iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "bad schema");
        let ffi: FfiError = e.into();
        assert_eq!(ffi.code, IcebergErrorCode::DataInvalid);
        assert!(ffi.message.contains("bad schema"));
    }

    #[test]
    fn result_from_error_uses_allocator() {
        let r = IcebergResultU64::from_result(
            Err(FfiError::new(IcebergErrorCode::InvalidArgument, "nope")),
            Some(allocate_error as AllocateErrorFn),
        );
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert_eq!(r.ok, 0);
        let e = take(r.err);
        assert_eq!(
            e,
            Some(TestError {
                code: IcebergErrorCode::InvalidArgument,
                message: "nope".to_owned()
            })
        );
    }

    #[test]
    fn unit_result_carries_only_tag_and_error() {
        let ok = IcebergResultUnit::from_result(Ok(()), Some(allocate_error as AllocateErrorFn));
        assert_eq!(ok.tag, IcebergResultTag::Ok);
        assert!(ok.err.is_null());
        let err = IcebergResultUnit::from_result(
            Err(FfiError::new(IcebergErrorCode::InvalidArgument, "bad")),
            Some(allocate_error as AllocateErrorFn),
        );
        assert_eq!(err.tag, IcebergResultTag::Err);
        assert_eq!(
            take(err.err).map(|e| e.code),
            Some(IcebergErrorCode::InvalidArgument)
        );
    }

    #[test]
    fn result_without_allocator_has_null_error() {
        let r = IcebergResultU64::from_result(Err(FfiError::invalid_handle("runtime")), None);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.err.is_null());
    }
}
