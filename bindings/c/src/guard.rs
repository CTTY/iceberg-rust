//! Panic containment for `extern "C"` entry points.
//!
//! # Rule: nothing that can panic runs before the guard
//!
//! A panic that unwinds out of an `extern "C"` frame aborts the process, so
//! the *entire* body of every `extern "C"` function, including the prologue
//! that resolves handles and copies the error allocator, must execute inside
//! [`ffi_guard!`]. The [`with_handle!`] family of macros exists so that
//! functions taking a handle get this for free: the handle is resolved and
//! the allocator is stored *inside* the guarded block, and the on-panic arm
//! reads whatever allocator was recorded up to the point of the panic.

use std::any::Any;

use crate::error::AllocateErrorFn;

/// Renders a panic payload as a message for the error path.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        format!("panic in iceberg-ffi: {s}")
    } else if let Some(s) = payload.downcast_ref::<String>() {
        format!("panic in iceberg-ffi: {s}")
    } else {
        "panic in iceberg-ffi (non-string payload)".to_owned()
    }
}

/// Something behind a handle that knows the engine's error allocator.
///
/// Implemented by every handle target whose `extern "C"` functions report
/// errors, so [`with_handle!`] can fetch the allocator once the handle has
/// been resolved inside the guard.
pub(crate) trait ErrorSource {
    /// The allocator to use for errors reported through this object.
    fn alloc_error(&self) -> AllocateErrorFn;
}

/// Runs `$body` under `catch_unwind`.
///
/// A panic reaching an `extern "C"` frame aborts the process, so every
/// `extern "C"` function in this crate wraps its body in this macro. On panic
/// the `$on_panic` closure receives the rendered panic message and must
/// produce the function's return value (normally an error result built with
/// [`IcebergErrorCode::Panic`](crate::error::IcebergErrorCode::Panic)).
macro_rules! ffi_guard {
    ($on_panic:expr, $body:block) => {
        match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(|| $body)) {
            Ok(value) => value,
            Err(payload) => {
                let message = $crate::guard::panic_message(payload.as_ref());
                let on_panic: &dyn Fn(String) -> _ = &$on_panic;
                on_panic(message)
            }
        }
    };
}

/// Guarded body for an `extern "C"` function that takes a handle.
///
/// `$handle` is the handle argument, `$resolve` the path of an `unsafe fn
/// (&Handle) -> FfiResult<&T>` where `T: ErrorSource`, `$result` the result
/// struct type to return, and `|$v| $body` a block evaluating to
/// `FfiResult<payload>` with `$v: &T` in scope.
///
/// Everything, including the handle resolution, runs inside [`ffi_guard!`].
/// The allocator is recorded in a `Cell` the moment the handle resolves, so a
/// panic after that point still produces an engine-allocated error, while a
/// panic (or a null handle) before it yields an error with a null `err`.
macro_rules! with_handle {
    ($handle:expr, $resolve:path, $result:ident, |$v:ident| $body:block) => {{
        let alloc: ::std::cell::Cell<Option<$crate::error::AllocateErrorFn>> =
            ::std::cell::Cell::new(None);
        ffi_guard!(
            |msg| $result::from_result(Err($crate::error::FfiError::panic(msg)), alloc.get()),
            {
                let body = || -> $crate::error::FfiResult<_> {
                    // SAFETY: the engine passes a handle it obtained from the
                    // matching `iceberg_*_new` and has not freed since.
                    let $v = unsafe { $resolve(&$handle) }?;
                    alloc.set(Some($crate::guard::ErrorSource::alloc_error($v)));
                    $body
                };
                $result::from_result(body(), alloc.get())
            }
        )
    }};
}

/// [`with_handle!`] specialised to a `Shared<Runtime>` handle.
macro_rules! with_runtime {
    ($handle:expr, $result:ident, |$rt:ident| $body:block) => {
        with_handle!(
            $handle,
            $crate::runtime::Runtime::from_handle,
            $result,
            |$rt| $body
        )
    };
}

/// [`with_handle!`] specialised to a `Shared<Table>` handle.
macro_rules! with_table {
    ($handle:expr, $result:ident, |$t:ident| $body:block) => {
        with_handle!($handle, $crate::table::Table::from_handle, $result, |$t| {
            $body
        })
    };
}
