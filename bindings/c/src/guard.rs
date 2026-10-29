//! Panic containment for `extern "C"` entry points.

use std::any::Any;

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
