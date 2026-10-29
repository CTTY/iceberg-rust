//! The tokio runtime handle the engine creates once per database.
//!
//! Every async operation in iceberg-rust is driven by `block_on` on this
//! runtime from an engine thread. The runtime also carries the engine's error
//! allocator and log callback, so later calls only need the runtime handle to
//! report errors and logs.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{AllocateErrorFn, FfiError, FfiResult, IcebergErrorCode, IcebergResultRuntime};
use crate::handle::Shared;
use crate::logging::{self, LogCallbackFn, SinkScope};

/// Default grace period for worker shutdown in `iceberg_runtime_free`.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// State behind a `Shared<Runtime>` handle.
pub struct Runtime {
    tokio: tokio::runtime::Runtime,
    alloc_error: AllocateErrorFn,
    log: LogCallbackFn,
}

impl Runtime {
    fn new(
        workers: u32,
        blocking: u32,
        alloc_error: AllocateErrorFn,
        log: LogCallbackFn,
    ) -> FfiResult<Self> {
        if workers == 0 {
            return Err(FfiError::new(
                IcebergErrorCode::InvalidArgument,
                "iceberg runtime needs at least one worker thread",
            ));
        }
        if blocking == 0 {
            return Err(FfiError::new(
                IcebergErrorCode::InvalidArgument,
                "iceberg runtime needs at least one blocking thread",
            ));
        }
        logging::install_global_subscriber();
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers as usize)
            .max_blocking_threads(blocking as usize)
            .thread_name("iceberg-rs")
            .on_thread_start(move || logging::set_thread_sink(log))
            .build()
            .map_err(|e| {
                FfiError::new(
                    IcebergErrorCode::Unexpected,
                    format!("cannot start tokio runtime: {e}"),
                )
            })?;
        Ok(Self {
            tokio,
            alloc_error,
            log,
        })
    }

    /// The engine's error allocator.
    pub fn alloc_error(&self) -> AllocateErrorFn {
        self.alloc_error
    }

    /// Runs a future to completion on this runtime from an engine thread.
    ///
    /// Fails with [`IcebergErrorCode::PreconditionFailed`] when called from a
    /// thread that is itself inside a tokio runtime, which would deadlock.
    pub fn block_on<F: Future>(&self, future: F) -> FfiResult<F::Output> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(FfiError::new(
                IcebergErrorCode::PreconditionFailed,
                "iceberg-ffi called from inside a tokio runtime thread",
            ));
        }
        let _sink = SinkScope::enter(self.log);
        Ok(self.tokio.block_on(future))
    }

    /// Resolves a handle to a runtime reference, reporting null handles.
    ///
    /// # Safety
    ///
    /// See [`Shared::borrow`].
    pub(crate) unsafe fn from_handle(handle: &Shared<Runtime>) -> FfiResult<&Runtime> {
        // SAFETY: forwarded to the caller.
        unsafe { handle.borrow() }.ok_or_else(|| FfiError::invalid_handle("runtime"))
    }
}

/// Creates a runtime.
///
/// `workers` is the number of tokio worker threads and `blocking` the maximum
/// number of threads for blocking work; both must be at least 1.
/// `alloc_error` is used for every error reported through this runtime,
/// including the error of this call. `log_cb` may be null to disable log
/// forwarding.
///
/// On success the engine owns the returned handle and must release it with
/// `iceberg_runtime_free`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_runtime_new(
    workers: u32,
    blocking: u32,
    alloc_error: AllocateErrorFn,
    log_cb: LogCallbackFn,
) -> IcebergResultRuntime {
    ffi_guard!(
        |msg| IcebergResultRuntime::from_result(Err(FfiError::panic(msg)), Some(alloc_error)),
        {
            let result = Runtime::new(workers, blocking, alloc_error, log_cb)
                .map(|rt| Shared::from_arc(Arc::new(rt)));
            IcebergResultRuntime::from_result(result, Some(alloc_error))
        }
    )
}

/// Releases the engine's reference to a runtime.
///
/// When this was the last reference, worker threads are asked to stop and
/// given a short grace period. A null handle is ignored. Must not be called
/// from a thread inside the runtime.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_runtime_free(runtime: Shared<Runtime>) {
    ffi_guard!(|_msg| (), {
        // SAFETY: the engine gives up its reference by calling free.
        if let Some(arc) = unsafe { runtime.into_arc() } {
            match Arc::try_unwrap(arc) {
                Ok(rt) => rt.tokio.shutdown_timeout(SHUTDOWN_TIMEOUT),
                Err(still_shared) => drop(still_shared),
            }
        }
    })
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::error::IcebergResultTag;
    use crate::error::test_support::{allocate_error, take};
    use crate::logging::IcebergLogLevel;
    use crate::logging::test_support::capture_log;

    /// Creates a small runtime through the C ABI for other tests.
    pub(crate) fn new_test_runtime() -> Shared<Runtime> {
        let r = iceberg_runtime_new(2, 2, allocate_error as AllocateErrorFn, Some(capture_log));
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        assert!(!r.ok.is_null());
        r.ok
    }

    #[test]
    fn create_and_free() {
        let rt = new_test_runtime();
        iceberg_runtime_free(rt);
    }

    #[test]
    fn free_null_is_noop() {
        iceberg_runtime_free(Shared::null());
    }

    #[test]
    fn zero_workers_is_an_error_through_the_allocator() {
        let r = iceberg_runtime_new(0, 1, allocate_error as AllocateErrorFn, None);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.ok.is_null());
        let e = take(r.err);
        assert_eq!(
            e.as_ref().map(|e| e.code),
            Some(IcebergErrorCode::InvalidArgument)
        );
        assert!(
            e.as_ref()
                .map(|e| e.message.contains("worker"))
                .unwrap_or(false)
        );
    }

    #[test]
    fn zero_blocking_is_an_error() {
        let r = iceberg_runtime_new(1, 0, allocate_error as AllocateErrorFn, None);
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert_eq!(
            take(r.err).map(|e| e.code),
            Some(IcebergErrorCode::InvalidArgument)
        );
    }

    #[test]
    fn block_on_from_inside_runtime_is_rejected() {
        let rt = new_test_runtime();
        // SAFETY: fresh non-null handle.
        let inner = unsafe { Runtime::from_handle(&rt) };
        let inner = inner.map_err(|e| e.message);
        assert!(inner.is_ok());
        if let Ok(inner) = inner {
            // The outer block_on enters the runtime context on this thread, so
            // the inner call sees `Handle::try_current()` succeed and must
            // refuse instead of deadlocking.
            let nested = inner.block_on(async { inner.block_on(async { 1 }).map_err(|e| e.code) });
            assert_eq!(nested.ok(), Some(Err(IcebergErrorCode::PreconditionFailed)));
        }
        iceberg_runtime_free(rt);
    }

    #[test]
    fn log_events_from_worker_threads_reach_the_callback() {
        let rt = new_test_runtime();
        // SAFETY: fresh non-null handle.
        let inner = unsafe { Runtime::from_handle(&rt) }.map_err(|e| e.message);
        assert!(inner.is_ok());
        if let Ok(inner) = inner {
            let r = inner.block_on(async {
                tokio::spawn(async {
                    tracing::info!("from a worker thread");
                })
                .await
                .is_ok()
            });
            assert_eq!(r.ok(), Some(true));
        }
        iceberg_runtime_free(rt);
        let captured = crate::logging::test_support::CAPTURED
            .lock()
            .map(|v| v.clone())
            .unwrap_or_default();
        assert!(
            captured
                .iter()
                .any(|(l, _, m)| *l == IcebergLogLevel::Info && m == "from a worker thread")
        );
    }
}
