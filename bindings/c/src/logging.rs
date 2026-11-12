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

//! Forwards `tracing` events emitted by iceberg-rust to the engine's logger.
//!
//! `tracing` allows exactly one global subscriber per process, while the
//! engine may create several runtimes (one per database). The subscriber
//! installed here therefore does not hold any callback itself: it reads the
//! callback from a thread-local *log sink*. The runtime sets the sink on every
//! tokio worker thread it starts, and every `block_on` sets it on the calling
//! thread for the duration of the call, so events are routed to the runtime
//! whose work produced them. Events raised on threads without a sink are
//! dropped.

use std::cell::RefCell;
use std::ffi::c_void;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};

use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber, span};

use crate::strings::StringSlice;

/// Severity of a forwarded log event.
///
/// Values match DuckDB's `LogLevel` so the engine can cast directly.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergLogLevel {
    /// Very fine-grained diagnostics.
    Trace = 10,
    /// Developer diagnostics.
    Debug = 20,
    /// Normal operation.
    Info = 30,
    /// Something unexpected that was handled.
    Warning = 40,
    /// A failure.
    Error = 50,
}

impl From<Level> for IcebergLogLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::TRACE => Self::Trace,
            Level::DEBUG => Self::Debug,
            Level::INFO => Self::Info,
            Level::WARN => Self::Warning,
            Level::ERROR => Self::Error,
        }
    }
}

/// Engine callback receiving one log event; nullable (`None` disables
/// forwarding).
///
/// The first argument is the `log_ctx` pointer the engine passed to
/// `iceberg_runtime_new`, returned untouched. `target` is the Rust module path
/// that emitted the event and `message` is the rendered event text. Both
/// slices are only valid during the callback. The callback may be invoked
/// concurrently from several threads.
pub type LogCallbackFn =
    Option<extern "C" fn(*mut c_void, IcebergLogLevel, StringSlice, StringSlice)>;

/// A log callback together with its engine context.
///
/// All clones of a sink share one `enabled` flag; [`LogSink::disable`] turns
/// every copy off at once, which `iceberg_runtime_free` uses to stop
/// forwarding before the worker threads are shut down (the engine's
/// `log_ctx` may be freed as soon as that call returns).
#[derive(Clone)]
pub(crate) struct LogSink {
    cb: extern "C" fn(*mut c_void, IcebergLogLevel, StringSlice, StringSlice),
    ctx: *mut c_void,
    enabled: Arc<AtomicBool>,
}

// SAFETY: the engine promises the callback may be invoked from any thread with
// this context (documented on `iceberg_runtime_new`); Rust only ever passes
// the pointer back and never dereferences it.
unsafe impl Send for LogSink {}
unsafe impl Sync for LogSink {}

impl LogSink {
    /// Pairs a nullable callback with its context; `None` if the callback is
    /// null.
    pub(crate) fn new(cb: LogCallbackFn, ctx: *mut c_void) -> Option<Self> {
        cb.map(|cb| Self {
            cb,
            ctx,
            enabled: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Stops this sink and every clone of it from invoking the callback.
    pub(crate) fn disable(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    fn emit(&self, level: IcebergLogLevel, target: &str, message: &str) {
        if !self.is_enabled() {
            return;
        }
        (self.cb)(
            self.ctx,
            level,
            StringSlice::borrowed(target),
            StringSlice::borrowed(message),
        );
    }
}

thread_local! {
    static LOG_SINK: RefCell<Option<LogSink>> = const { RefCell::new(None) };
}

/// Clones the current thread's sink out of the thread-local, releasing the
/// borrow before the caller runs the callback (which might itself log).
fn current_sink() -> Option<LogSink> {
    LOG_SINK.with(|s| s.borrow().clone())
}

/// Installs the forwarding subscriber as the global default, once.
///
/// If some other code already installed a global subscriber, that one wins and
/// iceberg-rust events are not forwarded; this is not an error.
pub(crate) fn install_global_subscriber() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        // Ignoring the error is intentional: see the doc comment.
        let _ = tracing::subscriber::set_global_default(ForwardingSubscriber);
    });
}

/// Sets the thread-local sink for the current thread.
pub(crate) fn set_thread_sink(sink: Option<LogSink>) {
    LOG_SINK.with(|s| *s.borrow_mut() = sink);
}

/// Sets the sink for the current thread and restores the previous value on
/// drop. Used around `block_on` on engine threads.
pub(crate) struct SinkScope {
    previous: Option<LogSink>,
}

impl SinkScope {
    pub(crate) fn enter(sink: Option<LogSink>) -> Self {
        let previous = LOG_SINK.with(|s| s.replace(sink));
        Self { previous }
    }
}

impl Drop for SinkScope {
    fn drop(&mut self) {
        let previous = self.previous.take();
        LOG_SINK.with(|s| *s.borrow_mut() = previous);
    }
}

struct ForwardingSubscriber;

impl Subscriber for ForwardingSubscriber {
    fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
        // `enabled` depends on thread state, so it must be re-evaluated on
        // every event instead of being cached per callsite.
        Interest::sometimes()
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        // No static hint: whether anything is forwarded depends on the calling
        // thread's sink, and the level filtering happens on the engine side.
        // Returning `None` keeps every level flowing into `enabled`/`event`.
        None
    }

    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        LOG_SINK.with(|s| s.borrow().as_ref().is_some_and(LogSink::is_enabled))
    }

    fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
        // Spans are not forwarded; a constant non-zero id is sufficient.
        span::Id::from_u64(1)
    }

    fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

    fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        let Some(sink) = current_sink() else {
            return;
        };
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let message = visitor.finish();
        let metadata = event.metadata();
        sink.emit(
            IcebergLogLevel::from(*metadata.level()),
            metadata.target(),
            &message,
        );
    }

    fn enter(&self, _span: &span::Id) {}

    fn exit(&self, _span: &span::Id) {}
}

/// Renders an event as `message key=value key=value`.
#[derive(Default)]
struct MessageVisitor {
    message: String,
    fields: String,
}

impl MessageVisitor {
    fn finish(self) -> String {
        if self.fields.is_empty() {
            self.message
        } else if self.message.is_empty() {
            self.fields
        } else {
            format!("{} {}", self.message, self.fields)
        }
    }

    fn push_field(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if !self.fields.is_empty() {
            self.fields.push(' ');
        }
        // Writing to a String cannot fail.
        let _ = write!(self.fields, "{}={:?}", field.name(), value);
    }
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            // Writing to a String cannot fail.
            let _ = write!(self.message, "{value:?}");
        } else {
            self.push_field(field, value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            self.push_field(field, &value);
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Mutex;

    use super::*;

    /// Events captured by [`capture_log`]: (ctx as integer, level, target,
    /// message).
    pub(crate) static CAPTURED: Mutex<Vec<(usize, IcebergLogLevel, String, String)>> =
        Mutex::new(Vec::new());

    /// Test log callback: records events in [`CAPTURED`].
    pub(crate) extern "C" fn capture_log(
        ctx: *mut c_void,
        level: IcebergLogLevel,
        target: StringSlice,
        msg: StringSlice,
    ) {
        // SAFETY: both slices are valid for the duration of the callback.
        let target = unsafe { target.as_str() }.unwrap_or("?").to_owned();
        let msg = unsafe { msg.as_str() }.unwrap_or("?").to_owned();
        if let Ok(mut v) = CAPTURED.lock() {
            v.push((ctx as usize, level, target, msg));
        }
    }

    /// Context value tests pass so they can tell their events apart.
    pub(crate) const TEST_CTX: usize = 0x1ce_be46;

    #[test]
    fn events_are_forwarded_only_with_a_sink() {
        install_global_subscriber();
        tracing::info!("dropped: no sink on this thread");
        {
            let _scope = SinkScope::enter(LogSink::new(Some(capture_log), TEST_CTX as *mut c_void));
            tracing::warn!(count = 3, "hello {}", "world");
        }
        tracing::info!("dropped again");
        let captured = CAPTURED.lock().map(|v| v.clone()).unwrap_or_default();
        let hit = captured
            .iter()
            .find(|(_, _, _, m)| m.starts_with("hello world"))
            .cloned();
        assert_eq!(
            hit,
            Some((
                TEST_CTX,
                IcebergLogLevel::Warning,
                module_path!().to_owned(),
                "hello world count=3".to_owned()
            ))
        );
        assert!(!captured.iter().any(|(_, _, _, m)| m.starts_with("dropped")));
    }

    #[test]
    fn null_callback_means_no_sink() {
        assert!(LogSink::new(None, TEST_CTX as *mut c_void).is_none());
    }

    #[test]
    fn disabled_sink_drops_events_on_every_clone() {
        install_global_subscriber();
        let sink = LogSink::new(Some(capture_log), TEST_CTX as *mut c_void);
        let _scope = SinkScope::enter(sink.clone());
        tracing::warn!("before disable");
        if let Some(sink) = &sink {
            sink.disable();
        }
        tracing::warn!("after disable");
        let captured = CAPTURED.lock().map(|v| v.clone()).unwrap_or_default();
        assert!(captured.iter().any(|(_, _, _, m)| m == "before disable"));
        assert!(!captured.iter().any(|(_, _, _, m)| m == "after disable"));
    }

    #[test]
    fn level_values_match_duckdb() {
        assert_eq!(IcebergLogLevel::from(Level::TRACE) as i32, 10);
        assert_eq!(IcebergLogLevel::from(Level::ERROR) as i32, 50);
    }
}
