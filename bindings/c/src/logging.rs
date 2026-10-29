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

use std::cell::Cell;
use std::fmt::Write as _;
use std::sync::Once;

use tracing::field::{Field, Visit};
use tracing::span;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};

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
/// `target` is the Rust module path that emitted the event and `message` is
/// the rendered event text. Both slices are only valid during the callback.
/// The callback may be invoked concurrently from several threads.
pub type LogCallbackFn = Option<extern "C" fn(IcebergLogLevel, StringSlice, StringSlice)>;

thread_local! {
    static LOG_SINK: Cell<LogCallbackFn> = const { Cell::new(None) };
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
pub(crate) fn set_thread_sink(cb: LogCallbackFn) {
    LOG_SINK.with(|s| s.set(cb));
}

/// Sets the sink for the current thread and restores the previous value on
/// drop. Used around `block_on` on engine threads.
pub(crate) struct SinkScope {
    previous: LogCallbackFn,
}

impl SinkScope {
    pub(crate) fn enter(cb: LogCallbackFn) -> Self {
        let previous = LOG_SINK.with(|s| s.replace(cb));
        Self { previous }
    }
}

impl Drop for SinkScope {
    fn drop(&mut self) {
        LOG_SINK.with(|s| s.set(self.previous));
    }
}

struct ForwardingSubscriber;

impl Subscriber for ForwardingSubscriber {
    fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
        // `enabled` depends on thread state, so it must be re-evaluated on
        // every event instead of being cached per callsite.
        Interest::sometimes()
    }

    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        LOG_SINK.with(|s| s.get().is_some())
    }

    fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
        // Spans are not forwarded; a constant non-zero id is sufficient.
        span::Id::from_u64(1)
    }

    fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

    fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        let Some(cb) = LOG_SINK.with(|s| s.get()) else {
            return;
        };
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let message = visitor.finish();
        let metadata = event.metadata();
        cb(
            IcebergLogLevel::from(*metadata.level()),
            StringSlice::borrowed(metadata.target()),
            StringSlice::borrowed(&message),
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
    use super::*;
    use std::sync::Mutex;

    /// Events captured by [`capture_log`].
    pub(crate) static CAPTURED: Mutex<Vec<(IcebergLogLevel, String, String)>> =
        Mutex::new(Vec::new());

    /// Test log callback: records events in [`CAPTURED`].
    pub(crate) extern "C" fn capture_log(
        level: IcebergLogLevel,
        target: StringSlice,
        msg: StringSlice,
    ) {
        // SAFETY: both slices are valid for the duration of the callback.
        let target = unsafe { target.as_str() }.unwrap_or("?").to_owned();
        let msg = unsafe { msg.as_str() }.unwrap_or("?").to_owned();
        if let Ok(mut v) = CAPTURED.lock() {
            v.push((level, target, msg));
        }
    }

    #[test]
    fn events_are_forwarded_only_with_a_sink() {
        install_global_subscriber();
        tracing::info!("dropped: no sink on this thread");
        {
            let _scope = SinkScope::enter(Some(capture_log));
            tracing::warn!(count = 3, "hello {}", "world");
        }
        tracing::info!("dropped again");
        let captured = CAPTURED.lock().map(|v| v.clone()).unwrap_or_default();
        let hit = captured
            .iter()
            .find(|(_, _, m)| m.starts_with("hello world"))
            .cloned();
        assert_eq!(
            hit,
            Some((
                IcebergLogLevel::Warning,
                module_path!().to_owned(),
                "hello world count=3".to_owned()
            ))
        );
        assert!(!captured.iter().any(|(_, _, m)| m.starts_with("dropped")));
    }

    #[test]
    fn level_values_match_duckdb() {
        assert_eq!(IcebergLogLevel::from(Level::TRACE) as i32, 10);
        assert_eq!(IcebergLogLevel::from(Level::ERROR) as i32, 50);
    }
}
