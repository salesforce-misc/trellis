//! The engine's `tracing` events, queued as plain records for the host's
//! logger (issue #149).
//!
//! The `trellis` crate emits through the `tracing` facade and never installs
//! a subscriber (ADR-0009 decision 3): that is the embedder's job, and for an
//! embedded deployment the embedder is the binding, acting for the host.
//! [`install_log_bridge`] installs a process-wide subscriber that turns each
//! event into a [`PlainLogRecord`] and queues it; the binding drains the queue
//! with [`LogBridge::take`] from a thread the host VM owns and hands each
//! record to the host's logger (Elixir's `Logger`, Ruby's `Logger`).
//!
//! The shape is chosen so logging can drop a line but can never crash or
//! block anything:
//!
//! - **Pull, not push.** The subscriber never calls into the host VM. It
//!   runs on whatever thread emitted the event: a Tokio worker, a drain
//!   thread, or a BEAM dirty scheduler running a NIF, where Rustler's
//!   `OwnedEnv::send_and_clear` would panic. So it only queues, and the
//!   binding pulls from a thread the host VM already manages. Nothing here
//!   depends on a host process being alive: with nobody draining, the queue
//!   fills and further records are dropped.
//! - **Bounded, and never blocking the emitter.** The queue holds
//!   [`LOG_QUEUE_CAPACITY`] records. A full queue drops the record and counts
//!   it, and [`LogBridge::take`] reports that count, so the host can log that
//!   lines were lost.
//! - **Events only.** Spans are disabled. The engine opens a span per drained
//!   batch (`staging.drain_once`), and recording those for a logger that
//!   never prints them would put a registry allocation on the drain hot
//!   path. An event's own fields carry its context: every engine event names
//!   its instance in a `trellis_instance` field.
//! - **Filtered at the callsite.** [`LogBridge::set_max_level`] sets the most
//!   verbose level forwarded and rebuilds `tracing`'s interest cache, so an
//!   event above that level costs what it costs with no subscriber at all.
//!
//! `tracing` has one global subscriber, set once, per copy of `tracing` that
//! is linked in. A binding's native library links its own copy, so the global
//! is per library, not per OS process: another native extension's subscriber
//! never sees the engine's events, and this bridge never sees its events.
//! Only code linked into the same library can set the global first, such as
//! a host's own build of a binding. Then [`install_log_bridge`] returns a
//! `conflict` error and the engine's events go to that subscriber instead. A
//! host that composes its own subscriber, from its own build of a binding,
//! simply never calls [`install_log_bridge`].

use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Mutex, OnceLock, PoisonError};

use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};
use trellis::ErrorCode;

use crate::PlainError;

/// How many records wait for the host before new ones are dropped.
pub const LOG_QUEUE_CAPACITY: usize = 10_000;

/// Every level word a [`PlainLogRecord`] carries, most severe first:
/// `tracing`'s own level names. A host maps each to its logger's level
/// (Elixir's `:warning` for `warn`; `trace` to its most verbose level).
pub const LOG_LEVELS: [&str; 5] = ["error", "warn", "info", "debug", "trace"];

/// The words [`LogBridge::set_max_level`] takes: [`LOG_LEVELS`], plus `off`.
pub const LOG_LEVEL_FILTERS: [&str; 6] = ["off", "error", "warn", "info", "debug", "trace"];

/// One `tracing` event, flattened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainLogRecord {
    /// One of [`LOG_LEVELS`].
    pub level: &'static str,
    /// The event's target: the emitting module's path, as in `trellis::client`.
    pub target: String,
    /// The event's message, then its other fields as `name=value` pairs, the
    /// way `tracing-subscriber`'s formatter writes them.
    pub message: String,
}

/// What [`LogBridge::take`] drained.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LogBatch {
    /// The records, oldest first.
    pub records: Vec<PlainLogRecord>,
    /// How many records were dropped on a full queue since the last `take`.
    pub dropped: u64,
}

/// The queue between the engine's events and the host's logger.
pub struct LogBridge {
    sender: SyncSender<PlainLogRecord>,
    receiver: Mutex<Receiver<PlainLogRecord>>,
    dropped: AtomicU64,
    /// A [`LOG_LEVEL_FILTERS`] index: 0 is `off`, 5 is `trace`.
    max_level: AtomicU8,
}

impl LogBridge {
    fn new(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        LogBridge {
            sender,
            receiver: Mutex::new(receiver),
            dropped: AtomicU64::new(0),
            max_level: AtomicU8::new(0),
        }
    }

    /// Up to `max` queued records, oldest first, and how many were dropped
    /// since the last call. Never waits for a record.
    pub fn take(&self, max: usize) -> LogBatch {
        let receiver = self.receiver.lock().unwrap_or_else(PoisonError::into_inner);
        let records = receiver.try_iter().take(max).collect();
        drop(receiver);
        LogBatch {
            records,
            dropped: self.dropped.swap(0, Ordering::Relaxed),
        }
    }

    /// Forwards events at `level` (one of [`LOG_LEVEL_FILTERS`]) and more
    /// severe ones; anything else is `validation`. Takes effect for events
    /// already compiled in, not just new ones.
    pub fn set_max_level(&self, level: &str) -> Result<(), PlainError> {
        let index = LOG_LEVEL_FILTERS
            .iter()
            .position(|word| *word == level)
            .ok_or_else(|| {
                PlainError::new(
                    ErrorCode::Validation,
                    format!(
                        "{level:?} is not a log level; expected one of {}",
                        LOG_LEVEL_FILTERS.join(", ")
                    ),
                )
            })?;
        // Every index fits: `LOG_LEVEL_FILTERS` has six entries.
        let index = index as u8;
        if self.max_level.swap(index, Ordering::Relaxed) != index {
            tracing::callsite::rebuild_interest_cache();
        }
        Ok(())
    }

    fn level_filter(&self) -> LevelFilter {
        match self.max_level.load(Ordering::Relaxed) {
            0 => LevelFilter::OFF,
            1 => LevelFilter::ERROR,
            2 => LevelFilter::WARN,
            3 => LevelFilter::INFO,
            4 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        }
    }

    fn forwards(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event() && *metadata.level() <= self.level_filter()
    }

    fn push(&self, record: PlainLogRecord) {
        match self.sender.try_send(record) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            // Unreachable while `self` holds the receiver; nothing to do.
            Err(TrySendError::Disconnected(_)) => {}
        }
    }

    fn subscriber(&'static self) -> BridgeSubscriber {
        BridgeSubscriber { bridge: self }
    }
}

/// Installs the bridge as the global `tracing` subscriber (of the copy of
/// `tracing` linked into this library; see the module docs), forwarding nothing until [`LogBridge::set_max_level`] says otherwise.
///
/// Idempotent: every call after the first returns the same bridge, or the
/// same error. The error is `conflict`, when another global subscriber was
/// set first; the engine's events then go to that one.
pub fn install_log_bridge() -> Result<&'static LogBridge, PlainError> {
    let installed = INSTALLED.get_or_init(|| {
        let bridge: &'static LogBridge = Box::leak(Box::new(LogBridge::new(LOG_QUEUE_CAPACITY)));
        tracing::subscriber::set_global_default(bridge.subscriber())
            .map(|()| bridge)
            .map_err(|_| ())
    });
    installed.map_err(|()| {
        PlainError::new(
            ErrorCode::Conflict,
            "another tracing subscriber is already the global default, so Trellis's log \
             lines go to it rather than to this bridge",
        )
    })
}

/// The bridge [`install_log_bridge`] installed, if a call to it has
/// succeeded. Never installs one.
pub fn installed_log_bridge() -> Option<&'static LogBridge> {
    INSTALLED.get().copied().and_then(Result::ok)
}

/// [`install_log_bridge`]'s one attempt: the bridge, or `Err` when another
/// subscriber was already the global default. Leaked, like the global
/// subscriber itself, which `tracing` never drops either.
static INSTALLED: OnceLock<Result<&'static LogBridge, ()>> = OnceLock::new();

/// The global subscriber: events only, never spans, and nothing but a queue
/// push on the emitting thread.
struct BridgeSubscriber {
    bridge: &'static LogBridge,
}

impl Subscriber for BridgeSubscriber {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if self.bridge.forwards(metadata) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(self.bridge.level_filter())
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.bridge.forwards(metadata)
    }

    // Spans are never enabled, so `tracing` never calls these four with an
    // `Id` of ours. `new_span` still has to return one.
    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}

    fn event(&self, event: &Event<'_>) {
        self.bridge.push(record(event));
    }
}

/// `event` as a [`PlainLogRecord`].
fn record(event: &Event<'_>) -> PlainLogRecord {
    let metadata = event.metadata();
    let mut visitor = MessageVisitor::default();
    event.record(&mut visitor);
    let mut message = visitor.message;
    if !visitor.fields.is_empty() {
        if !message.is_empty() {
            message.push(' ');
        }
        message.push_str(&visitor.fields);
    }
    PlainLogRecord {
        level: level_word(*metadata.level()),
        target: metadata.target().to_string(),
        message,
    }
}

fn level_word(level: Level) -> &'static str {
    match level {
        Level::ERROR => "error",
        Level::WARN => "warn",
        Level::INFO => "info",
        Level::DEBUG => "debug",
        Level::TRACE => "trace",
    }
}

/// Collects the `message` field bare and every other field as `name=value`,
/// `Debug`-formatted, as `tracing-subscriber`'s default formatter does.
#[derive(Default)]
struct MessageVisitor {
    message: String,
    fields: String,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            self.record_debug(field, &value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // Writing to a `String` can't fail.
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            if !self.fields.is_empty() {
                self.fields.push(' ');
            }
            let _ = write!(self.fields, "{}={value:?}", field.name());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes every test here that sets a level or installs a
    /// subscriber. Both rebuild `tracing`'s interest cache and its global max
    /// level, and when only one dispatcher is registered, `tracing-core`
    /// rebuilds from the *rebuilding thread's* default alone. A rebuild on
    /// another test's thread can then set the max level to `off` while a
    /// scoped test is emitting, and that test's events vanish. The global
    /// subscriber the bindings install never meets this: it's the only
    /// dispatcher, and every thread's default.
    fn serial() -> std::sync::MutexGuard<'static, ()> {
        static SERIAL: Mutex<()> = Mutex::new(());
        SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A bridge that isn't the global one, and a scoped dispatcher onto it,
    /// so these tests don't claim the process-wide subscriber.
    fn with_bridge(level: &str, emit: impl FnOnce()) -> &'static LogBridge {
        let _serial = serial();
        let bridge: &'static LogBridge = Box::leak(Box::new(LogBridge::new(4)));
        bridge.set_max_level(level).unwrap();
        tracing::subscriber::with_default(bridge.subscriber(), emit);
        bridge
    }

    #[test]
    fn an_event_crosses_as_its_level_target_message_and_fields() {
        let bridge = with_bridge("trace", || {
            tracing::warn!(table = "public.orders", attempts = 3, "capture waiting");
            tracing::error!("bare");
            tracing::info!(answer = 42);
            tracing::trace!(target: "custom::target", "fine detail");
        });
        let batch = bridge.take(10);
        assert_eq!(batch.dropped, 0);
        let module = module_path!().to_string();
        assert_eq!(
            batch.records,
            vec![
                PlainLogRecord {
                    level: "warn",
                    target: module.clone(),
                    message: "capture waiting table=\"public.orders\" attempts=3".into(),
                },
                PlainLogRecord {
                    level: "error",
                    target: module.clone(),
                    message: "bare".into(),
                },
                PlainLogRecord {
                    level: "info",
                    target: module,
                    message: "answer=42".into(),
                },
                PlainLogRecord {
                    level: "trace",
                    target: "custom::target".into(),
                    message: "fine detail".into(),
                },
            ]
        );
    }

    /// The engine's `trellis_instance` field (issue #874) is a displayed
    /// value, so it crosses as plain `name=value` text, with no quotes.
    #[test]
    fn the_engines_instance_field_crosses_as_plain_text() {
        let bridge = with_bridge("info", || {
            tracing::info!(trellis_instance = %"mydb/tenant_a", table = %"public.t", "capture reconciled");
        });
        let messages: Vec<_> = bridge
            .take(10)
            .records
            .into_iter()
            .map(|r| r.message)
            .collect();
        assert_eq!(
            messages,
            ["capture reconciled trellis_instance=mydb/tenant_a table=public.t"]
        );
    }

    #[test]
    fn events_more_verbose_than_the_max_level_are_not_forwarded() {
        let bridge = with_bridge("warn", || {
            tracing::error!("kept");
            tracing::warn!("kept too");
            tracing::info!("filtered");
            tracing::debug!("filtered");
        });
        let levels: Vec<_> = bridge.take(10).records.iter().map(|r| r.level).collect();
        assert_eq!(levels, ["error", "warn"]);

        let bridge = with_bridge("off", || tracing::error!("filtered"));
        assert!(bridge.take(10).records.is_empty());
    }

    #[test]
    fn events_inside_a_span_still_cross_and_the_span_is_not_recorded() {
        let bridge = with_bridge("trace", || {
            let span = tracing::info_span!("staging.drain_once", seg_seq = 7);
            let _entered = span.enter();
            assert!(span.is_disabled(), "the bridge never enables a span");
            tracing::info!("inside");
        });
        let messages: Vec<_> = bridge
            .take(10)
            .records
            .into_iter()
            .map(|r| r.message)
            .collect();
        assert_eq!(messages, ["inside"]);
    }

    #[test]
    fn a_full_queue_drops_and_counts_rather_than_blocking() {
        let bridge = with_bridge("info", || {
            for n in 0..10 {
                tracing::info!(n);
            }
        });
        let batch = bridge.take(100);
        let messages: Vec<_> = batch.records.into_iter().map(|r| r.message).collect();
        assert_eq!(
            messages,
            ["n=0", "n=1", "n=2", "n=3"],
            "the oldest are kept"
        );
        assert_eq!(batch.dropped, 6);
        assert_eq!(bridge.take(100), LogBatch::default(), "the count resets");
    }

    #[test]
    fn take_returns_at_most_max_records_oldest_first() {
        let bridge = with_bridge("info", || {
            for n in 0..3 {
                tracing::info!(n);
            }
        });
        let first: Vec<_> = bridge
            .take(2)
            .records
            .into_iter()
            .map(|r| r.message)
            .collect();
        assert_eq!(first, ["n=0", "n=1"]);
        let rest: Vec<_> = bridge
            .take(2)
            .records
            .into_iter()
            .map(|r| r.message)
            .collect();
        assert_eq!(rest, ["n=2"]);
    }

    /// The one test in this binary that claims the global subscriber. The
    /// scoped tests above are unaffected: a thread's scoped default wins.
    #[test]
    fn the_installed_bridge_is_global_and_installing_again_returns_it() {
        let _serial = serial();
        let bridge = install_log_bridge().unwrap();
        assert!(std::ptr::eq(bridge, install_log_bridge().unwrap()));
        assert!(std::ptr::eq(bridge, installed_log_bridge().unwrap()));
        bridge.set_max_level("info").unwrap();
        std::thread::spawn(|| {
            tracing::info!(target: "global_bridge_test", "from another thread");
            tracing::debug!(target: "global_bridge_test", "filtered");
        })
        .join()
        .unwrap();
        let ours: Vec<_> = bridge
            .take(LOG_QUEUE_CAPACITY)
            .records
            .into_iter()
            .filter(|r| r.target == "global_bridge_test")
            .map(|r| r.message)
            .collect();
        assert_eq!(ours, ["from another thread"]);
    }

    #[test]
    fn an_unknown_level_is_a_validation_error() {
        let _serial = serial();
        let bridge = LogBridge::new(1);
        for level in ["", "warning", "INFO", "verbose"] {
            let err = bridge.set_max_level(level).unwrap_err();
            assert_eq!(err.code, "validation", "{level:?}");
        }
        for level in LOG_LEVEL_FILTERS {
            bridge.set_max_level(level).unwrap();
        }
    }

    #[test]
    fn every_level_word_is_a_filter_word() {
        assert_eq!(LOG_LEVEL_FILTERS[1..], LOG_LEVELS);
        for level in [
            Level::ERROR,
            Level::WARN,
            Level::INFO,
            Level::DEBUG,
            Level::TRACE,
        ] {
            assert!(LOG_LEVELS.contains(&level_word(level)));
        }
    }
}
