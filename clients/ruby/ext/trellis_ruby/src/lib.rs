//! The Ruby binding's native extension: a thin Magnus wrapper over
//! [`trellis::BlockingTrellis`] (`docs/decisions/0010-embeddable-clients.md`).
//!
//! Only `lib/trellis/pg.rb` calls these; the public API, defaults, the
//! module-level singleton and the value classes live in the Ruby files under
//! `lib/`. The contract here is deliberately narrow:
//!
//! - **Every call that can block releases the GVL, and can be interrupted**
//!   (ADR-0010 decision 3). `BlockingTrellis` waits for its reply with a bare
//!   blocking receive that nothing can cut short, so this crate never waits
//!   on it from a Ruby thread. The call runs on a short-lived helper thread,
//!   and the Ruby thread waits for the helper's reply on a condition variable
//!   with the GVL released and an unblocking function that wakes it. See
//!   [`run_without_gvl`].
//! - **A handle refuses to be used from any process but the one that
//!   connected it.** Rust threads don't cross `fork`, so a handle a child
//!   inherits has nothing left to answer it. Every call checks the pid first
//!   and raises `Trellis::ForkedHandleError`, and a forked copy is never
//!   dropped either (see [`Handle`]'s `Drop`).
//! - **A process forked while its parent had an engine running can't
//!   connect one of its own** (issue #600): it may have inherited a lock one
//!   of the parent's engine threads held, which nothing in it can release.
//!   [`connect`] raises `Trellis::ForkedHandleError` instead of risking the
//!   hang. See the [`engine`] module.
//! - **Only plain data crosses** (decision 4). The flattening is
//!   `trellis-embed`'s; this crate turns its plain values into hashes, and
//!   `lib/trellis/pg.rb` turns those into `Data` objects. Words become symbols
//!   (statuses, quarantine states, relationship cardinalities, `apply`
//!   outcome kinds, `self_check` outcomes and divergence kinds), but only
//!   words from the closed sets `trellis-embed` lists, which [`init`] interns
//!   up front, never a string read from the database.
//! - **Errors cross as `(code, message)`.** Ruby's `Trellis::Error.from_native`
//!   picks the exception class for the code, so the code-to-class map lives in
//!   one place, `lib/trellis/error.rb`.
//!
//! The crate is a member of the repository's Cargo workspace, but everything
//! in it sits behind the `ruby` feature; see `Cargo.toml` for why.

#![cfg(feature = "ruby")]

mod engine;

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::Duration;

use engine::{Engine, ForkedWhileRunning};
use magnus::prelude::*;
use magnus::{
    Error, Exception, ExceptionClass, IntoValue, RArray, RClass, RHash, RModule, Ruby,
    StaticSymbol, Value, function, method,
};
use trellis::{BlockingTrellis, Config, ErrorCode, SelfCheckScope, TrellisError, TrellisOptions};
use trellis_embed::{
    DIVERGENCE_KINDS, ERROR_CODES, PlainApplied, PlainBackfillFailure, PlainCaptureFailure,
    PlainCaptureWait, PlainConfig, PlainDefinition, PlainDefinitionStatus, PlainDefinitionSummary,
    PlainDivergence, PlainDrainFailure, PlainError, PlainHeldKeys, PlainPoisonEntry,
    PlainQuarantineEntry, PlainRelationship, PlainRelationshipSummary, PlainSamplePage,
    PlainSelfCheckReport, SELF_CHECK_OUTCOMES, capture_failure_kind_names, decode_cursor,
    decode_watermark, encode_watermark, quarantine_state_names, relationship_cardinality_names,
    require_transform_statement, self_check_mode, system_time_from_epoch_micros,
    transform_status_names,
};

/// What a blocking call produces: its value, or the engine's error as plain
/// data, raised as a `Trellis::Error` once the GVL is back.
type Reply<T> = Result<T, PlainError>;

/// The live instance a [`Handle`] wraps. `shutdown` takes it out, so a call
/// after shutdown gets an error rather than a hang. The lock is read for
/// every call and written only by `shutdown`, and only ever taken on a
/// helper thread, never on a Ruby thread holding the GVL.
type Shared = Arc<RwLock<Option<Engine>>>;

/// One connected Trellis instance, owned by Ruby as `Trellis::Native::Handle`
/// and held by the `Trellis` module's singleton.
///
/// Calls from several Ruby threads don't wait on each other here: each runs
/// on its own helper thread, and the [`BlockingTrellis`] runs them one at a
/// time on its own thread.
#[magnus::wrap(class = "Trellis::Native::Handle", free_immediately, size)]
struct Handle {
    trellis: Shared,
    /// The OS process that connected (ADR-0010 decision 3's pid guard).
    owner_pid: u32,
}

impl Drop for Handle {
    /// A handle garbage-collected in a forked child is leaked, not dropped.
    /// Dropping an [`Engine`] shuts it down, and in a child neither its
    /// threads nor the runtime serving its job channel exist: the handle is a
    /// copy of the parent's state, and the only safe thing to do with it is
    /// leave it alone. In the owning process the drop is the ordinary
    /// backstop: the engine shuts down on a thread of its own, without
    /// blocking the GC that ran it.
    fn drop(&mut self) {
        if std::process::id() != self.owner_pid {
            std::mem::forget(Arc::clone(&self.trellis));
        }
    }
}

impl Handle {
    /// Runs `call` against the live instance with the GVL released, or
    /// raises why it can't.
    fn call<T: Send + 'static>(
        &self,
        ruby: &Ruby,
        call: impl FnOnce(&BlockingTrellis) -> Result<T, TrellisError> + Send + 'static,
    ) -> Result<T, Error> {
        self.check_pid(ruby)?;
        let shared = Arc::clone(&self.trellis);
        blocking(ruby, move || {
            let guard = shared.read().map_err(|_| poisoned())?;
            let trellis = guard.as_ref().ok_or_else(|| {
                PlainError::new(
                    ErrorCode::Validation,
                    "this Trellis handle has been shut down",
                )
            })?;
            call(trellis).map_err(PlainError::from)
        })
    }

    /// Raises `Trellis::ForkedHandleError` unless this is the process that
    /// connected. Checked before anything else a call does, so a forked child
    /// never reaches a lock, a channel or a thread it inherited.
    fn check_pid(&self, ruby: &Ruby) -> Result<(), Error> {
        let pid = std::process::id();
        if pid == self.owner_pid {
            return Ok(());
        }
        Err(forked_handle_error(
            ruby,
            format!(
                "this Trellis handle was connected by process {}, and this is process {pid}: \
                 a handle does not survive fork, so call Trellis.connect in this process \
                 (after forking: Puma's before_worker_boot, Unicorn's after_fork, Passenger's \
                 starting_worker_process; \"Forking servers\" in clients/ruby/README.md \
                 covers preload_app! and fork_worker)",
                self.owner_pid
            ),
        ))
    }

    fn owner_pid(&self) -> u32 {
        self.owner_pid
    }

    fn migrate(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        rb_self.call(ruby, BlockingTrellis::migrate)
    }

    /// Registers the `TRANSFORM` statement `text`.
    ///
    /// `BlockingTrellis::apply` takes every statement form, so `text` is
    /// checked first with `trellis::statement_kind` (through
    /// `trellis-embed`): any other form is a `validation` error and is never
    /// applied, and text that doesn't parse is the `parse` error `apply`
    /// would return.
    fn define(ruby: &Ruby, rb_self: &Self, text: String) -> Result<RHash, Error> {
        rb_self.check_pid(ruby)?;
        require_transform_statement(&text).map_err(|err| raise(ruby, err))?;
        let definition = rb_self.call(ruby, move |trellis| {
            let applied = trellis.apply(&text)?;
            Ok(applied.into_transform().map(|d| PlainDefinition::from(&d)))
        })?;
        // Unreachable: the check above is `apply`'s own parser. Kept as
        // defence, so a mistake there is an error rather than a panic.
        let definition = definition.ok_or_else(|| {
            raise(
                ruby,
                PlainError::new(
                    ErrorCode::Internal,
                    "define applied a statement that registered no transform",
                ),
            )
        })?;
        definition_hash(ruby, definition)
    }

    /// `target_table`'s status, or `nil` when no definition writes it.
    fn status(ruby: &Ruby, rb_self: &Self, target_table: String) -> Result<Option<RHash>, Error> {
        let status = rb_self.call(ruby, move |trellis| {
            Ok(trellis
                .status(&target_table)?
                .map(|status| PlainDefinitionStatus::from(&status)))
        })?;
        status.map(|status| status_hash(ruby, status)).transpose()
    }

    /// Runs one statement of Trellis's grammar, whatever its form, and
    /// reports what it did as `{kind:, definition:, relationship:, columns:,
    /// added:, dropped:, altered:}`: `kind` is one of [`PlainApplied::KINDS`],
    /// and each other field is set only for the kinds that carry it (see
    /// [`applied_hash`]).
    fn apply(ruby: &Ruby, rb_self: &Self, text: String) -> Result<RHash, Error> {
        let applied = rb_self.call(ruby, move |trellis| {
            Ok(PlainApplied::from(&trellis.apply(&text)?))
        })?;
        applied_hash(ruby, applied)
    }

    /// Every registered transform definition, oldest first.
    fn definitions(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let summaries = rb_self.call(ruby, |trellis| {
            Ok(trellis
                .definitions()?
                .iter()
                .map(PlainDefinitionSummary::from)
                .collect::<Vec<_>>())
        })?;
        array(ruby, summaries, definition_summary_hash)
    }

    /// Every registered relationship, oldest first.
    fn relationships(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let summaries = rb_self
            .call(ruby, BlockingTrellis::relationships)?
            .iter()
            .map(|summary| {
                PlainRelationshipSummary::try_from(summary).map_err(|err| raise(ruby, err))
            })
            .collect::<Result<Vec<_>, _>>()?;
        array(ruby, summaries, relationship_summary_hash)
    }

    /// Parks a go-live catch-up re-read of `source_table` for the staging
    /// worker.
    fn request_backfill(ruby: &Ruby, rb_self: &Self, source_table: String) -> Result<(), Error> {
        rb_self.call(ruby, move |trellis| trellis.request_backfill(&source_table))
    }

    /// Releases one key `transform` holds in quarantine: `source_table`
    /// (either spelling) and `key` as `sample_quarantined` reports them.
    fn release_key(
        ruby: &Ruby,
        rb_self: &Self,
        transform: String,
        source_table: String,
        key: String,
    ) -> Result<(), Error> {
        rb_self.call(ruby, move |trellis| {
            trellis.release_key(&transform, &source_table, &key)
        })
    }

    /// Every key poisoned after `watermark_micros` (epoch microseconds),
    /// oldest first.
    fn poisoned_since(ruby: &Ruby, rb_self: &Self, watermark_micros: i64) -> Result<RArray, Error> {
        rb_self.check_pid(ruby)?;
        let watermark =
            system_time_from_epoch_micros(watermark_micros).map_err(|err| raise(ruby, err))?;
        let entries = rb_self.call(ruby, move |trellis| {
            Ok(trellis
                .poisoned_since(watermark)?
                .iter()
                .map(PlainPoisonEntry::from)
                .collect::<Vec<_>>())
        })?;
        array(ruby, entries, poison_entry_hash)
    }

    /// Every quarantined transform and paused column.
    fn quarantined(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let entries = rb_self.call(ruby, |trellis| {
            Ok(trellis
                .quarantined()?
                .iter()
                .map(PlainQuarantineEntry::from)
                .collect::<Vec<_>>())
        })?;
        array(ruby, entries, quarantine_entry_hash)
    }

    /// One target's state, by its `transform` or `transform.column` address.
    fn quarantine_status(ruby: &Ruby, rb_self: &Self, target: String) -> Result<RHash, Error> {
        let entry = rb_self.call(ruby, move |trellis| {
            Ok(PlainQuarantineEntry::from(
                &trellis.quarantine_status(&target)?,
            ))
        })?;
        quarantine_entry_hash(ruby, entry)
    }

    /// Up to `limit` of `target`'s quarantined rows, after the opaque
    /// `cursor` (`nil` for the first page).
    fn sample_quarantined(
        ruby: &Ruby,
        rb_self: &Self,
        target: String,
        cursor: Option<String>,
        limit: i64,
    ) -> Result<RHash, Error> {
        rb_self.check_pid(ruby)?;
        let after = decode_cursor(cursor.as_deref()).map_err(|err| raise(ruby, err))?;
        let page = rb_self.call(ruby, move |trellis| {
            let samples = trellis.sample_quarantined(&target, after, limit)?;
            Ok(PlainSamplePage::new(&samples, cursor.as_deref()))
        })?;
        let samples = array(ruby, page.samples, |ruby, sample| {
            record(
                ruby,
                [
                    ("src_table", ruby.into_value(sample.src_table)),
                    ("key", ruby.into_value(sample.key)),
                    ("error_message", ruby.into_value(sample.error_message)),
                ],
            )
        })?;
        record(
            ruby,
            [
                ("samples", samples.as_value()),
                ("next_cursor", ruby.into_value(page.next_cursor)),
            ],
        )
    }

    /// Whether any drain worker in the fleet is alive.
    fn has_live_drain_workers(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        rb_self.call(ruby, BlockingTrellis::has_live_drain_workers)
    }

    /// Whether this instance's staging worker is alive anywhere in the fleet.
    fn has_live_staging_worker(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        rb_self.call(ruby, BlockingTrellis::has_live_staging_worker)
    }

    /// A read-your-writes token, as an opaque string.
    fn watermark_token(ruby: &Ruby, rb_self: &Self) -> Result<String, Error> {
        rb_self.call(ruby, |trellis| {
            trellis.watermark_token().map(encode_watermark)
        })
    }

    /// Waits up to `timeout_ms` for every change committed at or before
    /// `token` to reach its targets. The handle runs one call at a time, so
    /// every other call on it queues behind this one.
    fn await_converged(
        ruby: &Ruby,
        rb_self: &Self,
        token: String,
        timeout_ms: u64,
    ) -> Result<(), Error> {
        rb_self.check_pid(ruby)?;
        let token = decode_watermark(&token).map_err(|err| raise(ruby, err))?;
        rb_self.call(ruby, move |trellis| {
            trellis.await_converged(token, Duration::from_millis(timeout_ms))
        })
    }

    /// Audits up to `limit` of `target_table`'s keys after `after` (`nil`
    /// for the first page) against a fresh recompute from the source, in
    /// `mode` (`"standard"` or `"strict"`), waiting up to `timeout_ms` per
    /// convergence await. Like `await_converged`, it holds the handle for as
    /// long as it waits.
    fn self_check(
        ruby: &Ruby,
        rb_self: &Self,
        target_table: String,
        after: Option<String>,
        limit: i64,
        mode: String,
        timeout_ms: u64,
    ) -> Result<RHash, Error> {
        rb_self.check_pid(ruby)?;
        let mode = self_check_mode(&mode).map_err(|err| raise(ruby, err))?;
        let report = rb_self.call(ruby, move |trellis| {
            let report = trellis.self_check(
                &target_table,
                SelfCheckScope { after, limit },
                mode,
                Duration::from_millis(timeout_ms),
            )?;
            Ok(PlainSelfCheckReport::from(&report))
        })?;
        self_check_hash(ruby, report)
    }

    /// The configuration the handle connected with, all but the connection
    /// string (see [`PlainConfig`]).
    fn config(ruby: &Ruby, rb_self: &Self) -> Result<RHash, Error> {
        let config = rb_self.call(ruby, |trellis| Ok(PlainConfig::from(trellis.config())))?;
        record(
            ruby,
            [
                ("schema", ruby.into_value(config.schema)),
                ("target_schema", ruby.into_value(config.target_schema)),
                ("pool_max_size", ruby.into_value(config.pool_max_size)),
                (
                    "pool_wait_timeout_ms",
                    ruby.into_value(config.pool_wait_timeout_ms),
                ),
            ],
        )
    }

    /// Stops the instance's background work and joins its runtime thread.
    /// Idempotent: shutting down a handle that is already shut down is fine.
    fn shutdown(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        rb_self.check_pid(ruby)?;
        let shared = Arc::clone(&rb_self.trellis);
        blocking(ruby, move || {
            let trellis = shared.write().map_err(|_| poisoned())?.take();
            match trellis {
                Some(trellis) => trellis.shutdown().map_err(PlainError::from),
                None => Ok(()),
            }
        })
    }
}

/// Connects a new instance. Every option is required here: the defaults are
/// `lib/trellis/pg.rb`'s to document, and nothing falls back to the environment.
///
/// Raises `Trellis::ForkedHandleError`, before starting anything, in a
/// process forked while its parent had an engine running (issue #600).
fn connect(
    ruby: &Ruby,
    url: String,
    schema: String,
    target_schema: String,
    staging: bool,
    drain_threads: usize,
    worker_threads: usize,
) -> Result<Handle, Error> {
    engine::check().map_err(|err| forked_handle_error(ruby, forked_while_running(&err)))?;
    let trellis = blocking(ruby, move || {
        let config = Config::with_schema(url, schema)?.with_target_schema(target_schema)?;
        let options = TrellisOptions {
            staging,
            drain_threads,
            worker_threads: Some(worker_threads),
        };
        // Can't fail after `check` passed (a process's answer never
        // changes), but if it did, it's still an error rather than a hang.
        Engine::connect(config, options)
            .map_err(|err| PlainError::new(ErrorCode::Validation, forked_while_running(&err)))?
            .map_err(PlainError::from)
    })?;
    Ok(Handle {
        trellis: Arc::new(RwLock::new(Some(trellis))),
        owner_pid: std::process::id(),
    })
}

/// `Trellis::ForkedHandleError` with `message`.
fn forked_handle_error(ruby: &Ruby, message: String) -> Error {
    match trellis_module(ruby)
        .and_then(|module| module.const_get::<_, ExceptionClass>("ForkedHandleError"))
    {
        Ok(class) => Error::new(class, message),
        Err(err) => err,
    }
}

fn forked_while_running(err: &ForkedWhileRunning) -> String {
    format!(
        "this process ({}) was forked from process {} while that process had a Trellis engine \
         running, so it may have inherited a lock one of the engine's threads held, which \
         nothing in this process can release: it can't connect. Call Trellis.shutdown before \
         forking and Trellis.connect after: Puma's before_fork and before_worker_boot, and \
         with fork_worker, before_worker_fork and after_worker_fork too (\"Forking servers\" \
         in clients/ruby/README.md)",
        std::process::id(),
        err.parent
    )
}

fn poisoned() -> PlainError {
    PlainError::new(
        ErrorCode::Internal,
        "a call on this Trellis handle panicked; connect a new handle",
    )
}

/// Runs `call` off the GVL (see [`run_without_gvl`]) and raises its error, if
/// any, as the `Trellis::Error` subclass for its code.
fn blocking<T: Send + 'static>(
    ruby: &Ruby,
    call: impl FnOnce() -> Reply<T> + Send + 'static,
) -> Result<T, Error> {
    run_without_gvl(call)?.map_err(|err| raise(ruby, err))
}

/// Runs `call` on a helper thread and waits for its reply with the GVL
/// released, so other Ruby threads run meanwhile, and with an unblocking
/// function, so `Thread#kill`, `Thread#raise` and Ctrl-C reach a thread
/// that's waiting.
///
/// An interrupt abandons the wait, not the work: the helper thread finishes
/// the call (a `define` may still register its transform) and its reply is
/// dropped. That's the only way to honour the interrupt, because
/// `BlockingTrellis` can't cancel a job it has been sent.
///
/// The outer `Err` is the interrupt's (an exception, or `Thread#kill`'s
/// jump), for Magnus to resume once this returns; the inner [`Reply`] is the
/// call's own result.
fn run_without_gvl<T: Send + 'static>(
    call: impl FnOnce() -> Reply<T> + Send + 'static,
) -> Result<Reply<T>, Error> {
    let pending = Arc::new(Pending::<T>::new());
    let worker = Arc::clone(&pending);
    let spawned = std::thread::Builder::new()
        .name("trellis-ruby-call".to_string())
        .spawn(move || {
            // A panic must still produce a reply, or the wait below would
            // never end.
            let reply = catch_unwind(AssertUnwindSafe(call)).unwrap_or_else(|_| {
                Err(PlainError::new(
                    ErrorCode::Internal,
                    "a Trellis call panicked; the handle may be unusable",
                ))
            });
            worker.finish(reply);
        });
    if let Err(err) = spawned {
        return Ok(Err(PlainError::new(
            ErrorCode::Internal,
            format!("could not start a thread for the Trellis call: {err}"),
        )));
    }

    let data = Arc::as_ptr(&pending).cast_mut().cast::<c_void>();
    loop {
        // SAFETY: `wait_for_reply::<T>` and `interrupt::<T>` only read
        // `data` as the `Pending<T>` it points to, which `pending` keeps
        // alive for this whole call, and Ruby calls neither once
        // `rb_thread_call_without_gvl` has returned. Ruby may act on a
        // pending interrupt (raise, or unwind for `Thread#kill`) before or
        // after the wait; `protect` catches that jump, so it never unwinds
        // through this frame, and returns it as an `Error` to resume later.
        magnus::rb_sys::protect(|| unsafe {
            rb_sys::rb_thread_call_without_gvl(
                Some(wait_for_reply::<T>),
                data,
                Some(interrupt::<T>),
                data,
            );
            rb_sys::Qnil as rb_sys::VALUE
        })?;
        if let Some(reply) = pending.take_reply() {
            return Ok(reply);
        }
        // Woken by the unblocking function with no reply yet. Let Ruby act
        // on the interrupt: an exception or a kill leaves through `?`. One
        // that turns out to raise nothing (a signal trap that returns, say)
        // resumes the wait for the same helper thread's reply.
        //
        // SAFETY: as above, `protect` keeps the jump out of this frame.
        magnus::rb_sys::protect(|| unsafe {
            rb_sys::rb_thread_check_ints();
            rb_sys::Qnil as rb_sys::VALUE
        })?;
        pending.rearm();
    }
}

/// A reply the helper thread hands back to the waiting Ruby thread.
struct Pending<T> {
    slot: Mutex<Slot<T>>,
    changed: Condvar,
}

struct Slot<T> {
    reply: Option<Reply<T>>,
    /// Set by [`interrupt`], cleared by [`Pending::rearm`] before each wait.
    interrupted: bool,
}

impl<T> Pending<T> {
    fn new() -> Self {
        Pending {
            slot: Mutex::new(Slot {
                reply: None,
                interrupted: false,
            }),
            changed: Condvar::new(),
        }
    }

    /// Never panics: the callbacks below run inside Ruby's C frames, where a
    /// panic would abort the process. Nothing panics while holding the lock,
    /// so a poisoned one still guards a consistent slot.
    fn lock(&self) -> MutexGuard<'_, Slot<T>> {
        self.slot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn finish(&self, reply: Reply<T>) {
        self.lock().reply = Some(reply);
        self.changed.notify_all();
    }

    fn take_reply(&self) -> Option<Reply<T>> {
        self.lock().reply.take()
    }

    fn rearm(&self) {
        self.lock().interrupted = false;
    }
}

/// The GVL-released half of [`run_without_gvl`]: blocks until the reply
/// arrives or [`interrupt`] fires. Touches no Ruby object.
unsafe extern "C" fn wait_for_reply<T>(data: *mut c_void) -> *mut c_void {
    // SAFETY: see `run_without_gvl`.
    let pending = unsafe { &*data.cast::<Pending<T>>() };
    let mut slot = pending.lock();
    while slot.reply.is_none() && !slot.interrupted {
        slot = pending
            .changed
            .wait(slot)
            .unwrap_or_else(PoisonError::into_inner);
    }
    std::ptr::null_mut()
}

/// The unblocking function: Ruby calls it, from another thread, to wake a
/// thread in [`wait_for_reply`] that has an interrupt to handle. It takes a
/// lock, so it isn't async-signal-safe, and it doesn't claim to be
/// (`RB_NOGVL_UBF_ASYNC_SAFE`), so Ruby never calls it from a signal handler.
unsafe extern "C" fn interrupt<T>(data: *mut c_void) {
    // SAFETY: see `run_without_gvl`.
    let pending = unsafe { &*data.cast::<Pending<T>>() };
    pending.lock().interrupted = true;
    pending.changed.notify_all();
}

fn trellis_module(ruby: &Ruby) -> Result<RModule, Error> {
    ruby.class_object().const_get("Trellis")
}

/// `err` as the `Trellis::Error` subclass `Trellis::Error.from_native` picks
/// for its code.
fn raise(ruby: &Ruby, err: PlainError) -> Error {
    let exception = trellis_module(ruby)
        .and_then(|module| module.const_get::<_, RClass>("Error"))
        .and_then(|class| class.funcall::<_, _, Exception>("from_native", (err.code, err.message)));
    match exception {
        Ok(exception) => exception.into(),
        Err(err) => err,
    }
}

/// `word`'s symbol. `word` is always one of the `'static` words
/// [`symbol_words`] lists, all interned by [`init`], never text read from
/// the database.
fn word_symbol(ruby: &Ruby, word: &'static str) -> StaticSymbol {
    ruby.sym_new(word)
}

fn word_symbols(ruby: &Ruby, words: impl IntoIterator<Item = &'static str>) -> RArray {
    ruby.ary_from_iter(words.into_iter().map(|word| word_symbol(ruby, word)))
}

/// Every word this extension turns into a symbol: the closed sets [`init`]
/// interns.
fn symbol_words() -> impl Iterator<Item = &'static str> {
    transform_status_names()
        .into_iter()
        .chain(quarantine_state_names())
        .chain(relationship_cardinality_names())
        .chain(PlainApplied::KINDS)
        .chain(SELF_CHECK_OUTCOMES)
        .chain(DIVERGENCE_KINDS)
        .chain(capture_failure_kind_names())
}

/// A hash from symbol keys to `fields`' values: the shape every record
/// crosses as, for `lib/trellis/pg.rb` to turn into its `Data` value.
fn record<const N: usize>(ruby: &Ruby, fields: [(&str, Value); N]) -> Result<RHash, Error> {
    let hash = ruby.hash_new();
    for (name, value) in fields {
        hash.aset(ruby.sym_new(name), value)?;
    }
    Ok(hash)
}

/// `items`, each turned into a Ruby value by `convert`.
fn array<T, V: IntoValue>(
    ruby: &Ruby,
    items: Vec<T>,
    convert: impl Fn(&Ruby, T) -> Result<V, Error>,
) -> Result<RArray, Error> {
    let array = ruby.ary_new_capa(items.len());
    for item in items {
        array.push(convert(ruby, item)?)?;
    }
    Ok(array)
}

fn definition_hash(ruby: &Ruby, definition: PlainDefinition) -> Result<RHash, Error> {
    let columns = ruby.hash_new();
    for (name, type_name) in definition.source_columns {
        columns.aset(name, type_name)?;
    }
    record(
        ruby,
        [
            ("id", ruby.into_value(definition.id)),
            ("target_table", ruby.into_value(definition.target_table)),
            ("source_table", ruby.into_value(definition.source_table)),
            ("source_version", ruby.into_value(definition.source_version)),
            ("status", word_symbol(ruby, definition.status).as_value()),
            ("source_columns", columns.as_value()),
        ],
    )
}

fn definition_summary_hash(ruby: &Ruby, summary: PlainDefinitionSummary) -> Result<RHash, Error> {
    let failure = summary
        .backfill_failure
        .map(|failure| backfill_failure_hash(ruby, failure))
        .transpose()?;
    let halt = summary
        .halt
        .map(|halt| capture_failure_hash(ruby, halt))
        .transpose()?;
    record(
        ruby,
        [
            ("id", ruby.into_value(summary.id)),
            ("target_table", ruby.into_value(summary.target_table)),
            ("source_table", ruby.into_value(summary.source_table)),
            ("source_version", ruby.into_value(summary.source_version)),
            ("status", word_symbol(ruby, summary.status).as_value()),
            (
                "created_at_micros",
                ruby.into_value(summary.created_at_micros),
            ),
            ("backfill_failure", ruby.into_value(failure)),
            ("halt", ruby.into_value(halt)),
        ],
    )
}

fn status_hash(ruby: &Ruby, status: PlainDefinitionStatus) -> Result<RHash, Error> {
    let failure = status
        .backfill_failure
        .map(|failure| backfill_failure_hash(ruby, failure))
        .transpose()?;
    let wait = status
        .capture_wait
        .map(|wait| capture_wait_hash(ruby, wait))
        .transpose()?;
    let capture_failure = status
        .capture_failure
        .map(|failure| capture_failure_hash(ruby, failure))
        .transpose()?;
    let held_keys = status
        .held_keys
        .map(|held| held_keys_hash(ruby, held))
        .transpose()?;
    let drain_failure = status
        .drain_failure
        .map(|failure| drain_failure_hash(ruby, failure))
        .transpose()?;
    record(
        ruby,
        [
            ("status", word_symbol(ruby, status.status).as_value()),
            ("backfill_failure", ruby.into_value(failure)),
            ("capture_wait", ruby.into_value(wait)),
            ("capture_failure", ruby.into_value(capture_failure)),
            ("held_keys", ruby.into_value(held_keys)),
            ("drain_failure", ruby.into_value(drain_failure)),
        ],
    )
}

/// A drain page that keeps failing with nothing charged or paused.
fn drain_failure_hash(ruby: &Ruby, failure: PlainDrainFailure) -> Result<RHash, Error> {
    record(
        ruby,
        [
            ("seg_seq", ruby.into_value(failure.seg_seq)),
            ("tables", ruby.into_value(failure.tables)),
            ("error", ruby.into_value(failure.error)),
            ("sqlstate", ruby.into_value(failure.sqlstate)),
            ("since_micros", ruby.into_value(failure.since_micros)),
            (
                "last_seen_micros",
                ruby.into_value(failure.last_seen_micros),
            ),
            ("attempts", ruby.into_value(failure.attempts)),
        ],
    )
}

/// How many keys a definition holds in quarantine, and since when.
fn held_keys_hash(ruby: &Ruby, held: PlainHeldKeys) -> Result<RHash, Error> {
    record(
        ruby,
        [
            ("count", ruby.into_value(held.count)),
            (
                "oldest_poisoned_at_micros",
                ruby.into_value(held.oldest_poisoned_at_micros),
            ),
        ],
    )
}

fn capture_wait_hash(ruby: &Ruby, wait: PlainCaptureWait) -> Result<RHash, Error> {
    record(
        ruby,
        [
            ("table", ruby.into_value(wait.table)),
            ("operation", ruby.into_value(wait.operation)),
            ("lock_mode", ruby.into_value(wait.lock_mode)),
            (
                "waiting_since_micros",
                ruby.into_value(wait.waiting_since_micros),
            ),
            (
                "observed_at_micros",
                ruby.into_value(wait.observed_at_micros),
            ),
            ("blockers", ruby.into_value(wait.blockers)),
        ],
    )
}

fn capture_failure_hash(ruby: &Ruby, failure: PlainCaptureFailure) -> Result<RHash, Error> {
    record(
        ruby,
        [
            ("kind", word_symbol(ruby, failure.kind).as_value()),
            ("source_table", ruby.into_value(failure.source_table)),
            ("columns", ruby.into_value(failure.columns)),
            ("error", ruby.into_value(failure.error)),
            (
                "detected_at_micros",
                ruby.into_value(failure.detected_at_micros),
            ),
        ],
    )
}

fn backfill_failure_hash(ruby: &Ruby, failure: PlainBackfillFailure) -> Result<RHash, Error> {
    record(
        ruby,
        [
            ("source_table", ruby.into_value(failure.source_table)),
            ("attempts", ruby.into_value(failure.attempts)),
            ("last_error", ruby.into_value(failure.last_error)),
            (
                "next_attempt_at_micros",
                ruby.into_value(failure.next_attempt_at_micros),
            ),
        ],
    )
}

fn relationship_hash(ruby: &Ruby, relationship: PlainRelationship) -> Result<RHash, Error> {
    record(
        ruby,
        [
            ("id", ruby.into_value(relationship.id)),
            ("name", ruby.into_value(relationship.name)),
            ("from_schema", ruby.into_value(relationship.from_schema)),
            ("from_table", ruby.into_value(relationship.from_table)),
            ("from_col", ruby.into_value(relationship.from_col)),
            ("to_schema", ruby.into_value(relationship.to_schema)),
            ("to_table", ruby.into_value(relationship.to_table)),
            ("to_col", ruby.into_value(relationship.to_col)),
            (
                "cardinality",
                word_symbol(ruby, relationship.cardinality).as_value(),
            ),
            ("warnings", ruby.into_value(relationship.warnings)),
        ],
    )
}

fn relationship_summary_hash(
    ruby: &Ruby,
    summary: PlainRelationshipSummary,
) -> Result<RHash, Error> {
    record(
        ruby,
        [
            ("id", ruby.into_value(summary.id)),
            ("name", ruby.into_value(summary.name)),
            ("from_schema", ruby.into_value(summary.from_schema)),
            ("from_table", ruby.into_value(summary.from_table)),
            ("from_col", ruby.into_value(summary.from_col)),
            ("to_schema", ruby.into_value(summary.to_schema)),
            ("to_table", ruby.into_value(summary.to_table)),
            ("to_col", ruby.into_value(summary.to_col)),
            (
                "cardinality",
                word_symbol(ruby, summary.cardinality).as_value(),
            ),
            (
                "created_at_micros",
                ruby.into_value(summary.created_at_micros),
            ),
        ],
    )
}

/// What `apply` did. `kind` is one of [`PlainApplied::KINDS`]; each other
/// field is set only for the kinds that carry it (`definition` for
/// `transform_defined` and `altered`, `relationship` for
/// `relationship_defined`, `columns` for `resumed`, and `added`, `dropped`
/// and `altered` for `altered`), and is `nil` otherwise: the same flat shape
/// the Elixir NIF hands `Trellis.Applied`.
fn applied_hash(ruby: &Ruby, applied: PlainApplied) -> Result<RHash, Error> {
    let kind = word_symbol(ruby, applied.kind()).as_value();
    let nil = ruby.qnil().as_value();
    let (mut definition, mut relationship, mut columns) = (nil, nil, nil);
    let (mut added, mut dropped, mut altered) = (nil, nil, nil);
    match applied {
        PlainApplied::TransformDefined(plain) => {
            definition = definition_hash(ruby, plain)?.as_value();
        }
        PlainApplied::RelationshipDefined(plain) => {
            relationship = relationship_hash(ruby, plain)?.as_value();
        }
        PlainApplied::Resumed { columns: resumed } => columns = ruby.into_value(resumed),
        PlainApplied::Altered {
            definition: plain,
            added: plain_added,
            dropped: plain_dropped,
            altered: plain_altered,
        } => {
            definition = definition_hash(ruby, plain)?.as_value();
            added = ruby.into_value(plain_added);
            dropped = ruby.into_value(plain_dropped);
            altered = ruby.into_value(plain_altered);
        }
        PlainApplied::Paused | PlainApplied::Dropped | PlainApplied::Unknown => {}
    }
    record(
        ruby,
        [
            ("kind", kind),
            ("definition", definition),
            ("relationship", relationship),
            ("columns", columns),
            ("added", added),
            ("dropped", dropped),
            ("altered", altered),
        ],
    )
}

fn quarantine_entry_hash(ruby: &Ruby, entry: PlainQuarantineEntry) -> Result<RHash, Error> {
    record(
        ruby,
        [
            ("target", ruby.into_value(entry.target)),
            ("state", word_symbol(ruby, entry.state).as_value()),
            ("paused_at_micros", ruby.into_value(entry.paused_at_micros)),
            ("last_error", ruby.into_value(entry.last_error)),
        ],
    )
}

fn poison_entry_hash(ruby: &Ruby, entry: PlainPoisonEntry) -> Result<RHash, Error> {
    record(
        ruby,
        [
            ("transform", ruby.into_value(entry.transform)),
            ("src_table", ruby.into_value(entry.src_table)),
            ("key", ruby.into_value(entry.key)),
            ("last_error", ruby.into_value(entry.last_error)),
            (
                "poisoned_at_micros",
                ruby.into_value(entry.poisoned_at_micros),
            ),
        ],
    )
}

/// What `self_check` found. `outcome` is one of [`SELF_CHECK_OUTCOMES`];
/// `divergences` is empty unless it is `diverged`.
fn self_check_hash(ruby: &Ruby, report: PlainSelfCheckReport) -> Result<RHash, Error> {
    let divergences = array(ruby, report.divergences, divergence_hash)?;
    let held_keys = report
        .held_keys
        .map(|held| held_keys_hash(ruby, held))
        .transpose()?;
    let drain_failures = array(ruby, report.drain_failures, drain_failure_hash)?;
    record(
        ruby,
        [
            ("target", ruby.into_value(report.target)),
            ("checked_through", ruby.into_value(report.checked_through)),
            ("rows_compared", ruby.into_value(report.rows_compared)),
            ("next_after", ruby.into_value(report.next_after)),
            ("outcome", word_symbol(ruby, report.outcome).as_value()),
            ("divergences", divergences.as_value()),
            ("held_keys", ruby.into_value(held_keys)),
            ("drain_failures", drain_failures.as_value()),
        ],
    )
}

/// One divergence. `kind` is one of [`DIVERGENCE_KINDS`]; see
/// [`PlainDivergence`] for which other fields each kind sets.
fn divergence_hash(ruby: &Ruby, divergence: PlainDivergence) -> Result<RHash, Error> {
    record(
        ruby,
        [
            ("kind", word_symbol(ruby, divergence.kind).as_value()),
            ("key", ruby.into_value(divergence.key)),
            ("column", ruby.into_value(divergence.column)),
            ("persisted", ruby.into_value(divergence.persisted)),
            ("recomputed", ruby.into_value(divergence.recomputed)),
            ("table", ruby.into_value(divergence.table)),
            ("detail", ruby.into_value(divergence.detail)),
        ],
    )
}

/// Every error code `trellis-embed` maps explicitly, for the test asserting
/// each one has its own `Trellis::Error` subclass.
fn error_codes() -> Vec<&'static str> {
    ERROR_CODES.to_vec()
}

/// Every status symbol `define`, `status` and `definitions` can return.
fn status_names(ruby: &Ruby) -> RArray {
    word_symbols(ruby, transform_status_names())
}

/// Every state symbol `quarantined` and `quarantine_status` can return.
fn quarantine_states(ruby: &Ruby) -> RArray {
    word_symbols(ruby, quarantine_state_names())
}

/// Every cardinality symbol a relationship can carry.
fn cardinality_names(ruby: &Ruby) -> RArray {
    word_symbols(ruby, relationship_cardinality_names())
}

/// Every outcome kind `apply`'s result can carry.
fn applied_kinds(ruby: &Ruby) -> RArray {
    word_symbols(ruby, PlainApplied::KINDS)
}

/// Every outcome symbol `self_check`'s report can carry.
fn self_check_outcomes(ruby: &Ruby) -> RArray {
    word_symbols(ruby, SELF_CHECK_OUTCOMES)
}

/// Every divergence kind symbol `self_check`'s report can carry.
fn divergence_kinds(ruby: &Ruby) -> RArray {
    word_symbols(ruby, DIVERGENCE_KINDS)
}

/// Every kind symbol a capture failure can carry.
fn capture_failure_kinds(ruby: &Ruby) -> RArray {
    word_symbols(ruby, capture_failure_kind_names())
}

/// Every statement kind `trellis`'s grammar has, for the test asserting
/// `apply`'s round trip covers each one.
fn statement_kinds() -> Vec<&'static str> {
    trellis::StatementKind::ALL
        .iter()
        .map(|kind| kind.as_str())
        .collect()
}

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    // Interns every symbol a result can carry up front, from the closed
    // sets, so encoding a result never creates one.
    for word in symbol_words() {
        word_symbol(ruby, word);
    }

    let native = ruby.define_module("Trellis")?.define_module("Native")?;
    native.define_module_function("connect", function!(connect, 6))?;
    native.define_module_function("error_codes", function!(error_codes, 0))?;
    native.define_module_function("status_names", function!(status_names, 0))?;
    native.define_module_function("quarantine_states", function!(quarantine_states, 0))?;
    native.define_module_function("cardinality_names", function!(cardinality_names, 0))?;
    native.define_module_function("applied_kinds", function!(applied_kinds, 0))?;
    native.define_module_function("self_check_outcomes", function!(self_check_outcomes, 0))?;
    native.define_module_function("divergence_kinds", function!(divergence_kinds, 0))?;
    native.define_module_function("capture_failure_kinds", function!(capture_failure_kinds, 0))?;
    native.define_module_function("statement_kinds", function!(statement_kinds, 0))?;

    let handle = native.define_class("Handle", ruby.class_object())?;
    handle.define_method("owner_pid", method!(Handle::owner_pid, 0))?;
    handle.define_method("migrate", method!(Handle::migrate, 0))?;
    handle.define_method("define", method!(Handle::define, 1))?;
    handle.define_method("apply", method!(Handle::apply, 1))?;
    handle.define_method("status", method!(Handle::status, 1))?;
    handle.define_method("definitions", method!(Handle::definitions, 0))?;
    handle.define_method("relationships", method!(Handle::relationships, 0))?;
    handle.define_method("request_backfill", method!(Handle::request_backfill, 1))?;
    handle.define_method("release_key", method!(Handle::release_key, 3))?;
    handle.define_method("poisoned_since", method!(Handle::poisoned_since, 1))?;
    handle.define_method("quarantined", method!(Handle::quarantined, 0))?;
    handle.define_method("quarantine_status", method!(Handle::quarantine_status, 1))?;
    handle.define_method("sample_quarantined", method!(Handle::sample_quarantined, 3))?;
    handle.define_method(
        "has_live_drain_workers",
        method!(Handle::has_live_drain_workers, 0),
    )?;
    handle.define_method(
        "has_live_staging_worker",
        method!(Handle::has_live_staging_worker, 0),
    )?;
    handle.define_method("watermark_token", method!(Handle::watermark_token, 0))?;
    handle.define_method("await_converged", method!(Handle::await_converged, 2))?;
    handle.define_method("self_check", method!(Handle::self_check, 5))?;
    handle.define_method("config", method!(Handle::config, 0))?;
    handle.define_method("shutdown", method!(Handle::shutdown, 0))?;
    Ok(())
}
