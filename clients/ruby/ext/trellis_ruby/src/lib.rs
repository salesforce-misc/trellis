//! The Ruby binding's native extension: a thin Magnus wrapper over
//! [`trellis::BlockingTrellis`] (`docs/decisions/0010-embeddable-clients.md`).
//!
//! Only `lib/trellis.rb` calls these; the public API, defaults, the
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
//!   `lib/trellis.rb` turns those into `Data` objects. Status words become
//!   symbols, but only words from the closed set `trellis-embed` lists, which
//!   [`init`] interns up front, never a string read from the database.
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

use engine::{Engine, ForkedWhileRunning};
use magnus::prelude::*;
use magnus::{
    Error, Exception, ExceptionClass, RArray, RClass, RHash, RModule, Ruby, StaticSymbol, function,
    method,
};
use trellis::{BlockingTrellis, Config, ErrorCode, TrellisError, TrellisOptions};
use trellis_embed::{
    ERROR_CODES, PlainBackfillFailure, PlainDefinition, PlainDefinitionStatus, PlainError,
    require_transform_statement, transform_status_names,
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
                 (after forking: Puma's on_worker_boot, Passenger's starting_worker_process)",
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
/// `lib/trellis.rb`'s to document, and nothing falls back to the environment.
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
         forking (Puma's before_fork), and Trellis.connect after (on_worker_boot)",
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
/// [`transform_status_names`] lists, all interned by [`init`], never text
/// read from the database.
fn word_symbol(ruby: &Ruby, word: &'static str) -> StaticSymbol {
    ruby.sym_new(word)
}

fn definition_hash(ruby: &Ruby, definition: PlainDefinition) -> Result<RHash, Error> {
    let columns = ruby.hash_new();
    for (name, type_name) in definition.source_columns {
        columns.aset(name, type_name)?;
    }
    let hash = ruby.hash_new();
    hash.aset(ruby.sym_new("id"), definition.id)?;
    hash.aset(ruby.sym_new("target_table"), definition.target_table)?;
    hash.aset(ruby.sym_new("source_table"), definition.source_table)?;
    hash.aset(ruby.sym_new("source_version"), definition.source_version)?;
    hash.aset(ruby.sym_new("status"), word_symbol(ruby, definition.status))?;
    hash.aset(ruby.sym_new("source_columns"), columns)?;
    Ok(hash)
}

fn status_hash(ruby: &Ruby, status: PlainDefinitionStatus) -> Result<RHash, Error> {
    let failure = status
        .backfill_failure
        .map(|failure| backfill_failure_hash(ruby, failure))
        .transpose()?;
    let hash = ruby.hash_new();
    hash.aset(ruby.sym_new("status"), word_symbol(ruby, status.status))?;
    hash.aset(ruby.sym_new("backfill_failure"), failure)?;
    Ok(hash)
}

fn backfill_failure_hash(ruby: &Ruby, failure: PlainBackfillFailure) -> Result<RHash, Error> {
    let hash = ruby.hash_new();
    hash.aset(ruby.sym_new("source_table"), failure.source_table)?;
    hash.aset(ruby.sym_new("attempts"), failure.attempts)?;
    hash.aset(ruby.sym_new("last_error"), failure.last_error)?;
    hash.aset(
        ruby.sym_new("next_attempt_at_micros"),
        failure.next_attempt_at_micros,
    )?;
    Ok(hash)
}

/// Every error code `trellis-embed` maps explicitly, for the test asserting
/// each one has its own `Trellis::Error` subclass.
fn error_codes() -> Vec<&'static str> {
    ERROR_CODES.to_vec()
}

/// Every status symbol `define` and `status` can return.
fn status_names(ruby: &Ruby) -> RArray {
    ruby.ary_from_iter(
        transform_status_names()
            .into_iter()
            .map(|word| word_symbol(ruby, word)),
    )
}

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    // Interns every status symbol up front, from the closed set, so
    // encoding a result never creates one.
    status_names(ruby);

    let native = ruby.define_module("Trellis")?.define_module("Native")?;
    native.define_module_function("connect", function!(connect, 6))?;
    native.define_module_function("error_codes", function!(error_codes, 0))?;
    native.define_module_function("status_names", function!(status_names, 0))?;

    let handle = native.define_class("Handle", ruby.class_object())?;
    handle.define_method("owner_pid", method!(Handle::owner_pid, 0))?;
    handle.define_method("migrate", method!(Handle::migrate, 0))?;
    handle.define_method("define", method!(Handle::define, 1))?;
    handle.define_method("status", method!(Handle::status, 1))?;
    handle.define_method("shutdown", method!(Handle::shutdown, 0))?;
    Ok(())
}
