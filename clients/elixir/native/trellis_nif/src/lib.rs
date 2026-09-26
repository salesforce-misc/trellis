//! The Elixir binding's NIF: a thin Rustler wrapper over
//! [`trellis::BlockingTrellis`] (`docs/decisions/0010-embeddable-clients.md`).
//!
//! Only `Trellis.Native` calls these; the public API, defaults and error
//! structs live in the Elixir modules under `lib/`. The contract here is
//! deliberately narrow:
//!
//! - **Every NIF runs on a dirty IO scheduler** (ADR-0010 decision 3), the
//!   constant lookups included, so there is no rule to remember about which
//!   ones are "cheap".
//! - **Every NIF returns `{:ok, value}` or `{:error, {code, message}}`**, where
//!   `code` is [`trellis_embed::PlainError::code`] as a binary. Elixir maps it
//!   to an atom from a closed set, with a fallback for a code it doesn't know.
//! - **Only plain data crosses** (decision 4). The flattening itself is
//!   `trellis-embed`'s; this crate only turns its plain values into terms.
//!   Status words become atoms here, but only ever words from
//!   [`trellis_embed::transform_status_names`], which [`load`] allocates up
//!   front, never a string read from the database.
//!
//! The handle is a [`ResourceArc`] over [`Handle`]. Dropping the last
//! reference (the BEAM garbage-collecting it) drops the [`BlockingTrellis`],
//! which closes its job channel and lets its runtime thread wind down without
//! blocking the scheduler that ran the destructor. That is only the backstop:
//! `shutdown/1` is the clean, joined stop.

use std::collections::HashMap;
use std::sync::RwLock;

use rustler::{Atom, Env, NifMap, ResourceArc, Term};
use trellis::{BlockingTrellis, Config, ErrorCode, TrellisOptions};
use trellis_embed::{
    ERROR_CODES, PlainBackfillFailure, PlainDefinition, PlainDefinitionStatus, PlainError,
    transform_status_names,
};

/// What every NIF returns: `{:ok, T}` or `{:error, {code, message}}`.
type NifReply<T> = Result<T, (&'static str, String)>;

fn plain(err: impl Into<PlainError>) -> (&'static str, String) {
    let err = err.into();
    (err.code, err.message)
}

fn error(code: ErrorCode, message: impl Into<String>) -> (&'static str, String) {
    plain(PlainError::new(code, message))
}

/// The connection options `Trellis.connect/1` has already validated and
/// defaulted. Every field is required here: the defaults are the Elixir
/// module's to document, and nothing falls back to the environment.
#[derive(NifMap)]
struct ConnectOptions {
    url: String,
    schema: String,
    target_schema: String,
    staging: bool,
    drain_threads: usize,
    worker_threads: usize,
}

/// One connected Trellis instance, owned by the BEAM as a resource.
///
/// The lock is read for every call and written only by `shutdown`, which
/// takes the [`BlockingTrellis`] out; calls from several BEAM processes run
/// concurrently, and a call after shutdown gets an error, not a hang.
struct Handle {
    trellis: RwLock<Option<BlockingTrellis>>,
    /// The OS process that connected. The BEAM never forks, so this can't
    /// trip today; it is ADR-0010 decision 3's pid guard, carried here as it
    /// will be in the Ruby binding, where a handle inherited across `fork`
    /// has no threads left to answer it and would otherwise hang.
    owner_pid: u32,
}

#[rustler::resource_impl]
impl rustler::Resource for Handle {}

impl Handle {
    /// Runs `call` against the live instance, or reports why there isn't one.
    fn with<T>(
        &self,
        call: impl FnOnce(&BlockingTrellis) -> Result<T, trellis::TrellisError>,
    ) -> NifReply<T> {
        self.check_pid()?;
        let guard = self.trellis.read().map_err(|_| poisoned())?;
        let trellis = guard.as_ref().ok_or_else(|| {
            error(
                ErrorCode::Validation,
                "this Trellis handle has been shut down",
            )
        })?;
        call(trellis).map_err(plain)
    }

    fn check_pid(&self) -> NifReply<()> {
        let pid = std::process::id();
        if pid == self.owner_pid {
            return Ok(());
        }
        Err(error(
            ErrorCode::Validation,
            format!(
                "this Trellis handle belongs to OS process {}, not {pid}: a handle does not \
                 survive fork, so connect again in this process",
                self.owner_pid
            ),
        ))
    }
}

fn poisoned() -> (&'static str, String) {
    error(
        ErrorCode::Internal,
        "a call on this Trellis handle panicked; connect a new handle",
    )
}

/// A registered definition, as `define/2` returns it.
#[derive(NifMap)]
struct DefinitionTerm {
    id: i64,
    target_table: String,
    source_table: String,
    source_version: i64,
    status: Atom,
    source_columns: HashMap<String, String>,
}

/// A definition's status, as `status/2` returns it.
#[derive(NifMap)]
struct StatusTerm {
    status: Atom,
    backfill_failure: Option<BackfillFailureTerm>,
}

#[derive(NifMap)]
struct BackfillFailureTerm {
    source_table: String,
    attempts: u32,
    last_error: String,
    next_attempt_at_micros: i64,
}

impl From<PlainBackfillFailure> for BackfillFailureTerm {
    fn from(failure: PlainBackfillFailure) -> Self {
        BackfillFailureTerm {
            source_table: failure.source_table,
            attempts: failure.attempts,
            last_error: failure.last_error,
            next_attempt_at_micros: failure.next_attempt_at_micros,
        }
    }
}

/// `word`'s atom. `word` is always a [`trellis::TransformStatus::as_str`]
/// word, so this only ever looks up an atom [`load`] already allocated.
fn status_atom(env: Env, word: &'static str) -> NifReply<Atom> {
    Atom::from_str(env, word).map_err(|_| {
        error(
            ErrorCode::Internal,
            format!("could not encode the status word {word:?} as an atom"),
        )
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn connect(options: ConnectOptions) -> NifReply<ResourceArc<Handle>> {
    let config = Config::with_schema(options.url, options.schema)
        .and_then(|config| config.with_target_schema(options.target_schema))
        .map_err(plain)?;
    let trellis = BlockingTrellis::connect(
        config,
        TrellisOptions {
            staging: options.staging,
            drain_threads: options.drain_threads,
            worker_threads: Some(options.worker_threads),
        },
    )
    .map_err(plain)?;
    Ok(ResourceArc::new(Handle {
        trellis: RwLock::new(Some(trellis)),
        owner_pid: std::process::id(),
    }))
}

#[rustler::nif(schedule = "DirtyIo")]
fn migrate(handle: ResourceArc<Handle>) -> NifReply<Atom> {
    handle.with(BlockingTrellis::migrate)?;
    Ok(rustler::types::atom::ok())
}

/// Registers the `TRANSFORM` statement `text`.
///
/// `BlockingTrellis::apply` takes every statement form, and nothing public
/// says which form `text` is before it is applied, so a different form still
/// takes effect and is then reported as a `validation` error that says so.
/// The other forms get their own functions in the binding's full surface
/// (#147).
#[rustler::nif(schedule = "DirtyIo")]
fn define(env: Env, handle: ResourceArc<Handle>, text: String) -> NifReply<DefinitionTerm> {
    let applied = handle.with(|trellis| trellis.apply(&text))?;
    let definition = applied.into_transform().ok_or_else(|| {
        error(
            ErrorCode::Validation,
            "define/2 takes a TRANSFORM statement; this statement is another form, and it was \
             applied but registered no transform",
        )
    })?;
    let definition = PlainDefinition::from(&definition);
    Ok(DefinitionTerm {
        id: definition.id,
        target_table: definition.target_table,
        source_table: definition.source_table,
        source_version: definition.source_version,
        status: status_atom(env, definition.status)?,
        source_columns: definition.source_columns.into_iter().collect(),
    })
}

/// `target_table`'s status, or `nil` when no definition writes it.
#[rustler::nif(schedule = "DirtyIo")]
fn status(
    env: Env,
    handle: ResourceArc<Handle>,
    target_table: String,
) -> NifReply<Option<StatusTerm>> {
    let Some(status) = handle.with(|trellis| trellis.status(&target_table))? else {
        return Ok(None);
    };
    let status = PlainDefinitionStatus::from(&status);
    Ok(Some(StatusTerm {
        status: status_atom(env, status.status)?,
        backfill_failure: status.backfill_failure.map(BackfillFailureTerm::from),
    }))
}

/// Stops the instance's background work and joins its runtime thread.
/// Idempotent: shutting down a handle that is already shut down is `:ok`.
#[rustler::nif(schedule = "DirtyIo")]
fn shutdown(handle: ResourceArc<Handle>) -> NifReply<Atom> {
    handle.check_pid()?;
    let trellis = handle.trellis.write().map_err(|_| poisoned())?.take();
    if let Some(trellis) = trellis {
        trellis.shutdown().map_err(plain)?;
    }
    Ok(rustler::types::atom::ok())
}

/// Every error code `trellis-embed` maps explicitly, for the Elixir test
/// asserting each one has its own atom.
#[rustler::nif(schedule = "DirtyIo")]
fn error_codes() -> NifReply<Vec<&'static str>> {
    Ok(ERROR_CODES.to_vec())
}

/// Every status atom `define/2` and `status/2` can return.
#[rustler::nif(schedule = "DirtyIo")]
fn status_names(env: Env) -> NifReply<Vec<Atom>> {
    transform_status_names()
        .into_iter()
        .map(|name| status_atom(env, name))
        .collect()
}

/// Allocates the status atoms up front, from the closed set `trellis`
/// defines, so encoding a status never creates an atom.
fn load(env: Env, _info: Term) -> bool {
    transform_status_names()
        .into_iter()
        .all(|name| Atom::from_str(env, name).is_ok())
}

rustler::init!("Elixir.Trellis.Native", load = load);
