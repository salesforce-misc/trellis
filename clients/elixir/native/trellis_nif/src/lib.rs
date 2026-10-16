//! The Elixir binding's NIF: a thin Rustler wrapper over
//! [`trellis::BlockingTrellis`] (`docs/decisions/0010-embeddable-clients.md`).
//!
//! Only `Trellis.Native` calls these; the public API, defaults and error
//! structs live in the Elixir modules under `lib/`. The contract here is
//! deliberately narrow:
//!
//! - **A call on a handle never holds a scheduler while it waits** (issue
//!   #599). The BEAM can't interrupt a NIF running on a dirty scheduler:
//!   killing its caller ends the process, but the NIF keeps the scheduler
//!   until it returns, so every call stuck on the database would hold one of
//!   the node's few dirty IO schedulers, which the VM's file IO also runs on.
//!   Instead, each handle NIF runs on a **normal** scheduler and only
//!   *submits* the call through its `BlockingTrellis::start_*` twin, which
//!   queues a job and returns at once. It returns `{:ok, call_ref}`, and the
//!   engine sends `{call_ref, reply}` to the calling process when the call
//!   ends, from a thread of this crate's ([`reply_runtime`]) that waits for
//!   every pending call at once, so no thread or scheduler is dedicated to a
//!   call. `reply` is `{:ok, value}` or `{:error, {code, message}}`, as below.
//!   `Trellis.Native.call/3` is the `receive` for it, with a backstop of its
//!   own. A reply to a process that has exited is dropped (`send_and_clear`
//!   reports the failure, which is ignored). A call refused before it was
//!   submitted (a handle that has been shut down, a malformed argument) is
//!   `{:error, {code, message}}` at once, with no message to wait for.
//! - **The NIFs that touch no handle are dirty IO**, the constant lookups
//!   included, so there is no rule to remember about which ones are "cheap".
//!   Plus `connect` and `shutdown`, the handle's lifecycle, which wait on a
//!   connection and on the runtime's threads. Three exceptions, none of which
//!   touches the database (issue #149):
//!   - `render_prometheus` runs on a **dirty CPU** scheduler. It never waits
//!     on IO, which is why ADR-0010 singles it out, but its cost isn't
//!     bounded either: it formats every series in the process-wide registry
//!     and folds in every histogram sample recorded since the last render.
//!     On a dirty CPU scheduler that can't stall a normal scheduler, and it
//!     doesn't queue behind the dirty IO schedulers.
//!   - `take_log_records` runs on a **normal** scheduler. It never waits (an
//!     empty queue is an empty list) and copies at most `max` records, which
//!     `Trellis.LogBridge` keeps small. It runs every 100 ms.
//!   - `config` runs on a **normal** scheduler. It reads the configuration
//!     the handle connected with, from memory.
//! - **Every result is `{:ok, value}` or `{:error, {code, message}}`**, whether
//!   a NIF returns it or sends it as a call's reply, where `code` is
//!   [`trellis_embed::PlainError::code`] as a binary. Elixir maps it to an
//!   atom from a closed set, with a fallback for a code it doesn't know. A
//!   call that a shutdown cancelled is a `validation` error, like a call made
//!   after the shutdown.
//! - **Only plain data crosses** (decision 4). The flattening itself is
//!   `trellis-embed`'s; this crate only turns its plain values into terms.
//!   Words become atoms here (statuses, quarantine states, relationship
//!   cardinalities, `apply` outcome kinds, `self_check` outcomes and
//!   divergence kinds, log levels), but only ever words from the
//!   closed sets `trellis-embed` lists, which [`load`] allocates up front,
//!   never a string read from the database.
//!
//! The handle is a [`ResourceArc`] over [`Handle`]. Dropping the last
//! reference (the BEAM garbage-collecting it) drops the [`BlockingTrellis`],
//! which closes its job channel and lets its runtime thread wind down without
//! blocking the scheduler that ran the destructor. That is only the backstop:
//! `shutdown/1` is the clean, joined stop, and it cancels the calls still in
//! flight rather than waiting for them.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use std::time::Duration;

use rustler::env::OwnedEnv;
use rustler::{Atom, Encoder, Env, NifMap, ResourceArc, Term};
use trellis::{BlockingTrellis, Config, ErrorCode, PendingCall, TrellisOptions};
use trellis_embed::{
    DIVERGENCE_KINDS, ERROR_CODES, LOG_LEVELS, PlainApplied, PlainBackfillFailure,
    PlainCaptureFailure, PlainCaptureWait, PlainConfig, PlainCorrectingNode, PlainDefinition,
    PlainDefinitionStatus, PlainDefinitionSummary, PlainDivergence, PlainDrainFailure, PlainError,
    PlainHeldKeys, PlainPoisonEntry, PlainQuarantineEntry, PlainRelationship,
    PlainRelationshipSummary, PlainSamplePage, PlainSelfCheckJob, PlainSelfCheckReport,
    PlainUnindexedJoin, SELF_CHECK_MODES, SELF_CHECK_OUTCOMES, SELF_CHECK_STATES,
    capture_failure_kind_names, decode_cursor, decode_watermark, encode_watermark,
    quarantine_state_names, relationship_cardinality_names, require_transform_statement,
    self_check_mode, system_time_from_epoch_micros, transform_status_names,
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
/// The lock is read for as long as it takes to submit a call, and written by
/// `shutdown`, which takes the [`BlockingTrellis`] out, so a call after
/// shutdown gets an error, not a hang. It is never held while a call runs, so
/// calls from several BEAM processes don't wait on each other for it, and
/// `shutdown` doesn't wait for calls in flight: the [`BlockingTrellis`] runs
/// them in parallel on its own runtime and cancels them when it shuts down.
struct Handle {
    trellis: RwLock<Option<BlockingTrellis>>,
    /// The OS process that connected. The BEAM never forks, so this can't
    /// trip today; it is ADR-0010 decision 3's pid guard, carried here as it
    /// is in the Ruby binding, where a handle inherited across `fork`
    /// has no threads left to answer it and would otherwise hang.
    owner_pid: u32,
}

#[rustler::resource_impl]
impl rustler::Resource for Handle {}

/// The thread that waits for every pending call and sends its reply: one for
/// the whole VM, because a call waiting for the engine is a task on it, not a
/// thread (issue #599). A reply is encoded and sent here, which is a copy of
/// a result that is plain data, so one thread keeps up.
static REPLY_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn reply_runtime() -> &'static tokio::runtime::Runtime {
    REPLY_RUNTIME
        .get()
        .expect("`load` starts the reply runtime before any NIF can run")
}

impl Handle {
    /// Submits the call `start` makes against the live instance and returns
    /// at once with `{:ok, call_ref}`, or reports why there isn't an instance
    /// to submit to. When the call ends, `encode` turns its value into the
    /// term the caller gets, and `{call_ref, {:ok, term} | {:error, {code,
    /// message}}}` is sent to the calling process (see the module docs).
    fn submit<'a, R, T>(
        &self,
        env: Env<'a>,
        start: impl FnOnce(&BlockingTrellis) -> PendingCall<R>,
        encode: impl for<'e> FnOnce(Env<'e>, R) -> NifReply<T> + Send + 'static,
    ) -> NifReply<Term<'a>>
    where
        R: Send + 'static,
        T: Encoder,
    {
        self.check_pid()?;
        let pending = {
            let guard = self.trellis.read().map_err(|_| poisoned())?;
            start(guard.as_ref().ok_or_else(shut_down)?)
        };

        let caller = env.pid();
        let call_ref = env.make_ref();
        let mut reply_env = OwnedEnv::new();
        let saved_ref = reply_env.save(call_ref);
        // The task holds only the call's reply, not the handle: a handle dropped
        // or shut down meanwhile still answers it (with the result, or the
        // cancellation). The encoders return errors rather than panic; one
        // that did would end the task unsent, and the caller would get
        // `Trellis.Native.call/3`'s backstop `timeout`.
        reply_runtime().spawn(async move {
            let result = pending.await;
            // A caller that has exited can't be sent to: its reply is dropped.
            let _ = reply_env.send_and_clear(&caller, |env| {
                let reply: NifReply<T> = result.map_err(plain).and_then(|value| encode(env, value));
                (saved_ref.load(env), reply)
            });
        });
        Ok(call_ref.encode(env))
    }

    /// Runs `read` against the live instance without waiting on anything, or
    /// reports why there isn't one.
    fn with<T>(&self, read: impl FnOnce(&BlockingTrellis) -> T) -> NifReply<T> {
        self.check_pid()?;
        let guard = self.trellis.read().map_err(|_| poisoned())?;
        Ok(read(guard.as_ref().ok_or_else(shut_down)?))
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

fn shut_down() -> (&'static str, String) {
    error(
        ErrorCode::Validation,
        "this Trellis handle has been shut down",
    )
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
    capture_wait: Option<CaptureWaitTerm>,
    capture_failure: Option<CaptureFailureTerm>,
    held_keys: Option<HeldKeysTerm>,
    drain_failure: Option<DrainFailureTerm>,
    unindexed_joins: Vec<UnindexedJoinTerm>,
    correcting: Vec<CorrectingNodeTerm>,
}

/// A node of a partitioned source a definition is being corrected for, as
/// `status/2` reports it.
#[derive(NifMap)]
struct CorrectingNodeTerm {
    node: String,
    since_micros: i64,
    keys_remaining: Option<u64>,
}

impl From<PlainCorrectingNode> for CorrectingNodeTerm {
    fn from(node: PlainCorrectingNode) -> Self {
        CorrectingNodeTerm {
            node: node.node,
            since_micros: node.since_micros,
            keys_remaining: node.keys_remaining,
        }
    }
}

/// A join column of a relationship a definition reads that has no usable
/// index, as `status/2` and `self_check/3` report it.
#[derive(NifMap)]
struct UnindexedJoinTerm {
    relationship: String,
    table: String,
    column: String,
    fix: String,
}

impl From<PlainUnindexedJoin> for UnindexedJoinTerm {
    fn from(join: PlainUnindexedJoin) -> Self {
        UnindexedJoinTerm {
            relationship: join.relationship,
            table: join.table,
            column: join.column,
            fix: join.fix,
        }
    }
}

/// A drain page that keeps failing with nothing charged or paused, as
/// `status/2` and `self_check/3` report it.
#[derive(NifMap)]
struct DrainFailureTerm {
    seg_seq: i64,
    tables: Vec<String>,
    error: String,
    sqlstate: Option<String>,
    since_micros: i64,
    last_seen_micros: i64,
    attempts: u32,
}

impl From<PlainDrainFailure> for DrainFailureTerm {
    fn from(failure: PlainDrainFailure) -> Self {
        DrainFailureTerm {
            seg_seq: failure.seg_seq,
            tables: failure.tables,
            error: failure.error,
            sqlstate: failure.sqlstate,
            since_micros: failure.since_micros,
            last_seen_micros: failure.last_seen_micros,
            attempts: failure.attempts,
        }
    }
}

/// How many keys a definition holds in quarantine, and since when, as
/// `status/2` and `self_check/3` report it.
#[derive(NifMap)]
struct HeldKeysTerm {
    count: i64,
    oldest_poisoned_at_micros: i64,
}

impl From<PlainHeldKeys> for HeldKeysTerm {
    fn from(held: PlainHeldKeys) -> Self {
        HeldKeysTerm {
            count: held.count,
            oldest_poisoned_at_micros: held.oldest_poisoned_at_micros,
        }
    }
}

#[derive(NifMap)]
struct CaptureWaitTerm {
    table: String,
    operation: String,
    lock_mode: String,
    waiting_since_micros: i64,
    observed_at_micros: i64,
    blockers: Vec<String>,
}

impl From<PlainCaptureWait> for CaptureWaitTerm {
    fn from(wait: PlainCaptureWait) -> Self {
        CaptureWaitTerm {
            table: wait.table,
            operation: wait.operation,
            lock_mode: wait.lock_mode,
            waiting_since_micros: wait.waiting_since_micros,
            observed_at_micros: wait.observed_at_micros,
            blockers: wait.blockers,
        }
    }
}

#[derive(NifMap)]
struct CaptureFailureTerm {
    kind: Atom,
    source_table: String,
    columns: Vec<String>,
    error: String,
    detected_at_micros: i64,
}

impl CaptureFailureTerm {
    fn new(env: Env, failure: PlainCaptureFailure) -> NifReply<Self> {
        Ok(CaptureFailureTerm {
            kind: word_atom(env, failure.kind)?,
            source_table: failure.source_table,
            columns: failure.columns,
            error: failure.error,
            detected_at_micros: failure.detected_at_micros,
        })
    }
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

/// `word`'s atom. `word` is always one of the `'static` words in
/// [`atom_words`], never text read from the database, so this only ever
/// looks up an atom [`load`] already allocated.
fn word_atom(env: Env, word: &'static str) -> NifReply<Atom> {
    Atom::from_str(env, word).map_err(|_| {
        error(
            ErrorCode::Internal,
            format!("could not encode the word {word:?} as an atom"),
        )
    })
}

fn word_atoms(env: Env, words: Vec<&'static str>) -> NifReply<Vec<Atom>> {
    words.into_iter().map(|word| word_atom(env, word)).collect()
}

/// Every word this NIF turns into an atom: the closed sets [`load`]
/// allocates.
fn atom_words() -> impl Iterator<Item = &'static str> {
    transform_status_names()
        .into_iter()
        .chain(quarantine_state_names())
        .chain(relationship_cardinality_names())
        .chain(PlainApplied::KINDS)
        .chain(SELF_CHECK_OUTCOMES)
        .chain(SELF_CHECK_STATES)
        .chain(SELF_CHECK_MODES)
        .chain(DIVERGENCE_KINDS)
        .chain(capture_failure_kind_names())
        .chain(LOG_LEVELS)
}

impl DefinitionTerm {
    fn new(env: Env, definition: PlainDefinition) -> NifReply<Self> {
        Ok(DefinitionTerm {
            id: definition.id,
            target_table: definition.target_table,
            source_table: definition.source_table,
            source_version: definition.source_version,
            status: word_atom(env, definition.status)?,
            source_columns: definition.source_columns.into_iter().collect(),
        })
    }
}

/// A registered definition, as `definitions/1` lists it.
#[derive(NifMap)]
struct DefinitionSummaryTerm {
    id: i64,
    target_table: String,
    source_table: String,
    source_version: i64,
    status: Atom,
    created_at_micros: i64,
    backfill_failure: Option<BackfillFailureTerm>,
    halt: Option<CaptureFailureTerm>,
}

/// A relationship `apply/2` registered.
#[derive(NifMap)]
struct RelationshipTerm {
    id: i64,
    name: String,
    from_schema: String,
    from_table: String,
    from_col: String,
    to_schema: String,
    to_table: String,
    to_col: String,
    cardinality: Atom,
    warnings: Vec<String>,
}

/// A registered relationship, as `relationships/1` lists it.
#[derive(NifMap)]
struct RelationshipSummaryTerm {
    id: i64,
    name: String,
    from_schema: String,
    from_table: String,
    from_col: String,
    to_schema: String,
    to_table: String,
    to_col: String,
    cardinality: Atom,
    created_at_micros: i64,
}

/// What `apply/2` did. `kind` is one of [`PlainApplied::KINDS`]; each
/// other field is set only for the kinds that carry it (`definition` for
/// `transform_defined` and `altered`, `relationship` for
/// `relationship_defined`, `columns` for `resumed`, and `added`, `dropped`
/// and `altered` for `altered`), and is `nil` otherwise. `Trellis.Applied`
/// turns this into the tagged value `apply/2` returns.
#[derive(NifMap)]
struct AppliedTerm {
    kind: Atom,
    definition: Option<DefinitionTerm>,
    relationship: Option<RelationshipTerm>,
    columns: Option<Vec<String>>,
    added: Option<Vec<String>>,
    dropped: Option<Vec<String>>,
    altered: Option<Vec<String>>,
}

/// One entry of `quarantined/1`, or `quarantine_status/2`'s one entry.
#[derive(NifMap)]
struct QuarantineEntryTerm {
    target: String,
    state: Atom,
    paused_at_micros: Option<i64>,
    last_error: Option<String>,
}

/// One poisoned key, as `poisoned_since/2` lists it.
#[derive(NifMap)]
struct PoisonEntryTerm {
    transform: String,
    src_table: String,
    key: String,
    last_error: String,
    poisoned_at_micros: i64,
}

#[derive(NifMap)]
struct PoisonSampleTerm {
    src_table: String,
    key: String,
    error_message: String,
}

/// One page of `sample_quarantined/3`.
#[derive(NifMap)]
struct SamplePageTerm {
    samples: Vec<PoisonSampleTerm>,
    next_cursor: Option<String>,
}

/// A `self_check/3` job. `mode` is one of [`SELF_CHECK_MODES`] and `state` one
/// of [`SELF_CHECK_STATES`]; `report` is set once it is `done`.
#[derive(NifMap)]
struct SelfCheckJobTerm {
    id: i64,
    target: String,
    mode: Atom,
    state: Atom,
    rows_compared: i64,
    report: Option<SelfCheckReportTerm>,
    error: Option<String>,
}

impl SelfCheckJobTerm {
    fn new(env: Env, job: PlainSelfCheckJob) -> NifReply<Self> {
        Ok(SelfCheckJobTerm {
            id: job.id,
            target: job.target,
            mode: word_atom(env, job.mode)?,
            state: word_atom(env, job.state)?,
            rows_compared: job.rows_compared,
            report: job
                .report
                .map(|report| SelfCheckReportTerm::new(env, report))
                .transpose()?,
            error: job.error,
        })
    }
}

/// What a finished `self_check/3` job found. `outcome` is one of
/// [`SELF_CHECK_OUTCOMES`]; `divergences` is empty unless it is `diverged`,
/// and `status` is set only when it is `not_live`.
#[derive(NifMap)]
struct SelfCheckReportTerm {
    target: String,
    checked_through: String,
    rows_compared: i64,
    truncated: bool,
    outcome: Atom,
    status: Option<Atom>,
    divergences: Vec<DivergenceTerm>,
    held_keys: Option<HeldKeysTerm>,
    drain_failures: Vec<DrainFailureTerm>,
}

/// One divergence. `kind` is one of [`DIVERGENCE_KINDS`]; see
/// [`PlainDivergence`] for which other fields each kind sets.
#[derive(NifMap)]
struct DivergenceTerm {
    kind: Atom,
    key: Option<String>,
    column: Option<String>,
    persisted: Option<String>,
    recomputed: Option<String>,
    table: Option<String>,
    detail: Option<String>,
}

impl SelfCheckReportTerm {
    fn new(env: Env, report: PlainSelfCheckReport) -> NifReply<Self> {
        Ok(SelfCheckReportTerm {
            target: report.target,
            checked_through: report.checked_through,
            rows_compared: report.rows_compared,
            truncated: report.truncated,
            outcome: word_atom(env, report.outcome)?,
            status: report
                .status
                .map(|status| word_atom(env, status))
                .transpose()?,
            divergences: report
                .divergences
                .into_iter()
                .map(|divergence| DivergenceTerm::new(env, divergence))
                .collect::<NifReply<_>>()?,
            held_keys: report.held_keys.map(HeldKeysTerm::from),
            drain_failures: report
                .drain_failures
                .into_iter()
                .map(DrainFailureTerm::from)
                .collect(),
        })
    }
}

impl DivergenceTerm {
    fn new(env: Env, divergence: PlainDivergence) -> NifReply<Self> {
        Ok(DivergenceTerm {
            kind: word_atom(env, divergence.kind)?,
            key: divergence.key,
            column: divergence.column,
            persisted: divergence.persisted,
            recomputed: divergence.recomputed,
            table: divergence.table,
            detail: divergence.detail,
        })
    }
}

impl RelationshipTerm {
    fn new(env: Env, relationship: PlainRelationship) -> NifReply<Self> {
        Ok(RelationshipTerm {
            id: relationship.id,
            name: relationship.name,
            from_schema: relationship.from_schema,
            from_table: relationship.from_table,
            from_col: relationship.from_col,
            to_schema: relationship.to_schema,
            to_table: relationship.to_table,
            to_col: relationship.to_col,
            cardinality: word_atom(env, relationship.cardinality)?,
            warnings: relationship.warnings,
        })
    }
}

impl AppliedTerm {
    fn new(env: Env, applied: PlainApplied) -> NifReply<Self> {
        let mut term = AppliedTerm {
            kind: word_atom(env, applied.kind())?,
            definition: None,
            relationship: None,
            columns: None,
            added: None,
            dropped: None,
            altered: None,
        };
        match applied {
            PlainApplied::TransformDefined(definition) => {
                term.definition = Some(DefinitionTerm::new(env, definition)?);
            }
            PlainApplied::RelationshipDefined(relationship) => {
                term.relationship = Some(RelationshipTerm::new(env, relationship)?);
            }
            PlainApplied::Resumed { columns } => term.columns = Some(columns),
            PlainApplied::Altered {
                definition,
                added,
                dropped,
                altered,
            } => {
                term.definition = Some(DefinitionTerm::new(env, definition)?);
                term.added = Some(added);
                term.dropped = Some(dropped);
                term.altered = Some(altered);
            }
            PlainApplied::Paused | PlainApplied::Dropped | PlainApplied::Unknown => {}
        }
        Ok(term)
    }
}

impl QuarantineEntryTerm {
    fn new(env: Env, entry: PlainQuarantineEntry) -> NifReply<Self> {
        Ok(QuarantineEntryTerm {
            target: entry.target,
            state: word_atom(env, entry.state)?,
            paused_at_micros: entry.paused_at_micros,
            last_error: entry.last_error,
        })
    }
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
            ..Default::default()
        },
    )
    .map_err(plain)?;
    Ok(ResourceArc::new(Handle {
        trellis: RwLock::new(Some(trellis)),
        owner_pid: std::process::id(),
    }))
}

/// Applies Trellis's own migrations. Replies `:ok`.
#[rustler::nif]
fn migrate<'a>(env: Env<'a>, handle: ResourceArc<Handle>) -> NifReply<Term<'a>> {
    handle.submit(env, BlockingTrellis::start_migrate, |_, ()| {
        Ok(rustler::types::atom::ok())
    })
}

/// Registers the `TRANSFORM` statement `text`.
///
/// `BlockingTrellis::apply` takes every statement form, so `text` is checked
/// first with `trellis::statement_kind`: any other form (`DROP`, `PAUSE`,
/// ...) is a `validation` error and is never applied, and text that doesn't
/// parse is the `parse` error `apply` would return. Every form, this one
/// included, goes through `apply/2`.
#[rustler::nif]
fn define<'a>(env: Env<'a>, handle: ResourceArc<Handle>, text: String) -> NifReply<Term<'a>> {
    require_transform_statement(&text).map_err(plain)?;
    handle.submit(
        env,
        |trellis| trellis.start_apply(&text),
        |env, applied| {
            // Unreachable: the check above is `apply`'s own parser. Kept as
            // defence, so a mistake there is an error rather than a panic.
            let definition = applied.into_transform().ok_or_else(|| {
                error(
                    ErrorCode::Internal,
                    "define/2 applied a statement that registered no transform",
                )
            })?;
            DefinitionTerm::new(env, PlainDefinition::from(&definition))
        },
    )
}

/// Runs one statement of Trellis's grammar, whatever its form, and reports
/// what it did.
#[rustler::nif]
fn apply<'a>(env: Env<'a>, handle: ResourceArc<Handle>, text: String) -> NifReply<Term<'a>> {
    handle.submit(
        env,
        |trellis| trellis.start_apply(&text),
        |env, applied| AppliedTerm::new(env, PlainApplied::from(&applied)),
    )
}

/// Every registered transform definition, oldest first.
#[rustler::nif]
fn definitions<'a>(env: Env<'a>, handle: ResourceArc<Handle>) -> NifReply<Term<'a>> {
    handle.submit(env, BlockingTrellis::start_definitions, |env, summaries| {
        summaries
            .iter()
            .map(|summary| {
                let summary = PlainDefinitionSummary::from(summary);
                Ok(DefinitionSummaryTerm {
                    id: summary.id,
                    target_table: summary.target_table,
                    source_table: summary.source_table,
                    source_version: summary.source_version,
                    status: word_atom(env, summary.status)?,
                    created_at_micros: summary.created_at_micros,
                    backfill_failure: summary.backfill_failure.map(BackfillFailureTerm::from),
                    halt: summary
                        .halt
                        .map(|halt| CaptureFailureTerm::new(env, halt))
                        .transpose()?,
                })
            })
            .collect::<NifReply<Vec<_>>>()
    })
}

/// The SQL that loads this instance's definitions into a database built from
/// a schema-only dump (`dump_definitions_sql`), as a binary.
#[rustler::nif]
fn dump_definitions_sql<'a>(env: Env<'a>, handle: ResourceArc<Handle>) -> NifReply<Term<'a>> {
    handle.submit(
        env,
        BlockingTrellis::start_dump_definitions_sql,
        |_, sql| Ok(sql),
    )
}

/// Every registered relationship, oldest first.
#[rustler::nif]
fn relationships<'a>(env: Env<'a>, handle: ResourceArc<Handle>) -> NifReply<Term<'a>> {
    handle.submit(
        env,
        BlockingTrellis::start_relationships,
        |env, summaries| {
            summaries
                .iter()
                .map(|summary| {
                    let summary = PlainRelationshipSummary::try_from(summary).map_err(plain)?;
                    Ok(RelationshipSummaryTerm {
                        id: summary.id,
                        name: summary.name,
                        from_schema: summary.from_schema,
                        from_table: summary.from_table,
                        from_col: summary.from_col,
                        to_schema: summary.to_schema,
                        to_table: summary.to_table,
                        to_col: summary.to_col,
                        cardinality: word_atom(env, summary.cardinality)?,
                        created_at_micros: summary.created_at_micros,
                    })
                })
                .collect::<NifReply<Vec<_>>>()
        },
    )
}

/// Re-reads `source_table` for its applying readers: a rebuild, or a go-live
/// catch-up for the staging worker.
#[rustler::nif]
fn request_backfill<'a>(
    env: Env<'a>,
    handle: ResourceArc<Handle>,
    source_table: String,
) -> NifReply<Term<'a>> {
    handle.submit(
        env,
        |trellis| trellis.start_request_backfill(&source_table),
        |_, ()| Ok(rustler::types::atom::ok()),
    )
}

/// Every key poisoned after `watermark_micros` (epoch microseconds), oldest
/// first.
#[rustler::nif]
fn poisoned_since<'a>(
    env: Env<'a>,
    handle: ResourceArc<Handle>,
    watermark_micros: i64,
) -> NifReply<Term<'a>> {
    let watermark = system_time_from_epoch_micros(watermark_micros).map_err(plain)?;
    handle.submit(
        env,
        |trellis| trellis.start_poisoned_since(watermark),
        |_, entries| {
            Ok(entries
                .iter()
                .map(|entry| {
                    let entry = PlainPoisonEntry::from(entry);
                    PoisonEntryTerm {
                        transform: entry.transform,
                        src_table: entry.src_table,
                        key: entry.key,
                        last_error: entry.last_error,
                        poisoned_at_micros: entry.poisoned_at_micros,
                    }
                })
                .collect::<Vec<_>>())
        },
    )
}

/// Every quarantined transform and paused column.
#[rustler::nif]
fn quarantined<'a>(env: Env<'a>, handle: ResourceArc<Handle>) -> NifReply<Term<'a>> {
    handle.submit(env, BlockingTrellis::start_quarantined, |env, entries| {
        entries
            .iter()
            .map(|entry| QuarantineEntryTerm::new(env, PlainQuarantineEntry::from(entry)))
            .collect::<NifReply<Vec<_>>>()
    })
}

/// One target's state, by its `transform` or `transform.column` address.
#[rustler::nif]
fn quarantine_status<'a>(
    env: Env<'a>,
    handle: ResourceArc<Handle>,
    target: String,
) -> NifReply<Term<'a>> {
    handle.submit(
        env,
        |trellis| trellis.start_quarantine_status(&target),
        |env, entry| QuarantineEntryTerm::new(env, PlainQuarantineEntry::from(&entry)),
    )
}

/// Up to `limit` of `target`'s quarantined rows, after the opaque `cursor`
/// (`nil` for the first page).
#[rustler::nif]
fn sample_quarantined<'a>(
    env: Env<'a>,
    handle: ResourceArc<Handle>,
    target: String,
    cursor: Option<String>,
    limit: i64,
) -> NifReply<Term<'a>> {
    let after = decode_cursor(cursor.as_deref()).map_err(plain)?;
    handle.submit(
        env,
        |trellis| trellis.start_sample_quarantined(&target, after, limit),
        move |_, samples| {
            let page = PlainSamplePage::new(&samples, cursor.as_deref());
            Ok(SamplePageTerm {
                samples: page
                    .samples
                    .into_iter()
                    .map(|sample| PoisonSampleTerm {
                        src_table: sample.src_table,
                        key: sample.key,
                        error_message: sample.error_message,
                    })
                    .collect(),
                next_cursor: page.next_cursor,
            })
        },
    )
}

/// Releases one key `transform` holds in quarantine: `source_table` (either
/// spelling) and `key` as `sample_quarantined/3` reports them.
#[rustler::nif]
fn release_key<'a>(
    env: Env<'a>,
    handle: ResourceArc<Handle>,
    transform: String,
    source_table: String,
    key: String,
) -> NifReply<Term<'a>> {
    handle.submit(
        env,
        |trellis| trellis.start_release_key(&transform, &source_table, &key),
        |_, ()| Ok(rustler::types::atom::ok()),
    )
}

/// Whether any drain worker in the fleet is alive.
#[rustler::nif]
fn has_live_drain_workers<'a>(env: Env<'a>, handle: ResourceArc<Handle>) -> NifReply<Term<'a>> {
    handle.submit(
        env,
        BlockingTrellis::start_has_live_drain_workers,
        |_, live| Ok(live),
    )
}

/// Whether this instance's staging worker is alive anywhere in the fleet.
#[rustler::nif]
fn has_live_staging_worker<'a>(env: Env<'a>, handle: ResourceArc<Handle>) -> NifReply<Term<'a>> {
    handle.submit(
        env,
        BlockingTrellis::start_has_live_staging_worker,
        |_, live| Ok(live),
    )
}

/// A read-your-writes token, as an opaque string.
#[rustler::nif]
fn watermark_token<'a>(env: Env<'a>, handle: ResourceArc<Handle>) -> NifReply<Term<'a>> {
    handle.submit(env, BlockingTrellis::start_watermark_token, |_, token| {
        Ok(encode_watermark(token))
    })
}

/// Waits up to `timeout_ms` for every change committed at or before `token`
/// to reach its targets. The engine caps the wait at the call's 30-second
/// deadline, so a longer one is cut short with a `timeout` error.
///
/// The wait is the engine's, not a scheduler's: the call replies when it ends,
/// and every other call on the handle runs beside it.
#[rustler::nif]
fn await_converged<'a>(
    env: Env<'a>,
    handle: ResourceArc<Handle>,
    token: String,
    timeout_ms: u64,
) -> NifReply<Term<'a>> {
    let token = decode_watermark(&token).map_err(plain)?;
    handle.submit(
        env,
        |trellis| trellis.start_await_converged(token, Duration::from_millis(timeout_ms)),
        |_, ()| Ok(rustler::types::atom::ok()),
    )
}

/// Starts a background check of `target_table` against a fresh recompute
/// from the source, in `mode` (`"standard"` or `"strict"`), waiting up to
/// `timeout_ms` per convergence await its pages make, and returns the job at
/// once. A target whose job is still running gets that job back.
#[rustler::nif]
fn self_check<'a>(
    env: Env<'a>,
    handle: ResourceArc<Handle>,
    target_table: String,
    mode: String,
    timeout_ms: u64,
) -> NifReply<Term<'a>> {
    let mode = self_check_mode(&mode).map_err(plain)?;
    handle.submit(
        env,
        |trellis| trellis.start_self_check(&target_table, mode, Duration::from_millis(timeout_ms)),
        |env, job| SelfCheckJobTerm::new(env, PlainSelfCheckJob::from(&job)),
    )
}

/// The `self_check/3` job `id`, or `nil` when there is none: a newer
/// `self_check` of its target replaced it, or its transform was dropped.
#[rustler::nif]
fn self_check_job<'a>(env: Env<'a>, handle: ResourceArc<Handle>, id: i64) -> NifReply<Term<'a>> {
    handle.submit(
        env,
        |trellis| trellis.start_self_check_job(id),
        |env, job| {
            job.map(|job| SelfCheckJobTerm::new(env, PlainSelfCheckJob::from(&job)))
                .transpose()
        },
    )
}

/// The configuration a handle connected with, as `config/1` returns it.
#[derive(NifMap)]
struct ConfigTerm {
    schema: String,
    target_schema: String,
    pool_max_size: u64,
    pool_wait_timeout_ms: u64,
}

/// The configuration the handle connected with. Read from memory, so it
/// returns the term itself, on a normal scheduler.
#[rustler::nif]
fn config(handle: ResourceArc<Handle>) -> NifReply<ConfigTerm> {
    let config = handle.with(|trellis| PlainConfig::from(trellis.config()))?;
    Ok(ConfigTerm {
        schema: config.schema,
        target_schema: config.target_schema,
        pool_max_size: config.pool_max_size,
        pool_wait_timeout_ms: config.pool_wait_timeout_ms,
    })
}

/// `target_table`'s status, or `nil` when no definition writes it.
#[rustler::nif]
fn status<'a>(
    env: Env<'a>,
    handle: ResourceArc<Handle>,
    target_table: String,
) -> NifReply<Term<'a>> {
    handle.submit(
        env,
        |trellis| trellis.start_status(&target_table),
        |env, status| {
            let Some(status) = status else {
                return Ok(None);
            };
            let status = PlainDefinitionStatus::from(&status);
            Ok(Some(StatusTerm {
                status: word_atom(env, status.status)?,
                backfill_failure: status.backfill_failure.map(BackfillFailureTerm::from),
                capture_wait: status.capture_wait.map(CaptureWaitTerm::from),
                capture_failure: status
                    .capture_failure
                    .map(|failure| CaptureFailureTerm::new(env, failure))
                    .transpose()?,
                held_keys: status.held_keys.map(HeldKeysTerm::from),
                drain_failure: status.drain_failure.map(DrainFailureTerm::from),
                unindexed_joins: status
                    .unindexed_joins
                    .into_iter()
                    .map(UnindexedJoinTerm::from)
                    .collect(),
                correcting: status
                    .correcting
                    .into_iter()
                    .map(CorrectingNodeTerm::from)
                    .collect(),
            }))
        },
    )
}

/// Stops the instance's background work and joins its runtime thread.
/// Idempotent: shutting down a handle that is already shut down is `:ok`.
///
/// Calls still in flight are cancelled, not waited for (see
/// `BlockingTrellis::shutdown`): each of their callers gets a `validation`
/// error. So a call stuck on a lock doesn't hold this up, and it holds the
/// dirty IO scheduler it runs on only for the runtime's teardown. The lock
/// is taken only for as long as a call takes to submit.
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
    word_atoms(env, transform_status_names())
}

/// Every state atom `quarantined/1` and `quarantine_status/2` can return.
#[rustler::nif(schedule = "DirtyIo", name = "quarantine_state_names")]
fn quarantine_state_names_nif(env: Env) -> NifReply<Vec<Atom>> {
    word_atoms(env, quarantine_state_names())
}

/// Every cardinality atom a relationship can carry.
#[rustler::nif(schedule = "DirtyIo")]
fn cardinality_names(env: Env) -> NifReply<Vec<Atom>> {
    word_atoms(env, relationship_cardinality_names())
}

/// Every outcome kind `apply/2`'s result can carry.
#[rustler::nif(schedule = "DirtyIo")]
fn applied_kinds(env: Env) -> NifReply<Vec<Atom>> {
    word_atoms(env, PlainApplied::KINDS.to_vec())
}

/// Every outcome atom `self_check/3`'s report can carry.
#[rustler::nif(schedule = "DirtyIo")]
fn self_check_outcomes(env: Env) -> NifReply<Vec<Atom>> {
    word_atoms(env, SELF_CHECK_OUTCOMES.to_vec())
}

/// Every mode atom a `self_check/3` job can carry.
#[rustler::nif(schedule = "DirtyIo")]
fn self_check_modes(env: Env) -> NifReply<Vec<Atom>> {
    word_atoms(env, SELF_CHECK_MODES.to_vec())
}

/// Every state atom a `self_check/3` job can carry.
#[rustler::nif(schedule = "DirtyIo")]
fn self_check_states(env: Env) -> NifReply<Vec<Atom>> {
    word_atoms(env, SELF_CHECK_STATES.to_vec())
}

/// Every divergence kind atom `self_check/3`'s report can carry.
#[rustler::nif(schedule = "DirtyIo")]
fn divergence_kinds(env: Env) -> NifReply<Vec<Atom>> {
    word_atoms(env, DIVERGENCE_KINDS.to_vec())
}

/// Every kind atom a capture failure can carry.
#[rustler::nif(schedule = "DirtyIo")]
fn capture_failure_kinds(env: Env) -> NifReply<Vec<Atom>> {
    word_atoms(env, capture_failure_kind_names())
}

/// Every level atom `take_log_records/1` can return.
#[rustler::nif(schedule = "DirtyIo")]
fn log_levels(env: Env) -> NifReply<Vec<Atom>> {
    word_atoms(env, LOG_LEVELS.to_vec())
}

/// Every statement kind `trellis`'s grammar has, for the Elixir test
/// asserting `apply/2`'s round trip covers each one.
#[rustler::nif(schedule = "DirtyIo")]
fn statement_kinds() -> NifReply<Vec<&'static str>> {
    Ok(trellis::StatementKind::ALL
        .iter()
        .map(|kind| kind.as_str())
        .collect())
}

/// The process-wide metrics registry in Prometheus text exposition format:
/// `trellis::Metrics::render_prometheus`, the registry every handle in this
/// OS process records into. On a dirty CPU scheduler: see the module docs.
#[rustler::nif(schedule = "DirtyCpu")]
fn render_prometheus() -> NifReply<String> {
    Ok(trellis::Metrics::new().render_prometheus())
}

/// Installs `trellis-embed`'s log bridge as the global `tracing` subscriber
/// of this library's own copy of `tracing`, if no call has yet, and sets the most verbose level
/// it forwards (one of `trellis_embed::LOG_LEVEL_FILTERS`). A `conflict`
/// error means another subscriber was installed first.
#[rustler::nif(schedule = "DirtyIo")]
fn install_log_bridge(level: String) -> NifReply<Atom> {
    let bridge = trellis_embed::install_log_bridge().map_err(plain)?;
    bridge.set_max_level(&level).map_err(plain)?;
    Ok(rustler::types::atom::ok())
}

/// One log record, as `{level, target, message}`.
type LogRecordTerm = (Atom, String, String);

/// Up to `max` queued log records as `{level, target, message}`, oldest
/// first, and how many were dropped since the last call. Empty when the
/// bridge isn't installed. On a normal scheduler: see the module docs.
#[rustler::nif]
fn take_log_records(env: Env, max: usize) -> NifReply<(Vec<LogRecordTerm>, u64)> {
    let Some(bridge) = trellis_embed::installed_log_bridge() else {
        return Ok((Vec::new(), 0));
    };
    let batch = bridge.take(max);
    let records = batch
        .records
        .into_iter()
        .map(|record| Ok((word_atom(env, record.level)?, record.target, record.message)))
        .collect::<NifReply<_>>()?;
    Ok((records, batch.dropped))
}

/// Allocates every atom this NIF encodes up front, from the closed sets
/// `trellis-embed` lists, so encoding a result never creates an atom.
///
/// Also starts [`REPLY_RUNTIME`], so a NIF never has to build it (and so a
/// VM that can't start the thread fails to load the library, not a call).
fn load(env: Env, _info: Term) -> bool {
    if !atom_words().all(|word| Atom::from_str(env, word).is_ok()) {
        return false;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("trellis-nif-reply")
        .build();
    match runtime {
        Ok(runtime) => REPLY_RUNTIME.set(runtime).is_ok() || REPLY_RUNTIME.get().is_some(),
        Err(_) => false,
    }
}

rustler::init!("Elixir.Trellis.Native", load = load);
