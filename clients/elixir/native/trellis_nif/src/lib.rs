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
//!   Words become atoms here (statuses, quarantine states, relationship
//!   cardinalities, `apply` outcome kinds), but only ever words from the
//!   closed sets `trellis-embed` lists, which [`load`] allocates up front,
//!   never a string read from the database.
//!
//! The handle is a [`ResourceArc`] over [`Handle`]. Dropping the last
//! reference (the BEAM garbage-collecting it) drops the [`BlockingTrellis`],
//! which closes its job channel and lets its runtime thread wind down without
//! blocking the scheduler that ran the destructor. That is only the backstop:
//! `shutdown/1` is the clean, joined stop.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use rustler::{Atom, Env, NifMap, ResourceArc, Term};
use trellis::{BlockingTrellis, Config, ErrorCode, TrellisOptions};
use trellis_embed::{
    ERROR_CODES, PlainApplied, PlainBackfillFailure, PlainDefinition, PlainDefinitionStatus,
    PlainDefinitionSummary, PlainError, PlainPoisonEntry, PlainQuarantineEntry, PlainRelationship,
    PlainRelationshipSummary, PlainSamplePage, decode_cursor, decode_watermark, encode_watermark,
    quarantine_state_names, relationship_cardinality_names, require_transform_statement,
    system_time_from_epoch_micros, transform_status_names,
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
/// takes the [`BlockingTrellis`] out, so a call after shutdown gets an error,
/// not a hang. Calls from several BEAM processes don't wait on each other for
/// the lock, but the [`BlockingTrellis`] runs them one at a time on its own
/// thread, so each waiting call holds a dirty IO scheduler until its turn.
/// `shutdown` waits for calls already in flight before it takes the instance.
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
/// `BlockingTrellis::apply` takes every statement form, so `text` is checked
/// first with `trellis::statement_kind`: any other form (`DROP`, `PAUSE`,
/// ...) is a `validation` error and is never applied, and text that doesn't
/// parse is the `parse` error `apply` would return. Every form, this one
/// included, goes through `apply/2`.
#[rustler::nif(schedule = "DirtyIo")]
fn define(env: Env, handle: ResourceArc<Handle>, text: String) -> NifReply<DefinitionTerm> {
    require_transform_statement(&text).map_err(plain)?;
    let applied = handle.with(|trellis| trellis.apply(&text))?;
    // Unreachable: the check above is `apply`'s own parser. Kept as defence,
    // so a mistake there is an error rather than a panic.
    let definition = applied.into_transform().ok_or_else(|| {
        error(
            ErrorCode::Internal,
            "define/2 applied a statement that registered no transform",
        )
    })?;
    DefinitionTerm::new(env, PlainDefinition::from(&definition))
}

/// Runs one statement of Trellis's grammar, whatever its form, and reports
/// what it did.
#[rustler::nif(schedule = "DirtyIo")]
fn apply(env: Env, handle: ResourceArc<Handle>, text: String) -> NifReply<AppliedTerm> {
    let applied = handle.with(|trellis| trellis.apply(&text))?;
    AppliedTerm::new(env, PlainApplied::from(&applied))
}

/// Every registered transform definition, oldest first.
#[rustler::nif(schedule = "DirtyIo")]
fn definitions(env: Env, handle: ResourceArc<Handle>) -> NifReply<Vec<DefinitionSummaryTerm>> {
    handle
        .with(BlockingTrellis::definitions)?
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
            })
        })
        .collect()
}

/// Every registered relationship, oldest first.
#[rustler::nif(schedule = "DirtyIo")]
fn relationships(env: Env, handle: ResourceArc<Handle>) -> NifReply<Vec<RelationshipSummaryTerm>> {
    handle
        .with(BlockingTrellis::relationships)?
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
        .collect()
}

/// Parks a go-live catch-up re-read of `source_table` for the staging worker.
#[rustler::nif(schedule = "DirtyIo")]
fn request_backfill(handle: ResourceArc<Handle>, source_table: String) -> NifReply<Atom> {
    handle.with(|trellis| trellis.request_backfill(&source_table))?;
    Ok(rustler::types::atom::ok())
}

/// Every key poisoned after `watermark_micros` (epoch microseconds), oldest
/// first.
#[rustler::nif(schedule = "DirtyIo")]
fn poisoned_since(
    handle: ResourceArc<Handle>,
    watermark_micros: i64,
) -> NifReply<Vec<PoisonEntryTerm>> {
    let watermark = system_time_from_epoch_micros(watermark_micros).map_err(plain)?;
    Ok(handle
        .with(|trellis| trellis.poisoned_since(watermark))?
        .iter()
        .map(|entry| {
            let entry = PlainPoisonEntry::from(entry);
            PoisonEntryTerm {
                src_table: entry.src_table,
                key: entry.key,
                last_error: entry.last_error,
                poisoned_at_micros: entry.poisoned_at_micros,
            }
        })
        .collect())
}

/// Every quarantined transform and paused column.
#[rustler::nif(schedule = "DirtyIo")]
fn quarantined(env: Env, handle: ResourceArc<Handle>) -> NifReply<Vec<QuarantineEntryTerm>> {
    handle
        .with(BlockingTrellis::quarantined)?
        .iter()
        .map(|entry| QuarantineEntryTerm::new(env, PlainQuarantineEntry::from(entry)))
        .collect()
}

/// One target's state, by its `transform` or `transform.column` address.
#[rustler::nif(schedule = "DirtyIo")]
fn quarantine_status(
    env: Env,
    handle: ResourceArc<Handle>,
    target: String,
) -> NifReply<QuarantineEntryTerm> {
    let entry = handle.with(|trellis| trellis.quarantine_status(&target))?;
    QuarantineEntryTerm::new(env, PlainQuarantineEntry::from(&entry))
}

/// Up to `limit` of `target`'s quarantined rows, after the opaque `cursor`
/// (`nil` for the first page).
#[rustler::nif(schedule = "DirtyIo")]
fn sample_quarantined(
    handle: ResourceArc<Handle>,
    target: String,
    cursor: Option<String>,
    limit: i64,
) -> NifReply<SamplePageTerm> {
    let after = decode_cursor(cursor.as_deref()).map_err(plain)?;
    let samples = handle.with(|trellis| trellis.sample_quarantined(&target, after, limit))?;
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
}

/// Whether any drain worker in the fleet is alive.
#[rustler::nif(schedule = "DirtyIo")]
fn has_live_drain_workers(handle: ResourceArc<Handle>) -> NifReply<bool> {
    handle.with(BlockingTrellis::has_live_drain_workers)
}

/// Whether this instance's staging worker is alive anywhere in the fleet.
#[rustler::nif(schedule = "DirtyIo")]
fn has_live_staging_worker(handle: ResourceArc<Handle>) -> NifReply<bool> {
    handle.with(BlockingTrellis::has_live_staging_worker)
}

/// A read-your-writes token, as an opaque string.
#[rustler::nif(schedule = "DirtyIo")]
fn watermark_token(handle: ResourceArc<Handle>) -> NifReply<String> {
    handle
        .with(BlockingTrellis::watermark_token)
        .map(encode_watermark)
}

/// Waits up to `timeout_ms` for every change committed at or before `token`
/// to reach its targets.
///
/// Holds a dirty IO scheduler for as long as it waits, and the handle runs
/// one call at a time, so every other call on it queues behind this one.
#[rustler::nif(schedule = "DirtyIo")]
fn await_converged(handle: ResourceArc<Handle>, token: String, timeout_ms: u64) -> NifReply<Atom> {
    let token = decode_watermark(&token).map_err(plain)?;
    handle.with(|trellis| trellis.await_converged(token, Duration::from_millis(timeout_ms)))?;
    Ok(rustler::types::atom::ok())
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
        status: word_atom(env, status.status)?,
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

/// Every statement kind `trellis`'s grammar has, for the Elixir test
/// asserting `apply/2`'s round trip covers each one.
#[rustler::nif(schedule = "DirtyIo")]
fn statement_kinds() -> NifReply<Vec<&'static str>> {
    Ok(trellis::StatementKind::ALL
        .iter()
        .map(|kind| kind.as_str())
        .collect())
}

/// Allocates every atom this NIF encodes up front, from the closed sets
/// `trellis-embed` lists, so encoding a result never creates an atom.
fn load(env: Env, _info: Term) -> bool {
    atom_words().all(|word| Atom::from_str(env, word).is_ok())
}

rustler::init!("Elixir.Trellis.Native", load = load);
