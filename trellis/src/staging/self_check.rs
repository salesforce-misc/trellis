//! `self_check`: a production recompute audit (issue #174, ADR-0013).
//!
//! Trellis's one hard correctness promise is byte-identical convergence to a
//! from-scratch recompute at any caught-up LSN. The failure mode this module
//! guards against is a *silently stale target*: a wrong answer with no error
//! raised and no metric out of range, invisible without an independent
//! recompute. [`self_check`] makes that recompute a shipped,
//! operator-callable capability instead of something only the test suite can
//! perform — see [`crate::app::Trellis::self_check`] for the public facade.
//!
//! # Postgres is the oracle; the comparison is two-way
//!
//! [`self_check`] compares the persisted target against an equivalent
//! recompute *query* executed by Postgres. It does not run the engine's Rust
//! evaluator ([`crate::defs::eval::evaluate`]) as part of the comparison —
//! re-running the evaluator would check the engine against itself.
//!
//! # The leaf renderer is independent of the write path
//!
//! [`render_leaf`] below renders a calculated-field expression to SQL text
//! from scratch. It deliberately does **not** call
//! [`crate::defs::oracle::render_expr_sql`] or any of its siblings in
//! `defs::oracle` — those are used by the *production write path*
//! (`defs::backfill`'s direct-build backfill, and
//! `staging::ledger`'s incremental aggregate apply), not just by
//! tests. Auditing backfilled rows with a renderer the backfill path itself
//! uses would check the write path against itself: a rendering bug would
//! agree with itself, and the audit would pass on a wrong answer. This
//! renderer shares no code with `defs::oracle`'s, or with
//! `generative::oracle`'s own independent SQL renderer — the generative
//! suite's cross-check test (`generative/tests/self_check_cross_check.rs`)
//! is what keeps the two from silently drifting apart.
//!
//! # The capture audit comes first
//!
//! Before it waits or compares anything, [`self_check`] checks from the
//! catalog that every table the target is computed from is still captured
//! as `capture::install` installed it: all five triggers present, `ENABLE
//! ALWAYS` and calling their functions, the functions `SECURITY DEFINER` and
//! owned by the Trellis role, that role still holding the privileges the
//! functions use, and the table still outside any partition or inheritance
//! hierarchy ([`super::capture_audit`], #622 C9). It also checks that no
//! such table's row-level security applies to the Trellis role or the
//! caller's (#745), which would filter the recompute's reads as well as the
//! engine's, that the target's doesn't apply to the caller's, standing in
//! for the workers' that write it (#765), and that no logical-replication
//! subscription replicates into a table it is computed from (#751), whose
//! changes capture never sees. A fault there is reported as [`Divergence::Capture`], without a
//! recompute comparison: the convergence wait can't see a broken capture (it
//! is a predicate over ring rows, and the broken capture writes none), and
//! the comparison would only show its symptom, or agree with it.
//!
//! # Quiescence: "diverged" vs "not yet caught up"
//!
//! A single snapshot is not sufficient under live load: a correctly-working
//! target legitimately lags its source by CDC apply latency. [`self_check`]
//! takes a watermark token, awaits convergence through it (bounded by a
//! timeout — on timeout it reports [`SelfCheckOutcome::NotCaughtUp`], never a
//! divergence; any other failure of the wait is an `Err`, see [`caught_up`]),
//! then reads the target and runs the recompute in a single statement, so
//! under one snapshot ([`compare_once`]).
//!
//! Under [`SelfCheckMode::Standard`], a divergence is only reported as real
//! if it survives a re-check after a *fresh* await — a genuine divergence is
//! stable; a convergence race resolves. [`SelfCheckMode::Strict`] skips the
//! re-check: it is sound only when the caller has already stopped writes to
//! the audited tables (so there is no race left to resolve), and is what a
//! caller that can quiesce (e.g. the generative suite, after
//! `ManualBackend::quiesce`) should reach for.
//!
//! # Scope: 1-1 targets only, this issue
//!
//! Aggregate and relationship-enriched targets are out of scope for this
//! slice ([`SelfCheckError::UnsupportedKeySpace`] /
//! [`SelfCheckError::UnsupportedExpr`]) — extending [`render_leaf`] to
//! relationship paths, and [`compare_once`] to bound a group-key space
//! instead of a plain keyset, is the natural next step, not a rewrite (see
//! ADR-0013, "Scope: 1-1 first, then aggregates and relationships").
//!
//! # Bounded, keyset-scoped, mandatory
//!
//! There is no unbounded "check everything" convenience method here:
//! [`SelfCheckScope`] always bounds one call to a keyset page. A fleet-wide
//! sweep is a caller-side loop over [`crate::app::Trellis::definitions`] and
//! repeated [`self_check`] calls chained by [`SelfCheckReport::next_after`],
//! not a behaviour of this primitive itself.
//!
//! # Column-level quarantine
//!
//! A paused column ([`crate::staging::quarantine`]) holds a deliberately
//! stale value, so comparing it would report a false divergence on exactly
//! the targets an operator is most likely to be inspecting — [`self_check`]
//! excludes any currently-paused column of the audited transform from the
//! comparison entirely (see [`paused_columns`]).
//!
//! # Held keys
//!
//! A key the audited definition holds in quarantine
//! ([`crate::staging::quarantine`]) has a target row the definition stopped
//! writing, and a key with changes parked holds back the convergence wait.
//! Every report names how many keys the definition holds and since when
//! ([`SelfCheckReport::held_keys`]), read once the audit is done, so a
//! `Converged` page that never reached one, or a `NotCaughtUp` it causes,
//! can't hide it.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::time::Duration;

use tokio_postgres::types::PgLsn;

use crate::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate};
use crate::defs::catalog::{self, CatalogError};
use crate::defs::ddl::{self, DdlError, PrimaryKeyColumn};
use crate::defs::model::Definition;
use crate::defs::typed_literal;
use crate::error_code::{self, ErrorCode};
use crate::pool::{Pool, quote_ident};

use super::capture_audit::{self, CaptureFault};
use super::converge;
use super::error::StagingError;
use super::quarantine::{self, HeldKeys};

/// One page's worth of bound for a [`self_check`] call — see the module doc
/// comment's "Bounded, keyset-scoped, mandatory" section. `after` is a
/// keyset cursor (the last key seen by a previous call's
/// [`SelfCheckReport::next_after`]; `None` starts from the beginning of the
/// target's keyspace), and `limit` caps how many distinct keys this one call
/// examines. There is deliberately no `Default` impl that would let a caller
/// construct an effectively-unbounded scope by omission — every field must
/// be named explicitly at the call site.
#[derive(Debug, Clone)]
pub struct SelfCheckScope {
    pub after: Option<String>,
    pub limit: i64,
}

/// Standard (default) vs. strict auditing — see the module doc comment's
/// "Quiescence" section.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfCheckMode {
    /// A divergence is provisional until it survives a re-check after a
    /// fresh await — sound under live load, at the cost of up to double the
    /// work when something actually diverges.
    ///
    /// **Known, accepted limitation: the re-check's guarantee is identity
    /// stability, not value non-transience.** [`await_then_compare`] takes
    /// its watermark token and runs [`converge::await_converged`] on one
    /// connection borrowed from the pool, then [`compare_once`] runs its
    /// statement on a separate `pool.get()` call — which
    /// may hand back a *different* connection. A commit landing in that gap
    /// is visible to the source read but not yet applied to the target. Two
    /// independent passes can each catch this same lag race on the same hot
    /// `(key, column)`, with a different transient value each time, and
    /// because divergences are matched by identity (key/column) across
    /// passes rather than by exact value (see [`DivergenceIdentity`]), such
    /// a case is reported as a stable, real divergence even though it is
    /// still just lag.
    ///
    /// This is a deliberate tradeoff, not a bug to fix by changing the
    /// matching logic: matching by exact value instead would trade this
    /// narrow false-positive risk for false *negatives* — a genuinely-broken
    /// column that happens to change value on every pass under active writes
    /// would then be silently suppressed, which is worse for an audit tool.
    /// The window is also intrinsic to the whole re-check approach — you
    /// cannot await convergence for a snapshot you have already pinned — so
    /// there is no narrower fix available within this design.
    ///
    /// In short: a [`SelfCheckOutcome::Diverged`] under `Standard` means
    /// "this divergence's identity was stable across two passes," not "this
    /// divergence's value is guaranteed non-transient." Callers needing the
    /// stronger guarantee should quiesce writes themselves and use
    /// [`SelfCheckMode::Strict`], which skips the re-check (and therefore
    /// this window) entirely.
    Standard,
    /// Skips the re-check: a divergence found on the first pass is reported
    /// immediately. **Sound only when the caller has already stopped writes
    /// to the audited tables** — there is no convergence race left to
    /// resolve, so re-checking would only cost time, not change the answer.
    Strict,
}

/// One [`self_check`] call's result.
#[derive(Debug, Clone)]
pub struct SelfCheckReport {
    /// The audited target's bare table name (as passed to [`self_check`]).
    pub target: String,
    /// The watermark token this report's [`SelfCheckOutcome`] was checked
    /// through — the LSN [`converge::await_converged`] confirmed the target
    /// had caught up to (or failed to, for [`SelfCheckOutcome::NotCaughtUp`]).
    /// For a report of [`Divergence::Capture`] faults, which is made without
    /// awaiting anything, it is the WAL position when the capture audit read
    /// the catalog.
    pub checked_through: PgLsn,
    /// How many distinct keys this call actually compared — the union of
    /// every key seen on either side of the comparison, within `scope`'s
    /// page. Zero for a report whose outcome is
    /// [`SelfCheckOutcome::NotCaughtUp`], or that reports
    /// [`Divergence::Capture`] faults (the comparison never ran).
    pub rows_compared: i64,
    /// The keyset cursor a following call should pass as
    /// [`SelfCheckScope::after`] to continue past this page — the key this
    /// page's bound actually ended at, which is the *lower* of the two
    /// sides' last keys, in the order both sides are paged in (a
    /// single-column text key's own collation when it's deterministic, else
    /// `"C"`, see `page_collation`), whenever either side hit
    /// [`SelfCheckScope::limit`] (keys past it fell off one side's page and
    /// are deliberately left for the next call rather than diffed against a
    /// truncated counterpart). `None` means neither side hit the limit, so
    /// this page reached the end of the target's keyspace and a following
    /// call would have nothing to do.
    pub next_after: Option<String>,
    pub outcome: SelfCheckOutcome,
    /// The keys the audited definition holds in quarantine when the audit
    /// ends, `None` if it holds none (#759). Reported with every outcome,
    /// because a held key is one the audit can't vouch for: the definition
    /// leaves its changes out, so its target row stays as it was when the
    /// key was poisoned. It is a page's [`SelfCheckOutcome::Diverged`] row
    /// once its source row changes, if the page reaches it, and a key with
    /// changes parked holds back convergence, so the audit reports
    /// [`SelfCheckOutcome::NotCaughtUp`] until it is released. Sample the
    /// keys with `Trellis::sample_quarantined` and release each with
    /// `Trellis::release_key`, or resume the definition.
    pub held_keys: Option<HeldKeys>,
}

/// What [`self_check`] found — see the module doc comment's "Quiescence"
/// section for why [`SelfCheckOutcome::NotCaughtUp`] is a distinct outcome
/// from [`SelfCheckOutcome::Diverged`] rather than folded into it.
#[derive(Debug, Clone)]
pub enum SelfCheckOutcome {
    /// No divergence — every comparable cell, row, and column matched (under
    /// [`SelfCheckMode::Standard`], this also covers a divergence that
    /// resolved on re-check: a convergence race, not a real bug).
    Converged,
    /// The target never caught up to the watermark token within the caller's
    /// timeout budget. **Not a verdict on correctness** — a merely-lagging
    /// target reports this, never [`SelfCheckOutcome::Diverged`].
    NotCaughtUp,
    /// A stable divergence — present, and (under [`SelfCheckMode::Standard`])
    /// still present after a fresh re-check.
    Diverged(Vec<Divergence>),
}

/// One divergence [`self_check`] found: ADR-0013's four kinds, plus a
/// broken capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Divergence {
    /// A row exists on both sides, but `column`'s value differs.
    Cell {
        key: String,
        column: String,
        /// The value currently persisted in the target table (`::text`).
        persisted: Option<String>,
        /// The value the independent recompute produced (`::text`).
        recomputed: Option<String>,
    },
    /// The recompute produced this key, but the target has no row for it.
    MissingRow { key: String },
    /// The target has a row for this key, but the recompute didn't produce
    /// one.
    ExtraRow { key: String },
    /// The definition expects this column (its primary key, or one of its
    /// calculated fields), but the target table doesn't actually have it —
    /// schema drift, not a per-row divergence.
    MissingColumn { column: String },
    /// The target table has this column, but the definition doesn't expect
    /// it — schema drift, not a per-row divergence.
    ExtraColumn { column: String },
    /// A table the target is computed from isn't being captured as
    /// installed: a trigger is missing, disabled or calls the wrong
    /// function, a capture function is missing or has the wrong owner, the
    /// role the functions run as lost a privilege, the table joined a
    /// partition or inheritance hierarchy (#622 C9, see
    /// [`super::capture_audit`]), its row-level security applies to the
    /// Trellis role (#745), which would filter its reads, this audit's too
    /// (or the target's does, which would filter its writes, #765),
    /// or a logical-replication subscription replicates into it (#751). Its
    /// changes may not be reaching the target at all, so [`self_check`] reports these before, and instead
    /// of, a recompute comparison.
    Capture(CaptureFault),
}

/// Why a [`self_check`] call failed outright (as opposed to reporting a
/// [`SelfCheckOutcome::Diverged`], which is a successful audit that found a
/// problem, not a failure of the audit itself).
#[derive(Debug)]
pub enum SelfCheckError {
    /// No transform is registered under this target table name.
    TargetNotFound(String),
    /// [`self_check`] only audits a [`KeySpace::OneToOne`] target this issue
    /// — see the module doc comment's "Scope" section.
    UnsupportedKeySpace {
        target: String,
    },
    /// [`SelfCheckScope::limit`] wasn't a positive row count. Refused rather
    /// than run: a zero/negative limit compares nothing at all, and would
    /// otherwise report a cheerful `Converged` over an empty page — an audit
    /// that silently checks nothing is worse than one that declines.
    InvalidScope {
        limit: i64,
    },
    /// The audited definition's fields reference a construct [`render_leaf`]
    /// doesn't render yet (a relationship path) — see the module doc
    /// comment's "Scope" section.
    UnsupportedExpr {
        target: String,
        detail: String,
    },
    Ddl(DdlError),
    Catalog(CatalogError),
    Staging(StagingError),
    Db(tokio_postgres::Error),
    Pool(crate::error::Error),
}

impl SelfCheckError {
    /// This error's stable, coarse [`ErrorCode`] category
    /// (`docs/decisions/0008-public-api-design.md`, decision 3) — delegates
    /// to the wrapped error's own `code()` wherever one nests here, matching
    /// [`crate::app::TrellisError::code`]'s own composition-through-nesting
    /// convention.
    pub fn code(&self) -> ErrorCode {
        match self {
            SelfCheckError::TargetNotFound(_) => ErrorCode::NotFound,
            SelfCheckError::UnsupportedKeySpace { .. }
            | SelfCheckError::UnsupportedExpr { .. }
            | SelfCheckError::InvalidScope { .. } => ErrorCode::Validation,
            SelfCheckError::Ddl(err) => err.code(),
            SelfCheckError::Catalog(err) => err.code(),
            SelfCheckError::Staging(err) => err.code(),
            SelfCheckError::Db(err) => error_code::classify_pg_error(err),
            SelfCheckError::Pool(err) => err.code(),
        }
    }
}

impl fmt::Display for SelfCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelfCheckError::TargetNotFound(target) => {
                write!(f, "no transform named \"{target}\" is registered")
            }
            SelfCheckError::UnsupportedKeySpace { target } => write!(
                f,
                "self_check only audits a 1-1 target this issue; \"{target}\" is an aggregate \
                 target"
            ),
            SelfCheckError::UnsupportedExpr { target, detail } => {
                write!(f, "self_check can't audit \"{target}\": {detail}")
            }
            SelfCheckError::InvalidScope { limit } => write!(
                f,
                "self_check needs a positive SelfCheckScope::limit; got {limit}"
            ),
            SelfCheckError::Ddl(err) => write!(f, "{err}"),
            SelfCheckError::Catalog(err) => write!(f, "{err}"),
            SelfCheckError::Staging(err) => write!(f, "{err}"),
            SelfCheckError::Db(err) => {
                write!(f, "self_check database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            SelfCheckError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
        }
    }
}

impl std::error::Error for SelfCheckError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SelfCheckError::Ddl(err) => Some(err),
            SelfCheckError::Catalog(err) => Some(err),
            SelfCheckError::Staging(err) => Some(err),
            SelfCheckError::Db(err) => Some(err),
            SelfCheckError::Pool(err) => Some(err),
            SelfCheckError::TargetNotFound(_)
            | SelfCheckError::UnsupportedKeySpace { .. }
            | SelfCheckError::UnsupportedExpr { .. }
            | SelfCheckError::InvalidScope { .. } => None,
        }
    }
}

impl From<DdlError> for SelfCheckError {
    fn from(err: DdlError) -> Self {
        SelfCheckError::Ddl(err)
    }
}

impl From<CatalogError> for SelfCheckError {
    fn from(err: CatalogError) -> Self {
        SelfCheckError::Catalog(err)
    }
}

impl From<StagingError> for SelfCheckError {
    fn from(err: StagingError) -> Self {
        SelfCheckError::Staging(err)
    }
}

impl From<tokio_postgres::Error> for SelfCheckError {
    fn from(err: tokio_postgres::Error) -> Self {
        SelfCheckError::Db(err)
    }
}

impl From<crate::error::Error> for SelfCheckError {
    fn from(err: crate::error::Error) -> Self {
        SelfCheckError::Pool(err)
    }
}

/// Audits `target_table` — see the module doc comment for the full design.
/// `pool` is threaded straight through from [`crate::app::Trellis::pool`];
/// this is the function [`crate::app::Trellis::self_check`] is a thin facade
/// over.
pub async fn self_check(
    pool: &Pool,
    target_table: &str,
    scope: SelfCheckScope,
    mode: SelfCheckMode,
    timeout: Duration,
) -> Result<SelfCheckReport, SelfCheckError> {
    if scope.limit <= 0 {
        return Err(SelfCheckError::InvalidScope { limit: scope.limit });
    }

    let def = catalog::definition_by_target(pool, target_table)
        .await?
        .ok_or_else(|| SelfCheckError::TargetNotFound(target_table.to_string()))?;

    if !matches!(def.def.key_space, KeySpace::OneToOne) {
        return Err(SelfCheckError::UnsupportedKeySpace {
            target: target_table.to_string(),
        });
    }

    let mut report = audit(pool, &def, target_table, scope, mode, timeout).await?;
    // Read once the audit is done, so a key poisoned while it waited or
    // compared is reported too.
    let client = pool.get().await?;
    report.held_keys = quarantine::held_keys(&**client, def.id).await?;
    Ok(report)
}

/// [`self_check`]'s audit of `def`, the definition registered under
/// `target_table`, with no [`SelfCheckReport::held_keys`] yet.
async fn audit(
    pool: &Pool,
    def: &Definition,
    target_table: &str,
    scope: SelfCheckScope,
    mode: SelfCheckMode,
    timeout: Duration,
) -> Result<SelfCheckReport, SelfCheckError> {
    // #622 C9: a table whose capture is broken may not be feeding the target
    // at all, so that is reported first, and alone. A recompute comparison
    // would only show the symptom, and the convergence wait before it would
    // pass, because convergence is a predicate over ring rows the broken
    // capture never wrote. Nothing here is a race a re-check could resolve:
    // it is one catalog read of installed state.
    let capture = {
        let client = pool.get().await?;
        let faults = capture_audit::audit(&**client, pool.schema(), def).await?;
        if faults.is_empty() {
            None
        } else {
            Some((faults, converge::watermark_token(&**client).await?))
        }
    };
    if let Some((faults, read_at)) = capture {
        return Ok(SelfCheckReport {
            target: target_table.to_string(),
            checked_through: read_at,
            rows_compared: 0,
            next_after: scope.after.clone(),
            outcome: SelfCheckOutcome::Diverged(
                faults.into_iter().map(Divergence::Capture).collect(),
            ),
            held_keys: None,
        });
    }

    // Issue #121: this audit now keys on the source's full (possibly
    // composite) primary key, through the shared key-contract text
    // (`ddl::pk_key_sql_expr`) rather than a single named column.
    let pk = ddl::source_primary_key(pool, &def.source_table).await?;

    let schema_divergences = check_schema(pool, def, &pk).await?;
    let paused = paused_columns(pool, &def.def).await?;

    let pass1 = match await_then_compare(pool, def, &pk, &scope, &paused, timeout).await? {
        AwaitOutcome::NotCaughtUp { attempted } => {
            return Ok(SelfCheckReport {
                target: target_table.to_string(),
                checked_through: attempted,
                rows_compared: 0,
                next_after: scope.after.clone(),
                outcome: SelfCheckOutcome::NotCaughtUp,
                held_keys: None,
            });
        }
        AwaitOutcome::CaughtUp(pass) => pass,
    };

    let mut divergences = schema_divergences;
    divergences.extend(pass1.divergences);

    if divergences.is_empty() {
        return Ok(SelfCheckReport {
            target: target_table.to_string(),
            checked_through: pass1.checked_through,
            rows_compared: pass1.rows_compared,
            next_after: pass1.next_after,
            outcome: SelfCheckOutcome::Converged,
            held_keys: None,
        });
    }

    if mode == SelfCheckMode::Strict {
        return Ok(SelfCheckReport {
            target: target_table.to_string(),
            checked_through: pass1.checked_through,
            rows_compared: pass1.rows_compared,
            next_after: pass1.next_after,
            outcome: SelfCheckOutcome::Diverged(divergences),
            held_keys: None,
        });
    }

    // ADR-0013: "a divergence is reported as real only if it survives a
    // re-check after a fresh await — a genuine divergence is stable; a
    // convergence race resolves." Re-run the exact same bounded comparison
    // from scratch, behind a brand-new watermark token/await.
    let pass2 = match await_then_compare(pool, def, &pk, &scope, &paused, timeout).await? {
        AwaitOutcome::NotCaughtUp { attempted } => {
            return Ok(SelfCheckReport {
                target: target_table.to_string(),
                checked_through: attempted,
                rows_compared: 0,
                next_after: scope.after.clone(),
                outcome: SelfCheckOutcome::NotCaughtUp,
                held_keys: None,
            });
        }
        AwaitOutcome::CaughtUp(pass) => pass,
    };

    let mut second = check_schema(pool, def, &pk).await?;
    second.extend(pass2.divergences);
    let stable = reproduced(&divergences, second);

    let outcome = if stable.is_empty() {
        SelfCheckOutcome::Converged
    } else {
        SelfCheckOutcome::Diverged(stable)
    };

    Ok(SelfCheckReport {
        target: target_table.to_string(),
        checked_through: pass2.checked_through,
        rows_compared: pass2.rows_compared,
        next_after: pass2.next_after,
        outcome,
        held_keys: None,
    })
}

/// The re-check's filter (ADR-0013): of the divergences `second` (the
/// re-check pass) found, keeps only those whose [`DivergenceIdentity`] was
/// also in `first`. A divergence that resolved on re-check was a convergence
/// race, so it's dropped. One that shows up only on the re-check hasn't
/// survived two passes yet, so it isn't reported either. Returns `second`'s
/// copies, so a reproduced [`Divergence::Cell`] carries the fresher values.
fn reproduced(first: &[Divergence], second: Vec<Divergence>) -> Vec<Divergence> {
    let identities_seen_first: BTreeSet<DivergenceIdentity> =
        first.iter().map(DivergenceIdentity::of).collect();
    second
        .into_iter()
        .filter(|d| identities_seen_first.contains(&DivergenceIdentity::of(d)))
        .collect()
}

/// A [`Divergence`]'s identity for the purpose of deciding whether it
/// "reproduced" across a re-check (ADR-0013) — everything except the actual
/// `persisted`/`recomputed` text of a [`Divergence::Cell`], which is allowed
/// to differ between the two passes (a target genuinely mid-catch-up can
/// hold a different, still-wrong value on each pass and still be the *same*
/// stable divergence) as long as the cell keeps diverging at all.
///
/// Matching by identity rather than by exact value is what makes the
/// cross-connection lag window documented on [`SelfCheckMode::Standard`]
/// possible: a hot `(key, column)` caught mid-lag on both passes, with two
/// different transient values, matches here and is reported as stable. See
/// that doc comment for why this is the accepted tradeoff rather than a bug.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum DivergenceIdentity {
    Cell { key: String, column: String },
    MissingRow { key: String },
    ExtraRow { key: String },
    MissingColumn { column: String },
    ExtraColumn { column: String },
    Capture(CaptureFault),
}

impl DivergenceIdentity {
    fn of(d: &Divergence) -> Self {
        match d {
            Divergence::Cell { key, column, .. } => DivergenceIdentity::Cell {
                key: key.clone(),
                column: column.clone(),
            },
            Divergence::MissingRow { key } => DivergenceIdentity::MissingRow { key: key.clone() },
            Divergence::ExtraRow { key } => DivergenceIdentity::ExtraRow { key: key.clone() },
            Divergence::MissingColumn { column } => DivergenceIdentity::MissingColumn {
                column: column.clone(),
            },
            Divergence::ExtraColumn { column } => DivergenceIdentity::ExtraColumn {
                column: column.clone(),
            },
            Divergence::Capture(fault) => DivergenceIdentity::Capture(fault.clone()),
        }
    }
}

/// One [`compare_once`] pass's result, plus the token it was checked
/// through.
struct ComparePass {
    checked_through: PgLsn,
    rows_compared: i64,
    next_after: Option<String>,
    divergences: Vec<Divergence>,
}

/// [`await_then_compare`]'s result: either the target never caught up to
/// the fresh token it awaited (carrying that token, so the caller can report
/// exactly which LSN it failed to reach rather than fetching yet another,
/// even-fresher one just for the report), or it did and the bounded
/// comparison ran.
enum AwaitOutcome {
    NotCaughtUp { attempted: PgLsn },
    CaughtUp(ComparePass),
}

/// Takes a fresh watermark token, awaits convergence through it (bounded by
/// `timeout`), and — only if that succeeds — runs [`compare_once`].
async fn await_then_compare(
    pool: &Pool,
    def: &Definition,
    pk: &[PrimaryKeyColumn],
    scope: &SelfCheckScope,
    paused: &HashSet<String>,
    timeout: Duration,
) -> Result<AwaitOutcome, SelfCheckError> {
    let token = {
        let client = pool.get().await?;
        let token = converge::watermark_token(&**client).await?;
        // #799: on the audited definition's own held keys only. A key a
        // sibling holds is applied here as usual.
        if !caught_up(converge::await_converged_for(&client, token, timeout, def.id).await)? {
            return Ok(AwaitOutcome::NotCaughtUp { attempted: token });
        }
        token
    };

    let pass = compare_once(pool, def, pk, scope, paused, token).await?;
    Ok(AwaitOutcome::CaughtUp(pass))
}

/// Reads [`converge::await_converged`]'s result: `Ok(true)` if the target
/// caught up, `Ok(false)` only if the wait ran out of time
/// ([`StagingError::ConvergenceTimeout`]), which is the one failure that
/// means "not caught up yet" (issue #592). Any other error means the wait
/// itself couldn't run, say a lost connection or a failed query, so it
/// propagates. Reporting it as [`SelfCheckOutcome::NotCaughtUp`] would tell a
/// caller polling `self_check` to keep waiting on an audit that can't run.
fn caught_up(waited: Result<(), StagingError>) -> Result<bool, SelfCheckError> {
    match waited {
        Ok(()) => Ok(true),
        Err(StagingError::ConvergenceTimeout { .. }) => Ok(false),
        Err(err) => Err(err.into()),
    }
}

/// The bounded comparison itself (ADR-0013: the target read and the
/// recompute share one snapshot). Reads one keyset page of `def`'s source
/// (recomputed via [`render_leaf`]) and of the persisted target in a single
/// statement ([`page_sql`]), and diffs the two in Rust ([`diff_page`]).
async fn compare_once(
    pool: &Pool,
    def: &Definition,
    pk: &[PrimaryKeyColumn],
    scope: &SelfCheckScope,
    paused: &HashSet<String>,
    checked_through: PgLsn,
) -> Result<ComparePass, SelfCheckError> {
    // The recompute `SELECT` below carries no `WHERE` for the definition's
    // own partial-data predicate, which is sound only because `Predicate`
    // has exactly one variant today. Matched exhaustively (rather than
    // ignored) so that adding a real predicate variant breaks *here* — a
    // silently unfiltered recompute would report every predicate-excluded
    // source row as a `MissingRow`. `defs::oracle` and `generative::oracle`
    // pin the same assumption the same way.
    match def.def.predicate {
        Predicate::True => {}
    }

    let comparable: Vec<&FieldDef> = def
        .def
        .fields
        .iter()
        .filter(|f| !paused.contains(&f.name))
        .collect();

    // One statement reads both sides, so both read the same snapshot
    // (ADR-0013's "reads the target and runs the recompute" under one
    // snapshot), and it trims both pages to the same bound.
    let client = pool.get().await?;
    let collation = page_collation(&client, pk).await?;
    let sql = page_sql(def, pk, &collation, &comparable)?;
    let after: Option<&str> = scope.after.as_deref();
    let rows = client.query(sql.as_str(), &[&after, &scope.limit]).await?;

    let mut recomputed = Page::new();
    let mut persisted = Page::new();
    let mut page_end: Option<String> = None;
    for row in &rows {
        let is_recomputed: bool = row.get(0);
        page_end = row.get(1);
        let key: String = row.get(2);
        let values: Vec<Option<String>> = (0..comparable.len()).map(|i| row.get(i + 3)).collect();
        if is_recomputed {
            recomputed.insert(key, values);
        } else {
            persisted.insert(key, values);
        }
    }
    let columns: Vec<&str> = comparable.iter().map(|f| f.name.as_str()).collect();
    let diff = diff_page(&columns, recomputed, persisted, page_end);

    Ok(ComparePass {
        checked_through,
        rows_compared: diff.rows_compared,
        next_after: diff.next_after,
        divergences: diff.divergences,
    })
}

/// The collation [`page_sql`] compares both sides' key text under, so the
/// source and the target page in one order however their own key columns are
/// collated (issue #782). A 1-1 target's key is created with its source's
/// collation (#769), but `alter column … type text collate …` on the source
/// afterwards leaves the two ordering differently, and two pages read in two
/// orders hold different keys.
///
/// The source key index's collation when the key text is that one column
/// itself (a not-null collatable key of arity 1, whose `col::text` is the
/// column): naming the index's own collation keeps the source's index range
/// scan, and the target's whenever its key has the same collation, which is
/// always the case until the source is re-collated. After a re-collation the
/// target's page is read by a sequential scan and a sort, since its index
/// orders by the other collation; that costs an audit time, never a wrong
/// answer. Any other key text (a composite key's `array_to_string`, an
/// integer's `id::text`) is an expression no index orders, so it is compared
/// under `"C"`, the cheapest to sort.
///
/// The index's collation must be deterministic, though, so `"C"` takes its
/// place when it isn't. Define refuses a nondeterministic key collation
/// (#638), but an `alter` of the source key to one afterwards isn't refused,
/// and under one two distinct keys can compare equal: a page's `limit` could
/// cut between a target key and an extra target row tying with it, and the
/// next page's `> after` would skip the one cut, hiding it from every sweep.
/// A deterministic collation breaks every tie by bytes, so distinct keys are
/// never equal under it, and membership by byte equality ([`diff_page`])
/// agrees with the page's order.
///
/// [`SelfCheckScope::after`] is compared under this collation too, so a
/// cursor carried across a re-collation of the source key continues in the
/// new order: keys between the two orders' positions of the cursor may be
/// skipped or compared twice by that one sweep.
async fn page_collation(
    client: &tokio_postgres::Client,
    pk: &[PrimaryKeyColumn],
) -> Result<String, SelfCheckError> {
    let index_collation = match pk {
        [only] if !only.nullable => only.collation.as_deref(),
        _ => None,
    };
    let Some(collation) = index_collation else {
        return Ok(BYTE_ORDER.to_string());
    };
    let deterministic: Option<bool> = client
        .query_opt(
            "select co.collisdeterministic from pg_catalog.pg_collation co \
             where co.oid = pg_catalog.to_regcollation($1)",
            &[&collation],
        )
        .await?
        .map(|row| row.get(0));
    Ok(match deterministic {
        Some(true) => collation.to_string(),
        _ => BYTE_ORDER.to_string(),
    })
}

/// `"C"`, byte order.
const BYTE_ORDER: &str = r#"pg_catalog."C""#;

/// The statement [`compare_once`] runs: one keyset page of the recompute
/// over `def`'s source and of the persisted target, each `LIMIT`ed
/// separately, both ordered and bounded by the key text under `collation`
/// ([`page_collation`]), and both trimmed to the same end in SQL, where that
/// collation's ordering is.
///
/// `$1` is [`SelfCheckScope::after`] and `$2` its `limit`. Each row is
/// `(recomputed, page_end, key, compared values…)`: `recomputed` says which
/// side the row is from, and `page_end` is the page's bound, the same on
/// every row.
///
/// Why the trim: the two `LIMIT`ed reads are independent, so whenever a
/// divergence makes the two sides' key sets differ, their pages end at
/// *different* keys. A target missing one row inside the page pulls one key
/// in on the persisted side that the recompute side's own limit cut off.
/// Diffing the raw pages would report that key as an `ExtraRow` only because
/// it fell off the other side's page: a deterministic false divergence (it
/// reproduces on the re-check, so the ADR's re-check can't filter it), and
/// the cursor would skip past it, so no later page would compare it either.
/// So whichever side(s) filled the limit bound the page, and the *lowest*
/// such bound is its end. Keys past it are dropped from both sides and left
/// for the next page, which starts after `page_end`. A page where neither
/// side filled the limit reached the end of the keyspace: `page_end` is
/// null. The lowest bound has to be taken in the order the pages were read
/// in, which is why it is taken here and not in Rust (whose `String` order is
/// byte order, `"C"`'s, and not, say, `en-US`'s).
fn page_sql(
    def: &Definition,
    pk: &[PrimaryKeyColumn],
    collation: &str,
    comparable: &[&FieldDef],
) -> Result<String, SelfCheckError> {
    // `pk_key_sql_expr` is the source/target's shared key-contract text at
    // whatever arity `pk` has (issue #121): a bare `{col}::text` at arity 1.
    // Both sides page by it, under one collation, so they page in one order.
    // That order isn't the key's typed order (an integer key sorts as text),
    // and needn't be: the audit only ever compares the two sides key by key.
    let key = format!("({}) collate {collation}", ddl::pk_key_sql_expr(pk, None));
    let source_ident = ddl::qualified_source_table(&def.source_table);
    let target_ident = ddl::qualified_target_table_ident(&def.target_table);

    let mut recomputed = Vec::with_capacity(comparable.len());
    let mut persisted = Vec::with_capacity(comparable.len());
    for (i, field) in comparable.iter().enumerate() {
        let leaf = render_leaf(&field.expr).map_err(|detail| SelfCheckError::UnsupportedExpr {
            target: def.def.target.clone(),
            detail,
        })?;
        recomputed.push(format!(", ({leaf})::text as v{i}"));
        persisted.push(format!(", {}::text as v{i}", quote_ident(&field.name)));
    }
    let side = |table: &str, values: &[String]| {
        format!(
            "select {key} as k{} from {table} \
             where ($1::text is null or {key} > $1) order by {key} limit $2",
            values.concat()
        )
    };
    Ok(format!(
        "with recomputed as materialized ({}), \
              persisted as materialized ({}), \
              page as ( \
                select least( \
                  (select max(k) from recomputed having count(*) >= $2), \
                  (select max(k) from persisted having count(*) >= $2)) as page_end) \
         select true, page.page_end, recomputed.* from recomputed, page \
          where page.page_end is null or recomputed.k <= page.page_end \
         union all \
         select false, page.page_end, persisted.* from persisted, page \
          where page.page_end is null or persisted.k <= page.page_end",
        side(&source_ident, &recomputed),
        side(&target_ident, &persisted),
    ))
}

/// The plan of [`compare_once`]'s statement for `target_table`'s page after
/// `after`, as `explain`'s text, with sequential scans disabled so that a
/// table still read by one is a table no index can serve. For tests of the
/// plan's shape.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_page(
    pool: &Pool,
    target_table: &str,
    after: Option<&str>,
    limit: i64,
) -> Result<String, SelfCheckError> {
    let def = catalog::definition_by_target(pool, target_table)
        .await?
        .ok_or_else(|| SelfCheckError::TargetNotFound(target_table.to_string()))?;
    let pk = ddl::source_primary_key(pool, &def.source_table).await?;
    let comparable: Vec<&FieldDef> = def.def.fields.iter().collect();
    let mut client = pool.get().await?;
    let collation = page_collation(&client, &pk).await?;
    let sql = page_sql(&def, &pk, &collation, &comparable)?;
    let txn = client.transaction().await?;
    txn.batch_execute("set local enable_seqscan to off").await?;
    let rows = txn
        .query(format!("explain {sql}").as_str(), &[&after, &limit])
        .await?;
    txn.rollback().await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// One side of a [`compare_once`] page: key text to the `::text` value of
/// each compared column, in `columns` order.
type Page = BTreeMap<String, Vec<Option<String>>>;

/// [`diff_page`]'s result: [`ComparePass`] minus the token.
#[derive(Debug, PartialEq, Eq)]
struct PageDiff {
    rows_compared: i64,
    next_after: Option<String>,
    divergences: Vec<Divergence>,
}

/// The pure half of [`compare_once`]: diffs the recompute page against the
/// persisted page. `columns` names the compared columns, in the order each
/// page's value vectors hold them. Both pages are already trimmed to
/// `page_end`, the page's bound ([`page_sql`]), which becomes the cursor for
/// the next page.
fn diff_page(
    columns: &[&str],
    recomputed: Page,
    persisted: Page,
    page_end: Option<String>,
) -> PageDiff {
    let mut divergences = Vec::new();
    for (key, r_values) in &recomputed {
        match persisted.get(key) {
            None => divergences.push(Divergence::MissingRow { key: key.clone() }),
            Some(p_values) => {
                for (column, (r, p)) in columns.iter().zip(r_values.iter().zip(p_values.iter())) {
                    if r != p {
                        divergences.push(Divergence::Cell {
                            key: key.clone(),
                            column: column.to_string(),
                            persisted: p.clone(),
                            recomputed: r.clone(),
                        });
                    }
                }
            }
        }
    }
    for key in persisted.keys() {
        if !recomputed.contains_key(key) {
            divergences.push(Divergence::ExtraRow { key: key.clone() });
        }
    }

    let all_keys: BTreeSet<&String> = recomputed.keys().chain(persisted.keys()).collect();

    PageDiff {
        rows_compared: all_keys.len() as i64,
        next_after: page_end,
        divergences,
    }
}

/// Every column [`self_check`] must currently exclude from comparison for
/// `transform_table` — deliberately a fresh copy of the identical one-line
/// query [`crate::staging::quarantine::paused_columns_for`] already runs
/// (that helper is `pub(super)`, scoped to `staging::quarantine`'s own
/// callers, and `defs::backfill::paused_columns_for` already duplicates it
/// too, for the same layering reason: see that pair's own doc comments).
///
/// Closed over `def`'s alias readers, as that original is (issue #748).
async fn paused_columns(
    pool: &Pool,
    def: &crate::defs::ast::TransformDef,
) -> Result<HashSet<String>, SelfCheckError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select column_name from column_status where transform_table = $1",
            &[&def.target],
        )
        .await?;
    let mut paused: HashSet<String> = rows.into_iter().map(|row| row.get(0)).collect();
    if !paused.is_empty() {
        crate::defs::eval::AliasReaders::of(def).close(&mut paused);
    }
    Ok(paused)
}

/// The columns `def`'s target table is expected to physically have (its
/// primary key, plus every calculated field) vs. what it actually has —
/// [`Divergence::MissingColumn`]/[`Divergence::ExtraColumn`], ADR-0013's
/// schema-drift divergence kind. Cheap (one `pg_attribute` read), checked
/// once per [`self_check`] call rather than per comparison pass — schema
/// drift isn't something a convergence race could produce or resolve, so it
/// doesn't participate in the re-check dance [`self_check`] runs for
/// row-level divergences.
async fn check_schema(
    pool: &Pool,
    def: &Definition,
    pk: &[PrimaryKeyColumn],
) -> Result<Vec<Divergence>, SelfCheckError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select a.attname::text from pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attnum > 0 and not a.attisdropped",
            &[&ddl::regclass_arg(&def.target_table)],
        )
        .await?;
    let actual: HashSet<String> = rows.into_iter().map(|row| row.get(0)).collect();

    let mut expected: HashSet<String> = HashSet::with_capacity(def.def.fields.len() + pk.len());
    expected.extend(pk.iter().map(|c| c.name.clone()));
    for field in &def.def.fields {
        expected.insert(field.name.clone());
    }

    let mut divergences: Vec<Divergence> = expected
        .difference(&actual)
        .map(|column| Divergence::MissingColumn {
            column: column.clone(),
        })
        .collect();
    divergences.extend(
        actual
            .difference(&expected)
            .map(|column| Divergence::ExtraColumn {
                column: column.clone(),
            }),
    );
    // Deterministic order (`HashSet::difference` isn't) so two runs over an
    // unchanged schema report identical `Vec` contents/order — matters for
    // the re-check identity comparison in `self_check` above, and for a
    // caller/test asserting on the exact `Vec`.
    divergences.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    Ok(divergences)
}

/// Renders a calculated-field expression to Postgres SQL text — the "small"
/// leaf renderer ADR-0013 requires be duplicated for an honest audit rather
/// than shared with the write path or with either test-only oracle (see the
/// module doc comment's "The leaf renderer is independent of the write path"
/// section). Structurally similar to `defs::oracle::render_expr_sql` and
/// `generative::oracle`'s own `render_expr` (the same small grammar has only
/// so many reasonable ways to walk it), but written fresh, here, and
/// genuinely independent of both: neither of those functions is called from
/// anywhere in this module, and this function is called from nowhere else.
///
/// Unlike `defs::oracle::render_expr_sql` (which panics on a relationship
/// path — a test/benchmark-only oracle whose caller has already validated
/// the definition is relationship-free), this returns `Err` instead: a
/// production audit path must not panic on a shape it doesn't support yet,
/// it must fail the one audit call cleanly
/// ([`SelfCheckError::UnsupportedExpr`]).
fn render_leaf(expr: &Expr) -> Result<String, String> {
    Ok(match expr {
        Expr::Column(name) => quote_ident(name),
        Expr::NumberLiteral(text) => format!("{text}::numeric"),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::TypedLiteral { value_type, text } => typed_literal::render_sql(*value_type, text),
        Expr::RelationshipPath { rel, column } => {
            return Err(format!(
                "references relationship path '{rel}.{column}' — self_check doesn't audit a \
                 relationship-enriched target yet (ADR-0013's aggregate/relationship scope is \
                 deferred past this issue)"
            ));
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            };
            format!("({} {symbol} {})", render_leaf(lhs)?, render_leaf(rhs)?)
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let mut rendered = Vec::with_capacity(args.len());
            for arg in args {
                rendered.push(render_leaf(arg)?);
            }
            format!("{}({})", name.to_lowercase(), rendered.join(", "))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_leaf_renders_a_column_reference() {
        assert_eq!(
            render_leaf(&Expr::Column("price".to_string())).unwrap(),
            "\"price\""
        );
    }

    #[test]
    fn render_leaf_renders_a_number_literal_with_a_numeric_cast() {
        assert_eq!(
            render_leaf(&Expr::NumberLiteral("1.50".to_string())).unwrap(),
            "1.50::numeric"
        );
    }

    #[test]
    fn render_leaf_renders_a_string_literal_escaping_embedded_quotes() {
        assert_eq!(
            render_leaf(&Expr::StringLiteral("o'clock".to_string())).unwrap(),
            "'o''clock'::text"
        );
    }

    #[test]
    fn render_leaf_renders_addition() {
        let expr = Expr::BinaryOp {
            op: Operator::Add,
            lhs: Box::new(Expr::Column("a".to_string())),
            rhs: Box::new(Expr::Column("b".to_string())),
        };
        assert_eq!(render_leaf(&expr).unwrap(), "(\"a\" + \"b\")");
    }

    #[test]
    fn render_leaf_renders_greater_than() {
        let expr = Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(Expr::Column("a".to_string())),
            rhs: Box::new(Expr::NumberLiteral("0".to_string())),
        };
        assert_eq!(render_leaf(&expr).unwrap(), "(\"a\" > 0::numeric)");
    }

    #[test]
    fn render_leaf_renders_count_star() {
        let expr = Expr::FunctionCall {
            name: "COUNT".to_string(),
            args: Vec::new(),
        };
        assert_eq!(render_leaf(&expr).unwrap(), "count(*)");
    }

    #[test]
    fn render_leaf_renders_a_generic_function_call_lowercased() {
        let expr = Expr::FunctionCall {
            name: "STRPOS".to_string(),
            args: vec![
                Expr::Column("name".to_string()),
                Expr::StringLiteral("x".to_string()),
            ],
        };
        assert_eq!(render_leaf(&expr).unwrap(), "strpos(\"name\", 'x'::text)");
    }

    #[test]
    fn render_leaf_refuses_a_relationship_path_instead_of_panicking() {
        let expr = Expr::RelationshipPath {
            rel: "author".to_string(),
            column: "name".to_string(),
        };
        let err = render_leaf(&expr).unwrap_err();
        assert!(err.contains("author.name"));
    }

    #[test]
    fn divergence_identity_ignores_a_cells_persisted_and_recomputed_text() {
        let a = Divergence::Cell {
            key: "1".to_string(),
            column: "total".to_string(),
            persisted: Some("1".to_string()),
            recomputed: Some("2".to_string()),
        };
        let b = Divergence::Cell {
            key: "1".to_string(),
            column: "total".to_string(),
            persisted: Some("3".to_string()),
            recomputed: Some("4".to_string()),
        };
        assert_eq!(DivergenceIdentity::of(&a), DivergenceIdentity::of(&b));
    }

    /// Builds a [`Page`] of single-column rows from `(key, value)` pairs.
    fn page(rows: &[(&str, &str)]) -> Page {
        rows.iter()
            .map(|(key, value)| (key.to_string(), vec![Some(value.to_string())]))
            .collect()
    }

    fn cell(key: &str, persisted: &str, recomputed: &str) -> Divergence {
        Divergence::Cell {
            key: key.to_string(),
            column: "price".to_string(),
            persisted: Some(persisted.to_string()),
            recomputed: Some(recomputed.to_string()),
        }
    }

    fn missing(key: &str) -> Divergence {
        Divergence::MissingRow {
            key: key.to_string(),
        }
    }

    #[test]
    fn diff_page_reports_nothing_for_identical_pages_that_reach_the_end_of_the_keyspace() {
        let rows = [("1", "10"), ("2", "20")];
        assert_eq!(
            diff_page(&["price"], page(&rows), page(&rows), None),
            PageDiff {
                rows_compared: 2,
                next_after: None,
                divergences: Vec::new(),
            }
        );
    }

    #[test]
    fn diff_page_classifies_a_cell_a_missing_row_and_an_extra_row() {
        let diff = diff_page(
            &["price"],
            page(&[("1", "11"), ("2", "20")]),
            page(&[("1", "9999"), ("3", "30")]),
            None,
        );
        assert_eq!(
            diff.divergences,
            vec![
                cell("1", "9999", "11"),
                missing("2"),
                Divergence::ExtraRow {
                    key: "3".to_string()
                },
            ]
        );
        assert_eq!(diff.rows_compared, 3);
        assert_eq!(diff.next_after, None);
    }

    /// A page's bound, worked out in SQL ([`page_sql`]), is the cursor for
    /// the next page, and every key either side kept is compared once.
    #[test]
    fn diff_page_carries_the_page_end_to_the_cursor() {
        let diff = diff_page(
            &["price"],
            page(&[("1", "10"), ("2", "20"), ("3", "30")]),
            page(&[("1", "10"), ("3", "30")]),
            Some("3".to_string()),
        );
        assert_eq!(
            diff,
            PageDiff {
                rows_compared: 3,
                next_after: Some("3".to_string()),
                divergences: vec![missing("2")],
            }
        );
    }

    /// A divergence that doesn't reproduce on the re-check was a convergence
    /// race and must be dropped. A divergence that does reproduce is still
    /// reported, carrying the re-check's values. One that appears only on the
    /// re-check hasn't survived two passes, so it isn't reported yet.
    #[test]
    fn reproduced_keeps_only_divergences_seen_on_both_passes() {
        let first = vec![cell("1", "9999", "11"), missing("2")];
        let second = vec![cell("1", "8888", "11"), missing("5")];
        assert_eq!(reproduced(&first, second), vec![cell("1", "8888", "11")]);
    }

    #[test]
    fn reproduced_keeps_a_stable_divergence_rather_than_erasing_it() {
        let both = vec![cell("1", "9999", "11")];
        assert_eq!(reproduced(&both, both.clone()), both);
    }

    /// Only the wait running out of time reads as "not caught up yet": a
    /// genuine timeout still yields `Ok(false)`, so `self_check` reports
    /// [`SelfCheckOutcome::NotCaughtUp`].
    #[test]
    fn caught_up_reads_a_convergence_timeout_as_not_caught_up() {
        let timeout = StagingError::ConvergenceTimeout {
            token: PgLsn::from(42),
            waited: Duration::from_millis(150),
        };
        assert!(!caught_up(Err(timeout)).expect("a timeout is not an error"));
        assert!(caught_up(Ok(())).expect("a converged wait is not an error"));
    }

    /// Issue #592: any other wait failure means the audit couldn't run, so it
    /// must come back as `Err`, with the wrapped error's own code, never as a
    /// benign "not caught up".
    #[test]
    fn caught_up_propagates_every_other_wait_failure() {
        let err = caught_up(Err(StagingError::InvalidRingSlot(99)))
            .expect_err("a non-timeout wait failure must propagate");
        assert!(
            matches!(
                err,
                SelfCheckError::Staging(StagingError::InvalidRingSlot(99))
            ),
            "got {err:?}"
        );
        assert_eq!(err.code(), ErrorCode::Internal);
    }
}
