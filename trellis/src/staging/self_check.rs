//! `self_check`: a production recompute audit (ADR-0013).
//!
//! Trellis's one hard correctness promise is byte-identical convergence to a
//! from-scratch recompute at any caught-up LSN. The failure mode this module
//! guards against is a *silently stale target*: a wrong answer with no error
//! raised and no metric out of range, invisible without an independent
//! recompute. `self_check` makes that recompute a shipped,
//! operator-callable capability instead of something only the test suite can
//! perform — see [`crate::app::Trellis::self_check`] for the public facade
//! and [`super::self_check_job`] for the background job behind it.
//!
//! # Postgres is the oracle; the comparison is two-way
//!
//! `self_check` compares the persisted target against an equivalent
//! recompute *query* executed by Postgres. The engine computes no value
//! itself, so there is no second engine-side computation to compare with.
//!
//! # The leaf renderer is independent of the write path
//!
//! [`render_leaf`] below renders a calculated-field expression to SQL text
//! from scratch. It deliberately does **not** call
//! [`crate::defs::oracle::render_expr_sql`] or any of its siblings in
//! `defs::oracle` — those are used by the *production write path*
//! (the Re-derive build's 1-1 chunks, and
//! `staging::group_basis`'s aggregate apply), not just by
//! tests. Auditing backfilled rows with a renderer the backfill path itself
//! uses would check the write path against itself: a rendering bug would
//! agree with itself, and the audit would pass on a wrong answer. This
//! renderer shares no code with `defs::oracle`'s, or with
//! `generative::oracle`'s own independent SQL renderer — the generative
//! suite's cross-check test (`generative/tests/self_check_cross_check.rs`)
//! is what keeps the two from silently drifting apart.
//!
//! A 1-1 field that reads another calculated field by name is recomputed with
//! that field's expression inlined ([`FieldInliner`]), by the same independent
//! walk: it shares no code with `defs::backfill::substitute_all_fields`, and is
//! bounded by a node budget and a nesting limit so a definition define would
//! refuse fails the job with an error rather than expanding.
//!
//! # The capture audit comes first
//!
//! Before it waits or compares anything, `self_check` checks from the
//! catalog that every table the target is computed from is still captured
//! as `capture::install` installed it: all five triggers present, `ENABLE
//! ALWAYS` and calling their functions, the functions `SECURITY DEFINER` and
//! owned by the Trellis role, that role still holding the privileges the
//! functions use, and the table still outside any partition or inheritance
//! hierarchy ([`super::capture_audit`]). It also checks that no
//! such table's row-level security applies to the Trellis role or the
//! caller's, which would filter the recompute's reads as well as the
//! engine's, that the target's doesn't apply to the caller's, standing in
//! for the workers' that write it, and that no logical-replication
//! subscription replicates into a table it is computed from, whose
//! changes capture never sees. A fault there is reported as [`Divergence::Capture`], without a
//! recompute comparison: the convergence wait can't see a broken capture (it
//! is a predicate over ring rows, and the broken capture writes none), and
//! the comparison would only show its symptom, or agree with it.
//!
//! # Quiescence: "diverged" vs "not yet caught up"
//!
//! A single snapshot is not sufficient under live load: a correctly-working
//! target legitimately lags its source by CDC apply latency. `self_check`
//! takes a watermark token, awaits convergence through it (bounded by a
//! timeout — on timeout it reports [`SelfCheckOutcome::NotCaughtUp`], never a
//! divergence; any other failure of the wait is an `Err`, see [`caught_up`]),
//! then reads the target and runs the recompute in a single statement, so
//! under one snapshot ([`compare_once`]).
//!
//! A commit after the token is in the source that statement reads, and the
//! target may not have applied it yet. A 1-1 target, an aggregate on the group basis and a
//! `JOIN` target compare against the source minus the ring rows the target
//! hasn't applied yet, read in the same statement
//! ([`super::self_check_ring`]): capture writes a change's ring row in the
//! change's own transaction, so that is exactly what the target has applied,
//! and a divergence is reported from one pass. Under
//! [`SelfCheckMode::Standard`], a divergence the comparison can't vouch for
//! (one on a key or group an unsubtractable ring row may touch) is reported only if it survives a re-check after a *fresh* await —
//! a genuine divergence is stable; a convergence race resolves.
//! [`SelfCheckMode::Strict`] skips the re-check: it is sound only when the
//! caller has already stopped writes to the audited tables (so there is no
//! race left to resolve), and is what a caller that can quiesce (e.g. the
//! generative suite, after `ManualBackend::quiesce`) should reach for.
//!
//! # Only a `live` definition is compared
//!
//! The convergence wait is a predicate over the staging ring
//! ([`converge::await_converged`]); it never looks at a definition's status,
//! and a repair that rebuilds a definition (an explicit `request_backfill`, a
//! resume, an `ALTER TRANSFORM`, a capture re-install) does its work in
//! background jobs the ring knows nothing about. A comparison made while the
//! definition is being built would therefore agree or disagree with a state
//! the build is about to change. So `self_check` compares only a `live`
//! definition, and reports any other as [`SelfCheckOutcome::NotLive`] with
//! its status, after the capture audit and before the wait. `live` is strict:
//! every job of a rebuild is done before the definition reads it again, and
//! a repair's own transaction moves the definition out of it, so a caller
//! that polls for `live` and then checks is checking after the repair.
//!
//! # Scope: 1-1 targets, group-basis aggregates and JOIN targets
//!
//! A 1-1 target is compared row by row against its source. An aggregate on
//! the group basis ([`super::group_basis`]) is
//! compared a page of groups at a time by [`super::self_check_groups`]: each
//! group's hidden partials against a `GROUP BY` of the source, and its
//! visible row against the partials. A `JOIN` target
//! ([`super::join`]) is compared by [`super::self_check_joins`]: its visible
//! rows and `T`'s partials against the definition's `JOIN … GROUP BY`, and
//! `P`, `M` and `J` against their tables. Both detect and never repair
//! (ADR-0013).
//!
//! # How a page finds its keys
//!
//! A page reads its keys by a comparison on the key's raw columns,
//! `(k1, …, kn) > (c1, …, cn) order by k1, …, kn limit L`, on the source and
//! on the target ([`super::self_check_keyset`]), not by the key's encoded
//! text. The text is the key a divergence is reported by and the cursor a
//! walk saves; it orders nothing. A 1-1 target pages by its source's
//! primary key, which has its index, so each page is a range scan of both
//! sides and a walk reads each table about once: its cost is linear in the
//! rows, where paging by the text, which no index orders, would read the
//! whole source on every page: N / L times the table. An aggregate or a
//! `JOIN` pages its source by its group or join columns and is index-ordered
//! only if the source has an index leading with them
//! ([`super::self_check_groups`], [`super::self_check_joins`]).
//!
//! # One page at a time
//!
//! This module compares one keyset page ([`SelfCheckScope`]) of a target:
//! [`check_page`] and the audit under it. The public `self_check` is a
//! background job built from those pages, one per pass of a worker task
//! ([`super::self_check_job`]): it registers a row and
//! returns, a worker walks the target's keyspace page by page, and the
//! caller polls the row. No page runs inside a public call.
//!
//! # Column-level quarantine
//!
//! A paused column ([`crate::staging::quarantine`]) holds a deliberately
//! stale value, so comparing it would report a false divergence on exactly
//! the targets an operator is most likely to be inspecting — `self_check`
//! excludes any currently-paused column of the audited transform from the
//! comparison entirely (see [`paused_columns`]).
//!
//! # Held keys
//!
//! A key the audited definition holds in quarantine
//! ([`crate::staging::quarantine`]) has a target row the definition stopped
//! writing, and a key with changes parked holds back the convergence wait.
//! Every page's report names how many keys the definition holds and since when
//! ([`SelfCheckPage::held_keys`]), read once the audit is done, so a
//! `Converged` page that never reached one, or a `NotCaughtUp` it causes,
//! can't hide it.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_postgres::types::PgLsn;

use crate::defs::ast::{Expr, FieldDef, KeySpace, Predicate, as_extract};
use crate::defs::catalog::{self, CatalogError};
use crate::defs::ddl::{self, DdlError, PrimaryKeyColumn};
use crate::defs::model::{Definition, TransformStatus};
use crate::defs::registry;
use crate::defs::typed_literal;
use crate::error_code::{self, ErrorCode};
use crate::pool::{Pool, quote_ident};

use super::capture_audit::{self, CaptureFault};
use super::converge;
use super::error::StagingError;
use super::holdup::{self, DrainFailure};
use super::quarantine::{self, HeldKeys};
use super::self_check_keyset::Keyset;
use super::self_check_ring::Ring;

/// One page's worth of bound for a [`check_page`] call — see the module doc
/// comment's "Bounded, keyset-scoped, mandatory" section. `after` is a
/// keyset cursor (the last key seen by a previous call's
/// [`SelfCheckPage::next_after`]; `None` starts from the beginning of the
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
#[non_exhaustive]
pub enum SelfCheckMode {
    /// A divergence the comparison can't vouch for is provisional until it
    /// survives a re-check after a fresh await — sound under live load, at
    /// the cost of up to double the work for a page that holds one.
    ///
    /// **An aggregate on the group basis or a `JOIN` target** compares
    /// against the source minus the ring rows the target hasn't applied yet,
    /// in the one statement that reads the target
    /// (`super::self_check_ring`). Under one snapshot that is exactly
    /// what the target has applied, however hot its groups are, so a
    /// divergence on such a page is reported from one pass. Only a
    /// divergence on a group the subtraction can't vouch for is re-checked:
    /// one an unapplied change with no usable image (a `Recompute`) may
    /// touch, or a held key whose parked origin is unknown, every group
    /// while the source has an unapplied `TRUNCATE`, partition truncate or
    /// schema change, and every group of a `JOIN`'s target part while either
    /// table has any such change. The trade-off: a change stuck in the ring
    /// is subtracted rather than reported here. It shows up as a
    /// `SelfCheckPage::drain_failures` entry or a held key
    /// (`SelfCheckPage::held_keys`) instead.
    ///
    /// **A 1-1 target** compares each row of a page against what the target
    /// has applied of it, in the same statement that reads the target
    /// (`super::self_check_ring`): the old image of the key's
    /// earliest unapplied change its page would apply (no row, for an
    /// insert), or the live source row when the ring holds no such change
    /// for the key. A divergence on
    /// a key so compared is reported from one pass. Only a divergence on a
    /// key the comparison can't vouch for is re-checked: one with an
    /// unapplied change that has no usable image (a `Recompute`), a held key
    /// whose parked origin is unknown, or any key while the source has an
    /// unapplied `TRUNCATE`, partition truncate or schema change.
    ///
    /// For those fallbacks, a known and accepted limitation: **the re-check's
    /// guarantee is identity stability, not value non-transience.**
    /// `await_then_compare` takes its watermark token and runs
    /// `converge::await_converged` on one connection borrowed from the
    /// pool, then `compare_once` runs its statement on a separate
    /// `pool.get()` call, which may hand back a *different* connection. A
    /// commit landing in that gap is visible to the source read but not yet
    /// applied to the target. Two independent passes can each catch this
    /// same lag race on the same hot `(key, column)`, with a different
    /// transient value each time, and because divergences are matched by
    /// identity (key/column) across passes rather than by exact value (see
    /// `DivergenceIdentity`), such a case is reported as a stable, real
    /// divergence even though it is still just lag.
    ///
    /// Matching by exact value instead would trade this narrow
    /// false-positive risk for false *negatives*: a genuinely-broken column
    /// that happens to change value on every pass under active writes would
    /// then be silently suppressed, which is worse for an audit tool.
    ///
    /// In short: a [`SelfCheckOutcome::Diverged`] under `Standard` means the
    /// target differs from what its applied changes say, except on the
    /// fallbacks above, where it means "this divergence's identity was
    /// stable across two passes". Callers needing the stronger guarantee on
    /// those should quiesce writes themselves and use
    /// [`SelfCheckMode::Strict`], which skips the re-check (and therefore
    /// this window) entirely.
    Standard,
    /// Skips the re-check: a divergence found on the first pass is reported
    /// immediately. **Sound only when the caller has already stopped writes
    /// to the audited tables** — there is no convergence race left to
    /// resolve, so re-checking would only cost time, not change the answer.
    Strict,
}

/// One [`check_page`] call's result: a keyset page of the comparison.
///
/// The job ([`super::self_check_job`]) reads the comparison's own fields; the
/// rest is what `check_page`, the page on its own, adds for the tests of it.
#[cfg_attr(
    not(any(test, feature = "internals")),
    allow(
        dead_code,
        reason = "`target`, `held_keys` and `drain_failures` are read by tests through `internals`"
    )
)]
#[derive(Debug, Clone)]
pub struct SelfCheckPage {
    /// The audited target's bare table name (as passed to [`check_page`]).
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
    /// sides' last keys, in the order both sides are paged in (the key's
    /// own columns in order, `NULL` last, each collatable one under one
    /// collation, see [`super::self_check_keyset`]), whenever either side hit
    /// [`SelfCheckScope::limit`] (keys past it fell off one side's page and
    /// are deliberately left for the next call rather than diffed against a
    /// truncated counterpart). `None` means neither side hit the limit, so
    /// this page reached the end of the target's keyspace and a following
    /// call would have nothing to do.
    pub next_after: Option<String>,
    pub outcome: SelfCheckOutcome,
    /// The keys the audited definition holds in quarantine when the audit
    /// ends, `None` if it holds none. Reported with every outcome,
    /// because a held key is one the audit can't vouch for: the definition
    /// leaves its changes out, so its target row stays as it was when the
    /// key was poisoned. It is a page's [`SelfCheckOutcome::Diverged`] row
    /// once its source row changes, if the page reaches it, and a key with
    /// changes parked holds back convergence, so the audit reports
    /// [`SelfCheckOutcome::NotCaughtUp`] until it is released. Sample the
    /// keys with `Trellis::sample_quarantined` and release each with
    /// `Trellis::release_key`, or resume the definition.
    pub held_keys: Option<HeldKeys>,
    /// Every page the drain keeps failing on with nothing charged or paused,
    /// oldest first, whichever definitions read its tables: an
    /// instance-level finding, reported with every outcome. Each holds back
    /// the targets of the tables it holds changes to, and with them the
    /// convergence this audit waits on. Empty when there is none.
    pub drain_failures: Vec<DrainFailure>,
}

/// What `self_check` found — see the module doc comment's "Quiescence"
/// section for why [`SelfCheckOutcome::NotCaughtUp`] is a distinct outcome
/// from [`SelfCheckOutcome::Diverged`] rather than folded into it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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
    /// The definition isn't [`TransformStatus::Live`]: it is being
    /// built or rebuilt, catching up, paused or quarantined, with the
    /// carried status. Nothing was compared and nothing was awaited.
    /// **Not a verdict on correctness**, like
    /// [`SelfCheckOutcome::NotCaughtUp`]; poll the definition's status until
    /// it is `live` and check again. A rebuild a repair starts
    /// ([`crate::Trellis::request_backfill`]) is visible here from the call's
    /// return, so a caller that checks right after one is told so rather
    /// than shown a comparison the rebuild is still about to change.
    NotLive(TransformStatus),
}

/// One divergence `self_check` found: ADR-0013's four kinds, plus a
/// broken capture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
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
    /// partition or inheritance hierarchy (see
    /// `super::capture_audit`), its row-level security applies to the
    /// Trellis role, which would filter its reads, this audit's too
    /// (or the target's does, which would filter its writes),
    /// or a logical-replication subscription replicates into it. Its
    /// changes may not be reaching the target at all, so `self_check` reports these before, and instead
    /// of, a recompute comparison.
    Capture(CaptureFault),
}

/// Why a `check_page` call failed outright (as opposed to reporting a
/// [`SelfCheckOutcome::Diverged`], which is a successful audit that found a
/// problem, not a failure of the audit itself).
#[derive(Debug)]
#[non_exhaustive]
pub enum SelfCheckError {
    /// No transform is registered under this target table name.
    TargetNotFound(String),
    /// The target is neither 1-1, an aggregate on the group basis nor a
    /// `JOIN` that applies — see the module doc comment's "Scope" section.
    UnsupportedKeySpace {
        target: String,
    },
    /// `SelfCheckScope::limit` wasn't a positive row count. Refused rather
    /// than run: a zero/negative limit compares nothing at all, and would
    /// otherwise report a cheerful `Converged` over an empty page — an audit
    /// that silently checks nothing is worse than one that declines.
    InvalidScope {
        limit: i64,
    },
    /// The audited definition reads something the comparison can't render
    /// (a `JOIN` field outside the shapes it holds), or its recorded state
    /// is missing (a `JOIN` with no joined table, a keyset over a key that's
    /// gone).
    UnsupportedExpr {
        target: String,
        detail: String,
    },
    /// A page of a `self_check` job ran past its budget
    /// (`super::self_check_job::page_budget`) and the server stopped it.
    /// The job fails with this; a new `self_check` starts over.
    PageTimedOut {
        budget: Duration,
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
            SelfCheckError::PageTimedOut { .. } => ErrorCode::Timeout,
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
                "self_check audits a 1-1 target, an aggregate or a JOIN; \"{target}\" is an \
                 aggregate the group basis doesn't take, which it can't audit"
            ),
            SelfCheckError::UnsupportedExpr { target, detail } => {
                write!(f, "self_check can't audit \"{target}\": {detail}")
            }
            SelfCheckError::PageTimedOut { budget } => write!(
                f,
                "a self_check page ran past its {budget:?} budget, and the server stopped it"
            ),
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
            | SelfCheckError::PageTimedOut { .. }
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

/// Audits one keyset page of `target_table` — see the module doc comment for
/// the full design. The background job ([`super::self_check_job`]) runs this
/// once per pass of its worker, each from where the last ended; it is
/// not reached from a public call.
#[cfg(any(test, feature = "test-util"))]
pub async fn check_page(
    pool: &Pool,
    target_table: &str,
    scope: SelfCheckScope,
    mode: SelfCheckMode,
    timeout: Duration,
) -> Result<SelfCheckPage, SelfCheckError> {
    let def = comparable_definition(pool, target_table).await?;
    let mut page = audit(pool, &def, target_table, scope, mode, timeout).await?;
    // Read once the audit is done, so a key poisoned while it waited or
    // compared is reported too.
    let found = findings(pool, &def).await?;
    page.held_keys = found.held_keys;
    page.drain_failures = found.drain_failures;
    Ok(page)
}

/// [`check_page`] through a [`crate::Trellis`] handle, for the tests of the
/// page-level comparison (its keyset paging, collation and report fields),
/// which `Trellis::self_check`'s background job no longer exposes.
#[cfg(any(test, feature = "internals"))]
pub trait CheckPageExt {
    /// One page of the comparison of `target_table`, as the job's worker runs
    /// it, plus the findings a report carries.
    fn self_check_page(
        &self,
        target_table: &str,
        scope: SelfCheckScope,
        mode: SelfCheckMode,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<SelfCheckPage, crate::TrellisError>> + Send;

    /// `Trellis::self_check` run to its end: the job it starts, with every
    /// page run by hand ([`super::self_check_job::run_to_end`]), and its
    /// report, for the tests that want the verdict.
    fn self_check_to_end(
        &self,
        target_table: &str,
        mode: SelfCheckMode,
        timeout: Duration,
    ) -> impl std::future::Future<
        Output = Result<super::self_check_job::SelfCheckReport, crate::TrellisError>,
    > + Send;
}

#[cfg(any(test, feature = "internals"))]
impl CheckPageExt for crate::Trellis {
    async fn self_check_page(
        &self,
        target_table: &str,
        scope: SelfCheckScope,
        mode: SelfCheckMode,
        timeout: Duration,
    ) -> Result<SelfCheckPage, crate::TrellisError> {
        check_page(self.pool(), target_table, scope, mode, timeout)
            .await
            .map_err(crate::TrellisError::SelfCheck)
    }

    async fn self_check_to_end(
        &self,
        target_table: &str,
        mode: SelfCheckMode,
        timeout: Duration,
    ) -> Result<super::self_check_job::SelfCheckReport, crate::TrellisError> {
        let options = super::self_check_job::WorkerOptions::new(Duration::from_secs(30));
        let job =
            super::self_check_job::run_to_end(self, target_table, mode, timeout, &options).await?;
        match job.report {
            Some(report) => Ok(report),
            None => panic!(
                "the self_check job did not finish with a report: {:?} {:?}",
                job.state, job.error
            ),
        }
    }
}

/// The definition registered under `target_table`, if it is one `self_check`
/// can compare: a 1-1 target, an aggregate on the group basis or a `JOIN`
/// that applies. Refused with what is wrong otherwise.
pub(crate) async fn comparable_definition(
    pool: &Pool,
    target_table: &str,
) -> Result<Definition, SelfCheckError> {
    let def = catalog::definition_by_target(pool, target_table)
        .await?
        .ok_or_else(|| SelfCheckError::TargetNotFound(target_table.to_string()))?;

    if !matches!(def.def.key_space, KeySpace::OneToOne)
        && group_shape(&def).is_none()
        && !joined(&def)
    {
        return Err(SelfCheckError::UnsupportedKeySpace {
            target: target_table.to_string(),
        });
    }
    // A walk of a `GROUP BY` or `JOIN` target pages its source by an
    // index, and without one scans it once per page. Refused unless the
    // definition opted out; nothing else about the definition changes.
    if !def.def.without_source_index {
        let client = pool.get().await?;
        let joined = catalog::join_table_of(&**client, def.id).await?;
        let missing = crate::defs::source_index::unmet_for(
            &**client,
            &def.def,
            &def.source_columns,
            &def.source_table,
            joined.as_deref(),
        )
        .await?;
        if !missing.is_empty() {
            return Err(SelfCheckError::Catalog(CatalogError::SourceIndexMissing {
                target: def.def.target.clone(),
                missing,
            }));
        }
    }
    Ok(def)
}

/// The group-basis shape of `def`, if it is an aggregate on the group basis:
/// audited by [`super::self_check_groups`].
fn group_shape(def: &Definition) -> Option<super::group_basis::GroupShape> {
    super::group_basis::route(&def.def, &def.source_columns)
}

/// Whether `def` is a `JOIN` that applies ([`super::join::applies`]): audited by [`super::self_check_joins`].
fn joined(def: &Definition) -> bool {
    super::join::applies(&def.def)
}

/// Refuses a `JOIN` definition with a field the comparison can't render,
/// which the comparison would otherwise refuse only once it got that far.
/// Every other definition renders.
pub(crate) fn check_renders(def: &Definition) -> Result<(), SelfCheckError> {
    if group_shape(def).is_none() && joined(def) {
        return super::self_check_joins::check_renders(def);
    }
    Ok(())
}

/// What a report says about the instance and the definition, whatever its
/// outcome: read live, when the report is made.
pub(crate) struct Findings {
    pub(crate) held_keys: Option<HeldKeys>,
    pub(crate) drain_failures: Vec<DrainFailure>,
}

/// [`Findings`] for `def`.
pub(crate) async fn findings(pool: &Pool, def: &Definition) -> Result<Findings, SelfCheckError> {
    let client = pool.get().await?;
    Ok(Findings {
        held_keys: quarantine::held_keys(&**client, def.id).await?,
        drain_failures: holdup::open(&**client).await?,
    })
}

/// A 1-1 target's walk key: the source key's columns and types (the
/// cursor's cast and the ring's encoding), each collation the target key
/// column's. Define gives the target key the source's collation and nothing
/// re-collates it afterwards, so the walk's order holds when the source key
/// is re-collated between pages. The target's key columns are named
/// as the source's (`ddl::target_table_ddl`), so they pair by name; a source
/// key column with no namesake there was renamed since define, which pauses
/// the transform, and keeps its own collation meanwhile.
async fn walk_key(
    pool: &Pool,
    def: &Definition,
) -> Result<Vec<ddl::PrimaryKeyColumn>, SelfCheckError> {
    let mut pk = ddl::source_primary_key(pool, &def.source_table).await?;
    let client = pool.get().await?;
    let target = ddl::identity_key_columns(&**client, &def.target_table).await?;
    for column in &mut pk {
        if let Some(t) = target.iter().find(|t| t.name == column.name) {
            column.collation = t.collation.clone();
        }
    }
    Ok(pk)
}

/// [`check_page`]'s audit of `def`, the definition registered under
/// `target_table`, with no [`SelfCheckPage::held_keys`] yet.
pub(crate) async fn audit(
    pool: &Pool,
    def: &Definition,
    target_table: &str,
    scope: SelfCheckScope,
    mode: SelfCheckMode,
    timeout: Duration,
) -> Result<SelfCheckPage, SelfCheckError> {
    if scope.limit <= 0 {
        return Err(SelfCheckError::InvalidScope { limit: scope.limit });
    }

    // A table whose capture is broken may not be feeding the target
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
        return Ok(SelfCheckPage {
            target: target_table.to_string(),
            checked_through: read_at,
            rows_compared: 0,
            next_after: scope.after.clone(),
            outcome: SelfCheckOutcome::Diverged(
                faults.into_iter().map(Divergence::Capture).collect(),
            ),
            held_keys: None,
            drain_failures: Vec::new(),
        });
    }

    // Only a `live` definition is compared (see the module doc's
    // "Only a `live` definition is compared"). After the capture audit, a
    // fact about the catalog that holds whatever the status, and before the
    // wait and the comparison, which a definition that is being built would
    // pass or fail for reasons that have nothing to do with its steady
    // state.
    if def.status != TransformStatus::Live {
        let client = pool.get().await?;
        return Ok(SelfCheckPage {
            target: target_table.to_string(),
            checked_through: converge::watermark_token(&**client).await?,
            rows_compared: 0,
            next_after: scope.after.clone(),
            outcome: SelfCheckOutcome::NotLive(def.status),
            held_keys: None,
            drain_failures: Vec::new(),
        });
    }

    // This audit now keys on the source's full (possibly
    // composite) primary key, through the shared key-contract text
    // (`ddl::pk_key_sql_expr`) rather than a single named column.
    // An aggregate on the group basis or a `JOIN` is keyed by its `GROUP BY`
    // columns (its target's identity), and compared by
    // `super::self_check_groups` or `super::self_check_joins`.
    let group = group_shape(def);
    let pk = if group.is_some() || joined(def) {
        let client = pool.get().await?;
        ddl::identity_key_columns(&**client, &def.target_table).await?
    } else {
        walk_key(pool, def).await?
    };

    let schema_divergences = check_schema(pool, def, &pk).await?;
    let paused = paused_columns(pool, &def.def).await?;

    let pass1 =
        match await_then_compare(pool, def, group.as_ref(), &pk, &scope, &paused, timeout).await? {
            AwaitOutcome::NotCaughtUp { attempted } => {
                return Ok(SelfCheckPage {
                    target: target_table.to_string(),
                    checked_through: attempted,
                    rows_compared: 0,
                    next_after: scope.after.clone(),
                    outcome: SelfCheckOutcome::NotCaughtUp,
                    held_keys: None,
                    drain_failures: Vec::new(),
                });
            }
            AwaitOutcome::CaughtUp(pass) => pass,
        };

    let mut divergences = schema_divergences;
    divergences.extend(pass1.divergences);

    if divergences.is_empty() {
        return Ok(SelfCheckPage {
            target: target_table.to_string(),
            checked_through: pass1.checked_through,
            rows_compared: pass1.rows_compared,
            next_after: pass1.next_after,
            outcome: SelfCheckOutcome::Converged,
            held_keys: None,
            drain_failures: Vec::new(),
        });
    }

    // `Strict` reports the first pass. So does `Standard` when no divergence
    // falls on a key the pass couldn't vouch for exactly: a group-basis or
    // `JOIN` page compares against the source minus the ring rows its target
    // hasn't applied yet (`super::self_check_ring`), so what it finds is the
    // target's, not lag.
    if !rechecks(mode, &divergences, &pass1.uncertain) {
        return Ok(SelfCheckPage {
            target: target_table.to_string(),
            checked_through: pass1.checked_through,
            rows_compared: pass1.rows_compared,
            next_after: pass1.next_after,
            outcome: SelfCheckOutcome::Diverged(divergences),
            held_keys: None,
            drain_failures: Vec::new(),
        });
    }

    // ADR-0013: a divergence the pass couldn't vouch for (one on a key an
    // image-less or otherwise unsubtractable ring row touches) is reported as real only if
    // it survives a re-check after a fresh await. Re-run the exact same
    // bounded comparison from scratch, behind a brand-new watermark
    // token/await, and keep what reproduces.
    let pass2 =
        match await_then_compare(pool, def, group.as_ref(), &pk, &scope, &paused, timeout).await? {
            AwaitOutcome::NotCaughtUp { attempted } => {
                return Ok(SelfCheckPage {
                    target: target_table.to_string(),
                    checked_through: attempted,
                    rows_compared: 0,
                    next_after: scope.after.clone(),
                    outcome: SelfCheckOutcome::NotCaughtUp,
                    held_keys: None,
                    drain_failures: Vec::new(),
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

    Ok(SelfCheckPage {
        target: target_table.to_string(),
        checked_through: pass2.checked_through,
        rows_compared: pass2.rows_compared,
        next_after: pass2.next_after,
        outcome,
        held_keys: None,
        drain_failures: Vec::new(),
    })
}

/// Whether `divergences`, a first pass's, are re-checked before any is
/// reported: under [`SelfCheckMode::Standard`], when one falls on a key the
/// pass couldn't vouch for exactly (`uncertain`). Never under `Strict`.
fn rechecks(
    mode: SelfCheckMode,
    divergences: &[Divergence],
    uncertain: &super::self_check_groups::Uncertain,
) -> bool {
    mode == SelfCheckMode::Standard && divergences.iter().any(|d| uncertain.covers(d))
}

/// One comparison of a page of `target_table`, with no convergence wait
/// and no re-check: what [`check_page`]'s first pass finds, whatever the
/// ring holds. For the tests of the comparison against the source minus
/// the ring rows the target hasn't applied yet, which hold a page
/// undrained while they compare.
#[cfg(any(test, feature = "internals"))]
pub async fn compare_page(
    pool: &Pool,
    target_table: &str,
    scope: SelfCheckScope,
) -> Result<ComparedPage, SelfCheckError> {
    let def = comparable_definition(pool, target_table).await?;
    let group = group_shape(&def);
    let pk = if group.is_some() || joined(&def) {
        let client = pool.get().await?;
        ddl::identity_key_columns(&**client, &def.target_table).await?
    } else {
        walk_key(pool, &def).await?
    };
    let paused = paused_columns(pool, &def.def).await?;
    let pass = compare_once(
        pool,
        &def,
        group.as_ref(),
        &pk,
        &scope,
        &paused,
        PgLsn::from(0),
    )
    .await?;
    Ok(ComparedPage {
        divergences: pass.divergences,
        next_after: pass.next_after,
        uncertain_all: pass.uncertain.all,
        uncertain_keys: pass.uncertain.keys.into_iter().collect(),
    })
}

/// [`compare_page`]'s result.
#[cfg(any(test, feature = "internals"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComparedPage {
    pub divergences: Vec<Divergence>,
    pub next_after: Option<String>,
    /// Whether the page couldn't vouch for any key exactly: its source's
    /// ring holds an op that touches every key.
    pub uncertain_all: bool,
    /// The keys it couldn't vouch for exactly, sorted.
    pub uncertain_keys: Vec<String>,
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
/// possible on a re-checked page: a hot `(key, column)` caught mid-lag on
/// both passes, with two different transient values, matches here and is
/// reported as stable. See that doc comment for why this is the accepted
/// tradeoff rather than a bug.
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
pub(super) struct ComparePass {
    pub(super) checked_through: PgLsn,
    pub(super) rows_compared: i64,
    pub(super) next_after: Option<String>,
    pub(super) divergences: Vec<Divergence>,
    /// The keys whose divergence may be lag rather than the target's
    /// (`super::self_check_ring`): under [`SelfCheckMode::Standard`] a
    /// divergence on one is reported only if it reproduces on a re-check.
    pub(super) uncertain: super::self_check_groups::Uncertain,
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
    group: Option<&super::group_basis::GroupShape>,
    pk: &[PrimaryKeyColumn],
    scope: &SelfCheckScope,
    paused: &HashSet<String>,
    timeout: Duration,
) -> Result<AwaitOutcome, SelfCheckError> {
    let token = {
        let client = pool.get().await?;
        let token = converge::watermark_token(&**client).await?;
        // On the audited definition's own held keys only. A key a
        // sibling holds is applied here as usual.
        if !caught_up(converge::await_converged_for(&client, token, timeout, def.id).await)? {
            return Ok(AwaitOutcome::NotCaughtUp { attempted: token });
        }
        token
    };

    let pass = compare_once(pool, def, group, pk, scope, paused, token).await?;
    Ok(AwaitOutcome::CaughtUp(pass))
}

/// Reads [`converge::await_converged`]'s result: `Ok(true)` if the target
/// caught up, `Ok(false)` only if the wait ran out of time
/// ([`StagingError::ConvergenceTimeout`]), which is the one failure that
/// means "not caught up yet". Any other error means the wait
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
    group: Option<&super::group_basis::GroupShape>,
    pk: &[PrimaryKeyColumn],
    scope: &SelfCheckScope,
    paused: &HashSet<String>,
    checked_through: PgLsn,
) -> Result<ComparePass, SelfCheckError> {
    if let Some(shape) = group {
        return super::self_check_groups::compare(
            pool,
            def,
            shape,
            pk,
            scope,
            paused,
            checked_through,
        )
        .await;
    }
    if joined(def) {
        return super::self_check_joins::compare(pool, def, pk, scope, paused, checked_through)
            .await;
    }
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
    // (ADR-0013, comparison under one
    // snapshot), and it trims both pages to the same bound.
    let client = pool.get().await?;
    let mut keyset = Keyset::resolve(&client, pk).await?;
    keyset.set_cursor(scope.after.as_deref())?;
    let sql = page_sql(def, pk, &keyset, &comparable)?;
    let rows = client.query(sql.as_str(), &[&scope.limit]).await?;

    let mut recomputed = Page::new();
    let mut persisted = Page::new();
    let mut page_end: Option<String> = None;
    let mut uncertain = super::self_check_groups::Uncertain::default();
    for row in &rows {
        let side = row.get::<_, i32>(0);
        page_end = row.get(1);
        let key: String = row.get(2);
        match side {
            3 => {
                uncertain.keys.insert(key);
            }
            4 => uncertain.all = true,
            _ => {
                let values: Vec<Option<String>> = row.get(3);
                if side == 1 {
                    recomputed.insert(key, values);
                } else {
                    persisted.insert(key, values);
                }
            }
        }
    }
    let columns: Vec<&str> = comparable.iter().map(|f| f.name.as_str()).collect();
    let diff = diff_page(&columns, recomputed, persisted, page_end);

    Ok(ComparePass {
        checked_through,
        rows_compared: diff.rows_compared,
        next_after: diff.next_after,
        divergences: diff.divergences,
        uncertain,
    })
}

/// The statement [`compare_once`] runs: one keyset page of the recompute
/// over `def`'s source and of the persisted target, each `LIMIT`ed
/// separately, both ordered and bounded by the key's raw columns
/// ([`Keyset`]), and both trimmed to the same end in SQL.
///
/// `$1` is [`SelfCheckScope::limit`]; the cursor ([`SelfCheckScope::after`]) is
/// in the statement ([`Keyset::set_cursor`]). Each row is `(recomputed, page_end, key,
/// compared values…)`: `recomputed` says which side the row is from, and
/// `page_end` is the page's bound, the same on every row.
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
/// such bound is its end ([`Keyset::ranked`]). Keys past it are dropped from
/// both sides and left for the next page, which starts after `page_end`. A
/// page where neither side filled the limit reached the end of the keyspace:
/// `page_end` is null.
fn page_sql(
    def: &Definition,
    pk: &[PrimaryKeyColumn],
    keyset: &Keyset,
    comparable: &[&FieldDef],
) -> Result<String, SelfCheckError> {
    // `pk_key_sql_expr` is the source/target's shared key-contract text at
    // whatever arity the key has: a bare `{col}::text` at arity
    // 1. It is the key's identity, the text a divergence is reported by; the
    // page is ordered and bounded by the raw columns.
    let key = ddl::pk_key_sql_expr(keyset.columns(), None);
    let refs = keyset.refs(None);
    let carried = keyset.select_carried(&refs);
    let order = keyset.order_by(&refs);
    let source_ident = ddl::qualified_source_table(&def.source_table);
    let target_ident = ddl::qualified_target_table_ident(&def.target_table);

    let mut recomputed = Vec::with_capacity(comparable.len());
    let mut persisted = Vec::with_capacity(comparable.len());
    let mut inliner = FieldInliner::new(&def.target_table, &def.def.fields);
    for field in comparable {
        let leaf = inliner.render(field)?;
        recomputed.push(format!("({leaf})::text"));
        persisted.push(format!("{}::text", quote_ident(&field.name)));
    }
    // The recompute side reads the source as the target has applied it
    // (`super::self_check_ring`): a key with an unapplied change is its
    // earliest such change's old image (no row for an insert), any other key
    // its live row.
    let ring = Ring {
        src_table: &def.source_table,
        source_row: source_ident.clone(),
        source_pk: pk,
        transform_id: def.id,
        tag: "s",
    };
    let pend = ring.pend_cte();
    let expected = format!(
        "(select s.* from {source_ident} s \
          where not exists (select 1 from {pend} p where p.__k = {skey}) \
          union all \
          select r.* from {pend} p \
          cross join lateral jsonb_populate_record(null::{source_ident}, p.__i) r \
          where jsonb_typeof(p.__i) = 'object') e",
        skey = ddl::pk_key_sql_expr(pk, Some("s")),
    );
    let side = |table: &str, values: &[String]| {
        keyset.side(&refs, |cond| {
            format!(
                "select {key} as __k{carried}, array[{}]::text[] as __v from {table} \
                 where {cond} order by {order} limit $1",
                values.join(", ")
            )
        })
    };
    Ok(keyset.ranked(
        &format!(
            "{}recomputed as materialized ({}), persisted as materialized ({}), ",
            ring.one_to_one_ctes(&def.target_table),
            side(&expected, &recomputed),
            side(&target_ident, &persisted)
        ),
        &[(1, "recomputed"), (2, "persisted")],
        &[1, 2],
        &format!(
            " union all select 3, page.page_end, u.__k, null::text[] \
                from page, ({}) u \
              union all select 4, page.page_end, '', null::text[] from page where {}",
            ring.uncertain_keys_sql(),
            ring.global_sql()
        ),
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
    explain_page_with(pool, target_table, after, limit, false).await
}

/// [`explain_page`] after running the statement (`explain analyze`): the
/// plan also says which scans never executed. For tests of the plan's shape.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_page_executed(
    pool: &Pool,
    target_table: &str,
    after: Option<&str>,
    limit: i64,
) -> Result<String, SelfCheckError> {
    explain_page_with(pool, target_table, after, limit, true).await
}

/// `explain`, or `explain analyze` without timings, for the plan tests.
#[cfg(any(test, feature = "internals"))]
pub(super) fn explain_verb(executed: bool) -> &'static str {
    if executed {
        "explain (analyze, costs off, timing off, summary off)"
    } else {
        "explain"
    }
}

#[cfg(any(test, feature = "internals"))]
async fn explain_page_with(
    pool: &Pool,
    target_table: &str,
    after: Option<&str>,
    limit: i64,
    executed: bool,
) -> Result<String, SelfCheckError> {
    let def = catalog::definition_by_target(pool, target_table)
        .await?
        .ok_or_else(|| SelfCheckError::TargetNotFound(target_table.to_string()))?;
    if let Some(shape) = group_shape(&def) {
        let identity = {
            let client = pool.get().await?;
            ddl::identity_key_columns(&**client, &def.target_table).await?
        };
        return super::self_check_groups::explain(
            pool, &def, &shape, &identity, after, limit, executed,
        )
        .await;
    }
    let pk = walk_key(pool, &def).await?;
    let comparable: Vec<&FieldDef> = def.def.fields.iter().collect();
    let mut client = pool.get().await?;
    let mut keyset = Keyset::resolve(&client, &pk).await?;
    keyset.set_cursor(after)?;
    let sql = page_sql(&def, &pk, &keyset, &comparable)?;
    let txn = client.transaction().await?;
    txn.batch_execute("set local enable_seqscan to off").await?;
    let rows = txn
        .query(
            format!("{} {sql}", explain_verb(executed)).as_str(),
            &[&limit],
        )
        .await?;
    txn.rollback().await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// [`explain_page`] of a `JOIN` target's hidden part `part` (`p`, `m`, `j`
/// or `t`) after the key text `after`. For tests of the plan's shape.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_join_page(
    pool: &Pool,
    target_table: &str,
    part: &str,
    after: Option<&str>,
    limit: i64,
) -> Result<String, SelfCheckError> {
    let def = catalog::definition_by_target(pool, target_table)
        .await?
        .ok_or_else(|| SelfCheckError::TargetNotFound(target_table.to_string()))?;
    let identity = {
        let client = pool.get().await?;
        ddl::identity_key_columns(&**client, &def.target_table).await?
    };
    super::self_check_joins::explain(pool, &def, &identity, part, after, limit, false).await
}

/// [`explain_join_page`] after running the statement ([`explain_page_executed`]).
#[cfg(any(test, feature = "internals"))]
pub async fn explain_join_page_executed(
    pool: &Pool,
    target_table: &str,
    part: &str,
    after: Option<&str>,
    limit: i64,
) -> Result<String, SelfCheckError> {
    let def = catalog::definition_by_target(pool, target_table)
        .await?
        .ok_or_else(|| SelfCheckError::TargetNotFound(target_table.to_string()))?;
    let identity = {
        let client = pool.get().await?;
        ddl::identity_key_columns(&**client, &def.target_table).await?
    };
    super::self_check_joins::explain(pool, &def, &identity, part, after, limit, true).await
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

/// Every column `self_check` must currently exclude from comparison for
/// `transform_table` — deliberately a fresh copy of the identical one-line
/// query [`crate::staging::quarantine::paused_columns_for`] already runs
/// (that helper is `pub(super)`, scoped to `staging::quarantine`'s own
/// callers, and `defs::backfill::paused_columns_for` already duplicates it
/// too, for the same layering reason: see that pair's own doc comments).
///
/// Closed over `def`'s alias readers, as that original is.
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
        crate::defs::alias_readers::AliasReaders::of(def).close(&mut paused);
    }
    Ok(paused)
}

/// The columns `def`'s target table is expected to physically have (its
/// primary key, plus every calculated field) vs. what it actually has —
/// [`Divergence::MissingColumn`]/[`Divergence::ExtraColumn`], ADR-0013's
/// schema-drift divergence kind. Cheap (one `pg_attribute` read), checked
/// once per `self_check` call rather than per comparison pass — schema
/// drift isn't something a convergence race could produce or resolve, so it
/// doesn't participate in the re-check dance `self_check` runs for
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

/// Node budget for one page's field inlining ([`FieldInliner`]). Define
/// refuses a definition whose inlined fields exceed its own, equal budget
/// (`ValidationError::FieldExpansionTooLarge`), so this trips only on a
/// definition define never admitted.
const MAX_INLINED_NODES: usize = 100_000;

/// Bound on the nesting of one inlined expression. Define bounds an expression
/// at `MAX_EXPRESSION_DEPTH` including the fields it reads; twice that leaves
/// headroom and keeps the recursion shallow whatever the catalog holds.
const MAX_INLINED_DEPTH: usize = 2 * crate::defs::MAX_EXPRESSION_DEPTH;

/// Renders each calculated field of a 1-1 definition against the source row,
/// reading another calculated field by name as that field's own expression
/// (inlined), as `defs::backfill::substitute_all_fields` does for the write
/// path. Written fresh here on [`render_leaf`]'s grammar, sharing nothing
/// with the write path (ADR-0013): a name that is not another field is a
/// source column, and a field's own name inside its expression is the source
/// column of that name.
struct FieldInliner<'a> {
    target: &'a str,
    fields: HashMap<&'a str, &'a Expr>,
    /// Rendered SQL of the fields already inlined, with its node count.
    memo: HashMap<&'a str, (String, usize)>,
    nodes: usize,
}

impl<'a> FieldInliner<'a> {
    fn new(target: &'a str, fields: &'a [FieldDef]) -> Self {
        FieldInliner {
            target,
            fields: fields.iter().map(|f| (f.name.as_str(), &f.expr)).collect(),
            memo: HashMap::new(),
            nodes: 0,
        }
    }

    fn too_large(&self) -> SelfCheckError {
        SelfCheckError::UnsupportedExpr {
            target: self.target.to_string(),
            detail: "the calculated fields read each other so deeply or so many times that \
                     inlining them into one expression exceeds the recompute's size budget"
                .to_string(),
        }
    }

    fn render(&mut self, field: &'a FieldDef) -> Result<String, SelfCheckError> {
        self.expr(&field.expr, &field.name, 0)
    }

    fn charge(&mut self, cost: usize) -> Result<(), SelfCheckError> {
        self.nodes = self.nodes.saturating_add(cost);
        if self.nodes > MAX_INLINED_NODES {
            return Err(self.too_large());
        }
        Ok(())
    }

    fn expr(
        &mut self,
        expr: &'a Expr,
        owner: &str,
        depth: usize,
    ) -> Result<String, SelfCheckError> {
        if depth > MAX_INLINED_DEPTH {
            return Err(self.too_large());
        }
        if let Expr::Column(name) = expr
            && name != owner
            && let Some((key, body)) = self.fields.get_key_value(name.as_str())
        {
            // The read itself is replaced, so it costs nothing: the count is
            // the inlined expressions' size, the quantity define's equal
            // budget bounds.
            let (key, body) = (*key, *body);
            if let Some((done, nodes)) = self.memo.get(key) {
                // A memo hit still materializes its text: charge its nodes.
                let (done, nodes) = (done.clone(), *nodes);
                self.charge(nodes)?;
                return Ok(done);
            }
            let before = self.nodes;
            let rendered = self.expr(body, key, depth + 1)?;
            // Its own nodes, memo hits inside it included.
            self.memo
                .insert(key, (rendered.clone(), self.nodes - before));
            return Ok(rendered);
        }
        self.charge(1)?;
        Ok(match expr {
            Expr::BinaryOp { op, lhs, rhs } => {
                let symbol = registry::operator_spec(*op).symbol;
                let l = self.expr(lhs, owner, depth + 1)?;
                let r = self.expr(rhs, owner, depth + 1)?;
                format!("({l} {symbol} {r})")
            }
            Expr::Negate(operand) => {
                format!("(-({}))", self.expr(operand, owner, depth + 1)?)
            }
            Expr::Cast { expr, target } => {
                format!("cast({} as {target})", self.expr(expr, owner, depth + 1)?)
            }
            Expr::Not(operand) => {
                format!("(not {})", self.expr(operand, owner, depth + 1)?)
            }
            Expr::IsNull { expr, negated } => {
                let test = if *negated { "is not null" } else { "is null" };
                format!("({} {test})", self.expr(expr, owner, depth + 1)?)
            }
            Expr::Case {
                branches,
                otherwise,
            } => {
                let mut sql = String::from("(case");
                for (condition, result) in branches {
                    let c = self.expr(condition, owner, depth + 1)?;
                    let r = self.expr(result, owner, depth + 1)?;
                    sql.push_str(&format!(" when {c} then {r}"));
                }
                if let Some(otherwise) = otherwise {
                    let o = self.expr(otherwise, owner, depth + 1)?;
                    sql.push_str(&format!(" else {o}"));
                }
                sql.push_str(" end)");
                sql
            }
            Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
                "count(*)".to_string()
            }
            Expr::FunctionCall { name, args } if as_extract(name, args).is_some() => {
                let (field, operand) = as_extract(name, args).expect("checked");
                format!(
                    "extract({field} from {})",
                    self.expr(operand, owner, depth + 1)?
                )
            }
            Expr::FunctionCall { name, args } => {
                let mut rendered = Vec::with_capacity(args.len());
                for (i, arg) in args.iter().enumerate() {
                    let text = match arg {
                        Expr::NumberLiteral(_) => render_call_arg(name, i, arg, render_leaf),
                        _ => self.expr(arg, owner, depth + 1)?,
                    };
                    rendered.push(text);
                }
                format!("{}({})", name.to_lowercase(), rendered.join(", "))
            }
            leaf => render_leaf(leaf),
        })
    }
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
pub(super) fn render_leaf(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) | Expr::SourceColumn { column: name, .. } => quote_ident(name),
        Expr::NumberLiteral(text) => number_literal(text),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::TypedLiteral { value_type, text } => typed_literal::render_sql(*value_type, text),
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = registry::operator_spec(*op).symbol;
            format!("({} {symbol} {})", render_leaf(lhs), render_leaf(rhs))
        }
        Expr::Negate(operand) => format!("(-({}))", render_leaf(operand)),
        Expr::Cast { expr, target } => format!("cast({} as {target})", render_leaf(expr)),
        Expr::Not(operand) => format!("(not {})", render_leaf(operand)),
        Expr::IsNull { expr, negated } => {
            let test = if *negated { "is not null" } else { "is null" };
            format!("({} {test})", render_leaf(expr))
        }
        Expr::Case {
            branches,
            otherwise,
        } => {
            let mut sql = String::from("(case");
            for (condition, result) in branches {
                sql.push_str(&format!(
                    " when {} then {}",
                    render_leaf(condition),
                    render_leaf(result)
                ));
            }
            if let Some(otherwise) = otherwise {
                sql.push_str(&format!(" else {}", render_leaf(otherwise)));
            }
            sql.push_str(" end)");
            sql
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } if as_extract(name, args).is_some() => {
            let (field, operand) = as_extract(name, args).expect("checked");
            format!("extract({field} from {})", render_leaf(operand))
        }
        Expr::FunctionCall { name, args } => {
            let rendered: Vec<String> = args
                .iter()
                .enumerate()
                .map(|(i, arg)| render_call_arg(name, i, arg, render_leaf))
                .collect();
            format!("{}({})", name.to_lowercase(), rendered.join(", "))
        }
    }
}

/// A number literal as SQL, typed as the write path types it
/// ([`crate::defs::oracle::number_literal_sql`]: an integral one `integer`
/// when it fits, else `bigint`, else `numeric`, and a fractional one
/// `numeric`, as Postgres types the bare literal). A date takes
/// `date + integer` and no other numeric type, and `integer` arithmetic
/// raises where it overflows, as the field it audits does.
fn number_literal(text: &str) -> String {
    crate::defs::oracle::number_literal_sql(text)
}

/// One argument of a call, rendered: the scale of `ROUND(a, n)` is an
/// `integer` (Postgres has no `round(numeric, numeric)`), anything else is
/// rendered by `render`.
pub(super) fn render_call_arg(
    function: &str,
    index: usize,
    arg: &Expr,
    render: impl Fn(&Expr) -> String,
) -> String {
    match arg {
        Expr::NumberLiteral(text)
            if registry::INTEGER_LITERAL_ARGS
                .contains(&(function.to_ascii_uppercase().as_str(), index)) =>
        {
            format!("({text})::integer")
        }
        _ => render(arg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::Operator;

    fn field(name: &str, expr: Expr) -> FieldDef {
        FieldDef {
            name: name.to_string(),
            expr,
        }
    }

    fn col(name: &str) -> Expr {
        Expr::Column(name.to_string())
    }

    fn sum(lhs: Expr, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            op: Operator::Add,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }
    }

    #[test]
    fn inlining_replaces_a_field_read_and_keeps_a_fields_own_name_a_source_column() {
        let one = || Expr::NumberLiteral("1".into());
        let fields = vec![
            field("x", sum(col("x"), one())),
            field("b", sum(col("x"), col("x"))),
        ];
        let mut inliner = FieldInliner::new("t", &fields);
        // `x` reads the source column `x`; `b` reads the field `x`.
        assert_eq!(
            inliner.render(&fields[1]).unwrap(),
            "((\"x\" + 1::integer) + (\"x\" + 1::integer))"
        );
        assert_eq!(inliner.render(&fields[0]).unwrap(), "(\"x\" + 1::integer)");
    }

    #[test]
    fn inlining_reaches_a_field_read_under_negate_cast_and_a_call() {
        use crate::defs::ast::ValueType;
        let fields = vec![
            field("x", col("c")),
            field("n", Expr::Negate(Box::new(col("x")))),
            field(
                "k",
                Expr::Cast {
                    expr: Box::new(col("x")),
                    target: ValueType::Numeric,
                },
            ),
            field(
                "r",
                Expr::FunctionCall {
                    name: "ROUND".into(),
                    args: vec![col("x"), Expr::NumberLiteral("2".into())],
                },
            ),
        ];
        let mut inliner = FieldInliner::new("t", &fields);
        assert_eq!(inliner.render(&fields[1]).unwrap(), "(-(\"c\"))");
        assert_eq!(
            inliner.render(&fields[2]).unwrap(),
            "cast(\"c\" as numeric)"
        );
        assert_eq!(
            inliner.render(&fields[3]).unwrap(),
            "round(\"c\", (2)::integer)"
        );
    }

    #[test]
    fn inlining_a_doubling_chain_past_the_budget_fails_rather_than_expanding() {
        let mut fields = vec![field("f0", col("c"))];
        for k in 1..40 {
            let prev = format!("f{}", k - 1);
            fields.push(field(&format!("f{k}"), sum(col(&prev), col(&prev))));
        }
        let mut inliner = FieldInliner::new("t", &fields);
        let err = inliner.render(&fields[39]).unwrap_err();
        assert!(
            matches!(err, SelfCheckError::UnsupportedExpr { .. }),
            "{err:?}"
        );
    }

    /// The largest definition of `shape(n)` define admits (`n` grown until
    /// validation refuses it).
    fn largest_admitted(shape: impl Fn(usize) -> Vec<String>) -> crate::defs::TransformDef {
        let columns = HashMap::from([("c".to_string(), crate::defs::ValueType::Numeric)]);
        let mut admitted = None;
        for n in 1.. {
            let text = format!("TRANSFORM t FROM s SELECT {}", shape(n).join(", "));
            match crate::defs::parse(&text) {
                Ok(def) if crate::defs::validate::validate(&def, &columns).is_ok() => {
                    admitted = Some(def);
                }
                _ => break,
            }
        }
        admitted.expect("the smallest shape is admitted")
    }

    #[test]
    fn every_field_of_the_largest_definitions_define_admits_inlines() {
        type Shape = fn(usize) -> Vec<String>;
        // Each field reads the previous one twice: the node budget binds.
        let doubling: Shape = |n| {
            let mut fields = vec!["c AS f0".to_string()];
            for k in 1..n {
                fields.push(format!("f{p} + f{p} AS f{k}", p = k - 1));
            }
            fields
        };
        // One field reads another many times.
        let fan_out: Shape = |n| {
            vec![
                "c + c + c + c + c + c + c + c AS f0".to_string(),
                format!("COALESCE({}) AS g", vec!["f0"; n * 100].join(", ")),
            ]
        };
        // Bare alias reads, and reads one level down: the depth bound binds.
        let aliases: Shape = |n| {
            let mut fields = vec!["c AS f0".to_string()];
            for k in 1..n {
                fields.push(format!("f{} AS f{k}", k - 1));
            }
            fields
        };
        let plus_one: Shape = |n| {
            let mut fields = vec!["c AS f0".to_string()];
            for k in 1..n {
                fields.push(format!("COALESCE(f{}, 1) AS f{k}", k - 1));
            }
            fields
        };
        // On a 2 MiB stack, the default for a spawned thread in debug builds.
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                for shape in [doubling, fan_out, aliases, plus_one] {
                    let def = largest_admitted(shape);
                    let mut inliner = FieldInliner::new("t", &def.fields);
                    for field in &def.fields {
                        if let Err(err) = inliner.render(field) {
                            panic!("{} fields: {err:?}", def.fields.len());
                        }
                    }
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn inlining_a_cyclic_catalog_fails() {
        // Only a catalog define never admitted can hold a cycle.
        let fields = vec![field("a", col("b")), field("b", col("a"))];
        let mut inliner = FieldInliner::new("t", &fields);
        assert!(inliner.render(&fields[0]).is_err());
        let fields = vec![
            field("a", sum(col("b"), col("b"))),
            field("b", sum(col("a"), col("a"))),
        ];
        let mut inliner = FieldInliner::new("t", &fields);
        assert!(inliner.render(&fields[0]).is_err());
    }

    #[test]
    fn inlining_a_chain_deeper_than_any_define_admits_fails() {
        let mut fields = vec![field("f0", col("c"))];
        for k in 1..400 {
            fields.push(field(&format!("f{k}"), col(&format!("f{}", k - 1))));
        }
        let mut inliner = FieldInliner::new("t", &fields);
        assert!(inliner.render(&fields[399]).is_err());
    }

    #[test]
    fn render_leaf_renders_a_column_reference() {
        assert_eq!(render_leaf(&Expr::Column("price".to_string())), "\"price\"");
    }

    #[test]
    fn render_leaf_renders_a_number_literal_with_a_numeric_cast() {
        assert_eq!(
            render_leaf(&Expr::NumberLiteral("1.50".to_string())),
            "1.50::numeric"
        );
    }

    #[test]
    fn render_leaf_renders_a_string_literal_escaping_embedded_quotes() {
        assert_eq!(
            render_leaf(&Expr::StringLiteral("o'clock".to_string())),
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
        assert_eq!(render_leaf(&expr), "(\"a\" + \"b\")");
    }

    #[test]
    fn render_leaf_renders_greater_than() {
        let expr = Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(Expr::Column("a".to_string())),
            rhs: Box::new(Expr::NumberLiteral("0".to_string())),
        };
        assert_eq!(render_leaf(&expr), "(\"a\" > 0::integer)");
    }

    #[test]
    fn render_leaf_renders_count_star() {
        let expr = Expr::FunctionCall {
            name: "COUNT".to_string(),
            args: Vec::new(),
        };
        assert_eq!(render_leaf(&expr), "count(*)");
    }

    /// A bare literal is typed as Postgres types it, so `date + 1` and
    /// `d + 2 * 3` audit as the field computes them.
    #[test]
    fn render_leaf_types_a_number_literal_as_postgres_does() {
        for (text, expected) in [
            ("1", "1::integer"),
            ("2147483647", "2147483647::integer"),
            ("2147483648", "2147483648::bigint"),
            ("9223372036854775807", "9223372036854775807::bigint"),
            ("9223372036854775808", "9223372036854775808::numeric"),
            ("1.5", "1.5::numeric"),
            ("-1", "(-1)::integer"),
            ("-2147483648", "(-2147483648)::integer"),
            ("-2147483649", "(-2147483649)::bigint"),
            ("-0.5", "(-0.5)::numeric"),
        ] {
            assert_eq!(
                render_leaf(&Expr::NumberLiteral(text.to_string())),
                expected,
                "{text}"
            );
        }
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
        assert_eq!(render_leaf(&expr), "strpos(\"name\", 'x'::text)");
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

    /// Under `Standard`, a page re-checks only when a divergence falls
    /// on a key it couldn't vouch for exactly; a divergence on any other key,
    /// or on the schema, is reported from the one pass. A page whose source has
    /// an unapplied op that touches every key vouches for none, and `Strict`
    /// never re-checks.
    #[test]
    fn standard_rechecks_only_a_divergence_on_an_uncertain_key() {
        use super::super::self_check_groups::Uncertain;
        let exact = Uncertain::default();
        let all = Uncertain {
            all: true,
            keys: BTreeSet::new(),
        };
        let one = Uncertain {
            all: false,
            keys: BTreeSet::from(["2".to_string()]),
        };
        let column = Divergence::MissingColumn {
            column: "n".to_string(),
        };
        let on_1 = [cell("1", "5", "6"), missing("1"), column.clone()];
        assert!(!rechecks(SelfCheckMode::Standard, &on_1, &exact));
        assert!(!rechecks(SelfCheckMode::Standard, &on_1, &one));
        assert!(rechecks(
            SelfCheckMode::Standard,
            &[cell("2", "5", "6")],
            &one
        ));
        assert!(rechecks(SelfCheckMode::Standard, &[missing("2")], &one));
        assert!(rechecks(
            SelfCheckMode::Standard,
            std::slice::from_ref(&column),
            &all
        ));
        assert!(!rechecks(
            SelfCheckMode::Strict,
            &[cell("2", "5", "6")],
            &all
        ));
        assert!(!rechecks(SelfCheckMode::Standard, &[], &all));
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

    /// Any other wait failure means the audit couldn't run, so it
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
