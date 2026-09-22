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
//! `staging::apply_aggregate`'s incremental aggregate apply), not just by
//! tests. Auditing backfilled rows with a renderer the backfill path itself
//! uses would check the write path against itself: a rendering bug would
//! agree with itself, and the audit would pass on a wrong answer. This
//! renderer shares no code with `defs::oracle`'s, or with
//! `generative::oracle`'s own independent SQL renderer — the generative
//! suite's cross-check test (`generative/tests/self_check_cross_check.rs`)
//! is what keeps the two from silently drifting apart.
//!
//! # Quiescence: "diverged" vs "not yet caught up"
//!
//! A single snapshot is not sufficient under live load: a correctly-working
//! target legitimately lags its source by CDC apply latency. [`self_check`]
//! takes a watermark token, awaits convergence through it (bounded by a
//! timeout — on timeout it reports [`SelfCheckOutcome::NotCaughtUp`], never a
//! divergence), then reads the target and runs the recompute inside a single
//! `REPEATABLE READ` transaction ([`compare_once`]).
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

use std::collections::{BTreeSet, HashSet};
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

use super::converge;
use super::error::StagingError;

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
    /// connection borrowed from the pool, then [`compare_once`] opens its
    /// `REPEATABLE READ` transaction on a separate `pool.get()` call — which
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
    pub checked_through: PgLsn,
    /// How many distinct keys this call actually compared — the union of
    /// every key seen on either side of the comparison, within `scope`'s
    /// page. Zero for a report whose outcome is
    /// [`SelfCheckOutcome::NotCaughtUp`] (the comparison never ran).
    pub rows_compared: i64,
    /// The keyset cursor a following call should pass as
    /// [`SelfCheckScope::after`] to continue past this page — the key this
    /// page's bound actually ended at, which is the *lower* of the two
    /// sides' last keys whenever either side hit
    /// [`SelfCheckScope::limit`] (keys past it fell off one side's page and
    /// are deliberately left for the next call rather than diffed against a
    /// truncated counterpart). `None` means neither side hit the limit, so
    /// this page reached the end of the target's keyspace and a following
    /// call would have nothing to do.
    pub next_after: Option<String>,
    pub outcome: SelfCheckOutcome,
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

/// One divergence [`self_check`] found, per ADR-0013's four kinds.
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

    // Issue #121: this audit now keys on the source's full (possibly
    // composite) primary key, through the shared key-contract text
    // (`ddl::pk_key_sql_expr`) rather than a single named column.
    let pk = ddl::source_primary_key(pool, &def.source_table).await?;

    let schema_divergences = check_schema(pool, &def, &pk).await?;
    let paused = paused_columns(pool, &def.def.target).await?;

    let pass1 = match await_then_compare(pool, &def, &pk, &scope, &paused, timeout).await? {
        AwaitOutcome::NotCaughtUp { attempted } => {
            return Ok(SelfCheckReport {
                target: target_table.to_string(),
                checked_through: attempted,
                rows_compared: 0,
                next_after: scope.after.clone(),
                outcome: SelfCheckOutcome::NotCaughtUp,
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
        });
    }

    if mode == SelfCheckMode::Strict {
        return Ok(SelfCheckReport {
            target: target_table.to_string(),
            checked_through: pass1.checked_through,
            rows_compared: pass1.rows_compared,
            next_after: pass1.next_after,
            outcome: SelfCheckOutcome::Diverged(divergences),
        });
    }

    // ADR-0013: "a divergence is reported as real only if it survives a
    // re-check after a fresh await — a genuine divergence is stable; a
    // convergence race resolves." Re-run the exact same bounded comparison
    // from scratch, behind a brand-new watermark token/await.
    let pass2 = match await_then_compare(pool, &def, &pk, &scope, &paused, timeout).await? {
        AwaitOutcome::NotCaughtUp { attempted } => {
            return Ok(SelfCheckReport {
                target: target_table.to_string(),
                checked_through: attempted,
                rows_compared: 0,
                next_after: scope.after.clone(),
                outcome: SelfCheckOutcome::NotCaughtUp,
            });
        }
        AwaitOutcome::CaughtUp(pass) => pass,
    };

    let identities_seen_first: BTreeSet<DivergenceIdentity> =
        divergences.iter().map(DivergenceIdentity::of).collect();
    let stable: Vec<Divergence> = {
        let mut schema_again = check_schema(pool, &def, &pk).await?;
        schema_again.extend(pass2.divergences);
        schema_again
    }
    .into_iter()
    .filter(|d| identities_seen_first.contains(&DivergenceIdentity::of(d)))
    .collect();

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
    })
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
        if converge::await_converged(&**client, token, timeout)
            .await
            .is_err()
        {
            return Ok(AwaitOutcome::NotCaughtUp { attempted: token });
        }
        token
    };

    let pass = compare_once(pool, def, pk, scope, paused, token).await?;
    Ok(AwaitOutcome::CaughtUp(pass))
}

/// The bounded, single-transaction comparison itself (ADR-0013: "reads the
/// target and runs the recompute inside a single `REPEATABLE READ`
/// transaction"). Builds two independently-rendered `SELECT`s — one against
/// `def`'s source (via [`render_leaf`]), one against the persisted target —
/// each bounded to `scope`'s keyset page, runs both in the same transaction,
/// and diffs the results in Rust.
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

    // Issue #121: `pk_key_expr` is the source/target's shared key-contract
    // text at whatever arity `pk` has (a bare `{col}::text` at arity 1,
    // byte-identical to before this issue). Both queries below page and order
    // by this same text on both sides, so — while that's not necessarily the
    // key's own *typed* order at arity > 1 — it's the same order on both
    // sides, which is all a divergence detector that only ever compares the
    // two sides key-by-key actually needs (this was already true of a plain
    // `pk::text` order at arity 1, e.g. a numeric key sorting lexicographically
    // rather than numerically).
    let pk_key_expr = ddl::pk_key_sql_expr(pk, None);
    let source_ident = ddl::qualified_source_table(&def.source_table);
    let target_ident = ddl::qualified_target_table_ident(&def.target_table);

    let mut recompute_select = vec![pk_key_expr.clone()];
    for field in &comparable {
        let leaf = render_leaf(&field.expr).map_err(|detail| SelfCheckError::UnsupportedExpr {
            target: def.def.target.clone(),
            detail,
        })?;
        recompute_select.push(format!("({leaf})::text"));
    }
    let recompute_sql = format!(
        "select {} from {source_ident} where ($1::text is null or {pk_key_expr} > $1) \
         order by {pk_key_expr} limit $2",
        recompute_select.join(", ")
    );

    let mut persisted_select = vec![pk_key_expr.clone()];
    for field in &comparable {
        persisted_select.push(format!("{}::text", quote_ident(&field.name)));
    }
    let persisted_sql = format!(
        "select {} from {target_ident} where ($1::text is null or {pk_key_expr} > $1) \
         order by {pk_key_expr} limit $2",
        persisted_select.join(", ")
    );

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    // Must be the transaction's first statement (Postgres requires `SET
    // TRANSACTION` before any other command) — only a REPEATABLE READ
    // transaction holds its snapshot steady across the recompute and
    // persisted-read queries below, matching
    // `intake::publication::initial_snapshot_handshake`'s identical
    // first-statement requirement.
    txn.execute("set transaction isolation level repeatable read", &[])
        .await?;

    let after: Option<&str> = scope.after.as_deref();
    let recompute_rows = txn
        .query(recompute_sql.as_str(), &[&after, &scope.limit])
        .await?;
    let persisted_rows = txn
        .query(persisted_sql.as_str(), &[&after, &scope.limit])
        .await?;
    txn.commit().await?;

    let mut recomputed: std::collections::BTreeMap<String, Vec<Option<String>>> =
        std::collections::BTreeMap::new();
    for row in &recompute_rows {
        let key: String = row.get(0);
        let values: Vec<Option<String>> = (0..comparable.len()).map(|i| row.get(i + 1)).collect();
        recomputed.insert(key, values);
    }
    let mut persisted: std::collections::BTreeMap<String, Vec<Option<String>>> =
        std::collections::BTreeMap::new();
    for row in &persisted_rows {
        let key: String = row.get(0);
        let values: Vec<Option<String>> = (0..comparable.len()).map(|i| row.get(i + 1)).collect();
        persisted.insert(key, values);
    }

    // The two `LIMIT`ed reads are keyset-scoped independently, so whenever a
    // divergence makes the two sides' key sets differ, their pages end at
    // *different* keys: a target missing one row inside the page pulls one
    // extra key in on the persisted side that the recompute side's own limit
    // cut off. Diffing the raw pages would then report that trailing key as a
    // `ExtraRow`/`MissingRow` purely because it fell off the other side's
    // page — a deterministic false divergence (it reproduces on the re-check
    // pass, so the ADR's re-check can't filter it), and `next_after` would
    // skip past it, never comparing it honestly on the following page.
    //
    // So: whichever side(s) actually hit the limit bound this page, and the
    // *lowest* such bound is the page's real end. Keys past it belong to the
    // next page and are dropped from both sides here; `next_after` is that
    // boundary, so the following call picks them up. A page where neither
    // side hit the limit reached the end of the keyspace — no boundary, and
    // `next_after` is `None`.
    let recompute_bound = (recompute_rows.len() as i64 >= scope.limit)
        .then(|| recomputed.keys().next_back().cloned())
        .flatten();
    let persisted_bound = (persisted_rows.len() as i64 >= scope.limit)
        .then(|| persisted.keys().next_back().cloned())
        .flatten();
    let page_end = match (recompute_bound, persisted_bound) {
        (Some(r), Some(p)) => Some(r.min(p)),
        (Some(r), None) => Some(r),
        (None, Some(p)) => Some(p),
        (None, None) => None,
    };
    if let Some(page_end) = &page_end {
        recomputed.retain(|key, _| key <= page_end);
        persisted.retain(|key, _| key <= page_end);
    }

    let mut divergences = Vec::new();
    for (key, r_values) in &recomputed {
        match persisted.get(key) {
            None => divergences.push(Divergence::MissingRow { key: key.clone() }),
            Some(p_values) => {
                for (field, (r, p)) in comparable.iter().zip(r_values.iter().zip(p_values.iter())) {
                    if r != p {
                        divergences.push(Divergence::Cell {
                            key: key.clone(),
                            column: field.name.clone(),
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
    let rows_compared = all_keys.len() as i64;

    Ok(ComparePass {
        checked_through,
        rows_compared,
        next_after: page_end,
        divergences,
    })
}

/// Every column [`self_check`] must currently exclude from comparison for
/// `transform_table` — deliberately a fresh copy of the identical one-line
/// query [`crate::staging::quarantine::paused_columns_for`] already runs
/// (that helper is `pub(super)`, scoped to `staging::quarantine`'s own
/// callers, and `defs::backfill::paused_columns_for` already duplicates it
/// too, for the same layering reason: see that pair's own doc comments).
async fn paused_columns(
    pool: &Pool,
    transform_table: &str,
) -> Result<HashSet<String>, SelfCheckError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select column_name from column_status where transform_table = $1",
            &[&transform_table],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
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
            &[&def.target_table],
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
}
