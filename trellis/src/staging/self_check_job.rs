//! `self_check` as a background job (#1023, part of #599).
//!
//! The comparison of a whole target against a fresh recompute can take far
//! longer than a public call may (30 seconds, `docs/decisions/0008-public-api-design.md`
//! decision 6), so no public call runs it. [`start`] registers a row in
//! `self_check_jobs` and returns it; a drain worker
//! (`ClientOptions::application_threads`, in any process of the fleet) claims
//! the row and runs the comparison one keyset page per pass of its loop
//! ([`work_once`]); the caller reads the row back by its id ([`get`]).
//! This is the shape of a build, which `apply` registers and a worker
//! finishes, and like a build it needs a worker somewhere: with none, a job
//! stays `queued`.
//!
//! # Pages
//!
//! A page is [`super::self_check::audit`] over [`WorkerOptions::page_keys`]
//! keys, exactly the comparison the old synchronous call made, including its
//! re-check behind a fresh wait ([`SelfCheckMode::Standard`]). After each
//! page the worker saves the cursor, the count and the divergences so far in
//! the row, so a page is the unit of progress, of a poll's view, and of
//! resumption. [`fold_page`] decides, from a page, whether the walk goes on.
//! A page that finds the target not live, not caught up, or its capture
//! broken ends the job: those are not verdicts on the rows.
//!
//! # Bounded work, no held locks
//!
//! A page runs under a deadline ([`page_budget`]) as a public call does
//! (`crate::deadline::within`): the server stops any statement of it at the
//! budget, and the job fails with [`SelfCheckError::PageTimedOut`] rather
//! than sit on a connection. A page is read-only and holds nothing between
//! its statements; its comparison is one statement under one snapshot. The
//! claim and the saves are single indexed statements.
//!
//! # Who owns a job
//!
//! `claimed_by` names the worker task running a job, and every save is
//! conditional on it still being the claimant. A job whose claimant stopped
//! saving (a crash) is taken over by the next worker once
//! `reclaim_ttl` and a page's budget have passed, and resumes at its saved cursor; the old
//! claimant's late save, if it ever comes, changes nothing.
//!
//! * **Shutdown.** A worker that is told to stop drops its page in flight
//!   and calls [`cancel_claimed`], which marks its jobs `cancelled`. A caller
//!   polling sees that, and starts a new check.
//! * **`DROP TRANSFORM`.** The job's row goes with the definition (a
//!   cascading delete), so the worker's next save finds no row and its walk
//!   ends there. A poll for the job finds nothing.
//! * **A second `self_check`** of a target whose job is `queued` or
//!   `running` returns that job (same id), whatever mode and timeout it
//!   passes; one unfinished job per definition. A finished job stays until
//!   the next `self_check` of the target replaces it.

use std::time::Duration;

use tokio_postgres::types::PgLsn;

use crate::defs::model::TransformStatus;
use crate::pool::Pool;

use super::holdup::DrainFailure;
use super::quarantine::HeldKeys;
use super::self_check::{
    self, Divergence, SelfCheckError, SelfCheckMode, SelfCheckOutcome, SelfCheckPage,
    SelfCheckScope,
};

/// How many keys one page compares unless a worker is told otherwise.
pub const DEFAULT_PAGE_KEYS: i64 = 10_000;

/// How many divergences a job collects before it stops walking and reports
/// what it has, with [`SelfCheckReport::truncated`] set. A target that wrong
/// is wrong everywhere, and the rest only makes the row large.
pub const MAX_DIVERGENCES: usize = 1_000;

/// What a page may spend on its comparison and its catalog reads, on top of
/// the convergence waits it makes ([`page_budget`]), unless a worker is told
/// otherwise.
pub const DEFAULT_COMPARE_BUDGET: Duration = Duration::from_secs(60);

/// How long a page may run: `compare` for its comparison and catalog reads,
/// plus the two convergence waits (`await_timeout` each) a
/// [`SelfCheckMode::Standard`] page can make.
pub fn page_budget(compare: Duration, await_timeout: Duration) -> Duration {
    compare.saturating_add(await_timeout.saturating_mul(2))
}

/// What a worker needs to run jobs.
#[derive(Debug, Clone, Copy)]
pub struct WorkerOptions {
    /// Keys per page ([`DEFAULT_PAGE_KEYS`]).
    pub page_keys: i64,
    /// How long a claimant may go without saving before its job is taken
    /// over, on top of the longest a page can take ([`page_budget`]).
    pub reclaim_ttl: Duration,
    /// What a page may spend on its comparison ([`DEFAULT_COMPARE_BUDGET`]).
    pub compare_budget: Duration,
}

impl WorkerOptions {
    /// The options a drain worker runs with.
    pub fn new(reclaim_ttl: Duration) -> Self {
        WorkerOptions {
            page_keys: DEFAULT_PAGE_KEYS,
            reclaim_ttl,
            compare_budget: DEFAULT_COMPARE_BUDGET,
        }
    }
}

/// Where a [`SelfCheckJob`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfCheckJobState {
    /// Registered, and no worker has taken it yet.
    Queued,
    /// A worker is walking the target.
    Running,
    /// The comparison ended: [`SelfCheckJob::report`] says how.
    Done,
    /// The comparison could not run to an end: [`SelfCheckJob::error`] says
    /// why.
    Failed,
    /// A worker shut down under it: [`SelfCheckJob::error`] says so. Nothing
    /// was concluded.
    Cancelled,
}

impl SelfCheckJobState {
    /// The state as the catalog stores it.
    pub fn as_str(self) -> &'static str {
        match self {
            SelfCheckJobState::Queued => "queued",
            SelfCheckJobState::Running => "running",
            SelfCheckJobState::Done => "done",
            SelfCheckJobState::Failed => "failed",
            SelfCheckJobState::Cancelled => "cancelled",
        }
    }

    fn from_persisted(text: &str) -> Self {
        match text {
            "queued" => SelfCheckJobState::Queued,
            "running" => SelfCheckJobState::Running,
            "done" => SelfCheckJobState::Done,
            "failed" => SelfCheckJobState::Failed,
            "cancelled" => SelfCheckJobState::Cancelled,
            other => panic!("self_check_jobs.state held unrecognized value '{other}'"),
        }
    }

    /// Whether the job has ended: no worker will touch it again.
    pub fn is_finished(self) -> bool {
        matches!(
            self,
            SelfCheckJobState::Done | SelfCheckJobState::Failed | SelfCheckJobState::Cancelled
        )
    }
}

/// A `self_check` job: what [`crate::Trellis::self_check`] returns when it
/// starts one and [`crate::Trellis::self_check_job`] returns when it is
/// polled.
#[derive(Debug, Clone)]
pub struct SelfCheckJob {
    /// The job's id, which [`crate::Trellis::self_check_job`] takes.
    pub id: i64,
    /// The audited target's bare table name.
    pub target: String,
    pub mode: SelfCheckMode,
    pub state: SelfCheckJobState,
    /// How many distinct keys the pages so far compared. Moves while the job
    /// is `running`.
    pub rows_compared: i64,
    /// The result, once the job is [`SelfCheckJobState::Done`].
    pub report: Option<SelfCheckReport>,
    /// Why the job is [`SelfCheckJobState::Failed`] or
    /// [`SelfCheckJobState::Cancelled`].
    pub error: Option<String>,
}

/// What a finished `self_check` job found: the whole target's comparison,
/// not a page of it.
#[derive(Debug, Clone)]
pub struct SelfCheckReport {
    /// The audited target's bare table name.
    pub target: String,
    /// The watermark token the last page was checked through (the position
    /// `await_converged` confirmed the target had caught up to, or failed to,
    /// for [`SelfCheckOutcome::NotCaughtUp`]; for a [`Divergence::Capture`]
    /// fault, the WAL position when the capture audit read the catalog).
    pub checked_through: PgLsn,
    /// How many distinct keys the job compared, the union of every key seen
    /// on either side of the comparison.
    pub rows_compared: i64,
    /// Whether the job stopped before the end of the target's keys: it found
    /// [`MAX_DIVERGENCES`] divergences, or a page found the target no longer
    /// live or not caught up (or its capture broken) after earlier pages had
    /// compared rows. `rows_compared` says how far it got.
    pub truncated: bool,
    pub outcome: SelfCheckOutcome,
    /// The keys the audited definition holds in quarantine, read when the
    /// job is polled, `None` if it holds none (#759). Reported with every
    /// outcome, because a held key is one the audit can't vouch for: the
    /// definition leaves its changes out, so its target row stays as it was
    /// when the key was poisoned. It is a [`SelfCheckOutcome::Diverged`] row
    /// once its source row changes, if the walk reaches it, and a key with
    /// changes parked holds back convergence, so the audit reports
    /// [`SelfCheckOutcome::NotCaughtUp`] until it is released. Sample the
    /// keys with `Trellis::sample_quarantined` and release each with
    /// `Trellis::release_key`, or resume the definition.
    pub held_keys: Option<HeldKeys>,
    /// Every page the drain keeps failing on with nothing charged or paused,
    /// oldest first (#817), read when the job is polled: an instance-level
    /// finding, reported with every outcome. Each holds back the targets of
    /// the tables it holds changes to, and with them the convergence the
    /// audit waits on. Empty when there is none.
    pub drain_failures: Vec<DrainFailure>,
}

/// A job's row, as [`get`] reads it.
const JOB_COLUMNS: &str = "j.id, split_part(d.target_table, '.', 2), j.mode, j.state, \
     j.rows_compared, j.divergences::text, j.outcome, j.not_live_status, j.checked_through, \
     j.truncated, j.error";

fn mode_word(mode: SelfCheckMode) -> &'static str {
    match mode {
        SelfCheckMode::Standard => "standard",
        SelfCheckMode::Strict => "strict",
    }
}

fn mode_from_word(word: &str) -> SelfCheckMode {
    match word {
        "standard" => SelfCheckMode::Standard,
        "strict" => SelfCheckMode::Strict,
        other => panic!("self_check_jobs.mode held unrecognized value '{other}'"),
    }
}

fn millis(timeout: Duration) -> i64 {
    i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX)
}

/// Registers a job to check `target_table` and returns it, or returns the
/// unfinished job the target already has (see the module doc). Refuses what
/// the check could never run on: no such transform, an aggregate target, a
/// field that reads a relationship. `timeout` bounds each convergence wait
/// the job's pages make; see [`crate::Trellis::await_converged`] for how to
/// size it.
pub async fn start(
    pool: &Pool,
    target_table: &str,
    mode: SelfCheckMode,
    timeout: Duration,
) -> Result<SelfCheckJob, SelfCheckError> {
    let def = self_check::comparable_definition(pool, target_table).await?;
    self_check::check_renders(&def)?;

    let mut client = pool.get().await?;
    let id = loop {
        let txn = client.transaction().await?;
        // The previous check's result goes; this one replaces it.
        txn.execute(
            "delete from self_check_jobs \
             where definition_id = $1 and state in ('done', 'failed', 'cancelled')",
            &[&def.id],
        )
        .await?;
        let inserted = txn
            .query_opt(
                "insert into self_check_jobs (definition_id, mode, await_timeout_ms, state) \
                 values ($1, $2, $3, 'queued') \
                 on conflict (definition_id) where state in ('queued', 'running') do nothing \
                 returning id",
                &[&def.id, &mode_word(mode), &millis(timeout)],
            )
            .await?;
        let id = match inserted {
            Some(row) => Some(row.get::<_, i64>(0)),
            None => txn
                .query_opt(
                    "select id from self_check_jobs \
                     where definition_id = $1 and state in ('queued', 'running')",
                    &[&def.id],
                )
                .await?
                .map(|row| row.get(0)),
        };
        txn.commit().await?;
        // `None` only if the job it conflicted with finished in between.
        if let Some(id) = id {
            break id;
        }
    };
    drop(client);

    get(pool, id)
        .await?
        .ok_or_else(|| SelfCheckError::TargetNotFound(target_table.to_string()))
}

/// The job `id`, or `None` if there is none: it never existed, a newer
/// `self_check` of its target replaced it, or its transform was dropped.
pub async fn get(pool: &Pool, id: i64) -> Result<Option<SelfCheckJob>, SelfCheckError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            &format!(
                "select {JOB_COLUMNS} from self_check_jobs j \
                 join transform_definitions d on d.id = j.definition_id \
                 where j.id = $1"
            ),
            &[&id],
        )
        .await?;
    drop(client);
    let Some(row) = row else {
        return Ok(None);
    };

    let target: String = row.get(1);
    let state = SelfCheckJobState::from_persisted(row.get(3));
    let rows_compared: i64 = row.get(4);
    let report = if state == SelfCheckJobState::Done {
        Some(report_of(pool, &target, &row).await?)
    } else {
        None
    };
    Ok(Some(SelfCheckJob {
        id: row.get(0),
        target,
        mode: mode_from_word(row.get(2)),
        state,
        rows_compared,
        report,
        error: row.get(10),
    }))
}

/// The report of a finished job's row: what it saved, plus what is read live
/// now (the keys held and the drain failures).
async fn report_of(
    pool: &Pool,
    target: &str,
    row: &tokio_postgres::Row,
) -> Result<SelfCheckReport, SelfCheckError> {
    let divergences: String = row.get(5);
    let divergences: Vec<Divergence> =
        serde_json::from_str(&divergences).expect("self_check_jobs.divergences held our JSON");
    let outcome = match row.get::<_, Option<&str>>(6) {
        Some("converged") => SelfCheckOutcome::Converged,
        Some("not_caught_up") => SelfCheckOutcome::NotCaughtUp,
        Some("diverged") => SelfCheckOutcome::Diverged(divergences),
        Some("not_live") => {
            let status: &str = row
                .get::<_, Option<&str>>(7)
                .expect("a not_live job saved the status it found");
            SelfCheckOutcome::NotLive(
                TransformStatus::from_persisted(status)
                    .expect("self_check_jobs.not_live_status held a status"),
            )
        }
        other => panic!("a done self_check job held outcome {other:?}"),
    };
    // The transform can be dropped between the two reads; there is then
    // nothing of it to report.
    let findings = match crate::defs::catalog::definition_by_target(pool, target).await? {
        Some(def) => Some(self_check::findings(pool, &def).await?),
        None => None,
    };
    let (held_keys, drain_failures) = match findings {
        Some(found) => (found.held_keys, found.drain_failures),
        None => (None, Vec::new()),
    };
    Ok(SelfCheckReport {
        target: target.to_string(),
        checked_through: row
            .get::<_, Option<PgLsn>>(8)
            .expect("a done job saved the position it checked through"),
        rows_compared: row.get(4),
        truncated: row.get(9),
        outcome,
        held_keys,
        drain_failures,
    })
}

/// What a worker holds while it runs a job: the row, as it saved it last.
#[derive(Debug)]
struct Claim {
    id: i64,
    /// The audited target's bare name.
    target: String,
    mode: SelfCheckMode,
    await_timeout: Duration,
    progress: Progress,
}

/// How far a job's walk has got: what a page starts from and a save records.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Progress {
    /// The keyset cursor the next page starts after.
    after: Option<String>,
    rows_compared: i64,
    divergences: Vec<Divergence>,
}

/// How a job ended, as a save records it.
#[derive(Debug, PartialEq, Eq)]
struct Finish {
    outcome: SelfCheckOutcome,
    checked_through: PgLsn,
    truncated: bool,
}

/// What [`fold_page`] decides.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// Run the next page, from `Progress::after`.
    Continue,
    Finish(Finish),
}

/// Folds one compared `page` into the walk's `progress` and decides whether
/// it goes on.
///
/// A page that is not a comparison (the target not live, not caught up, or
/// its capture broken) ends the walk. The divergences the earlier pages found
/// are still findings, so they are reported, and the walk is `truncated`. The
/// two schema findings repeat on every page, so they are kept once.
fn fold_page(progress: &mut Progress, page: SelfCheckPage) -> Step {
    let checked_through = page.checked_through;
    let finish = |progress: &mut Progress, stopped: SelfCheckOutcome, truncated: bool| {
        let outcome = if progress.divergences.is_empty() {
            stopped
        } else {
            SelfCheckOutcome::Diverged(std::mem::take(&mut progress.divergences))
        };
        Step::Finish(Finish {
            outcome,
            checked_through,
            truncated,
        })
    };

    match page.outcome {
        SelfCheckOutcome::NotLive(status) => {
            return finish(progress, SelfCheckOutcome::NotLive(status), true);
        }
        SelfCheckOutcome::NotCaughtUp => {
            return finish(progress, SelfCheckOutcome::NotCaughtUp, true);
        }
        SelfCheckOutcome::Converged => {}
        SelfCheckOutcome::Diverged(found) => {
            // A capture fault is a page that compared nothing.
            let capture = found.iter().any(|d| matches!(d, Divergence::Capture(_)));
            for divergence in found {
                let repeats = matches!(
                    divergence,
                    Divergence::MissingColumn { .. } | Divergence::ExtraColumn { .. }
                ) && progress.divergences.contains(&divergence);
                if !repeats {
                    progress.divergences.push(divergence);
                }
            }
            if capture {
                return finish(progress, SelfCheckOutcome::Converged, true);
            }
        }
    }
    progress.rows_compared += page.rows_compared;

    if progress.divergences.len() >= MAX_DIVERGENCES {
        return finish(progress, SelfCheckOutcome::Converged, true);
    }
    match page.next_after {
        None => finish(progress, SelfCheckOutcome::Converged, false),
        // A cursor that didn't move would repeat the page for ever.
        Some(next) if progress.after.as_deref() == Some(next.as_str()) => {
            finish(progress, SelfCheckOutcome::Converged, true)
        }
        Some(next) => {
            progress.after = Some(next);
            Step::Continue
        }
    }
}

/// Claims a job and runs one page of it. Returns whether there was a job to
/// run: `false` leaves the caller free to wait. A job this worker claimed
/// stays its own, so it comes back to it on the next call until it ends.
///
/// A failure of the page ends the job as `failed`; an error returned here is
/// one of the claim or the save themselves, which leaves the job as it was
/// for a later pass (or another worker, once it is stale).
pub async fn work_once(
    pool: &Pool,
    claimed_by: &str,
    options: &WorkerOptions,
) -> Result<bool, SelfCheckError> {
    let Some(mut claim) = claim(pool, claimed_by, options).await? else {
        return Ok(false);
    };

    let page = run_page(pool, &claim, options).await;
    match page {
        Err(error) => fail(pool, claimed_by, claim.id, &error).await?,
        Ok(page) => match fold_page(&mut claim.progress, page) {
            Step::Continue => save(pool, claimed_by, claim.id, &claim.progress).await?,
            Step::Finish(finish) => {
                finish_job(pool, claimed_by, claim.id, &claim.progress, &finish).await?
            }
        },
    }
    Ok(true)
}

/// One page of `claim`'s walk, under its budget.
async fn run_page(
    pool: &Pool,
    claim: &Claim,
    options: &WorkerOptions,
) -> Result<SelfCheckPage, SelfCheckError> {
    let budget = page_budget(options.compare_budget, claim.await_timeout);
    let started = std::time::Instant::now();
    let scope = SelfCheckScope {
        after: claim.progress.after.clone(),
        limit: options.page_keys,
    };
    let page = crate::deadline::within(budget, async {
        #[cfg(any(test, feature = "test-util"))]
        {
            let mut client = pool.get().await?;
            let txn = client.transaction().await?;
            super::interleave::pause_at(
                &*txn,
                super::interleave::PausePoint::BeforeSelfCheckPage,
                &claim.target,
            )
            .await?;
            txn.commit().await?;
        }
        let def = self_check::comparable_definition(pool, &claim.target).await?;
        self_check::audit(
            pool,
            &def,
            &claim.target,
            scope,
            claim.mode,
            claim.await_timeout,
        )
        .await
    })
    .await;
    match page {
        // The server's timer, which fires at the budget, not before it.
        Ok(Err(SelfCheckError::Db(error)))
            if error.code() == Some(&tokio_postgres::error::SqlState::QUERY_CANCELED)
                && started.elapsed() >= budget =>
        {
            Err(SelfCheckError::PageTimedOut { budget })
        }
        Ok(page) => page,
        Err(expired) => Err(SelfCheckError::PageTimedOut {
            budget: expired.budget,
        }),
    }
}

/// Takes the oldest job that is `queued`, that this worker already runs, or
/// that its claimant has left, and returns it as it was last saved.
async fn claim(
    pool: &Pool,
    claimed_by: &str,
    options: &WorkerOptions,
) -> Result<Option<Claim>, SelfCheckError> {
    let client = pool.get().await?;
    let ttl_secs = options.reclaim_ttl.as_secs_f64();
    let row = client
        .query_opt(
            "with claimed as ( \
                 update self_check_jobs set state = 'running', claimed_by = $1, claimed_at = now() \
                 where id = ( \
                     select id from self_check_jobs \
                     where state = 'queued' \
                        or (state = 'running' and (claimed_by = $1 \
                            or claimed_at < now() - (interval '1 second' * $2 \
                                + interval '1 millisecond' * (2 * await_timeout_ms + $3)))) \
                     order by id limit 1 \
                     for update skip locked) \
                 returning * ) \
             select c.id, split_part(d.target_table, '.', 2), c.mode, c.await_timeout_ms, \
                    c.after_key, c.rows_compared, c.divergences::text \
             from claimed c join transform_definitions d on d.id = c.definition_id",
            &[&claimed_by, &ttl_secs, &millis(options.compare_budget)],
        )
        .await?;
    Ok(row.map(|row| {
        let divergences: String = row.get(6);
        Claim {
            id: row.get(0),
            target: row.get(1),
            mode: mode_from_word(row.get(2)),
            await_timeout: Duration::from_millis(row.get::<_, i64>(3) as u64),
            progress: Progress {
                after: row.get(4),
                rows_compared: row.get(5),
                divergences: serde_json::from_str(&divergences)
                    .expect("self_check_jobs.divergences held our JSON"),
            },
        }
    }))
}

fn divergences_json(progress: &Progress) -> String {
    serde_json::to_string(&progress.divergences).expect("divergences serialize")
}

/// Records the walk so far. Does nothing if the job is no longer `claimed_by`'s
/// to save: it was dropped with its transform, or another worker took it.
async fn save(
    pool: &Pool,
    claimed_by: &str,
    id: i64,
    progress: &Progress,
) -> Result<(), SelfCheckError> {
    let client = pool.get().await?;
    client
        .execute(
            "update self_check_jobs \
             set after_key = $3, rows_compared = $4, divergences = $5::text::jsonb, \
                 claimed_at = now() \
             where id = $1 and claimed_by = $2 and state = 'running'",
            &[
                &id,
                &claimed_by,
                &progress.after,
                &progress.rows_compared,
                &divergences_json(progress),
            ],
        )
        .await?;
    Ok(())
}

/// Ends the job as `done`. Like [`save`], conditional on the claim.
async fn finish_job(
    pool: &Pool,
    claimed_by: &str,
    id: i64,
    progress: &Progress,
    finish: &Finish,
) -> Result<(), SelfCheckError> {
    let (outcome, not_live) = match &finish.outcome {
        SelfCheckOutcome::Converged => ("converged", None),
        SelfCheckOutcome::NotCaughtUp => ("not_caught_up", None),
        SelfCheckOutcome::Diverged(_) => ("diverged", None),
        SelfCheckOutcome::NotLive(status) => ("not_live", Some(status.as_str())),
    };
    let divergences = match &finish.outcome {
        SelfCheckOutcome::Diverged(found) => {
            serde_json::to_string(found).expect("divergences serialize")
        }
        _ => "[]".to_string(),
    };
    let client = pool.get().await?;
    client
        .execute(
            "update self_check_jobs \
             set state = 'done', outcome = $3, not_live_status = $4, checked_through = $5, \
                 truncated = $6, rows_compared = $7, divergences = $8::text::jsonb, \
                 after_key = $9, finished_at = now() \
             where id = $1 and claimed_by = $2 and state = 'running'",
            &[
                &id,
                &claimed_by,
                &outcome,
                &not_live,
                &finish.checked_through,
                &finish.truncated,
                &progress.rows_compared,
                &divergences,
                &progress.after,
            ],
        )
        .await?;
    Ok(())
}

/// Ends the job as `failed` with `error`. Like [`save`], conditional on the
/// claim.
async fn fail(
    pool: &Pool,
    claimed_by: &str,
    id: i64,
    error: &SelfCheckError,
) -> Result<(), SelfCheckError> {
    let client = pool.get().await?;
    client
        .execute(
            "update self_check_jobs \
             set state = 'failed', error = $3, finished_at = now() \
             where id = $1 and claimed_by = $2 and state = 'running'",
            &[&id, &claimed_by, &error.to_string()],
        )
        .await?;
    Ok(())
}

/// Ends the jobs `claimed_by` is running as `cancelled`: it is shutting down.
/// Returns how many.
pub async fn cancel_claimed(pool: &Pool, claimed_by: &str) -> Result<u64, SelfCheckError> {
    let client = pool.get().await?;
    Ok(client
        .execute(
            "update self_check_jobs \
             set state = 'cancelled', finished_at = now(), \
                 error = 'cancelled: the worker running it shut down' \
             where claimed_by = $1 and state = 'running'",
            &[&claimed_by],
        )
        .await?)
}

/// [`crate::Trellis::self_check`] run to its end by hand, for tests: starts
/// the job through the facade, runs the pages a drain worker would
/// ([`work_once`], with `options`) until the job is finished, and returns the
/// job as a poll reads it. There is no wait in it: each pass is the next page.
#[cfg(any(test, feature = "internals"))]
pub async fn run_to_end(
    trellis: &crate::Trellis,
    target_table: &str,
    mode: SelfCheckMode,
    timeout: Duration,
    options: &WorkerOptions,
) -> Result<SelfCheckJob, crate::TrellisError> {
    let job = trellis.self_check(target_table, mode, timeout).await?;
    while work_once(trellis.pool(), "run_to_end", options)
        .await
        .map_err(crate::TrellisError::SelfCheck)?
    {}
    Ok(trellis
        .self_check_job(job.id)
        .await?
        .expect("the job exists until its transform is dropped or it is replaced"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(outcome: SelfCheckOutcome, rows: i64, next_after: Option<&str>) -> SelfCheckPage {
        SelfCheckPage {
            target: "t".to_string(),
            checked_through: PgLsn::from(7),
            rows_compared: rows,
            next_after: next_after.map(str::to_string),
            outcome,
            held_keys: None,
            drain_failures: Vec::new(),
            unindexed_joins: Vec::new(),
        }
    }

    fn cell(key: &str) -> Divergence {
        Divergence::Cell {
            key: key.to_string(),
            column: "c".to_string(),
            persisted: Some("1".to_string()),
            recomputed: Some("2".to_string()),
        }
    }

    fn finished(step: Step) -> Finish {
        match step {
            Step::Finish(finish) => finish,
            Step::Continue => panic!("expected the walk to end"),
        }
    }

    #[test]
    fn a_page_that_reaches_the_end_of_the_keys_ends_the_walk_converged() {
        let mut progress = Progress::default();
        let finish = finished(fold_page(
            &mut progress,
            page(SelfCheckOutcome::Converged, 10, None),
        ));
        assert!(matches!(finish.outcome, SelfCheckOutcome::Converged));
        assert!(!finish.truncated);
        assert_eq!(progress.rows_compared, 10);
    }

    #[test]
    fn a_page_that_hit_its_limit_continues_from_where_it_ended() {
        let mut progress = Progress::default();
        assert_eq!(
            fold_page(
                &mut progress,
                page(SelfCheckOutcome::Converged, 10, Some("k10"))
            ),
            Step::Continue
        );
        assert_eq!(progress.after.as_deref(), Some("k10"));
        let finish = finished(fold_page(
            &mut progress,
            page(SelfCheckOutcome::Converged, 3, None),
        ));
        assert!(matches!(finish.outcome, SelfCheckOutcome::Converged));
        assert_eq!(progress.rows_compared, 13, "the pages' counts add up");
    }

    #[test]
    fn divergences_from_every_page_are_reported_together() {
        let mut progress = Progress::default();
        let step = fold_page(
            &mut progress,
            page(SelfCheckOutcome::Diverged(vec![cell("a")]), 10, Some("k10")),
        );
        assert_eq!(step, Step::Continue);
        let finish = finished(fold_page(
            &mut progress,
            page(SelfCheckOutcome::Diverged(vec![cell("z")]), 3, None),
        ));
        assert_eq!(
            finish.outcome,
            SelfCheckOutcome::Diverged(vec![cell("a"), cell("z")])
        );
        assert!(!finish.truncated);
    }

    #[test]
    fn a_schema_finding_every_page_repeats_is_kept_once() {
        let drift = Divergence::MissingColumn {
            column: "c".to_string(),
        };
        let mut progress = Progress::default();
        let page_of =
            |next: Option<&str>| page(SelfCheckOutcome::Diverged(vec![drift.clone()]), 5, next);
        assert_eq!(fold_page(&mut progress, page_of(Some("a"))), Step::Continue);
        assert_eq!(fold_page(&mut progress, page_of(Some("b"))), Step::Continue);
        let finish = finished(fold_page(&mut progress, page_of(None)));
        assert_eq!(finish.outcome, SelfCheckOutcome::Diverged(vec![drift]));
    }

    #[test]
    fn a_walk_stops_at_the_divergence_cap_and_says_it_was_cut_short() {
        let mut progress = Progress::default();
        let many: Vec<Divergence> = (0..MAX_DIVERGENCES).map(|i| cell(&i.to_string())).collect();
        let finish = finished(fold_page(
            &mut progress,
            page(SelfCheckOutcome::Diverged(many), 5_000, Some("more")),
        ));
        assert!(finish.truncated);
        assert!(
            matches!(finish.outcome, SelfCheckOutcome::Diverged(d) if d.len() == MAX_DIVERGENCES)
        );
    }

    #[test]
    fn a_page_that_is_not_a_comparison_ends_the_walk_without_a_verdict() {
        for stopped in [
            SelfCheckOutcome::NotCaughtUp,
            SelfCheckOutcome::NotLive(TransformStatus::Paused),
        ] {
            let mut progress = Progress::default();
            let finish = finished(fold_page(
                &mut progress,
                page(stopped.clone(), 0, Some("k")),
            ));
            assert_eq!(finish.outcome, stopped);
            assert!(finish.truncated);
        }
    }

    #[test]
    fn a_page_that_is_not_a_comparison_keeps_what_the_earlier_pages_found() {
        let mut progress = Progress::default();
        fold_page(
            &mut progress,
            page(SelfCheckOutcome::Diverged(vec![cell("a")]), 10, Some("k10")),
        );
        let finish = finished(fold_page(
            &mut progress,
            page(SelfCheckOutcome::NotCaughtUp, 0, Some("k10")),
        ));
        assert_eq!(finish.outcome, SelfCheckOutcome::Diverged(vec![cell("a")]));
        assert!(finish.truncated, "the walk did not reach the end");
    }

    #[test]
    fn a_capture_fault_ends_the_walk_at_once() {
        let fault = Divergence::Capture(super::super::CaptureFault::NotInstalled {
            table: "public.t".to_string(),
        });
        let mut progress = Progress::default();
        let finish = finished(fold_page(
            &mut progress,
            page(SelfCheckOutcome::Diverged(vec![fault.clone()]), 0, None),
        ));
        assert_eq!(finish.outcome, SelfCheckOutcome::Diverged(vec![fault]));
        assert!(finish.truncated, "no row was compared");
    }

    #[test]
    fn a_cursor_that_does_not_move_ends_the_walk_instead_of_repeating_it() {
        let mut progress = Progress {
            after: Some("k10".to_string()),
            ..Progress::default()
        };
        let finish = finished(fold_page(
            &mut progress,
            page(SelfCheckOutcome::Converged, 0, Some("k10")),
        ));
        assert!(finish.truncated);
    }

    #[test]
    fn a_page_budget_covers_the_waits_a_standard_page_can_make() {
        let budget = page_budget(DEFAULT_COMPARE_BUDGET, Duration::from_secs(30));
        assert_eq!(budget, DEFAULT_COMPARE_BUDGET + Duration::from_secs(60));
    }

    #[test]
    fn the_states_round_trip_through_their_catalog_words() {
        for state in [
            SelfCheckJobState::Queued,
            SelfCheckJobState::Running,
            SelfCheckJobState::Done,
            SelfCheckJobState::Failed,
            SelfCheckJobState::Cancelled,
        ] {
            assert_eq!(SelfCheckJobState::from_persisted(state.as_str()), state);
        }
    }

    #[test]
    fn divergences_survive_the_json_a_job_keeps_them_in() {
        let all = vec![
            cell("a"),
            Divergence::MissingRow { key: "b".into() },
            Divergence::ExtraRow { key: "c".into() },
            Divergence::MissingColumn { column: "d".into() },
            Divergence::ExtraColumn { column: "e".into() },
            Divergence::Capture(super::super::CaptureFault::MissingPrivilege {
                role: "r".into(),
                privilege: "INSERT".into(),
                object: "ring".into(),
            }),
            Divergence::Capture(super::super::CaptureFault::Hierarchy(
                crate::defs::hierarchy::Hierarchy::Partition {
                    table: "public.t".into(),
                    parent: "public.p".into(),
                },
            )),
        ];
        let json = serde_json::to_string(&all).unwrap();
        let back: Vec<Divergence> = serde_json::from_str(&json).unwrap();
        assert_eq!(back, all);
    }
}
