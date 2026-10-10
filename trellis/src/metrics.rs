//! An internal facade over the `metrics`/`metrics-exporter-prometheus`
//! in-process registry (issue #51, `docs/decisions/0009-observability-decisions.md`).
//!
//! Call sites elsewhere in this crate (`staging::apply::flush_apply_metrics`,
//! today — called from `staging::apply::drain_once`/`drain_many` once a
//! batch's apply transaction has actually committed; see that function's doc
//! comment for why recording happens there and not from `staging::apply::compute`
//! itself, which merely buffers the data these functions end up recording)
//! record through the plain functions below rather than reaching for the
//! `metrics` crate's own macros/types directly — so a future change to the
//! recording backend (a different facade, a second exporter, richer label
//! sets) stays contained to this one module instead of touching every call
//! site.
//!
//! This module builds and populates the in-process registry, and exposes it
//! as issue #53's [`Metrics::render_prometheus`] (Prometheus text
//! exposition, obtained through [`crate::app::Trellis::metrics`] or
//! [`crate::blocking::BlockingTrellis::metrics`], matching
//! `docs/observability.md`'s `trellis.metrics().render_prometheus()` sketch).
//! Cross-process aggregation (summing buckets/counters across every engine
//! process scraping into the same Prometheus) is deliberately left to
//! Prometheus itself, not persisted by this crate — see
//! `docs/observability.md`'s "Retention" section.
//!
//! ## One label on every series (issue #873, epic #806)
//!
//! Several [`crate::app::Trellis`] handles can share a process, so every
//! series carries `trellis_instance="<database>/<schema>"`, the same value
//! every log event of that instance carries (`crate::instance_log::name_of`).
//! The recording functions below take no instance argument: they read the
//! instance the calling code runs under (`crate::instance_log::current`), the
//! scope each handle already sets for its log events. Code outside any handle
//! (a test calling an engine function directly) records under `unknown`. The
//! label is `trellis_instance`, not `instance`, because Prometheus attaches
//! its own `instance` label (the scrape target) and renames a clash to
//! `exported_instance`.
//!
//! ## A stopped instance's series
//!
//! The registry is process-wide, so a stopped handle's series stay in it. Its
//! counters and histograms stay flat, and their rate is 0. Its gauges would
//! keep their last value, so the recorder drops a gauge that has not been set
//! for [`GAUGE_IDLE_TIMEOUT`], and a running instance sets every gauge it owns
//! on each tick of [`refresh_instance_gauges`]. `trellis_instance_up` says
//! which instances run: see [`RunningInstance`].
//!
//! ## Recorder installation
//!
//! `metrics`'s macros (`counter!`/`histogram!`/`gauge!`) record against
//! whichever [`metrics::Recorder`] is currently installed as the process's
//! *global* recorder — a single, process-wide registry, not one per
//! [`crate::app::Trellis`] instance, matching `docs/observability.md`'s "one
//! in-process registry" design. [`ensure_installed`] lazily builds a
//! [`metrics_exporter_prometheus::PrometheusRecorder`] (recorder + text
//! encoder only — the exporter's optional Hyper-listener feature is not
//! enabled in `Cargo.toml`, so this never binds a socket) and installs it
//! the first time any recording function in this module runs. Installation
//! is idempotent and best-effort: if a global recorder is already installed
//! (a second call racing the [`OnceLock`], or — someday — an embedder
//! installing its own before this crate's first call), later attempts
//! simply lose and every macro call below still records into *this*
//! module's own handle instead of whatever won, since nothing else in this
//! process installs a recorder yet. A real multi-installer story is out of
//! scope for this issue.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::instance_log;

/// The label every series carries: the instance's `<database>/<schema>`.
pub const INSTANCE_LABEL: &str = "trellis_instance";

/// 1 while a handle of the instance runs in this process, 0 once the last one
/// has stopped (issue #873). Rendered by [`Metrics::render_prometheus`] from
/// [`RUNNING`], not recorded through the recorder, so the idle timeout never
/// drops it.
const INSTANCE_UP_METRIC: &str = "trellis_instance_up";

/// How long a gauge can go without being set before the recorder drops it
/// (issue #873). A running instance sets every gauge it owns every
/// maintenance interval (300 ms by default), so only a stopped instance's
/// gauges age out. Counters and histograms never expire.
pub const GAUGE_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// ADR-0009 decision 6: exponential bucket boundaries spanning ~10ms-60s
/// (`docs/observability.md`'s "sub-second to tens-of-seconds propagation
/// range"), applied as a single global default to every histogram this
/// crate records — per-transform latency today; end-to-end latency (issue
/// #52) is expected to reuse the same set rather than introduce its own.
pub const LATENCY_BUCKETS: &[f64] = &[
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0,
];

/// Per-transform hop latency: time from a change becoming available at a
/// transform's input to its output being applied (`docs/observability.md`'s
/// "What 'latency' means"). Labeled `transform` — the transform's target
/// table name, the same identifier `staging::quarantine::resume_column`'s
/// `transform` parameter and `ApplyError::ColumnNotPaused`/`DefinitionNotLive`
/// already use, not a separate "transform name" field (there isn't one —
/// see [`crate::defs::ast::TransformDef`]).
const TRANSFORM_LATENCY_METRIC: &str = "trellis_transform_latency_seconds";

/// Staged ring rows each transform folded and applied (issue #409): one
/// change per row a capture trigger staged, or per row an
/// upstream hop's target write staged for this one. Counted in rows, not
/// folded changes, so the number doesn't depend on how the rows happened to
/// be batched and can be compared with the source's own write rate. Not the
/// latency histograms' denominator: those observe once per folded change,
/// so their own `_count` series is that.
const CHANGES_APPLIED_METRIC: &str = "trellis_changes_applied_total";

/// End-to-end latency: time from the *source* commit to the *terminal*
/// transform's apply (`docs/observability.md`'s "What 'latency' means",
/// ADR-0009 decision 2). Labeled `transform` — same convention as
/// [`TRANSFORM_LATENCY_METRIC`] — but only ever recorded for a transform
/// whose target has no downstream reader of its own (a DAG sink), never for
/// an intermediate hop: keyed by terminal transform only, summed across
/// every source feeding it, not by source->sink pair (issue #52).
const END_TO_END_LATENCY_METRIC: &str = "trellis_end_to_end_latency_seconds";

/// ADR-0009 decision 5's cheap, system-level gauge: a count of `segments`
/// rows by [`crate::staging::SegmentState`], labeled `state`.
const STAGING_SEGMENTS_METRIC: &str = "trellis_staging_segments";

/// Issue #134/#135 (epic #127): count of to-one relationship reverse deltas
/// deferred by one of issue #132's four guards, labeled `guard` — the plan
/// doc's own `d5_block_barrier`/`d5_block_gen`/`d5_block_inflight`/
/// `d5_block_order` names, used verbatim as this one metric's label
/// *values* (`ReverseGuardFailure::metric_label`) rather than as four
/// separate metric names, matching this module's existing
/// one-metric-plus-label convention. Issue #135 (starvation freedom, not
/// yet built) is the reason this exists now: a fairness mechanism needs
/// exactly this per-guard deferral rate to model against, and it needs to be
/// observable in production before that mechanism ships, not just modeled
/// offline.
const RELATIONSHIP_REVERSE_DEFERRED_METRIC: &str = "trellis_relationship_reverse_deferred_total";

/// Issue #135 (epic #127): count of to-one relationship reverse *transitions*
/// (one parent's old-image/new-image pair) that exhausted
/// [`crate::staging::apply::RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD`]'s
/// guard-gated retry budget and were resolved by the fairness-escalation
/// path instead — advancing the settled parent projection immediately and
/// falling back to the pre-#131 image-less recompute for the aggregate
/// correction, rather than deferring again. Deliberately a *separate* metric
/// from [`RELATIONSHIP_REVERSE_DEFERRED_METRIC`] rather than one more label
/// value on it: a deferral is the expected, common-case outcome of a guard
/// rejection (#134's own doc section), while an escalation means the normal
/// retry loop never found a clean window in
/// [`crate::staging::apply::RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD`] attempts
/// — a signal worth its own series (a sustained non-zero rate names a
/// specific hot parent worth investigating), not something an operator
/// should have to pick out of a per-guard breakdown built for a different
/// question.
const RELATIONSHIP_REVERSE_FAIRNESS_ESCALATED_METRIC: &str =
    "trellis_relationship_reverse_fairness_escalated_total";

/// #625 F2: worker time a Re-derive build spends in each of its statement
/// classes, labeled `class` ([`BuildStatement`]). The `_sum` per class is
/// worker-seconds; the profile divides it by the rows built.
const BUILD_STATEMENT_METRIC: &str = "trellis_build_statement_seconds";

/// #625 F2: one Re-derive build chunk's transaction, from its begin to its
/// commit (the span its entry locks and snapshot are held).
const BUILD_CHUNK_METRIC: &str = "trellis_build_chunk_seconds";

/// #625 F2: the longest [`BUILD_CHUNK_METRIC`] this instance has recorded.
const BUILD_CHUNK_MAX_METRIC: &str = "trellis_build_chunk_seconds_max";

/// #625 F2: source keys a Re-derive build's chunks re-derived.
const BUILD_ROWS_METRIC: &str = "trellis_build_rows_total";

/// #625 F2: group-delta rows Re-derive build chunks appended, and that
/// mergers folded into groups, labeled `step` (`appended`/`merged`).
const BUILD_DELTA_ROWS_METRIC: &str = "trellis_build_delta_rows_total";

/// #625 F2: Re-derive build chunks that gave up on their entry lock's short
/// timeout and were released for a retry.
const BUILD_CHUNK_LOCK_TIMEOUTS_METRIC: &str = "trellis_build_chunk_lock_timeouts_total";

/// #625 F2: seals refused because the ring had no free slot, after the
/// retirement the seal answers that with (`staging::seal_if_active_nonempty`).
const SEAL_REFUSED_METRIC: &str = "trellis_seal_refused_total";

/// #922: waits for the column-pause lock (`locks::lock_column_pauses`) that
/// ran out the transaction's `lock_timeout`, labeled `op` (`pause`, `resume`,
/// `fuse`, `cascade`, `define`, `drop`, `alter`, `capture`). Nonzero means a
/// holder stayed in the lock longer than the timeout, or the path got hot.
const COLUMN_PAUSE_LOCK_TIMEOUTS_METRIC: &str = "trellis_column_pause_lock_timeouts_total";

/// [`BUILD_CHUNK_METRIC`]'s buckets: finer than [`LATENCY_BUCKETS`], since a
/// chunk's percentiles are a profile column (#625 §4). About 25% apart from
/// 1 ms to 2 minutes.
const BUILD_CHUNK_BUCKETS: &[f64] = &[
    0.001, 0.00125, 0.0016, 0.002, 0.0025, 0.0032, 0.004, 0.005, 0.0063, 0.008, 0.01, 0.0125,
    0.016, 0.02, 0.025, 0.032, 0.04, 0.05, 0.063, 0.08, 0.1, 0.125, 0.16, 0.2, 0.25, 0.32, 0.4,
    0.5, 0.63, 0.8, 1.0, 1.25, 1.6, 2.0, 2.5, 3.2, 4.0, 5.0, 6.3, 8.0, 10.0, 12.5, 16.0, 20.0,
    25.0, 32.0, 40.0, 50.0, 63.0, 80.0, 100.0, 125.0,
];

/// A Re-derive build's statement classes, [`BUILD_STATEMENT_METRIC`]'s
/// `class` label (#625 F2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildStatement {
    /// A chunk's catalog reads before its transaction's work: its
    /// definition and target plan, a pooled connection, the begin and its
    /// claim fence.
    ChunkSetup,
    /// A chunk's read of the keys in its range.
    ChunkKeys,
    /// A chunk's entry lock: the sorted `for update` of the keys that had
    /// an entry before its insert.
    ChunkLock,
    /// A chunk's read-and-write statements: the insert of the entries of
    /// keys with none, and the rewrite of the others' (each the snapshot,
    /// the entries and the group-delta insert).
    ChunkWrite,
    /// A chunk's commit, and marking it done.
    ChunkCommit,
    /// A merger pass's catalog reads before its upsert: its definition and
    /// target plan, a pooled connection, the begin and the definition row's
    /// `for key share`.
    MergeSetup,
    /// The merger's claim, delete, sum and group upsert (one statement).
    MergeUpsert,
    /// The merger's rewrite of the recomputed fields of the groups it wrote
    /// (#625 F5): a fold of the entries that entered each, or a recompute
    /// from all of its entries.
    MergeRecompute,
    /// The merger's all-zero group delete and the seam.
    MergeFinish,
    /// The merger's commit.
    MergeCommit,
    /// A merger's periodic vacuum of the target's delta table, after its
    /// commit (#625 F2b, `staging::build::VACUUM_EVERY`).
    MergeVacuum,
    /// The plan job's boundary walk and chunk inserts.
    Plan,
}

impl BuildStatement {
    /// Every class, in label order.
    pub const ALL: [BuildStatement; 12] = [
        BuildStatement::ChunkSetup,
        BuildStatement::ChunkKeys,
        BuildStatement::ChunkLock,
        BuildStatement::ChunkWrite,
        BuildStatement::ChunkCommit,
        BuildStatement::MergeSetup,
        BuildStatement::MergeUpsert,
        BuildStatement::MergeRecompute,
        BuildStatement::MergeFinish,
        BuildStatement::MergeCommit,
        BuildStatement::MergeVacuum,
        BuildStatement::Plan,
    ];

    /// The `class` label.
    pub fn label(self) -> &'static str {
        match self {
            BuildStatement::ChunkSetup => "chunk_setup",
            BuildStatement::ChunkKeys => "chunk_keys",
            BuildStatement::ChunkLock => "chunk_lock",
            BuildStatement::ChunkWrite => "chunk_write",
            BuildStatement::ChunkCommit => "chunk_commit",
            BuildStatement::MergeSetup => "merge_setup",
            BuildStatement::MergeUpsert => "merge_upsert",
            BuildStatement::MergeRecompute => "merge_recompute",
            BuildStatement::MergeFinish => "merge_finish",
            BuildStatement::MergeCommit => "merge_commit",
            BuildStatement::MergeVacuum => "merge_vacuum",
            BuildStatement::Plan => "plan",
        }
    }
}

/// Records `elapsed` worker time in Re-derive build statement class
/// `class` (#625 F2).
pub fn record_build_statement(class: BuildStatement, elapsed: Duration) {
    ensure_installed();
    metrics::histogram!(BUILD_STATEMENT_METRIC, INSTANCE_LABEL => instance(), "class" => class.label())
        .record(elapsed.as_secs_f64());
}

/// Records one committed Re-derive build chunk (#625 F2): its transaction's
/// duration, the keys it re-derived and the delta rows it appended.
pub fn record_build_chunk(elapsed: Duration, keys: u64, delta_rows: u64) {
    ensure_installed();
    let instance = instance();
    metrics::histogram!(BUILD_CHUNK_METRIC, INSTANCE_LABEL => instance.clone())
        .record(elapsed.as_secs_f64());
    let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    let state = gauge_state(&instance_log::current());
    let max = state
        .chunk_max_nanos
        .fetch_max(nanos, Ordering::Relaxed)
        .max(nanos);
    metrics::gauge!(BUILD_CHUNK_MAX_METRIC, INSTANCE_LABEL => instance.clone())
        .set(max as f64 / 1e9);
    metrics::counter!(BUILD_ROWS_METRIC, INSTANCE_LABEL => instance.clone()).increment(keys);
    metrics::counter!(BUILD_DELTA_ROWS_METRIC, INSTANCE_LABEL => instance, "step" => "appended")
        .increment(delta_rows);
}

/// Records group-delta rows a merger folded (#625 F2).
pub fn record_build_merge(delta_rows: u64) {
    ensure_installed();
    metrics::counter!(BUILD_DELTA_ROWS_METRIC, INSTANCE_LABEL => instance(), "step" => "merged")
        .increment(delta_rows);
}

/// Counts a Re-derive build chunk that gave up on its entry lock (#625 F2).
pub fn increment_build_chunk_lock_timeouts() {
    ensure_installed();
    metrics::counter!(BUILD_CHUNK_LOCK_TIMEOUTS_METRIC, INSTANCE_LABEL => instance()).increment(1);
}

/// Counts a wait for the column-pause lock that timed out (#922). `op` is
/// `locks::ColumnPauseOp::label`.
pub fn increment_column_pause_lock_timeouts(op: &'static str) {
    ensure_installed();
    metrics::counter!(COLUMN_PAUSE_LOCK_TIMEOUTS_METRIC, INSTANCE_LABEL => instance(), "op" => op)
        .increment(1);
}

/// Counts a seal refused for a full ring (#625 F2).
pub fn increment_seal_refused() {
    ensure_installed();
    metrics::counter!(SEAL_REFUSED_METRIC, INSTANCE_LABEL => instance()).increment(1);
}

/// The process-wide recorder handle, built and installed on first use. See
/// the module doc comment's "Recorder installation" section.
fn handle() -> &'static PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE.get_or_init(|| {
        let recorder = described_recorder(Some(GAUGE_IDLE_TIMEOUT));
        let handle = recorder.handle();
        // Best-effort install — see the module doc comment. `build_recorder`
        // (rather than `install`/`install_recorder`) is used deliberately:
        // those two are only compiled under the exporter's `http-listener`
        // feature, which this crate does not enable (no bound socket).
        let _ = metrics::set_global_recorder(recorder);
        handle
    })
}

/// A recorder with this crate's buckets and every metric's description, that
/// drops a gauge nothing has set for `gauge_idle_timeout` (none: never).
///
/// The descriptions go to this recorder directly, not through the macros'
/// current-recorder lookup: a thread with a local recorder set (a test's
/// `set_default_local_recorder`) that happens to be first to call
/// [`handle`] would otherwise take every `# HELP` line with it.
fn described_recorder(
    gauge_idle_timeout: Option<Duration>,
) -> metrics_exporter_prometheus::PrometheusRecorder {
    let recorder = PrometheusBuilder::new()
        .idle_timeout(metrics_util::MetricKindMask::GAUGE, gauge_idle_timeout)
        .set_buckets(LATENCY_BUCKETS)
        .expect("LATENCY_BUCKETS is non-empty and every boundary is finite")
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(BUILD_CHUNK_METRIC.to_string()),
            BUILD_CHUNK_BUCKETS,
        )
        .expect("BUILD_CHUNK_BUCKETS is non-empty and every boundary is finite")
        .build_recorder();
    metrics::with_local_recorder(&recorder, describe_metrics);
    recorder
}

/// Registers a `# HELP` description for every metric this module records,
/// once, on the recorder [`described_recorder`] builds. Without this,
/// `metrics-exporter-prometheus` still renders a `# TYPE` line per series
/// (inferred from the macro used to record it — `histogram!`/`counter!`/
/// `gauge!`) but omits `# HELP` entirely, since it has no description to
/// put there; issue #53's exposition is meant to be self-documenting for an
/// operator reading a raw scrape, so every series gets one.
fn describe_metrics() {
    metrics::describe_histogram!(
        BUILD_STATEMENT_METRIC,
        metrics::Unit::Seconds,
        "Worker time a Re-derive build spent per statement class, labeled by class."
    );
    metrics::describe_histogram!(
        BUILD_CHUNK_METRIC,
        metrics::Unit::Seconds,
        "Duration of each committed Re-derive build chunk's transaction."
    );
    metrics::describe_gauge!(
        BUILD_CHUNK_MAX_METRIC,
        metrics::Unit::Seconds,
        "The longest Re-derive build chunk transaction this instance has committed."
    );
    metrics::describe_counter!(
        BUILD_ROWS_METRIC,
        "Count of source keys Re-derive build chunks re-derived."
    );
    metrics::describe_counter!(
        BUILD_DELTA_ROWS_METRIC,
        "Count of group-delta rows Re-derive build chunks appended and mergers folded, labeled \
         by step (appended/merged)."
    );
    metrics::describe_counter!(
        BUILD_CHUNK_LOCK_TIMEOUTS_METRIC,
        "Count of Re-derive build chunks that gave up on their entry lock's timeout and were \
         released for a retry."
    );
    metrics::describe_counter!(
        COLUMN_PAUSE_LOCK_TIMEOUTS_METRIC,
        "Count of waits for the column-pause lock that ran out the transaction's lock_timeout, \
         by the operation that waited."
    );
    metrics::describe_counter!(
        SEAL_REFUSED_METRIC,
        "Count of seals refused because the staging ring had no free slot."
    );
    metrics::describe_histogram!(
        TRANSFORM_LATENCY_METRIC,
        metrics::Unit::Seconds,
        "Time from a change becoming available at a transform's input to its output being \
         applied, labeled by transform."
    );
    metrics::describe_counter!(
        CHANGES_APPLIED_METRIC,
        "Count of staged changes (one per source row change or upstream-hop recompute row) a \
         transform folded and applied, labeled by transform."
    );
    metrics::describe_histogram!(
        END_TO_END_LATENCY_METRIC,
        metrics::Unit::Seconds,
        "Time from the source commit to a terminal (sink) transform's apply, labeled by that \
         terminal transform and summed across every source feeding it."
    );
    metrics::describe_gauge!(
        STAGING_SEGMENTS_METRIC,
        "Count of staging ring segments, labeled by state (active/sealed/draining/drained)."
    );
    metrics::describe_counter!(
        RELATIONSHIP_REVERSE_DEFERRED_METRIC,
        "Count of to-one relationship reverse deltas deferred by a guard rejection, labeled by \
         guard (d5_block_barrier/d5_block_gen/d5_block_inflight/d5_block_order)."
    );
    metrics::describe_counter!(
        RELATIONSHIP_REVERSE_FAIRNESS_ESCALATED_METRIC,
        "Count of to-one relationship reverse transitions that exhausted their guard-gated \
         retry budget and were resolved via the fairness-escalation fallback instead of \
         deferring again."
    );
}

/// Ensures the registry is installed. Every recording function below calls
/// this too, so callers never need to call it explicitly — it's exposed
/// purely so something that wants the registry ready before its first
/// observation (an embedder, a test) can force that at a known point.
pub fn ensure_installed() {
    let _ = handle();
}

/// Records one observation of [`TRANSFORM_LATENCY_METRIC`] for `transform`.
/// Called from `staging::apply::flush_apply_metrics` once per
/// applied change that carries an origin timestamp (`FoldedChange::src_changed`
/// is `None` for a bare recompute trigger with no source change behind it —
/// nothing to measure latency against, so callers skip this and call only
/// [`increment_changes_applied`] for such a change) — only once the batch
/// that produced the observation has actually committed, per that
/// function's own doc comment.
pub fn record_transform_latency(transform: &str, latency: Duration) {
    ensure_installed();
    metrics::histogram!(TRANSFORM_LATENCY_METRIC, INSTANCE_LABEL => instance(), "transform" => transform.to_string())
        .record(latency.as_secs_f64());
}

/// Records one observation of [`END_TO_END_LATENCY_METRIC`] for `transform`
/// — issue #52. Called from `staging::apply::flush_apply_metrics`
/// (same post-commit timing as [`record_transform_latency`] — see that
/// function's doc comment) once per applied change whose *consuming*
/// transform is terminal (no downstream reader — see
/// [`crate::defs::catalog::transforms_for_source`]) and that carries an
/// origin timestamp, mirroring
/// [`record_transform_latency`]'s `src_changed` gate exactly: the value
/// observed is the same `now - src_changed` duration, just gated to
/// terminal transforms and recorded under a different metric name. Reuses
/// [`LATENCY_BUCKETS`], the same global bucket set every histogram in this
/// module shares (ADR-0009 decision 6) — no separate boundary set for this
/// metric.
pub fn record_end_to_end_latency(transform: &str, latency: Duration) {
    ensure_installed();
    metrics::histogram!(END_TO_END_LATENCY_METRIC, INSTANCE_LABEL => instance(), "transform" => transform.to_string())
        .record(latency.as_secs_f64());
}

/// Increments [`CHANGES_APPLIED_METRIC`] by `rows` for `transform`. Called
/// once per applied folded change, from the same post-commit flush as
/// [`record_transform_latency`], with `rows` the number of staged ring rows
/// that change folded (`FoldedChange::row_count`).
pub fn increment_changes_applied(transform: &str, rows: u64) {
    ensure_installed();
    metrics::counter!(CHANGES_APPLIED_METRIC, INSTANCE_LABEL => instance(), "transform" => transform.to_string())
        .increment(rows);
}

/// Sets [`STAGING_SEGMENTS_METRIC`] for `state` to `count` — ADR-0009
/// decision 5's cheap segment-state gauge, refreshed on-demand (today: once
/// per [`crate::client`]'s maintenance tick) rather than incremented on the
/// hot append/fold path. The count is also kept for
/// [`refresh_instance_gauges`], which keeps the series alive while the
/// instance runs but its maintenance tick is not reaching this call (a
/// reconcile pass that takes longer than [`GAUGE_IDLE_TIMEOUT`], say).
pub fn set_staging_segments(state: &str, count: u64) {
    ensure_installed();
    gauge_state(&instance_log::current())
        .segments
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(state.to_string(), count);
    metrics::gauge!(STAGING_SEGMENTS_METRIC, INSTANCE_LABEL => instance(), "state" => state.to_string())
        .set(count as f64);
}

/// What an instance's gauges are set from. The gauges drop out of the
/// recorder after [`GAUGE_IDLE_TIMEOUT`] unset, so a value that changes only
/// on an event (a build chunk's longest duration) has to be kept here and set
/// again on a timer.
#[derive(Default)]
struct GaugeState {
    /// Nanoseconds of the longest build chunk this instance committed. 0
    /// until there is one, and then no series is set.
    chunk_max_nanos: AtomicU64,
    /// The last segment count per state.
    segments: Mutex<BTreeMap<String, u64>>,
}

/// `instance`'s [`GaugeState`]. Entries live as long as the process: one per
/// instance name it has seen.
fn gauge_state(instance: &Arc<str>) -> Arc<GaugeState> {
    static STATES: Mutex<BTreeMap<Arc<str>, Arc<GaugeState>>> = Mutex::new(BTreeMap::new());
    let mut states = STATES.lock().unwrap_or_else(PoisonError::into_inner);
    Arc::clone(states.entry(Arc::clone(instance)).or_default())
}

/// Sets again every gauge the current instance owns, from the values
/// recorded so far, so that [`GAUGE_IDLE_TIMEOUT`] drops only a stopped
/// instance's gauges. Each running [`crate::client::Client`] calls it every
/// maintenance interval, from a task of its own: the maintenance loop does
/// not reach its gauge call on a tick where one of its steps failed, and the
/// build chunk gauge is set by whichever drain worker commits a chunk, which
/// may be a client with no maintenance loop at all.
pub fn refresh_instance_gauges() {
    ensure_installed();
    let name = instance_log::current();
    let state = gauge_state(&name);
    let label = name.to_string();
    let nanos = state.chunk_max_nanos.load(Ordering::Relaxed);
    if nanos > 0 {
        metrics::gauge!(BUILD_CHUNK_MAX_METRIC, INSTANCE_LABEL => label.clone())
            .set(nanos as f64 / 1e9);
    }
    let segments = state
        .segments
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    for (segment_state, count) in segments.iter() {
        metrics::gauge!(STAGING_SEGMENTS_METRIC, INSTANCE_LABEL => label.clone(), "state" => segment_state.clone())
            .set(*count as f64);
    }
}

/// The instance the calling code runs under, as the label value.
fn instance() -> String {
    instance_log::current().to_string()
}

/// How many handles of each instance run in this process. An instance stays
/// listed after its last handle stops, at 0: that is the series
/// `trellis_instance_up` keeps reporting.
#[derive(Default)]
struct Running(Mutex<BTreeMap<Arc<str>, usize>>);

/// The process-wide [`Running`].
static RUNNING: Running = Running(Mutex::new(BTreeMap::new()));

impl Running {
    fn start(&self, name: &Arc<str>) {
        *self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(Arc::clone(name))
            .or_default() += 1;
    }

    fn stop(&self, name: &Arc<str>) {
        if let Some(count) = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(name)
        {
            *count = count.saturating_sub(1);
        }
    }

    /// Appends [`INSTANCE_UP_METRIC`], one series per instance seen.
    fn render(&self, out: &mut String) {
        let running = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if running.is_empty() {
            return;
        }
        let _ = writeln!(
            out,
            "# HELP {INSTANCE_UP_METRIC} 1 while a handle of the instance runs in this process, 0 \
             once the last one has stopped."
        );
        let _ = writeln!(out, "# TYPE {INSTANCE_UP_METRIC} gauge");
        for (name, count) in running.iter() {
            let _ = writeln!(
                out,
                "{INSTANCE_UP_METRIC}{{{INSTANCE_LABEL}=\"{}\"}} {}",
                escape_label_value(name),
                u8::from(*count > 0)
            );
        }
    }
}

/// `value` as a quoted Prometheus label value's contents.
fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Marks an instance as running in this process for as long as it lives
/// (issue #873): `trellis_instance_up` is 1 while at least one has not been
/// dropped, and 0 after the last one is, for as long as the process lives.
/// [`crate::app::Trellis`] and [`crate::client::Client`] each hold one.
///
/// The series is rendered from [`RUNNING`], a registry that outlives every
/// handle, rather than recorded as a gauge: the recorder would drop a gauge
/// nothing sets for [`GAUGE_IDLE_TIMEOUT`], and a stopped instance's 0 must
/// stay.
pub(crate) struct RunningInstance {
    name: Arc<str>,
}

impl RunningInstance {
    pub(crate) fn new(name: &Arc<str>) -> Self {
        RUNNING.start(name);
        Self {
            name: Arc::clone(name),
        }
    }
}

impl Drop for RunningInstance {
    fn drop(&mut self) {
        RUNNING.stop(&self.name);
    }
}

/// Increments [`RELATIONSHIP_REVERSE_DEFERRED_METRIC`] by one for `guard`
/// (issue #134/#135). Called from
/// `staging::apply::flush_relationship_reverse_deferral_metrics`, itself
/// called from `drain_once`/`drain_many` immediately after (and only after)
/// their own `txn.commit().await?` succeeds — buffered, not recorded
/// eagerly from inside Phase 3 (`apply_and_mark_drained_many`'s "3d" step,
/// where the rejection is discovered) — matching
/// `record_transform_latency`/`increment_changes_applied`'s own established
/// pattern (`ApplyPlan`'s buffered fields, flushed by `flush_apply_metrics`
/// post-commit): review follow-up to this issue found the eager version
/// double- (or triple-, ...) counted whenever `drain_once`/`drain_many`'s
/// retry loop re-ran the same folded input's Phase 3 pass in full on a
/// `VersionFenceMiss`/transient failure (doc 06's `FenceMissBackoff` — a
/// routine occurrence, not a rare edge case), inflating exactly the counter
/// #135's fairness/starvation decisions need to read accurately, and doing
/// so most under the high-contention conditions where those decisions
/// matter most.
pub fn increment_relationship_reverse_deferred(guard: &str) {
    ensure_installed();
    metrics::counter!(RELATIONSHIP_REVERSE_DEFERRED_METRIC, INSTANCE_LABEL => instance(), "guard" => guard.to_string())
        .increment(1);
}

/// Increments [`RELATIONSHIP_REVERSE_FAIRNESS_ESCALATED_METRIC`] by one
/// (issue #135). Called from
/// `staging::apply::flush_relationship_reverse_fairness_escalation_metric`,
/// under the same post-commit-only contract as
/// [`increment_relationship_reverse_deferred`] — see that function's own doc
/// comment for why (the same `VersionFenceMiss`/transient-failure retry loop
/// can attempt the same folded input's Phase 3 pass more than once).
pub fn increment_relationship_reverse_fairness_escalated() {
    ensure_installed();
    metrics::counter!(RELATIONSHIP_REVERSE_FAIRNESS_ESCALATED_METRIC, INSTANCE_LABEL => instance())
        .increment(1);
}

/// A handle onto this process's in-process metrics registry — the public,
/// embedder-facing entry point for Prometheus exposition (issue #53).
///
/// Obtained via [`crate::app::Trellis::metrics`] (or
/// [`crate::blocking::BlockingTrellis::metrics`]), matching
/// `docs/observability.md`'s `trellis.metrics().render_prometheus()` sketch.
/// Carries no fields: per the module doc comment's "Recorder installation"
/// section, recording happens against one process-wide global registry, not
/// one scoped to a particular `Trellis` connection, so there's no per-instance
/// state to hold. Every handle in the process renders the same body: each
/// series carries its instance's `trellis_instance` label. It's a named type rather than a bare free function so the
/// `trellis.metrics().render_prometheus()` method chain reads naturally and
/// so a future addition (another export format, say) has an obvious home;
/// [`Metrics::new`] is public too since some callers (this crate's own
/// integration tests, the CLI's `--prometheus-bind` listener) read the
/// registry without going through a full [`crate::app::Trellis`]
/// connection.
#[derive(Debug, Clone, Copy)]
pub struct Metrics {
    _private: (),
}

impl Metrics {
    /// Ensures the registry is installed (see [`ensure_installed`]) and
    /// returns a handle onto it. Cheap and side-effect-free beyond that
    /// first-call installation — safe to call as often as wanted.
    pub fn new() -> Self {
        ensure_installed();
        Self { _private: () }
    }

    /// Renders the registry's current contents in Prometheus text exposition
    /// format (`# HELP`/`# TYPE` lines followed by each series' samples).
    ///
    /// Just a `String` — no HTTP framework, no bound socket (ADR-0009
    /// decision 1; `docs/observability.md`'s "Exposition: a mountable
    /// handler, not a bound port"). The operator serves the result from
    /// their own HTTP stack's `/metrics` route, e.g. with the `text/plain;
    /// version=0.0.4` content type Prometheus's exposition format expects:
    ///
    /// ```no_run
    /// # async fn example(trellis: &trellis::Trellis) {
    /// let body = trellis.metrics().render_prometheus();
    /// // ...serve `body` from an axum/actix/hyper (or hand-rolled, as
    /// // `cli/src/commands/run.rs`'s `--prometheus-bind` does) `/metrics` route...
    /// # }
    /// ```
    pub fn render_prometheus(&self) -> String {
        let mut rendered = handle().render();
        RUNNING.render(&mut rendered);
        rendered
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;

    use super::*;

    #[test]
    fn transform_latency_and_changes_applied_are_recorded_and_render() {
        record_transform_latency("metrics_facade_test_target", Duration::from_millis(120));
        increment_changes_applied("metrics_facade_test_target", 1);

        let rendered = Metrics::new().render_prometheus();
        assert!(
            rendered.contains("trellis_transform_latency_seconds"),
            "rendered output missing the latency histogram: {rendered}"
        );
        assert!(
            rendered.contains("trellis_changes_applied_total"),
            "rendered output missing the throughput counter: {rendered}"
        );
        assert!(
            rendered.contains("metrics_facade_test_target"),
            "rendered output missing the transform label: {rendered}"
        );
    }

    #[test]
    fn end_to_end_latency_is_recorded_and_renders_with_the_shared_bucket_set() {
        record_end_to_end_latency("metrics_facade_test_terminal", Duration::from_millis(250));

        let rendered = Metrics::new().render_prometheus();
        assert!(
            rendered.contains("trellis_end_to_end_latency_seconds"),
            "rendered output missing the end-to-end latency histogram: {rendered}"
        );
        assert!(
            rendered.contains("metrics_facade_test_terminal"),
            "rendered output missing the transform label: {rendered}"
        );
        // ADR-0009 decision 6: this histogram reuses LATENCY_BUCKETS, the
        // same global default the per-transform histogram uses — spot-check
        // one boundary shared by both rather than asserting the whole set,
        // since `render_prometheus` renders every histogram's buckets
        // interleaved.
        assert!(
            rendered.contains("le=\"0.25\""),
            "rendered output missing a LATENCY_BUCKETS boundary: {rendered}"
        );
    }

    /// #625 F2: a build chunk's histogram renders with its own ~25% buckets,
    /// not [`LATENCY_BUCKETS`], since the profile reads its percentiles off
    /// them.
    #[test]
    fn build_chunk_seconds_render_with_their_own_buckets() {
        record_build_chunk(Duration::from_millis(150), 10, 3);

        let rendered = Metrics::new().render_prometheus();
        for le in ["0.125", "0.16", "0.2"] {
            assert!(
                rendered.contains(&format!(
                    "{BUILD_CHUNK_METRIC}_bucket{{trellis_instance=\"unknown\",le=\"{le}\"}}"
                )),
                "missing the chunk histogram's {le} bound: {rendered}"
            );
        }
        assert!(
            rendered.contains(&format!("{BUILD_CHUNK_MAX_METRIC}{{")),
            "missing the chunk max gauge: {rendered}"
        );
    }

    #[test]
    fn staging_segments_gauge_is_recorded_and_renders() {
        set_staging_segments("metrics_facade_test_state", 3);

        let rendered = Metrics::new().render_prometheus();
        assert!(
            rendered.contains("trellis_staging_segments"),
            "rendered output missing the segment-state gauge: {rendered}"
        );
        assert!(
            rendered.contains("metrics_facade_test_state"),
            "rendered output missing the state label: {rendered}"
        );
    }

    /// A thread with a local recorder set (a test's own registry) can be the
    /// first to build the global one. Its descriptions
    /// must still land on the global recorder: they used to follow the
    /// local one, and every `# HELP` line was missing from the process's
    /// scrape (which failed the test below whenever a test with a local
    /// recorder ran first).
    #[test]
    fn a_local_recorder_does_not_take_the_descriptions() {
        let local = PrometheusBuilder::new().build_recorder();
        let _guard = metrics::set_default_local_recorder(&local);

        let recorder = described_recorder(None);
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!(CHANGES_APPLIED_METRIC, "transform" => "t").increment(1);
        });

        let rendered = recorder.handle().render();
        assert!(
            rendered.contains(&format!("# HELP {CHANGES_APPLIED_METRIC} ")),
            "the description went elsewhere: {rendered}"
        );
    }

    /// Runs `f` with a registry of the test's own as the recorder, built as
    /// the global one is (this crate's buckets and descriptions, and
    /// `gauge_idle_timeout` on gauges). `f` gets the registry's rendering.
    fn local(gauge_idle_timeout: Option<Duration>, f: impl FnOnce(&dyn Fn() -> String)) {
        let recorder = described_recorder(gauge_idle_timeout);
        let render = || recorder.handle().render();
        metrics::with_local_recorder(&recorder, || f(&render));
    }

    fn name(s: &str) -> Arc<str> {
        Arc::from(s)
    }

    /// The value of the series `metric` whose label block contains every one
    /// of `labels`, if exactly one does.
    fn sample(rendered: &str, metric: &str, labels: &[&str]) -> Option<f64> {
        let mut found = rendered.lines().filter(|line| {
            line.strip_prefix(metric)
                .is_some_and(|rest| rest.starts_with('{'))
                && labels.iter().all(|label| line.contains(label))
        });
        let line = found.next()?;
        assert!(
            found.next().is_none(),
            "more than one series matches {labels:?}: {rendered}"
        );
        line.rsplit(' ').next()?.parse().ok()
    }

    fn label(instance: &str) -> String {
        format!("{INSTANCE_LABEL}=\"{instance}\"")
    }

    /// Records one of every metric this module has, under the current
    /// instance.
    fn record_everything() {
        record_build_statement(BuildStatement::Plan, Duration::from_millis(1));
        record_build_chunk(Duration::from_millis(1), 1, 1);
        record_build_merge(1);
        increment_build_chunk_lock_timeouts();
        increment_column_pause_lock_timeouts("pause");
        increment_seal_refused();
        record_transform_latency("t", Duration::from_millis(1));
        record_end_to_end_latency("t", Duration::from_millis(1));
        increment_changes_applied("t", 1);
        set_staging_segments("active", 1);
        increment_relationship_reverse_deferred("d5_block_order");
        increment_relationship_reverse_fairness_escalated();
    }

    /// Issue #873: two instances in one process write the same transform
    /// name and stay two series, each with its own count.
    #[test]
    fn two_instances_writing_the_same_transform_stay_two_series() {
        local(None, |render| {
            let (a, b) = (name("db_a/trellis"), name("db_b/trellis"));
            instance_log::run_scoped(&a, || {
                increment_changes_applied("same", 1);
                record_transform_latency("same", Duration::from_millis(10));
            });
            instance_log::run_scoped(&b, || {
                increment_changes_applied("same", 2);
                increment_changes_applied("same", 4);
            });

            let rendered = render();
            let applied = |instance: &str| {
                sample(
                    &rendered,
                    CHANGES_APPLIED_METRIC,
                    &[&label(instance), "transform=\"same\""],
                )
            };
            assert_eq!(applied("db_a/trellis"), Some(1.0), "{rendered}");
            assert_eq!(applied("db_b/trellis"), Some(6.0), "{rendered}");
            assert_eq!(
                sample(
                    &rendered,
                    &format!("{TRANSFORM_LATENCY_METRIC}_count"),
                    &[&label("db_a/trellis")]
                ),
                Some(1.0),
                "{rendered}"
            );
            assert_eq!(
                sample(
                    &rendered,
                    &format!("{TRANSFORM_LATENCY_METRIC}_count"),
                    &[&label("db_b/trellis")]
                ),
                None,
                "{rendered}"
            );
        });
    }

    /// Issue #873: each instance's gauges keep their own value, and the
    /// build chunk maximum is per instance (it was one process-wide static).
    #[test]
    fn each_instances_gauges_keep_their_own_value() {
        local(None, |render| {
            let (a, b) = (name("gauge_a/trellis"), name("gauge_b/trellis"));
            instance_log::run_scoped(&a, || {
                set_staging_segments("active", 3);
                record_build_chunk(Duration::from_secs(5), 1, 1);
                record_build_chunk(Duration::from_secs(2), 1, 1);
            });
            instance_log::run_scoped(&b, || {
                set_staging_segments("active", 9);
                record_build_chunk(Duration::from_secs(1), 1, 1);
            });

            let rendered = render();
            let segments = |instance: &str| {
                sample(
                    &rendered,
                    STAGING_SEGMENTS_METRIC,
                    &[&label(instance), "state=\"active\""],
                )
            };
            assert_eq!(segments("gauge_a/trellis"), Some(3.0), "{rendered}");
            assert_eq!(segments("gauge_b/trellis"), Some(9.0), "{rendered}");
            let max =
                |instance: &str| sample(&rendered, BUILD_CHUNK_MAX_METRIC, &[&label(instance)]);
            assert_eq!(max("gauge_a/trellis"), Some(5.0), "{rendered}");
            assert_eq!(max("gauge_b/trellis"), Some(1.0), "{rendered}");
        });
    }

    /// Issue #873: every series carries the label, including one recorded
    /// outside any instance.
    #[test]
    fn every_series_carries_the_instance_label() {
        local(None, |render| {
            instance_log::run_scoped(&name("every/trellis"), record_everything);
            record_everything();

            let rendered = render();
            let series: Vec<&str> = rendered
                .lines()
                .filter(|line| !line.is_empty() && !line.starts_with('#'))
                .collect();
            assert!(series.len() > 12, "{rendered}");
            for line in &series {
                assert!(
                    line.contains(&label("every/trellis")) || line.contains(&label("unknown")),
                    "a series without the instance label: {line}"
                );
            }
            for metric in [
                BUILD_STATEMENT_METRIC,
                BUILD_CHUNK_METRIC,
                BUILD_CHUNK_MAX_METRIC,
                BUILD_ROWS_METRIC,
                BUILD_DELTA_ROWS_METRIC,
                BUILD_CHUNK_LOCK_TIMEOUTS_METRIC,
                COLUMN_PAUSE_LOCK_TIMEOUTS_METRIC,
                SEAL_REFUSED_METRIC,
                TRANSFORM_LATENCY_METRIC,
                END_TO_END_LATENCY_METRIC,
                CHANGES_APPLIED_METRIC,
                STAGING_SEGMENTS_METRIC,
                RELATIONSHIP_REVERSE_DEFERRED_METRIC,
                RELATIONSHIP_REVERSE_FAIRNESS_ESCALATED_METRIC,
            ] {
                assert!(
                    series
                        .iter()
                        .any(|line| line.starts_with(metric)
                            && line.contains(&label("every/trellis"))),
                    "{metric} was not recorded under the instance: {rendered}"
                );
            }
        });
    }

    /// A new recording site must go through the label: no `metrics` macro in
    /// this file's non-test code leaves it out.
    #[test]
    fn every_recording_macro_names_the_instance_label() {
        let source = include_str!("metrics.rs");
        let code = source.split("#[cfg(test)]\nmod tests").next().unwrap();
        let mut sites = 0;
        for macro_name in ["counter!(", "gauge!(", "histogram!("] {
            for (at, _) in code.match_indices(&format!("metrics::{macro_name}")) {
                let call = &code[at..];
                let call = &call[..call.find(')').unwrap()];
                sites += 1;
                assert!(
                    call.contains("INSTANCE_LABEL"),
                    "no instance label: {call})"
                );
            }
        }
        assert!(sites >= 14, "found {sites} recording sites");
    }

    /// Issue #873: a gauge nothing sets for the idle timeout drops out, one
    /// the running instance sets again stays, and counters never drop.
    #[test]
    fn a_stopped_instances_gauges_expire_and_a_running_ones_stay() {
        // Every render is one look at each gauge; a gauge unchanged since
        // the last look and older than the timeout is dropped on this one.
        let timeout = Duration::from_millis(1);
        local(Some(timeout), |render| {
            let (running, stopped) = (
                name("expiry_running/trellis"),
                name("expiry_stopped/trellis"),
            );
            for instance in [&running, &stopped] {
                instance_log::run_scoped(instance, || {
                    set_staging_segments("active", 1);
                    record_build_chunk(Duration::from_millis(7), 1, 1);
                });
            }
            let first = render();
            for instance in ["expiry_running/trellis", "expiry_stopped/trellis"] {
                assert!(
                    sample(&first, STAGING_SEGMENTS_METRIC, &[&label(instance)]).is_some(),
                    "{first}"
                );
            }

            std::thread::sleep(timeout * 5);
            // The running instance's tick.
            instance_log::run_scoped(&running, refresh_instance_gauges);
            let second = render();

            let gauges = |instance: &str| {
                (
                    sample(&second, STAGING_SEGMENTS_METRIC, &[&label(instance)]),
                    sample(&second, BUILD_CHUNK_MAX_METRIC, &[&label(instance)]),
                )
            };
            assert_eq!(
                gauges("expiry_running/trellis"),
                (Some(1.0), Some(0.007)),
                "{second}"
            );
            assert_eq!(gauges("expiry_stopped/trellis"), (None, None), "{second}");
            assert_eq!(
                sample(
                    &second,
                    &format!("{BUILD_CHUNK_METRIC}_count"),
                    &[&label("expiry_stopped/trellis")]
                ),
                Some(1.0),
                "a histogram must outlive its instance: {second}"
            );
            assert_eq!(
                sample(
                    &second,
                    BUILD_ROWS_METRIC,
                    &[&label("expiry_stopped/trellis")]
                ),
                Some(1.0),
                "a counter must outlive its instance: {second}"
            );
        });
    }

    /// The control for the expiry test above: the same two looks at a
    /// recorder built without an idle timeout keep the gauge, so it is the
    /// timeout that drops one.
    #[test]
    fn without_an_idle_timeout_a_gauge_is_kept() {
        local(None, |render| {
            instance_log::run_scoped(&name("no_expiry/trellis"), || {
                set_staging_segments("active", 2)
            });
            let _ = render();
            std::thread::sleep(Duration::from_millis(5));
            let gauge = sample(
                &render(),
                STAGING_SEGMENTS_METRIC,
                &[&label("no_expiry/trellis")],
            );
            assert_eq!(gauge, Some(2.0));
        });
    }

    /// Issue #873: `trellis_instance_up` is 1 while a handle runs and stays
    /// listed at 0 after the last one stops.
    #[test]
    fn instance_up_is_one_while_running_and_zero_after() {
        let running = Running::default();
        let render = |running: &Running| {
            let mut out = String::new();
            running.render(&mut out);
            out
        };
        assert_eq!(render(&running), "", "no instance seen, no series");

        let (a, b) = (name("up_a/trellis"), name("up_b/trellis"));
        running.start(&a);
        running.start(&a);
        running.start(&b);
        let rendered = render(&running);
        assert!(
            rendered.contains(&format!("# TYPE {INSTANCE_UP_METRIC} gauge")),
            "{rendered}"
        );
        assert_eq!(
            sample(&rendered, INSTANCE_UP_METRIC, &[&label("up_a/trellis")]),
            Some(1.0)
        );
        assert_eq!(
            sample(&rendered, INSTANCE_UP_METRIC, &[&label("up_b/trellis")]),
            Some(1.0)
        );

        running.stop(&b);
        running.stop(&a);
        let rendered = render(&running);
        assert_eq!(
            sample(&rendered, INSTANCE_UP_METRIC, &[&label("up_a/trellis")]),
            Some(1.0),
            "a second handle of the instance still runs"
        );
        assert_eq!(
            sample(&rendered, INSTANCE_UP_METRIC, &[&label("up_b/trellis")]),
            Some(0.0)
        );

        running.stop(&a);
        running.stop(&a);
        let rendered = render(&running);
        assert_eq!(
            sample(&rendered, INSTANCE_UP_METRIC, &[&label("up_a/trellis")]),
            Some(0.0),
            "{rendered}"
        );
    }

    /// The process-wide registry follows the handle's life, and its series
    /// reach the scrape body.
    #[test]
    fn a_running_instance_guard_drives_the_rendered_series() {
        let instance = name("guard_test/trellis");
        let guard = RunningInstance::new(&instance);
        let up = |rendered: &str| {
            sample(
                rendered,
                INSTANCE_UP_METRIC,
                &[&label("guard_test/trellis")],
            )
        };
        assert_eq!(up(&Metrics::new().render_prometheus()), Some(1.0));
        drop(guard);
        assert_eq!(up(&Metrics::new().render_prometheus()), Some(0.0));
    }

    #[test]
    fn a_label_value_is_escaped() {
        assert_eq!(escape_label_value("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }

    /// Issue #53's acceptance criteria calls for "a snapshot test on the
    /// rendered body." A byte-exact snapshot isn't a good fit here: the
    /// registry is one process-wide global (see the module doc comment's
    /// "Recorder installation" section) shared by every test in this binary,
    /// so the exact set/order of series `render_prometheus()` returns
    /// depends on whichever other tests happened to run first in this
    /// process — not something this test controls or should pin to. Instead
    /// this asserts the *shape* is valid Prometheus text exposition format:
    /// every metric this crate records gets a `# HELP`/`# TYPE` pair, and
    /// every non-comment sample line parses as `name{labels} value`.
    #[test]
    fn render_prometheus_produces_a_valid_exposition_format_body() {
        record_transform_latency("metrics_shape_test_target", Duration::from_millis(42));
        increment_changes_applied("metrics_shape_test_target", 1);
        record_end_to_end_latency("metrics_shape_test_target", Duration::from_millis(84));
        set_staging_segments("metrics_shape_test_state", 7);

        let rendered = Metrics::new().render_prometheus();

        for metric in [
            TRANSFORM_LATENCY_METRIC,
            CHANGES_APPLIED_METRIC,
            END_TO_END_LATENCY_METRIC,
            STAGING_SEGMENTS_METRIC,
        ] {
            assert!(
                rendered.contains(&format!("# HELP {metric} ")),
                "rendered output missing a HELP line for {metric}: {rendered}"
            );
            assert!(
                rendered.contains(&format!("# TYPE {metric} ")),
                "rendered output missing a TYPE line for {metric}: {rendered}"
            );
        }

        // Shape-check every non-comment, non-blank line against the
        // exposition format's sample-line grammar (metric name, optional
        // `{label="value", ...}` block, whitespace, a value) — loose enough
        // to tolerate metrics-exporter-prometheus's own label/bucket
        // ordering, strict enough to catch a gross regression (labels or
        // values landing somewhere they shouldn't).
        let sample_line =
            Regex::new(r#"^[a-zA-Z_:][a-zA-Z0-9_:]*(\{[^}]*\})?\s+\S+$"#).expect("valid regex");
        for line in rendered.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            assert!(
                sample_line.is_match(line),
                "line does not look like a valid Prometheus sample: {line:?}"
            );
        }

        assert!(
            rendered.contains("metrics_shape_test_target"),
            "rendered output missing the transform label: {rendered}"
        );
        assert!(
            rendered.contains("metrics_shape_test_state"),
            "rendered output missing the state label: {rendered}"
        );
    }
}
