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
//! Cross-instance aggregation (summing buckets/counters across every engine
//! process scraping into the same Prometheus) is deliberately left to
//! Prometheus itself, not persisted by this crate — see
//! `docs/observability.md`'s "Retention" section.
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

use std::sync::OnceLock;
use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

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

/// Throughput denominator for [`TRANSFORM_LATENCY_METRIC`]
/// (`docs/observability.md`'s "Recommended supporting counters/gauges so
/// the histograms are interpretable"): one increment per applied change,
/// recorded at the same call site as the latency observation.
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

/// Issue #325: count of times the client's CDC intake stopped and was
/// restarted, labeled `outcome` (`error`: `run()` returned an error, or a
/// restart's reconnect failed; `stream_ended`: the replication stream closed).
/// Any sustained non-zero rate means source changes are not being staged.
/// The matching `error!` log line carries the actual error.
const INTAKE_RESTARTS_METRIC: &str = "trellis_intake_restarts_total";

/// The process-wide recorder handle, built and installed on first use. See
/// the module doc comment's "Recorder installation" section.
fn handle() -> &'static PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE.get_or_init(|| {
        let builder = PrometheusBuilder::new()
            .set_buckets(LATENCY_BUCKETS)
            .expect("LATENCY_BUCKETS is non-empty and every boundary is finite");
        let recorder = builder.build_recorder();
        let handle = recorder.handle();
        // Best-effort install — see the module doc comment. `build_recorder`
        // (rather than `install`/`install_recorder`) is used deliberately:
        // those two are only compiled under the exporter's `http-listener`
        // feature, which this crate does not enable (no bound socket).
        let _ = metrics::set_global_recorder(recorder);
        describe_metrics();
        handle
    })
}

/// Registers a `# HELP` description for every metric this module records,
/// once, right after [`handle`] installs the global recorder. Without this,
/// `metrics-exporter-prometheus` still renders a `# TYPE` line per series
/// (inferred from the macro used to record it — `histogram!`/`counter!`/
/// `gauge!`) but omits `# HELP` entirely, since it has no description to
/// put there; issue #53's exposition is meant to be self-documenting for an
/// operator reading a raw scrape, so every series gets one.
fn describe_metrics() {
    metrics::describe_histogram!(
        TRANSFORM_LATENCY_METRIC,
        metrics::Unit::Seconds,
        "Time from a change becoming available at a transform's input to its output being \
         applied, labeled by transform."
    );
    metrics::describe_counter!(
        CHANGES_APPLIED_METRIC,
        "Count of changes applied, labeled by transform — the throughput denominator for \
         trellis_transform_latency_seconds."
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
    metrics::describe_counter!(
        INTAKE_RESTARTS_METRIC,
        "Count of times CDC intake stopped and was restarted, labeled by outcome \
         (error/stream_ended). Source changes are not staged while intake is down."
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
    metrics::histogram!(TRANSFORM_LATENCY_METRIC, "transform" => transform.to_string())
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
    metrics::histogram!(END_TO_END_LATENCY_METRIC, "transform" => transform.to_string())
        .record(latency.as_secs_f64());
}

/// Increments [`CHANGES_APPLIED_METRIC`] by one for `transform`. Called
/// once per applied change, at the same call site as
/// [`record_transform_latency`] (when that change carries an origin
/// timestamp) so the two series stay consistent.
pub fn increment_changes_applied(transform: &str) {
    ensure_installed();
    metrics::counter!(CHANGES_APPLIED_METRIC, "transform" => transform.to_string()).increment(1);
}

/// Sets [`STAGING_SEGMENTS_METRIC`] for `state` to `count` — ADR-0009
/// decision 5's cheap segment-state gauge, refreshed on-demand (today: once
/// per [`crate::client`]'s maintenance tick) rather than incremented on the
/// hot append/fold path.
pub fn set_staging_segments(state: &str, count: u64) {
    ensure_installed();
    metrics::gauge!(STAGING_SEGMENTS_METRIC, "state" => state.to_string()).set(count as f64);
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
    metrics::counter!(RELATIONSHIP_REVERSE_DEFERRED_METRIC, "guard" => guard.to_string())
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
    metrics::counter!(RELATIONSHIP_REVERSE_FAIRNESS_ESCALATED_METRIC).increment(1);
}

/// Increments [`INTAKE_RESTARTS_METRIC`] by one for `outcome` (issue #325).
/// Called from `client::supervise_intake` each time intake stops.
pub fn increment_intake_restarts(outcome: &str) {
    ensure_installed();
    metrics::counter!(INTAKE_RESTARTS_METRIC, "outcome" => outcome.to_string()).increment(1);
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
/// state to hold. It's a named type rather than a bare free function so the
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
        handle().render()
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
        increment_changes_applied("metrics_facade_test_target");

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
        increment_changes_applied("metrics_shape_test_target");
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
