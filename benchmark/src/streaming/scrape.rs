//! Readers for [`trellis::Metrics::render_prometheus`] output.
//!
//! No bucket-boundary changes, no raw-sample recording, no new histograms:
//! issue #266 states its latency targets at exactly
//! [`trellis::metrics::LATENCY_BUCKETS`]' existing boundaries, so a
//! cumulative-bucket-fraction read off a scrape answers them with no
//! interpolation and no estimator. The `bucket_count`/`total_count` shape
//! here mirrors the engine's own `tests/end_to_end_latency.rs` private
//! helpers of the same name (this crate can't import those — they're private
//! to another crate's test binary — so this is a deliberate small
//! re-implementation, not a divergent one).
//!
//! **Bucket label format**: `metrics-exporter-prometheus` renders a
//! whole-number `f64` bound with no trailing `.0` (`le="1"`, not `le="1.0"`),
//! which is why [`LE_MAX`] is `"1"`.
//!
//! **Why everything here diffs against a baseline**: the metrics registry is
//! process-wide ([`trellis::metrics`]' `OnceLock` handle), not
//! per-`Client`/per-database. A scenario that starts several clients in one
//! process — a depth ladder looping over depths, a ramp looping over rates —
//! sees every earlier run's counts still sitting in the same series. Taking a
//! scrape immediately before a measurement window and subtracting it isolates
//! that window's own contribution. Every series read here is cumulative
//! (Prometheus convention: bucket counts and `_count`/`_total` only increase),
//! so the subtraction is exact, not an approximation.

use std::collections::HashMap;

/// `trellis_end_to_end_latency_seconds` — source commit to terminal apply,
/// the histogram T1 is stated against.
pub const END_TO_END_LATENCY_METRIC: &str = "trellis_end_to_end_latency_seconds";
/// `trellis_transform_latency_seconds` — recorded per *transform*, which is
/// what makes [`per_hop_mean_ms`] possible for every hop rather than only the
/// terminal one.
pub const TRANSFORM_LATENCY_METRIC: &str = "trellis_transform_latency_seconds";
/// `trellis_changes_applied_total` — the counter every scenario cross-checks
/// against the rows its generator actually committed.
pub const CHANGES_APPLIED_METRIC: &str = "trellis_changes_applied_total";

/// The `le` label for T1's p50 boundary (<= 250 ms).
pub const LE_P50: &str = "0.25";
/// The `le` label for T1's p99 boundary (<= 500 ms).
pub const LE_P99: &str = "0.5";
/// The `le` label for T1's "reliably < 1 s" boundary.
pub const LE_MAX: &str = "1";

/// The three T1 boundaries, in scrape order.
pub const T1_BOUNDS: [&str; 3] = [LE_P50, LE_P99, LE_MAX];

/// Takes one scrape of the process-wide registry.
pub fn scrape() -> String {
    trellis::Metrics::new().render_prometheus()
}

/// One histogram series' cumulative counts at the bounds asked for (not every
/// bound the histogram carries), plus its total sample count, for one
/// `metric{transform="..."}` series.
#[derive(Debug, Clone, Default)]
pub struct HistogramSnapshot {
    buckets: HashMap<String, u64>,
    pub count: u64,
}

impl HistogramSnapshot {
    pub fn capture(rendered: &str, metric: &str, transform: &str, bounds: &[&str]) -> Self {
        let buckets = bounds
            .iter()
            .map(|le| {
                (
                    (*le).to_string(),
                    bucket_count(rendered, metric, transform, le),
                )
            })
            .collect();
        Self {
            buckets,
            count: total_count(rendered, metric, transform),
        }
    }

    /// This snapshot's contribution since `baseline` — see the module doc
    /// comment on why a diff, not a raw scrape, is what a scenario wants.
    pub fn since(&self, baseline: &HistogramSnapshot) -> HistogramSnapshot {
        let buckets = self
            .buckets
            .iter()
            .map(|(le, count)| {
                let base = baseline.buckets.get(le).copied().unwrap_or(0);
                (le.clone(), count.saturating_sub(base))
            })
            .collect();
        HistogramSnapshot {
            buckets,
            count: self.count.saturating_sub(baseline.count),
        }
    }

    /// The cumulative count captured for bound `le` (`0` if `le` wasn't asked
    /// for at capture time, or was genuinely never observed).
    pub fn bucket(&self, le: &str) -> u64 {
        self.buckets.get(le).copied().unwrap_or(0)
    }

    /// `bucket(le) / count`, or `0.0` when nothing was observed.
    pub fn fraction(&self, le: &str) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.bucket(le) as f64 / self.count as f64
        }
    }
}

/// The trailing value on the first line matching `prefix`-and-`labels`, or
/// `None` if no such line exists — a scrape taken before a fresh transform
/// has ever been observed legitimately has no line at all, which is not an
/// error.
fn metric_line_value(rendered: &str, prefix: &str, labels: &[String]) -> Option<f64> {
    rendered
        .lines()
        .find(|line| {
            line.starts_with(prefix) && labels.iter().all(|label| line.contains(label.as_str()))
        })
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

fn transform_label(transform: &str) -> String {
    format!("transform=\"{transform}\"")
}

/// The cumulative count in one histogram bucket for
/// `metric{transform=...,le=...}`, `0` if absent.
fn bucket_count(rendered: &str, metric: &str, transform: &str, le: &str) -> u64 {
    metric_line_value(
        rendered,
        &format!("{metric}_bucket"),
        &[transform_label(transform), format!("le=\"{le}\"")],
    )
    .unwrap_or(0.0) as u64
}

/// A histogram's total sample count (`<metric>_count{transform=...}`), `0` if
/// absent.
fn total_count(rendered: &str, metric: &str, transform: &str) -> u64 {
    metric_line_value(
        rendered,
        &format!("{metric}_count"),
        &[transform_label(transform)],
    )
    .unwrap_or(0.0) as u64
}

/// A plain counter's current value for one `metric{transform=...}` series
/// (e.g. [`CHANGES_APPLIED_METRIC`]) — no `_bucket`/`_count` suffix, since a
/// counter has neither. `0` if absent.
pub fn counter_value(rendered: &str, metric: &str, transform: &str) -> u64 {
    metric_line_value(rendered, metric, &[transform_label(transform)]).unwrap_or(0.0) as u64
}

/// One histogram's `_sum` and `_count` for a series — the pair that gives an
/// **exact** mean, with no bucket-boundary estimation error, unlike
/// [`HistogramSnapshot`]'s deliberately coarse boundary fractions.
///
/// Both are cumulative Prometheus counters, so a post-minus-pre subtraction
/// over a measurement window is exact. This is what makes `per_hop_mean_ms`
/// legible (#268): `trellis_transform_latency_seconds` already fires per
/// transform, so every hop's mean is sitting in a scrape the harness was
/// already taking — no new instrumentation, and none of T1's bucket
/// coarseness.
#[derive(Debug, Clone, Copy, Default)]
pub struct SumCountSnapshot {
    pub sum: f64,
    pub count: u64,
}

impl SumCountSnapshot {
    pub fn capture(rendered: &str, metric: &str, transform: &str) -> Self {
        Self {
            sum: metric_line_value(
                rendered,
                &format!("{metric}_sum"),
                &[transform_label(transform)],
            )
            .unwrap_or(0.0),
            count: total_count(rendered, metric, transform),
        }
    }

    pub fn since(&self, baseline: &SumCountSnapshot) -> SumCountSnapshot {
        SumCountSnapshot {
            sum: (self.sum - baseline.sum).max(0.0),
            count: self.count.saturating_sub(baseline.count),
        }
    }

    /// Exact mean in milliseconds over this window, or `None` when nothing
    /// was observed (rather than a bogus `0.0/0`).
    pub fn mean_ms(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.sum / self.count as f64 * 1000.0)
        }
    }
}

/// Exact per-hop mean latency (ms) for every `transform` in `hops`, over the
/// window between `baseline` and `rendered_after` (#268's `per_hop_mean_ms`).
/// `None` for a hop with no samples in the window. Index 0 is hop 1.
pub fn per_hop_mean_ms(
    rendered_after: &str,
    hops: &[String],
    baseline: &[SumCountSnapshot],
) -> Vec<Option<f64>> {
    hops.iter()
        .zip(baseline.iter())
        .map(|(hop, base)| {
            SumCountSnapshot::capture(rendered_after, TRANSFORM_LATENCY_METRIC, hop)
                .since(base)
                .mean_ms()
        })
        .collect()
}

/// A baseline [`SumCountSnapshot`] of [`TRANSFORM_LATENCY_METRIC`] for every
/// hop, to pair with [`per_hop_mean_ms`].
pub fn per_hop_baseline(rendered_before: &str, hops: &[String]) -> Vec<SumCountSnapshot> {
    hops.iter()
        .map(|hop| SumCountSnapshot::capture(rendered_before, TRANSFORM_LATENCY_METRIC, hop))
        .collect()
}

/// Renders a `per_hop_mean_ms` vector as a JSON array, `null` for a hop with
/// no samples.
pub fn per_hop_json(per_hop_mean_ms: &[Option<f64>]) -> String {
    per_hop_mean_ms
        .iter()
        .map(|v| match v {
            Some(ms) => format!("{ms:.3}"),
            None => "null".to_string(),
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// T1's three targets (#266) as cumulative-bucket-fraction pass/fail against
/// one histogram window. Deliberately reports fractions, not an interpolated
/// percentile — the issue's own words: "'p99 <= 500 ms' is answerable; 'our
/// p99 is 380 ms' is not."
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct T1Evaluation {
    /// Total end-to-end observations in the window. `0` means nothing was
    /// observed at all — check the commits-issued vs. changes-applied
    /// cross-check before trusting anything else here.
    pub count: u64,
    /// `bucket(le=0.25) / count` — target `>= 0.50`.
    pub p50_frac: f64,
    /// `bucket(le=0.5) / count` — target `>= 0.99`.
    pub p99_frac: f64,
    /// `bucket(le=1) == count` — every observation under 1 s.
    pub max_ok: bool,
    pub p50_pass: bool,
    pub p99_pass: bool,
}

impl T1Evaluation {
    pub fn evaluate(window: &HistogramSnapshot) -> Self {
        if window.count == 0 {
            return Self {
                count: 0,
                p50_frac: 0.0,
                p99_frac: 0.0,
                max_ok: false,
                p50_pass: false,
                p99_pass: false,
            };
        }
        let p50_frac = window.fraction(LE_P50);
        let p99_frac = window.fraction(LE_P99);
        Self {
            count: window.count,
            p50_frac,
            p99_frac,
            max_ok: window.bucket(LE_MAX) == window.count,
            p50_pass: p50_frac >= 0.50,
            p99_pass: p99_frac >= 0.99,
        }
    }

    pub fn all_pass(&self) -> bool {
        self.count > 0 && self.p50_pass && self.p99_pass && self.max_ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"0.25\"} 5\n\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"0.5\"} 9\n\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"1\"} 10\n\
trellis_end_to_end_latency_seconds_count{transform=\"t\"} 10\n\
trellis_changes_applied_total{transform=\"t\"} 10\n";

    fn e2e(rendered: &str) -> HistogramSnapshot {
        HistogramSnapshot::capture(rendered, END_TO_END_LATENCY_METRIC, "t", &T1_BOUNDS)
    }

    #[test]
    fn captures_bucket_and_total_counts() {
        let snap = e2e(SAMPLE);
        assert_eq!(snap.count, 10);
        assert_eq!(snap.bucket(LE_P50), 5);
        assert_eq!(snap.bucket(LE_P99), 9);
        assert_eq!(snap.bucket(LE_MAX), 10);
        assert_eq!(counter_value(SAMPLE, CHANGES_APPLIED_METRIC, "t"), 10);
    }

    #[test]
    fn absent_series_reads_as_zero_rather_than_panicking() {
        let snap = e2e("");
        assert_eq!(snap.count, 0);
        assert_eq!(snap.bucket(LE_P50), 0);
        assert_eq!(snap.fraction(LE_P50), 0.0);
        assert_eq!(counter_value("", CHANGES_APPLIED_METRIC, "t"), 0);
    }

    #[test]
    fn since_subtracts_a_baseline_cumulative_scrape() {
        let baseline = e2e(SAMPLE);
        // A later scrape after 5 more observations, all under the p50 bound.
        let later = "\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"0.25\"} 10\n\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"0.5\"} 14\n\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"1\"} 15\n\
trellis_end_to_end_latency_seconds_count{transform=\"t\"} 15\n";
        let window = e2e(later).since(&baseline);
        assert_eq!(window.count, 5);
        assert_eq!(window.bucket(LE_P50), 5);
        assert_eq!(window.bucket(LE_P99), 5);
        assert_eq!(window.bucket(LE_MAX), 5);
    }

    #[test]
    fn t1_evaluation_matches_the_issues_boundary_fractions() {
        let eval = T1Evaluation::evaluate(&e2e(SAMPLE));
        assert_eq!(eval.p50_frac, 0.5);
        assert!(eval.p50_pass, "5/10 == 0.50 satisfies the >= 0.50 boundary");
        assert!(
            !eval.p99_pass,
            "9/10 == 0.90 is under the >= 0.99 p99 boundary"
        );
        assert!(eval.max_ok, "bucket(le=1) == count in this fixture");
        assert!(!eval.all_pass());
    }

    #[test]
    fn zero_observations_never_reports_a_pass() {
        let eval = T1Evaluation::evaluate(&HistogramSnapshot::default());
        assert_eq!(eval.count, 0);
        assert!(!eval.all_pass());
    }

    const PER_HOP_SAMPLE: &str = "\
trellis_transform_latency_seconds_sum{transform=\"h1\"} 2.5\n\
trellis_transform_latency_seconds_count{transform=\"h1\"} 10\n\
trellis_transform_latency_seconds_sum{transform=\"h2\"} 6.0\n\
trellis_transform_latency_seconds_count{transform=\"h2\"} 10\n";

    #[test]
    fn sum_count_snapshot_computes_exact_mean_ms() {
        let snap = SumCountSnapshot::capture(PER_HOP_SAMPLE, TRANSFORM_LATENCY_METRIC, "h1");
        assert_eq!(snap.sum, 2.5);
        assert_eq!(snap.count, 10);
        assert_eq!(snap.mean_ms(), Some(250.0));
    }

    #[test]
    fn sum_count_snapshot_zero_count_has_no_mean() {
        assert_eq!(SumCountSnapshot::default().mean_ms(), None);
    }

    #[test]
    fn per_hop_means_diff_every_hop_against_its_own_baseline() {
        let hops = vec!["h1".to_string(), "h2".to_string(), "h3".to_string()];
        let baseline = per_hop_baseline(PER_HOP_SAMPLE, &hops);
        let later = "\
trellis_transform_latency_seconds_sum{transform=\"h1\"} 3.5\n\
trellis_transform_latency_seconds_count{transform=\"h1\"} 20\n\
trellis_transform_latency_seconds_sum{transform=\"h2\"} 10.0\n\
trellis_transform_latency_seconds_count{transform=\"h2\"} 30\n";
        let means = per_hop_mean_ms(later, &hops, &baseline);
        // h1: 1.0s over 10 new samples; h2: 4.0s over 20; h3 never observed.
        assert_eq!(means, vec![Some(100.0), Some(200.0), None]);
        assert_eq!(per_hop_json(&means), "100.000,200.000,null");
    }
}
