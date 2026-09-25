//! The in-window rate check every throughput verdict rests on (issues #276,
//! #319).
//!
//! A probe offers load for a window, then waits up to a grace period for the
//! pipeline to catch up. "Caught up within grace" alone is not a throughput
//! verdict: a pipeline running at half the offered rate still drains a 20s
//! window's backlog inside a 30s grace, and a ramp judged that way reports a
//! knee well above what the engine can actually hold. What the verdict needs
//! is the rate the pipeline processed rows at *while* load was arriving.
//!
//! [`sample_progress`] samples a monotone "rows processed so far" signal
//! across the offer window (after [`SETTLE_FRACTION`] of it), [`fitted_rate`]
//! fits a line through the samples, and [`kept_target_rate`] is the verdict:
//! drained **and** the fitted rate within [`GENERATOR_UNDERSHOOT_TOLERANCE`]
//! of the target. A pipeline that keeps up tracks the offered rate at a
//! constant lag, so its slope equals the offered rate whatever that lag is;
//! one that falls behind has the slope of its own, lower rate.

use std::future::Future;
use std::time::{Duration, Instant};

use crate::streaming::load::GENERATOR_UNDERSHOOT_TOLERANCE;

/// How much of the offer window to skip before sampling: long enough for the
/// pipeline's start-up lag (first seal, first claim) to pass, short enough to
/// leave most of the window to fit over.
pub const SETTLE_FRACTION: f64 = 0.25;
const SAMPLE_INTERVAL: Duration = Duration::from_millis(200);

/// Samples `progress` (rows processed so far) every [`SAMPLE_INTERVAL`] from
/// [`SETTLE_FRACTION`] of the way into the offer window until it closes, as
/// `(seconds since offer_start, rows)` pairs. Runs alongside the generator,
/// so `progress` must never use a generator connection.
pub async fn sample_progress<F, Fut>(
    offer_start: Instant,
    duration: Duration,
    mut progress: F,
) -> Vec<(f64, f64)>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = f64>,
{
    tokio::time::sleep_until((offer_start + duration.mul_f64(SETTLE_FRACTION)).into()).await;
    let window_end = offer_start + duration;
    let mut samples = Vec::new();
    while Instant::now() < window_end {
        // A query reads one snapshot as of its start, so stamp it then.
        let at = offer_start.elapsed().as_secs_f64();
        samples.push((at, progress().await));
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
    samples
}

/// Least-squares slope of `samples`' `y` over `x`: rows per second. A fit over
/// many samples rather than a two-point difference, because the pipeline
/// commits in batches: the progress signal is a step function, and two
/// points can each land anywhere in a step. `None` with fewer than two
/// distinct `x`s.
pub fn fitted_rate(samples: &[(f64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let n = samples.len() as f64;
    let mean_x = samples.iter().map(|(x, _)| x).sum::<f64>() / n;
    let mean_y = samples.iter().map(|(_, y)| y).sum::<f64>() / n;
    let (mut sxy, mut sxx) = (0.0, 0.0);
    for (x, y) in samples {
        sxy += (x - mean_x) * (y - mean_y);
        sxx += (x - mean_x) * (x - mean_x);
    }
    (sxx > 0.0).then(|| sxy / sxx)
}

/// The throughput verdict: the backlog drained within grace, **and** the
/// pipeline processed rows at the target rate (within
/// [`GENERATOR_UNDERSHOOT_TOLERANCE`]) while load was arriving. Fails closed:
/// no fit (too few samples) is not a pass.
pub fn kept_target_rate(
    drained: bool,
    in_window_rate: Option<f64>,
    target_rows_per_sec: f64,
) -> bool {
    drained
        && in_window_rate.is_some_and(|rate| {
            rate >= target_rows_per_sec * (1.0 - GENERATOR_UNDERSHOOT_TOLERANCE)
        })
}

/// Whether the engine kept up with the load it actually *received* — the
/// `engine_kept_up` input to [`crate::streaming::load::generator_bound`]:
/// drained, and processed rows at the generator's achieved rate while it was
/// offered. `drained` alone would call a probe generator-bound when the
/// generator undershot but the engine couldn't hold even that lower rate,
/// only drain it within grace — blaming the instrument for an engine limit.
pub fn kept_up_with_offer(
    drained: bool,
    in_window_rate: Option<f64>,
    achieved_rows_per_sec: f64,
) -> bool {
    kept_target_rate(drained, in_window_rate, achieved_rows_per_sec)
}

/// A rate as JSON: one decimal, or `null` when there is none.
pub fn json_rate(rate: Option<f64>) -> String {
    match rate {
        Some(rate) => format!("{rate:.1}"),
        None => "null".to_string(),
    }
}

/// A rate for a human-readable line.
pub fn human_rate(rate: Option<f64>) -> String {
    match rate {
        Some(rate) => format!("{rate:.0} rows/sec"),
        None => "(too few samples to fit)".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rows processed so far for a pipeline offered `rate` rows/sec that
    /// commits in batches every `batch_secs`, each batch landing `lag_secs`
    /// after the rows it holds were committed, sampled every 200ms across
    /// `span`.
    fn batched_progress(
        rate: f64,
        lag_secs: f64,
        batch_secs: f64,
        span: (f64, f64),
    ) -> Vec<(f64, f64)> {
        let mut samples = Vec::new();
        let mut t = span.0;
        while t < span.1 {
            let visible_through = t - lag_secs;
            let batches = (visible_through / batch_secs).floor().max(0.0);
            samples.push((t, rate * batches * batch_secs));
            t += 0.2;
        }
        samples
    }

    #[test]
    fn a_pipeline_that_keeps_up_at_a_lag_processes_at_the_offered_rate() {
        // 400k rows/sec over a 5s window, in 150ms batches half a second
        // behind: keeping up exactly. A drain-time rate reads this as 5 / 5.5
        // of the target, but the in-window slope doesn't depend on the lag.
        let samples = batched_progress(400_000.0, 0.5, 0.15, (1.25, 5.0));
        let rate = fitted_rate(&samples).expect("enough samples");
        assert!(
            (rate - 400_000.0).abs() < 400_000.0 * 0.01,
            "slope {rate} should track the offered 400k"
        );
        assert!(kept_target_rate(true, Some(rate), 400_000.0));
    }

    #[test]
    fn a_pipeline_that_falls_behind_processes_at_its_own_rate() {
        let samples = batched_progress(85_000.0, 0.5, 0.15, (5.0, 20.0));
        let rate = fitted_rate(&samples).expect("enough samples");
        assert!((rate - 85_000.0).abs() < 85_000.0 * 0.01, "slope {rate}");
        assert!(!kept_target_rate(true, Some(rate), 400_000.0));
    }

    /// Issue #319's false green: at half the target, a 20s window's backlog
    /// (10s of rows) drains well inside a 30s grace, so "drained" alone read
    /// as sustained. The in-window rate says otherwise.
    #[test]
    fn half_the_target_rate_is_not_kept_even_when_it_drains_in_grace() {
        let target = 100_000.0;
        let samples = batched_progress(target * 0.5, 0.3, 0.1, (5.0, 20.0));
        let rate = fitted_rate(&samples).expect("enough samples");
        let drained = true;
        assert!(!kept_target_rate(drained, Some(rate), target));
    }

    /// The generator undershot (80k of 100k) and the backlog drained, but the
    /// engine processed only 40k/sec of what it got: an engine measurement,
    /// not a generator-bound one.
    #[test]
    fn an_undershoot_the_engine_only_drained_in_grace_is_not_generator_bound() {
        use crate::streaming::load::generator_bound;
        let slow = kept_up_with_offer(true, Some(40_000.0), 80_000.0);
        assert!(!generator_bound(Some(100_000.0), 80_000.0, slow));
        let kept = kept_up_with_offer(true, Some(79_500.0), 80_000.0);
        assert!(generator_bound(Some(100_000.0), 80_000.0, kept));
    }

    #[test]
    fn kept_target_rate_needs_a_drain_and_a_fit() {
        assert!(!kept_target_rate(false, Some(400_000.0), 400_000.0));
        assert!(!kept_target_rate(true, None, 400_000.0));
        assert!(kept_target_rate(true, Some(393_000.0), 400_000.0));
        assert!(!kept_target_rate(true, Some(390_000.0), 400_000.0));
    }

    #[test]
    fn too_few_samples_fit_no_line() {
        assert_eq!(fitted_rate(&[]), None);
        assert_eq!(fitted_rate(&[(1.0, 10.0)]), None);
        assert_eq!(fitted_rate(&[(1.0, 10.0), (1.0, 20.0)]), None);
        assert_eq!(fitted_rate(&[(1.0, 10.0), (2.0, 30.0)]), Some(20.0));
    }
}
