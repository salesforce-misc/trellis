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
//! across the offer window (after [`SETTLE_FRACTION`] of it),
//! [`in_window_rate`] fits a line through the samples, and
//! [`kept_target_rate`] is the verdict: drained **and** the fitted rate within
//! the fit's tolerance of the rate offered. A pipeline that keeps up tracks
//! the offered rate at a constant lag, so its slope equals the offered rate
//! whatever that lag is; one that falls behind has the slope of its own,
//! lower rate.
//!
//! ## How exact the fit is, and the tolerance that follows
//!
//! Progress is a staircase, not a line: rows arrive a commit at a time and
//! become visible a sealed segment at a time. A least-squares slope through a
//! sampled staircase that exactly keeps up still misses the offered rate, in
//! two ways that a fixed 2% tolerance does not absorb (issue #319's review):
//!
//! * **Aliasing.** Sampled at a fixed period close to the step period (a 200ms
//!   sampler against a 200-300ms seal/poll cadence), the sampler sees the
//!   steps drift slowly through its ticks and reads the beat as a slope. In
//!   simulation a 202.5ms sampler against a 200ms cadence read a pipeline
//!   keeping up exactly ~1.2% low, at every phase and window length.
//!   [`sample_progress`] therefore spaces samples by a golden-ratio
//!   sequence (uniform over 0.5-1.5x the mean interval, never periodic), and
//!   an in-process counter is sampled every 50ms rather than 200ms.
//! * **Step size.** With steps `P` seconds apart over a fitted span `T`, the
//!   partial steps at the span's two ends bias the slope by up to about
//!   `2.5 (P/T)^2` of the rate, whatever the sampling. At 10,000 rows/commit
//!   and 20k rows/sec (`P` = 0.5s) over an 8s window (`T` = 6s), the worst
//!   phase reads 1.7% low sampled every 50ms and 2.4% low every 200ms: the
//!   whole 2% on its own. [`in_window_rate`] widens the tolerance by
//!   `3 (P/T)^2`, with `P` the coarser of the commit interval and the seal
//!   cadence, and declines to fit at all (fails closed) with fewer than
//!   [`MIN_STEPS_IN_FIT`] steps in the span.
//!
//! Simulated over random lags and phases, a pipeline that exactly keeps up
//! then reads within its tolerance for every commit shape from 1 to 37,500
//! rows/commit and windows of 8s and longer, with at least 1.2% to spare
//! (unless the window holds too few commits to fit). At the default 20s
//! windows the widening is about 0.3% at most for the shapes the scenarios
//! use. Below 8s the fit's noise grows past the tolerance again.

use std::future::Future;
use std::time::{Duration, Instant};

use crate::streaming::load::GENERATOR_UNDERSHOOT_TOLERANCE;

/// How much of the offer window to skip before sampling: long enough for the
/// pipeline's start-up lag (first seal, first claim) to pass, short enough to
/// leave most of the window to fit over.
pub const SETTLE_FRACTION: f64 = 0.25;

/// Mean sample interval for an in-process counter: a read with no database
/// round trip, so dense sampling costs nothing and shrinks the fit's noise.
pub const COUNTER_SAMPLE_INTERVAL: Duration = Duration::from_millis(50);

/// Mean sample interval for a progress signal read by SQL, which competes
/// with the drain it is watching.
pub const QUERY_SAMPLE_INTERVAL: Duration = Duration::from_millis(200);

/// Fewest progress steps (see [`progress_step_secs`]) the fitted span must
/// cover for the fit to be a verdict. Below it the step-size bias would need
/// a tolerance so wide the check would pass a pipeline falling behind.
pub const MIN_STEPS_IN_FIT: f64 = 8.0;

/// Multiplier on `(P/T)^2` (see the module docs) added to
/// [`GENERATOR_UNDERSHOOT_TOLERANCE`]: above the ~2.5 the end effects can
/// reach, so a pipeline that keeps up is never failed on commit shape alone.
const STEP_BIAS_ALLOWANCE: f64 = 3.0;

/// The `k`th gap between samples: `mean` scaled by a value in `[0.5, 1.5)`
/// from the golden-ratio sequence, which covers that range evenly and never
/// repeats, so no fixed engine cadence can alias with the sampler.
fn dithered_gap(mean: Duration, k: u64) -> Duration {
    const GOLDEN_FRACTION: f64 = 0.618_033_988_749_895;
    mean.mul_f64(0.5 + (k as f64 * GOLDEN_FRACTION).fract())
}

/// Samples `progress` (rows processed so far) from [`SETTLE_FRACTION`] of
/// the way into the offer window until it closes, as `(seconds since
/// offer_start, rows)` pairs, at dithered gaps averaging `mean_interval` —
/// [`COUNTER_SAMPLE_INTERVAL`] or [`QUERY_SAMPLE_INTERVAL`]. Runs alongside
/// the generator, so `progress` must never use a generator connection.
pub async fn sample_progress<F, Fut>(
    offer_start: Instant,
    duration: Duration,
    mean_interval: Duration,
    mut progress: F,
) -> Vec<(f64, f64)>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = f64>,
{
    tokio::time::sleep_until((offer_start + duration.mul_f64(SETTLE_FRACTION)).into()).await;
    let window_end = offer_start + duration;
    let mut samples = Vec::new();
    let mut k = 0;
    while Instant::now() < window_end {
        // A query reads one snapshot as of its start, so stamp it then.
        let at = offer_start.elapsed().as_secs_f64();
        samples.push((at, progress().await));
        k += 1;
        tokio::time::sleep(dithered_gap(mean_interval, k)).await;
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

/// The coarsest spacing, in seconds, at which a probe's progress signal can
/// advance: one commit (`rows_per_commit` at `target_rows_per_sec`) or one
/// sealed segment (`seal_interval`, the engine's maintenance cadence),
/// whichever is longer.
pub fn progress_step_secs(
    rows_per_commit: usize,
    target_rows_per_sec: f64,
    seal_interval: Duration,
) -> f64 {
    (rows_per_commit as f64 / target_rows_per_sec).max(seal_interval.as_secs_f64())
}

/// A probe's in-window rate and the tolerance it is judged with.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InWindowRate {
    pub rows_per_sec: f64,
    /// [`GENERATOR_UNDERSHOOT_TOLERANCE`] widened by the step-size bias this
    /// fit can carry (see the module docs).
    pub tolerance: f64,
}

/// Fits the in-window rate to `samples` of a signal that advances in steps
/// up to `step_secs` apart ([`progress_step_secs`]). `None` — which fails
/// [`kept_target_rate`] — when there is no line to fit or the fitted span
/// covers fewer than [`MIN_STEPS_IN_FIT`] steps.
pub fn in_window_rate(samples: &[(f64, f64)], step_secs: f64) -> Option<InWindowRate> {
    let rows_per_sec = fitted_rate(samples)?;
    let span = samples.last()?.0 - samples.first()?.0;
    if span < MIN_STEPS_IN_FIT * step_secs {
        return None;
    }
    Some(InWindowRate {
        rows_per_sec,
        tolerance: GENERATOR_UNDERSHOOT_TOLERANCE
            + STEP_BIAS_ALLOWANCE * (step_secs / span).powi(2),
    })
}

/// The throughput verdict: the backlog drained within grace, **and** the
/// pipeline processed rows at the offered rate (within the fit's
/// [`InWindowRate::tolerance`]) while load was arriving. Fails closed: no
/// fit is not a pass.
///
/// The offered rate is the target, or the generator's `achieved` rate when
/// that was lower. An undershoot inside [`GENERATOR_UNDERSHOOT_TOLERANCE`]
/// still counts as offering the target, and holding the engine to the full
/// target then would leave it almost none of the fit's tolerance. A larger
/// undershoot makes the probe
/// [`generator_bound`](crate::streaming::load::generator_bound) exactly when
/// this verdict holds: the engine kept up with everything it got, and the
/// probe measured the generator.
pub fn kept_target_rate(
    drained: bool,
    in_window: Option<InWindowRate>,
    target_rows_per_sec: f64,
    achieved_rows_per_sec: f64,
) -> bool {
    let offered = target_rows_per_sec.min(achieved_rows_per_sec);
    drained && in_window.is_some_and(|fit| fit.rows_per_sec >= offered * (1.0 - fit.tolerance))
}

/// Issue #423's cross-check: `trellis_changes_applied_total` counts staged
/// source rows, one per committed row, so it can only pass the rows committed
/// if something staged a source row a second time inside the window (a
/// catch-up backfill's discharge stages every source row again). The probe
/// then measured more work than it offered. The run fails on it, so the
/// probe's own verdict must not claim a pass either (#509).
pub fn restaged_in_window(changes_applied: u64, rows_issued: u64) -> bool {
    changes_applied > rows_issued
}

/// An in-window rate as JSON: one decimal, or `null` when there is none.
pub fn json_rate(rate: Option<f64>) -> String {
    match rate {
        Some(rate) => format!("{rate:.1}"),
        None => "null".to_string(),
    }
}

/// An in-window fit as the JSON fields `"<name>":<rate>` and
/// `"rate_tolerance":<tolerance>`, `null`s when there is no fit.
pub fn json_in_window(name: &str, in_window: Option<InWindowRate>) -> String {
    format!(
        "\"{name}\":{},\"rate_tolerance\":{}",
        json_rate(in_window.map(|fit| fit.rows_per_sec)),
        match in_window {
            Some(fit) => format!("{:.4}", fit.tolerance),
            None => "null".to_string(),
        }
    )
}

/// An in-window fit for a human-readable line.
pub fn human_rate(in_window: Option<InWindowRate>) -> String {
    match in_window {
        Some(fit) => format!("{:.0} rows/sec", fit.rows_per_sec),
        None => "(no fit: too few samples or commits in the window)".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic pipeline: `rows_per_commit`-row commits offered at
    /// `rate` rows/sec from t = 0, each visible `lag` seconds after it
    /// commits, rounded up to the next tick of a `seal`-second cadence
    /// starting at `seal_phase` (`seal` = 0: no sealing).
    #[derive(Clone, Copy)]
    struct Staircase {
        rate: f64,
        rows_per_commit: f64,
        lag: f64,
        seal: f64,
        seal_phase: f64,
    }

    impl Staircase {
        fn exact(rate: f64, rows_per_commit: usize) -> Self {
            Self {
                rate,
                rows_per_commit: rows_per_commit as f64,
                lag: 0.0,
                seal: 0.0,
                seal_phase: 0.0,
            }
        }

        /// Rows visible at `t`.
        fn at(&self, t: f64) -> f64 {
            let commit_interval = self.rows_per_commit / self.rate;
            // The latest commit whose rows are visible by `t`: committed by
            // `t - lag`, and sealed by a tick at or before `t`.
            let sealed_by = if self.seal > 0.0 {
                ((t - self.seal_phase) / self.seal).floor() * self.seal + self.seal_phase
            } else {
                t
            };
            let committed_by = sealed_by - self.lag;
            if committed_by < 0.0 {
                return 0.0;
            }
            ((committed_by / commit_interval).floor() + 1.0) * self.rows_per_commit
        }

        /// Samples `[SETTLE_FRACTION * window, window)` at the gaps
        /// `sample_progress` would take (`dithered`), or at a fixed period
        /// (the pre-review sampler).
        fn sample(&self, window: f64, mean_interval: f64, dithered: bool) -> Vec<(f64, f64)> {
            let mean = Duration::from_secs_f64(mean_interval);
            let mut samples = Vec::new();
            let mut t = window * SETTLE_FRACTION;
            let mut k = 0;
            while t < window {
                samples.push((t, self.at(t)));
                k += 1;
                t += if dithered {
                    dithered_gap(mean, k).as_secs_f64()
                } else {
                    mean_interval
                };
            }
            samples
        }
    }

    /// Every lag/seal-phase combination on a grid: the fit has to hold for
    /// whichever phase a real run happens to land on.
    fn phases(base: &Staircase) -> Vec<Staircase> {
        let mut out = Vec::new();
        for lag_step in 0..60 {
            for phase_step in 0..6 {
                out.push(Staircase {
                    lag: 0.02 + 0.0231 * lag_step as f64,
                    seal_phase: base.seal * phase_step as f64 / 6.0,
                    ..*base
                });
            }
        }
        out
    }

    const SEAL: Duration = Duration::from_millis(300);

    /// The judged fit for `pipeline`'s progress over a `window`-second offer.
    fn judge(pipeline: &Staircase, window: f64, mean_interval: f64) -> Option<InWindowRate> {
        let step = progress_step_secs(pipeline.rows_per_commit as usize, pipeline.rate, SEAL);
        in_window_rate(&pipeline.sample(window, mean_interval, true), step)
    }

    #[test]
    fn a_pipeline_that_keeps_up_at_a_lag_processes_at_the_offered_rate() {
        // 400k rows/sec over a 5s window, half a second behind: keeping up
        // exactly. A drain-time rate reads this as 5 / 5.5 of the target, but
        // the in-window slope doesn't depend on the lag.
        let pipeline = Staircase {
            lag: 0.5,
            ..Staircase::exact(400_000.0, 200)
        };
        let fit = judge(&pipeline, 5.0, 0.2).expect("enough steps");
        assert!(
            (fit.rows_per_sec - 400_000.0).abs() < 400_000.0 * 0.01,
            "slope {} should track the offered 400k",
            fit.rows_per_sec
        );
        assert!(kept_target_rate(true, Some(fit), 400_000.0, 400_000.0));
    }

    /// The review's false-red check: a pipeline that keeps up *exactly*,
    /// quantized by commit shape and seal cadence, passes at every phase for
    /// every commit shape the scenarios use, down to an 8s window, whether
    /// sampled as a counter (50ms) or by SQL (200ms) — unless the window is
    /// too short to hold [`MIN_STEPS_IN_FIT`] of its commits, where there is
    /// no fit to judge.
    #[test]
    fn an_exactly_keeping_up_staircase_passes_at_every_phase_and_shape() {
        for (rate, rows_per_commit) in [
            (20_000.0, 1),
            (20_000.0, 100),
            (20_000.0, 1_000),
            (20_000.0, 10_000),
            (20_000.0, 20_000),
            (100_000.0, 2_000),
            (400_000.0, 200),
        ] {
            for window in [8.0, 10.0, 20.0] {
                for mean_interval in [0.05, 0.2] {
                    for seal in [0.0, 0.2, 0.3] {
                        let base = Staircase {
                            seal,
                            ..Staircase::exact(rate, rows_per_commit)
                        };
                        let span = window * (1.0 - SETTLE_FRACTION);
                        let step = progress_step_secs(rows_per_commit, rate, SEAL);
                        if span < MIN_STEPS_IN_FIT * step {
                            continue;
                        }
                        for pipeline in phases(&base) {
                            let fit = judge(&pipeline, window, mean_interval);
                            assert!(
                                kept_target_rate(true, fit, rate, rate),
                                "{rows_per_commit} rows/commit at {rate} rows/sec, {window}s \
                                 window, {mean_interval}s samples, seal {seal}, lag {}: {fit:?}",
                                pipeline.lag
                            );
                        }
                    }
                }
            }
        }
    }

    /// The 10,000-row shape at 20k rows/sec over an 8s window (issue #319's
    /// smoke read 19,622 against a 19,600 bar): the partial steps at the
    /// span's ends alone push some exact-keep-up phases past a flat 2%, which
    /// the step-size allowance absorbs.
    #[test]
    fn coarse_commits_in_a_short_window_need_the_step_allowance() {
        let fits: Vec<InWindowRate> = [0.0, 0.2, 0.3]
            .into_iter()
            .flat_map(|seal| {
                phases(&Staircase {
                    seal,
                    ..Staircase::exact(20_000.0, 10_000)
                })
            })
            .map(|p| judge(&p, 8.0, 0.2).expect("16 commits over a 6s span is enough"))
            .collect();
        let worst = fits
            .iter()
            .map(|fit| fit.rows_per_sec / 20_000.0)
            .fold(f64::INFINITY, f64::min);
        assert!(
            worst < 1.0 - GENERATOR_UNDERSHOOT_TOLERANCE,
            "some phase should read below a flat 2% bar (worst {worst})"
        );
        for fit in fits {
            assert!(fit.tolerance > GENERATOR_UNDERSHOOT_TOLERANCE + 0.01);
            assert!(
                kept_target_rate(true, Some(fit), 20_000.0, 20_000.0),
                "{fit:?}"
            );
        }
    }

    /// Fixed-period sampling at a period just off the seal cadence reads a
    /// pipeline that keeps up exactly as ~1.2% slow at *every* window length:
    /// the beat between the two looks like a slope. Dithered sampling
    /// doesn't.
    #[test]
    fn dithered_sampling_does_not_alias_with_the_seal_cadence() {
        let pipeline = Staircase {
            seal: 0.2,
            lag: 0.3,
            ..Staircase::exact(100_000.0, 2_000)
        };
        let fixed = fitted_rate(&pipeline.sample(20.0, 0.2025, false)).unwrap() / 100_000.0;
        assert!(fixed < 0.99, "fixed-period samples alias: {fixed}");
        let dithered = fitted_rate(&pipeline.sample(20.0, 0.2025, true)).unwrap() / 100_000.0;
        assert!((dithered - 1.0).abs() < 0.005, "dithered: {dithered}");
    }

    #[test]
    fn dithered_gaps_average_the_mean_and_stay_within_half_of_it() {
        let mean = Duration::from_millis(200);
        let gaps: Vec<f64> = (1..=1000)
            .map(|k| dithered_gap(mean, k).as_secs_f64())
            .collect();
        assert!(gaps.iter().all(|g| (0.1..0.3).contains(g)));
        let average = gaps.iter().sum::<f64>() / gaps.len() as f64;
        assert!((average - 0.2).abs() < 0.001, "average gap {average}");
    }

    /// The allowance doesn't swallow a real shortfall: at the ramp's shape
    /// and the default 20s window, a pipeline 3% short fails at every phase.
    #[test]
    fn a_pipeline_three_percent_short_still_fails_at_the_default_window() {
        let base = Staircase {
            seal: 0.3,
            ..Staircase::exact(100_000.0 * 0.97, 2_000)
        };
        for pipeline in phases(&base) {
            let fit = judge(&pipeline, 20.0, 0.05);
            assert!(
                !kept_target_rate(true, fit, 100_000.0, 100_000.0),
                "lag {}: {fit:?}",
                pipeline.lag
            );
        }
    }

    #[test]
    fn a_pipeline_that_falls_behind_processes_at_its_own_rate() {
        let pipeline = Staircase {
            lag: 0.5,
            seal: 0.15,
            ..Staircase::exact(85_000.0, 1_700)
        };
        let fit = judge(&pipeline, 20.0, 0.05).expect("enough steps");
        assert!(
            (fit.rows_per_sec - 85_000.0).abs() < 85_000.0 * 0.01,
            "slope {}",
            fit.rows_per_sec
        );
        assert!(!kept_target_rate(true, Some(fit), 400_000.0, 400_000.0));
    }

    /// Issue #319's false green: at half the target, a 20s window's backlog
    /// (10s of rows) drains well inside a 30s grace, so "drained" alone read
    /// as sustained. The in-window rate says otherwise.
    #[test]
    fn half_the_target_rate_is_not_kept_even_when_it_drains_in_grace() {
        let target = 100_000.0;
        let pipeline = Staircase {
            lag: 0.3,
            seal: 0.1,
            ..Staircase::exact(target * 0.5, 1_000)
        };
        let fit = judge(&pipeline, 20.0, 0.05);
        let drained = true;
        assert!(!kept_target_rate(drained, fit, target, target));
    }

    /// Fewer than `MIN_STEPS_IN_FIT` commits in the fitted span: no fit, so
    /// no pass, rather than a verdict on a handful of steps.
    #[test]
    fn too_few_commits_in_the_span_fit_no_rate() {
        // 20,000-row commits at 20k rows/sec: one a second, six in an 8s
        // window's 6s span.
        let pipeline = Staircase::exact(20_000.0, 20_000);
        assert_eq!(judge(&pipeline, 8.0, 0.05), None);
        assert!(!kept_target_rate(true, None, 20_000.0, 20_000.0));
        // Over a 20s window's 15s span there are enough.
        assert!(judge(&pipeline, 20.0, 0.05).is_some());
    }

    /// An undershoot inside the generator's tolerance is judged against what
    /// was offered: an engine that kept up with 98.5% of the target isn't
    /// failed for the generator's 1.5%.
    #[test]
    fn a_small_generator_undershoot_is_judged_against_what_was_offered() {
        let fit = Some(InWindowRate {
            rows_per_sec: 19_650.0,
            tolerance: GENERATOR_UNDERSHOOT_TOLERANCE,
        });
        assert!(kept_target_rate(true, fit, 20_000.0, 19_700.0));
        // Offered the full target, 19,650 would still be inside 2%...
        assert!(kept_target_rate(true, fit, 20_000.0, 20_400.0));
        // ...but not 19,500.
        let short = Some(InWindowRate {
            rows_per_sec: 19_500.0,
            tolerance: GENERATOR_UNDERSHOOT_TOLERANCE,
        });
        assert!(!kept_target_rate(true, short, 20_000.0, 20_400.0));
    }

    /// The generator undershot (80k of 100k) and the backlog drained, but the
    /// engine processed only 40k/sec of what it got: an engine measurement,
    /// not a generator-bound one.
    #[test]
    fn an_undershoot_the_engine_only_drained_in_grace_is_not_generator_bound() {
        use crate::streaming::load::generator_bound;
        let fit = |rows_per_sec| {
            Some(InWindowRate {
                rows_per_sec,
                tolerance: GENERATOR_UNDERSHOOT_TOLERANCE,
            })
        };
        let slow = kept_target_rate(true, fit(40_000.0), 100_000.0, 80_000.0);
        assert!(!slow);
        assert!(!generator_bound(Some(100_000.0), 80_000.0, slow));
        let kept = kept_target_rate(true, fit(79_500.0), 100_000.0, 80_000.0);
        assert!(kept);
        assert!(generator_bound(Some(100_000.0), 80_000.0, kept));
    }

    #[test]
    fn kept_target_rate_needs_a_drain_and_a_fit() {
        let fit = |rows_per_sec| {
            Some(InWindowRate {
                rows_per_sec,
                tolerance: GENERATOR_UNDERSHOOT_TOLERANCE,
            })
        };
        assert!(!kept_target_rate(
            false,
            fit(400_000.0),
            400_000.0,
            400_000.0
        ));
        assert!(!kept_target_rate(true, None, 400_000.0, 400_000.0));
        assert!(kept_target_rate(true, fit(393_000.0), 400_000.0, 400_000.0));
        assert!(!kept_target_rate(
            true,
            fit(390_000.0),
            400_000.0,
            400_000.0
        ));
    }

    #[test]
    fn too_few_samples_fit_no_line() {
        assert_eq!(fitted_rate(&[]), None);
        assert_eq!(fitted_rate(&[(1.0, 10.0)]), None);
        assert_eq!(fitted_rate(&[(1.0, 10.0), (1.0, 20.0)]), None);
        assert_eq!(fitted_rate(&[(1.0, 10.0), (2.0, 30.0)]), Some(20.0));
        assert_eq!(in_window_rate(&[], 0.1), None);
    }

    #[test]
    fn a_fit_renders_its_rate_and_tolerance_or_nulls() {
        let fit = InWindowRate {
            rows_per_sec: 19_622.25,
            tolerance: 0.0214,
        };
        assert_eq!(
            json_in_window("in_window_applied_rows_per_sec", Some(fit)),
            "\"in_window_applied_rows_per_sec\":19622.2,\"rate_tolerance\":0.0214"
        );
        assert_eq!(
            json_in_window("in_window_applied_rows_per_sec", None),
            "\"in_window_applied_rows_per_sec\":null,\"rate_tolerance\":null"
        );
    }
}
