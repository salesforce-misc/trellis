//! The planted-bug sweep: runs a tier's seeded
//! cases once with no plant, as the baseline, then once per planted bug
//! (`trellis::dev::plant::Plant`), and judges whether the tier catches each.
//! Shared by the concurrent tier (`tests/concurrent_convergence.rs`,
//! `planted_bugs_are_caught`) and the `JOIN` tier (`tests/join_convergence.rs`,
//! `join_planted_bugs_are_caught`); `docs/generative-test-suite.md`, "The
//! baseline bar", says how a sweep is judged.
//!
//! A plant is armed per process by `TRELLIS_TEST_PLANT`, so [`sweep`] is
//! called by a test that re-runs its own binary, on itself alone, once per
//! plant: the parent judges, each child runs every case and prints one
//! [`SWEEP_LINE`] per case.

use crate::baseline_quarantine;
use trellis::dev::defs::ast::KeySpace;
use trellis::dev::plant::{PLANT_ENV, Plant};

/// Names the plants a sweep runs: `all`, `none` (the baseline alone), or a
/// comma-separated list of `trellis::dev::plant::Plant` names. Unset, a
/// sweep returns at once.
pub const PLANTS_ENV: &str = "GENERATIVE_PLANTS";
/// How many seeds a sweep draws its cases from (default [`PLANT_SEEDS`]).
pub const PLANT_SEEDS_ENV: &str = "GENERATIVE_PLANT_SEEDS";
/// How many cases a sweep draws from each seed (default [`PLANT_CASES`]).
pub const PLANT_CASES_ENV: &str = "GENERATIVE_PLANT_CASES";
/// Which of the concurrent tier's case shapes a sweep draws:
/// `cooling_key` (the default), `hot_key`, `mid_burst`, `steady_load` or
/// `cooling_backfill`. The `JOIN` tier has one.
pub const PLANT_TIER_ENV: &str = "GENERATIVE_PLANT_TIER";
/// Runs one case only, as `<seed>:<case>` (1-based), to look at a failure
/// the sweep reported. Each failing case's report heads are printed to
/// stderr.
pub const PLANT_ONLY_ENV: &str = "GENERATIVE_PLANT_ONLY";
/// Set by the sweep on each process it spawns: run the cases and print one
/// [`SWEEP_LINE`] per case, rather than spawn more processes. Such a process
/// exits 0 whatever its cases do: only the parent judges them, against the
/// baseline bar, so a repro loop that runs it directly reads its rows.
pub const PLANT_CHILD_ENV: &str = "GENERATIVE_PLANT_CHILD";
/// Prefixes each case's result on a sweep process's stdout.
pub const SWEEP_LINE: &str = "plant-sweep\t";
/// The default seed count.
pub const PLANT_SEEDS: u64 = 4;
/// The default case count per seed.
pub const PLANT_CASES: usize = 12;

/// `name`'s value, parsed, or `default` when it is unset.
///
/// # Panics
///
/// When it is set and doesn't parse.
pub fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(v) => v
            .parse()
            .unwrap_or_else(|_| panic!("{name}={v:?} doesn't parse")),
        Err(_) => default,
    }
}

/// How a sweep judges a tier.
pub struct SweepTier {
    /// The tier's name, as the quarantine list names it.
    pub name: &'static str,
    /// Plants the tier is known not to catch yet. The sweep runs and reports
    /// them, but doesn't fail when they go uncaught; it says so when one is
    /// caught, so the entry can go.
    pub known_misses: &'static [&'static str],
    /// Plants the tier doesn't gate, each with why: another tier (or a
    /// deterministic pin) runs the shape it needs on purpose, and this one
    /// reaches it rarely or never. `GENERATIVE_PLANTS=all` leaves them out;
    /// named, they run and are reported, and the sweep never fails on them.
    pub not_gated: Vec<(&'static str, &'static str)>,
}

/// Which apply path a plant breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlantPath {
    /// A 1-1 target.
    OneToOne,
    /// A group-basis aggregate's page or build.
    GroupBasis,
    /// A group-basis aggregate, or a `JOIN`'s `P` and `M` cells: the
    /// group-basis writer's statements (`staging::group_basis`) that both
    /// share.
    GroupBasisWriter,
    /// A `JOIN`.
    Join,
    /// A 1-1 `JOIN` (ADR-0021): no tier draws one yet, so these plants are
    /// pinned by `trellis`'s `join_one_to_one_interleavings` until the
    /// generative tier of milestone 9 (#1404) gates them.
    OneToOneJoin,
}

/// The path `plant` breaks.
pub fn plant_path(plant: Plant) -> PlantPath {
    match plant {
        // The tombstone GC collects a 1-1 target's ledger, and a
        // plain 1-1 target takes the Re-derive build's chunks.
        Plant::StaleOneToOneWrite | Plant::EarlyTombstoneGc | Plant::ChunkWithoutEntryLock => {
            PlantPath::OneToOne
        }
        Plant::GbSkipGroupLock | Plant::GbBuildSnapshotBeforeLocks => PlantPath::GroupBasis,
        Plant::GbLsnNotVisibility | Plant::GbFoldSidesPerKey | Plant::GbEarlyZeroGc => {
            PlantPath::GroupBasisWriter
        }
        Plant::JoinNoJLock
        | Plant::JoinOtherSideBeforeJ
        | Plant::JoinDropFlip
        | Plant::JoinTSkipLock => PlantPath::Join,
        Plant::Join1NoJLock
        | Plant::Join1NoRecheck
        | Plant::Join1LsnOnly
        | Plant::Join1VisOnly
        | Plant::Join1NoEntryLock
        | Plant::Join1OldKeyUnlocked
        | Plant::Join1SnapshotAfterRead
        | Plant::Join1RdNoKey
        | Plant::Join1BrdNoKey
        | Plant::Join1NoPlaceholder
        | Plant::Join1MBeforeJ
        | Plant::Join1ChunkNoKey
        | Plant::Join1NoOrphanPass
        | Plant::Join1OrphanDelete
        | Plant::Join1MPassBOnly
        | Plant::Join1TruncateNoFloor
        | Plant::Join1TruncateShareLock
        | Plant::Join1TruncateKeepsR
        | Plant::Join1TruncateKeepsM
        | Plant::Join1TruncateNoRebuild
        | Plant::Join1TruncateDefAfterLock => PlantPath::OneToOneJoin,
    }
}

/// Whether `plant` can make a target with `key_space` diverge: each plant
/// breaks one apply path, so a divergence in a target it never writes is an
/// unplanted failure, not a catch. The baseline reaches everything.
pub fn plant_reaches(plant: &str, key_space: &KeySpace) -> bool {
    let Some(plant) = Plant::from_name(plant) else {
        return true;
    };
    let aggregate = matches!(key_space, KeySpace::Aggregate { .. });
    let join = matches!(key_space, KeySpace::Join(_));
    match plant_path(plant) {
        PlantPath::OneToOne => !aggregate && !join,
        PlantPath::GroupBasis => aggregate,
        PlantPath::GroupBasisWriter => aggregate || join,
        PlantPath::Join => join,
        PlantPath::OneToOneJoin => matches!(key_space, KeySpace::JoinOneToOne(_)),
    }
}

/// Whether `plant` can have caused a failure that diverged in targets with
/// these key spaces (`None` for a target the program doesn't define, which is
/// given the benefit of the doubt): any one of them it writes is enough.
/// A failure with no diverging target at all (an error or a quiesce
/// timeout) is reachable too.
pub fn divergence_reachable<'a>(
    plant: &str,
    key_spaces: impl IntoIterator<Item = Option<&'a KeySpace>>,
) -> bool {
    let mut key_spaces = key_spaces.into_iter().peekable();
    key_spaces.peek().is_none()
        || key_spaces.any(|key_space| key_space.is_none_or(|ks| plant_reaches(plant, ks)))
}

/// Seed `seed`'s first `cases` cases of `strategy`. The same seed draws the
/// same cases in every process, which is what lets a sweep judge each plant
/// against an unplanted baseline over exactly the cases the plant ran.
#[cfg(feature = "proptest")]
pub fn seeded_cases<T: std::fmt::Debug>(
    strategy: &proptest::strategy::BoxedStrategy<T>,
    seed: u64,
    cases: usize,
) -> Vec<T> {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
    let mut bytes = [0u8; 32];
    for chunk in bytes.chunks_mut(8) {
        chunk.copy_from_slice(&seed.to_le_bytes());
    }
    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &bytes),
    );
    (0..cases)
        .map(|_| {
            strategy
                .new_tree(&mut runner)
                .expect("the tier's strategy draws a case")
                .current()
        })
        .collect()
}

/// One case's result in a sweep, as a sweep process prints it.
#[derive(Debug)]
pub struct SweepCase {
    pub plant: String,
    pub seed: u64,
    pub case: usize,
    pub failed: bool,
    /// How many times the plant changed the engine's behavior in this case.
    pub fired: u64,
    /// Whether the plant can have caused the failure: any of the targets the
    /// case diverged in is one the plant writes ([`plant_reaches`]), or the
    /// case failed without diverging (an error or a quiesce timeout). `true`
    /// for a pass.
    pub reachable: bool,
    pub secs: f64,
    pub reason: String,
}

impl SweepCase {
    /// Whether this case caught its plant: it failed, the plant changed the
    /// engine's behavior in it, and the failure is one the plant can cause
    /// (`reachable`). A planted case that failed any other way hit an
    /// unplanted divergence, and says nothing about the plant. For the
    /// baseline, whether it failed.
    ///
    /// The baseline passing the same case doesn't rule out an unplanted
    /// failure in the planted run, since the schedule isn't replayed, only
    /// the case. The target check narrows that, and a plant can still be
    /// credited with an unplanted failure in a target it writes, at the
    /// baseline's rate.
    pub fn caught(&self) -> bool {
        self.failed && (self.plant == "baseline" || (self.fired > 0 && self.reachable))
    }

    pub fn to_line(&self) -> String {
        format!(
            "{SWEEP_LINE}{}\t{}\t{}\t{}\t{}\t{}\t{:.1}\t{}",
            self.plant,
            self.seed,
            self.case,
            if self.failed { "FAIL" } else { "pass" },
            self.fired,
            self.reachable,
            self.secs,
            self.reason
        )
    }

    pub fn from_line(line: &str) -> Option<SweepCase> {
        let mut fields = line.strip_prefix(SWEEP_LINE)?.splitn(8, '\t');
        let mut next = || fields.next().expect("a sweep line has 8 fields");
        Some(SweepCase {
            plant: next().to_string(),
            seed: next().parse().ok()?,
            case: next().parse().ok()?,
            failed: next() == "FAIL",
            fired: next().parse().ok()?,
            reachable: next().parse().ok()?,
            secs: next().parse().ok()?,
            reason: next().to_string(),
        })
    }
}

/// How one case failed: the key space of every target that diverged (`None`
/// for a target the program doesn't define; empty when the case failed
/// without diverging), and the report.
pub struct CaseFailure {
    pub key_spaces: Vec<Option<KeySpace>>,
    pub message: String,
}

/// The name a sweep reports this process's plant under.
pub fn armed_name() -> &'static str {
    trellis::dev::plant::armed().map_or("baseline", |p| p.name())
}

/// Runs a tier's planted-bug sweep (see the module doc), from the test named
/// `test`, which must call this and nothing else first. `draw` gives a
/// seed's first cases; `run` runs one. In a sweep's child process it runs
/// every case and prints its rows; otherwise, with [`PLANTS_ENV`] set, it
/// spawns the children and judges them against the baseline bar and the
/// tier's gate, and panics on a miss.
pub fn sweep<C>(
    test: &str,
    tier: &SweepTier,
    draw: impl Fn(u64, usize) -> Vec<C>,
    run: impl FnMut(&C) -> Result<(), CaseFailure>,
) {
    if std::env::var_os(PLANT_CHILD_ENV).is_some() {
        run_cases(tier.name, draw, run);
        return;
    }
    let Ok(requested) = std::env::var(PLANTS_ENV) else {
        eprintln!("{test}: skipped, {PLANTS_ENV} is not set");
        return;
    };
    judge(test, tier, &requested);
}

/// A sweep process: runs every seed's cases against this process's plant
/// (or none, for the baseline), printing one [`SWEEP_LINE`] per case.
fn run_cases<C>(
    tier: &str,
    draw: impl Fn(u64, usize) -> Vec<C>,
    mut run: impl FnMut(&C) -> Result<(), CaseFailure>,
) {
    let seeds: u64 = env_or(PLANT_SEEDS_ENV, PLANT_SEEDS);
    let cases: usize = env_or(PLANT_CASES_ENV, PLANT_CASES);
    let plant = armed_name();
    let only: Option<(u64, usize)> = std::env::var(PLANT_ONLY_ENV).ok().map(|v| {
        let (seed, case) = v
            .split_once(':')
            .unwrap_or_else(|| panic!("{PLANT_ONLY_ENV}={v:?}: expected <seed>:<case>"));
        (
            seed.parse().expect("a seed number"),
            case.parse().expect("a case number"),
        )
    });
    for seed in 1..=seeds {
        for (index, case) in draw(seed, cases).iter().enumerate() {
            if only.is_some_and(|only| only != (seed, index + 1)) {
                continue;
            }
            let fired_before = trellis::dev::plant::fired();
            let started = std::time::Instant::now();
            let result = run(case);
            let reachable = match &result {
                Err(failure) => {
                    divergence_reachable(plant, failure.key_spaces.iter().map(Option::as_ref))
                }
                Ok(()) => true,
            };
            let reason = match &result {
                Ok(()) => String::new(),
                Err(failure) => {
                    // The report's head names the wrong rows; the rest is
                    // the whole program.
                    let text = &failure.message;
                    let head: Vec<&str> = text.lines().take(24).collect();
                    eprintln!(
                        "({tier}) {plant} seed {seed} case {} failed:\n{}",
                        index + 1,
                        head.join("\n")
                    );
                    text.replace(['\t', '\n'], " ").chars().take(240).collect()
                }
            };
            let line = SweepCase {
                plant: plant.to_string(),
                seed,
                case: index + 1,
                failed: result.is_err(),
                fired: trellis::dev::plant::fired() - fired_before,
                reachable,
                secs: started.elapsed().as_secs_f64(),
                reason,
            }
            .to_line();
            println!("{line}");
            eprintln!("({tier}) {line}");
        }
    }
}

/// The sweep's parent: the baseline and each requested plant in a process of
/// its own, then the verdict.
fn judge(test: &str, tier: &SweepTier, requested: &str) {
    let quarantine = baseline_quarantine::checked_in();
    let closed =
        baseline_quarantine::closed_entries(&quarantine, baseline_quarantine::github_issue_state)
            .unwrap_or_else(|e| {
                panic!("can't confirm generative/baseline-quarantine.txt is current: {e}")
            });
    assert!(
        closed.is_empty(),
        "generative/baseline-quarantine.txt lists cases whose issue is closed; remove each \
         entry, or if the case still fails, reopen or file its issue:\n{}",
        closed
            .iter()
            .map(|e| format!("  {e}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    let plants: Vec<Plant> = if requested.trim() == "none" {
        Vec::new()
    } else if requested.trim() == "all" {
        for (name, why) in &tier.not_gated {
            eprintln!(
                "{test}: {name} not run, the {} tier doesn't gate it: {why}",
                tier.name
            );
        }
        Plant::ALL
            .iter()
            .copied()
            .filter(|p| !tier.not_gated.iter().any(|(name, _)| *name == p.name()))
            .collect()
    } else {
        requested
            .split(',')
            .map(|name| {
                Plant::from_name(name.trim())
                    .unwrap_or_else(|| panic!("{PLANTS_ENV}: {name:?} names no plant"))
            })
            .collect()
    };

    let exe = std::env::current_exe().expect("this test binary's path");
    let mut results: Vec<SweepCase> = Vec::new();
    for plant in std::iter::once(None).chain(plants.iter().copied().map(Some)) {
        let mut command = std::process::Command::new(&exe);
        command
            .args([
                test,
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(PLANT_CHILD_ENV, "1")
            .stderr(std::process::Stdio::inherit());
        match plant {
            Some(plant) => command.env(PLANT_ENV, plant.name()),
            None => command.env_remove(PLANT_ENV),
        };
        let output = command.output().expect("spawn a sweep process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let lines: Vec<SweepCase> = stdout.lines().filter_map(SweepCase::from_line).collect();
        assert!(
            output.status.success() && !lines.is_empty(),
            "the sweep process for {} failed ({}):\n{stdout}",
            plant.map_or("baseline", |p| p.name()),
            output.status
        );
        results.extend(lines);
    }

    // The baseline meets the bar only with no failure outside the list. A
    // case the baseline failed, or that the list names, says nothing about
    // any plant, so it is left out of every plant's count (the report says
    // which).
    let baseline: Vec<baseline_quarantine::BaselineCase> = results
        .iter()
        .filter(|r| r.plant == "baseline")
        .map(|r| baseline_quarantine::BaselineCase {
            seed: r.seed,
            case: r.case,
            failed: r.failed,
        })
        .collect();
    let verdict = baseline_quarantine::judge(&quarantine, tier.name, &baseline);
    let mut excluded: Vec<(u64, usize)> = verdict
        .unlisted
        .iter()
        .chain(&verdict.quarantined)
        .copied()
        .chain(
            quarantine
                .iter()
                .filter(|e| e.tier == tier.name)
                .map(|e| (e.seed, e.case)),
        )
        .collect();
    excluded.sort_unstable();
    excluded.dedup();
    let report = report(tier, &results, &excluded);
    eprintln!("{report}");
    for entry in &verdict.now_passing {
        eprintln!(
            "{test}: quarantined case {entry} passed every baseline run; once #{} is fixed, \
             remove its entry",
            entry.issue
        );
    }
    assert!(
        verdict.passes(),
        "the unplanted baseline failed {} case(s) the quarantine list doesn't name: {}. No \
         unknown failure is accepted: triage each into a filed bug (then list it in \
         generative/baseline-quarantine.txt) or a fixed harness defect (see the report \
         above; rerun one with {PLANT_ONLY_ENV}=<seed>:<case>)",
        verdict.unlisted.len(),
        verdict
            .unlisted
            .iter()
            .map(|(seed, case)| format!("{seed}:{case}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let caught = |name: &str| {
        results
            .iter()
            .any(|r| r.plant == name && r.caught() && !excluded.contains(&(r.seed, r.case)))
    };
    for name in tier.known_misses.iter().filter(|name| caught(name)) {
        eprintln!("{test}: known miss {name} was caught; consider gating it");
    }
    let missed: Vec<&str> = plants
        .iter()
        .map(|p| p.name())
        .filter(|name| !tier.known_misses.contains(name) && !caught(name))
        .filter(|name| !tier.not_gated.iter().any(|(not, _)| not == name))
        .collect();
    assert!(
        missed.is_empty(),
        "the {} tier never caught {missed:?} (see the report above)",
        tier.name
    );
}

/// The sweep's summary: per plant, how many cases it was caught in, how many
/// cases it fired in, and each seed's cases-to-first-catch.
/// `excluded` are the cases the baseline failed or the quarantine list
/// names, left out of every plant's row.
fn report(tier: &SweepTier, results: &[SweepCase], excluded: &[(u64, usize)]) -> String {
    use std::collections::HashMap;
    use std::fmt::Write;
    let mut plants: Vec<&str> = Vec::new();
    for r in results {
        if !plants.contains(&r.plant.as_str()) {
            plants.push(&r.plant);
        }
    }
    let mut out = format!(
        "planted-bug sweep ({} tier; plant rows leave out the {} case(s) the baseline \
         failed or the quarantine list names):\n\
         plant | caught | fired in | failed, not attributable | cases to first catch, per seed | \
         mean case secs\n",
        tier.name,
        excluded.len()
    );
    for plant in plants {
        let rows: Vec<&SweepCase> = results
            .iter()
            .filter(|r| {
                r.plant == plant && (plant == "baseline" || !excluded.contains(&(r.seed, r.case)))
            })
            .collect();
        let caught = rows.iter().filter(|r| r.caught()).count();
        let fired = rows.iter().filter(|r| r.fired > 0).count();
        // Failures a plant can't have caused: see `SweepCase::caught`.
        let unattributable = rows.iter().filter(|r| r.failed && !r.caught()).count();
        let mut seeds: Vec<u64> = rows.iter().map(|r| r.seed).collect();
        seeds.dedup();
        let firsts: Vec<String> = seeds
            .iter()
            .map(|&seed| {
                let of_seed: Vec<&&SweepCase> = rows.iter().filter(|r| r.seed == seed).collect();
                of_seed
                    .iter()
                    .find(|r| r.caught())
                    .map_or(format!(">{}", of_seed.len()), |r| r.case.to_string())
            })
            .collect();
        let secs = rows.iter().map(|r| r.secs).sum::<f64>() / rows.len().max(1) as f64;
        let known = if tier.known_misses.contains(&plant) {
            " (known miss)"
        } else if tier.not_gated.iter().any(|(name, _)| *name == plant) {
            " (not gated here)"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "{plant}{known} | {caught}/{} | {fired}/{} | {unattributable} | {} | {secs:.1}",
            rows.len(),
            rows.len(),
            firsts.join(", ")
        );
    }
    // The first few failures per plant, to say what the tier saw.
    let mut listed: HashMap<&str, usize> = HashMap::new();
    for r in results.iter().filter(|r| r.failed) {
        let count = listed.entry(r.plant.as_str()).or_default();
        *count += 1;
        if *count > 3 {
            continue;
        }
        let _ = writeln!(
            out,
            "  {} seed {} case {} ({}): {}",
            r.plant,
            r.seed,
            r.case,
            if r.caught() {
                "caught"
            } else if r.fired == 0 {
                "plant never fired"
            } else {
                "in a target the plant doesn't write"
            },
            r.reason
        );
    }
    out
}

/// The plants whose path a tier's programs never take, each with `why`:
/// for a tier's `not_gated`.
pub fn not_on_path(
    paths: &[PlantPath],
    why: &'static str,
) -> impl Iterator<Item = (&'static str, &'static str)> {
    let paths = paths.to_vec();
    Plant::ALL
        .iter()
        .filter(move |p| paths.contains(&plant_path(**p)))
        .map(move |p| (p.name(), why))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plants_divergence_is_credited_behind_an_unplanted_one() {
        let group_by = KeySpace::Aggregate {
            group_by: Vec::new(),
        };
        let one_to_one = KeySpace::OneToOne;
        let plant = "stale_one_to_one_write";
        // An unplanted `GROUP BY` divergence first in definition order must
        // not hide the plant's own 1-1 divergence behind it.
        assert!(divergence_reachable(
            plant,
            [Some(&group_by), Some(&one_to_one)]
        ));
        assert!(!divergence_reachable(plant, [Some(&group_by)]));
        assert!(divergence_reachable(plant, std::iter::empty()));
        assert!(divergence_reachable(plant, [None]));
    }

    #[test]
    fn group_basis_plants_reach_aggregates_and_join_plants_reach_joins() {
        let group_by = KeySpace::Aggregate {
            group_by: Vec::new(),
        };
        let one_to_one = KeySpace::OneToOne;
        for plant in Plant::ALL {
            let name = plant.name();
            match plant_path(*plant) {
                PlantPath::GroupBasis | PlantPath::GroupBasisWriter => {
                    assert!(plant_reaches(name, &group_by), "{name}");
                    assert!(!plant_reaches(name, &one_to_one), "{name}");
                }
                PlantPath::Join => {
                    assert!(!plant_reaches(name, &group_by), "{name}");
                    assert!(!plant_reaches(name, &one_to_one), "{name}");
                }
                PlantPath::OneToOne => {
                    assert!(plant_reaches(name, &one_to_one), "{name}");
                    assert!(!plant_reaches(name, &group_by), "{name}");
                }
                PlantPath::OneToOneJoin => {
                    assert!(!plant_reaches(name, &one_to_one), "{name}");
                    assert!(!plant_reaches(name, &group_by), "{name}");
                }
            }
        }
    }
}
