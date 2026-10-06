//! The planted-bug sweep's baseline bar (#786): an unplanted baseline passes
//! only with **no failure outside the checked-in quarantine list**,
//! `generative/baseline-quarantine.txt`. See `docs/generative-test-suite.md`,
//! "The baseline bar", for the rule and why it replaced a failure-rate bar.
//!
//! Each entry names one case, by tier, seed and case number (the
//! `GENERATIVE_PLANT_ONLY=<seed>:<case>` numbering), and the **open** issue
//! that pins its failure. The list can't go stale: a sweep fails before it
//! runs anything if a listed issue is closed ([`closed_entries`]), and it
//! reports a listed case that passed ([`BaselineVerdict::now_passing`]), so
//! the entry gets removed once its fix has landed.
//!
//! Only the "is the issue open" check leaves the process (`gh api`); the
//! rest is pure and unit-tested here.

use std::collections::HashSet;

/// The checked-in list.
pub const CHECKED_IN: &str = include_str!("../baseline-quarantine.txt");

/// The sweep tiers an entry can name (`concurrent_convergence.rs`'s
/// `GENERATIVE_PLANT_TIER` values).
pub const TIERS: &[&str] = &["cooling_key", "hot_key", "mid_burst", "steady_load"];

/// The repository whose issues the entries name.
pub const ISSUE_REPO: &str = "salesforce-misc/trellis";

/// One quarantined case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub tier: String,
    pub seed: u64,
    /// 1-based, as a sweep numbers its cases.
    pub case: usize,
    /// The open issue that pins this case's failure.
    pub issue: u64,
    /// What fails, in a few words.
    pub note: String,
}

impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}:{} (#{}: {})",
            self.tier, self.seed, self.case, self.issue, self.note
        )
    }
}

/// Parses a list: one entry per line, `<tier> <seed>:<case> #<issue> <what
/// fails>`, fields separated by whitespace. Blank lines and lines starting
/// with `#` are comments. Rejects an unknown tier, a missing note, and a case
/// listed twice.
pub fn parse(text: &str) -> Result<Vec<Entry>, String> {
    let mut entries: Vec<Entry> = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let at = |why: &str| format!("line {}: {why}: {line:?}", number + 1);
        let mut fields = line.split_whitespace();
        let tier = fields.next().expect("a non-empty line has a field");
        if !TIERS.contains(&tier) {
            return Err(at(&format!("unknown tier, expected one of {TIERS:?}")));
        }
        let (seed, case) = fields
            .next()
            .and_then(|f| f.split_once(':'))
            .and_then(|(s, c)| Some((s.parse::<u64>().ok()?, c.parse::<usize>().ok()?)))
            .filter(|&(seed, case)| seed > 0 && case > 0)
            .ok_or_else(|| at("expected <seed>:<case>, both from 1"))?;
        let issue = fields
            .next()
            .and_then(|f| f.strip_prefix('#'))
            .and_then(|n| n.parse::<u64>().ok())
            .ok_or_else(|| at("expected #<issue>"))?;
        let note = fields.collect::<Vec<_>>().join(" ");
        if note.is_empty() {
            return Err(at("say what fails after the issue"));
        }
        if entries
            .iter()
            .any(|e| e.tier == tier && e.seed == seed && e.case == case)
        {
            return Err(at("this case is already listed"));
        }
        entries.push(Entry {
            tier: tier.to_string(),
            seed,
            case,
            issue,
            note,
        });
    }
    Ok(entries)
}

/// The checked-in list, parsed. Panics if it doesn't parse; a unit test
/// keeps that from reaching a sweep.
pub fn checked_in() -> Vec<Entry> {
    parse(CHECKED_IN).unwrap_or_else(|e| panic!("generative/baseline-quarantine.txt: {e}"))
}

/// One baseline case's result, as the sweep judges it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaselineCase {
    pub seed: u64,
    pub case: usize,
    pub failed: bool,
}

/// How a tier's baseline fared against the list.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BaselineVerdict {
    /// Failed cases the list doesn't name: each fails the baseline until it
    /// is triaged into a filed bug (then listed) or a fixed harness defect.
    pub unlisted: Vec<(u64, usize)>,
    /// Failed cases the list names: left out of the bar and of every plant's
    /// count.
    pub quarantined: Vec<(u64, usize)>,
    /// Listed cases that ran and passed every time this sweep ran them. A
    /// race may pass now and then, so it is reported, not failed; once the
    /// fix lands, remove the entry.
    pub now_passing: Vec<Entry>,
}

impl BaselineVerdict {
    /// Whether the baseline meets the bar: no failure outside the list.
    pub fn passes(&self) -> bool {
        self.unlisted.is_empty()
    }
}

/// Judges `tier`'s baseline results against `entries`.
pub fn judge(entries: &[Entry], tier: &str, baseline: &[BaselineCase]) -> BaselineVerdict {
    let listed = |seed: u64, case: usize| {
        entries
            .iter()
            .any(|e| e.tier == tier && e.seed == seed && e.case == case)
    };
    let mut verdict = BaselineVerdict::default();
    let mut seen = HashSet::new();
    for r in baseline.iter().filter(|r| r.failed) {
        if !seen.insert((r.seed, r.case)) {
            continue;
        }
        if listed(r.seed, r.case) {
            verdict.quarantined.push((r.seed, r.case));
        } else {
            verdict.unlisted.push((r.seed, r.case));
        }
    }
    verdict.now_passing = entries
        .iter()
        .filter(|e| e.tier == tier)
        .filter(|e| {
            let runs: Vec<&BaselineCase> = baseline
                .iter()
                .filter(|r| r.seed == e.seed && r.case == e.case)
                .collect();
            !runs.is_empty() && runs.iter().all(|r| !r.failed)
        })
        .cloned()
        .collect();
    verdict
}

/// The entries whose issue `state` says is closed. Any error from `state`
/// (the issue can't be looked up) is returned, so a sweep that can't confirm
/// its list is current doesn't run on it.
pub fn closed_entries(
    entries: &[Entry],
    mut state: impl FnMut(u64) -> Result<IssueState, String>,
) -> Result<Vec<Entry>, String> {
    let mut closed = Vec::new();
    for entry in entries {
        match state(entry.issue)? {
            IssueState::Open => {}
            IssueState::Closed => closed.push(entry.clone()),
        }
    }
    Ok(closed)
}

/// An issue's state on GitHub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssueState {
    Open,
    Closed,
}

/// How many times [`github_issue_state`] tries a lookup that gets no answer,
/// and how long it waits between tries.
const LOOKUP_ATTEMPTS: u32 = 3;
const LOOKUP_RETRY_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// Looks `issue` up in [`ISSUE_REPO`] with the GitHub CLI (`gh api`), which
/// reads its token from `GH_TOKEN` or its own login.
///
/// A lookup that gets no answer (no network, a server error) is tried again
/// a couple of times, so one blip doesn't stop a sweep; one that still gets
/// none is an error, so a sweep that can't confirm its list doesn't run. A
/// refusal (an HTTP 4xx, such as not found or bad credentials) is an error at
/// once, and so is a number that names a pull request: an entry names the
/// issue that pins its failure.
pub fn github_issue_state(issue: u64) -> Result<IssueState, String> {
    let mut attempt = 1;
    loop {
        match github_issue_state_once(issue) {
            Ok(answer) => return answer,
            Err(e) if attempt < LOOKUP_ATTEMPTS => {
                eprintln!("baseline quarantine: {e}; trying again");
                std::thread::sleep(LOOKUP_RETRY_WAIT);
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// One lookup: `Err` when it got no answer and is worth trying again, else
/// the answer.
fn github_issue_state_once(issue: u64) -> Result<Result<IssueState, String>, String> {
    let output = std::process::Command::new("gh")
        .args([
            "api",
            &format!("repos/{ISSUE_REPO}/issues/{issue}"),
            "--jq",
            r#"if .pull_request then "pull request" else .state end"#,
        ])
        .output()
        .map_err(|e| format!("couldn't run `gh` to look up #{issue}: {e}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let failed = || {
        format!(
            "`gh api` couldn't look up #{issue} in {ISSUE_REPO} ({}): {} {}",
            output.status,
            stdout.trim(),
            stderr.trim()
        )
    };
    match (output.status.success(), stdout.trim()) {
        (true, "open") => Ok(Ok(IssueState::Open)),
        (true, "closed") => Ok(Ok(IssueState::Closed)),
        (true, "pull request") => Ok(Err(format!(
            "#{issue} in {ISSUE_REPO} is a pull request, not the issue that pins a failure"
        ))),
        (false, _) if stderr.contains("(HTTP 4") => Ok(Err(failed())),
        _ => Err(failed()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_checked_in_list_parses() {
        checked_in();
    }

    #[test]
    fn an_entry_names_a_tier_a_case_an_issue_and_what_fails() {
        let entries = parse(
            "# a comment\n\
             \n\
             cooling_key  3:1   #784  t3 rel_agg keeps a deleted parent\n",
        )
        .expect("parses");
        assert_eq!(
            entries,
            vec![Entry {
                tier: "cooling_key".into(),
                seed: 3,
                case: 1,
                issue: 784,
                note: "t3 rel_agg keeps a deleted parent".into(),
            }]
        );
        for bad in [
            "warm_key 3:1 #784 x",
            "cooling_key 3 #784 x",
            "cooling_key 0:1 #784 x",
            "cooling_key 3:1 784 x",
            "cooling_key 3:1 #784",
            "cooling_key 3:1 #784 x\ncooling_key 3:1 #785 y",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} parsed");
        }
        // The same seed and case in another tier is another case.
        assert_eq!(
            parse("hot_key 3:1 #1 x\nmid_burst 3:1 #2 y")
                .expect("parses")
                .len(),
            2
        );
    }

    fn run(seed: u64, case: usize, failed: bool) -> BaselineCase {
        BaselineCase { seed, case, failed }
    }

    #[test]
    fn only_a_failure_outside_the_list_fails_the_baseline() {
        let entries = parse("cooling_key 1:2 #10 x\nhot_key 1:3 #11 y").expect("parses");
        let verdict = judge(
            &entries,
            "cooling_key",
            &[run(1, 1, false), run(1, 2, true), run(1, 3, false)],
        );
        assert!(verdict.passes());
        assert_eq!(verdict.quarantined, vec![(1, 2)]);
        assert!(verdict.now_passing.is_empty());

        // Another tier's entry doesn't cover this tier's case.
        let verdict = judge(&entries, "cooling_key", &[run(1, 3, true), run(1, 3, true)]);
        assert!(!verdict.passes());
        assert_eq!(verdict.unlisted, vec![(1, 3)]);
    }

    #[test]
    fn a_listed_case_that_passed_every_run_is_reported() {
        let entries = parse("steady_load 2:5 #10 x\nsteady_load 2:6 #11 y\nsteady_load 2:7 #12 z")
            .expect("parses");
        // 2:5 passed both runs, 2:6 failed one of two, 2:7 didn't run.
        let verdict = judge(
            &entries,
            "steady_load",
            &[
                run(2, 5, false),
                run(2, 5, false),
                run(2, 6, false),
                run(2, 6, true),
            ],
        );
        assert!(verdict.passes());
        let passing: Vec<(u64, usize)> = verdict
            .now_passing
            .iter()
            .map(|e| (e.seed, e.case))
            .collect();
        assert_eq!(passing, vec![(2, 5)]);
    }

    #[test]
    fn a_closed_issue_is_found_and_a_failed_lookup_is_an_error() {
        let entries = parse("hot_key 1:1 #10 x\nhot_key 1:2 #11 y").expect("parses");
        let closed = closed_entries(&entries, |issue| {
            Ok(if issue == 11 {
                IssueState::Closed
            } else {
                IssueState::Open
            })
        })
        .expect("looked up");
        assert_eq!(closed, vec![entries[1].clone()]);
        assert!(closed_entries(&entries, |_| Err("offline".into())).is_err());
        assert_eq!(closed_entries(&[], |_| Err("offline".into())), Ok(vec![]));
    }
}
