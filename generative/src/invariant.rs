//! The 1-1 `JOIN` tier's in-run hidden-state invariant (#1709): while a
//! case's lanes write and the engine drains, a definition's `R`, `M`, `J`,
//! target and sources as applied are read under one snapshot
//! (`trellis::dev::staging::one_to_one_join_snapshot`) and [`check`]ed
//! against each other.
//!
//! An end-state oracle sees only what a run leaves, and later writes
//! overwrite a lost update before the run ends (#1459's model: a check every
//! 10 ms of the hidden state on one committed snapshot caught lost-update
//! plants that the final oracle caught in none of its runs). ADR-0021's
//! rules keep the hidden state consistent at every commit, so the check
//! holds at every snapshot, not only once the pipeline settles.
//!
//! # The clauses
//!
//! A definition that isn't `live`, or has a build chunk not done, is skipped:
//! a build fills `M` before `R` and the target, and a `TRUNCATE b` starts a
//! rebuild, so the target trails its copies until the build ends. Otherwise:
//!
//! - [`Clause::FromEntry`]: `R`'s live entries are `a` as `R` has applied
//!   it (the live rows minus the changes the ring holds unapplied, as
//!   `self_check` subtracts them), in both directions, on every column `R`
//!   copies. Keys the ring can't vouch for are left out, and so is every key
//!   while an unapplied `TRUNCATE a` is in the ring.
//! - [`Clause::JoinedEntry`]: the same for `M` and `b`.
//! - [`Clause::Target`]: each live `R` entry's target row is its fields over
//!   the entry and the newest live `M` entry on its join key (the latest
//!   applied position, then the highest key), evaluated by Postgres in the
//!   snapshot. `INNER` has a row only when such an entry exists, `LEFT`
//!   always. A target row with no live `R` entry is extra.
//! - [`Clause::JoinPick`]: the `M` entry the snapshot read a row with is the
//!   newest live one on the `R` entry's join key, by this module's own
//!   reading of `M` (a disagreement is a bug in the read or the check, not in
//!   the engine, and is reported as such).
//! - [`Clause::KeyRow`]: every join key a live `R` or `M` entry holds with no
//!   `NULL` part has its `J` row, which each writer inserts before it locks it
//!   and nothing deletes while the definition is live.
//!
//! The check compares join keys, entry keys and values by their `::text`,
//! where the snapshot's join of `R` to `M` uses `=`. The two agree on a join
//! key: define admits only the text-stable key types (`join_key_types`), the
//! same type and collation on both sides of `ON`, and no nondeterministic
//! collation, so two keys are `=` exactly when their texts are equal. A value
//! is compared with a copy of itself, or with the same expression over the
//! same values, so its text is the same.
//!
//! # What a pending fan-out exempts
//!
//! A `b` change's page recomputes the target rows of a key that fits its
//! chunk budget, and leaves a marker `(k, cursor)` for any other key
//! (ADR-0021 "A `b` change's fan-out runs in chunks under a pending
//! marker"). A chunk recomputes the next live `R` entries of `k` after the
//! cursor in entry-key order (under `"C"`) and advances it, under `J[k]`; a
//! later `b` change on `k` sets the cursor back to the start. So under any
//! snapshot the rows of `k` at or before the cursor were last written under
//! `J[k]` from the `M` that snapshot holds, and the rows after it (every
//! row, while the cursor is at the start) may still join an older `M`
//! entry. The exemption is exactly that: [`Clause::Target`] leaves out the
//! live `R` entries on a marker's key whose entry key is after its cursor,
//! or every entry on it when the cursor is `NULL`, both the row's values and
//! whether it exists (an `INNER` row whose key lost its `b` row stays until
//! its chunk). A row an `a` change writes behind or past the cursor is exact,
//! but the check can't tell it from the others past the cursor, so it
//! exempts it too. Nothing else is exempt: an extra row (no live `R` entry)
//! is never a fan-out's, since a delete or key move writes its own row.
//!
//! Rows of `a` held in quarantine (`poison_held`) are left out of
//! [`Clause::FromEntry`] and [`Clause::Target`]: a held row keeps the change
//! that failed in its `R` entry and its target row as it was until its
//! release. The tier draws no held row.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

use trellis::dev::pool::Pool;
use trellis::dev::staging::one_to_one_join_snapshot::{
    HiddenEntry, HiddenSnapshot, SourceView, hidden_snapshot,
};

/// Turns the in-run invariant off (`off`) or sets its sampling interval in
/// milliseconds (default [`DEFAULT_INTERVAL_MS`]), for the 1-1 `JOIN` tier.
pub const INVARIANT_ENV: &str = "GENERATIVE_JOIN1_INVARIANT";
/// The default pause between two rounds of samples.
pub const DEFAULT_INTERVAL_MS: u64 = 10;

/// The in-run invariant's sampling: after each round, which reads and checks
/// every 1-1 `JOIN` definition installed so far once, the sampler sleeps
/// `interval`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InRunInvariant {
    pub interval: Duration,
}

impl InRunInvariant {
    /// [`INVARIANT_ENV`]'s setting: `None` when it is `off`.
    ///
    /// # Panics
    ///
    /// When it is set to anything else that isn't a whole number.
    pub fn from_env() -> Option<Self> {
        match std::env::var(INVARIANT_ENV) {
            Ok(v) if v == "off" => None,
            Ok(v) => Some(InRunInvariant {
                interval: Duration::from_millis(
                    v.parse()
                        .unwrap_or_else(|_| panic!("{INVARIANT_ENV}={v:?} doesn't parse")),
                ),
            }),
            Err(_) => Some(InRunInvariant {
                interval: Duration::from_millis(DEFAULT_INTERVAL_MS),
            }),
        }
    }
}

/// What a run's samples did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SampleStats {
    /// Snapshots read and checked.
    pub checked: usize,
    /// Snapshots skipped: the read gave up on a lock, or [`check`] skipped
    /// the definition (not `live`, or building).
    pub skipped: usize,
    /// Live `R` entries a pending fan-out exempted, summed over the checks.
    pub exempt: usize,
}

impl SampleStats {
    pub fn add(&mut self, other: &SampleStats) {
        self.checked += other.checked;
        self.skipped += other.skipped;
        self.exempt += other.exempt;
    }
}

/// A sample that broke the invariant: its target, and each violation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub target: String,
    pub violations: Vec<Violation>,
}

/// Reads and checks `target` once, counting the result in `stats`.
///
/// # Errors
///
/// When the read fails other than by skipping.
pub async fn sample(
    pool: &Pool,
    target: &str,
    stats: &mut SampleStats,
) -> Result<Option<Failure>, String> {
    let snapshot = hidden_snapshot(pool, target)
        .await
        .map_err(|e| format!("read {target}'s hidden snapshot: {e}"))?;
    let Some(snapshot) = snapshot else {
        stats.skipped += 1;
        return Ok(None);
    };
    match check(&snapshot) {
        Verdict::Skipped(_) => {
            stats.skipped += 1;
            Ok(None)
        }
        Verdict::Checked { violations, exempt } => {
            stats.checked += 1;
            stats.exempt += exempt;
            Ok((!violations.is_empty()).then(|| Failure {
                target: target.to_string(),
                violations,
            }))
        }
    }
}

/// What a [`Violation`] breaks (the module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Clause {
    FromEntry,
    JoinedEntry,
    Target,
    JoinPick,
    KeyRow,
}

impl Clause {
    pub fn name(self) -> &'static str {
        match self {
            Clause::FromEntry => "R entry matches a as applied",
            Clause::JoinedEntry => "M entry matches b as applied",
            Clause::Target => "target row is its R entry joined to the newest live M entry",
            Clause::JoinPick => "the snapshot's M pick is the newest live entry",
            Clause::KeyRow => "every join key of a live R or M entry has its J row",
        }
    }
}

/// One way a snapshot breaks the invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub clause: Clause,
    /// The entry's or row's key (a join key's columns, for [`Clause::KeyRow`]).
    pub key: String,
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.clause.name(), self.key, self.detail)
    }
}

/// What [`check`] made of a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Not checked, and why.
    Skipped(String),
    Checked {
        violations: Vec<Violation>,
        /// Live `R` entries a pending fan-out exempted from
        /// [`Clause::Target`].
        exempt: usize,
    },
}

impl Verdict {
    pub fn violations(&self) -> &[Violation] {
        match self {
            Verdict::Skipped(_) => &[],
            Verdict::Checked { violations, .. } => violations,
        }
    }
}

fn show(values: &[Option<String>]) -> String {
    let parts: Vec<&str> = values
        .iter()
        .map(|v| v.as_deref().unwrap_or("NULL"))
        .collect();
    format!("({})", parts.join(", "))
}

/// Checks `snapshot` (the module doc). Pure: it reads nothing but the
/// snapshot.
pub fn check(snapshot: &HiddenSnapshot) -> Verdict {
    if snapshot.status != "live" {
        return Verdict::Skipped(format!("status {}", snapshot.status));
    }
    if snapshot.building {
        return Verdict::Skipped("a build is running".to_string());
    }
    let mut violations = Vec::new();
    ledger_clause(
        Clause::FromEntry,
        &snapshot.from_entries,
        &snapshot.from_applied,
        &snapshot.held,
        &mut violations,
    );
    ledger_clause(
        Clause::JoinedEntry,
        &snapshot.joined_entries,
        &snapshot.joined_applied,
        &BTreeSet::new(),
        &mut violations,
    );
    let exempt = target_clause(snapshot, &mut violations);
    key_clause(snapshot, &mut violations);
    Verdict::Checked { violations, exempt }
}

fn live(entries: &[HiddenEntry]) -> impl Iterator<Item = &HiddenEntry> {
    entries.iter().filter(|e| !e.tombstone)
}

/// [`Clause::FromEntry`] or [`Clause::JoinedEntry`].
fn ledger_clause(
    clause: Clause,
    entries: &[HiddenEntry],
    applied: &SourceView,
    held: &BTreeSet<String>,
    out: &mut Vec<Violation>,
) {
    if applied.global {
        return;
    }
    let skip = |key: &str| applied.uncertain.contains(key) || held.contains(key);
    let entries: BTreeMap<&str, &HiddenEntry> =
        live(entries).map(|e| (e.key.as_str(), e)).collect();
    let rows: BTreeMap<&str, &Vec<Option<String>>> =
        applied.rows.iter().map(|(k, v)| (k.as_str(), v)).collect();
    for (key, values) in &rows {
        if skip(key) {
            continue;
        }
        match entries.get(key) {
            None => out.push(Violation {
                clause,
                key: key.to_string(),
                detail: format!("the row {} has no live entry", show(values)),
            }),
            Some(entry) if entry.values != **values => out.push(Violation {
                clause,
                key: key.to_string(),
                detail: format!(
                    "the entry holds {} where the row as applied is {} (columns {:?})",
                    show(&entry.values),
                    show(values),
                    applied.columns
                ),
            }),
            Some(_) => {}
        }
    }
    for key in entries.keys() {
        if !skip(key) && !rows.contains_key(key) {
            out.push(Violation {
                clause,
                key: key.to_string(),
                detail: "a live entry whose row, as applied, is gone".to_string(),
            });
        }
    }
}

/// The join key's columns when none is `NULL` (a key with a `NULL` part
/// joins nothing and has no `J` row).
fn full_key(key: &[Option<String>]) -> Option<Vec<&str>> {
    key.iter().map(|part| part.as_deref()).collect()
}

/// The newest live `M` entry on `key` (the apply's rule): the latest applied
/// position, an entry with none last, then the highest entry key.
fn newest<'a>(joined: &'a [HiddenEntry], key: &[&str]) -> Option<&'a HiddenEntry> {
    live(joined)
        .filter(|m| full_key(&m.join_key).is_some_and(|k| k == key))
        .max_by(|x, y| {
            // `None` orders below every `Some`.
            (x.applied_lsn, &x.key).cmp(&(y.applied_lsn, &y.key))
        })
}

/// [`Clause::Target`] and [`Clause::JoinPick`]; returns how many live `R`
/// entries a marker exempted.
fn target_clause(s: &HiddenSnapshot, out: &mut Vec<Violation>) -> usize {
    let rows: BTreeMap<&str, &Vec<Option<String>>> =
        s.rows.iter().map(|r| (r.key.as_str(), &r.values)).collect();
    let joined_rows: BTreeMap<&str, _> =
        s.joined_rows.iter().map(|j| (j.key.as_str(), j)).collect();
    let markers: BTreeMap<Vec<&str>, Option<&str>> = s
        .markers
        .iter()
        .map(|m| {
            (
                m.join_key.iter().map(String::as_str).collect(),
                m.cursor.as_deref(),
            )
        })
        .collect();
    let mut exempt = 0;
    let mut backed = BTreeSet::new();
    for entry in live(&s.from_entries) {
        backed.insert(entry.key.as_str());
        if s.held.contains(&entry.key) {
            continue;
        }
        let key = full_key(&entry.join_key);
        if let Some(cursor) = key.as_ref().and_then(|k| markers.get(k))
            && cursor.is_none_or(|c| entry.key.as_str() > c)
        {
            exempt += 1;
            continue;
        }
        let Some(joined) = joined_rows.get(entry.key.as_str()) else {
            out.push(Violation {
                clause: Clause::JoinPick,
                key: entry.key.clone(),
                detail: "the snapshot read no joined row for a live R entry".to_string(),
            });
            continue;
        };
        let pick = key.as_ref().and_then(|k| newest(&s.joined_entries, k));
        if pick.map(|m| m.key.as_str()) != joined.joined.as_deref() {
            out.push(Violation {
                clause: Clause::JoinPick,
                key: entry.key.clone(),
                detail: format!(
                    "the snapshot read M entry {:?}, the newest live one is {:?}",
                    joined.joined,
                    pick.map(|m| &m.key)
                ),
            });
            continue;
        }
        let expected = (s.left || joined.joined.is_some()).then_some(&joined.values);
        match (expected, rows.get(entry.key.as_str())) {
            (Some(want), Some(got)) if want != *got => out.push(Violation {
                clause: Clause::Target,
                key: entry.key.clone(),
                detail: format!(
                    "the target holds {} where R and M give {} (fields {:?}, M entry {:?})",
                    show(got),
                    show(want),
                    s.fields,
                    joined.joined
                ),
            }),
            (Some(want), None) => out.push(Violation {
                clause: Clause::Target,
                key: entry.key.clone(),
                detail: format!(
                    "no target row where R and M give {} (M entry {:?})",
                    show(want),
                    joined.joined
                ),
            }),
            (None, Some(got)) => out.push(Violation {
                clause: Clause::Target,
                key: entry.key.clone(),
                detail: format!(
                    "an INNER target row {} where the R entry's key {} has no live M entry",
                    show(got),
                    show(&entry.join_key)
                ),
            }),
            _ => {}
        }
    }
    for (key, got) in &rows {
        if !backed.contains(key) && !s.held.contains(*key) {
            out.push(Violation {
                clause: Clause::Target,
                key: key.to_string(),
                detail: format!("a target row {} with no live R entry", show(got)),
            });
        }
    }
    exempt
}

/// [`Clause::KeyRow`].
fn key_clause(s: &HiddenSnapshot, out: &mut Vec<Violation>) {
    let keys: BTreeSet<Vec<&str>> = s.keys.iter().filter_map(|k| full_key(k)).collect();
    let mut reported = BTreeSet::new();
    for entry in live(&s.from_entries).chain(live(&s.joined_entries)) {
        let Some(key) = full_key(&entry.join_key) else {
            continue;
        };
        if !keys.contains(&key) && reported.insert(key.clone()) {
            out.push(Violation {
                clause: Clause::KeyRow,
                key: show(&entry.join_key),
                detail: format!("entry {} holds a join key with no J row", entry.key),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trellis::dev::staging::one_to_one_join_snapshot::{FanOutMarker, JoinedRow, TargetRow};

    fn some(values: &[&str]) -> Vec<Option<String>> {
        values.iter().map(|v| Some(v.to_string())).collect()
    }

    fn entry(key: &str, join_key: &str, value: &str, lsn: u64) -> HiddenEntry {
        HiddenEntry {
            key: key.to_string(),
            join_key: some(&[join_key]),
            values: some(&[join_key, value]),
            tombstone: false,
            applied_lsn: Some(lsn),
        }
    }

    fn view(rows: &[(&str, &str, &str)]) -> SourceView {
        SourceView {
            columns: vec!["k".to_string(), "x".to_string()],
            rows: rows
                .iter()
                .map(|(key, k, x)| (key.to_string(), some(&[k, x])))
                .collect(),
            ..SourceView::default()
        }
    }

    fn row(key: &str, values: &[&str]) -> TargetRow {
        TargetRow {
            key: key.to_string(),
            values: some(values),
        }
    }

    fn joined(key: &str, m: Option<&str>, values: &[&str]) -> JoinedRow {
        JoinedRow {
            key: key.to_string(),
            joined: m.map(str::to_string),
            values: some(values),
        }
    }

    /// `a` rows 1, 2 on key 10 and 3 on key 20; `b` row 7 on key 10 and none
    /// on 20; a `LEFT` join whose field is `a.x || b.y`. Consistent.
    fn good() -> HiddenSnapshot {
        HiddenSnapshot {
            target: "t".to_string(),
            status: "live".to_string(),
            building: false,
            left: true,
            fields: vec!["f".to_string()],
            from_entries: vec![
                entry("1", "10", "a1", 5),
                entry("2", "10", "a2", 6),
                entry("3", "20", "a3", 7),
            ],
            joined_entries: vec![entry("7", "10", "b7", 4)],
            keys: vec![some(&["10"]), some(&["20"])],
            rows: vec![row("1", &["a1b7"]), row("2", &["a2b7"]), row("3", &["a3"])],
            joined_rows: vec![
                joined("1", Some("7"), &["a1b7"]),
                joined("2", Some("7"), &["a2b7"]),
                joined("3", None, &["a3"]),
            ],
            from_applied: view(&[("1", "10", "a1"), ("2", "10", "a2"), ("3", "20", "a3")]),
            joined_applied: view(&[("7", "10", "b7")]),
            markers: Vec::new(),
            held: BTreeSet::new(),
        }
    }

    fn clauses(s: &HiddenSnapshot) -> Vec<Clause> {
        let mut found: Vec<Clause> = check(s).violations().iter().map(|v| v.clause).collect();
        found.dedup();
        found
    }

    #[test]
    fn a_consistent_snapshot_passes() {
        assert_eq!(
            check(&good()),
            Verdict::Checked {
                violations: Vec::new(),
                exempt: 0
            }
        );
    }

    #[test]
    fn a_definition_not_live_or_building_is_skipped() {
        let mut s = good();
        s.rows.clear();
        s.status = "backfilling".to_string();
        assert!(matches!(check(&s), Verdict::Skipped(_)));
        s.status = "live".to_string();
        s.building = true;
        assert!(matches!(check(&s), Verdict::Skipped(_)));
    }

    #[test]
    fn an_r_entry_that_differs_from_its_row_as_applied_is_caught() {
        let mut s = good();
        s.from_applied.rows[0].1 = some(&["10", "a1-new"]);
        assert_eq!(clauses(&s), [Clause::FromEntry]);
        // A key the ring can't vouch for is left out, and so is every key
        // under an unapplied truncate.
        s.from_applied.uncertain.insert("1".to_string());
        assert_eq!(clauses(&s), []);
        s.from_applied.uncertain.clear();
        s.from_applied.global = true;
        assert_eq!(clauses(&s), []);
    }

    #[test]
    fn a_missing_or_orphaned_r_entry_is_caught() {
        let mut s = good();
        s.from_applied
            .rows
            .push(("4".to_string(), some(&["20", "a4"])));
        let v = check(&s);
        assert_eq!(v.violations().len(), 1, "{v:?}");
        assert_eq!(v.violations()[0].key, "4");
        let mut s = good();
        s.from_applied.rows.remove(2);
        assert_eq!(clauses(&s), [Clause::FromEntry]);
    }

    #[test]
    fn an_m_entry_that_differs_from_its_row_as_applied_is_caught() {
        let mut s = good();
        s.joined_applied.rows[0].1 = some(&["10", "b7-new"]);
        assert_eq!(clauses(&s), [Clause::JoinedEntry]);
        let mut s = good();
        s.joined_applied.rows.clear();
        assert_eq!(clauses(&s), [Clause::JoinedEntry]);
    }

    /// The lost update a missing `J` lock leaves: `M` moved on, a target row
    /// still joins the old `b` values.
    #[test]
    fn a_target_row_that_joins_a_stale_m_entry_is_caught() {
        let mut s = good();
        s.rows[1] = row("2", &["a2b7-old"]);
        let v = check(&s);
        assert_eq!(v.violations().len(), 1, "{v:?}");
        assert_eq!(v.violations()[0].clause, Clause::Target);
        assert_eq!(v.violations()[0].key, "2");
    }

    #[test]
    fn a_missing_or_extra_target_row_is_caught() {
        let mut s = good();
        s.rows.remove(2);
        assert_eq!(clauses(&s), [Clause::Target]);
        let mut s = good();
        s.rows.push(row("9", &["x"]));
        assert_eq!(clauses(&s), [Clause::Target]);
        // A held row is left out.
        s.held.insert("9".to_string());
        assert_eq!(clauses(&s), []);
    }

    #[test]
    fn inner_keeps_only_matched_rows() {
        let mut s = good();
        s.left = false;
        // Row 3's key has no live M entry, so INNER has no row for it.
        assert_eq!(clauses(&s), [Clause::Target]);
        s.rows.remove(2);
        assert_eq!(clauses(&s), []);
        // A tombstoned M entry matches nothing.
        s.joined_entries[0].tombstone = true;
        s.joined_applied.rows.clear();
        for j in &mut s.joined_rows {
            j.joined = None;
        }
        assert_eq!(clauses(&s), [Clause::Target]);
        s.rows.clear();
        assert_eq!(clauses(&s), []);
    }

    #[test]
    fn a_tombstoned_r_entry_backs_no_row() {
        let mut s = good();
        s.from_entries[2].tombstone = true;
        s.from_applied.rows.remove(2);
        s.joined_rows.remove(2);
        assert_eq!(clauses(&s), [Clause::Target]);
        s.rows.remove(2);
        assert_eq!(clauses(&s), []);
    }

    /// A key handed between two `b` rows applied out of order holds two live
    /// entries for a moment, and a row joins the newest.
    #[test]
    fn a_row_joins_the_newest_of_two_live_m_entries() {
        let mut s = good();
        s.joined_entries.push(entry("8", "10", "b8", 9));
        s.joined_applied
            .rows
            .push(("8".to_string(), some(&["10", "b8"])));
        assert_eq!(clauses(&s), [Clause::JoinPick]);
        s.joined_rows[0] = joined("1", Some("8"), &["a1b8"]);
        s.joined_rows[1] = joined("2", Some("8"), &["a2b8"]);
        assert_eq!(clauses(&s), [Clause::Target]);
        s.rows[0] = row("1", &["a1b8"]);
        s.rows[1] = row("2", &["a2b8"]);
        assert_eq!(clauses(&s), []);
        // On a tie of applied positions the higher key wins; an entry with
        // no applied position is the oldest.
        s.joined_entries[1].applied_lsn = Some(4);
        assert_eq!(clauses(&s), []);
        s.joined_entries[1].applied_lsn = None;
        assert_eq!(clauses(&s), [Clause::JoinPick]);
    }

    #[test]
    fn a_pending_marker_exempts_exactly_the_rows_after_its_cursor() {
        let mut s = good();
        // Row 2, after the cursor at 1, still joins the old b row; row 1,
        // at the cursor, has been recomputed.
        s.rows[1] = row("2", &["a2b-old"]);
        s.markers.push(FanOutMarker {
            join_key: vec!["10".to_string()],
            cursor: Some("1".to_string()),
        });
        assert_eq!(
            check(&s),
            Verdict::Checked {
                violations: Vec::new(),
                exempt: 1
            }
        );
        // A row at or before the cursor is not exempt.
        s.rows[0] = row("1", &["a1b-old"]);
        assert_eq!(clauses(&s), [Clause::Target]);
        // A cursor at the key's start exempts every row on it.
        s.markers[0].cursor = None;
        assert_eq!(
            check(&s),
            Verdict::Checked {
                violations: Vec::new(),
                exempt: 2
            }
        );
        // A marker of another key exempts nothing here.
        s.markers[0].join_key = vec!["20".to_string()];
        assert_eq!(clauses(&s), [Clause::Target]);
    }

    /// The cursor compares as `"C"` does: bytes, not numbers.
    #[test]
    fn the_cursor_orders_entry_keys_as_bytes() {
        let mut s = good();
        s.from_entries[1].key = "10".to_string();
        s.from_applied.rows[1].0 = "10".to_string();
        s.joined_rows[1].key = "10".to_string();
        s.rows[1] = row("10", &["stale"]);
        s.markers.push(FanOutMarker {
            join_key: vec!["10".to_string()],
            cursor: Some("1".to_string()),
        });
        // "10" > "1" bytewise, so row "10" is after the cursor.
        assert!(check(&s).violations().is_empty());
        s.markers[0].cursor = Some("2".to_string());
        assert_eq!(clauses(&s), [Clause::Target]);
    }

    #[test]
    fn a_null_join_key_matches_nothing_and_needs_no_j_row() {
        let mut s = good();
        s.from_entries[2].join_key = vec![None];
        s.from_entries[2].values = vec![None, Some("a3".to_string())];
        s.from_applied.rows[2].1 = vec![None, Some("a3".to_string())];
        s.keys.pop();
        assert_eq!(clauses(&s), []);
    }

    #[test]
    fn a_join_key_with_no_j_row_is_caught() {
        let mut s = good();
        s.keys.remove(0);
        let v = check(&s);
        assert_eq!(v.violations().len(), 1, "{v:?}");
        assert_eq!(v.violations()[0].clause, Clause::KeyRow);
    }

    #[test]
    fn the_snapshots_pick_is_checked_against_m() {
        let mut s = good();
        s.joined_rows[2] = joined("3", Some("7"), &["a3b7"]);
        assert_eq!(clauses(&s), [Clause::JoinPick]);
        let mut s = good();
        s.joined_rows.pop();
        assert_eq!(clauses(&s), [Clause::JoinPick]);
    }
}
