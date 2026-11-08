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
//! # A star
//!
//! A star 1-1 `JOIN` (#1460) has an `M_p` and `J` rows per position, and an
//! `R` entry holds a join key per position. [`check_star`] runs the same
//! clauses over a [`StarSnapshot`], position by position:
//! [`Clause::JoinedEntry`] and [`Clause::KeyRow`] against each position's
//! `M_p`, table and `J` rows (a key's `J` row at one position doesn't stand
//! for it at another), [`Clause::JoinPick`] at each position, and
//! [`Clause::Target`] against the fields read with every position's pick,
//! with a row only when every `INNER` position matched. A pending fan-out at
//! any position exempts the entries it covers there. A violation about one
//! position names it. The two-table JOIN is the one-position case: [`check`]
//! is [`check_star`] over [`StarSnapshot::from`] its snapshot, and names no
//! position. [`sample`] reads the two-table snapshot; a star's sampling
//! reads a [`StarSnapshot`] once the snapshot read covers every position.
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
    FanOutMarker, HiddenEntry, HiddenSnapshot, SourceView, TargetRow, hidden_snapshot,
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
    /// The star position whose `M`, `J` or pick it concerns, for a star of
    /// more than one position; `None` for the two-table JOIN and for the
    /// clauses over `R` and the target alone.
    pub position: Option<u8>,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.clause.name(), self.key, self.detail)?;
        if let Some(p) = self.position {
            write!(f, " (position {p})")?;
        }
        Ok(())
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

/// One `R` entry of a star: its key, its join key at each position, the
/// columns `R` copies, and its state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StarFromEntry {
    pub key: String,
    /// The join key's columns as text at each position (`join_keys[p - 1]`).
    pub join_keys: Vec<Vec<Option<String>>>,
    pub values: Vec<Option<String>>,
    pub tombstone: bool,
}

/// One position's hidden state: its kind, `M_p`, its `J` rows, its table as
/// `M_p` has applied it, and its pending fan-outs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StarPositionView {
    /// `LEFT` (else `INNER`).
    pub left: bool,
    /// `M_p`, every entry.
    pub joined_entries: Vec<HiddenEntry>,
    /// `J`'s rows at this position: each join key's columns as text.
    pub keys: Vec<Vec<Option<String>>>,
    /// The position's table as `M_p` has applied it.
    pub joined_applied: SourceView,
    pub markers: Vec<FanOutMarker>,
}

/// The target row a live `R` entry's fields give, read with the newest live
/// `M_p` entry on its join key at each position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StarJoinedRow {
    pub key: String,
    /// The `M_p` entry read at each position (`joined[p - 1]`), or `None`.
    pub joined: Vec<Option<String>>,
    pub values: Vec<Option<String>>,
}

/// A star 1-1 `JOIN` definition's hidden state under one snapshot, position
/// by position (the layout of #1771: `R`, an `M_p` per position, `J` keyed
/// by `(position, key)`). The two-table JOIN's [`HiddenSnapshot`] is the
/// one-position case ([`StarSnapshot::from`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StarSnapshot {
    pub target: String,
    pub status: String,
    pub building: bool,
    pub fields: Vec<String>,
    /// `R`, every entry.
    pub from_entries: Vec<StarFromEntry>,
    /// Position `p` at `positions[p - 1]`.
    pub positions: Vec<StarPositionView>,
    pub rows: Vec<TargetRow>,
    /// One per live `R` entry.
    pub joined_rows: Vec<StarJoinedRow>,
    pub from_applied: SourceView,
    pub held: BTreeSet<String>,
}

impl From<&HiddenSnapshot> for StarSnapshot {
    fn from(s: &HiddenSnapshot) -> Self {
        StarSnapshot {
            target: s.target.clone(),
            status: s.status.clone(),
            building: s.building,
            fields: s.fields.clone(),
            from_entries: s
                .from_entries
                .iter()
                .map(|e| StarFromEntry {
                    key: e.key.clone(),
                    join_keys: vec![e.join_key.clone()],
                    values: e.values.clone(),
                    tombstone: e.tombstone,
                })
                .collect(),
            positions: vec![StarPositionView {
                left: s.left,
                joined_entries: s.joined_entries.clone(),
                keys: s.keys.clone(),
                joined_applied: s.joined_applied.clone(),
                markers: s.markers.clone(),
            }],
            rows: s.rows.clone(),
            joined_rows: s
                .joined_rows
                .iter()
                .map(|j| StarJoinedRow {
                    key: j.key.clone(),
                    joined: vec![j.joined.clone()],
                    values: j.values.clone(),
                })
                .collect(),
            from_applied: s.from_applied.clone(),
            held: s.held.clone(),
        }
    }
}

/// Checks `snapshot` (the module doc). Pure: it reads nothing but the
/// snapshot. The two-table JOIN's check is the star's at one position
/// ([`check_star`]).
pub fn check(snapshot: &HiddenSnapshot) -> Verdict {
    check_star(&StarSnapshot::from(snapshot))
}

/// [`check`] over a star, at every position: [`Clause::JoinedEntry`] and
/// [`Clause::KeyRow`] per position against that position's `M_p`, table and
/// `J` rows; [`Clause::JoinPick`] at each position; and [`Clause::Target`]
/// for each live `R` entry against its fields read with every position's
/// pick, a row only when every `INNER` position matched. A pending fan-out
/// at any position exempts an entry it covers (the module doc).
pub fn check_star(snapshot: &StarSnapshot) -> Verdict {
    if snapshot.status != "live" {
        return Verdict::Skipped(format!("status {}", snapshot.status));
    }
    if snapshot.building {
        return Verdict::Skipped("a build is running".to_string());
    }
    let star = snapshot.positions.len() > 1;
    let label = |p: usize| star.then_some(p as u8 + 1);
    let mut violations = Vec::new();
    ledger_clause(
        Clause::FromEntry,
        None,
        snapshot
            .from_entries
            .iter()
            .filter(|e| !e.tombstone)
            .map(|e| (e.key.as_str(), &e.values))
            .collect(),
        &snapshot.from_applied,
        &snapshot.held,
        &mut violations,
    );
    for (p, position) in snapshot.positions.iter().enumerate() {
        ledger_clause(
            Clause::JoinedEntry,
            label(p),
            live(&position.joined_entries)
                .map(|e| (e.key.as_str(), &e.values))
                .collect(),
            &position.joined_applied,
            &BTreeSet::new(),
            &mut violations,
        );
    }
    let exempt = target_clause(snapshot, &mut violations);
    for p in 0..snapshot.positions.len() {
        key_clause(snapshot, p, label(p), &mut violations);
    }
    Verdict::Checked { violations, exempt }
}

fn live(entries: &[HiddenEntry]) -> impl Iterator<Item = &HiddenEntry> {
    entries.iter().filter(|e| !e.tombstone)
}

/// [`Clause::FromEntry`] or [`Clause::JoinedEntry`], over the live entries'
/// values by entry key.
fn ledger_clause(
    clause: Clause,
    position: Option<u8>,
    entries: BTreeMap<&str, &Vec<Option<String>>>,
    applied: &SourceView,
    held: &BTreeSet<String>,
    out: &mut Vec<Violation>,
) {
    if applied.global {
        return;
    }
    let skip = |key: &str| applied.uncertain.contains(key) || held.contains(key);
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
                position,
            }),
            Some(entry) if **entry != **values => out.push(Violation {
                clause,
                key: key.to_string(),
                detail: format!(
                    "the entry holds {} where the row as applied is {} (columns {:?})",
                    show(entry),
                    show(values),
                    applied.columns
                ),
                position,
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
                position,
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

/// The `M` entries a row was read with: the one position's alone for the
/// two-table JOIN (as its messages always read), every position's for a
/// star.
fn show_picks(picks: &[Option<String>]) -> String {
    match picks {
        [one] => format!("{one:?}"),
        all => format!("{all:?}"),
    }
}

/// [`Clause::Target`] and [`Clause::JoinPick`]; returns how many live `R`
/// entries a marker exempted.
fn target_clause(s: &StarSnapshot, out: &mut Vec<Violation>) -> usize {
    let star = s.positions.len() > 1;
    let rows: BTreeMap<&str, &Vec<Option<String>>> =
        s.rows.iter().map(|r| (r.key.as_str(), &r.values)).collect();
    let joined_rows: BTreeMap<&str, _> =
        s.joined_rows.iter().map(|j| (j.key.as_str(), j)).collect();
    let markers: Vec<BTreeMap<Vec<&str>, Option<&str>>> = s
        .positions
        .iter()
        .map(|position| {
            position
                .markers
                .iter()
                .map(|m| {
                    (
                        m.join_key.iter().map(String::as_str).collect(),
                        m.cursor.as_deref(),
                    )
                })
                .collect()
        })
        .collect();
    let mut exempt = 0;
    let mut backed = BTreeSet::new();
    'entries: for entry in s.from_entries.iter().filter(|e| !e.tombstone) {
        backed.insert(entry.key.as_str());
        if s.held.contains(&entry.key) {
            continue;
        }
        let keys: Vec<Option<Vec<&str>>> = (0..s.positions.len())
            .map(|p| entry.join_keys.get(p).and_then(|k| full_key(k)))
            .collect();
        let covered = keys.iter().zip(&markers).any(|(key, markers)| {
            key.as_ref()
                .and_then(|k| markers.get(k))
                .is_some_and(|cursor| cursor.is_none_or(|c| entry.key.as_str() > c))
        });
        if covered {
            exempt += 1;
            continue;
        }
        let Some(joined) = joined_rows.get(entry.key.as_str()) else {
            out.push(Violation {
                clause: Clause::JoinPick,
                key: entry.key.clone(),
                detail: "the snapshot read no joined row for a live R entry".to_string(),
                position: None,
            });
            continue;
        };
        for (p, (position, key)) in s.positions.iter().zip(&keys).enumerate() {
            let pick = key
                .as_ref()
                .and_then(|k| newest(&position.joined_entries, k));
            let read = joined.joined.get(p).cloned().flatten();
            if pick.map(|m| m.key.as_str()) != read.as_deref() {
                out.push(Violation {
                    clause: Clause::JoinPick,
                    key: entry.key.clone(),
                    detail: format!(
                        "the snapshot read M entry {:?}, the newest live one is {:?}",
                        read,
                        pick.map(|m| &m.key)
                    ),
                    position: star.then_some(p as u8 + 1),
                });
                continue 'entries;
            }
        }
        // The first `INNER` position the row matched nothing at.
        let unmatched = s
            .positions
            .iter()
            .enumerate()
            .find(|(p, position)| {
                !position.left && joined.joined.get(*p).cloned().flatten().is_none()
            })
            .map(|(p, _)| p);
        let expected = unmatched.is_none().then_some(&joined.values);
        let picks = show_picks(&joined.joined);
        match (expected, rows.get(entry.key.as_str())) {
            (Some(want), Some(got)) if want != *got => out.push(Violation {
                clause: Clause::Target,
                key: entry.key.clone(),
                detail: format!(
                    "the target holds {} where R and M give {} (fields {:?}, M entry {})",
                    show(got),
                    show(want),
                    s.fields,
                    picks
                ),
                position: None,
            }),
            (Some(want), None) => out.push(Violation {
                clause: Clause::Target,
                key: entry.key.clone(),
                detail: format!(
                    "no target row where R and M give {} (M entry {})",
                    show(want),
                    picks
                ),
                position: None,
            }),
            (None, Some(got)) => {
                let p = unmatched.expect("no expected row means an unmatched INNER position");
                out.push(Violation {
                    clause: Clause::Target,
                    key: entry.key.clone(),
                    detail: format!(
                        "an INNER target row {} where the R entry's key {} has no live M entry",
                        show(got),
                        show(entry.join_keys.get(p).map_or(&[][..], Vec::as_slice))
                    ),
                    position: star.then_some(p as u8 + 1),
                });
            }
            _ => {}
        }
    }
    for (key, got) in &rows {
        if !backed.contains(key) && !s.held.contains(*key) {
            out.push(Violation {
                clause: Clause::Target,
                key: key.to_string(),
                detail: format!("a target row {} with no live R entry", show(got)),
                position: None,
            });
        }
    }
    exempt
}

/// [`Clause::KeyRow`] at position `p` (from 0).
fn key_clause(s: &StarSnapshot, p: usize, position: Option<u8>, out: &mut Vec<Violation>) {
    let view = &s.positions[p];
    let keys: BTreeSet<Vec<&str>> = view.keys.iter().filter_map(|k| full_key(k)).collect();
    let mut reported = BTreeSet::new();
    let from = s
        .from_entries
        .iter()
        .filter(|e| !e.tombstone)
        .filter_map(|e| Some((e.key.as_str(), e.join_keys.get(p)?)));
    let joined = live(&view.joined_entries).map(|e| (e.key.as_str(), &e.join_key));
    for (entry, join_key) in from.chain(joined) {
        let Some(key) = full_key(join_key) else {
            continue;
        };
        if !keys.contains(&key) && reported.insert(key.clone()) {
            out.push(Violation {
                clause: Clause::KeyRow,
                key: show(join_key),
                detail: format!("entry {entry} holds a join key with no J row"),
                position,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trellis::dev::staging::one_to_one_join_snapshot::JoinedRow;

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

    /// The two-table JOIN's messages read as they did before the check
    /// took positions: no position, and the one `M` pick alone.
    #[test]
    fn the_two_table_verdicts_name_no_position() {
        let mut s = good();
        s.rows[1] = row("2", &["a2b7-old"]);
        s.joined_rows[2] = joined("3", Some("7"), &["a3b7"]);
        s.keys.remove(0);
        let texts: Vec<String> = check(&s)
            .violations()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            texts,
            [
                "[target row is its R entry joined to the newest live M entry] 2: the target \
                 holds (a2b7-old) where R and M give (a2b7) (fields [\"f\"], M entry Some(\"7\"))",
                "[the snapshot's M pick is the newest live entry] 3: the snapshot read M entry \
                 Some(\"7\"), the newest live one is None",
                "[every join key of a live R or M entry has its J row] (10): entry 1 holds a \
                 join key with no J row",
            ]
        );
    }

    /// A star of three tables (two positions), as #1771 lays it out: `R`
    /// with a join key per position, an `M_p` and `J` rows per position.
    /// `a` rows 1 and 2 on keys (10, 30), 3 on (20, 30); position 1's `b`
    /// row 7 on 10, position 2's row 8 on 30. Position 1 `LEFT`, position 2
    /// `INNER`; the field is `a.x || b1.y || b2.y`. Consistent.
    fn star() -> StarSnapshot {
        let from = |key: &str, k1: &str, k2: &str, value: &str| StarFromEntry {
            key: key.to_string(),
            join_keys: vec![some(&[k1]), some(&[k2])],
            values: some(&[k1, k2, value]),
            tombstone: false,
        };
        let star_view = |rows: &[(&str, &str, &str, &str)]| SourceView {
            columns: vec!["k1".to_string(), "k2".to_string(), "x".to_string()],
            rows: rows
                .iter()
                .map(|(key, k1, k2, x)| (key.to_string(), some(&[k1, k2, x])))
                .collect(),
            ..SourceView::default()
        };
        let joined = |key: &str, picks: [Option<&str>; 2], values: &[&str]| StarJoinedRow {
            key: key.to_string(),
            joined: picks.iter().map(|p| p.map(str::to_string)).collect(),
            values: some(values),
        };
        StarSnapshot {
            target: "t".to_string(),
            status: "live".to_string(),
            building: false,
            fields: vec!["f".to_string()],
            from_entries: vec![
                from("1", "10", "30", "a1"),
                from("2", "10", "30", "a2"),
                from("3", "20", "30", "a3"),
            ],
            positions: vec![
                StarPositionView {
                    left: true,
                    joined_entries: vec![entry("7", "10", "b7", 4)],
                    keys: vec![some(&["10"]), some(&["20"])],
                    joined_applied: view(&[("7", "10", "b7")]),
                    markers: Vec::new(),
                },
                StarPositionView {
                    left: false,
                    joined_entries: vec![entry("8", "30", "c8", 5)],
                    keys: vec![some(&["30"])],
                    joined_applied: view(&[("8", "30", "c8")]),
                    markers: Vec::new(),
                },
            ],
            rows: vec![
                row("1", &["a1b7c8"]),
                row("2", &["a2b7c8"]),
                row("3", &["a3c8"]),
            ],
            joined_rows: vec![
                joined("1", [Some("7"), Some("8")], &["a1b7c8"]),
                joined("2", [Some("7"), Some("8")], &["a2b7c8"]),
                joined("3", [None, Some("8")], &["a3c8"]),
            ],
            from_applied: star_view(&[
                ("1", "10", "30", "a1"),
                ("2", "10", "30", "a2"),
                ("3", "20", "30", "a3"),
            ]),
            held: BTreeSet::new(),
        }
    }

    fn star_found(s: &StarSnapshot) -> Vec<(Clause, Option<u8>)> {
        let mut found: Vec<(Clause, Option<u8>)> = check_star(s)
            .violations()
            .iter()
            .map(|v| (v.clause, v.position))
            .collect();
        found.dedup();
        found
    }

    #[test]
    fn a_consistent_star_snapshot_passes() {
        assert_eq!(
            check_star(&star()),
            Verdict::Checked {
                violations: Vec::new(),
                exempt: 0
            }
        );
    }

    /// A lost update at position 2 (#1459's Q1 plant): `M_2` moved on, a
    /// target row still joins the old values there.
    #[test]
    fn a_star_target_row_stale_at_one_position_is_caught() {
        let mut s = star();
        s.rows[1] = row("2", &["a2b7c8-old"]);
        assert_eq!(star_found(&s), [(Clause::Target, None)]);
        let v = check_star(&s);
        assert!(
            v.violations()[0]
                .detail
                .contains(r#"[Some("7"), Some("8")]"#),
            "{v:?}"
        );
    }

    #[test]
    fn a_star_m_entry_off_its_table_is_named_by_position() {
        let mut s = star();
        s.positions[1].joined_applied.rows[0].1 = some(&["30", "c8-new"]);
        assert_eq!(star_found(&s), [(Clause::JoinedEntry, Some(2))]);
        let mut s = star();
        s.positions[0].joined_applied.rows.clear();
        assert_eq!(star_found(&s), [(Clause::JoinedEntry, Some(1))]);
    }

    /// `J` is keyed by `(position, key)`: a key's row at one position
    /// doesn't stand for it at another.
    #[test]
    fn a_star_join_key_needs_its_j_row_at_its_own_position() {
        let mut s = star();
        s.positions[1].keys.clear();
        s.positions[1].keys.push(some(&["10"]));
        let v = check_star(&s);
        assert_eq!(star_found(&s), [(Clause::KeyRow, Some(2))], "{v:?}");
        assert_eq!(v.violations().len(), 1, "{v:?}");
    }

    /// An `INNER` position drops a row it matches nothing at, whatever the
    /// others matched; a `LEFT` one keeps it.
    #[test]
    fn a_star_row_needs_a_match_at_every_inner_position() {
        let mut s = star();
        s.positions[1].joined_entries[0].tombstone = true;
        s.positions[1].joined_applied.rows.clear();
        for j in &mut s.joined_rows {
            j.joined[1] = None;
        }
        assert_eq!(star_found(&s), [(Clause::Target, Some(2))]);
        s.rows.clear();
        assert_eq!(star_found(&s), []);
        s.positions[1].left = true;
        assert_eq!(star_found(&s), [(Clause::Target, None)]);
    }

    #[test]
    fn a_star_pick_is_checked_at_each_position() {
        let mut s = star();
        s.joined_rows[2].joined[1] = None;
        assert_eq!(star_found(&s), [(Clause::JoinPick, Some(2))]);
        let mut s = star();
        s.joined_rows[2].joined[0] = Some("7".to_string());
        assert_eq!(star_found(&s), [(Clause::JoinPick, Some(1))]);
    }

    /// A pending fan-out at any position exempts the rows after its cursor
    /// on its key there, and nothing at another position's same key.
    #[test]
    fn a_star_marker_exempts_at_its_own_position() {
        let mut s = star();
        s.rows[1] = row("2", &["stale"]);
        s.positions[1].markers.push(FanOutMarker {
            join_key: vec!["30".to_string()],
            cursor: Some("1".to_string()),
        });
        assert_eq!(
            check_star(&s),
            Verdict::Checked {
                violations: Vec::new(),
                exempt: 2
            }
        );
        // Key 30 at position 1 holds nothing: the marker exempts no row.
        s.positions[0].markers = std::mem::take(&mut s.positions[1].markers);
        assert_eq!(star_found(&s), [(Clause::Target, None)]);
    }

    /// The same check at four tables (three positions), the root row hot at
    /// every position on one key.
    #[test]
    fn a_star_of_four_tables_is_checked_at_every_position() {
        let mut s = star();
        for entry in &mut s.from_entries {
            entry.join_keys.push(some(&["30"]));
        }
        s.positions.push(StarPositionView {
            left: true,
            joined_entries: vec![entry("9", "30", "d9", 6)],
            keys: vec![some(&["30"])],
            joined_applied: view(&[("9", "30", "d9")]),
            markers: Vec::new(),
        });
        for j in &mut s.joined_rows {
            j.joined.push(Some("9".to_string()));
        }
        assert_eq!(star_found(&s), []);
        s.positions[2].keys.clear();
        assert_eq!(star_found(&s), [(Clause::KeyRow, Some(3))]);
        s.positions[2].keys.push(some(&["30"]));
        s.positions[2]
            .joined_entries
            .push(entry("10", "30", "d10", 7));
        s.positions[2]
            .joined_applied
            .rows
            .push(("10".to_string(), some(&["30", "d10"])));
        assert_eq!(star_found(&s), [(Clause::JoinPick, Some(3))]);
    }
}
