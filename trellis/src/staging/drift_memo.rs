//! The capture pass's memo of which definitions' typed copies it found
//! current.
//!
//! [`super::schema_change::pause_readers_of_retyped`] compares every column
//! Trellis created from the source's types ([`crate::defs::copies`]) with the
//! type define would give it from the live schema. That costs a handful of
//! queries per definition per table, every `reconcile_interval`, and nearly
//! every pass finds nothing changed. This module lets the pass skip the
//! comparison for a definition whose inputs are exactly what they were when
//! the comparison last found every column current.
//!
//! # What the skip rests on
//!
//! The comparison is a function of:
//!
//! 1. the definition, its source and target (the *context*); and
//! 2. the catalog state of every relation the comparison reads
//!    ([`crate::defs::copies::inputs`]): the source, the target and, for a
//!    `JOIN`, its joined table and its hidden tables.
//!
//! The *stamp* of a definition is its context and the [`fingerprint`] of each
//! of those relations. A fingerprint covers the relation's oid (a dropped and
//! re-created table is another one), every `pg_attribute` row (number, name,
//! type, modifier, collation, nullability, dropped flag) and the `xmin` of
//! that row, and the `xmin` of every `pg_index` row. A change to a column
//! writes a new row version, so its `xmin` is new: a column re-typed and
//! re-typed back is not the fingerprint it was. The pass skips a definition
//! only when its stamp equals the one recorded when its comparison found no
//! column of the table drifted.
//!
//! # The fingerprints are read before the checks
//!
//! The pass reads every fingerprint once ([`Fingerprints::read`]) before any
//! check runs, and records that stamp, not one read afterwards. A change that
//! commits between the fingerprint read and a check is seen by the check but
//! not by the stamp it records, so the next pass's stamp differs and the
//! definition is checked again, at the cost of one more comparison. Were the
//! stamp read after the check, a change in between would be recorded as
//! checked, and skipped from then on. And because a fingerprint can't return
//! to an earlier value, a stamp that is equal now says the relations were not
//! changed at any point since it was read, so what the check read is what the
//! relations hold now.
//!
//! # What it can miss
//!
//! - A change after the pass read its fingerprints is found by the next
//!   pass, as a change after a check always was; a skipped definition is
//!   checked against the state as of the start of the pass, not of the
//!   moment. Detection of a change that commits during a pass is at most one
//!   `reconcile_interval` later than without the memo.
//! - What the comparison reads outside the relations' columns and indexes:
//!   a type's own definition (an `ALTER TYPE ... RENAME`), the operators and
//!   functions define's type inference resolves an expression's type with,
//!   `search_path`. None of them changes a column's type oid or modifier,
//!   which is what drift is.
//! - `xmin` is 32 bits. Vacuum freezing a row keeps its `xmin` (Postgres
//!   marks the row frozen in its header bits), so a freeze changes no
//!   fingerprint; a repeat of an `xmin` needs a wraparound of the transaction
//!   counter between two passes.
//!
//! The capture pass's re-check of a 1-1 `JOIN`'s unique key on its joined
//! table skips the same way, on that table's fingerprint alone
//! ([`key_skippable`]), in a memo of its own, and so does its check of the
//! index backing a table's declared `KEY (...)` ([`backing_skippable`]).
//!
//! Its check of a `MIN`/`MAX` definition's probe indexes skips the same way
//! while every probe index was met, and it also holds, for each index it
//! found missing or invalid, the time it first found it so
//! ([`ProbeMemo`]): the definition pauses only once the index has read
//! missing on every check for [`EXTREME_INDEX_GRACE`] ([`grace`]). That time
//! lives in this process only, so a restart, or another process taking over
//! the staging worker, starts the clock again, which can only delay a pause.
//!
//! Nothing resets the memo for correctness: every input of the comparison is
//! in the stamp. It is dropped for a definition when its comparison finds a
//! column drifted, when it is paused (its table is checked without it), and
//! for an instance when its staging worker stops here
//! ([`forget_instance`]); a restarted process starts empty, and its first
//! pass checks everything.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio_postgres::GenericClient;
use tokio_postgres::types::Type;

use crate::capture::columns::{CaptureCatalog, CaptureReader};
use crate::defs::copies;

/// One pass's view of the catalog, read before any check: every
/// fingerprint a stamp needs.
#[derive(Debug, Default)]
pub(crate) struct Fingerprints {
    /// The [`fingerprint`] of each relation, by the name SQL resolves it with
    /// (`copies::inputs`). A relation that doesn't exist has one too.
    relations: HashMap<String, String>,
}

impl Fingerprints {
    /// Reads the fingerprints of every relation the checks of `catalog`'s
    /// definitions read: one query, however many definitions. A paused
    /// definition's are read too, for the refresh of its message
    /// (`staging::pause_message`).
    pub(crate) async fn read(
        client: &impl GenericClient,
        schema: &str,
        catalog: &CaptureCatalog,
    ) -> Result<Fingerprints, tokio_postgres::Error> {
        let mut names: BTreeSet<String> = BTreeSet::new();
        for reader in &catalog.definitions {
            names.extend(inputs_of(schema, reader));
        }
        let names: Vec<String> = names.into_iter().collect();
        let relations = if names.is_empty() {
            HashMap::new()
        } else {
            client
                .query_typed(FINGERPRINT_SQL, &[(&names, Type::TEXT_ARRAY)])
                .await?
                .into_iter()
                .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
                .collect()
        };
        Ok(Fingerprints { relations })
    }

    /// The stamp of `reader`: its context and the fingerprint of each
    /// relation its check reads. `None` when a fingerprint is missing, which
    /// never lets the definition be skipped.
    pub(crate) fn stamp(&self, schema: &str, reader: &CaptureReader) -> Option<String> {
        use std::fmt::Write;
        let mut stamp = String::new();
        let _ = write!(
            stamp,
            "{}|{}|{}|{:?}",
            reader.id, reader.source, reader.target, reader.def
        );
        for name in inputs_of(schema, reader) {
            let _ = write!(stamp, "|{name}={}", self.relations.get(&name)?);
        }
        Some(stamp)
    }

    /// The [`fingerprint`] of `relation`, named as SQL resolves it
    /// ([`crate::defs::ddl::regclass_arg`]), when a definition's check reads
    /// it.
    pub(crate) fn of(&self, relation: &str) -> Option<&str> {
        self.relations.get(relation).map(String::as_str)
    }
}

/// The relations `reader`'s check reads, as SQL resolves them.
fn inputs_of(schema: &str, reader: &CaptureReader) -> Vec<String> {
    copies::inputs(
        schema,
        &reader.def,
        &reader.source,
        &reader.target,
        reader.join.as_deref().map(|joined| (reader.id, joined)),
    )
}

/// Each name's fingerprint: its oid (`absent` when there is no such
/// relation), then a digest of its columns' rows and its indexes' rows. The
/// `xmin` of a row is the transaction that wrote its current version.
const FINGERPRINT_SQL: &str = "\
select k.name, \
       coalesce(c.oid::text, 'absent') || ':' || coalesce( \
         pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to( \
           coalesce((select pg_catalog.string_agg( \
                       a.attnum::text || ',' || a.attname::text || ',' || a.atttypid::text \
                         || ',' || a.atttypmod::text || ',' || a.attcollation::text \
                         || ',' || a.attisdropped::text || ',' || a.attnotnull::text \
                         || ',' || a.xmin::text, \
                       ';' order by a.attnum) \
                     from pg_catalog.pg_attribute a \
                     where a.attrelid = c.oid and a.attnum > 0), '') \
           || '#' || \
           coalesce((select pg_catalog.string_agg( \
                       i.indexrelid::text || ',' || i.xmin::text, ';' order by i.indexrelid) \
                     from pg_catalog.pg_index i where i.indrelid = c.oid), ''), \
         'UTF8')), 'hex'), '') \
from pg_catalog.unnest($1::text[]) as k(name) \
left join pg_catalog.pg_class c on c.oid = pg_catalog.to_regclass(k.name)";

/// By capture instance, table checked and definition id: the stamp that
/// definition's last check of that table found every column current at.
type Memo = BTreeMap<(String, String, i64), String>;

static MEMO: Mutex<Memo> = Mutex::new(BTreeMap::new());

fn with_memo<T>(f: impl FnOnce(&mut Memo) -> T) -> T {
    f(&mut MEMO.lock().unwrap_or_else(PoisonError::into_inner))
}

/// Whether `id`'s last check of `table` found every column current at
/// `stamp`.
pub(crate) fn skippable(instance: &str, table: &str, id: i64, stamp: &str) -> bool {
    with_memo(|memo| {
        memo.get(&(instance.to_string(), table.to_string(), id))
            .is_some_and(|found| found == stamp)
    })
}

/// Records the outcome of one full comparison of `id`'s copies on `table`:
/// `Some` of the stamp (read before the comparison) when every column was
/// current, `None` when one had drifted.
pub(crate) fn record(instance: &str, table: &str, id: i64, current: Option<String>) {
    let key = (instance.to_string(), table.to_string(), id);
    with_memo(|memo| match current {
        Some(stamp) => {
            memo.insert(key, stamp);
        }
        None => {
            memo.remove(&key);
        }
    });
    #[cfg(any(test, feature = "internals"))]
    probe::count(instance, id);
}

/// Keeps only the entries of `table` for the definitions in `ids`.
pub(crate) fn retain_readers(instance: &str, table: &str, ids: &[i64]) {
    with_memo(|memo| {
        memo.retain(|(i, t, id), _| i != instance || t != table || ids.contains(id));
    });
}

/// Keeps only the entries of `instance` for the tables in `known`.
pub(crate) fn retain_tables(instance: &str, known: &BTreeSet<&str>) {
    with_memo(|memo| memo.retain(|(i, t, _), _| i != instance || known.contains(t.as_str())));
    with_keys(|memo| memo.retain(|(i, t, _), _| i != instance || known.contains(t.as_str())));
    with_backings(|memo| memo.retain(|(i, t), _| i != instance || known.contains(t.as_str())));
    with_probes(|memo| memo.retain(|(i, t, _), _| i != instance || known.contains(t.as_str())));
}

/// Forgets everything remembered for `instance`: its staging worker in this
/// process stopped.
pub(crate) fn forget_instance(instance: &str) {
    with_memo(|memo| memo.retain(|(i, _, _), _| i != instance));
    with_keys(|memo| memo.retain(|(i, _, _), _| i != instance));
    with_backings(|memo| memo.retain(|(i, _), _| i != instance));
    with_probes(|memo| memo.retain(|(i, _, _), _| i != instance));
}

/// The same skip for the capture pass's re-check of a 1-1 `JOIN`'s unique
/// key (`schema_change::pause_joins_without_unique_key`), which reads only
/// the joined table's indexes: by capture instance, joined table and
/// definition id, the stamp (the joined table's [`fingerprint`] and the
/// `ON` columns) at which the last check found the key. A drop, a create or
/// a validity change of any index writes a new `pg_index` row version, so
/// it changes the fingerprint and forces the check.
static KEYS: Mutex<Memo> = Mutex::new(BTreeMap::new());

fn with_keys<T>(f: impl FnOnce(&mut Memo) -> T) -> T {
    f(&mut KEYS.lock().unwrap_or_else(PoisonError::into_inner))
}

/// Keeps only the key-check entries of `table` for the 1-1 `JOIN`s in `ids`:
/// a paused or dropped definition's is forgotten.
pub(crate) fn retain_key_readers(instance: &str, table: &str, ids: &[i64]) {
    with_keys(|memo| {
        memo.retain(|(i, t, id), _| i != instance || t != table || ids.contains(id));
    });
}

/// Whether `id`'s last key check of `table` found the key at `stamp`.
pub(crate) fn key_skippable(instance: &str, table: &str, id: i64, stamp: &str) -> bool {
    with_keys(|memo| {
        memo.get(&(instance.to_string(), table.to_string(), id))
            .is_some_and(|found| found == stamp)
    })
}

/// Records the outcome of one key check of `id` on `table`: `Some` of the
/// stamp (read before the check) when the key was there, `None` when it
/// was gone.
pub(crate) fn record_key(instance: &str, table: &str, id: i64, found: Option<String>) {
    let key = (instance.to_string(), table.to_string(), id);
    with_keys(|memo| match found {
        Some(stamp) => {
            memo.insert(key, stamp);
        }
        None => {
            memo.remove(&key);
        }
    });
    #[cfg(any(test, feature = "internals"))]
    probe::count_key(instance, id);
}

/// The same skip for the capture pass's check of the unique index backing
/// a table's declared `KEY (...)`
/// (`schema_change::pause_readers_of_unbacked_key`): by capture instance and
/// table, the stamp (the table's [`fingerprint`] and the key's columns) at
/// which the last check found an index backing the key. The key is the
/// table's, not a definition's: every transform reading a table keys it one
/// way (ADR-0026).
static BACKINGS: Mutex<BTreeMap<(String, String), String>> = Mutex::new(BTreeMap::new());

fn with_backings<T>(f: impl FnOnce(&mut BTreeMap<(String, String), String>) -> T) -> T {
    f(&mut BACKINGS.lock().unwrap_or_else(PoisonError::into_inner))
}

/// Whether the last check of `table`'s `KEY` found it backed at `stamp`.
pub(crate) fn backing_skippable(instance: &str, table: &str, stamp: &str) -> bool {
    with_backings(|memo| {
        memo.get(&(instance.to_string(), table.to_string()))
            .is_some_and(|found| found == stamp)
    })
}

/// Records the outcome of one check of `table`'s `KEY`: `Some` of the
/// stamp (read before the check) when an index backed it, `None` when none
/// did.
pub(crate) fn record_backing(instance: &str, table: &str, found: Option<String>) {
    let key = (instance.to_string(), table.to_string());
    with_backings(|memo| match found {
        Some(stamp) => {
            memo.insert(key, stamp);
        }
        None => {
            memo.remove(&key);
        }
    });
    #[cfg(any(test, feature = "internals"))]
    probe::count_backing(instance, table);
}

/// Forgets the last check of `table`'s `KEY`: no running transform keys
/// the table by one now.
pub(crate) fn forget_backing(instance: &str, table: &str) {
    with_backings(|memo| memo.remove(&(instance.to_string(), table.to_string())));
}

/// How long a `MIN`/`MAX` definition's probe index may read missing or
/// invalid on every check before the capture pass pauses the definition
/// (`schema_change::pause_readers_of_missing_probe_index`): long enough for a
/// rebuild that drops the old index before it builds the new one on most
/// tables. Wall-clock time, not a count of passes, whose interval is
/// configurable. A fixed constant, not an option.
pub(crate) const EXTREME_INDEX_GRACE: Duration = Duration::from_secs(5 * 60);

/// Where one check of a probe index leaves its grace ([`grace`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Grace {
    /// The index is there: no clock runs.
    Met,
    /// Missing or invalid on every check since `since`, for less than the
    /// grace.
    Running { since: Instant },
    /// Missing or invalid on every check since `since`, for the grace or
    /// longer: the definition pauses.
    Passed { since: Instant },
}

/// One check of a probe index at `now`: `met` is what it read, and
/// `missing_since` the time the checks before it first read it missing, if
/// they still do. A met check resets the clock; a first missed check starts
/// it at `now`.
pub(crate) fn grace(
    missing_since: Option<Instant>,
    now: Instant,
    met: bool,
    grace: Duration,
) -> Grace {
    if met {
        return Grace::Met;
    }
    let since = missing_since.unwrap_or(now);
    if now.saturating_duration_since(since) >= grace {
        Grace::Passed { since }
    } else {
        Grace::Running { since }
    }
}

/// The grace the capture pass of instance `instance` gives a probe index:
/// [`EXTREME_INDEX_GRACE`], unless a test set another
/// ([`set_extreme_index_grace`]).
pub(crate) fn extreme_index_grace(instance: &str) -> Duration {
    #[cfg(any(test, feature = "internals"))]
    if let Some(grace) = GRACE_OVERRIDES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(instance)
    {
        return *grace;
    }
    let _ = instance;
    EXTREME_INDEX_GRACE
}

#[cfg(any(test, feature = "internals"))]
static GRACE_OVERRIDES: Mutex<BTreeMap<String, Duration>> = Mutex::new(BTreeMap::new());

/// Sets the grace a probe index gets on the instance with schema `schema` in
/// database `database`, in this process, for tests that would otherwise wait
/// [`EXTREME_INDEX_GRACE`]. Never an option of a running instance.
#[cfg(any(test, feature = "internals"))]
pub fn set_extreme_index_grace(database: &str, schema: &str, grace: Duration) {
    GRACE_OVERRIDES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(
            crate::capture::reconcile::instance_key(database, schema),
            grace,
        );
}

/// What the capture pass remembers of one `MIN`/`MAX` definition's probe
/// indexes on one table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ProbeMemo {
    /// The stamp (the table's fingerprint and the definition) its last check
    /// found every probe index met at.
    pub(crate) met_at: Option<String>,
    /// Each probe index (by its columns) its checks have read missing or
    /// invalid since the time given, every check since.
    pub(crate) missing_since: BTreeMap<Vec<String>, Instant>,
}

/// By capture instance, table and definition id.
static PROBES: Mutex<BTreeMap<(String, String, i64), ProbeMemo>> = Mutex::new(BTreeMap::new());

fn with_probes<T>(f: impl FnOnce(&mut BTreeMap<(String, String, i64), ProbeMemo>) -> T) -> T {
    f(&mut PROBES.lock().unwrap_or_else(PoisonError::into_inner))
}

/// What the capture pass remembers of definition `id`'s probe indexes on
/// `table`.
pub(crate) fn probe_memo(instance: &str, table: &str, id: i64) -> ProbeMemo {
    with_probes(|memo| {
        memo.get(&(instance.to_string(), table.to_string(), id))
            .cloned()
            .unwrap_or_default()
    })
}

/// Records one check of definition `id`'s probe indexes on `table`.
pub(crate) fn record_probe(instance: &str, table: &str, id: i64, found: ProbeMemo) {
    with_probes(|memo| {
        memo.insert((instance.to_string(), table.to_string(), id), found);
    });
    #[cfg(any(test, feature = "internals"))]
    probe::count_probe(instance, id);
}

/// Keeps only the probe entries of `table` for the definitions in `ids`: a
/// paused or dropped definition's clock is forgotten.
pub(crate) fn retain_probe_readers(instance: &str, table: &str, ids: &[i64]) {
    with_probes(|memo| {
        memo.retain(|(i, t, id), _| i != instance || t != table || ids.contains(id));
    });
}

/// How many times the capture pass has run the full comparison for a
/// definition, for tests that pin the skip. Compiled only under
/// `cfg(any(test, feature = "internals"))`.
#[cfg(any(test, feature = "internals"))]
mod probe {
    use super::*;

    static CHECKS: Mutex<BTreeMap<(String, i64), u64>> = Mutex::new(BTreeMap::new());

    pub(super) fn count(instance: &str, id: i64) {
        *CHECKS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((instance.to_string(), id))
            .or_default() += 1;
    }

    #[cfg(any(test, feature = "internals"))]
    pub(super) fn get(instance: &str, id: i64) -> u64 {
        CHECKS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(instance.to_string(), id))
            .copied()
            .unwrap_or(0)
    }

    static KEY_CHECKS: Mutex<BTreeMap<(String, i64), u64>> = Mutex::new(BTreeMap::new());

    pub(super) fn count_key(instance: &str, id: i64) {
        *KEY_CHECKS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((instance.to_string(), id))
            .or_default() += 1;
    }

    pub(super) fn get_key(instance: &str, id: i64) -> u64 {
        KEY_CHECKS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(instance.to_string(), id))
            .copied()
            .unwrap_or(0)
    }

    static PROBE_CHECKS: Mutex<BTreeMap<(String, i64), u64>> = Mutex::new(BTreeMap::new());

    pub(super) fn count_probe(instance: &str, id: i64) {
        *PROBE_CHECKS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((instance.to_string(), id))
            .or_default() += 1;
    }

    pub(super) fn get_probe(instance: &str, id: i64) -> u64 {
        PROBE_CHECKS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(instance.to_string(), id))
            .copied()
            .unwrap_or(0)
    }

    static BACKING_CHECKS: Mutex<BTreeMap<(String, String), u64>> = Mutex::new(BTreeMap::new());

    pub(super) fn count_backing(instance: &str, table: &str) {
        *BACKING_CHECKS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((instance.to_string(), table.to_string()))
            .or_default() += 1;
    }

    pub(super) fn get_backing(instance: &str, table: &str) -> u64 {
        BACKING_CHECKS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(instance.to_string(), table.to_string()))
            .copied()
            .unwrap_or(0)
    }
}

/// The number of checks the capture pass has run of `MIN`/`MAX` definition
/// `id`'s probe indexes, for the instance with schema `schema` in database
/// `database`, for tests that pin the skip.
#[cfg(any(test, feature = "internals"))]
pub fn probe_index_checks(database: &str, schema: &str, id: i64) -> u64 {
    probe::get_probe(
        &crate::capture::reconcile::instance_key(database, schema),
        id,
    )
}

/// The number of checks the capture pass has run of the index backing
/// `table`'s declared `KEY`, for the instance with schema `schema` in
/// database `database`, for tests that pin the skip.
#[cfg(any(test, feature = "internals"))]
pub fn backing_checks(database: &str, schema: &str, table: &str) -> u64 {
    probe::get_backing(
        &crate::capture::reconcile::instance_key(database, schema),
        table,
    )
}

/// The number of unique-key checks the capture pass has run for 1-1 `JOIN`
/// `id` of the instance with schema `schema` in database `database`, for
/// tests that pin the skip.
#[cfg(any(test, feature = "internals"))]
pub fn key_checks(database: &str, schema: &str, id: i64) -> u64 {
    probe::get_key(
        &crate::capture::reconcile::instance_key(database, schema),
        id,
    )
}

/// The number of full comparisons the capture pass has run for definition
/// `id` of the instance with schema `schema` in database `database`.
#[cfg(any(test, feature = "internals"))]
pub fn checks(database: &str, schema: &str, id: i64) -> u64 {
    probe::get(
        &crate::capture::reconcile::instance_key(database, schema),
        id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn current(stamp: &str) -> Option<String> {
        Some(stamp.to_string())
    }

    #[test]
    fn an_equal_stamp_skips_and_a_different_one_does_not() {
        record("i1", "public.a", 1, current("s1"));
        assert!(skippable("i1", "public.a", 1, "s1"));
        assert!(!skippable("i1", "public.a", 1, "s2"));
        assert!(!skippable("i1", "public.b", 1, "s1"));
        assert!(!skippable("i1", "public.a", 2, "s1"));
        assert!(!skippable("other", "public.a", 1, "s1"));
    }

    #[test]
    fn a_drifted_check_forgets_the_entry() {
        record("i2", "public.a", 1, current("s1"));
        record("i2", "public.a", 1, None);
        assert!(!skippable("i2", "public.a", 1, "s1"));
    }

    /// The grace as a function of (missing since, now, met): a met check
    /// resets the clock, a first miss starts it, and only misses on every
    /// check for the whole grace pass it.
    #[test]
    fn the_probe_grace_runs_only_through_unbroken_misses() {
        let grace_len = Duration::from_secs(300);
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);

        assert_eq!(grace(None, at(0), true, grace_len), Grace::Met);
        assert_eq!(grace(Some(at(0)), at(400), true, grace_len), Grace::Met);
        assert_eq!(
            grace(None, at(10), false, grace_len),
            Grace::Running { since: at(10) },
            "a first miss starts the clock"
        );
        assert_eq!(
            grace(Some(at(10)), at(309), false, grace_len),
            Grace::Running { since: at(10) }
        );
        assert_eq!(
            grace(Some(at(10)), at(310), false, grace_len),
            Grace::Passed { since: at(10) },
            "missed on every check for the whole grace"
        );
        // A met check in between resets: the next miss starts again.
        assert_eq!(grace(Some(at(10)), at(200), true, grace_len), Grace::Met);
        assert_eq!(
            grace(None, at(400), false, grace_len),
            Grace::Running { since: at(400) }
        );
        // A clock that reads earlier than the start never passes early.
        assert_eq!(
            grace(Some(at(10)), at(5), false, grace_len),
            Grace::Running { since: at(10) }
        );
        // No grace: the first miss passes.
        assert_eq!(
            grace(None, at(1), false, Duration::ZERO),
            Grace::Passed { since: at(1) }
        );
    }

    #[test]
    fn the_grace_is_five_minutes_unless_a_test_sets_another() {
        assert_eq!(EXTREME_INDEX_GRACE, Duration::from_secs(300));
        assert_eq!(extreme_index_grace("no-override"), EXTREME_INDEX_GRACE);
        set_extreme_index_grace("db", "s", Duration::ZERO);
        assert_eq!(
            extreme_index_grace(&crate::capture::reconcile::instance_key("db", "s")),
            Duration::ZERO
        );
    }

    #[test]
    fn retention_is_per_instance() {
        record("i3", "public.a", 1, current("s"));
        record("i3", "public.a", 2, current("s"));
        record("i3", "public.b", 1, current("s"));
        record("i4", "public.a", 1, current("s"));
        retain_readers("i3", "public.a", &[2]);
        assert!(!skippable("i3", "public.a", 1, "s"));
        assert!(skippable("i3", "public.a", 2, "s"));
        assert!(skippable("i3", "public.b", 1, "s"));
        retain_tables("i3", &BTreeSet::from(["public.a"]));
        assert!(!skippable("i3", "public.b", 1, "s"));
        assert!(skippable("i4", "public.a", 1, "s"));
        forget_instance("i4");
        assert!(!skippable("i4", "public.a", 1, "s"));
    }

    use crate::{Config, Trellis, TrellisOptions};
    use tokio_postgres::{Client, NoTls};

    const SCHEMA: &str = "trellis";

    /// `public.posts` with two definitions on it, in a database of its own.
    async fn fixture(
        cluster: &testkit::TestCluster,
    ) -> (testkit::TestDatabase, Client, CaptureCatalog, Vec<i64>) {
        let db = cluster.create_isolated_database().await;
        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(
            "set search_path to trellis, public; \
             create table public.posts (id int primary key, kind int, label varchar(10))",
        )
        .await
        .expect("seed");
        let trellis = Trellis::connect(
            Config::from_dsn(db.dsn().to_string()).expect("dsn"),
            TrellisOptions::default(),
        )
        .await
        .expect("connect a definer");
        for text in [
            "TRANSFORM per_kind FROM public.posts GROUP BY kind WITHOUT SOURCE INDEX SELECT kind AS kind, COUNT(*) AS n",
            "TRANSFORM labels FROM public.posts SELECT label AS label",
        ] {
            trellis.apply(text).await.expect(text);
        }
        let catalog = catalog(&raw).await;
        let ids = catalog.definitions.iter().map(|r| r.id).collect();
        (db, raw, catalog, ids)
    }

    async fn catalog(raw: &Client) -> CaptureCatalog {
        crate::capture::columns::load_catalog(raw)
            .await
            .expect("catalog")
    }

    /// One check of `public.posts` with `drift` read earlier.
    async fn check(
        raw: &mut Client,
        instance: &str,
        catalog: &CaptureCatalog,
        drift: &Fingerprints,
    ) -> bool {
        crate::staging::schema_change::pause_readers_of_retyped(
            raw,
            SCHEMA,
            &crate::defs::floor::Floors::of_this_binary(),
            instance,
            catalog,
            drift,
            "public.posts",
        )
        .await
        .expect("check")
    }

    async fn read(raw: &Client, catalog: &CaptureCatalog) -> Fingerprints {
        Fingerprints::read(raw, SCHEMA, catalog)
            .await
            .expect("fingerprints")
    }

    fn runs(instance: &str, ids: &[i64]) -> Vec<u64> {
        ids.iter().map(|id| probe::get(instance, *id)).collect()
    }

    /// The race the stamp's order exists for: a retype that commits after
    /// the pass read its fingerprints and before a definition's check. The
    /// check sees it, so a definition not yet recorded is paused in that
    /// very pass, and one recorded as current is skipped (it is compared
    /// against the state the pass started from) and paused by the next.
    #[tokio::test]
    async fn a_retype_between_the_fingerprint_read_and_the_check_is_found_by_the_next_pass() {
        let cluster = testkit::TestCluster::start();
        let (_db, mut raw, catalog, ids) = fixture(&cluster).await;

        // Not recorded yet: compared against the live schema.
        let instance = "race-unrecorded";
        let before = read(&raw, &catalog).await;
        raw.batch_execute("alter table public.posts alter column kind type bigint")
            .await
            .expect("widen between the read and the check");
        assert!(check(&mut raw, instance, &catalog, &before).await);
        let stamp = before
            .stamp(SCHEMA, &catalog.definitions[0])
            .expect("stamp");
        assert!(!skippable(instance, "public.posts", ids[0], &stamp));

        // Recorded as current, then retyped after the read.
        let (_db, mut raw, catalog, ids) = fixture(&cluster).await;
        let instance = "race-recorded";
        let settled = read(&raw, &catalog).await;
        assert!(!check(&mut raw, instance, &catalog, &settled).await);
        assert_eq!(runs(instance, &ids), [1, 1]);
        assert!(!check(&mut raw, instance, &catalog, &settled).await);
        assert_eq!(
            runs(instance, &ids),
            [1, 1],
            "skipped while nothing changed"
        );

        let stale = read(&raw, &catalog).await;
        raw.batch_execute("alter table public.posts alter column kind type bigint")
            .await
            .expect("widen between the read and the check");
        assert!(
            !check(&mut raw, instance, &catalog, &stale).await,
            "compared against the state the pass read"
        );
        let fresh = read(&raw, &catalog).await;
        assert!(
            check(&mut raw, instance, &catalog, &fresh).await,
            "found by the next pass"
        );
    }

    /// A change that commits between the read and a check, and one that
    /// returns the schema to what it was, both leave the check recorded
    /// under the stamp read before them: the next pass's stamp differs, so
    /// it compares again.
    #[tokio::test]
    async fn a_change_during_a_check_is_never_recorded_as_the_state_after_it() {
        let cluster = testkit::TestCluster::start();
        for (instance, alter) in [
            (
                "adds-a-column",
                "alter table public.posts add column extra int",
            ),
            (
                "there-and-back",
                "alter table public.posts alter column kind type bigint; \
                 alter table public.posts alter column kind type integer",
            ),
        ] {
            let (_db, mut raw, catalog, ids) = fixture(&cluster).await;
            let before = read(&raw, &catalog).await;
            raw.batch_execute(alter).await.expect(alter);
            assert!(!check(&mut raw, instance, &catalog, &before).await);
            assert_eq!(runs(instance, &ids), [1, 1], "{instance}");

            let after = read(&raw, &catalog).await;
            assert!(!check(&mut raw, instance, &catalog, &after).await);
            assert_eq!(runs(instance, &ids), [2, 2], "{instance}: compared again");
            assert!(!check(&mut raw, instance, &catalog, &after).await);
            assert_eq!(runs(instance, &ids), [2, 2], "{instance}: and then settled");
        }
    }
}
