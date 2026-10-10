//! The capture pass's memo of which definitions' typed copies it found
//! current (issue #858).
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
//! 1. the definition, its source and target, and the relationships it can
//!    read through, with the projection table each has (the *context*); and
//! 2. the catalog state of every relation the comparison reads
//!    ([`crate::defs::copies::inputs`]): the source, the to-side of each
//!    relationship, the target and, for an aggregate, its ledger and
//!    group-delta table, and each relationship's projection table.
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
//! - `xmin` is 32 bits and is replaced by a frozen marker when vacuum freezes
//!   the row. A frozen row compares as a change once (a recheck); a repeat of
//!   an `xmin` needs a wraparound of the transaction counter between two
//!   passes.
//!
//! Nothing resets the memo for correctness: every input of the comparison is
//! in the stamp. It is dropped for a definition when its comparison finds a
//! column drifted, when it is paused (its table is checked without it), and
//! for an instance when its staging worker stops here
//! ([`forget_instance`]); a restarted process starts empty, and its first
//! pass checks everything.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Mutex, PoisonError};

use tokio_postgres::GenericClient;

use crate::capture::columns::{CaptureCatalog, CaptureReader};
use crate::defs::copies;

/// One pass's view of the catalog, read before any check: each relationship's
/// projection table and every fingerprint a stamp needs.
#[derive(Debug, Default)]
pub(crate) struct Fingerprints {
    /// Each relationship's projection table, by relationship id.
    projections: HashMap<i64, String>,
    /// The [`fingerprint`] of each relation, by the name SQL resolves it with
    /// (`copies::inputs`). A relation that doesn't exist has one too.
    relations: HashMap<String, String>,
}

impl Fingerprints {
    /// Reads the fingerprints of every relation the checks of `catalog`'s
    /// unpaused definitions read: two queries, however many definitions.
    pub(crate) async fn read(
        client: &impl GenericClient,
        schema: &str,
        catalog: &CaptureCatalog,
    ) -> Result<Fingerprints, tokio_postgres::Error> {
        let projections: HashMap<i64, String> = client
            .query(
                "select relationship_id, projection_table from relationship_projections",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        let mut names: BTreeSet<String> = BTreeSet::new();
        for reader in catalog.definitions.iter().filter(|r| !r.capture_failed) {
            names.extend(inputs_of(schema, catalog, &projections, reader));
        }
        let names: Vec<String> = names.into_iter().collect();
        let relations = if names.is_empty() {
            HashMap::new()
        } else {
            client
                .query(FINGERPRINT_SQL, &[&names])
                .await?
                .into_iter()
                .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
                .collect()
        };
        Ok(Fingerprints {
            projections,
            relations,
        })
    }

    /// The stamp of `reader`: its context and the fingerprint of each
    /// relation its check reads. `None` when a fingerprint is missing, which
    /// never lets the definition be skipped.
    pub(crate) fn stamp(
        &self,
        schema: &str,
        catalog: &CaptureCatalog,
        reader: &CaptureReader,
    ) -> Option<String> {
        use std::fmt::Write;
        let mut stamp = String::new();
        let _ = write!(
            stamp,
            "{}|{}|{}|{:?}",
            reader.id, reader.source, reader.target, reader.def
        );
        for rel in catalog
            .relationships
            .iter()
            .filter(|r| r.qualified_from_table() == reader.source)
        {
            let _ = write!(
                stamp,
                "|rel {} {} {}.{} {} {} {:?} {:?}",
                rel.id,
                rel.def.name,
                rel.qualified_from_table(),
                rel.def.from_col,
                rel.qualified_to_table(),
                rel.def.to_col,
                rel.cardinality,
                self.projections.get(&rel.id),
            );
        }
        for name in inputs_of(schema, catalog, &self.projections, reader) {
            let _ = write!(stamp, "|{name}={}", self.relations.get(&name)?);
        }
        Some(stamp)
    }
}

/// The relations `reader`'s check reads, as SQL resolves them.
fn inputs_of(
    schema: &str,
    catalog: &CaptureCatalog,
    projections: &HashMap<i64, String>,
    reader: &CaptureReader,
) -> Vec<String> {
    let declared: Vec<_> = catalog
        .relationships
        .iter()
        .filter(|r| r.qualified_from_table() == reader.source)
        .collect();
    let projection_tables: Vec<&str> = declared
        .iter()
        .filter_map(|r| projections.get(&r.id).map(String::as_str))
        .collect();
    copies::inputs(
        schema,
        &reader.def,
        &reader.source,
        &reader.target,
        &declared,
        &projection_tables,
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

/// What the memo remembers of one definition's check of one table: the stamp
/// it found every column current at, and the tables Trellis created that it
/// found a column on (for the in-place re-type's owners).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    stamp: String,
    copy_tables: Vec<String>,
}

/// By capture instance, table checked and definition id.
type Memo = BTreeMap<(String, String, i64), Entry>;

static MEMO: Mutex<Memo> = Mutex::new(BTreeMap::new());

fn with_memo<T>(f: impl FnOnce(&mut Memo) -> T) -> T {
    f(&mut MEMO.lock().unwrap_or_else(PoisonError::into_inner))
}

/// The tables holding a column `id`'s last check of `table` found, if that
/// check found every column current at `stamp`.
pub(crate) fn skippable(instance: &str, table: &str, id: i64, stamp: &str) -> Option<Vec<String>> {
    with_memo(|memo| {
        memo.get(&(instance.to_string(), table.to_string(), id))
            .filter(|entry| entry.stamp == stamp)
            .map(|entry| entry.copy_tables.clone())
    })
}

/// Records the outcome of one full comparison of `id`'s copies on `table`:
/// `Some` of the stamp (read before the comparison) and the tables holding a
/// copy when every column was current, `None` when one had drifted.
pub(crate) fn record(instance: &str, table: &str, id: i64, current: Option<(String, Vec<String>)>) {
    let key = (instance.to_string(), table.to_string(), id);
    with_memo(|memo| match current {
        Some((stamp, copy_tables)) => {
            memo.insert(key, Entry { stamp, copy_tables });
        }
        None => {
            memo.remove(&key);
        }
    });
    #[cfg(any(test, feature = "test-util"))]
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
}

/// Forgets everything remembered for `instance`: its staging worker in this
/// process stopped.
pub(crate) fn forget_instance(instance: &str) {
    with_memo(|memo| memo.retain(|(i, _, _), _| i != instance));
}

/// How many times the capture pass has run the full comparison for a
/// definition, for tests that pin the skip. Compiled only under
/// `cfg(any(test, feature = "test-util"))`.
#[cfg(any(test, feature = "test-util"))]
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

    pub(super) fn get(instance: &str, id: i64) -> u64 {
        CHECKS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(instance.to_string(), id))
            .copied()
            .unwrap_or(0)
    }
}

/// The number of full comparisons the capture pass has run for definition
/// `id` of the instance with schema `schema` in database `database`.
#[cfg(any(test, feature = "test-util"))]
pub fn checks(database: &str, schema: &str, id: i64) -> u64 {
    probe::get(
        &crate::capture::reconcile::instance_key(database, schema),
        id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn current(stamp: &str) -> Option<(String, Vec<String>)> {
        Some((stamp.to_string(), vec!["t".to_string()]))
    }

    #[test]
    fn an_equal_stamp_skips_and_a_different_one_does_not() {
        record("i1", "public.a", 1, current("s1"));
        assert_eq!(skippable("i1", "public.a", 1, "s1"), Some(vec!["t".into()]));
        assert_eq!(skippable("i1", "public.a", 1, "s2"), None);
        assert_eq!(skippable("i1", "public.b", 1, "s1"), None);
        assert_eq!(skippable("i1", "public.a", 2, "s1"), None);
        assert_eq!(skippable("other", "public.a", 1, "s1"), None);
    }

    #[test]
    fn a_drifted_check_forgets_the_entry() {
        record("i2", "public.a", 1, current("s1"));
        record("i2", "public.a", 1, None);
        assert_eq!(skippable("i2", "public.a", 1, "s1"), None);
    }

    #[test]
    fn retention_is_per_instance() {
        record("i3", "public.a", 1, current("s"));
        record("i3", "public.a", 2, current("s"));
        record("i3", "public.b", 1, current("s"));
        record("i4", "public.a", 1, current("s"));
        retain_readers("i3", "public.a", &[2]);
        assert_eq!(skippable("i3", "public.a", 1, "s"), None);
        assert!(skippable("i3", "public.a", 2, "s").is_some());
        assert!(skippable("i3", "public.b", 1, "s").is_some());
        retain_tables("i3", &BTreeSet::from(["public.a"]));
        assert_eq!(skippable("i3", "public.b", 1, "s"), None);
        assert!(skippable("i4", "public.a", 1, "s").is_some());
        forget_instance("i4");
        assert_eq!(skippable("i4", "public.a", 1, "s"), None);
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
            "TRANSFORM per_kind FROM public.posts GROUP BY kind SELECT kind AS kind, COUNT(*) AS n",
            "TRANSFORM labels FROM public.posts SELECT label AS label",
        ] {
            trellis.apply(text).await.expect(text);
        }
        let catalog = catalog(&raw).await;
        let ids = catalog.definitions.iter().map(|r| r.id).collect();
        (db, raw, catalog, ids)
    }

    async fn catalog(raw: &Client) -> CaptureCatalog {
        crate::capture::columns::load_catalog(raw, SCHEMA)
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
            .stamp(SCHEMA, &catalog, &catalog.definitions[0])
            .expect("stamp");
        assert_eq!(skippable(instance, "public.posts", ids[0], &stamp), None);

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
