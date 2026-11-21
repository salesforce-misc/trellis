//! Hidden-row invariants of the epoch tier (#2187, sub-step 8c): what a
//! chain leaves behind in the ring, the hidden tables, the catalog and the
//! capture registry, checked at each quiet point.
//!
//! - **Ring.** Every `seg_<n>` table is empty, and the registry holds the
//!   active segment and at most one other: the empty segment the seal step
//!   sealed when the ring went idle, already drained
//!   (`docs/staging-and-claiming/06-cleanup-and-reclaim.md`, "An idle ring
//!   retires its tail").
//! - **Hidden tables.** A 1-1 target's `<target>__ledger` and an aggregate's
//!   `agg_<id>` exist for each current definition and no other, and each
//!   holds as many rows as the oracle says the target has: a ledger entry
//!   per key the target holds, a group row per group it holds. A tombstone
//!   or a zero group is collected once the drained prefix passes it, so a
//!   count is checked after the engine has settled, not at the first look.
//! - **Catalog and registry.** `transform_definitions` names exactly the
//!   current definitions, the schema graph and the key and capture
//!   registries name exactly the tables a current definition reads or
//!   writes, and no table that carries a definition id holds one that isn't
//!   current.
//! - **Capture.** A capture trigger or function exists only on (a partition
//!   of) a table some current definition reads.
//!
//! The expected counts come from [`crate::oracle::sql_oracle`], the model's
//! authority (the definition as a `SELECT` over the source rows), never
//! from a Trellis table.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use tokio_postgres::{Client, NoTls};
use trellis::Config;
use trellis::dev::defs::ast::{KeySpace, TransformDef};
use trellis::dev::pool::Pool;

use super::{RunError, source_table};
use crate::backend::Roles;
use crate::model::Program;
use crate::oracle;

/// How long the engine has, at a quiet point, to retire the ring's tail and
/// collect the tombstones and zero groups the drained prefix has passed
/// (the maintenance loop's housekeeping pass runs every few seconds).
pub const SETTLE: Duration = Duration::from_secs(45);
const RETRY: Duration = Duration::from_millis(200);

/// The suffix of the ledger a 1-1 target keeps beside it
/// (`trellis::defs::ledger::LEDGER_SUFFIX`, not exported).
const LEDGER_SUFFIX: &str = "__ledger";

/// What a run's hidden-row checks saw at the last quiet point.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HiddenReport {
    /// Each hidden table's qualified name, with the rows the oracle expects
    /// and the rows it holds.
    pub hidden: BTreeMap<String, (i64, i64)>,
    /// The segments the registry held beside the active one.
    pub idle_tail: usize,
    /// The tables whose capture triggers exist.
    pub captured: BTreeSet<String>,
    /// `transform_definitions`' rows.
    pub definitions: i64,
}

/// [`check_hidden_once`], retried for up to `settle` while it fails, so a
/// tail still to retire or a tombstone still to collect isn't a failure
/// until the engine has had the time to do it. `defined` is every
/// definition defined at this quiet point.
///
/// Runs as the operator ([`Roles::admin`]): the catalog is the engine's, not
/// the application's.
pub async fn check_hidden(
    config: &Config,
    roles: &Roles,
    pool: &Pool,
    program: &Program,
    defined: &[TransformDef],
    ever: &[TransformDef],
    settle: Duration,
) -> Result<HiddenReport, RunError> {
    let (admin, connection) = tokio_postgres::connect(&roles.admin, NoTls)
        .await
        .map_err(|e| RunError::Residue {
            failures: vec![format!("hidden: connect: {e}")],
        })?;
    let connection = tokio::spawn(connection);
    let deadline = Instant::now() + settle;
    let result = loop {
        let outcome = check_hidden_once(&admin, config, pool, program, defined, ever).await;
        match outcome {
            Ok(report) => break Ok(report),
            Err(failures) if Instant::now() >= deadline => {
                break Err(RunError::Residue { failures });
            }
            Err(_) => tokio::time::sleep(RETRY).await,
        }
    };
    drop(admin);
    let _ = connection.await;
    result
}

/// `schema.name`, unquoted: how the catalog keeps a table's name.
fn qualified(schema: &str, name: &str) -> String {
    format!("{schema}.{name}")
}

/// One look at the instance. `Err` is every invariant that does not hold.
async fn check_hidden_once(
    admin: &Client,
    config: &Config,
    pool: &Pool,
    program: &Program,
    defined: &[TransformDef],
    ever: &[TransformDef],
) -> Result<HiddenReport, Vec<String>> {
    let mut failures: Vec<String> = Vec::new();
    let mut report = HiddenReport::default();
    let schema = config.schema();

    macro_rules! ask {
        ($what:expr, $sql:expr, $params:expr) => {
            match admin.query($sql, $params).await {
                Ok(rows) => rows,
                Err(e) => {
                    failures.push(format!("{}: {e}", $what));
                    return Err(failures);
                }
            }
        };
    }
    macro_rules! count {
        ($what:expr, $table:expr) => {
            match admin
                .query_one(&format!("select count(*)::bigint from {}", $table), &[])
                .await
            {
                Ok(row) => row.get::<_, i64>(0),
                Err(e) => {
                    failures.push(format!("{}: {e}", $what));
                    return Err(failures);
                }
            }
        };
    }

    // The ring.
    for slot in 0..super::residue::RING_SLOTS {
        let n = count!(
            format!("rows of seg_{slot}"),
            format!("\"{schema}\".\"seg_{slot}\"")
        );
        if n != 0 {
            failures.push(format!("ring: seg_{slot} holds {n} rows at a quiet point"));
        }
    }
    let active = ask!(
        "active segment",
        &format!("select active_seq from \"{schema}\".segment_pointer"),
        &[]
    )
    .first()
    .map(|r| r.get::<_, i64>(0));
    let segments = ask!(
        "segments",
        &format!(
            "select seg_seq, ring_slot::int, state from \"{schema}\".segments order by seg_seq"
        ),
        &[]
    );
    let others: Vec<(i64, i32, String)> = segments
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .filter(|(seq, _, _)| Some(*seq) != active)
        .collect();
    report.idle_tail = others.len();
    if segments.len() as i64 > 2 || others.len() > 1 {
        failures.push(format!(
            "ring: {} segments registered at a quiet point (active {active:?}); \
             a slot other than the active one is not recycled: {others:?}",
            segments.len()
        ));
    }
    if let Some((seq, _, state)) = others.first()
        && state != "drained"
    {
        failures.push(format!(
            "ring: segment {seq} is {state} at a quiet point, not drained"
        ));
    }

    // What is defined, from the program.
    let target_schema = |d: &TransformDef| {
        d.explicit_target_schema
            .clone()
            .unwrap_or_else(|| config.target_schema().to_string())
    };
    // A program table is in the application's schema, `public` (the tiers
    // that call this give their instance no source schema of its own); a
    // chained definition's source is a target, in the target schema.
    let source_schema = |d: &TransformDef| {
        d.explicit_source_schema.clone().unwrap_or_else(|| {
            if program.tables.iter().any(|t| t.name == d.source) {
                "public".to_string()
            } else {
                config.target_schema().to_string()
            }
        })
    };
    let targets: BTreeSet<String> = defined
        .iter()
        .map(|d| qualified(&target_schema(d), &d.target))
        .collect();
    let sources: BTreeSet<String> = defined
        .iter()
        .map(|d| qualified(&source_schema(d), &d.source))
        .collect();
    let tables: BTreeSet<String> = targets.union(&sources).cloned().collect();

    // The catalog's definitions, and their ids.
    let rows = ask!(
        "definitions",
        &format!("select id, target_table from \"{schema}\".transform_definitions"),
        &[]
    );
    report.definitions = rows.len() as i64;
    let ids: BTreeMap<String, i64> = rows
        .iter()
        .map(|r| (r.get::<_, String>(1), r.get::<_, i64>(0)))
        .collect();
    let registered: BTreeSet<String> = ids.keys().cloned().collect();
    diff_sets(
        "catalog: transform_definitions",
        &targets,
        &registered,
        &mut failures,
    );
    if rows.len() != defined.len() {
        failures.push(format!(
            "catalog: transform_definitions holds {} rows for {} definitions",
            rows.len(),
            defined.len()
        ));
    }

    // The hidden tables: which exist.
    let hidden_names: Vec<String> = ask!(
        "hidden tables",
        "select n.nspname || '.' || c.relname from pg_class c \
         join pg_namespace n on n.oid = c.relnamespace \
         where c.relkind in ('r', 'p') and c.relpersistence <> 't' and \
               ((n.nspname = $1 and (c.relname ~ '^agg_[0-9]+$' or c.relname ~ '^join_[0-9]+_[a-z]$')) \
                or c.relname like '%\\_\\_ledger') \
         order by 1",
        &[&schema]
    )
    .iter()
    .map(|r| r.get(0))
    .collect();
    let mut expected_hidden: BTreeMap<String, Option<i64>> = BTreeMap::new();
    for def in defined {
        let target = qualified(&target_schema(def), &def.target);
        let id = ids.get(&target).copied();
        match &def.key_space {
            KeySpace::OneToOne => {
                expected_hidden.insert(
                    qualified(
                        &target_schema(def),
                        &format!("{}{LEDGER_SUFFIX}", def.target),
                    ),
                    Some(0),
                );
            }
            KeySpace::Aggregate { .. } => {
                if let Some(id) = id {
                    expected_hidden.insert(qualified(schema, &format!("agg_{id}")), Some(0));
                }
            }
            // The epoch tier never defines a JOIN: its tables are allowed
            // here, their counts are not modelled.
            KeySpace::Join(_) | KeySpace::JoinOneToOne(_) => {
                if let Some(id) = id {
                    for part in ["p", "m", "j", "t", "l"] {
                        expected_hidden
                            .insert(qualified(schema, &format!("join_{id}_{part}")), None);
                    }
                }
            }
        }
    }
    let present: BTreeSet<String> = hidden_names.iter().cloned().collect();
    let wanted: BTreeSet<String> = expected_hidden.keys().cloned().collect();
    for extra in present.difference(&wanted) {
        failures.push(format!(
            "hidden tables: {extra} exists, and no current definition owns it"
        ));
    }
    for missing in wanted.difference(&present) {
        // A JOIN kind that keeps fewer than five tables is not a leak.
        if expected_hidden[missing].is_some() {
            failures.push(format!(
                "hidden tables: {missing} is missing for a current definition"
            ));
        }
    }

    // The hidden tables: what they hold, against the oracle.
    for def in defined {
        let counted = match &def.key_space {
            KeySpace::OneToOne | KeySpace::Aggregate { .. } => true,
            KeySpace::Join(_) | KeySpace::JoinOneToOne(_) => false,
        };
        if !counted {
            continue;
        }
        let target = qualified(&target_schema(def), &def.target);
        let hidden = match &def.key_space {
            KeySpace::OneToOne => qualified(
                &target_schema(def),
                &format!("{}{LEDGER_SUFFIX}", def.target),
            ),
            _ => match ids.get(&target) {
                Some(id) => qualified(schema, &format!("agg_{id}")),
                None => continue,
            },
        };
        if !present.contains(&hidden) {
            continue;
        }
        let Some(source) = source_table(program, defined, &def.source) else {
            failures.push(format!(
                "hidden tables: {target} reads {:?}, which the program does not declare",
                def.source
            ));
            continue;
        };
        let expected = match oracle::sql_oracle(pool, def, &source.pk_col).await {
            Ok(rows) => rows.len() as i64,
            Err(e) => {
                failures.push(format!("hidden tables: oracle for {target}: {e:?}"));
                continue;
            }
        };
        let (name_schema, name) = hidden.split_once('.').expect("qualified");
        // #2240: an aggregate's zero group stamped at the active segment
        // outlives an idle ring, since the drained prefix cannot pass a
        // segment that is never sealed. Only that shape is left out of the
        // count: `__n = 0`, `__zero`, and the stamp the active segment's.
        // The same holds for a 1-1 ledger's tombstone (#2240): its GC waits
        // for the drained prefix to reach `__applied_seg`, so a tombstone
        // stamped at the active segment is left out, and no other.
        let left_out = match (&def.key_space, active) {
            (KeySpace::Aggregate { .. }, Some(active)) => {
                format!(" where not (__zero and __n = 0 and __stamp = {active})")
            }
            (KeySpace::OneToOne, Some(active)) => {
                format!(" where not (__tombstone and __applied_seg = {active})")
            }
            _ => String::new(),
        };
        let actual = count!(
            format!("rows of {hidden}"),
            format!("\"{name_schema}\".\"{name}\"{left_out}")
        );
        report.hidden.insert(hidden.clone(), (expected, actual));
        if actual != expected {
            let rows: Vec<String> = match admin
                .query(
                    &format!(
                        "select row_to_json(x)::text from \"{name_schema}\".\"{name}\" x limit 3"
                    ),
                    &[],
                )
                .await
            {
                Ok(rows) => rows.iter().map(|r| r.get(0)).collect(),
                Err(e) => vec![e.to_string()],
            };
            failures.push(format!(
                "hidden tables: {hidden} holds {actual} rows, and the oracle expects {expected} \
                 for {target}; first rows: {rows:?}"
            ));
        }
    }

    // The catalog and registries name what is defined, and nothing else.
    let sources_vec: Vec<String> = sources.iter().cloned().collect();
    for (what, sql, expect) in [
        (
            "schema_nodes",
            format!("select table_name from \"{schema}\".schema_nodes"),
            &tables,
        ),
        (
            "definition_keys",
            format!("select table_name from \"{schema}\".definition_keys"),
            &tables,
        ),
    ] {
        let seen: BTreeSet<String> = ask!(what, &sql, &[])
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        diff_sets(&format!("catalog: {what}"), expect, &seen, &mut failures);
    }
    let edges = count!("schema_edges", format!("\"{schema}\".schema_edges"));
    if edges != defined.len() as i64 {
        failures.push(format!(
            "catalog: schema_edges holds {edges} rows for {} definitions",
            defined.len()
        ));
    }
    // Registries keyed by a read table, which a partitioned one spreads over
    // its leaves.
    let read_tree: BTreeSet<String> = ask!(
        "partition trees of the read tables",
        "with roots as ( \
             select to_regclass(quote_ident(split_part(s, '.', 1)) || '.' \
                                || quote_ident(split_part(s, '.', 2)))::oid as oid \
             from unnest($1::text[]) s) \
         select n.nspname || '.' || c.relname from pg_class c \
         join pg_namespace n on n.oid = c.relnamespace \
         where c.oid in (select oid from roots where oid is not null \
                         union select pt.relid from roots, lateral pg_partition_tree(roots.oid) pt \
                         where pt.relid is not null)",
        &[&sources_vec]
    )
    .iter()
    .map(|r| r.get::<_, String>(0))
    .collect();
    for (what, sql) in [
        (
            "capture_triggers",
            format!("select table_name from \"{schema}\".capture_triggers"),
        ),
        (
            "capture_baselines",
            format!("select table_name from \"{schema}\".capture_baselines"),
        ),
        (
            "source_table_versions",
            format!("select source_table from \"{schema}\".source_table_versions"),
        ),
    ] {
        let seen: BTreeSet<String> = ask!(what, &sql, &[])
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        // #2239: the version fence keeps a table's row after its last
        // definition goes. Allowed for a table a definition once read,
        // nothing else.
        let kept: BTreeSet<String> = if what == "source_table_versions" {
            ever.iter()
                .map(|d| qualified(&source_schema(d), &d.source))
                .collect()
        } else {
            BTreeSet::new()
        };
        for table in unexplained(&seen, &read_tree, &kept) {
            failures.push(format!(
                "catalog: {what} names {table}, which no current definition reads"
            ));
        }
    }
    // Every table that carries a definition id holds only current ones.
    let id_columns = ask!(
        "definition id columns",
        "select c.table_name, c.column_name from information_schema.columns c \
         join information_schema.tables t using (table_schema, table_name) \
         where c.table_schema = $1 and t.table_type = 'BASE TABLE' \
           and c.column_name in ('transform_id', 'definition_id') \
         order by 1, 2",
        &[&schema]
    );
    for row in &id_columns {
        let (table, column): (String, String) = (row.get(0), row.get(1));
        let orphans = count!(
            format!("orphans of {table}.{column}"),
            format!(
                "\"{schema}\".\"{table}\" where \"{column}\" not in \
                 (select id from \"{schema}\".transform_definitions)"
            )
        );
        if orphans != 0 {
            failures.push(format!(
                "catalog: {table} holds {orphans} rows for a definition that isn't defined"
            ));
        }
    }

    // Capture triggers and functions exist only on read tables.
    let triggers = ask!(
        "capture triggers",
        "select distinct n.nspname || '.' || c.relname from pg_trigger t \
         join pg_class c on c.oid = t.tgrelid \
         join pg_namespace n on n.oid = c.relnamespace \
         join pg_proc p on p.oid = t.tgfoid \
         join pg_namespace pn on pn.oid = p.pronamespace \
         where not t.tgisinternal and pn.nspname = $1 and p.proname like 'cap\\_%'",
        &[&schema]
    );
    for row in &triggers {
        let table: String = row.get(0);
        if !read_tree.contains(&table) {
            failures.push(format!(
                "capture: {table} has a capture trigger, and no current definition reads it"
            ));
        }
        report.captured.insert(table);
    }
    let orphan_functions = ask!(
        "capture functions",
        "select p.proname from pg_proc p \
         join pg_namespace pn on pn.oid = p.pronamespace \
         where pn.nspname = $1 and p.proname like 'cap\\_%' \
           and not exists (select 1 from pg_trigger t where t.tgfoid = p.oid) \
         order by 1",
        &[&schema]
    );
    for row in &orphan_functions {
        let name: String = row.get(0);
        failures.push(format!(
            "capture: function {name} is used by no trigger, so no table a definition reads has it"
        ));
    }

    if failures.is_empty() {
        Ok(report)
    } else {
        Err(failures)
    }
}

/// The names in `seen` that neither `read` nor `allowed` explains.
fn unexplained<'a>(
    seen: &'a BTreeSet<String>,
    read: &BTreeSet<String>,
    allowed: &BTreeSet<String>,
) -> Vec<&'a String> {
    seen.iter()
        .filter(|t| !read.contains(*t) && !allowed.contains(*t))
        .collect()
}

/// Pushes a failure for each name `expected` and `seen` don't share.
fn diff_sets(
    what: &str,
    expected: &BTreeSet<String>,
    seen: &BTreeSet<String>,
    failures: &mut Vec<String>,
) {
    for extra in seen.difference(expected) {
        failures.push(format!("{what} names {extra}, which is not defined"));
    }
    for missing in expected.difference(seen) {
        failures.push(format!("{what} has no row for {missing}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    /// #2239's allowance covers a table once read, and no other.
    #[test]
    fn a_version_row_is_allowed_for_a_table_once_read_only() {
        let seen = set(&["public.t0", "public.gone", "public.never"]);
        let read = set(&["public.t0"]);
        let ever = set(&["public.t0", "public.gone"]);
        assert_eq!(unexplained(&seen, &read, &ever), vec!["public.never"]);
        assert_eq!(
            unexplained(&seen, &read, &set(&["public.never"])),
            vec!["public.gone"]
        );
    }
}
