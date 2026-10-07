//! A table in a partition or inheritance hierarchy (issues #376, #622 C9,
//! #707).
//!
//! Capture is statement-level triggers, and a statement trigger fires only
//! for the table a statement names, never for its partitions or children
//! (`docs/staging-and-claiming/01-capture-by-triggers.md`). So Trellis can't
//! read a table in a hierarchy:
//!
//! - **a partitioned table** (`relkind = 'p'`): a write aimed at one of its
//!   partitions directly fires only the partition's triggers;
//! - **a partition** (`relispartition`): a write routed through its parent
//!   fires only the parent's;
//! - **an inheritance child**: a write through its parent that reaches the
//!   child's rows fires only the parent's;
//! - **an inheritance parent**: a statement on it that reaches a child's rows
//!   puts them in its own transition tables, staged as if they were its own,
//!   under a primary key that doesn't span them.
//!
//! [`hierarchy`] is the one check for it, read from `pg_class` and
//! `pg_inherits`:
//!
//! - **Defining** a transform refuses such a source, or a relationship
//!   endpoint it would capture: `catalog::change_keyed` is a plain table
//!   with a primary key that [`hierarchy`] finds nothing for. Every resume
//!   re-runs it.
//! - **The staging worker's capture pass** pauses every definition that
//!   reads a captured table [`hierarchy`] finds something for, recording why
//!   in `capture_failures`, and installs no capture on the table
//!   (`staging::schema_change::pause_readers_in_hierarchy`). The table can
//!   join a hierarchy after define: `ATTACH PARTITION`, `INHERIT`, a child
//!   created `INHERITS` it, or the table dropped and recreated under the
//!   same name as one of these.
//! - **`self_check`'s capture audit** reports it
//!   (`staging::capture_audit::CaptureFault::Hierarchy`).
//!
//! A table another definition of this instance targets is exempt in all
//! three, as it is from every capture check: the target-mutation seam feeds
//! its readers, not a trigger.
//!
//! These are catalog facts that only DDL naming the hierarchy changes.
//! `VACUUM FULL`, `CLUSTER`, `REINDEX` and Trellis's own capture DDL leave
//! them alone, so the pass's check has no false positives from routine
//! maintenance.

use std::fmt;

use tokio_postgres::GenericClient;

use crate::defs::ddl::regclass_arg;

/// One way a table is in a partition or inheritance hierarchy, found by
/// [`hierarchy`]. Every table is an unquoted `schema.table` identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Hierarchy {
    /// The table is partitioned. A write aimed at one of its partitions
    /// directly fires only the partition's statement triggers.
    Partitioned { table: String },
    /// The table is a partition of `parent`. A write through `parent` fires
    /// `parent`'s statement triggers, not the table's.
    Partition { table: String, parent: String },
    /// The table inherits from `parent`. A write through `parent` to the
    /// table's rows fires only `parent`'s.
    InheritanceChild { table: String, parent: String },
    /// `child` inherits from the table. A statement on the table that reaches
    /// `child`'s rows puts them in the table's transition tables, keyed as if
    /// they were the table's own.
    InheritanceParent { table: String, child: String },
}

impl Hierarchy {
    /// The table this is about.
    pub fn table(&self) -> &str {
        match self {
            Hierarchy::Partitioned { table }
            | Hierarchy::Partition { table, .. }
            | Hierarchy::InheritanceChild { table, .. }
            | Hierarchy::InheritanceParent { table, .. } => table,
        }
    }
}

impl fmt::Display for Hierarchy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const UNSUPPORTED: &str = "Trellis doesn't support reading a table in a partition or \
                                   inheritance hierarchy";
        match self {
            Hierarchy::Partitioned { table } => write!(
                f,
                "{table} is a partitioned table, so writes aimed at its partitions directly \
                 aren't captured. {UNSUPPORTED}. Recreate {table} as a plain table"
            ),
            Hierarchy::Partition { table, parent } => write!(
                f,
                "{table} became a partition of {parent}, so writes through {parent} aren't \
                 captured. {UNSUPPORTED}. Detach it (ALTER TABLE {parent} DETACH PARTITION \
                 {table})"
            ),
            Hierarchy::InheritanceChild { table, parent } => write!(
                f,
                "{table} inherits from {parent}, so writes through {parent} aren't captured. \
                 {UNSUPPORTED}. Remove it from the hierarchy (ALTER TABLE {table} NO INHERIT \
                 {parent})"
            ),
            Hierarchy::InheritanceParent { table, child } => write!(
                f,
                "{child} inherits from {table}, so a statement on {table} captures {child}'s \
                 rows as {table}'s. {UNSUPPORTED}. Remove {child} from the hierarchy (ALTER \
                 TABLE {child} NO INHERIT {table})"
            ),
        }
    }
}

/// Every way `table` (an unquoted `schema.table` identity) is in a
/// partition or inheritance hierarchy (see the module doc), in a
/// deterministic order: partitioned first, then each partition-of,
/// inheritance-child-of and inheritance-parent-of, by kind and by the other
/// table's name. A partitioned table's partitions aren't listed: it is
/// reported once, as partitioned. Empty for a plain table outside any
/// hierarchy, and for a table that doesn't exist.
pub async fn hierarchy(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<Hierarchy>, tokio_postgres::Error> {
    let rows = client
        .query(
            "with t as (select pg_catalog.to_regclass($1) as oid) \
             select 0, ''::text from pg_catalog.pg_class c, t \
             where c.oid = t.oid and c.relkind = 'p' \
             union all \
             select case when c.relispartition then 1 else 2 end, \
                    pn.nspname::text || '.' || p.relname::text \
             from pg_catalog.pg_inherits h \
             join pg_catalog.pg_class c on c.oid = h.inhrelid \
             join pg_catalog.pg_class p on p.oid = h.inhparent \
             join pg_catalog.pg_namespace pn on pn.oid = p.relnamespace, t \
             where h.inhrelid = t.oid \
             union all \
             select 3, cn.nspname::text || '.' || c.relname::text \
             from pg_catalog.pg_inherits h \
             join pg_catalog.pg_class c on c.oid = h.inhrelid \
             join pg_catalog.pg_namespace cn on cn.oid = c.relnamespace, t \
             where h.inhparent = t.oid and not c.relispartition \
             order by 1, 2",
            &[&regclass_arg(table)],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let table = table.to_string();
            let other: String = row.get(1);
            match row.get::<_, i32>(0) {
                0 => Hierarchy::Partitioned { table },
                1 => Hierarchy::Partition {
                    table,
                    parent: other,
                },
                2 => Hierarchy::InheritanceChild {
                    table,
                    parent: other,
                },
                _ => Hierarchy::InheritanceParent {
                    table,
                    child: other,
                },
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_message_names_the_table_the_cause_and_the_remedy() {
        let cases = [
            (
                Hierarchy::Partitioned {
                    table: "public.t".to_string(),
                },
                "public.t is a partitioned table",
                "Recreate public.t as a plain table",
            ),
            (
                Hierarchy::Partition {
                    table: "public.t".to_string(),
                    parent: "public.all_t".to_string(),
                },
                "public.t became a partition of public.all_t",
                "ALTER TABLE public.all_t DETACH PARTITION public.t",
            ),
            (
                Hierarchy::InheritanceChild {
                    table: "public.t".to_string(),
                    parent: "public.base".to_string(),
                },
                "public.t inherits from public.base",
                "ALTER TABLE public.t NO INHERIT public.base",
            ),
            (
                Hierarchy::InheritanceParent {
                    table: "public.t".to_string(),
                    child: "public.more".to_string(),
                },
                "public.more inherits from public.t",
                "ALTER TABLE public.more NO INHERIT public.t",
            ),
        ];
        for (hierarchy, starts, remedy) in cases {
            assert_eq!(hierarchy.table(), "public.t");
            let text = hierarchy.to_string();
            assert!(text.starts_with(starts), "{text}");
            assert!(text.contains("doesn't support"), "{text}");
            assert!(text.contains(remedy), "{text}");
        }
    }
}
