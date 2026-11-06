//! The source index a `GROUP BY` or `JOIN` target needs.
//!
//! A `self_check` walk of a group-basis aggregate or a `JOIN` pages its
//! source by the key's own columns, and a page is a range scan only when an
//! index leads with them (`staging::self_check_keyset`). Without one every
//! page scans the source, so a walk reads it once per page. The index
//! affects speed, never correctness.
//!
//! | Target | Required on the source |
//! |---|---|
//! | 1-1 | nothing new (the primary key) |
//! | group-basis aggregate | an index leading with the `GROUP BY` columns, in `GROUP BY` order |
//! | `JOIN` | on each side, an index leading with that side's `ON` columns, in `ON` order |
//!
//! An index counts when it is a btree that is valid, ready and live, with no
//! predicate, whose leading key columns are the plain columns (an expression
//! has no column) under each column's own collation and its type's default
//! operator class (a `text_pattern_ops` column orders by other operators than
//! a page's), all ascending with `NULL` last as a page orders, or all
//! descending with `NULL` first, which a backward scan reads in that order. A
//! leading column in another direction leaves each page to sort every row
//! sharing the columns before it. A primary key or a unique constraint is
//! such an index. Nothing builds an index. `self_check` steers a page by the
//! index ([`has_index`]) under the same rule.
//!
//! Define and a whole-transform `RESUME` refuse a definition whose index is
//! missing ([`CatalogError::SourceIndexMissing`](super::catalog::CatalogError)),
//! unless it was defined `WITHOUT SOURCE INDEX`. Later, `status` reports the
//! index (`SourceIndexGap`) and `self_check` refuses to walk; nothing pauses.
//! A `JOIN`'s `T` part, with group columns from both tables, is served by no
//! one table's index and needs none.
//!
//! A `MIN`/`MAX` field's probe reads the extreme of a group as one endpoint
//! of an index on the `GROUP BY` columns then its argument
//! ([`extreme_indexes`]), which also serves as the `GROUP BY` index. Define
//! and a whole-transform `RESUME` refuse a definition that lacks one
//! ([`CatalogError::ExtremeIndexMissing`](super::catalog::CatalogError)),
//! and `WITHOUT SOURCE INDEX` doesn't waive it. The probe's statement
//! doesn't depend on the index: without one Postgres scans the group (by
//! the `GROUP BY` index when there is one), which is exact and slower, so
//! the probe never reads the catalog. An index dropped later is reported by
//! `status` (`ExtremeIndexGap`); nothing pauses and `self_check` still
//! walks.

use std::collections::HashMap;
use std::fmt::Write as _;

use tokio_postgres::GenericClient;

use super::ast::ValueType;
use super::ast::{KeySpace, TransformDef};
use super::ddl::regclass_arg;
use super::join::{self, Side};
use crate::pool::quote_ident;

/// The part of a definition that needs a source index. In a self-join both
/// sides sit on the same table with the same columns, and the role is what
/// tells their two requirements apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IndexRole {
    /// A group-basis aggregate's `GROUP BY` columns.
    GroupBy,
    /// A `JOIN`'s `ON` columns on its from side.
    JoinFrom,
    /// A `JOIN`'s `ON` columns on its joined side.
    JoinJoined,
}

impl IndexRole {
    /// Every role, for a client to intern its words.
    pub const ALL: [IndexRole; 3] = [
        IndexRole::GroupBy,
        IndexRole::JoinFrom,
        IndexRole::JoinJoined,
    ];

    /// The role as one word, the form every client carries it in.
    pub fn as_str(self) -> &'static str {
        match self {
            IndexRole::GroupBy => "group_by",
            IndexRole::JoinFrom => "join_from",
            IndexRole::JoinJoined => "join_joined",
        }
    }

    /// The role in words, for a message.
    pub fn describe(self) -> &'static str {
        match self {
            IndexRole::GroupBy => "the GROUP BY columns",
            IndexRole::JoinFrom => "the JOIN's ON columns (from side)",
            IndexRole::JoinJoined => "the JOIN's ON columns (joined side)",
        }
    }
}

/// An index a definition needs on one of its source tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceIndex {
    /// The table, qualified (`schema.table`).
    pub table: String,
    /// The columns the index leads with, in order.
    pub columns: Vec<String>,
    /// The part of the definition that needs it.
    pub role: IndexRole,
}

impl SourceIndex {
    /// A statement creating an index that meets the requirement, for the
    /// operator to run (Trellis never does).
    pub fn create_index(&self) -> String {
        let (schema, table) = self
            .table
            .split_once('.')
            .map_or((None, self.table.as_str()), |(s, t)| (Some(s), t));
        let mut sql = String::from("create index on ");
        if let Some(schema) = schema {
            let _ = write!(sql, "{}.", quote_ident(schema));
        }
        let _ = write!(sql, "{} (", quote_ident(table));
        let columns: Vec<String> = self.columns.iter().map(|c| quote_ident(c)).collect();
        sql.push_str(&columns.join(", "));
        sql.push(')');
        sql
    }
}

/// A source index a definition lacks now, as `DefinitionStatus::source_index_gaps`
/// reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SourceIndexGap {
    /// The table the index is missing on, qualified.
    pub table: String,
    /// The columns the index should lead with.
    pub columns: Vec<String>,
    /// The part of the definition that needs the index.
    pub role: IndexRole,
    /// Whether the definition was defined `WITHOUT SOURCE INDEX`: it runs
    /// without the index and its `self_check` walks at the per-page scan
    /// cost, rather than refusing.
    pub opted_out: bool,
}

impl SourceIndexGap {
    /// What to do about it: a statement creating a qualifying index, which
    /// Trellis never runs.
    pub fn fix(&self) -> String {
        SourceIndex::from(self).create_index()
    }
}

impl From<&SourceIndexGap> for SourceIndex {
    fn from(gap: &SourceIndexGap) -> Self {
        SourceIndex {
            table: gap.table.clone(),
            columns: gap.columns.clone(),
            role: gap.role,
        }
    }
}

/// The gaps in `def`'s source indexes now, for `status` (read live).
pub(crate) async fn gaps(
    client: &impl GenericClient,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    source_table: &str,
    join_table: Option<&str>,
) -> Result<Vec<SourceIndexGap>, tokio_postgres::Error> {
    Ok(
        unmet_for(client, def, source_columns, source_table, join_table)
            .await?
            .into_iter()
            .map(|index| SourceIndexGap {
                table: index.table,
                columns: index.columns,
                role: index.role,
                opted_out: def.without_source_index,
            })
            .collect(),
    )
}

/// A source index a `MIN`/`MAX` field's probe reads one endpoint of, which
/// the source lacks now, as `DefinitionStatus::extreme_index_gaps` reports
/// it. Without it each probe scans the group for its extreme (exact, at the
/// cost of reading every row of each probed group). Nothing pauses and
/// `self_check` still walks: its pages lead with the `GROUP BY` columns
/// alone ([`SourceIndexGap`] reports that index).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExtremeIndexGap {
    /// The table the index is missing on, qualified.
    pub table: String,
    /// The columns the index should lead with: the `GROUP BY` columns, in
    /// order, then [`Self::argument`].
    pub columns: Vec<String>,
    /// The source column the `MIN`/`MAX` reads.
    pub argument: String,
    /// Whether the definition was defined `WITHOUT SOURCE INDEX`.
    pub opted_out: bool,
}

impl ExtremeIndexGap {
    /// What to do about it: a statement creating a qualifying index, which
    /// Trellis never runs.
    pub fn fix(&self) -> String {
        SourceIndex {
            table: self.table.clone(),
            columns: self.columns.clone(),
            role: IndexRole::GroupBy,
        }
        .create_index()
    }
}

/// The indexes `def`'s `MIN`/`MAX` probes read one endpoint of, on `table`:
/// per argument, one leading with the `GROUP BY` columns then the argument,
/// under [`has_index`]'s rule (the probe reads it forward for `MIN`,
/// backward for `MAX`). Each also leads with the `GROUP BY` columns, so it
/// serves `self_check`'s pages too. Empty for every definition with no
/// `MIN` or `MAX`, which then costs no catalog read.
pub(crate) fn extreme_indexes(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    table: &str,
) -> Vec<SourceIndex> {
    let Some(shape) = crate::staging::group_basis::route(def, source_columns) else {
        return Vec::new();
    };
    shape
        .extreme_arguments()
        .into_iter()
        .map(|argument| {
            let mut columns = shape.group_columns().to_vec();
            columns.push(argument);
            SourceIndex {
                table: table.to_string(),
                columns,
                role: IndexRole::GroupBy,
            }
        })
        .collect()
}

/// The message of a refusal of `target`'s `MIN`/`MAX` for its probe
/// `indexes`: missing ones, or, when it was declared `WITHOUT SOURCE INDEX`
/// (`opted_out`), every one, since the opt-out doesn't apply to `MIN`/`MAX`.
pub fn extreme_refusal_text(target: &str, indexes: &[SourceIndex], opted_out: bool) -> String {
    const WHY: &str = "when a group's extreme is deleted, the next one is re-read through a \
                       source index on the GROUP BY columns then the argument";
    let mut text = if opted_out {
        format!(
            "{target} is defined WITHOUT SOURCE INDEX, which doesn't apply to MIN/MAX: {WHY}, \
             and without one each re-read scans the group. Define it without WITHOUT SOURCE \
             INDEX, with"
        )
    } else {
        format!("{target} needs a source index for its MIN/MAX that it doesn't have: {WHY}.")
    };
    for (i, index) in indexes.iter().enumerate() {
        let argument = index.columns.last().map_or("", String::as_str);
        let columns = index.columns.join(", ");
        let create = index.create_index();
        let table = &index.table;
        let _ = if opted_out {
            write!(
                text,
                "{} an index on {table} leading with ({columns}) in that order, for \
                 MIN/MAX({argument}): create one with `{create}`",
                if i == 0 { "" } else { ";" }
            )
        } else {
            write!(
                text,
                " {table} has none leading with ({columns}) in that order, for \
                 MIN/MAX({argument}); create one with `{create}`."
            )
        };
    }
    if opted_out {
        text.push('.');
    }
    text.push_str(" The same index serves as the GROUP BY columns' index.");
    if !opted_out {
        text.push_str(" WITHOUT SOURCE INDEX doesn't apply to MIN/MAX.");
    }
    text
}

/// The gaps in `def`'s `MIN`/`MAX` probe indexes now, for `status` (read
/// live, like [`gaps`]). The probe itself never reads the catalog: its
/// statement is the same with or without the index, and Postgres plans it
/// as an endpoint read where the index exists and a scan of the group
/// where it doesn't.
pub(crate) async fn extreme_gaps(
    client: &impl GenericClient,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    source_table: &str,
) -> Result<Vec<ExtremeIndexGap>, tokio_postgres::Error> {
    let mut out = Vec::new();
    for index in extreme_indexes(def, source_columns, source_table) {
        if !is_met(client, &index.table, &index.columns).await? {
            out.push(ExtremeIndexGap {
                argument: index.columns.last().cloned().unwrap_or_default(),
                table: index.table,
                columns: index.columns,
                opted_out: def.without_source_index,
            });
        }
    }
    Ok(out)
}

/// The indexes `def` needs: an aggregate on the group basis needs one on its
/// source, an aggregate `JOIN` one on each side, and any other target none.
/// `source_columns` are the source's, which decide whether the group basis
/// takes an aggregate. `join_table` is a `JOIN`'s joined table, qualified.
pub(crate) fn required(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    source_table: &str,
    join_table: Option<&str>,
) -> Vec<SourceIndex> {
    match &def.key_space {
        // A 1-1 JOIN walks `a` by its primary key, and the uniqueness define
        // requires of `b`'s `ON` columns is an index on them.
        KeySpace::OneToOne | KeySpace::JoinOneToOne(_) => Vec::new(),
        KeySpace::Aggregate { group_by } => {
            if crate::staging::group_basis::route(def, source_columns).is_none() {
                return Vec::new();
            }
            let columns: Vec<String> = group_by.to_vec();
            vec![SourceIndex {
                table: source_table.to_string(),
                columns,
                role: IndexRole::GroupBy,
            }]
        }
        KeySpace::Join(_) => [
            (Side::From, IndexRole::JoinFrom, Some(source_table)),
            (Side::Joined, IndexRole::JoinJoined, join_table),
        ]
        .into_iter()
        .filter_map(|(side, role, table)| {
            let columns = join::join_key(def, side);
            if columns.is_empty() {
                return None;
            }
            Some(SourceIndex {
                table: table?.to_string(),
                columns,
                role,
            })
        })
        .collect(),
    }
}

/// One index of a table, as `pg_index` records it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexKey {
    /// The key columns' attribute numbers (`indkey`); 0 is an expression.
    attnums: Vec<i16>,
    /// Each key column's collation (`indcollation`).
    collations: Vec<u32>,
    /// Each key column's ordering flags (`indoption`): `INDOPTION_DESC` and
    /// `INDOPTION_NULLS_FIRST`.
    options: Vec<i16>,
}

/// `indoption`'s `DESC` bit.
const INDOPTION_DESC: i16 = 0x0001;
/// `indoption`'s `NULLS FIRST` bit.
const INDOPTION_NULLS_FIRST: i16 = 0x0002;

/// Whether `index` meets `want`: columns' `(attnum, collation)`, leading the
/// index in this order, all ascending with `NULL` last or all descending
/// with `NULL` first (read backward, the same order).
fn meets(index: &IndexKey, want: &[(i16, u32)]) -> bool {
    let n = want.len();
    if n == 0 || index.attnums.len() < n || index.collations.len() < n || index.options.len() < n {
        return false;
    }
    let leading = index.attnums.iter().zip(&index.collations).take(n);
    let ordered = leading
        .zip(want)
        .all(|((attnum, collation), w)| (*attnum, *collation) == *w);
    let forward = 0;
    let backward = INDOPTION_DESC | INDOPTION_NULLS_FIRST;
    let direction = index.options[0];
    ordered
        && (direction == forward || direction == backward)
        && index.options[..n].iter().all(|o| *o == direction)
}

/// Whether `table` (qualified, `schema.table`) has an index leading with
/// `columns` that [`meets`] them: the predicate define requires, which
/// `self_check` also steers a page by.
pub(crate) async fn has_index(
    client: &impl GenericClient,
    table: &str,
    columns: &[String],
) -> Result<bool, tokio_postgres::Error> {
    is_met(client, table, columns).await
}

/// Whether `table` has an index leading with `columns`. A table or a column that no
/// longer exists is reported elsewhere (the definition's capture failure or
/// resume refusal) and counts as met: there is no index to create.
///
/// One statement reads the wanted columns' attribute numbers and collations
/// together with the table's indexes, so one catalog snapshot: a caller under
/// `READ COMMITTED` (a pooled client, or a transaction) gets a fresh snapshot
/// per statement, and a collation-changing `ALTER COLUMN TYPE` that committed
/// between two reads would pair the old collation with the rebuilt index and
/// report a valid index missing. The statement always returns one row: the
/// columns aggregate to `NULL` when none is found, and the indexes' fields
/// are `NULL` when the table has no qualifying index.
async fn is_met(
    client: &impl GenericClient,
    table: &str,
    columns: &[String],
) -> Result<bool, tokio_postgres::Error> {
    let regclass = regclass_arg(table);
    let rows = client
        .query(
            "with cols as ( \
                 select array_agg(a.attname::text) as names, \
                        array_agg(a.attnum) as attnums, \
                        array_agg(a.attcollation::oid) as collations \
                 from pg_catalog.pg_attribute a \
                 where a.attrelid = pg_catalog.to_regclass($1) and a.attname = any($2) \
                   and a.attnum > 0 and not a.attisdropped) \
             select cols.names, cols.attnums, cols.collations, \
                    idx.indkey, idx.indcollation, idx.indnkeyatts, idx.indoption, idx.defaults \
             from cols \
             left join lateral ( \
                 select i.indkey::int2[] as indkey, i.indcollation::oid[] as indcollation, \
                        i.indnkeyatts::int4 as indnkeyatts, i.indoption::int2[] as indoption, \
                        array(select oc.opcdefault \
                              from unnest(i.indclass::oid[]) with ordinality as u(opclass, n) \
                              join pg_catalog.pg_opclass oc on oc.oid = u.opclass \
                              order by u.n) as defaults \
                 from pg_catalog.pg_index i \
                 join pg_catalog.pg_class c on c.oid = i.indexrelid \
                 join pg_catalog.pg_am am on am.oid = c.relam \
                 where i.indrelid = pg_catalog.to_regclass($1) and am.amname = 'btree' \
                   and i.indisvalid and i.indisready and i.indislive and i.indpred is null \
             ) idx on true",
            &[&regclass, &columns],
        )
        .await?;
    let Some(first) = rows.first() else {
        return Ok(true);
    };
    let names: Vec<String> = first.get::<_, Option<Vec<String>>>(0).unwrap_or_default();
    let attnums: Vec<i16> = first.get::<_, Option<Vec<i16>>>(1).unwrap_or_default();
    let attcollations: Vec<u32> = first.get::<_, Option<Vec<u32>>>(2).unwrap_or_default();
    let by_name: HashMap<&str, (i16, u32)> = names
        .iter()
        .map(String::as_str)
        .zip(attnums.into_iter().zip(attcollations))
        .collect();
    let mut want = Vec::with_capacity(columns.len());
    for column in columns {
        match by_name.get(column.as_str()) {
            Some(found) => want.push(*found),
            None => return Ok(true),
        }
    }
    let indexes: Vec<&tokio_postgres::Row> = rows
        .iter()
        .filter(|row| row.get::<_, Option<Vec<i16>>>(3).is_some())
        .collect();
    Ok(indexes.iter().any(|row| {
        let mut attnums: Vec<i16> = row.get(3);
        let mut collations: Vec<u32> = row.get(4);
        let keys: i32 = row.get(5);
        let mut options: Vec<i16> = row.get(6);
        let default_opclass: Vec<bool> = row.get(7);
        attnums.truncate(keys.max(0) as usize);
        collations.truncate(keys.max(0) as usize);
        options.truncate(keys.max(0) as usize);
        // A column under a non-default operator class (`text_pattern_ops`)
        // orders by other operators than a page's `>` and `order by`, so the
        // index can't serve the page: it counts as no column.
        for (attnum, default) in attnums.iter_mut().zip(&default_opclass) {
            if !default {
                *attnum = 0;
            }
        }
        meets(
            &IndexKey {
                attnums,
                collations,
                options,
            },
            &want,
        )
    }))
}

/// The indexes of `needs` that don't exist (or are not valid) now: a catalog
/// lookup, read live on each call.
pub(crate) async fn unmet(
    client: &impl GenericClient,
    needs: Vec<SourceIndex>,
) -> Result<Vec<SourceIndex>, tokio_postgres::Error> {
    let mut out = Vec::new();
    for need in needs {
        if !is_met(client, &need.table, &need.columns).await? {
            out.push(need);
        }
    }
    Ok(out)
}

/// What a definition lacks of [`required`] now, whether or not it opted out:
/// empty when every index exists.
pub(crate) async fn unmet_for(
    client: &impl GenericClient,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    source_table: &str,
    join_table: Option<&str>,
) -> Result<Vec<SourceIndex>, tokio_postgres::Error> {
    unmet(
        client,
        required(def, source_columns, source_table, join_table),
    )
    .await
}

/// The message of a refusal for the missing `indexes` of `target`, shared by
/// define, a resume and `self_check`.
pub fn refusal_text(target: &str, indexes: &[SourceIndex]) -> String {
    let mut text = format!("{target} needs a source index it doesn't have:");
    for index in indexes {
        let _ = write!(
            text,
            " {} has none leading with ({}) in that order, for {}; create one with `{}`;",
            index.table,
            index.columns.join(", "),
            index.role.describe(),
            index.create_index()
        );
    }
    text.push_str(
        " or define it WITHOUT SOURCE INDEX (after the GROUP BY or JOIN clause), which \
         accepts a self_check walk that scans the source once per page",
    );
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(attnums: &[i16]) -> IndexKey {
        IndexKey {
            attnums: attnums.to_vec(),
            collations: vec![0; attnums.len()],
            options: vec![0; attnums.len()],
        }
    }

    fn ordered(attnums: &[i16], options: &[i16]) -> IndexKey {
        IndexKey {
            options: options.to_vec(),
            ..key(attnums)
        }
    }

    #[test]
    fn an_aggregate_needs_its_columns_in_group_by_order() {
        let want = [(1, 0), (2, 0)];
        assert!(meets(&key(&[1, 2]), &want));
        assert!(meets(&key(&[1, 2, 5]), &want));
        assert!(!meets(&key(&[2, 1, 5]), &want));
        assert!(!meets(&key(&[1, 5, 2]), &want));
        assert!(!meets(&key(&[1]), &want));
        assert!(!meets(&key(&[0, 1, 2]), &want));
    }

    #[test]
    fn a_join_side_needs_its_columns_in_on_order() {
        let want = [(1, 0), (2, 0)];
        assert!(meets(&key(&[1, 2, 9]), &want));
        assert!(!meets(&key(&[2, 1]), &want));
    }

    #[test]
    fn a_column_under_another_collation_is_not_served() {
        let index = IndexKey {
            attnums: vec![1],
            collations: vec![950],
            options: vec![0],
        };
        assert!(meets(&index, &[(1, 950)]));
        assert!(!meets(&index, &[(1, 100)]));
    }

    /// A page orders each column ascending, `NULL` last. An index read
    /// forward (all ascending, `NULL` last) or backward (all descending,
    /// `NULL` first) gives that order; a mixed one, or `NULL` on the other
    /// end, leaves the page to sort. Trailing columns past the key are free.
    #[test]
    fn the_leading_columns_run_in_the_page_order_forward_or_backward() {
        let want = [(1, 0), (2, 0)];
        let desc = INDOPTION_DESC;
        let nulls_first = INDOPTION_NULLS_FIRST;
        let backward = desc | nulls_first;
        assert!(meets(&ordered(&[1, 2], &[0, 0]), &want));
        assert!(meets(&ordered(&[1, 2], &[backward, backward]), &want));
        assert!(meets(&ordered(&[1, 2, 3], &[0, 0, desc]), &want));
        assert!(!meets(&ordered(&[1, 2], &[0, desc]), &want));
        assert!(!meets(&ordered(&[1, 2], &[0, nulls_first]), &want));
        assert!(!meets(&ordered(&[1, 2], &[desc, desc]), &want));
        assert!(!meets(&ordered(&[1, 2], &[backward, 0]), &want));
        assert!(!meets(&ordered(&[1], &[nulls_first]), &[(1, 0)]));
    }

    fn needs(text: &str, columns: &[(&str, ValueType)], join: Option<&str>) -> Vec<SourceIndex> {
        use crate::defs::parser::parse;
        let columns = columns
            .iter()
            .map(|(name, ty)| (name.to_string(), *ty))
            .collect();
        required(&parse(text).expect("parses"), &columns, "public.t", join)
    }

    #[test]
    fn an_aggregate_needs_its_index_in_the_group_by_role() {
        let found = needs(
            "TRANSFORM x FROM t GROUP BY g SELECT SUM(v) AS s",
            &[("g", ValueType::Text), ("v", ValueType::Numeric)],
            None,
        );
        assert_eq!(
            found,
            [SourceIndex {
                table: "public.t".to_string(),
                columns: vec!["g".to_string()],
                role: IndexRole::GroupBy,
            }]
        );
    }

    #[test]
    fn each_join_side_needs_its_index_in_its_own_role() {
        let found = needs(
            "TRANSFORM x FROM t JOIN u ON t.a = u.b GROUP BY t.g SELECT COUNT(*) AS n",
            &[],
            Some("public.u"),
        );
        let roles: Vec<_> = found
            .iter()
            .map(|i| (i.table.as_str(), i.columns.clone(), i.role))
            .collect();
        assert_eq!(
            roles,
            [
                ("public.t", vec!["a".to_string()], IndexRole::JoinFrom),
                ("public.u", vec!["b".to_string()], IndexRole::JoinJoined),
            ]
        );
    }

    #[test]
    fn two_sides_on_one_table_are_two_indexes_told_apart_by_role() {
        // The grammar refuses a self-join (two tables sharing a bare name),
        // so no definition reaches this today; `required` keeps both sides
        // anyway, so a grammar that allowed one would report both gaps.
        let found = needs(
            "TRANSFORM x FROM t JOIN u ON t.a = u.b GROUP BY t.g SELECT COUNT(*) AS n",
            &[],
            Some("public.t"),
        );
        assert_eq!(found.len(), 2, "{found:?}");
        assert!(found.iter().all(|i| i.table == "public.t"));
        assert_eq!(found[0].role, IndexRole::JoinFrom);
        assert_eq!(found[1].role, IndexRole::JoinJoined);
        assert_ne!(found[0], found[1]);
    }

    #[test]
    fn the_refusal_names_the_role() {
        let text = refusal_text(
            "public.x",
            &[SourceIndex {
                table: "public.regions".to_string(),
                columns: vec!["id".to_string()],
                role: IndexRole::JoinJoined,
            }],
        );
        assert!(
            text.contains("for the JOIN's ON columns (joined side)"),
            "{text}"
        );
    }

    #[test]
    fn create_index_quotes_the_table_and_columns() {
        let need = SourceIndex {
            table: "public.Order Lines".to_string(),
            columns: vec!["customer id".to_string(), "kind".to_string()],
            role: IndexRole::GroupBy,
        };
        assert_eq!(
            need.create_index(),
            r#"create index on "public"."Order Lines" ("customer id", "kind")"#
        );
    }

    #[test]
    fn the_opt_out_parses_renders_and_is_refused_for_a_one_to_one_target() {
        use crate::defs::ast::render_definition_text;
        use crate::defs::parser::parse;
        let text = "TRANSFORM t FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS x";
        let def = parse(text).expect("parses");
        assert!(def.without_source_index);
        assert_eq!(
            parse(&render_definition_text(&def)).expect("round trip"),
            def
        );
        assert!(
            !parse("TRANSFORM t FROM s GROUP BY g SELECT SUM(v) AS x")
                .expect("parses")
                .without_source_index
        );
        assert!(parse("TRANSFORM t FROM s WITHOUT SOURCE INDEX SELECT v AS x").is_err());
    }

    /// A 1-1 JOIN walks `a` by its primary key and requires `b`'s `ON`
    /// columns to be unique (so indexed) at define: no source index.
    #[test]
    fn a_one_to_one_join_requires_no_source_index() {
        let def = crate::defs::parse("TRANSFORM v FROM a LEFT JOIN b ON a.k = b.k SELECT b.x AS x")
            .unwrap();
        assert!(required(&def, &HashMap::new(), "public.a", Some("public.b")).is_empty());
    }
}

/// [`is_met`] reads the wanted columns and the table's indexes in one
/// statement, so one catalog snapshot. Two statements under READ COMMITTED
/// are two snapshots: a collation-changing `ALTER COLUMN TYPE` committing
/// between them would leave the first read's old collation against the
/// second read's rebuilt index, and a valid index would read as missing.
#[cfg(test)]
mod one_snapshot_tests {
    use super::*;
    use tokio_postgres::{Client, NoTls};

    /// Held by the checking session until its hook runs; the other session
    /// takes it, so it learns the check is mid-read.
    const READY: i64 = 7_311_001;
    /// Held by the other session until its `ALTER` has committed; the hook
    /// waits on it.
    const GO: i64 = 7_311_002;

    async fn connect(dsn: &str) -> Client {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    /// The pause point, without a line of production code: an `=` between two
    /// `name`s in schema `hook`, which a session whose
    /// `search_path` puts `hook` ahead of `pg_catalog` resolves instead of
    /// the built-in. The column read filters `attname = any($2)` through it,
    /// so the first time it runs the check is inside that read, after its
    /// snapshot: it releases [`READY`] (the other session is waiting for
    /// that) and waits for [`GO`].
    const HOOK: &str = "create schema hook; \
        create function hook.name_eq_name(name, name) returns boolean \
        language plpgsql as $$ \
        begin \
          if pg_catalog.pg_advisory_unlock(7311001) then \
            perform pg_catalog.pg_advisory_lock(7311002); \
            perform pg_catalog.pg_advisory_unlock(7311002); \
          end if; \
          return $1 operator(pg_catalog.=) $2; \
        end $$; \
        create operator hook.= (leftarg = name, rightarg = name, \
                                function = hook.name_eq_name);";

    #[tokio::test]
    async fn a_collation_change_committing_mid_check_leaves_a_valid_index_met() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let other = connect(db.dsn()).await;
        other
            .batch_execute(
                "create table public.t (g text not null, v integer not null); \
                 create index t_g_v on public.t (g, v);",
            )
            .await
            .expect("table and index");
        other.batch_execute(HOOK).await.expect("hook");
        let checker = connect(db.dsn()).await;
        checker
            .batch_execute(&format!(
                "set search_path to hook, pg_catalog; set lock_timeout to '10s'; \
                 select pg_advisory_lock({READY})"
            ))
            .await
            .expect("checker takes READY");
        other
            .batch_execute(&format!(
                "set lock_timeout to '10s'; select pg_advisory_lock({GO})"
            ))
            .await
            .expect("other takes GO");
        let columns = ["g".to_string(), "v".to_string()];

        let check = is_met(&checker, "public.t", &columns);
        let alter = async {
            // Waits until the check's hook has released READY: the check is
            // inside its column read.
            other
                .batch_execute(&format!(
                    "select pg_advisory_lock({READY}); \
                     alter table public.t alter column g type text collate \"C\"; \
                     select pg_advisory_unlock({GO})"
                ))
                .await
                .expect("the check reaches its pause point, then the ALTER commits");
        };
        let (met, ()) = tokio::join!(check, alter);

        assert!(
            met.expect("check"),
            "the index was valid before and after the ALTER, so it is met"
        );
        // The collation did change: a later check reads the rebuilt index.
        assert!(is_met(&checker, "public.t", &columns).await.expect("again"));
    }
}

/// [`is_met`]'s decoding of its one row per index, or one row of `NULL`s
/// when the table or the columns are not there or the table has no
/// qualifying index.
#[cfg(test)]
mod catalog_tests {
    use super::*;
    use tokio_postgres::NoTls;

    fn columns(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[tokio::test]
    async fn an_absent_table_or_column_is_met_and_a_table_without_a_qualifying_index_is_not() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "create table public.bare (g text, v integer); \
                 create table public.t (g text, v integer, w integer); \
                 create index on public.t (v, g); \
                 create index on public.t (g, v) where v > 0; \
                 create index on public.t using hash (g); \
                 create index on public.t (g, v);",
            )
            .await
            .expect("tables");
        let gv = columns(&["g", "v"]);
        assert!(
            is_met(&client, "public.absent", &gv)
                .await
                .expect("absent table")
        );
        assert!(
            is_met(&client, "public.t", &columns(&["g", "gone"]))
                .await
                .expect("absent column")
        );
        assert!(
            is_met(&client, "public.bare", &columns(&["gone"]))
                .await
                .expect("absent column, no index")
        );
        assert!(!is_met(&client, "public.bare", &gv).await.expect("no index"));
        assert!(
            is_met(&client, "public.t", &gv)
                .await
                .expect("one qualifying index of four")
        );
        assert!(
            !is_met(&client, "public.t", &columns(&["w"]))
                .await
                .expect("none leads with w")
        );
    }
}
