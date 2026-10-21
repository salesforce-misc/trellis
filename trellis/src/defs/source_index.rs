//! The source index a `GROUP BY` or `JOIN` target needs (#1167).
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
//! | group-basis aggregate | an index leading with the `GROUP BY` columns, in any order |
//! | `JOIN` | on each side, an index leading with that side's `ON` columns, in `ON` order |
//!
//! An index counts when it is a btree that is valid, ready and live, with no
//! predicate, whose leading key columns are the plain columns (an expression
//! has no column) under each column's own collation and its type's default
//! operator class (a `text_pattern_ops` column orders by other operators than
//! a page's). A primary key or a
//! unique constraint is such an index. Nothing builds an index.
//!
//! Define and a whole-transform `RESUME` refuse a definition whose index is
//! missing ([`CatalogError::SourceIndexMissing`](super::catalog::CatalogError)),
//! unless it was defined `WITHOUT SOURCE INDEX`. Later, `status` reports the
//! index (`SourceIndexGap`) and `self_check` refuses to walk; nothing pauses.
//! A `JOIN`'s `T` part, with group columns from both tables, is served by no
//! one table's index and needs none.

use std::collections::HashMap;
use std::fmt::Write as _;

use tokio_postgres::GenericClient;

use super::ast::ValueType;
use super::ast::{GroupByKey, KeySpace, TransformDef};
use super::ddl::regclass_arg;
use super::join::{self, Side};
use crate::pool::quote_ident;

/// An index a definition needs on one of its source tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceIndex {
    /// The table, qualified (`schema.table`).
    pub table: String,
    /// The columns the index leads with.
    pub columns: Vec<String>,
    /// Whether the columns may come in any order (an aggregate's `GROUP BY`
    /// set), rather than in this order (a `JOIN` side's `ON` columns).
    pub any_order: bool,
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
/// reports it (#1167).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceIndexGap {
    /// The table the index is missing on, qualified.
    pub table: String,
    /// The columns the index should lead with.
    pub columns: Vec<String>,
    /// Whether the columns may come in any order.
    pub any_order: bool,
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
            any_order: gap.any_order,
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
                any_order: index.any_order,
                opted_out: def.without_source_index,
            })
            .collect(),
    )
}

/// The indexes `def` needs: an aggregate on the group basis needs one on its
/// source, a `JOIN` one on each side, and any other target none.
/// `source_columns` are the source's, which decide whether the group basis
/// takes an aggregate. `join_table` is a `JOIN`'s joined table, qualified.
pub(crate) fn required(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    source_table: &str,
    join_table: Option<&str>,
) -> Vec<SourceIndex> {
    match &def.key_space {
        KeySpace::OneToOne => Vec::new(),
        KeySpace::Aggregate { group_by } => {
            if crate::staging::group_basis::route(def, source_columns).is_none() {
                return Vec::new();
            }
            let columns: Vec<String> = group_by
                .iter()
                .filter_map(|key| match key {
                    GroupByKey::Column(column) => Some(column.clone()),
                    GroupByKey::RelationshipPath { .. } => None,
                })
                .collect();
            vec![SourceIndex {
                table: source_table.to_string(),
                columns,
                any_order: true,
            }]
        }
        KeySpace::Join(_) => [(Side::From, Some(source_table)), (Side::Joined, join_table)]
            .into_iter()
            .filter_map(|(side, table)| {
                let columns = join::join_key(def, side);
                if columns.is_empty() {
                    return None;
                }
                Some(SourceIndex {
                    table: table?.to_string(),
                    columns,
                    any_order: false,
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
}

/// Whether `index` meets `want`: columns' `(attnum, collation)`.
fn meets(index: &IndexKey, want: &[(i16, u32)], any_order: bool) -> bool {
    let n = want.len();
    if n == 0 || index.attnums.len() < n || index.collations.len() < n {
        return false;
    }
    let leading = index.attnums.iter().zip(&index.collations).take(n);
    if any_order {
        let mut remaining = want.to_vec();
        for (attnum, collation) in leading {
            match remaining.iter().position(|w| *w == (*attnum, *collation)) {
                Some(at) => {
                    remaining.swap_remove(at);
                }
                None => return false,
            }
        }
        true
    } else {
        leading
            .zip(want)
            .all(|((attnum, collation), w)| (*attnum, *collation) == *w)
    }
}

/// Whether `table` has an index meeting `need`. A table or a column that no
/// longer exists is reported elsewhere (the definition's capture failure or
/// resume refusal) and counts as met: there is no index to create.
async fn is_met(
    client: &impl GenericClient,
    need: &SourceIndex,
) -> Result<bool, tokio_postgres::Error> {
    let regclass = regclass_arg(&need.table);
    let columns = client
        .query(
            "select a.attname, a.attnum, a.attcollation::oid \
             from pg_catalog.pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) and a.attname = any($2) \
               and a.attnum > 0 and not a.attisdropped",
            &[&regclass, &need.columns],
        )
        .await?;
    let mut by_name: HashMap<String, (i16, u32)> = HashMap::new();
    for row in &columns {
        by_name.insert(row.get(0), (row.get(1), row.get(2)));
    }
    let mut want = Vec::with_capacity(need.columns.len());
    for column in &need.columns {
        match by_name.get(column) {
            Some(found) => want.push(*found),
            None => return Ok(true),
        }
    }
    let indexes = client
        .query(
            "select i.indkey::int2[], i.indcollation::oid[], i.indnkeyatts::int4, \
                    array(select oc.opcdefault \
                          from unnest(i.indclass::oid[]) with ordinality as u(opclass, n) \
                          join pg_catalog.pg_opclass oc on oc.oid = u.opclass \
                          order by u.n) \
             from pg_catalog.pg_index i \
             join pg_catalog.pg_class c on c.oid = i.indexrelid \
             join pg_catalog.pg_am am on am.oid = c.relam \
             where i.indrelid = pg_catalog.to_regclass($1) and am.amname = 'btree' \
               and i.indisvalid and i.indisready and i.indislive and i.indpred is null",
            &[&regclass],
        )
        .await?;
    Ok(indexes.iter().any(|row| {
        let mut attnums: Vec<i16> = row.get(0);
        let mut collations: Vec<u32> = row.get(1);
        let keys: i32 = row.get(2);
        let default_opclass: Vec<bool> = row.get(3);
        attnums.truncate(keys.max(0) as usize);
        collations.truncate(keys.max(0) as usize);
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
            },
            &want,
            need.any_order,
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
        if !is_met(client, &need).await? {
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
            " {} has none leading with ({}){}; create one with `{}`;",
            index.table,
            index.columns.join(", "),
            if index.any_order { " in any order" } else { "" },
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
        }
    }

    #[test]
    fn an_aggregate_accepts_its_columns_in_any_order() {
        let want = [(1, 0), (2, 0)];
        assert!(meets(&key(&[1, 2]), &want, true));
        assert!(meets(&key(&[2, 1, 5]), &want, true));
        assert!(!meets(&key(&[1, 5, 2]), &want, true));
        assert!(!meets(&key(&[1]), &want, true));
        assert!(!meets(&key(&[0, 1, 2]), &want, true));
    }

    #[test]
    fn a_join_side_needs_its_columns_in_on_order() {
        let want = [(1, 0), (2, 0)];
        assert!(meets(&key(&[1, 2, 9]), &want, false));
        assert!(!meets(&key(&[2, 1]), &want, false));
    }

    #[test]
    fn a_column_under_another_collation_is_not_served() {
        let index = IndexKey {
            attnums: vec![1],
            collations: vec![950],
        };
        assert!(meets(&index, &[(1, 950)], true));
        assert!(!meets(&index, &[(1, 100)], true));
    }

    #[test]
    fn create_index_quotes_the_table_and_columns() {
        let need = SourceIndex {
            table: "public.Order Lines".to_string(),
            columns: vec!["customer id".to_string(), "kind".to_string()],
            any_order: true,
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
}
