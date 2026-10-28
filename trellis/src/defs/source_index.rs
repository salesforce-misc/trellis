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
async fn is_met(
    client: &impl GenericClient,
    table: &str,
    columns: &[String],
) -> Result<bool, tokio_postgres::Error> {
    let regclass = regclass_arg(table);
    let found = client
        .query(
            "select a.attname, a.attnum, a.attcollation::oid \
             from pg_catalog.pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) and a.attname = any($2) \
               and a.attnum > 0 and not a.attisdropped",
            &[&regclass, &columns],
        )
        .await?;
    let mut by_name: HashMap<String, (i16, u32)> = HashMap::new();
    for row in &found {
        by_name.insert(row.get(0), (row.get(1), row.get(2)));
    }
    let mut want = Vec::with_capacity(columns.len());
    for column in columns {
        match by_name.get(column) {
            Some(found) => want.push(*found),
            None => return Ok(true),
        }
    }
    let indexes = client
        .query(
            "select i.indkey::int2[], i.indcollation::oid[], i.indnkeyatts::int4, \
                    i.indoption::int2[], \
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
        let mut options: Vec<i16> = row.get(3);
        let default_opclass: Vec<bool> = row.get(4);
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
