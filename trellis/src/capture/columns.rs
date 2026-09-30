//! Which columns a table's capture triggers image (#622 C2).
//!
//! A trigger images the primary key plus every column some consumer of the
//! table's ring rows reads. Imaging less poisons rows with
//! `EvalError::MissingColumn`, or worse, silently reads a missing value as
//! `NULL`. Imaging more costs the writer (#565 E3: every column of a table
//! with a 100 KB column cost 40x a narrow image). The set, for table `T`:
//!
//! 1. **`T`'s primary key.**
//! 2. **Columns `T`'s own definitions read.** For every definition whose
//!    source is `T`, in any status:
//!    - every source column its fields reference
//!      ([`crate::defs::oracle::referenced_source_columns`]: a self-named
//!      passthrough reads its own column, a reference to another calculated
//!      field doesn't);
//!    - its plain `GROUP BY` columns.
//!
//!    Aggregate apply evaluates every field over both images, and a missing
//!    `GROUP BY` value would silently put the row in the `NULL` group.
//!    `WHERE` accepts only `TRUE` today, so no filter reads a column.
//! 3. **`from_col` of every relationship declared on `T`**, whether or not a
//!    definition uses it. A reader's relationship path looks its related row
//!    up by it. The ring's `group_key` (#133) is built from it. Apply's
//!    reverse guard also scans ring images for it
//!    (`staging::apply::from_side_change_in_flight`), and a missing value
//!    there silently passes the guard.
//! 4. **For every relationship whose to-side is `T`:**
//!    - its `to_col`;
//!    - every to-side column a definition reads through it
//!      ([`crate::defs::eval::relationship_references`], fields and `GROUP
//!      BY`);
//!    - every data column of its settled parent projection. Apply upserts
//!      the projection from the to-side's new image
//!      (`jsonb_populate_record`), so a column missing from the image would
//!      be written as `NULL`. A projection only widens, so it can carry a
//!      column no current definition reads. A definition registered later
//!      would then read that stale `NULL`.
//!
//! A chained definition's source is another definition's target. That table
//! isn't captured by triggers in C (the target-mutation seam feeds it), so
//! nothing here needs it.

use std::collections::{BTreeSet, HashMap};

use tokio_postgres::GenericClient;

use crate::defs::ast::{GroupByKey, KeySpace, TransformDef};
use crate::defs::model::{RelationshipCardinality, RelationshipDefinition};

use super::CaptureError;
use super::sql::CaptureSpec;

/// Everything in the catalog the column set depends on.
#[derive(Debug, Clone, Default)]
pub struct CaptureCatalog {
    /// Every registered definition, in any status.
    pub definitions: Vec<CaptureReader>,
    /// Every relationship.
    pub relationships: Vec<RelationshipDefinition>,
    /// Each existing settled parent projection's data columns (bookkeeping
    /// columns excluded), keyed by relationship id.
    pub projection_columns: HashMap<i64, Vec<String>>,
}

/// One registered definition, as capture sees it.
#[derive(Debug, Clone)]
pub struct CaptureReader {
    pub id: i64,
    /// The qualified `source_table`.
    pub source: String,
    pub def: TransformDef,
    /// A `schema_changed` marker paused it (#622 C6; `capture_failures`).
    /// Until it resumes, which rebuilds it, it reads nothing, so its columns
    /// don't count and the table's capture can drop a column only it read.
    pub capture_failed: bool,
}

/// The columns [`read_columns`] finds a table's readers need, before the
/// primary key is added.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadColumns {
    /// Every column an image must carry.
    pub columns: BTreeSet<String>,
    /// The subset whose values make up the ring's `group_key`: every
    /// outbound relationship's `from_col`.
    pub group_key: BTreeSet<String>,
}

/// The columns [`CaptureCatalog`]'s readers need from `table`'s images
/// (this module's rules 2–4). `table` is the unquoted `schema.table`
/// identity. A [`CaptureReader::capture_failed`] definition doesn't count.
pub fn read_columns(catalog: &CaptureCatalog, table: &str) -> ReadColumns {
    let mut read = ReadColumns::default();

    for reader in catalog.definitions.iter().filter(|r| !r.capture_failed) {
        if reader.source == table {
            read.columns.extend(own_columns(&reader.def));
        }
    }

    for rel in &catalog.relationships {
        if rel.qualified_from_table() == table {
            read.columns.insert(rel.def.from_col.clone());
            read.group_key.insert(rel.def.from_col.clone());
        }
        if rel.qualified_to_table() == table {
            read.columns.insert(rel.def.to_col.clone());
            let from = rel.qualified_from_table();
            for reader in catalog.definitions.iter().filter(|r| !r.capture_failed) {
                if reader.source == from {
                    read.columns.extend(columns_through(&reader.def, rel));
                }
            }
            if rel.cardinality == RelationshipCardinality::ToOne
                && let Some(columns) = catalog.projection_columns.get(&rel.id)
            {
                read.columns.extend(columns.iter().cloned());
            }
        }
    }
    read
}

/// Every definition that reads one of `missing` from `table`'s images, or,
/// with `key_missing`, every definition that reads `table` at all (#622 C6:
/// the definitions a `schema_changed` marker pauses). In id order.
///
/// A definition reads a column of its own source through its fields, its
/// `GROUP BY` and the `from_col` of each relationship it reads through; and
/// a column of a relationship's to-side through that relationship: its
/// `to_col` and the columns its paths name. The rest of [`read_columns`]'s
/// set, the `from_col`/`to_col` of a relationship nothing reads through and
/// the projection columns no definition reads, belongs to no definition.
pub fn readers_of(
    catalog: &CaptureCatalog,
    table: &str,
    missing: &BTreeSet<String>,
    key_missing: bool,
) -> Vec<i64> {
    let mut ids: Vec<i64> = catalog
        .definitions
        .iter()
        .filter(|reader| {
            let mut reads_table = false;
            let mut reads: BTreeSet<String> = BTreeSet::new();
            if reader.source == table {
                reads_table = true;
                reads.extend(own_columns(&reader.def));
            }
            for rel in &catalog.relationships {
                if rel.qualified_from_table() != reader.source || !reads_through(&reader.def, rel) {
                    continue;
                }
                if rel.qualified_from_table() == table {
                    reads.insert(rel.def.from_col.clone());
                }
                if rel.qualified_to_table() == table {
                    reads_table = true;
                    reads.insert(rel.def.to_col.clone());
                    reads.extend(columns_through(&reader.def, rel));
                }
            }
            (key_missing && reads_table) || !reads.is_disjoint(missing)
        })
        .map(|reader| reader.id)
        .collect();
    ids.sort_unstable();
    ids
}

/// The columns of its own source `def` reads directly: every source column
/// its fields reference ([`crate::defs::oracle::referenced_source_columns`])
/// and its plain `GROUP BY` columns.
fn own_columns(def: &TransformDef) -> BTreeSet<String> {
    let mut columns: BTreeSet<String> = crate::defs::oracle::referenced_source_columns(def)
        .into_iter()
        .collect();
    if let KeySpace::Aggregate { group_by } = &def.key_space {
        for key in group_by {
            if let GroupByKey::Column(column) = key {
                columns.insert(column.clone());
            }
        }
    }
    columns
}

/// Whether `def` reads a column through `rel` (a definition names a
/// relationship of its own source).
fn reads_through(def: &TransformDef, rel: &RelationshipDefinition) -> bool {
    crate::defs::eval::relationship_references(def)
        .iter()
        .any(|(name, _)| *name == rel.def.name)
}

/// The to-side columns `def` reads through `rel`, by the relationship's
/// name. The caller checks that `def`'s source is `rel`'s from-side.
fn columns_through(def: &TransformDef, rel: &RelationshipDefinition) -> BTreeSet<String> {
    crate::defs::eval::relationship_references(def)
        .into_iter()
        .filter(|(name, _)| *name == rel.def.name)
        .map(|(_, column)| column)
        .collect()
}

/// Reads [`CaptureCatalog`] from the instance's catalog. `schema` is the
/// instance schema, where the settled parent projections live; the rest is
/// read through `client`'s `search_path`, like every catalog query.
pub async fn load_catalog(
    client: &impl GenericClient,
    schema: &str,
) -> Result<CaptureCatalog, crate::defs::catalog::CatalogError> {
    let (definitions, relationships) = crate::defs::catalog::capture_readers(client).await?;
    let definitions = definitions
        .into_iter()
        .map(|(id, source, def, capture_failed)| CaptureReader {
            id,
            source,
            def,
            capture_failed,
        })
        .collect();
    let bookkeeping = [
        crate::defs::ddl::PROJECTION_GEN_COLUMN,
        crate::defs::ddl::PROJECTION_LSN_COLUMN,
        crate::defs::ddl::RECOMPUTE_LSN_COLUMN,
    ];
    let mut projection_columns: HashMap<i64, Vec<String>> = HashMap::new();
    for row in client
        .query(
            "select rp.relationship_id, a.attname::text \
             from relationship_projections rp \
             join pg_catalog.pg_attribute a \
               on a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', $1::text, rp.projection_table)) \
             where a.attnum > 0 and not a.attisdropped and a.attname::text <> all($2::text[]) \
             order by rp.relationship_id, a.attnum",
            &[&schema, &bookkeeping.as_slice()],
        )
        .await?
    {
        projection_columns
            .entry(row.get(0))
            .or_default()
            .push(row.get(1));
    }
    Ok(CaptureCatalog {
        definitions,
        relationships,
        projection_columns,
    })
}

/// `table`'s live columns by name, with their attribute numbers, and its
/// primary key's columns in key order (empty when it has none). Both
/// [`capture_spec`] and the reconcile pass's schema-change check
/// (`staging::schema_change::pause_readers_of_missing`) read the key here,
/// so they can't disagree about it.
pub(crate) async fn live_columns(
    client: &impl GenericClient,
    table: &str,
) -> Result<(HashMap<String, i16>, Vec<String>), CaptureError> {
    let rows = client
        .query(
            "select a.attname::text, a.attnum, \
                    pg_catalog.array_position(i.indkey, a.attnum) \
             from pg_catalog.pg_attribute a \
             left join pg_catalog.pg_index i \
               on i.indrelid = a.attrelid and i.indisprimary \
              and a.attnum = any(i.indkey) \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attnum > 0 and not a.attisdropped",
            &[&crate::defs::ddl::regclass_arg(table)],
        )
        .await?;
    let mut attnums: HashMap<String, i16> = HashMap::with_capacity(rows.len());
    let mut key: Vec<(i32, String)> = Vec::new();
    for row in rows {
        let name: String = row.get(0);
        if let Some(position) = row.get::<_, Option<i32>>(2) {
            key.push((position, name.clone()));
        }
        attnums.insert(name, row.get(1));
    }
    key.sort();
    Ok((attnums, key.into_iter().map(|(_, name)| name).collect()))
}

/// The [`CaptureSpec`] for `table` (an unquoted `schema.table` identity):
/// its primary key plus [`read_columns`], checked against the table's live
/// columns.
///
/// Fails with [`CaptureError::NoPrimaryKey`] for a table without a primary
/// key, and with [`CaptureError::MissingColumn`] when a definition that
/// isn't [`CaptureReader::capture_failed`] reads a column the table no
/// longer has. A missing column no such definition reads is left out of the
/// spec (#622 C6).
pub async fn capture_spec(
    client: &impl GenericClient,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<CaptureSpec, CaptureError> {
    let regclass = crate::defs::ddl::regclass_arg(table);
    let exists: bool = client
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&regclass],
        )
        .await?
        .get(0);
    if !exists {
        return Err(CaptureError::UnknownTable {
            table: table.to_string(),
        });
    }
    let (attnums, key) = live_columns(client, table).await?;
    if key.is_empty() {
        return Err(CaptureError::NoPrimaryKey {
            table: table.to_string(),
        });
    }

    let mut read = read_columns(catalog, table);
    let missing: BTreeSet<String> = read
        .columns
        .iter()
        .filter(|c| !attnums.contains_key(*c))
        .cloned()
        .collect();
    if !missing.is_empty() {
        // #622 C6: a column some definition still reads is gone. The
        // reconcile pass pauses that definition before it gets here
        // (`staging::schema_change::pause_readers_of_missing`), as the drain
        // does on the table's next write's marker; after that it no longer
        // counts here. Until then the installed functions stay as they are.
        let active = |id: &i64| {
            catalog
                .definitions
                .iter()
                .any(|r| r.id == *id && !r.capture_failed)
        };
        if let Some(column) = missing.iter().find(|c| {
            readers_of(catalog, table, &BTreeSet::from([(*c).clone()]), false)
                .iter()
                .any(active)
        }) {
            return Err(CaptureError::MissingColumn {
                table: table.to_string(),
                column: column.clone(),
            });
        }
        // Only a relationship nothing reads through, or a projection column
        // no definition reads, still names it: image what's left.
        read.columns.retain(|c| !missing.contains(c));
        read.group_key.retain(|c| !missing.contains(c));
    }
    let mut group_key: Vec<String> = read.group_key.into_iter().collect();
    group_key.sort_by_key(|c| attnums[c]);
    CaptureSpec::new(table, key, read.columns, group_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::RelationshipDef;
    use crate::defs::parse;

    fn def(text: &str) -> TransformDef {
        parse(text).unwrap_or_else(|e| panic!("{text}: {e:?}"))
    }

    fn rel(
        id: i64,
        name: &str,
        from: (&str, &str, &str),
        to: (&str, &str, &str),
        cardinality: RelationshipCardinality,
    ) -> RelationshipDefinition {
        RelationshipDefinition {
            id,
            from_schema: from.0.to_string(),
            to_schema: to.0.to_string(),
            def: RelationshipDef {
                name: name.to_string(),
                from_table: from.1.to_string(),
                from_col: from.2.to_string(),
                to_table: to.1.to_string(),
                to_col: to.2.to_string(),
            },
            cardinality,
            warnings: Vec::new(),
        }
    }

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn catalog(
        definitions: &[(&str, &str)],
        relationships: Vec<RelationshipDefinition>,
    ) -> CaptureCatalog {
        CaptureCatalog {
            definitions: definitions
                .iter()
                .enumerate()
                .map(|(i, (source, text))| CaptureReader {
                    id: i as i64 + 1,
                    source: source.to_string(),
                    def: def(text),
                    capture_failed: false,
                })
                .collect(),
            relationships,
            projection_columns: HashMap::new(),
        }
    }

    #[test]
    fn a_plain_definition_reads_its_referenced_columns_only() {
        let catalog = catalog(
            &[(
                "public.orders",
                "TRANSFORM out FROM orders SELECT amount + 2 AS doubled, doubled + tax AS total",
            )],
            vec![],
        );
        let read = read_columns(&catalog, "public.orders");
        assert_eq!(read.columns, set(&["amount", "tax"]));
        assert!(read.group_key.is_empty());
        assert_eq!(
            read_columns(&catalog, "public.other"),
            ReadColumns::default()
        );
    }

    #[test]
    fn a_self_named_passthrough_reads_its_own_column() {
        let catalog = catalog(
            &[(
                "public.orders",
                "TRANSFORM out FROM orders SELECT amount AS amount",
            )],
            vec![],
        );
        assert_eq!(
            read_columns(&catalog, "public.orders").columns,
            set(&["amount"])
        );
    }

    #[test]
    fn an_aggregate_reads_its_group_by_and_argument_columns() {
        let catalog = catalog(
            &[(
                "public.sales",
                "TRANSFORM by_store FROM sales GROUP BY store, region \
                 SELECT store AS store, region AS region, SUM(amount) AS total, COUNT(*) AS n",
            )],
            vec![],
        );
        assert_eq!(
            read_columns(&catalog, "public.sales").columns,
            set(&["amount", "region", "store"])
        );
    }

    #[test]
    fn where_true_reads_nothing() {
        let catalog = catalog(
            &[(
                "public.orders",
                "TRANSFORM out FROM orders SELECT amount AS a WHERE TRUE",
            )],
            vec![],
        );
        assert_eq!(
            read_columns(&catalog, "public.orders").columns,
            set(&["amount"])
        );
    }

    #[test]
    fn a_from_side_images_every_relationships_from_col_as_a_group_key() {
        let catalog = catalog(
            &[(
                "public.posts",
                "TRANSFORM enriched FROM posts SELECT title AS title, author.name AS author_name",
            )],
            vec![
                rel(
                    1,
                    "author",
                    ("public", "posts", "author_id"),
                    ("public", "users", "id"),
                    RelationshipCardinality::ToOne,
                ),
                // Declared but unused: its from_col is still a group key.
                rel(
                    2,
                    "editor",
                    ("public", "posts", "editor_id"),
                    ("public", "users", "id"),
                    RelationshipCardinality::ToOne,
                ),
            ],
        );
        let read = read_columns(&catalog, "public.posts");
        assert_eq!(read.columns, set(&["author_id", "editor_id", "title"]));
        assert_eq!(read.group_key, set(&["author_id", "editor_id"]));
    }

    #[test]
    fn a_to_side_images_its_to_col_and_every_column_read_through_it() {
        let mut catalog = catalog(
            &[
                (
                    "public.posts",
                    "TRANSFORM enriched FROM posts SELECT author.name AS author_name",
                ),
                (
                    "public.posts",
                    "TRANSFORM by_country FROM posts GROUP BY author.country \
                     SELECT author.country AS country, COUNT(*) AS n",
                ),
                // Same relationship name on another table: not this one.
                (
                    "other.posts",
                    "TRANSFORM elsewhere FROM other.posts SELECT author.email AS email",
                ),
            ],
            vec![
                rel(
                    1,
                    "author",
                    ("public", "posts", "author_id"),
                    ("public", "users", "user_id"),
                    RelationshipCardinality::ToOne,
                ),
                rel(
                    2,
                    "author",
                    ("other", "posts", "author_id"),
                    ("other", "users", "id"),
                    RelationshipCardinality::ToOne,
                ),
            ],
        );
        catalog
            .projection_columns
            .insert(1, vec!["user_id".to_string(), "retired_col".to_string()]);
        catalog
            .projection_columns
            .insert(2, vec!["not_this_table".to_string()]);
        let read = read_columns(&catalog, "public.users");
        assert_eq!(
            read.columns,
            set(&["country", "name", "retired_col", "user_id"])
        );
        assert!(read.group_key.is_empty(), "a to-side has no group key");
    }

    /// `a_to_side_images_its_to_col_and_every_column_read_through_it`'s
    /// catalog: `enriched` (1) reads `author.name`, `by_country` (2) groups
    /// by `author.country`, and `elsewhere` (3) reads another `author`.
    fn author_catalog() -> CaptureCatalog {
        let mut catalog = catalog(
            &[
                (
                    "public.posts",
                    "TRANSFORM enriched FROM posts SELECT author.name AS author_name",
                ),
                (
                    "public.posts",
                    "TRANSFORM by_country FROM posts GROUP BY author.country \
                     SELECT author.country AS country, COUNT(*) AS n",
                ),
                (
                    "other.posts",
                    "TRANSFORM elsewhere FROM other.posts SELECT author.email AS email",
                ),
            ],
            vec![
                rel(
                    1,
                    "author",
                    ("public", "posts", "author_id"),
                    ("public", "users", "user_id"),
                    RelationshipCardinality::ToOne,
                ),
                rel(
                    2,
                    "author",
                    ("other", "posts", "author_id"),
                    ("other", "users", "id"),
                    RelationshipCardinality::ToOne,
                ),
            ],
        );
        catalog
            .projection_columns
            .insert(1, vec!["user_id".to_string(), "retired_col".to_string()]);
        catalog
    }

    #[test]
    fn a_schema_change_pauses_only_the_definitions_that_read_a_missing_column() {
        let catalog = author_catalog();
        let users = "public.users";
        assert_eq!(readers_of(&catalog, users, &set(&["name"]), false), [1]);
        assert_eq!(readers_of(&catalog, users, &set(&["country"]), false), [2]);
        assert_eq!(
            readers_of(&catalog, users, &set(&["user_id"]), false),
            [1, 2],
            "every definition reading through a relationship reads its to_col"
        );
        assert!(
            readers_of(&catalog, users, &set(&["retired_col", "email"]), false).is_empty(),
            "a projection column no definition reads belongs to none"
        );
        assert_eq!(readers_of(&catalog, users, &set(&[]), true), [1, 2]);
        assert_eq!(
            readers_of(&catalog, "public.posts", &set(&["author_id"]), false),
            [1, 2],
            "the from_col of a relationship a definition reads through"
        );
        assert_eq!(
            readers_of(&catalog, "public.posts", &set(&[]), true),
            [1, 2]
        );
        assert_eq!(
            readers_of(&catalog, "other.users", &set(&["email"]), false),
            [3]
        );
    }

    #[test]
    fn a_capture_failed_definition_reads_nothing() {
        let mut catalog = author_catalog();
        catalog.definitions[0].capture_failed = true;
        assert_eq!(
            read_columns(&catalog, "public.users").columns,
            set(&["country", "retired_col", "user_id"]),
            "`name` was read only by the paused definition"
        );
        assert_eq!(
            readers_of(&catalog, "public.users", &set(&["name"]), false),
            [1],
            "it is still the reader a marker names"
        );
    }

    #[test]
    fn a_to_many_to_side_images_its_to_col_and_the_columns_read_through_it() {
        let catalog = catalog(
            &[(
                "public.users",
                "TRANSFORM spend FROM users SELECT SUM(orders.amount) AS spent",
            )],
            vec![rel(
                1,
                "orders",
                ("public", "users", "id"),
                ("public", "orders", "user_id"),
                RelationshipCardinality::ToMany,
            )],
        );
        let read = read_columns(&catalog, "public.orders");
        assert_eq!(read.columns, set(&["amount", "user_id"]));
        let from = read_columns(&catalog, "public.users");
        assert_eq!(from.columns, set(&["id"]));
        assert_eq!(from.group_key, set(&["id"]));
    }

    #[test]
    fn a_chained_relationship_table_is_both_a_to_side_and_a_from_side() {
        // comments.post -> posts.id, posts.author -> users.id
        let catalog = catalog(
            &[(
                "public.comments",
                "TRANSFORM c FROM comments SELECT post.title AS title",
            )],
            vec![
                rel(
                    1,
                    "post",
                    ("public", "comments", "post_id"),
                    ("public", "posts", "id"),
                    RelationshipCardinality::ToOne,
                ),
                rel(
                    2,
                    "author",
                    ("public", "posts", "author_id"),
                    ("public", "users", "id"),
                    RelationshipCardinality::ToOne,
                ),
            ],
        );
        let posts = read_columns(&catalog, "public.posts");
        assert_eq!(posts.columns, set(&["author_id", "id", "title"]));
        assert_eq!(posts.group_key, set(&["author_id"]));
        let users = read_columns(&catalog, "public.users");
        assert_eq!(users.columns, set(&["id"]));
        let comments = read_columns(&catalog, "public.comments");
        assert_eq!(comments.columns, set(&["post_id"]));
        assert_eq!(comments.group_key, set(&["post_id"]));
    }
}
