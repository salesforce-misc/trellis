//! Which columns a table's capture triggers image.
//!
//! A trigger images the primary key plus every column some consumer of the
//! table's ring rows reads. Imaging less poisons rows that lack a column
//! a reader needs (a 1-1 field fails on it,
//! `staging::one_to_one_render`), or worse, silently reads a missing value
//! as `NULL`. Imaging more costs the writer (every column of a table
//! with a 100 KB column cost 40x a narrow image). The set, for table `T`:
//!
//! 1. **`T`'s primary key.**
//! 2. **Columns `T`'s own definitions read.** For every definition whose
//!    source is `T`, in any status (a `JOIN` reads both of its tables, and
//!    from each its join key, its `GROUP BY` columns and every column its
//!    fields read there):
//!    - every source column its fields reference
//!      ([`crate::defs::oracle::referenced_source_columns`]: a self-named
//!      passthrough reads its own column, a reference to another calculated
//!      field doesn't);
//!    - its plain `GROUP BY` columns.
//!
//!    Aggregate apply evaluates every field over both images, and a missing
//!    `GROUP BY` value would silently put the row in the `NULL` group.
//!    `WHERE` accepts only `TRUE` today, so no filter reads a column.
//!
//! A chained definition's source is another definition's target. That table
//! isn't captured by triggers in C (the target-mutation seam feeds it), so
//! nothing here needs it.

use std::collections::{BTreeSet, HashMap};

use tokio_postgres::GenericClient;
use tokio_postgres::types::Type;

use crate::defs::ast::{KeySpace, TransformDef};

use super::CaptureError;
use super::sql::CaptureSpec;

/// Everything in the catalog the column set depends on.
#[derive(Debug, Clone, Default)]
pub struct CaptureCatalog {
    /// Every registered definition, in any status.
    pub definitions: Vec<CaptureReader>,
}

/// One registered definition, as capture sees it.
#[derive(Debug, Clone)]
pub struct CaptureReader {
    pub id: i64,
    /// The qualified `source_table`.
    pub source: String,
    pub def: TransformDef,
    /// A `schema_changed` marker paused it (`capture_failures`).
    /// Until it resumes, which rebuilds it, it reads nothing, so its columns
    /// don't count and the table's capture can drop a column only it read.
    pub capture_failed: bool,
    /// The qualified `target_table` it writes.
    pub target: String,
    /// A `JOIN`'s qualified joined table, its second source.
    pub join: Option<String>,
}

impl CaptureReader {
    /// The columns of `table` this definition reads, as its source or as
    /// either table of a `JOIN`; `None` if it doesn't read `table`.
    pub(crate) fn reads(&self, table: &str) -> Option<BTreeSet<String>> {
        if self.join.is_some() {
            if self.source != table && self.join.as_deref() != Some(table) {
                return None;
            }
            return crate::defs::join::side_of_table(&self.def, table)
                .map(|side| crate::defs::join::side_columns(&self.def, side));
        }
        (self.source == table).then(|| own_columns(&self.def))
    }
}

/// The columns [`read_columns`] finds a table's readers need, before the
/// primary key is added.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadColumns {
    /// Every column an image must carry.
    pub columns: BTreeSet<String>,
}

/// The columns [`CaptureCatalog`]'s readers need from `table`'s images
/// (this module's rule 2). `table` is the unquoted `schema.table`
/// identity. A [`CaptureReader::capture_failed`] definition doesn't count.
pub fn read_columns(catalog: &CaptureCatalog, table: &str) -> ReadColumns {
    let mut read = ReadColumns::default();

    for reader in catalog.definitions.iter().filter(|r| !r.capture_failed) {
        if let Some(columns) = reader.reads(table) {
            read.columns.extend(columns);
        }
    }
    read
}

/// Every definition that reads one of `missing` from `table`'s images, or,
/// with `key_missing`, every definition that reads `table` at all (the
/// definitions a `schema_changed` marker pauses). In id order.
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
            reader
                .reads(table)
                .is_some_and(|reads| key_missing || !reads.is_disjoint(missing))
        })
        .map(|reader| reader.id)
        .collect();
    ids.sort_unstable();
    ids
}

/// The columns of its own source `def` reads directly: every source column
/// its fields reference ([`crate::defs::oracle::referenced_source_columns`])
/// and its `GROUP BY` columns.
fn own_columns(def: &TransformDef) -> BTreeSet<String> {
    let mut columns: BTreeSet<String> = crate::defs::oracle::referenced_source_columns(def)
        .into_iter()
        .collect();
    if let KeySpace::Aggregate { group_by } = &def.key_space {
        columns.extend(group_by.iter().cloned());
    }
    columns
}

/// Reads [`CaptureCatalog`] from the instance's catalog, through `client`'s
/// `search_path`, like every catalog query.
pub async fn load_catalog(
    client: &impl GenericClient,
) -> Result<CaptureCatalog, crate::defs::catalog::CatalogError> {
    let definitions = crate::defs::catalog::capture_readers(client)
        .await?
        .into_iter()
        .map(|row| CaptureReader {
            id: row.id,
            source: row.source,
            def: row.def,
            capture_failed: row.capture_failed,
            target: row.target,
            join: row.join,
        })
        .collect();
    Ok(CaptureCatalog { definitions })
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
        .query_typed(
            "select a.attname::text, a.attnum, \
                    pg_catalog.array_position(i.indkey, a.attnum) \
             from pg_catalog.pg_attribute a \
             left join pg_catalog.pg_index i \
               on i.indrelid = a.attrelid and i.indisprimary \
              and a.attnum = any(i.indkey) \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attnum > 0 and not a.attisdropped",
            &[(&crate::defs::ddl::regclass_arg(table), Type::TEXT)],
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
/// spec.
pub async fn capture_spec(
    client: &impl GenericClient,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<CaptureSpec, CaptureError> {
    let regclass = crate::defs::ddl::regclass_arg(table);
    let exists: bool = client
        .query_typed_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[(&regclass, Type::TEXT)],
        )
        .await?
        .get(0);
    if !exists {
        return Err(CaptureError::UnknownTable {
            table: table.to_string(),
        });
    }
    let partitioned: bool = client
        .query_one(
            "select exists (select 1 from pg_catalog.pg_class \
                 where oid = pg_catalog.to_regclass($1) and relkind = 'p')",
            &[&regclass],
        )
        .await?
        .get(0);
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
        // A column some definition still reads is gone. The
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
        // Only a capture-failed definition still names it: image what's
        // left.
        read.columns.retain(|c| !missing.contains(c));
    }
    Ok(CaptureSpec::new(table, key, read.columns)?.with_partitioned(partitioned))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::parse;

    fn def(text: &str) -> TransformDef {
        parse(text).unwrap_or_else(|e| panic!("{text}: {e:?}"))
    }

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn catalog(definitions: &[(&str, &str)]) -> CaptureCatalog {
        CaptureCatalog {
            definitions: definitions
                .iter()
                .enumerate()
                .map(|(i, (source, text))| CaptureReader {
                    id: i as i64 + 1,
                    source: source.to_string(),
                    def: def(text),
                    capture_failed: false,
                    target: format!("public.target_{}", i + 1),
                    join: None,
                })
                .collect(),
        }
    }

    #[test]
    fn a_plain_definition_reads_its_referenced_columns_only() {
        let catalog = catalog(&[(
            "public.orders",
            "TRANSFORM out FROM orders SELECT amount + 2 AS doubled, doubled + tax AS total",
        )]);
        let read = read_columns(&catalog, "public.orders");
        assert_eq!(read.columns, set(&["amount", "tax"]));
        assert_eq!(
            read_columns(&catalog, "public.other"),
            ReadColumns::default()
        );
    }

    #[test]
    fn a_join_reads_each_side_and_is_a_reader_of_both() {
        let mut catalog = catalog(&[(
            "public.orders",
            "TRANSFORM by_sku FROM orders LEFT JOIN items ON orders.id = items.order_id \
                 GROUP BY items.sku SELECT items.sku AS sku, SUM(orders.total) AS total, \
                 COUNT(items.qty) AS n",
        )]);
        catalog.definitions[0].join = Some("public.items".to_string());
        assert_eq!(
            read_columns(&catalog, "public.orders").columns,
            set(&["id", "total"])
        );
        assert_eq!(
            read_columns(&catalog, "public.items").columns,
            set(&["order_id", "qty", "sku"])
        );
        // A table that merely shares a side's bare name isn't read.
        assert_eq!(
            read_columns(&catalog, "archive.items"),
            ReadColumns::default()
        );
        // A missing column, or a missing key, on either side pauses it.
        assert_eq!(
            readers_of(&catalog, "public.items", &set(&["sku"]), false),
            [1]
        );
        assert_eq!(
            readers_of(&catalog, "public.orders", &set(&["total"]), false),
            [1]
        );
        assert_eq!(
            readers_of(&catalog, "public.items", &BTreeSet::new(), true),
            [1]
        );
        assert!(readers_of(&catalog, "public.items", &set(&["price"]), false).is_empty());
    }

    #[test]
    fn a_self_named_passthrough_reads_its_own_column() {
        let catalog = catalog(&[(
            "public.orders",
            "TRANSFORM out FROM orders SELECT amount AS amount",
        )]);
        assert_eq!(
            read_columns(&catalog, "public.orders").columns,
            set(&["amount"])
        );
    }

    #[test]
    fn an_aggregate_reads_its_group_by_and_argument_columns() {
        let catalog = catalog(&[(
            "public.sales",
            "TRANSFORM by_store FROM sales GROUP BY store, region \
                 SELECT store AS store, region AS region, SUM(amount) AS total, COUNT(*) AS n",
        )]);
        assert_eq!(
            read_columns(&catalog, "public.sales").columns,
            set(&["amount", "region", "store"])
        );
    }

    #[test]
    fn where_true_reads_nothing() {
        let catalog = catalog(&[(
            "public.orders",
            "TRANSFORM out FROM orders SELECT amount AS a WHERE TRUE",
        )]);
        assert_eq!(
            read_columns(&catalog, "public.orders").columns,
            set(&["amount"])
        );
    }

    /// `names` (1) reads `name` and `by_country` (2) groups by `country`, both
    /// on `public.users`; `elsewhere` (3) reads a table of the same name in
    /// another schema.
    fn users_catalog() -> CaptureCatalog {
        catalog(&[
            (
                "public.users",
                "TRANSFORM names FROM users SELECT name AS user_name",
            ),
            (
                "public.users",
                "TRANSFORM by_country FROM users GROUP BY country \
                 SELECT country AS country, COUNT(*) AS n",
            ),
            (
                "other.users",
                "TRANSFORM elsewhere FROM other.users SELECT email AS user_email",
            ),
        ])
    }

    #[test]
    fn a_schema_change_pauses_only_the_definitions_that_read_a_missing_column() {
        let catalog = users_catalog();
        let users = "public.users";
        assert_eq!(readers_of(&catalog, users, &set(&["name"]), false), [1]);
        assert_eq!(readers_of(&catalog, users, &set(&["country"]), false), [2]);
        assert!(
            readers_of(&catalog, users, &set(&["email"]), false).is_empty(),
            "only the other schema's table's reader reads `email`"
        );
        assert_eq!(readers_of(&catalog, users, &set(&[]), true), [1, 2]);
        assert_eq!(
            readers_of(&catalog, "other.users", &set(&["email"]), false),
            [3]
        );
    }

    #[test]
    fn a_capture_failed_definition_reads_nothing() {
        let mut catalog = users_catalog();
        catalog.definitions[0].capture_failed = true;
        assert_eq!(
            read_columns(&catalog, "public.users").columns,
            set(&["country"]),
            "`name` was read only by the paused definition"
        );
        assert_eq!(
            readers_of(&catalog, "public.users", &set(&["name"]), false),
            [1],
            "it is still the reader a marker names"
        );
    }

    /// Capture images a 1-1 JOIN's sides by the columns each side's join key
    /// and fields read (the `b` side too, for its old join key).
    #[test]
    fn a_one_to_one_join_reads_the_columns_of_each_side() {
        let mut catalog = catalog(&[(
            "public.orders",
            "TRANSFORM v FROM orders LEFT JOIN customers ON orders.cust = customers.id \
             SELECT customers.name AS who, orders.total * 2 AS doubled",
        )]);
        catalog.definitions[0].join = Some("public.customers".to_string());
        assert_eq!(
            read_columns(&catalog, "public.orders").columns,
            set(&["cust", "total"])
        );
        assert_eq!(
            read_columns(&catalog, "public.customers").columns,
            set(&["id", "name"])
        );
    }
}
