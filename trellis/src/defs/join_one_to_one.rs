//! 1-1 `JOIN` transforms: the tables Trellis keeps for one (ADR-0021).
//!
//! A 1-1 `JOIN` definition (`FROM a [INNER|LEFT] JOIN b ON ...` with no
//! `GROUP BY`, [`super::ast::KeySpace::JoinOneToOne`]) reads the same two
//! tables an aggregate `JOIN` does, so [`super::join::spec`] and the helpers
//! built on it (`side_columns`, `join_key`, `side_of_table`) serve both. What
//! differs is the state kept. Each definition owns four tables in the
//! instance's catalog schema, named `join1_{id}_{r|m|j|l}` by its definition
//! id:
//!
//! - `R` ([`HiddenTable::FromEntries`]): one entry per `a` row, keyed by
//!   `a`'s primary key text, holding the 1-1 ledger's ordering state
//!   (`super::ledger`), the row's join key and the `a` columns the fields
//!   read. Indexed on the join key and then the entry key (how a `b` change
//!   finds its rows, and a fan-out chunk walks them from a cursor), and by
//!   the partial `where __tombstone` index the tombstone collection walks;
//! - `M` ([`HiddenTable::JoinedEntries`]): the same, one entry per `b` row;
//! - `J` ([`HiddenTable::Keys`]): one lock row per join key;
//! - the lock row ([`HiddenTable::TruncateLock`]): the pair's truncate lock.
//!
//! The target is keyed by `a`'s primary key and holds only the declared
//! columns, with no ledger of its own: `R` is its ledger.
//!
//! They're fully derived: define creates them empty, `DROP TRANSFORM` drops
//! them, and a resume drops and recreates them.
//!
//! Pages of either side's changes, Re-derives and `TRUNCATE`s, the chunks of
//! a pending fan-out, and the chunked build, run in
//! `crate::staging::join_one_to_one`.

use super::floor::Floors;
use std::collections::HashMap;

use tokio_postgres::GenericClient;

use super::ast::{Expr, FieldDef, TransformDef};
use super::ddl::PrimaryKeyColumn;
use super::join::{
    Side, column_type, join_key, keys_table_ddl, lock_row_ddl, side_columns, typed_column,
};
use super::validate::{JoinColumnInfo, ValidationError};
use crate::pool::quote_ident;

/// One hidden table of a 1-1 `JOIN` definition (this module's doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HiddenTable {
    /// `R`: an entry per `a` row.
    FromEntries,
    /// `M`: an entry per `b` row.
    JoinedEntries,
    /// `J`: a lock row per join key.
    Keys,
    /// The pair's one truncate lock row.
    TruncateLock,
}

impl HiddenTable {
    pub(crate) const ALL: [HiddenTable; 4] = [
        HiddenTable::FromEntries,
        HiddenTable::JoinedEntries,
        HiddenTable::Keys,
        HiddenTable::TruncateLock,
    ];

    /// The two ledger-bearing tables: `R` for the `FROM` side, `M` for the
    /// joined one.
    pub(crate) const LEDGERS: [HiddenTable; 2] =
        [HiddenTable::FromEntries, HiddenTable::JoinedEntries];

    fn part(self) -> &'static str {
        match self {
            HiddenTable::FromEntries => "r",
            HiddenTable::JoinedEntries => "m",
            HiddenTable::Keys => "j",
            HiddenTable::TruncateLock => "l",
        }
    }

    /// The table's bare name in the catalog schema, for definition `id`.
    pub(crate) fn name(self, id: i64) -> String {
        format!("join1_{id}_{}", self.part())
    }

    /// [`Self::name`] as SQL, over the definition id expression `id`.
    pub(crate) fn name_sql(self, id: &str) -> String {
        format!("('join1_' || {id} || '_{}')", self.part())
    }

    /// The `ledger_truncate_floor` keys of the two hidden ledgers of
    /// definition `id`: their quoted, qualified idents, as a `JOIN`'s `P`
    /// and `M` floors are keyed.
    pub(crate) fn floor_keys(catalog_schema: &str, id: i64) -> Vec<String> {
        HiddenTable::LEDGERS
            .iter()
            .map(|table| table.qualified(catalog_schema, id))
            .collect()
    }

    /// The table, schema-qualified and quoted, for definition `id` in
    /// `catalog_schema`.
    pub(crate) fn qualified(self, catalog_schema: &str, id: i64) -> String {
        format!(
            "{}.{}",
            quote_ident(catalog_schema),
            quote_ident(&self.name(id))
        )
    }
}

/// The columns `side`'s ledger table copies from its source, in order: the
/// join key, then the other columns the fields read.
pub(crate) fn read_columns(def: &TransformDef, side: Side) -> Vec<String> {
    let key = join_key(def, side);
    let mut read = key.clone();
    read.extend(
        side_columns(def, side)
            .into_iter()
            .filter(|column| !key.contains(column)),
    );
    read
}

/// Each table a 1-1 `JOIN` definition creates whose width follows its
/// definition, apart from the target (whose columns are the primary key's and
/// the fields'): the two ledgers `R` and `M`, with the subject a refusal
/// names each by. `J` is as wide as the join key and one column, which
/// `check_index_key_width` bounds. Each width counts the columns the ledger's
/// DDL ([`hidden_tables_ddl`]) is built from.
pub(crate) fn ledger_widths(def: &TransformDef) -> Vec<(String, usize)> {
    let Some(join) = super::join::spec(def) else {
        return Vec::new();
    };
    [(Side::From, &def.source), (Side::Joined, &join.right)]
        .into_iter()
        .map(|(side, table)| {
            (
                format!("{table}'s hidden ledger table"),
                1 + super::ledger::ORDERING_STATE_COLUMN_COUNT + read_columns(def, side).len(),
            )
        })
        .collect()
}

/// Renders the `create table` statements of `def`'s hidden tables
/// ([`HiddenTable`]) in `catalog_schema`, for definition `id`. `from` and
/// `joined` are the two sides' live columns, as define read them.
pub(crate) fn hidden_tables_ddl(
    catalog_schema: &str,
    id: i64,
    def: &TransformDef,
    from: &HashMap<String, JoinColumnInfo>,
    joined: &HashMap<String, JoinColumnInfo>,
) -> String {
    let mut sql = String::new();
    for (table, side, columns) in [
        (HiddenTable::FromEntries, Side::From, from),
        (HiddenTable::JoinedEntries, Side::Joined, joined),
    ] {
        let name = table.qualified(catalog_schema, id);
        let key = join_key(def, side);
        // The join key leads, then the other columns the fields read.
        let read = read_columns(def, side);
        let mut body = vec![format!(
            "{} text collate \"C\" primary key",
            quote_ident(super::ledger::KEY_COLUMN)
        )];
        body.push(super::ledger::ordering_state_columns());
        body.extend(read.iter().map(|column| typed_column(columns, column)));
        let mut quoted_key: Vec<String> = key.iter().map(|c| quote_ident(c)).collect();
        // `R`'s index ends with the entry key: a fan-out chunk walks one
        // join key's entries in key order from a cursor
        // (`crate::staging::join_one_to_one::fan_out`).
        if table == HiddenTable::FromEntries {
            quoted_key.push(quote_ident(super::ledger::KEY_COLUMN));
        }
        sql.push_str(&format!(
            "create table {name} ({});\n\
             create index on {name} ({});\n\
             create index on {name} ({}) where {};\n",
            body.join(", "),
            quoted_key.join(", "),
            quote_ident(super::ledger::APPLIED_SEG_COLUMN),
            quote_ident(super::ledger::TOMBSTONE_COLUMN),
        ));
    }
    sql.push_str(&keys_table_ddl(
        &HiddenTable::Keys.qualified(catalog_schema, id),
        def,
        from,
        &[],
    ));
    sql.push_str(&lock_row_ddl(
        &HiddenTable::TruncateLock.qualified(catalog_schema, id),
    ));
    sql
}

/// The statements dropping every hidden table of definition `id` from
/// `catalog_schema`, any that is already gone included, and deleting its
/// pending fan-outs (`join1_fan_out`), which name keys of the `R` and `J`
/// they drop: a resume's build recomputes every row.
pub(crate) fn drop_hidden_tables_sql(catalog_schema: &str, id: i64) -> String {
    let tables: Vec<String> = HiddenTable::ALL
        .iter()
        .map(|table| table.qualified(catalog_schema, id))
        .collect();
    format!(
        "drop table if exists {}; delete from {}.join1_fan_out where transform_id = {id}",
        tables.join(", "),
        quote_ident(catalog_schema),
    )
}

/// The Postgres type of a 1-1 `JOIN` field: a bare column of either table
/// keeps that column's type with no modifier and its collation, as a copy
/// does (ADR-0020); any other expression has the type its inference gives.
fn field_column_type(
    def: &TransformDef,
    field: &FieldDef,
    from: &HashMap<String, JoinColumnInfo>,
    joined: &HashMap<String, JoinColumnInfo>,
) -> Result<String, ValidationError> {
    if let Expr::SourceColumn { table, column, .. } = &field.expr {
        let columns = if *table == def.source { from } else { joined };
        if let Some(info) = columns.get(column) {
            return Ok(column_type(info));
        }
    }
    super::join::field_pg_type(def, field, from, joined)
}

/// Renders the `create table` statement of a 1-1 `JOIN` definition's visible
/// target, `qualified_target` (already quoted): `a`'s primary key `pk`, as a
/// 1-1 target mirrors its source's, then one column per field. No ledger of
/// its own: `R` is the ledger.
pub(crate) fn target_ddl(
    def: &TransformDef,
    qualified_target: &str,
    pk: &[PrimaryKeyColumn],
    from: &HashMap<String, JoinColumnInfo>,
    joined: &HashMap<String, JoinColumnInfo>,
) -> Result<String, ValidationError> {
    let (mut columns, constraint) = super::ddl::pk_columns_ddl(pk);
    for field in &def.fields {
        let pg_type = field_column_type(def, field, from, joined)?;
        columns.push(format!("{} {pg_type}", quote_ident(&field.name)));
    }
    columns.push(constraint);
    Ok(format!(
        "create table {qualified_target} ({})",
        columns.join(", ")
    ))
}

/// Whether some unique key of the joined table lies within `on_columns`, so
/// that at most one row of it matches a join key. Each of `unique_keys` is a
/// set of columns that is unique on its own.
pub(crate) fn key_is_unique(unique_keys: &[Vec<String>], on_columns: &[String]) -> bool {
    unique_keys
        .iter()
        .any(|key| !key.is_empty() && key.iter().all(|column| on_columns.contains(column)))
}

/// The column sets of `qualified`'s unique keys: its primary key, and every
/// valid, non-partial unique index or constraint made of plain columns
/// (expressions and `INCLUDE` columns don't count; a deferrable constraint
/// does). A unique key that allows several `NULL`s still serves: a `NULL`
/// key never matches in an equi-join.
pub(crate) async fn unique_keys_of(
    client: &impl GenericClient,
    qualified: &str,
) -> Result<Vec<Vec<String>>, tokio_postgres::Error> {
    let rows = client
        .query(
            "select array_agg(a.attname::text order by k.ord) \
             from pg_catalog.pg_index i \
             cross join lateral unnest(i.indkey::int2[]) with ordinality as k(attnum, ord) \
             join pg_catalog.pg_attribute a \
               on a.attrelid = i.indrelid and a.attnum = k.attnum \
             where i.indrelid = pg_catalog.to_regclass($1) \
               and i.indisunique and i.indisvalid and i.indpred is null \
               and k.ord <= i.indnkeyatts \
             group by i.indexrelid, i.indnkeyatts \
             having count(*) = i.indnkeyatts",
            &[&super::ddl::regclass_arg(qualified)],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// Whether `joined` (qualified), `def`'s joined table, has a unique key
/// within its `ON` columns ([`unique_keys_of`], [`key_is_unique`]): the rule
/// define and `RESUME` hold a 1-1 `JOIN` to ([`check_define`]), and that the
/// staging worker's capture pass re-checks while it runs
/// (`staging::schema_change::pause_joins_without_unique_key`). One catalog
/// query. A table that doesn't exist has no key.
pub(crate) async fn joined_key_is_unique(
    client: &impl GenericClient,
    def: &TransformDef,
    joined: &str,
) -> Result<bool, tokio_postgres::Error> {
    Ok(key_is_unique(
        &unique_keys_of(client, joined).await?,
        &joined_on_columns(def),
    ))
}

/// The joined table's `ON` columns of `def`, in `ON` order.
pub(crate) fn joined_on_columns(def: &TransformDef) -> Vec<String> {
    join_key(def, Side::Joined)
}

/// The key space of the definition whose target is `qualified_table`, if one
/// is: the definition is read back from its stored text.
///
/// A definition this release can't run (ADR-0024 rule 3) isn't parsed, so its
/// key space can't be known: the check that asks refuses, naming why, rather
/// than let a reader of a target this release can't vouch for through.
async fn target_key_space(
    client: &impl GenericClient,
    floors: &Floors,
    qualified_table: &str,
) -> Result<Option<super::ast::KeySpace>, super::catalog::CatalogError> {
    let Some(row) = client
        .query_opt(
            "select definition_text, grammar_version, floor_version \
             from transform_definitions where target_table = $1",
            &[&qualified_table],
        )
        .await?
    else {
        return Ok(None);
    };
    if let Some(unrunnable) = floors.unrunnable(row.get(2), row.get(1)) {
        return Err(super::catalog::CatalogError::CannotRun {
            action: format!("define a transform reading {qualified_table}"),
            reason: unrunnable.reason(qualified_table),
        });
    }
    Ok(
        super::grammar::load_stored(floors, row.get(0), row.get(1), row.get(2))
            .ok()
            .flatten()
            .map(|def| def.key_space),
    )
}

/// The define-time checks of a 1-1 `JOIN` that read the catalog, once its
/// joined table is resolved (`joined`, qualified): the joined table's `ON`
/// columns are unique, and a joined table that is another transform's 1-1
/// target is joined on columns that include its primary key. Trellis orders
/// changes per primary-key value only, so the primary key is the one column
/// set of a target that stays unique at every instant while its upstream
/// drains; a unique index on any other column can hold transient duplicates.
/// The `FROM` table has no such rule: its `ON` columns carry no uniqueness.
pub(crate) async fn check_define(
    client: &impl GenericClient,
    floors: &Floors,
    def: &TransformDef,
    joined: &str,
) -> Result<(), super::catalog::CatalogError> {
    let on = joined_on_columns(def);
    if !joined_key_is_unique(client, def, joined).await? {
        return Err(ValidationError::OneToOneJoinKeyNotUnique {
            table: joined.to_string(),
            columns: on,
        }
        .into());
    }
    if matches!(
        target_key_space(client, floors, joined).await?,
        Some(super::ast::KeySpace::OneToOne)
    ) {
        let pk: Vec<String> = super::ddl::identity_key_columns(client, joined)
            .await?
            .into_iter()
            .map(|c| c.name)
            .collect();
        if !pk.iter().all(|column| on.contains(column)) {
            return Err(ValidationError::OneToOneJoinUnsupported {
                what: format!(
                    "'{joined}', the 1-1 target of another transform, joined on columns that \
                     leave out part of its primary key ({})",
                    pk.join(", ")
                ),
                remedy: "join on columns that include its primary key, or join the table that \
                         transform reads"
                    .to_string(),
            }
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::ValueType;
    use crate::defs::parse;
    use crate::integer::IntWidth;

    fn info(pg_type: &str, value_type: ValueType) -> JoinColumnInfo {
        JoinColumnInfo {
            value_type,
            pg_type: pg_type.to_string(),
            type_oid: 0,
            typmod: -1,
            collation_oid: 0,
            collation: None,
            collation_deterministic: true,
            is_enum: false,
            copy_type: pg_type.split('(').next().unwrap_or(pg_type).to_string(),
            copy_collation: None,
            trellis_target: false,
        }
    }

    const DEF: &str = "TRANSFORM v FROM orders LEFT JOIN customers \
        ON orders.cust = customers.id AND orders.tenant = customers.tenant \
        SELECT customers.name AS who, orders.total * 2 AS doubled";

    fn sides() -> (
        HashMap<String, JoinColumnInfo>,
        HashMap<String, JoinColumnInfo>,
    ) {
        let int = || info("integer", ValueType::Integer(IntWidth::Int4));
        (
            HashMap::from([
                ("cust".to_string(), int()),
                ("tenant".to_string(), int()),
                (
                    "total".to_string(),
                    info("numeric(10,2)", ValueType::Numeric),
                ),
            ]),
            HashMap::from([
                ("id".to_string(), int()),
                ("tenant".to_string(), int()),
                ("name".to_string(), info("text", ValueType::Text)),
            ]),
        )
    }

    #[test]
    fn hidden_tables_are_named_by_definition_id_apart_from_an_aggregate_joins() {
        assert_eq!(HiddenTable::FromEntries.name(7), "join1_7_r");
        assert_eq!(HiddenTable::JoinedEntries.name(7), "join1_7_m");
        assert_eq!(HiddenTable::Keys.name(7), "join1_7_j");
        assert_eq!(HiddenTable::TruncateLock.name(7), "join1_7_l");
        assert_eq!(
            HiddenTable::FromEntries.qualified("trellis", 7),
            "\"trellis\".\"join1_7_r\""
        );
        for table in HiddenTable::ALL {
            assert!(
                !crate::defs::join::HiddenTable::ALL
                    .iter()
                    .any(|other| other.name(7) == table.name(7))
            );
        }
        assert_eq!(
            drop_hidden_tables_sql("trellis", 7),
            "drop table if exists \"trellis\".\"join1_7_r\", \"trellis\".\"join1_7_m\", \
             \"trellis\".\"join1_7_j\", \"trellis\".\"join1_7_l\"; \
             delete from \"trellis\".join1_fan_out where transform_id = 7"
        );
    }

    /// The widths a definition is checked against are the widths of the
    /// ledgers its DDL creates.
    #[test]
    fn the_checked_ledger_widths_are_the_widths_of_the_created_ledgers() {
        let def = parse(DEF).unwrap();
        let (from, joined) = sides();
        let created = crate::defs::ddl::declared_column_counts(&hidden_tables_ddl(
            "trellis", 7, &def, &from, &joined,
        ));
        let created = |name: &str| {
            created
                .iter()
                .find(|(table, _)| table.ends_with(name))
                .unwrap_or_else(|| panic!("{name} in {created:?}"))
                .1
        };
        assert_eq!(
            ledger_widths(&def),
            [
                (
                    "orders's hidden ledger table".to_string(),
                    created("join1_7_r\"")
                ),
                (
                    "customers's hidden ledger table".to_string(),
                    created("join1_7_m\"")
                ),
            ]
        );
        // The key, four ordering-state columns, then the three columns `a`
        // reads (cust, tenant, total).
        assert_eq!(ledger_widths(&def)[0].1, 1 + 4 + 3);
    }

    #[test]
    fn the_hidden_tables_ddl_holds_the_ledger_the_join_key_and_the_columns_read() {
        let def = parse(DEF).unwrap();
        let (from, joined) = sides();
        let ddl = hidden_tables_ddl("trellis", 7, &def, &from, &joined);
        // `R`: the ledger, the join key in `ON` order, then what the fields
        // read of `a`; both indexes the ledger's collection needs.
        assert!(
            ddl.contains(
                "create table \"trellis\".\"join1_7_r\" (\"__from_key\" text collate \"C\" \
                 primary key, \"__applied_lsn\" pg_lsn, \"__applied_seg\" bigint, \
                 \"__basis\" pg_snapshot, \"__tombstone\" boolean not null default false, \
                 \"cust\" integer, \"tenant\" integer, \"total\" numeric);\n\
                 create index on \"trellis\".\"join1_7_r\" (\"cust\", \"tenant\", \"__from_key\");\n\
                 create index on \"trellis\".\"join1_7_r\" (\"__applied_seg\") \
                 where \"__tombstone\";\n"
            ),
            "{ddl}"
        );
        assert!(
            ddl.contains(
                "\"join1_7_m\" (\"__from_key\" text collate \"C\" primary key, \
                 \"__applied_lsn\" pg_lsn, \"__applied_seg\" bigint, \"__basis\" pg_snapshot, \
                 \"__tombstone\" boolean not null default false, \"id\" integer, \
                 \"tenant\" integer, \"name\" text);\n\
                 create index on \"trellis\".\"join1_7_m\" (\"id\", \"tenant\");\n"
            ),
            "{ddl}"
        );
        // `J`: one row per join key, with no match count.
        assert!(
            ddl.contains(
                "create table \"trellis\".\"join1_7_j\" (\"cust\" integer, \"tenant\" integer, \
                 primary key (\"cust\", \"tenant\"));\n"
            ),
            "{ddl}"
        );
        assert!(
            ddl.contains("\"join1_7_l\" (id boolean primary key default true check (id))"),
            "{ddl}"
        );
        assert!(!ddl.contains("__matches"), "{ddl}");
    }

    #[test]
    fn the_target_is_keyed_by_the_from_tables_key_with_no_ledger() {
        let def = parse(DEF).unwrap();
        let (from, joined) = sides();
        let pk = vec![PrimaryKeyColumn {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            nullable: false,
            collation: None,
        }];
        let ddl = target_ddl(&def, "\"public\".\"v\"", &pk, &from, &joined).unwrap();
        // A copy of a column keeps its type (no modifier); an expression has
        // the type its inference gives.
        assert_eq!(
            ddl,
            "create table \"public\".\"v\" (\"id\" integer, \"who\" text, \
             \"doubled\" numeric, primary key (\"id\"))"
        );
    }

    #[test]
    fn a_unique_key_within_the_on_columns_makes_the_joined_side_unique() {
        let keys = vec![
            vec!["id".to_string()],
            vec!["a".to_string(), "b".to_string()],
        ];
        let on = |cols: &[&str]| cols.iter().map(|c| c.to_string()).collect::<Vec<_>>();
        assert!(key_is_unique(&keys, &on(&["id"])));
        assert!(
            key_is_unique(&keys, &on(&["id", "x"])),
            "a superset is unique"
        );
        assert!(key_is_unique(&keys, &on(&["b", "a"])));
        assert!(!key_is_unique(&keys, &on(&["a"])), "part of a composite");
        assert!(!key_is_unique(&keys, &on(&["x"])));
        assert!(!key_is_unique(&[], &on(&["id"])));
        assert!(
            !key_is_unique(&[vec![]], &on(&["id"])),
            "an empty key is no key"
        );
    }

    /// The key rule define, `RESUME` and the capture pass share, over the
    /// catalog: which of `b`'s indexes make its `ON` columns unique. A
    /// deferrable constraint, `NULLS NOT DISTINCT` and an `INCLUDE` column
    /// keep a key, as do `ON` columns that hold a key and more, in any order;
    /// an `INVALID`, partial or expression index (wholly or in part), part of
    /// a composite key, a non-unique index, and a table that doesn't exist
    /// don't.
    #[tokio::test]
    async fn the_key_rule_reads_which_indexes_make_the_on_columns_unique() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "create table public.b (bid int primary key, d int, n int, i int, v int, \
                     p int, e int, c1 int, c2 int, x int, m1 int, m2 int); \
                 alter table public.b add constraint b_d unique (d) deferrable initially deferred; \
                 create unique index b_n on public.b (n) nulls not distinct; \
                 create unique index b_i on public.b (i) include (v); \
                 create unique index b_p on public.b (p) where p is not null; \
                 create unique index b_e on public.b ((e + 0)); \
                 create unique index b_c on public.b (c1, c2); \
                 create index b_x on public.b (x); \
                 create unique index b_m on public.b (m1, (m2 + 0)); \
                 insert into public.b (bid, n, v) values (1, 1, 1), (2, 2, 1)",
            )
            .await
            .expect("create b");
        client
            .batch_execute("create unique index concurrently b_v on public.b (v)")
            .await
            .expect_err("v is duplicated, so the build leaves an INVALID index");
        let joined = |on: &str| {
            parse(&format!(
                "TRANSFORM t FROM a LEFT JOIN b ON {on} SELECT a.id AS id"
            ))
            .unwrap_or_else(|e| panic!("{on}: {e:?}"))
        };
        for (on, unique) in [
            ("a.k = b.bid", true),
            ("a.k = b.d", true),
            ("a.k = b.n", true),
            ("a.k = b.i", true),
            ("a.k = b.c1 AND a.j = b.c2", true),
            ("a.k = b.c2 AND a.j = b.c1", true),
            ("a.k = b.bid AND a.j = b.v", true),
            ("a.k = b.c1 AND a.j = b.x AND a.m = b.c2", true),
            ("a.k = b.v", false),
            ("a.k = b.p", false),
            ("a.k = b.e", false),
            ("a.k = b.c1", false),
            ("a.k = b.c1 AND a.j = b.x", false),
            ("a.k = b.x", false),
            ("a.k = b.m1 AND a.j = b.m2", false),
        ] {
            let def = joined(on);
            assert_eq!(
                joined_key_is_unique(&client, &def, "public.b")
                    .await
                    .expect("read the keys"),
                unique,
                "{on}"
            );
        }
        assert!(
            !joined_key_is_unique(&client, &joined("a.k = b.bid"), "public.nope")
                .await
                .expect("read the keys"),
            "a table that doesn't exist has no key"
        );
    }
}
