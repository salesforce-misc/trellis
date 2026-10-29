//! 1-1 `JOIN` transforms: the tables Trellis keeps for one, and the dev knob
//! that admits them (ADR-0021).
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
//!   read. Indexed on the join key (how a `b` change finds its rows) and by
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
//! **The dev knob.** The whole feature is admitted only while
//! [`enabled`]: `TRELLIS_DEV_ONE_TO_ONE_JOIN=1`, or
//! `trellis::dev::defs::set_one_to_one_join(true)` before a client starts.
//! The gate is at define (`super::validate_join_one_to_one`), never at parse,
//! so a definition stored while the knob was on still loads with it off.
//! The `FROM` side's page and a one-transaction build run
//! (`crate::staging::join_one_to_one`); a change of the joined side, a
//! `TRUNCATE` of either side and a schema change halt the transform with the
//! reason rather than run wrong.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};

use tokio_postgres::GenericClient;

use super::ast::{Expr, FieldDef, TransformDef};
use super::ddl::PrimaryKeyColumn;
use super::join::{Side, join_key, side_columns};
use super::validate::{JoinColumnInfo, ValidationError};
use crate::pool::quote_ident;

/// The environment variable that turns the knob on (`1`, `true` or `on`)
/// when nothing has set it: see the module doc.
pub const KNOB_ENV: &str = "TRELLIS_DEV_ONE_TO_ONE_JOIN";

/// 0: not read yet; 1: off; 2: on.
static KNOB: AtomicU8 = AtomicU8::new(0);

/// Whether 1-1 `JOIN` transforms are admitted (see the module doc).
pub fn enabled() -> bool {
    match KNOB.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var(KNOB_ENV)
                .is_ok_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "on"));
            KNOB.store(if on { 2 } else { 1 }, Ordering::Relaxed);
            on
        }
    }
}

/// Turns the knob on or off for this process. Only before any client starts:
/// the stored definitions and the staging worker read it at their own pace.
#[cfg_attr(not(any(test, feature = "test-util")), allow(dead_code))]
pub fn set_enabled(on: bool) {
    KNOB.store(if on { 2 } else { 1 }, Ordering::Relaxed);
}

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

/// A column's `create table` type: its type with no modifier, qualified, and
/// its collation when that isn't the database default, so a value compares in
/// a hidden table exactly as it does in its source.
fn column_type(info: &JoinColumnInfo) -> String {
    match info.copy_collation.as_deref() {
        Some(collation) => format!("{} collate {collation}", info.copy_type),
        None => info.copy_type.clone(),
    }
}

fn typed(columns: &HashMap<String, JoinColumnInfo>, name: &str) -> String {
    let pg_type = columns
        .get(name)
        .map_or_else(|| "text".to_string(), column_type);
    format!("{} {pg_type}", quote_ident(name))
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
        let mut read: Vec<String> = key.clone();
        read.extend(
            side_columns(def, side)
                .into_iter()
                .filter(|column| !key.contains(column)),
        );
        let mut body = vec![format!(
            "{} text collate \"C\" primary key",
            quote_ident(super::ledger::KEY_COLUMN)
        )];
        body.push(super::ledger::ordering_state_columns());
        body.extend(read.iter().map(|column| typed(columns, column)));
        let quoted_key: Vec<String> = key.iter().map(|c| quote_ident(c)).collect();
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
    // `J` keys on the `FROM` side's join key columns; each pair has one type
    // and collation (define checks it), and a copy has no modifier, so either
    // side's would do.
    let key = join_key(def, Side::From);
    let mut body: Vec<String> = key.iter().map(|c| typed(from, c)).collect();
    let quoted: Vec<String> = key.iter().map(|c| quote_ident(c)).collect();
    body.push(format!("primary key ({})", quoted.join(", ")));
    sql.push_str(&format!(
        "create table {} ({});\n",
        HiddenTable::Keys.qualified(catalog_schema, id),
        body.join(", ")
    ));
    let lock = HiddenTable::TruncateLock.qualified(catalog_schema, id);
    sql.push_str(&format!(
        "create table {lock} (id boolean primary key default true check (id));\n\
         insert into {lock} default values;\n"
    ));
    sql
}

/// The statement dropping every hidden table of definition `id` from
/// `catalog_schema`, any that is already gone included.
pub(crate) fn drop_hidden_tables_sql(catalog_schema: &str, id: i64) -> String {
    let tables: Vec<String> = HiddenTable::ALL
        .iter()
        .map(|table| table.qualified(catalog_schema, id))
        .collect();
    format!("drop table if exists {}", tables.join(", "))
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

/// The joined table's `ON` columns of `def`, in `ON` order.
pub(crate) fn joined_on_columns(def: &TransformDef) -> Vec<String> {
    join_key(def, Side::Joined)
}

/// The key space of the definition whose target is `qualified_table`, if one
/// is: the definition is read back from its stored text.
async fn target_key_space(
    client: &impl GenericClient,
    qualified_table: &str,
) -> Result<Option<super::ast::KeySpace>, super::catalog::CatalogError> {
    let text: Option<String> = client
        .query_opt(
            "select definition_text from transform_definitions where target_table = $1",
            &[&qualified_table],
        )
        .await?
        .map(|row| row.get(0));
    Ok(text
        .and_then(|text| super::parser::parse(&text).ok())
        .map(|def| def.key_space))
}

/// Refuses a definition that reads `tables` (qualified) when one is the
/// target of a 1-1 `JOIN`: no transform may read one in v1
/// ([`ValidationError::OneToOneJoinTargetRead`]). Every definition's define
/// makes the check, whatever its own key space.
pub(crate) async fn reject_reader_of_target(
    client: &impl GenericClient,
    tables: &[&str],
) -> Result<(), super::catalog::CatalogError> {
    for table in tables {
        if let Some(super::ast::KeySpace::JoinOneToOne(_)) = target_key_space(client, table).await?
        {
            return Err(ValidationError::OneToOneJoinTargetRead {
                table: (*table).to_string(),
            }
            .into());
        }
    }
    Ok(())
}

/// The define-time checks of a 1-1 `JOIN` that read the catalog, once its two
/// tables are resolved (`from` and `joined`, qualified): the joined table's
/// `ON` columns are unique, and a side that is another transform's 1-1 target
/// is joined on its primary key.
pub(crate) async fn check_define(
    client: &impl GenericClient,
    def: &TransformDef,
    from: &str,
    joined: &str,
) -> Result<(), super::catalog::CatalogError> {
    let on = joined_on_columns(def);
    if !key_is_unique(&unique_keys_of(client, joined).await?, &on) {
        return Err(ValidationError::OneToOneJoinKeyNotUnique {
            table: joined.to_string(),
            columns: on,
        }
        .into());
    }
    for (side, table) in [(Side::From, from), (Side::Joined, joined)] {
        if !matches!(
            target_key_space(client, table).await?,
            Some(super::ast::KeySpace::OneToOne)
        ) {
            continue;
        }
        let mut on_side = join_key(def, side);
        on_side.sort();
        let mut pk: Vec<String> = super::ddl::identity_key_columns(client, table)
            .await?
            .into_iter()
            .map(|c| c.name)
            .collect();
        pk.sort();
        if on_side != pk {
            return Err(ValidationError::OneToOneJoinUnsupported {
                what: format!(
                    "'{table}', the 1-1 target of another transform, joined on columns other \
                     than its primary key ({})",
                    pk.join(", ")
                ),
                remedy: "join on its whole primary key, or join the table that transform reads"
                    .to_string(),
            }
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::defs::ast::ValueType;
    use crate::defs::parse;
    use crate::integer::IntWidth;

    /// Tests that set the knob take this, so they don't see each other's.
    pub(crate) static KNOB_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
             \"trellis\".\"join1_7_j\", \"trellis\".\"join1_7_l\""
        );
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
                 create index on \"trellis\".\"join1_7_r\" (\"cust\", \"tenant\");\n\
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

    #[test]
    fn the_knob_is_settable() {
        let _lock = KNOB_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_enabled(true);
        assert!(enabled());
        set_enabled(false);
        assert!(!enabled());
    }
}
