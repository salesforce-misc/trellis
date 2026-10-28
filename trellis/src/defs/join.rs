//! A `JOIN` transform's two sides, and the tables Trellis keeps for one
//! (ADR-0019).
//!
//! A `JOIN` definition reads two tables: its `FROM` table
//! (`transform_definitions.source_table`) and its joined table
//! (`transform_definitions.join_table`). Self-joins are refused at define, so
//! the two have different bare names, and a qualified table's side is found
//! by its bare name alone ([`side_of_table`]).
//!
//! **Hidden tables.** Each `JOIN` definition owns five tables in the
//! instance's catalog schema, named by its definition id, never by its
//! target:
//!
//! - `P` ([`HiddenTable::FromPartials`]): the `FROM` side grouped by its
//!   `GROUP BY` columns and its join key, with the row count and, per
//!   aggregate argument on that side, a non-null count and a `numeric` sum,
//!   and the cell's basis and stamp (ADR-0002's group basis). A cell whose
//!   partials are all zero (`__zero`, a stored generated column, with a
//!   partial index on the cell identity) is collected as an aggregate's zero
//!   group is (`crate::staging::group_basis`'s "Zero groups");
//! - `M` ([`HiddenTable::JoinedPartials`]): the same for the joined side;
//! - `J` ([`HiddenTable::Matches`]): one row per join key, the key's lock,
//!   holding the joined side's row count under it;
//! - `T` ([`HiddenTable::TargetPartials`]): the target's groups with their
//!   joined partials: the row count, then a non-null count and a sum per
//!   argument, the `FROM` side's arguments first. It has no basis, so a
//!   group whose partials are all zero (`__zero`, indexed as `P`'s) is
//!   collected whatever the drained prefix (`crate::staging::group_basis`'s
//!   "Zero groups");
//! - the lock row ([`HiddenTable::TruncateLock`]): one row every writer of
//!   the pair takes `FOR SHARE` before any `J` row (ADR-0019's lock order),
//!   and a truncate of either side would take `FOR UPDATE`.
//!
//! They're fully derived: define creates them empty, `DROP TRANSFORM` drops
//! them, and a resume drops and recreates them. Between those, the columns
//! typed from a side follow that side's widenings as every column Trellis
//! copies does (`super::copies`): the staging worker's pass re-types them in
//! place when only the catalog changes, and pauses the definition when one
//! is outgrown. The visible target holds only the declared columns.
//! `crate::staging::join` writes them.

use std::collections::{BTreeSet, HashMap};

use super::ast::{Expr, JoinSpec, KeySpace, QualifiedColumn, TransformDef, ValueType};
use super::validate::{JoinColumnInfo, ValidationError};
use crate::integer::IntWidth;
use crate::pool::quote_ident;

/// Which table of a `JOIN` definition a column or table is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    /// The `FROM` table.
    From,
    /// The `JOIN` table.
    Joined,
}

/// `def`'s [`JoinSpec`], if it is a `JOIN` definition of either kind: an
/// aggregate JOIN ([`KeySpace::Join`]) or a 1-1 JOIN
/// ([`KeySpace::JoinOneToOne`]). Everything built on it (sides, join key,
/// columns captured) is right for both; [`is_grouped`] and [`is_one_to_one`]
/// tell them apart.
pub(crate) fn spec(def: &TransformDef) -> Option<&JoinSpec> {
    match &def.key_space {
        KeySpace::Join(spec) | KeySpace::JoinOneToOne(spec) => Some(spec),
        _ => None,
    }
}

/// Whether `def` is an aggregate (`GROUP BY`) JOIN.
pub(crate) fn is_grouped(def: &TransformDef) -> bool {
    matches!(def.key_space, KeySpace::Join(_))
}

/// Whether `def` is a 1-1 JOIN (ADR-0021).
pub(crate) fn is_one_to_one(def: &TransformDef) -> bool {
    matches!(def.key_space, KeySpace::JoinOneToOne(_))
}

/// The side a column written `<table>.<column>` belongs to, by `table`.
fn side_of_written(def: &TransformDef, join: &JoinSpec, table: &str) -> Option<Side> {
    if table == def.source {
        Some(Side::From)
    } else if table == join.right {
        Some(Side::Joined)
    } else {
        None
    }
}

/// The side the qualified `table` (`schema.table`) is of `def`, matched on
/// its bare name. `None` for a definition that isn't a `JOIN`, or a table
/// that is neither side.
pub(crate) fn side_of_table(def: &TransformDef, table: &str) -> Option<Side> {
    let join = spec(def)?;
    let bare = table.split_once('.').map_or(table, |(_, bare)| bare);
    side_of_written(def, join, bare)
}

/// `side`'s columns in the `ON` clause, in `ON` order: its join key.
pub(crate) fn join_key(def: &TransformDef, side: Side) -> Vec<String> {
    let Some(join) = spec(def) else {
        return Vec::new();
    };
    join.on
        .iter()
        .filter_map(|term| {
            [&term.lhs, &term.rhs]
                .into_iter()
                .find(|c| side_of_written(def, join, &c.table) == Some(side))
                .map(|c| c.column.clone())
        })
        .collect()
}

/// `side`'s `GROUP BY` columns, in the order written.
pub(crate) fn group_columns(def: &TransformDef, side: Side) -> Vec<String> {
    let Some(join) = spec(def) else {
        return Vec::new();
    };
    join.group_by
        .iter()
        .filter(|key| side_of_written(def, join, &key.table) == Some(side))
        .map(|key| key.column.clone())
        .collect()
}

/// Every column of `side` that `def` reads: its join key, its `GROUP BY`
/// columns and every column its fields read. Capture images these.
pub(crate) fn side_columns(def: &TransformDef, side: Side) -> BTreeSet<String> {
    let mut columns: BTreeSet<String> = join_key(def, side).into_iter().collect();
    columns.extend(group_columns(def, side));
    if let Some(join) = spec(def) {
        for field in &def.fields {
            walk_columns(&field.expr, &mut |column| {
                if side_of_written(def, join, &column.table) == Some(side) {
                    columns.insert(column.column.clone());
                }
            });
        }
    }
    columns
}

/// `def`'s `GROUP BY` keys in the order written, each with its side: the
/// visible target's and `T`'s group columns.
pub(crate) fn group_by_sides(def: &TransformDef) -> Vec<(Side, String)> {
    let Some(join) = spec(def) else {
        return Vec::new();
    };
    join.group_by
        .iter()
        .filter_map(|key| {
            side_of_written(def, join, &key.table).map(|side| (side, key.column.clone()))
        })
        .collect()
}

/// The columns `expr`, one of `def`'s field expressions, reads, each once
/// with its side, in the order first read.
pub(crate) fn expr_columns(def: &TransformDef, expr: &Expr) -> Vec<(Side, String)> {
    let Some(join) = spec(def) else {
        return Vec::new();
    };
    let mut columns = Vec::new();
    walk_columns(expr, &mut |c| {
        if let Some(side) = side_of_written(def, join, &c.table) {
            let read = (side, c.column.clone());
            if !columns.contains(&read) {
                columns.push(read);
            }
        }
    });
    columns
}

/// Whether `expr`, one of `def`'s field expressions, reads `side`'s
/// `column`.
pub(crate) fn expr_reads(def: &TransformDef, expr: &Expr, side: Side, column: &str) -> bool {
    let Some(join) = spec(def) else {
        return false;
    };
    let mut reads = false;
    walk_columns(expr, &mut |c| {
        reads |= c.column == column && side_of_written(def, join, &c.table) == Some(side);
    });
    reads
}

fn walk_columns(expr: &Expr, visit: &mut impl FnMut(&QualifiedColumn)) {
    match expr {
        Expr::SourceColumn {
            schema,
            table,
            column,
        } => visit(&QualifiedColumn {
            schema: schema.clone(),
            table: table.clone(),
            column: column.clone(),
        }),
        Expr::BinaryOp { lhs, rhs, .. } => {
            walk_columns(lhs, visit);
            walk_columns(rhs, visit);
        }
        Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => walk_columns(operand, visit),
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
            for child in expr.logic_children() {
                walk_columns(child, visit);
            }
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                walk_columns(arg, visit);
            }
        }
        Expr::Column(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => {}
    }
}

/// Whether `name` (uppercased) is an aggregate a `JOIN` admits.
fn is_aggregate(name: &str) -> bool {
    matches!(name, "COUNT" | "SUM" | "AVG")
}

/// The distinct aggregate arguments `def` reads on `side`, in the order its
/// fields first use them: the expressions `side`'s partials keep a non-null
/// count and a sum of.
pub(crate) fn side_arguments(def: &TransformDef, side: Side) -> Vec<Expr> {
    let Some(join) = spec(def) else {
        return Vec::new();
    };
    fn collect(expr: &Expr, def: &TransformDef, join: &JoinSpec, side: Side, out: &mut Vec<Expr>) {
        match expr {
            Expr::FunctionCall { name, args } if is_aggregate(&name.to_ascii_uppercase()) => {
                let [arg] = args.as_slice() else { return };
                let mut sides = BTreeSet::new();
                walk_columns(arg, &mut |column| {
                    if let Some(s) = side_of_written(def, join, &column.table) {
                        sides.insert(s == Side::From);
                    }
                });
                if sides == BTreeSet::from([side == Side::From]) && !out.contains(arg) {
                    out.push(arg.clone());
                }
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    collect(arg, def, join, side, out);
                }
            }
            Expr::BinaryOp { lhs, rhs, .. } => {
                collect(lhs, def, join, side, out);
                collect(rhs, def, join, side, out);
            }
            Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => {
                collect(operand, def, join, side, out);
            }
            Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
                for child in expr.logic_children() {
                    collect(child, def, join, side, out);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    for field in &def.fields {
        collect(&field.expr, def, join, side, &mut out);
    }
    out
}

/// One hidden table of a `JOIN` definition (this module's doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HiddenTable {
    /// `P`: the `FROM` side's partials.
    FromPartials,
    /// `M`: the joined side's partials.
    JoinedPartials,
    /// `J`: one row per join key.
    Matches,
    /// `T`: the target groups' joined partials.
    TargetPartials,
    /// The pair's one lock row.
    TruncateLock,
}

impl HiddenTable {
    pub(crate) const ALL: [HiddenTable; 5] = [
        HiddenTable::FromPartials,
        HiddenTable::JoinedPartials,
        HiddenTable::Matches,
        HiddenTable::TargetPartials,
        HiddenTable::TruncateLock,
    ];

    /// `side`'s partials: `P` for the `FROM` side, `M` for the joined one.
    pub(crate) fn partials(side: Side) -> Self {
        match side {
            Side::From => HiddenTable::FromPartials,
            Side::Joined => HiddenTable::JoinedPartials,
        }
    }

    fn part(self) -> &'static str {
        match self {
            HiddenTable::FromPartials => "p",
            HiddenTable::JoinedPartials => "m",
            HiddenTable::Matches => "j",
            HiddenTable::TargetPartials => "t",
            HiddenTable::TruncateLock => "l",
        }
    }

    /// The table's bare name in the catalog schema, for definition `id`.
    pub(crate) fn name(self, id: i64) -> String {
        format!("join_{id}_{}", self.part())
    }

    /// [`Self::name`] as SQL, over the definition id expression `id`.
    pub(crate) fn name_sql(self, id: &str) -> String {
        format!("('join_' || {id} || '_{}')", self.part())
    }
}

/// The `create table` column type of `info`: its type with no modifier,
/// schema-qualified, and its collation when that isn't the database default,
/// so a key compares in the hidden table exactly as it does in its source
/// ([`super::copy_type`]).
fn column_type(info: &JoinColumnInfo) -> String {
    match info.copy_collation.as_deref() {
        Some(collation) => format!("{} collate {collation}", info.copy_type),
        None => info.copy_type.clone(),
    }
}

/// The partial columns of a hidden table: the row count, then a non-null
/// count and a `numeric` sum per aggregate argument.
fn partial_columns(arguments: usize) -> Vec<String> {
    let mut columns = vec!["__n bigint not null default 0".to_string()];
    for i in 1..=arguments {
        columns.push(format!("__count_{i} bigint not null default 0"));
        columns.push(format!("__sum_{i} numeric not null default 0"));
    }
    columns
}

/// The `__zero` column of a hidden partials table with `arguments`
/// aggregate arguments: whether every partial is zero, as an aggregate's zero
/// group (`crate::staging::group_basis`'s "Zero groups"). Stored and
/// generated, so every writer keeps it current; the tombstone pass collects
/// zero rows through a partial index `where __zero` on the table's identity.
fn zero_column(arguments: usize) -> String {
    let mut zero = vec!["__n = 0".to_string()];
    for i in 1..=arguments {
        zero.push(format!("__count_{i} = 0"));
        zero.push(format!("__sum_{i} = 0"));
    }
    format!(
        "__zero boolean not null generated always as ({}) stored",
        zero.join(" and ")
    )
}

/// `side`'s key columns in its hidden partials table, in the order of the
/// table's cell identity: its join key columns, then its `GROUP BY` columns
/// not already among them. The join key leads so the table's identity index
/// serves both a `self_check` page's range scan and the other
/// side's lookup of a key's cells.
pub(crate) fn side_key_columns(def: &TransformDef, side: Side) -> Vec<String> {
    let mut columns = join_key(def, side);
    for column in group_columns(def, side) {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    columns
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
    let schema = quote_ident(catalog_schema);
    let typed = |columns: &HashMap<String, JoinColumnInfo>, name: &String| {
        let pg_type = columns
            .get(name)
            .map_or_else(|| "text".to_string(), column_type);
        format!("{} {pg_type}", quote_ident(name))
    };
    let mut sql = String::new();
    for (table, side, columns) in [
        (HiddenTable::FromPartials, Side::From, from),
        (HiddenTable::JoinedPartials, Side::Joined, joined),
    ] {
        let keys = side_key_columns(def, side);
        let arguments = side_arguments(def, side).len();
        let mut body: Vec<String> = keys.iter().map(|c| typed(columns, c)).collect();
        body.extend(partial_columns(arguments));
        body.push("__basis pg_snapshot".to_string());
        body.push("__stamp bigint not null default 0".to_string());
        body.push(zero_column(arguments));
        // The cell identity is the key columns in order, the join key first
        // (`side_key_columns`).
        let identity: Vec<String> = keys.iter().map(|c| quote_ident(c)).collect();
        let identity = identity.join(", ");
        body.push(format!("unique nulls not distinct ({identity})"));
        let name = format!("{schema}.{}", quote_ident(&table.name(id)));
        sql.push_str(&format!(
            "create table {name} ({});\n\
             create index on {name} ({identity}) where __zero;\n",
            body.join(", ")
        ));
    }
    // `J` keys on the `FROM` side's join key columns; each pair has one type
    // (`text` and `varchar` are one) and collation (define checks it), and a
    // copy has no modifier, so either side's would do.
    let key = join_key(def, Side::From);
    let mut body: Vec<String> = key.iter().map(|c| typed(from, c)).collect();
    body.push("__matches bigint not null default 0".to_string());
    let quoted: Vec<String> = key.iter().map(|c| quote_ident(c)).collect();
    body.push(format!("primary key ({})", quoted.join(", ")));
    sql.push_str(&format!(
        "create table {schema}.{} ({});\n",
        quote_ident(&HiddenTable::Matches.name(id)),
        body.join(", ")
    ));
    // `T`: the target's group columns, each typed as its side's column.
    if let Some(join) = spec(def) {
        let mut body: Vec<String> = join
            .group_by
            .iter()
            .map(|key| match side_of_written(def, join, &key.table) {
                Some(Side::Joined) => typed(joined, &key.column),
                _ => typed(from, &key.column),
            })
            .collect();
        let arguments =
            side_arguments(def, Side::From).len() + side_arguments(def, Side::Joined).len();
        body.extend(partial_columns(arguments));
        body.push(zero_column(arguments));
        let groups: Vec<String> = join
            .group_by
            .iter()
            .map(|k| quote_ident(&k.column))
            .collect();
        let groups = groups.join(", ");
        body.push(format!("unique nulls not distinct ({groups})"));
        let name = format!(
            "{schema}.{}",
            quote_ident(&HiddenTable::TargetPartials.name(id))
        );
        sql.push_str(&format!(
            "create table {name} ({});\n\
             create index on {name} ({groups}) where __zero;\n",
            body.join(", ")
        ));
    }
    let lock = format!(
        "{schema}.{}",
        quote_ident(&HiddenTable::TruncateLock.name(id))
    );
    sql.push_str(&format!(
        "create table {lock} (id boolean primary key default true check (id));\n\
         insert into {lock} default values;\n"
    ));
    sql
}

/// The statement dropping every hidden table of definition `id` from
/// `catalog_schema`, any that is already gone included.
pub(crate) fn drop_hidden_tables_sql(catalog_schema: &str, id: i64) -> String {
    let schema = quote_ident(catalog_schema);
    let tables: Vec<String> = HiddenTable::ALL
        .iter()
        .map(|table| format!("{schema}.{}", quote_ident(&table.name(id))))
        .collect();
    format!("drop table if exists {}", tables.join(", "))
}

/// The value type of `arg`, an aggregate argument over one side's columns.
fn argument_type(
    def: &TransformDef,
    arg: &Expr,
    from: &HashMap<String, JoinColumnInfo>,
    joined: &HashMap<String, JoinColumnInfo>,
) -> Result<ValueType, ValidationError> {
    let mut side = Side::From;
    if let Some(join) = spec(def) {
        walk_columns(arg, &mut |column| {
            if let Some(s) = side_of_written(def, join, &column.table) {
                side = s;
            }
        });
    }
    let columns = match side {
        Side::From => from,
        Side::Joined => joined,
    };
    let types: HashMap<String, ValueType> = columns
        .iter()
        .map(|(name, info)| (name.clone(), info.value_type))
        .collect();
    super::validate::infer_expr_type(arg, &types)
}

/// The Postgres type of a `JOIN` field that isn't a `GROUP BY` key: an
/// aggregate's result type as Postgres gives it (`count` and `sum` over
/// `smallint`/`integer` are `bigint`, `sum` over `bigint`/`numeric` and
/// `avg` are `numeric`), and a composed field's inferred type.
pub(crate) fn field_pg_type(
    def: &TransformDef,
    field: &super::ast::FieldDef,
    from: &HashMap<String, JoinColumnInfo>,
    joined: &HashMap<String, JoinColumnInfo>,
) -> Result<String, ValidationError> {
    if let Expr::FunctionCall { name, args } = &field.expr {
        match (name.to_ascii_uppercase().as_str(), args.as_slice()) {
            ("COUNT", _) => return Ok("bigint".to_string()),
            ("AVG", _) => return Ok("numeric".to_string()),
            ("SUM", [arg]) => {
                let narrow = matches!(
                    argument_type(def, arg, from, joined)?,
                    ValueType::Integer(IntWidth::Int2 | IntWidth::Int4)
                );
                return Ok(if narrow { "bigint" } else { "numeric" }.to_string());
            }
            _ => {}
        }
    }
    let value_type = super::validate::join_field_value_type(def, field, from, joined)?;
    Ok(super::ddl::pg_type_name(value_type).into_owned())
}

/// Renders the `create table` statement of a `JOIN` definition's visible
/// target, `qualified_target` (already quoted): one column per `GROUP BY`
/// key, typed as its source column, then one per other field, and the keys'
/// `unique nulls not distinct` group identity. No hidden columns: the
/// partials live in the hidden tables.
pub(crate) fn target_ddl(
    def: &TransformDef,
    qualified_target: &str,
    from: &HashMap<String, JoinColumnInfo>,
    joined: &HashMap<String, JoinColumnInfo>,
) -> Result<String, ValidationError> {
    let Some(join) = spec(def) else {
        panic!("join::target_ddl called on a definition that isn't a JOIN");
    };
    let mut columns = Vec::with_capacity(join.group_by.len() + def.fields.len());
    let mut keys = Vec::with_capacity(join.group_by.len());
    for key in &join.group_by {
        let side_columns = match side_of_written(def, join, &key.table) {
            Some(Side::Joined) => joined,
            _ => from,
        };
        // With its collation, as the hidden tables carry it, so the two
        // compare a group's values alike.
        let pg_type = side_columns
            .get(&key.column)
            .map_or_else(|| "text".to_string(), column_type);
        columns.push(format!("{} {pg_type}", quote_ident(&key.column)));
        keys.push(quote_ident(&key.column));
    }
    for field in &def.fields {
        if join.group_by.iter().any(|key| key.column == field.name) {
            continue;
        }
        let pg_type = field_pg_type(def, field, from, joined)?;
        columns.push(format!("{} {pg_type}", quote_ident(&field.name)));
    }
    columns.push(format!("unique nulls not distinct ({})", keys.join(", ")));
    Ok(format!(
        "create table {qualified_target} ({})",
        columns.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::parse;

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

    const DEF: &str = "TRANSFORM by_region FROM orders LEFT JOIN items \
        ON orders.id = items.order_id AND orders.tenant = items.tenant \
        GROUP BY orders.region, items.sku \
        SELECT orders.region AS region, items.sku AS sku, COUNT(*) AS n, \
               SUM(items.qty) AS qty, AVG(orders.total) AS avg_total, \
               COALESCE(SUM(items.qty + 1), 0) AS composed, COUNT(items.qty) AS nq";

    #[test]
    fn sides_are_found_by_bare_name() {
        let def = parse(DEF).unwrap();
        assert_eq!(side_of_table(&def, "public.orders"), Some(Side::From));
        assert_eq!(side_of_table(&def, "shop.items"), Some(Side::Joined));
        assert_eq!(side_of_table(&def, "public.other"), None);
        let plain = parse("TRANSFORM t FROM orders SELECT id AS id").unwrap();
        assert_eq!(side_of_table(&plain, "public.orders"), None);
    }

    #[test]
    fn each_side_reads_its_key_group_and_argument_columns() {
        let def = parse(DEF).unwrap();
        assert_eq!(join_key(&def, Side::From), ["id", "tenant"]);
        assert_eq!(join_key(&def, Side::Joined), ["order_id", "tenant"]);
        assert_eq!(
            side_columns(&def, Side::From),
            BTreeSet::from(["id", "tenant", "region", "total"].map(String::from))
        );
        assert_eq!(
            side_columns(&def, Side::Joined),
            BTreeSet::from(["order_id", "tenant", "sku", "qty"].map(String::from))
        );
        // `items.qty` serves `SUM` and `COUNT`; `items.qty + 1` is a second
        // argument.
        assert_eq!(side_arguments(&def, Side::Joined).len(), 2);
        assert_eq!(side_arguments(&def, Side::From).len(), 1);
    }

    #[test]
    fn hidden_tables_are_named_by_definition_id() {
        let def = parse(DEF).unwrap();
        let from = HashMap::from([
            (
                "id".to_string(),
                info("integer", ValueType::Integer(IntWidth::Int4)),
            ),
            (
                "tenant".to_string(),
                info("integer", ValueType::Integer(IntWidth::Int4)),
            ),
            ("region".to_string(), info("text", ValueType::Text)),
            (
                "total".to_string(),
                info("numeric(10,2)", ValueType::Numeric),
            ),
        ]);
        let joined = HashMap::from([
            (
                "order_id".to_string(),
                info("integer", ValueType::Integer(IntWidth::Int4)),
            ),
            (
                "tenant".to_string(),
                info("integer", ValueType::Integer(IntWidth::Int4)),
            ),
            ("sku".to_string(), info("text", ValueType::Text)),
            (
                "qty".to_string(),
                info("integer", ValueType::Integer(IntWidth::Int4)),
            ),
        ]);
        let ddl = hidden_tables_ddl("trellis", 7, &def, &from, &joined);
        assert!(ddl.contains("create table \"trellis\".\"join_7_p\" (\"id\" integer, \"tenant\" integer, \"region\" text, __n bigint"), "{ddl}");
        assert!(
            ddl.contains("\"join_7_m\" (\"order_id\" integer, \"tenant\" integer, \"sku\" text"),
            "{ddl}"
        );
        assert!(ddl.contains("\"join_7_j\" (\"id\" integer, \"tenant\" integer, __matches bigint not null default 0, primary key (\"id\", \"tenant\"))"), "{ddl}");
        // The join key leads each side's cell identity. A zero cell is every
        // partial zero, with a partial index on the cell identity for the
        // zero-cell collection.
        assert!(
            ddl.contains(
                "__stamp bigint not null default 0, __zero boolean not null generated always as \
                 (__n = 0 and __count_1 = 0 and __sum_1 = 0) stored, \
                 unique nulls not distinct (\"id\", \"tenant\", \"region\"));\n\
                 create index on \"trellis\".\"join_7_p\" (\"id\", \"tenant\", \"region\") \
                 where __zero;\n"
            ),
            "{ddl}"
        );
        // `T`: the group columns, then the `FROM` side's one argument and the
        // joined side's two, and its zero groups' column and partial index.
        assert!(
            ddl.contains(
                "\"join_7_t\" (\"region\" text, \"sku\" text, __n bigint not null default 0, \
                 __count_1 bigint not null default 0, __sum_1 numeric not null default 0, \
                 __count_2 bigint not null default 0, __sum_2 numeric not null default 0, \
                 __count_3 bigint not null default 0, __sum_3 numeric not null default 0, \
                 __zero boolean not null generated always as (__n = 0 and __count_1 = 0 \
                 and __sum_1 = 0 and __count_2 = 0 and __sum_2 = 0 and __count_3 = 0 \
                 and __sum_3 = 0) stored, \
                 unique nulls not distinct (\"region\", \"sku\"));\n\
                 create index on \"trellis\".\"join_7_t\" (\"region\", \"sku\") where __zero;\n"
            ),
            "{ddl}"
        );
        assert!(
            ddl.contains("\"join_7_l\" (id boolean primary key default true check (id))"),
            "{ddl}"
        );
        assert_eq!(
            drop_hidden_tables_sql("trellis", 7),
            "drop table if exists \"trellis\".\"join_7_p\", \"trellis\".\"join_7_m\", \
             \"trellis\".\"join_7_j\", \"trellis\".\"join_7_t\", \"trellis\".\"join_7_l\""
        );

        let target = target_ddl(&def, "\"public\".\"by_region\"", &from, &joined).unwrap();
        assert_eq!(
            target,
            "create table \"public\".\"by_region\" (\"region\" text, \"sku\" text, \
             \"n\" bigint, \"qty\" bigint, \"avg_total\" numeric, \"composed\" bigint, \
             \"nq\" bigint, unique nulls not distinct (\"region\", \"sku\"))"
        );
    }

    const ONE_TO_ONE: &str = "TRANSFORM v FROM orders LEFT JOIN items \
        ON orders.id = items.order_id SELECT items.sku AS sku, orders.total * 2 AS doubled";

    /// A 1-1 JOIN reads the same two tables an aggregate JOIN does, so the
    /// helpers built on `spec` serve it, and the two predicates tell the
    /// kinds apart.
    #[test]
    fn a_one_to_one_join_has_a_spec_sides_and_columns_but_is_not_grouped() {
        let def = parse(ONE_TO_ONE).unwrap();
        assert!(is_one_to_one(&def) && !is_grouped(&def));
        assert!(spec(&def).is_some_and(|join| join.group_by.is_empty()));
        assert_eq!(side_of_table(&def, "public.orders"), Some(Side::From));
        assert_eq!(side_of_table(&def, "shop.items"), Some(Side::Joined));
        assert_eq!(join_key(&def, Side::Joined), ["order_id"]);
        assert_eq!(
            side_columns(&def, Side::From),
            BTreeSet::from(["id", "total"].map(String::from))
        );
        assert_eq!(
            side_columns(&def, Side::Joined),
            BTreeSet::from(["order_id", "sku"].map(String::from))
        );
        let grouped = parse(DEF).unwrap();
        assert!(is_grouped(&grouped) && !is_one_to_one(&grouped));
        // Only an aggregate JOIN takes the aggregate JOIN apply.
        assert!(!crate::staging::join::applies(&def));
        assert!(crate::staging::join::applies(&grouped));
    }
}
