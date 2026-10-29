//! Define-time validation of a 1-1 `JOIN` definition (ADR-0021): the shared
//! `JOIN` rules (tables, `ON` terms and their key types), then the rules of a
//! `JOIN` that keeps one row per `FROM` row. Admitted only while the dev knob
//! is on ([`super::join_one_to_one::enabled`]); with it off the definition is
//! refused as a `JOIN` without a `GROUP BY` always was.
//!
//! This is the part that needs only the two tables' live columns. The
//! uniqueness of the joined table's `ON` columns and the refusals that read
//! the catalog (partitioned sides, a side that is another 1-1 target, a reader
//! of a 1-1 `JOIN` target) are the catalog's
//! ([`super::catalog`]'s `check_one_to_one_join`).

use std::collections::{HashMap, HashSet};

use super::ast::{Expr, KeySpace, QualifiedColumn, TransformDef};
use super::join::Side;
use super::validate::{
    JoinColumnInfo, JoinSide, RESERVED_COLUMN_PREFIX, ValidationError, is_deferred_aggregate_name,
    join_field_value_type, join_resolver, validate_join_tables_and_on,
};

/// Validates a [`KeySpace::JoinOneToOne`] definition against the live columns
/// of its two tables (`left` is the `FROM` table, `right` the joined one).
///
/// Admitted when the knob is on, and: the tables differ; every column is
/// qualified and exists; each `ON` term is an equality between one column of
/// each table, of one type and collation, on the join-key allowlist; no
/// column a hidden table copies is named like one of its ledger columns; and
/// each field is any expression over qualified columns of either table, with
/// no aggregate call, that types.
pub fn validate(
    def: &TransformDef,
    left: &HashMap<String, JoinColumnInfo>,
    right: &HashMap<String, JoinColumnInfo>,
) -> Result<(), ValidationError> {
    let KeySpace::JoinOneToOne(join) = &def.key_space else {
        return Ok(());
    };
    if !super::join_one_to_one::enabled() {
        return Err(ValidationError::OneToOneJoinDisabled);
    }
    validate_join_tables_and_on(def, join, left, right)?;
    let resolve = join_resolver(def, join, left, right);

    // The hidden tables copy these columns beside their ledger columns.
    for side in [Side::From, Side::Joined] {
        for column in super::join::side_columns(def, side) {
            if column.starts_with(RESERVED_COLUMN_PREFIX) {
                return Err(ValidationError::OneToOneJoinUnsupported {
                    what: format!("the column '{column}'"),
                    remedy: format!(
                        "a column starting with '{RESERVED_COLUMN_PREFIX}' would collide with \
                         the ledger columns Trellis keeps beside the columns it copies; read \
                         a column with another name"
                    ),
                });
            }
        }
    }

    let mut field_names = HashSet::with_capacity(def.fields.len());
    for field in &def.fields {
        if !field_names.insert(field.name.as_str()) {
            return Err(ValidationError::DuplicateFieldName {
                name: field.name.clone(),
            });
        }
        check_field_expr(&field.expr, &field.name, &resolve)?;
        // The field's type over both tables' columns, as define gives its
        // target column.
        join_field_value_type(def, field, left, right)?;
    }
    super::validate::check_join_collation_reads(def, join, left, right)?;
    Ok(())
}

/// Walks one field's expression: every column is qualified and resolves, and
/// no call is an aggregate. Every [`Expr`] node is named, so a new one is a
/// compile error here until its rule is decided.
fn check_field_expr<'a>(
    expr: &Expr,
    field: &str,
    resolve: &impl Fn(&QualifiedColumn) -> Result<(JoinSide, &'a JoinColumnInfo), ValidationError>,
) -> Result<(), ValidationError> {
    match expr {
        Expr::Column(column) => Err(ValidationError::JoinUnqualifiedColumn {
            field: field.to_string(),
            column: column.clone(),
        }),
        Expr::SourceColumn {
            schema,
            table,
            column,
        } => resolve(&QualifiedColumn {
            schema: schema.clone(),
            table: table.clone(),
            column: column.clone(),
        })
        .map(|_| ()),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => Ok(()),
        Expr::BinaryOp { lhs, rhs, .. } => {
            check_field_expr(lhs, field, resolve)?;
            check_field_expr(rhs, field, resolve)
        }
        Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => {
            check_field_expr(operand, field, resolve)
        }
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => expr
            .logic_children()
            .into_iter()
            .try_for_each(|child| check_field_expr(child, field, resolve)),
        Expr::FunctionCall { name, args } => {
            let upper = name.to_ascii_uppercase();
            if is_deferred_aggregate_name(&upper)
                || super::registry::AGGREGATE_FUNCTIONS.contains(&upper.as_str())
            {
                return Err(ValidationError::OneToOneJoinAggregate {
                    field: field.to_string(),
                    function: upper,
                });
            }
            args.iter()
                .try_for_each(|arg| check_field_expr(arg, field, resolve))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::join_one_to_one::{set_enabled, tests::KNOB_LOCK};
    use crate::defs::parse;
    use crate::defs::validate::validate_join;
    use crate::defs::{PgType, ValueType};
    use crate::integer::IntWidth;

    fn info(pg_type: &str, value_type: ValueType) -> JoinColumnInfo {
        JoinColumnInfo {
            value_type,
            pg_type: pg_type.to_string(),
            type_oid: match pg_type {
                "integer" => 23,
                _ => 25,
            },
            typmod: -1,
            collation_oid: if pg_type == "text" { 100 } else { 0 },
            collation: None,
            collation_deterministic: true,
            is_enum: false,
            copy_type: pg_type.to_string(),
            copy_collation: None,
            trellis_target: false,
        }
    }

    fn sides() -> (
        HashMap<String, JoinColumnInfo>,
        HashMap<String, JoinColumnInfo>,
    ) {
        let int = || info("integer", ValueType::Integer(IntWidth::Int4));
        (
            HashMap::from([
                ("cust".to_string(), int()),
                ("total".to_string(), int()),
                ("note".to_string(), info("text", ValueType::Text)),
                ("__basis".to_string(), int()),
            ]),
            HashMap::from([
                ("id".to_string(), int()),
                ("name".to_string(), info("text", ValueType::Text)),
                (
                    "when".to_string(),
                    info("date", ValueType::Other(PgType::Date)),
                ),
            ]),
        )
    }

    fn check(select: &str) -> Result<(), ValidationError> {
        let def = parse(&format!(
            "TRANSFORM v FROM orders LEFT JOIN customers ON orders.cust = customers.id \
             SELECT {select}"
        ))
        .unwrap();
        let (from, joined) = sides();
        validate_join(&def, &from, &joined)
    }

    #[test]
    fn the_knob_gates_a_one_to_one_join_at_validate() {
        let _lock = KNOB_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_enabled(false);
        assert_eq!(
            check("customers.name AS who"),
            Err(ValidationError::OneToOneJoinDisabled)
        );
        // An aggregate JOIN is not behind the knob.
        let def = parse(
            "TRANSFORM v FROM orders JOIN customers ON orders.cust = customers.id \
             GROUP BY orders.note SELECT orders.note AS note, COUNT(*) AS n",
        )
        .unwrap();
        let (from, joined) = sides();
        assert_eq!(validate_join(&def, &from, &joined), Ok(()));
        set_enabled(true);
        assert_eq!(check("customers.name AS who"), Ok(()));
        set_enabled(false);
    }

    #[test]
    fn a_field_is_any_expression_over_qualified_columns_of_either_table() {
        let _lock = KNOB_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_enabled(true);
        for select in [
            "customers.name AS who, orders.note AS note",
            "orders.total + customers.id AS s",
            "CASE WHEN customers.id IS NULL THEN 0 ELSE orders.total END AS c",
            "COALESCE(customers.name, orders.note) AS pick",
            "NULLIF(orders.note, customers.name) AS n",
            "orders.total <> customers.id AS differs",
            "NOT (orders.total = 1) AS neg",
            "CAST(orders.total AS bigint) AS t",
        ] {
            assert_eq!(check(select), Ok(()), "{select}");
        }
        set_enabled(false);
    }

    #[test]
    fn aggregates_unqualified_and_unknown_columns_are_named_refusals() {
        let _lock = KNOB_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_enabled(true);
        // The deferred families parse, so the validator names them.
        for select in ["MAX(customers.id) AS m", "BOOL_AND(orders.total) AS b"] {
            assert!(
                matches!(
                    check(select),
                    Err(ValidationError::OneToOneJoinAggregate { .. })
                ),
                "{select}"
            );
        }
        assert!(matches!(
            check("total AS t"),
            Err(ValidationError::JoinUnqualifiedColumn { .. })
        ));
        assert!(matches!(
            check("other.total AS t"),
            Err(ValidationError::JoinUnknownTable { .. })
        ));
        assert!(matches!(
            check("customers.missing AS t"),
            Err(ValidationError::JoinUnresolvedColumn { .. })
        ));
        assert!(matches!(
            check("customers.name AS x, orders.note AS x"),
            Err(ValidationError::DuplicateFieldName { .. })
        ));
        // A column named like a ledger column would collide in `R`.
        assert!(matches!(
            check("orders.__basis AS b"),
            Err(ValidationError::OneToOneJoinUnsupported { .. })
        ));
        // A field that doesn't type is refused by the inference.
        assert!(check("orders.total + customers.name AS bad").is_err());
        set_enabled(false);
    }

    #[test]
    fn a_nondeterministic_collation_under_a_text_function_is_refused() {
        let _lock = KNOB_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_enabled(true);
        let def = parse(
            "TRANSFORM v FROM orders LEFT JOIN customers ON orders.cust = customers.id \
             SELECT STRPOS(customers.name, 'a') AS pos, customers.name AS plain",
        )
        .unwrap();
        let (from, mut joined) = sides();
        assert_eq!(validate_join(&def, &from, &joined), Ok(()));
        joined.get_mut("name").unwrap().collation_deterministic = false;
        joined.get_mut("name").unwrap().collation = Some("ci".to_string());
        assert!(matches!(
            validate_join(&def, &from, &joined),
            Err(ValidationError::NondeterministicCollationTextFunction(refused))
                if refused.field == "pos" && refused.table == "customers"
        ));
        set_enabled(false);
    }
}
