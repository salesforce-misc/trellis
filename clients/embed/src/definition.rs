//! A registered transform definition crosses as its summary fields, never as
//! [`Definition`] itself: that carries the parsed `TransformDef` AST and a
//! `HashMap<String, ValueType>`, neither of which a host can represent
//! without inventing semantics (ADR-0010 decision 4). An embedder who wants
//! the AST wants the Rust crate.
//!
//! `trellis` reports a definition in two shapes, and each flattens here on its
//! own: [`Definition`] (what `apply` returns for a `TRANSFORM` statement) has
//! the source columns but no creation time, and [`DefinitionSummary`] (what
//! `definitions()` lists) has the creation time but no source columns.

use std::collections::BTreeMap;

use trellis::{Definition, DefinitionSummary};

use crate::{epoch_micros, transform_status};

/// A [`Definition`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainDefinition {
    pub id: i64,
    /// The fully-qualified `schema.table` the definition writes.
    pub target_table: String,
    /// The fully-qualified `schema.table` the definition reads.
    pub source_table: String,
    pub source_version: i64,
    /// The status's `as_str()` word; see [`crate::transform_status`].
    pub status: &'static str,
    /// Each source column the definition was validated against, by name, with
    /// its type's name (`numeric`, `bigint`, `text`, `inet`, ...). Ordered by
    /// column name, so a host sees the same order on every call.
    pub source_columns: BTreeMap<String, String>,
}

impl From<&Definition> for PlainDefinition {
    fn from(definition: &Definition) -> Self {
        PlainDefinition {
            id: definition.id,
            target_table: definition.target_table.clone(),
            source_table: definition.source_table.clone(),
            source_version: definition.source_version,
            status: transform_status(definition.status),
            source_columns: definition
                .source_columns
                .iter()
                .map(|(name, value_type)| (name.clone(), value_type.to_string()))
                .collect(),
        }
    }
}

/// A [`DefinitionSummary`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainDefinitionSummary {
    pub id: i64,
    /// The fully-qualified `schema.table` the definition writes.
    pub target_table: String,
    /// The fully-qualified `schema.table` the definition reads.
    pub source_table: String,
    pub source_version: i64,
    /// The status's `as_str()` word; see [`crate::transform_status`].
    pub status: &'static str,
    /// When the definition was registered, as [`crate::epoch_micros`].
    pub created_at_micros: i64,
}

impl From<&DefinitionSummary> for PlainDefinitionSummary {
    fn from(summary: &DefinitionSummary) -> Self {
        PlainDefinitionSummary {
            id: summary.id,
            target_table: summary.target_table.clone(),
            source_table: summary.source_table.clone(),
            source_version: summary.source_version,
            status: transform_status(summary.status),
            created_at_micros: epoch_micros(summary.created_at),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, UNIX_EPOCH};

    use trellis::dev::defs::{PgType, ValueType, parse};
    use trellis::{FloatWidth, IntWidth, TransformStatus};

    use super::*;

    #[test]
    fn a_definition_crosses_as_its_summary_and_column_type_names() {
        let definition = Definition {
            id: 7,
            source_version: 3,
            def: parse("TRANSFORM order_totals FROM orders SELECT amount + tax AS total").unwrap(),
            source_columns: HashMap::from([
                ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
                ("amount".to_string(), ValueType::Numeric),
                ("tax".to_string(), ValueType::Float(FloatWidth::Float8)),
                ("note".to_string(), ValueType::Text),
                ("shipped".to_string(), ValueType::Boolean),
                ("buyer".to_string(), ValueType::Uuid),
                ("origin".to_string(), ValueType::Other(PgType::Inet)),
            ]),
            status: TransformStatus::Backfilling,
            source_table: "public.orders".to_string(),
            target_table: "public.order_totals".to_string(),
        };

        let plain = PlainDefinition::from(&definition);

        assert_eq!(
            plain,
            PlainDefinition {
                id: 7,
                target_table: "public.order_totals".to_string(),
                source_table: "public.orders".to_string(),
                source_version: 3,
                status: "backfilling",
                source_columns: BTreeMap::from([
                    ("amount".to_string(), "numeric".to_string()),
                    ("buyer".to_string(), "uuid".to_string()),
                    ("id".to_string(), "bigint".to_string()),
                    ("note".to_string(), "text".to_string()),
                    ("origin".to_string(), "inet".to_string()),
                    ("shipped".to_string(), "boolean".to_string()),
                    ("tax".to_string(), "double precision".to_string()),
                ]),
            }
        );
    }

    #[test]
    fn a_summary_crosses_with_its_creation_time_in_microseconds() {
        let summary = DefinitionSummary {
            id: 7,
            target_table: "public.order_totals".to_string(),
            source_table: "public.orders".to_string(),
            source_version: 3,
            status: TransformStatus::Live,
            created_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_123_456),
        };

        let plain = PlainDefinitionSummary::from(&summary);

        assert_eq!(
            plain,
            PlainDefinitionSummary {
                id: 7,
                target_table: "public.order_totals".to_string(),
                source_table: "public.orders".to_string(),
                source_version: 3,
                status: "live",
                created_at_micros: 1_727_222_400_123_456,
            }
        );
    }
}
