//! A relationship declaration crosses as its plain fields. `trellis` reports
//! one in two shapes, flattened here on their own like a definition's:
//! [`RelationshipDefinition`] (what `apply` returns for a `RELATIONSHIP`
//! statement) carries creation-time warnings but no creation time, and
//! [`RelationshipSummary`] (what `relationships()` lists) has the creation
//! time but no warnings.
//!
//! The cardinality crosses as [`RelationshipCardinality::as_str`]'s word,
//! `one` or `many`, which a host turns into an atom or symbol from the closed
//! set [`relationship_cardinality_names`] lists, never from the database
//! value itself.

use trellis::{ErrorCode, RelationshipCardinality, RelationshipDefinition, RelationshipSummary};

use crate::{PlainError, epoch_micros};

/// Every word a relationship's `cardinality` can cross as: the set a host
/// allocates its names from at load time.
pub fn relationship_cardinality_names() -> Vec<&'static str> {
    [
        RelationshipCardinality::ToOne,
        RelationshipCardinality::ToMany,
    ]
    .into_iter()
    .map(RelationshipCardinality::as_str)
    .collect()
}

/// A [`RelationshipDefinition`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainRelationship {
    pub id: i64,
    pub name: String,
    /// The schema the from-side table resolved to when it was declared.
    pub from_schema: String,
    pub from_table: String,
    pub from_col: String,
    /// The schema the to-side table resolved to when it was declared.
    pub to_schema: String,
    pub to_table: String,
    pub to_col: String,
    /// `one` or `many`; see [`relationship_cardinality_names`].
    pub cardinality: &'static str,
    /// Each non-fatal caveat the declaration raised (a missing from-side
    /// index, say), as its message.
    pub warnings: Vec<String>,
}

impl From<&RelationshipDefinition> for PlainRelationship {
    fn from(relationship: &RelationshipDefinition) -> Self {
        let def = &relationship.def;
        PlainRelationship {
            id: relationship.id,
            name: def.name.clone(),
            from_schema: relationship.from_schema.clone(),
            from_table: def.from_table.clone(),
            from_col: def.from_col.clone(),
            to_schema: relationship.to_schema.clone(),
            to_table: def.to_table.clone(),
            to_col: def.to_col.clone(),
            cardinality: relationship.cardinality.as_str(),
            warnings: relationship
                .warnings
                .iter()
                .map(ToString::to_string)
                .collect(),
        }
    }
}

/// A [`RelationshipSummary`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainRelationshipSummary {
    pub id: i64,
    pub name: String,
    /// The schema the from-side table resolved to when it was declared.
    pub from_schema: String,
    pub from_table: String,
    pub from_col: String,
    /// The schema the to-side table resolved to when it was declared.
    pub to_schema: String,
    pub to_table: String,
    pub to_col: String,
    /// `one` or `many`; see [`relationship_cardinality_names`].
    pub cardinality: &'static str,
    /// When the relationship was declared, as [`crate::epoch_micros`].
    pub created_at_micros: i64,
}

impl TryFrom<&RelationshipSummary> for PlainRelationshipSummary {
    type Error = PlainError;

    /// An `internal` error if the persisted cardinality is neither word
    /// `trellis` writes, rather than handing a host a word outside the set it
    /// allocated.
    fn try_from(summary: &RelationshipSummary) -> Result<Self, PlainError> {
        let cardinality = RelationshipCardinality::from_persisted(&summary.cardinality)
            .ok_or_else(|| {
                PlainError::new(
                    ErrorCode::Internal,
                    format!(
                        "relationship {:?} has the unrecognized cardinality {:?}",
                        summary.name, summary.cardinality
                    ),
                )
            })?;
        Ok(PlainRelationshipSummary {
            id: summary.id,
            name: summary.name.clone(),
            from_schema: summary.from_schema.clone(),
            from_table: summary.from_table.clone(),
            from_col: summary.from_col.clone(),
            to_schema: summary.to_schema.clone(),
            to_table: summary.to_table.clone(),
            to_col: summary.to_col.clone(),
            cardinality: cardinality.as_str(),
            created_at_micros: epoch_micros(summary.created_at),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use trellis::dev::defs::ast::RelationshipDef;

    use super::*;

    /// Exhaustive on purpose: a cardinality `trellis` adds fails to compile
    /// here until it is in the load-time set.
    #[test]
    fn every_cardinality_is_in_the_load_time_set() {
        let names = relationship_cardinality_names();
        for cardinality in [
            RelationshipCardinality::ToOne,
            RelationshipCardinality::ToMany,
        ] {
            match cardinality {
                RelationshipCardinality::ToOne | RelationshipCardinality::ToMany => {
                    assert!(names.contains(&cardinality.as_str()));
                }
            }
        }
        assert_eq!(names, ["one", "many"]);
    }

    #[test]
    fn a_relationship_definition_crosses_as_its_fields() {
        let relationship = RelationshipDefinition {
            id: 3,
            from_schema: "shop".to_string(),
            to_schema: "public".to_string(),
            def: RelationshipDef {
                name: "owner".to_string(),
                from_table: "widgets".to_string(),
                from_col: "owner_id".to_string(),
                to_table: "users".to_string(),
                to_col: "id".to_string(),
            },
            cardinality: RelationshipCardinality::ToOne,
            warnings: Vec::new(),
        };

        assert_eq!(
            PlainRelationship::from(&relationship),
            PlainRelationship {
                id: 3,
                name: "owner".to_string(),
                from_schema: "shop".to_string(),
                from_table: "widgets".to_string(),
                from_col: "owner_id".to_string(),
                to_schema: "public".to_string(),
                to_table: "users".to_string(),
                to_col: "id".to_string(),
                cardinality: "one",
                warnings: Vec::new(),
            }
        );
    }

    fn summary(cardinality: &str) -> RelationshipSummary {
        RelationshipSummary {
            id: 3,
            name: "orders".to_string(),
            from_schema: "public".to_string(),
            from_table: "users".to_string(),
            from_col: "id".to_string(),
            to_schema: "public".to_string(),
            to_table: "orders".to_string(),
            to_col: "user_id".to_string(),
            cardinality: cardinality.to_string(),
            created_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_123_456),
        }
    }

    #[test]
    fn a_summary_crosses_with_its_creation_time_in_microseconds() {
        assert_eq!(
            PlainRelationshipSummary::try_from(&summary("many")).unwrap(),
            PlainRelationshipSummary {
                id: 3,
                name: "orders".to_string(),
                from_schema: "public".to_string(),
                from_table: "users".to_string(),
                from_col: "id".to_string(),
                to_schema: "public".to_string(),
                to_table: "orders".to_string(),
                to_col: "user_id".to_string(),
                cardinality: "many",
                created_at_micros: 1_727_222_400_123_456,
            }
        );
    }

    #[test]
    fn an_unrecognized_persisted_cardinality_is_internal() {
        let err = PlainRelationshipSummary::try_from(&summary("to_many")).unwrap_err();
        assert_eq!(err.code, "internal");
        assert!(err.message.contains("\"to_many\""), "{}", err.message);
    }
}
