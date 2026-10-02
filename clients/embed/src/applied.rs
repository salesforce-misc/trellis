//! What `apply` did crosses as a [`PlainApplied`]: one variant per
//! [`Applied`] variant, each carrying only plain data.
//!
//! [`Applied`] is `#[non_exhaustive]`, so the flattening has a fallback,
//! [`PlainApplied::Unknown`], for an outcome `trellis` has added since this
//! crate was written. The statement was still applied; the binding just has
//! no shape for its result yet. A host reports that as a success with no
//! detail rather than failing a call that already took effect.

use trellis::{Applied, QuarantineTarget};

use crate::{PlainDefinition, PlainRelationship, quarantine_address};

/// An [`Applied`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlainApplied {
    /// A `TRANSFORM` statement registered this definition.
    TransformDefined(PlainDefinition),
    /// A `RELATIONSHIP` statement registered this relationship.
    RelationshipDefined(PlainRelationship),
    /// A `PAUSE TRANSFORM` statement froze its subject, or found it frozen.
    Paused,
    /// A `RESUME TRANSFORM` statement unfroze its subject. `columns` is every
    /// column a column resume resumed, as its `transform.column` address
    /// (see [`crate::quarantine_address`]); empty for a whole-transform
    /// resume.
    Resumed { columns: Vec<String> },
    /// A `DROP` statement removed its subject, or found it gone.
    Dropped,
    /// An `ALTER TRANSFORM` statement edited its subject: the definition's new
    /// state, and the fields this call added, dropped and altered.
    Altered {
        definition: PlainDefinition,
        added: Vec<String>,
        dropped: Vec<String>,
        altered: Vec<String>,
    },
    /// An outcome this crate has no flattening for yet. The statement was
    /// applied.
    Unknown,
}

impl PlainApplied {
    /// Every word [`PlainApplied::kind`] can return: the set a host allocates
    /// its names from at load time.
    pub const KINDS: [&'static str; 7] = [
        "transform_defined",
        "relationship_defined",
        "paused",
        "resumed",
        "dropped",
        "altered",
        "unknown",
    ];

    /// The variant's stable name, the word a host keys its result off:
    /// `transform_defined`, `relationship_defined`, `paused`, `resumed`,
    /// `dropped`, `altered` or `unknown`.
    pub fn kind(&self) -> &'static str {
        match self {
            PlainApplied::TransformDefined(_) => "transform_defined",
            PlainApplied::RelationshipDefined(_) => "relationship_defined",
            PlainApplied::Paused => "paused",
            PlainApplied::Resumed { .. } => "resumed",
            PlainApplied::Dropped => "dropped",
            PlainApplied::Altered { .. } => "altered",
            PlainApplied::Unknown => "unknown",
        }
    }
}

impl From<&Applied> for PlainApplied {
    fn from(applied: &Applied) -> Self {
        match applied {
            Applied::TransformDefined(definition) => {
                PlainApplied::TransformDefined(PlainDefinition::from(definition))
            }
            Applied::RelationshipDefined(relationship) => {
                PlainApplied::RelationshipDefined(PlainRelationship::from(relationship))
            }
            Applied::Paused => PlainApplied::Paused,
            Applied::Resumed { columns } => PlainApplied::Resumed {
                columns: columns
                    .iter()
                    .map(|(transform, column)| {
                        quarantine_address(&QuarantineTarget::Column(
                            transform.clone(),
                            column.clone(),
                        ))
                    })
                    .collect(),
            },
            Applied::Dropped => PlainApplied::Dropped,
            Applied::Altered {
                definition,
                added,
                dropped,
                altered,
            } => PlainApplied::Altered {
                definition: PlainDefinition::from(definition),
                added: added.clone(),
                dropped: dropped.clone(),
                altered: altered.clone(),
            },
            _ => PlainApplied::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use trellis::dev::defs::ast::RelationshipDef;
    use trellis::dev::defs::{ValueType, parse};
    use trellis::{
        Definition, IntWidth, RelationshipCardinality, RelationshipDefinition, TransformStatus,
    };

    use super::*;

    fn definition(status: TransformStatus) -> Definition {
        Definition {
            id: 7,
            source_version: 1,
            def: parse("TRANSFORM widget_prices FROM widgets SELECT price AS price").unwrap(),
            source_columns: HashMap::from([(
                "price".to_string(),
                ValueType::Integer(IntWidth::Int4),
            )]),
            status,
            source_table: "public.widgets".to_string(),
            target_table: "public.widget_prices".to_string(),
        }
    }

    fn plain_definition(status: &'static str) -> PlainDefinition {
        PlainDefinition {
            id: 7,
            target_table: "public.widget_prices".to_string(),
            source_table: "public.widgets".to_string(),
            source_version: 1,
            status,
            source_columns: BTreeMap::from([("price".to_string(), "integer".to_string())]),
        }
    }

    #[test]
    fn a_defined_transform_crosses_as_its_plain_definition() {
        let applied = Applied::TransformDefined(definition(TransformStatus::WaitingToBackfill));

        let plain = PlainApplied::from(&applied);

        assert_eq!(
            plain,
            PlainApplied::TransformDefined(plain_definition("waiting_to_backfill"))
        );
        assert_eq!(plain.kind(), "transform_defined");
    }

    #[test]
    fn a_defined_relationship_crosses_as_its_plain_relationship() {
        let applied = Applied::RelationshipDefined(RelationshipDefinition {
            id: 2,
            from_schema: "public".to_string(),
            to_schema: "public".to_string(),
            def: RelationshipDef {
                name: "owner".to_string(),
                from_table: "widgets".to_string(),
                from_col: "owner_id".to_string(),
                to_table: "users".to_string(),
                to_col: "id".to_string(),
            },
            cardinality: RelationshipCardinality::ToMany,
            warnings: Vec::new(),
        });

        let plain = PlainApplied::from(&applied);

        let PlainApplied::RelationshipDefined(relationship) = &plain else {
            panic!("expected a relationship, got {plain:?}");
        };
        assert_eq!(relationship.name, "owner");
        assert_eq!(relationship.cardinality, "many");
        assert_eq!(plain.kind(), "relationship_defined");
    }

    #[test]
    fn a_column_resume_crosses_as_the_addresses_it_resumed() {
        let applied = Applied::Resumed {
            columns: vec![
                ("order_totals".to_string(), "total".to_string()),
                ("order_summaries".to_string(), "grand_total".to_string()),
            ],
        };

        let plain = PlainApplied::from(&applied);

        assert_eq!(
            plain,
            PlainApplied::Resumed {
                columns: vec![
                    "order_totals.total".to_string(),
                    "order_summaries.grand_total".to_string(),
                ],
            }
        );
        assert_eq!(plain.kind(), "resumed");
    }

    #[test]
    fn an_alteration_crosses_with_the_fields_it_changed() {
        let applied = Applied::Altered {
            definition: definition(TransformStatus::Live),
            added: vec!["doubled".to_string()],
            dropped: Vec::new(),
            altered: vec!["price".to_string()],
        };

        let plain = PlainApplied::from(&applied);

        assert_eq!(
            plain,
            PlainApplied::Altered {
                definition: plain_definition("live"),
                added: vec!["doubled".to_string()],
                dropped: Vec::new(),
                altered: vec!["price".to_string()],
            }
        );
        assert_eq!(plain.kind(), "altered");
    }

    #[test]
    fn a_pause_and_a_drop_carry_nothing_but_their_kind() {
        assert_eq!(PlainApplied::from(&Applied::Paused), PlainApplied::Paused);
        assert_eq!(PlainApplied::from(&Applied::Dropped), PlainApplied::Dropped);
        assert_eq!(PlainApplied::Paused.kind(), "paused");
        assert_eq!(PlainApplied::Dropped.kind(), "dropped");
        assert_eq!(PlainApplied::Unknown.kind(), "unknown");
    }

    #[test]
    fn every_kind_is_in_the_load_time_set() {
        let definition = plain_definition("live");
        for plain in [
            PlainApplied::TransformDefined(definition.clone()),
            PlainApplied::RelationshipDefined(PlainRelationship {
                id: 1,
                name: "owner".to_string(),
                from_schema: "public".to_string(),
                from_table: "widgets".to_string(),
                from_col: "owner_id".to_string(),
                to_schema: "public".to_string(),
                to_table: "users".to_string(),
                to_col: "id".to_string(),
                cardinality: "one",
                warnings: Vec::new(),
            }),
            PlainApplied::Paused,
            PlainApplied::Resumed {
                columns: Vec::new(),
            },
            PlainApplied::Dropped,
            PlainApplied::Altered {
                definition,
                added: Vec::new(),
                dropped: Vec::new(),
                altered: Vec::new(),
            },
            PlainApplied::Unknown,
        ] {
            assert!(PlainApplied::KINDS.contains(&plain.kind()), "{plain:?}");
        }
    }
}
