//! What `Trellis::status` reports about a `JOIN` transform: how big
//! its hidden tables are, whether `P` has grown close to the fact table, and
//! the step its build runs.
//! The source indexes it needs are every target kind's
//! (`DefinitionStatus::source_index_gaps`). Detection only:
//! nothing here changes the definition's status or repairs anything.
//!
//! Sizes and row counts are the planner's estimates (`pg_class.reltuples`,
//! `pg_total_relation_size`), so a status call costs the same on a billion
//! rows as on ten; they are as fresh as the last `ANALYZE` or autovacuum of
//! the table.

use tokio_postgres::GenericClient;

use super::build::{self, JoinStepId};
use super::join_one_to_one_status::{FanOutWarning, OneToOneBuildStep};
use super::partition_correction;
use crate::capture::{CaptureError, partitions};
use crate::defs::ast::TransformDef;
use crate::defs::chunk_queue::{KIND_PLAN, KIND_SWEEP};
use crate::defs::ddl::regclass_arg;
use crate::defs::join::{self as spec, HiddenTable, Side};
use crate::defs::model::TransformStatus;

/// `P` is reported once its estimated rows reach this share of the fact
/// table's, as a percentage: past it the partials no longer compress the
/// fact table, and the `JOIN` costs a fact-table-sized hidden table.
pub(crate) const PARTIALS_WARNING_PERCENT: i64 = 50;

/// Below this many estimated fact rows `P` is never reported: a small table
/// whose groups are mostly distinct is not a cost worth a warning.
pub(crate) const PARTIALS_WARNING_MIN_FACT_ROWS: i64 = 1000;

/// One hidden table of a `JOIN` definition.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HiddenTableSize {
    /// Which table: `P` (the `FROM` side's partials), `M` (the joined
    /// side's), `J` (the matches per join key) or `T` (the target's
    /// partials).
    pub table: &'static str,
    /// The table's name in the catalog schema.
    pub name: String,
    /// The estimated live rows (`reltuples`; 0 before the first analyze).
    pub estimated_rows: i64,
    /// The table's total size on disk with its indexes, in bytes.
    pub bytes: i64,
}

/// `P` has grown close to the fact table's size; see [`JoinStatus::partials_warning`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PartialsWarning {
    /// `P`'s estimated rows.
    pub partials_rows: i64,
    /// The `FROM` (fact) table's estimated rows.
    pub fact_rows: i64,
}

/// A `JOIN` definition's hidden tables; see
/// `DefinitionStatus::join`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct JoinStatus {
    /// `P`, `M`, `J` and `T`, in that order. A table that doesn't exist is
    /// left out.
    pub hidden_tables: Vec<HiddenTableSize>,
    /// Set when `P`'s estimated rows reach
    /// `PARTIALS_WARNING_PERCENT` percent of the fact table's. Never set for
    /// a 1-1 `JOIN`.
    pub partials_warning: Option<PartialsWarning>,
    /// A 1-1 `JOIN`'s largest group of `a` rows on one join key, when it
    /// passes the threshold: a change of its `b` row rewrites that many target
    /// rows in one transaction. Sampled; see [`FanOutWarning`]. Always `None`
    /// for an aggregate `JOIN`.
    pub fan_out_warning: Option<FanOutWarning>,
    /// The step the definition's build runs now, or runs next if no worker
    /// holds its job; `None` when no build is under way or the definition
    /// is paused or quarantined.
    pub building: Option<JoinBuildStep>,
}

/// A side of a `JOIN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JoinSide {
    /// The `FROM` table, whose partials are `P`.
    From,
    /// The `JOIN` table, whose partials are `M`.
    Joined,
}

/// The step of a `JOIN` build; see [`JoinStatus::building`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum JoinBuildStep {
    /// The whole build in one transaction: neither side is a partitioned
    /// table with rows, so the build has no steps.
    Whole,
    /// A step of a build with a partitioned side: one side, or one part of
    /// it.
    Side { side: JoinSide, part: JoinSidePart },
    /// The final step, which writes `T` and the target and completes the
    /// build.
    Final,
    /// A step of a 1-1 `JOIN` build.
    OneToOne(OneToOneBuildStep),
}

/// What a [`JoinBuildStep::Side`] step builds of its side.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum JoinSidePart {
    /// The whole side: it isn't a partitioned table with rows.
    Whole,
    /// One leaf of the side's recorded partition tree, by its recorded
    /// `schema.table` name. Leaves build in oid order.
    Leaf(String),
    /// The side's cells that route to none of its recorded leaves, after
    /// its last leaf.
    Remainder,
    /// The side's partition tree isn't recorded yet, or one of its leaves
    /// has no usable constraint text, so the build can't route its cells to
    /// leaves and fails on this side (`DefinitionStatus::backfill_failure`
    /// says why).
    Unroutable,
}

/// The status of `def`, definition `id` in `catalog_schema` over
/// `source_table` and `join_table`, if it is a `JOIN`.
pub(crate) async fn for_definition(
    client: &impl GenericClient,
    catalog_schema: &str,
    id: i64,
    source_table: &str,
    join_table: Option<&str>,
    def: &TransformDef,
) -> Result<Option<JoinStatus>, tokio_postgres::Error> {
    // A 1-1 JOIN keeps other hidden tables (`join1_{id}_*`).
    if spec::is_one_to_one(def) {
        return Ok(Some(
            super::join_one_to_one_status::for_definition(client, catalog_schema, id, def).await?,
        ));
    }
    if !spec::is_grouped(def) {
        return Ok(None);
    }
    let mut hidden_tables = Vec::new();
    for (label, table) in [
        ("P", HiddenTable::FromPartials),
        ("M", HiddenTable::JoinedPartials),
        ("J", HiddenTable::Matches),
        ("T", HiddenTable::TargetPartials),
    ] {
        if let Some(size) =
            hidden_table_size(client, catalog_schema, label, &table.name(id)).await?
        {
            hidden_tables.push(size);
        }
    }
    let partials_warning = partials_warning(client, &hidden_tables, source_table).await?;
    let building = building(client, id, source_table, join_table).await?;
    Ok(Some(JoinStatus {
        hidden_tables,
        partials_warning,
        fan_out_warning: None,
        building,
    }))
}

/// The size of the hidden table `name` in `catalog_schema`, labelled `label`,
/// or `None` if it doesn't exist.
pub(super) async fn hidden_table_size(
    client: &impl GenericClient,
    catalog_schema: &str,
    label: &'static str,
    name: &str,
) -> Result<Option<HiddenTableSize>, tokio_postgres::Error> {
    let qualified = format!("{catalog_schema}.{name}");
    let row = client
        .query_opt(
            "select c.reltuples::bigint, pg_catalog.pg_total_relation_size(c.oid) \
             from pg_catalog.pg_class c where c.oid = pg_catalog.to_regclass($1)",
            &[&regclass_arg(&qualified)],
        )
        .await?;
    Ok(row.map(|row| HiddenTableSize {
        table: label,
        name: name.to_string(),
        estimated_rows: row.get::<_, i64>(0).max(0),
        bytes: row.get(1),
    }))
}

/// The step the definition's build runs now or next ([`JoinStatus::building`]):
/// the one after the step its job's cursor names, by the build's own order
/// (`build::join_steps`).
///
/// The job is the definition's current plan (fresh build or `RESUME`) or
/// sweep (rebuild in place) job that isn't done, while the definition isn't
/// frozen: a paused or quarantined definition's job is never handed out, and
/// its `RESUME` discards it. Each step writes the cursor
/// in its own transaction, so this plain `select`, which takes no row lock,
/// sees the last step that committed: it neither waits on a running step nor
/// holds one up.
async fn building(
    client: &impl GenericClient,
    id: i64,
    source_table: &str,
    join_table: Option<&str>,
) -> Result<Option<JoinBuildStep>, tokio_postgres::Error> {
    let Some(job) = client
        .query_opt(
            "select bc.lo from backfill_chunks bc \
             join transform_definitions d on d.id = bc.definition_id \
             where bc.definition_id = $1 and bc.kind = any($2) and bc.fields is null \
               and not bc.done and bc.fuse_rearmed_at is not distinct from d.fuse_rearmed_at \
               and d.status = any($3) \
             order by bc.id desc limit 1",
            &[
                &id,
                &[KIND_PLAN, KIND_SWEEP].as_slice(),
                &TransformStatus::dispatchable(),
            ],
        )
        .await?
    else {
        return Ok(None);
    };
    let cursor: Option<String> = job.get(0);
    let mut sides = vec![(Side::From, source_table)];
    sides.extend(join_table.map(|t| (Side::Joined, t)));
    let mut partitioned = Vec::new();
    for (side, table) in sides {
        if !build::builds_per_leaf(client, table).await? {
            continue;
        }
        let recorded = match partitions::recorded(client, table).await {
            Ok(recorded) => recorded,
            Err(CaptureError::Db(e)) => return Err(e),
            Err(_) => None,
        };
        let leaves = recorded
            .as_ref()
            .and_then(|r| partition_correction::rebuild_leaves(r).ok());
        let (Some(recorded), Some(leaves)) = (recorded, leaves) else {
            // The build fails on the first side it can't route, before any
            // step.
            return Ok(Some(JoinBuildStep::Side {
                side: public_side(side),
                part: JoinSidePart::Unroutable,
            }));
        };
        partitioned.push((side, recorded, leaves));
    }
    if partitioned.is_empty() {
        return Ok(Some(JoinBuildStep::Whole));
    }
    let order: Vec<(Side, Vec<u32>)> = partitioned
        .iter()
        .map(|(side, _, leaves)| (*side, leaves.iter().map(|l| l.oid).collect()))
        .collect();
    let steps = build::join_steps(&order);
    let Some(next) = build::join_steps_after(&steps, cursor.as_deref()).first() else {
        return Ok(Some(JoinBuildStep::Final));
    };
    let (side, part) = match *next {
        JoinStepId::Whole(side) => (side, JoinSidePart::Whole),
        JoinStepId::Remainder(side) => (side, JoinSidePart::Remainder),
        JoinStepId::Leaf(side, oid) => {
            let name = partitioned
                .iter()
                .find(|(s, _, _)| *s == side)
                .and_then(|(_, recorded, _)| recorded.nodes.iter().find(|n| n.oid == oid))
                .map_or_else(|| oid.to_string(), |n| n.name.clone());
            (side, JoinSidePart::Leaf(name))
        }
    };
    Ok(Some(JoinBuildStep::Side {
        side: public_side(side),
        part,
    }))
}

fn public_side(side: Side) -> JoinSide {
    match side {
        Side::From => JoinSide::From,
        Side::Joined => JoinSide::Joined,
    }
}

async fn partials_warning(
    client: &impl GenericClient,
    hidden_tables: &[HiddenTableSize],
    source_table: &str,
) -> Result<Option<PartialsWarning>, tokio_postgres::Error> {
    let Some(p) = hidden_tables.iter().find(|t| t.table == "P") else {
        return Ok(None);
    };
    let fact_rows: i64 = client
        .query_opt(
            "select c.reltuples::bigint from pg_catalog.pg_class c \
             where c.oid = pg_catalog.to_regclass($1)",
            &[&regclass_arg(source_table)],
        )
        .await?
        .map_or(0, |row| row.get::<_, i64>(0).max(0));
    Ok(
        warns(p.estimated_rows, fact_rows).then_some(PartialsWarning {
            partials_rows: p.estimated_rows,
            fact_rows,
        }),
    )
}

/// Whether `partials` estimated `P` rows warrant a warning against
/// `fact_rows` estimated fact rows.
pub(crate) fn warns(partials: i64, fact_rows: i64) -> bool {
    fact_rows >= PARTIALS_WARNING_MIN_FACT_ROWS
        && partials.saturating_mul(100) >= fact_rows.saturating_mul(PARTIALS_WARNING_PERCENT)
}

#[cfg(test)]
mod tests {
    use super::warns;

    #[test]
    fn partials_warn_at_half_the_fact_table_and_not_for_a_small_one() {
        assert!(!warns(499, 1000));
        assert!(warns(500, 1000));
        assert!(warns(5000, 1000));
        assert!(!warns(10, 10), "a tiny fact table never warns");
        assert!(!warns(0, 0));
    }
}
