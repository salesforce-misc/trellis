//! What `Trellis::status` reports about a 1-1 `JOIN` transform (ADR-0021,
//! #1402): how big its hidden tables are, the step its build runs, and a
//! warning when one join key has so many `a` rows that a change of its `b` row
//! rewrites them all in one transaction. It is [`super::join_status`]'s
//! [`JoinStatus`], filled for a definition with no `P` and `T`: `hidden_tables`
//! lists `R`, `M` and `J`, `building` names a [`OneToOneBuildStep`] and
//! `fan_out_warning` is set. Detection only: nothing here changes the
//! definition's status or repairs anything.
//!
//! Sizes and row counts are the planner's estimates, as the aggregate `JOIN`'s
//! are (`pg_class.reltuples`, `pg_total_relation_size`): as fresh as the last
//! `ANALYZE` or autovacuum, at the same cost on a billion rows as on ten.
//! `J` has a row for every join key either table has held and never shrinks,
//! so its size only grows; `R` and `M` keep tombstones until the collection
//! reaches them.
//!
//! # The fan-out warning
//!
//! A change of a `b` row rewrites every target row on its key, in the
//! transaction that applies it, while an `a` change on the key waits
//! (ADR-0021). The warning names the largest group of live `R` entries on one
//! join key when it holds more than [`fan_out_warning_rows`] of them.
//!
//! It is *sampled*, not read from `pg_stats`: the statistics are as stale as
//! the last analyze, which after a build is the empty table. The sample is
//! [`SAMPLE_PAGES`] pages of `R` (all of it, below that), chosen by
//! `TABLESAMPLE SYSTEM`. A row is in the sample in proportion to its page, so
//! a key is sampled in proportion to its group's size, and the keys met most
//! often are the largest groups. The [`PROBE_KEYS`] most frequent keys of the
//! sample are counted exactly over `R`'s join-key index, each stopping at ten
//! times the threshold; the largest count is reported. So a group is found
//! with a probability that grows with its share of `R`: a group of more than
//! `1 / SAMPLE_PAGES` of its pages is met several times. A very large `R`
//! holding one group just over the threshold can go unreported until the group
//! grows.
//!
//! The threshold is the default of a knob ([`FAN_OUT_WARNING_ENV`]) whose value
//! belongs to the benchmark of the SQL fan-out (#1404, milestone 9), which sets
//! it from the cost per row there.

use std::sync::atomic::{AtomicI64, Ordering};

use tokio_postgres::GenericClient;

use super::build;
use super::join_status::{HiddenTableSize, JoinBuildStep, JoinStatus, hidden_table_size};
use crate::defs::ast::TransformDef;
use crate::defs::chunk_queue::{KIND_PLAN, KIND_REDERIVE, KIND_SWEEP};
use crate::defs::join::{self as spec, Side};
use crate::defs::join_one_to_one::HiddenTable;
use crate::defs::ledger as schema;
use crate::defs::model::TransformStatus;
use crate::pool::quote_ident;

/// The environment variable that sets the fan-out warning threshold: the most
/// live `a` rows one join key may hold before `status()` warns. Read once,
/// when nothing has set it.
pub const FAN_OUT_WARNING_ENV: &str = "TRELLIS_ONE_TO_ONE_JOIN_FAN_OUT_WARNING";

/// The fan-out warning threshold when [`FAN_OUT_WARNING_ENV`] isn't set (or
/// isn't a positive number): a placeholder until milestone 9's benchmark of the
/// SQL fan-out sets it (#1404). ADR-0021 names 100k.
pub const DEFAULT_FAN_OUT_WARNING_ROWS: i64 = 100_000;

/// 0: not read yet; otherwise the threshold.
static FAN_OUT_WARNING_ROWS: AtomicI64 = AtomicI64::new(0);

/// The fan-out warning threshold ([`FAN_OUT_WARNING_ENV`]).
pub fn fan_out_warning_rows() -> i64 {
    match FAN_OUT_WARNING_ROWS.load(Ordering::Relaxed) {
        0 => {
            let rows = std::env::var(FAN_OUT_WARNING_ENV)
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|rows| *rows > 0)
                .unwrap_or(DEFAULT_FAN_OUT_WARNING_ROWS);
            FAN_OUT_WARNING_ROWS.store(rows, Ordering::Relaxed);
            rows
        }
        rows => rows,
    }
}

/// Sets the threshold for this process, for tests; `0` reads
/// [`FAN_OUT_WARNING_ENV`] again on next use.
#[cfg_attr(not(any(test, feature = "test-util")), allow(dead_code))]
pub fn set_fan_out_warning_rows(rows: i64) {
    FAN_OUT_WARNING_ROWS.store(rows, Ordering::Relaxed);
}

/// The pages of `R` a fan-out sample reads (the module doc).
const SAMPLE_PAGES: i64 = 2000;

/// The keys of the sample counted exactly (the module doc).
const PROBE_KEYS: i64 = 8;

/// How many times the threshold an exact count runs to before it stops.
const COUNT_CAP_FACTOR: i64 = 10;

/// A join key whose `a` group is large enough that a change of its `b` row
/// rewrites that many target rows in one transaction; see
/// [`JoinStatus::fan_out_warning`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FanOutWarning {
    /// The join key's text: its columns' values, joined by `,`.
    pub key: String,
    /// The live `R` entries on the key. Exact up to ten times the threshold,
    /// where the count stops.
    pub rows: i64,
    /// The threshold the group passed (`fan_out_warning_rows`).
    pub threshold: i64,
}

/// The step of a 1-1 `JOIN` build; see [`JoinStatus::building`]. The build is
/// the plan job's `M` pass and walk of `a`, then the chunks of `a`'s primary
/// key, then the rebuild sweep of a rebuild in place (ADR-0021).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum OneToOneBuildStep {
    /// The `M` pass over `b`'s rows in primary-key order.
    JoinedRows,
    /// The `M` pass over the rows of `b` whose primary key has a `NULL` part.
    JoinedNullKeys,
    /// The `M` pass over `M`'s live entries whose `b` row is gone.
    JoinedGone,
    /// The plan job walks `a`'s primary key and enqueues the chunks.
    PlanningChunks,
    /// The chunks of `a`'s primary key: `remaining` are still to run.
    Chunks { remaining: i64 },
    /// The sweep's pass over `R`'s stale entries.
    SweepStale,
    /// The sweep's pass over the target rows no live `R` entry backs.
    SweepOrphans,
}

/// The status of definition `id`, a 1-1 `JOIN`, in `catalog_schema`.
pub(crate) async fn for_definition(
    client: &impl GenericClient,
    catalog_schema: &str,
    id: i64,
    def: &TransformDef,
) -> Result<JoinStatus, tokio_postgres::Error> {
    let mut hidden_tables = Vec::new();
    for (label, table) in [
        ("R", HiddenTable::FromEntries),
        ("M", HiddenTable::JoinedEntries),
        ("J", HiddenTable::Keys),
    ] {
        if let Some(size) =
            hidden_table_size(client, catalog_schema, label, &table.name(id)).await?
        {
            hidden_tables.push(size);
        }
    }
    let fan_out_warning = fan_out_warning(client, catalog_schema, id, def, &hidden_tables).await?;
    let building = building(client, id).await?.map(JoinBuildStep::OneToOne);
    Ok(JoinStatus {
        hidden_tables,
        partials_warning: None,
        fan_out_warning,
        building,
    })
}

/// The percentage of `R` a sample reads when `R` has `pages` pages.
fn sample_percent(pages: i64) -> f64 {
    (100.0 * SAMPLE_PAGES as f64 / pages.max(1) as f64).min(100.0)
}

/// The largest group of live `R` entries on one join key, if it passes the
/// threshold (the module doc).
async fn fan_out_warning(
    client: &impl GenericClient,
    catalog_schema: &str,
    id: i64,
    def: &TransformDef,
    hidden_tables: &[HiddenTableSize],
) -> Result<Option<FanOutWarning>, tokio_postgres::Error> {
    // No `R`, or an empty one, has no group.
    if !hidden_tables.iter().any(|t| t.table == "R") {
        return Ok(None);
    }
    let threshold = fan_out_warning_rows();
    let r = HiddenTable::FromEntries.qualified(catalog_schema, id);
    let key = spec::join_key(def, Side::From);
    if key.is_empty() {
        return Ok(None);
    }
    // `0` for an `R` dropped since its size was read: no sample.
    let pages: i64 = client
        .query_one(
            "select coalesce(pg_catalog.pg_relation_size(pg_catalog.to_regclass($1)), 0) \
                    / pg_catalog.current_setting('block_size')::bigint",
            &[&r],
        )
        .await?
        .get(0);
    if pages == 0 {
        return Ok(None);
    }
    let columns: Vec<String> = key.iter().map(|c| quote_ident(c)).collect();
    let not_null: Vec<String> = columns.iter().map(|c| format!("{c} is not null")).collect();
    let same: Vec<String> = columns.iter().map(|c| format!("r.{c} = top.{c}")).collect();
    let texts: Vec<String> = columns.iter().map(|c| format!("top.{c}::text")).collect();
    let tomb = quote_ident(schema::TOMBSTONE_COLUMN);
    let cap = threshold.saturating_mul(COUNT_CAP_FACTOR);
    let sql = format!(
        "with sample as ( \
             select {columns} from {r} tablesample system ($1::float8) \
             where not {tomb} and {not_null}), \
         top as ( \
             select {columns}, count(*) as n from sample \
             group by {columns} order by n desc, {columns} limit {PROBE_KEYS}) \
         select concat_ws(',', {texts}), \
                (select count(*) from (select 1 from {r} r \
                                       where {same} and not r.{tomb} limit $2) c) as rows \
         from top order by rows desc, 1 limit 1",
        columns = columns.join(", "),
        not_null = not_null.join(" and "),
        same = same.join(" and "),
        texts = texts.join(", "),
    );
    let Some(row) = client
        .query_opt(&sql, &[&sample_percent(pages), &cap])
        .await?
    else {
        return Ok(None);
    };
    let rows: i64 = row.get(1);
    Ok((rows > threshold).then(|| FanOutWarning {
        key: row.get(0),
        rows,
        threshold,
    }))
}

/// The step the definition's build runs now or next ([`JoinStatus::building`]):
/// the plan job's stage while it is not done, else the chunks still to run,
/// else the rebuild sweep's stage.
///
/// The jobs are the definition's current ones (those of its `fuse_rearmed_at`)
/// that aren't done, while the definition isn't frozen: a paused or
/// quarantined definition's job is never handed out, and its `RESUME` discards
/// it. Each step writes its cursor in its own transaction, so this plain
/// `select`, which takes no row lock, sees the last step that committed.
async fn building(
    client: &impl GenericClient,
    id: i64,
) -> Result<Option<OneToOneBuildStep>, tokio_postgres::Error> {
    let rows = client
        .query(
            "select bc.kind, bc.lo from backfill_chunks bc \
             join transform_definitions d on d.id = bc.definition_id \
             where bc.definition_id = $1 and bc.kind = any($2) and bc.fields is null \
               and not bc.done and bc.fuse_rearmed_at is not distinct from d.fuse_rearmed_at \
               and d.status = any($3) \
             order by bc.id desc",
            &[
                &id,
                &[KIND_PLAN, KIND_REDERIVE, KIND_SWEEP].as_slice(),
                &TransformStatus::dispatchable(),
            ],
        )
        .await?;
    let job = |kind: &str| {
        rows.iter()
            .find(|row| row.get::<_, String>(0) == kind)
            .map(|row| row.get::<_, Option<String>>(1))
    };
    if let Some(cursor) = job(KIND_PLAN) {
        return Ok(Some(build::plan_step(cursor.as_deref())));
    }
    let chunks = rows
        .iter()
        .filter(|row| row.get::<_, String>(0) == KIND_REDERIVE)
        .count() as i64;
    if chunks > 0 {
        return Ok(Some(OneToOneBuildStep::Chunks { remaining: chunks }));
    }
    Ok(job(KIND_SWEEP).map(|cursor| build::sweep_step(cursor.as_deref())))
}

#[cfg(test)]
mod tests {
    use super::{SAMPLE_PAGES, sample_percent};

    /// A table that fits the sample is read in full; a larger one by the
    /// share of its pages the sample budget allows.
    #[test]
    fn the_sample_reads_all_of_a_small_table_and_a_share_of_a_large_one() {
        assert_eq!(sample_percent(0), 100.0);
        assert_eq!(sample_percent(1), 100.0);
        assert_eq!(sample_percent(SAMPLE_PAGES), 100.0);
        assert_eq!(sample_percent(SAMPLE_PAGES * 2), 50.0);
        assert_eq!(sample_percent(SAMPLE_PAGES * 100), 1.0);
    }
}
