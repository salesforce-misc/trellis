//! Residue invariants of the epoch tier (#2187, sub-step 8b): what a chain
//! leaves behind in the Trellis tables (the instance schema's, and each
//! defined target with its 1-1 ledger), checked at each quiet point after a
//! `VACUUM`.
//!
//! - `n_dead_tup` is `0`: nothing holds the vacuum horizon back, and no
//!   Trellis table carries tuples plain `VACUUM` could not reclaim.
//! - A pages backstop: `pages <= 4 * ceil(peak * width / 8192) + 8`, where
//!   `peak` is the most live rows the *model* (the program's ops) ever put in
//!   the table's source, and `width` the widest row the table has held at any
//!   quiet point. There is no bound against current live rows: plain
//!   `VACUUM` keeps the high-water mark (#779).
//!
//! [`run_epochs_lapping`] also laps the ring: each op's capture is sealed on
//! the spot, so each op advances the active slot by one, and a chain of
//! hundreds of ops goes round the four slots a hundred times.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio_postgres::NoTls;
use trellis::Config;
use trellis::dev::defs::ast::TransformDef;
use trellis::dev::pool::Pool;

use super::pause::RunKind;
use super::{Outcome, RunError, apply_and_check_outcome, quiesce_snapshot_and_check};
use crate::backend::{Backend, ConcurrentBackend, ManualBackend, Roles};
use crate::generate::epoch::{EpochChain, EpochStep, apply_step};
use crate::model::{Op, OpOutcome, Program};

/// The engine's maintenance interval in a lapping run, so its own ticks seal
/// between the harness's seals too.
pub const LAP_SEAL_INTERVAL: Duration = Duration::from_millis(50);

/// How many times a table with dead tuples is vacuumed before they count,
/// and the pause between.
const VACUUM_ATTEMPTS: u32 = 40;
const VACUUM_RETRY: Duration = Duration::from_millis(50);

/// The suffix of the ledger a 1-1 target keeps beside it
/// (`trellis::defs::ledger::LEDGER_SUFFIX`, not exported).
const LEDGER_SUFFIX: &str = "__ledger";

/// The ring's slot count (`trellis::staging`): a seal advances one slot.
pub const RING_SLOTS: i64 = 4;

/// What a run's residue checks saw.
#[derive(Debug, Clone, Default)]
pub struct ResidueReport {
    /// Segments sealed over the whole chain; a lap of the ring is
    /// [`RING_SLOTS`] of them.
    pub seals: i64,
    /// Each table's widest row at any quiet point.
    pub widths: BTreeMap<String, i64>,
    /// The tables checked at the last quiet point.
    pub last: Vec<TableResidue>,
    /// What the hidden-row checks saw at the last quiet point
    /// ([`super::hidden`]).
    pub hidden: super::hidden::HiddenReport,
}

impl ResidueReport {
    /// Times the chain went round the ring.
    pub fn laps(&self) -> i64 {
        self.seals / RING_SLOTS
    }
}

/// One Trellis table as measured after a `VACUUM`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableResidue {
    pub table: String,
    pub pages: i64,
    pub dead: i64,
    /// Widest live row, in bytes; `0` for an empty table.
    pub width: i64,
}

/// The most live rows each of `program`'s tables holds at any point of its
/// ops, replayed from their expected outcomes. A table truncated is counted
/// as emptied.
pub fn peak_live_rows(program: &Program) -> BTreeMap<String, i64> {
    let mut live: BTreeMap<String, i64> = BTreeMap::new();
    let mut peak: BTreeMap<String, i64> = BTreeMap::new();
    for op in &program.ops {
        let table = op.table().to_string();
        let now = live.entry(table.clone()).or_insert(0);
        // The outer op's outcome decides: a `DO NOTHING` upsert of a row that
        // exists changes nothing, whatever its inner insert expects.
        let succeeds = matches!(op.expect(), OpOutcome::Succeeds);
        let mut write = op.write();
        while let Op::Leaf { op, .. } = write {
            write = op.write();
        }
        match write {
            Op::Insert { .. } if succeeds => *now += 1,
            Op::BulkInsert { rows, .. } if succeeds => *now += rows.len() as i64,
            Op::Delete { .. } if succeeds => *now -= 1,
            Op::Truncate { .. } => *now = 0,
            _ => {}
        }
        let high = peak.entry(table).or_insert(0);
        *high = (*high).max(*now);
    }
    peak
}

/// The pages `peak` live rows of `width` bytes may leave a table with.
pub fn pages_bound(peak: i64, width: i64) -> i64 {
    // 4 bytes of line pointer per row, beside the row itself.
    let bytes = peak.max(0) * (width + 4);
    4 * ((bytes + 8191) / 8192) + 8
}

/// [`super::run_epochs`] with the ring lapped and the residue checked: every
/// op's capture is sealed as soon as it commits, and after each epoch's
/// quiesce and oracle check every Trellis table is vacuumed and measured
/// ([`check_residue`]). `config` is the instance's, and `roles` the case's
/// ([`crate::backend::CaseDatabase::roles`]).
pub async fn run_epochs_lapping(
    backend: &mut ManualBackend,
    pool: &Pool,
    config: &Config,
    roles: &Roles,
    chain: &EpochChain,
) -> Result<(Outcome, ResidueReport), RunError> {
    let program = &chain.program;
    let in_epoch = |epoch: usize| {
        move |error: RunError| RunError::Epoch {
            epoch,
            error: Box::new(error),
        }
    };
    backend
        .install(&Program {
            tables: program.tables.clone(),
            defs: program.defs.clone(),
            def_install_after_op: vec![0; program.defs.len()],
            ops: Vec::new(),
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        })
        .await
        .map_err(|e| in_epoch(0)(RunError::Install(format!("{e:?}"))))?;
    // The install drew the targets' schema; residue names them there.
    let config = &config
        .clone()
        .with_target_schema(backend.target_schema().to_string())
        .map_err(|e| in_epoch(0)(RunError::Install(format!("{e:?}"))))?;
    let mut defined: Vec<TransformDef> = program.defs.clone();
    let mut ever: Vec<TransformDef> = program.defs.clone();
    let peaks = peak_live_rows(program);
    let mut report = ResidueReport::default();

    for (index, epoch) in chain.epochs.iter().enumerate() {
        for step in &epoch.steps {
            match step {
                EpochStep::Restart => backend
                    .restart()
                    .await
                    .map_err(|e| in_epoch(index)(RunError::Restart(format!("{e:?}"))))?,
                EpochStep::Act(action) => {
                    backend.act(program, action).await.map_err(|e| {
                        in_epoch(index)(RunError::BurstAction {
                            burst: index,
                            action: action.clone(),
                            error: format!("{e:?}"),
                        })
                    })?;
                }
            }
            apply_step(&mut defined, step);
            if let EpochStep::Act(crate::model::BurstAction::Define { def, .. }) = step {
                ever.push((**def).clone());
            }
        }
        for op_index in epoch.ops.clone() {
            apply_and_check_outcome(backend, op_index, &program.ops[op_index], false)
                .await
                .map_err(in_epoch(index))?;
            let pending = backend
                .has_pending()
                .await
                .map_err(|e| in_epoch(index)(RunError::Quiesce(format!("{e:?}"))))?;
            if pending {
                backend
                    .force_seal_active_segment()
                    .await
                    .map_err(|e| in_epoch(index)(RunError::Quiesce(format!("seal: {e:?}"))))?;
            }
        }
        quiesce_snapshot_and_check(
            backend,
            pool,
            program,
            epoch.ops.end.saturating_sub(1),
            &defined,
            false,
            RunKind::Epoch,
        )
        .await
        .map_err(in_epoch(index))?;
        let targets: Vec<String> = defined.iter().map(|d| d.target.clone()).collect();
        let failures = backend
            .strict_audit(&targets)
            .await
            .map_err(|e| in_epoch(index)(RunError::Quiesce(format!("strict audit: {e:?}"))))?;
        if !failures.is_empty() {
            return Err(in_epoch(index)(RunError::Audit {
                burst: index,
                failures,
            }));
        }
        report.seals = sealed_segments(config, roles)
            .await
            .map_err(|e| in_epoch(index)(RunError::Quiesce(format!("seal count: {e}"))))?;
        report.hidden = super::hidden::check_hidden(
            config,
            roles,
            pool,
            program,
            &defined,
            &ever,
            super::hidden::SETTLE,
        )
        .await
        .map_err(in_epoch(index))?;
        report.last = check_residue(config, roles, &defined, &ever, &peaks, &mut report.widths)
            .await
            .map_err(in_epoch(index))?;
    }
    Ok((Outcome::Ran, report))
}

/// The other sessions that could hold the vacuum horizon back, for a
/// failure's message.
async fn holders(admin: &tokio_postgres::Client) -> String {
    let rows = admin
        .query(
            "select pid, state, backend_xmin::text, xact_start::text, left(query, 80) \
             from pg_stat_activity \
             where datname = current_database() and pid <> pg_backend_pid() \
               and (backend_xmin is not null or backend_xid is not null or state <> 'idle')",
            &[],
        )
        .await;
    match rows {
        Ok(rows) => rows
            .iter()
            .map(|r| {
                format!(
                    "[pid {} {:?} xmin {:?} since {:?}: {:?}]",
                    r.get::<_, i32>(0),
                    r.get::<_, Option<String>>(1),
                    r.get::<_, Option<String>>(2),
                    r.get::<_, Option<String>>(3),
                    r.get::<_, Option<String>>(4),
                )
            })
            .collect(),
        Err(e) => format!("(could not read: {e})"),
    }
}

/// Segments sealed so far: `active_seq` is the monotonic sequence of the
/// segment filling now, so it is one more than the seals behind it.
async fn sealed_segments(config: &Config, roles: &Roles) -> Result<i64, tokio_postgres::Error> {
    let (admin, connection) = tokio_postgres::connect(&roles.admin, NoTls).await?;
    let connection = tokio::spawn(connection);
    let row = admin
        .query_one(
            &format!(
                "select active_seq from \"{}\".segment_pointer",
                config.schema()
            ),
            &[],
        )
        .await;
    drop(admin);
    let _ = connection.await;
    Ok(row?.get::<_, i64>(0) - 1)
}

/// Vacuums every table of the instance schema and each of `defined`'s
/// targets and their ledgers, then checks each one's `n_dead_tup` and page count. `widths`
/// carries each table's widest row over the earlier quiet points. `ever`
/// is every definition the chain has defined, which says what each target
/// reads.
///
/// Runs as the operator ([`Roles::admin`]): `VACUUM` is an operator's
/// statement, and the application and Trellis roles may not run it on each
/// other's tables.
pub async fn check_residue(
    config: &Config,
    roles: &Roles,
    defined: &[TransformDef],
    ever: &[TransformDef],
    peaks: &BTreeMap<String, i64>,
    widths: &mut BTreeMap<String, i64>,
) -> Result<Vec<TableResidue>, RunError> {
    let fail = |what: &str, e: tokio_postgres::Error| RunError::Residue {
        failures: vec![format!("{what}: {e}")],
    };
    let (admin, connection) = tokio_postgres::connect(&roles.admin, NoTls)
        .await
        .map_err(|e| fail("connect", e))?;
    let connection = tokio::spawn(connection);

    let mut tables: Vec<(String, String)> = Vec::new();
    for row in admin
        .query(
            "select n.nspname, c.relname from pg_class c \
             join pg_namespace n on n.oid = c.relnamespace \
             where c.relkind in ('r', 'p') and n.nspname = $1 order by 1, 2",
            &[&config.schema()],
        )
        .await
        .map_err(|e| fail("list instance tables", e))?
    {
        tables.push((row.get(0), row.get(1)));
    }
    // Each target, and the ledger a 1-1 target keeps beside it
    // (`trellis::defs::ledger`, `<target>__ledger`): a Trellis table outside
    // the instance schema.
    let mut beside: BTreeMap<String, String> = BTreeMap::new();
    for def in defined {
        let schema = def
            .explicit_target_schema
            .clone()
            .unwrap_or_else(|| config.target_schema().to_string());
        tables.push((schema.clone(), def.target.clone()));
        let ledger = format!("{}{LEDGER_SUFFIX}", def.target);
        let exists: bool = admin
            .query_one(
                "select to_regclass($1) is not null",
                &[&format!("\"{schema}\".\"{ledger}\"")],
            )
            .await
            .map_err(|e| fail(&format!("find {ledger}"), e))?
            .get(0);
        if exists {
            beside.insert(ledger.clone(), def.target.clone());
            tables.push((schema, ledger));
        }
    }

    // The source of each target, ever.
    let sources: BTreeMap<&str, &str> = ever
        .iter()
        .map(|d| (d.target.as_str(), d.source.as_str()))
        .collect();
    let total_peak: i64 = peaks.values().sum();

    let mut out = Vec::new();
    let mut failures = Vec::new();
    for (schema, name) in tables {
        let qualified = format!("\"{schema}\".\"{name}\"");
        // The engine's own maintenance tick reads the catalog now and then,
        // and a `VACUUM` run beside one cannot reclaim what that snapshot
        // can still see. So a table that shows dead tuples is vacuumed again
        // until the tick is between two, for up to `VACUUM_ATTEMPTS`: a
        // session that keeps its snapshot past that is what the check is
        // for.
        let mut dead = 0;
        for attempt in 0..VACUUM_ATTEMPTS {
            admin
                .batch_execute(&format!("vacuum {qualified}"))
                .await
                .map_err(|e| fail(&format!("vacuum {qualified}"), e))?;
            dead = admin
                .query_one(
                    "select coalesce((select n_dead_tup from pg_stat_all_tables \
                                      where relid = $1::text::regclass), 0)::bigint",
                    &[&qualified],
                )
                .await
                .map_err(|e| fail(&format!("dead tuples of {qualified}"), e))?
                .get(0);
            if dead == 0 || attempt + 1 == VACUUM_ATTEMPTS {
                break;
            }
            tokio::time::sleep(VACUUM_RETRY).await;
        }
        let width: i64 = admin
            .query_one(
                &format!("select coalesce(max(pg_column_size(t)), 0)::bigint from {qualified} t"),
                &[],
            )
            .await
            .map_err(|e| fail(&format!("width of {qualified}"), e))?
            .get(0);
        let entry = widths.entry(qualified.clone()).or_insert(0);
        *entry = (*entry).max(width);
        let pages: i64 = admin
            .query_one(
                "select (pg_relation_size($1::text::regclass) / 8192)::bigint",
                &[&qualified],
            )
            .await
            .map_err(|e| fail(&format!("measure {qualified}"), e))?
            .get(0);
        let seen = TableResidue {
            table: qualified.clone(),
            pages,
            dead,
            width: *entry,
        };
        if dead != 0 {
            failures.push(format!(
                "{qualified}: n_dead_tup {dead} after VACUUM; other sessions: {}",
                holders(&admin).await
            ));
        }
        let target = beside.get(&name).unwrap_or(&name);
        let peak = match sources.get(target.as_str()) {
            Some(source) if schema != config.schema() => peaks.get(*source).copied().unwrap_or(0),
            _ => total_peak,
        };
        let bound = pages_bound(peak, seen.width.max(64));
        if pages > bound {
            failures.push(format!(
                "{qualified}: {pages} pages after VACUUM, over {bound} for a peak of {peak} \
                 live rows of {} bytes",
                seen.width
            ));
        }
        out.push(seen);
    }
    drop(admin);
    let _ = connection.await;
    if failures.is_empty() {
        Ok(out)
    } else {
        Err(RunError::Residue { failures })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::trivial_program;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;

    #[test]
    fn the_pages_bound_is_four_times_the_peak_plus_eight() {
        assert_eq!(pages_bound(0, 100), 8);
        // 100 rows of 96 bytes + 4 of line pointer: 10000 bytes, 2 pages.
        assert_eq!(pages_bound(100, 96), 4 * 2 + 8);
        assert_eq!(pages_bound(1, 8000), 4 + 8);
    }

    #[test]
    fn the_peak_is_the_most_rows_the_ops_ever_held() {
        let mut runner = TestRunner::deterministic();
        let strategy = trivial_program();
        for _ in 0..50 {
            let program = strategy.new_tree(&mut runner).unwrap().current();
            let peaks = peak_live_rows(&program);
            let mut live: BTreeMap<&str, i64> = BTreeMap::new();
            for op in &program.ops {
                let now = live.entry(op.table()).or_insert(0);
                let mut write = op.write();
                while let Op::Leaf { op, .. } = write {
                    write = op.write();
                }
                match (write, op.expect()) {
                    (Op::Insert { .. }, OpOutcome::Succeeds) => *now += 1,
                    (Op::BulkInsert { rows, .. }, OpOutcome::Succeeds) => *now += rows.len() as i64,
                    (Op::Delete { .. }, OpOutcome::Succeeds) => *now -= 1,
                    (Op::Truncate { .. }, _) => *now = 0,
                    _ => {}
                }
                assert!(
                    peaks.get(op.table()).copied().unwrap_or(0) >= *now,
                    "{peaks:?} under {live:?}"
                );
            }
        }
    }
}
