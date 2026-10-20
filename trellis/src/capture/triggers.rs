//! Disruption of a captured table's capture that Trellis didn't make (#707,
//! with #999's table-gone cases).
//!
//! A statement trigger only captures what it sees. An operator who removes,
//! swaps, disables or recreates Trellis's capture triggers, or replaces a
//! capture function by hand, leaves writes uncaptured that Trellis can't
//! recover from the table's current state, and a repair the pass makes by
//! itself would hide that. So the pass pauses every definition that reads
//! the table, with a `capture_failure` naming what it found, and waits for a
//! `RESUME`, which re-validates the definition against the table now at that
//! name, reinstalls capture, re-records it and rebuilds. Nothing here
//! reinstalls or rebuilds on its own.
//!
//! # What is recorded
//!
//! `capture_triggers` keeps, for each of a plain table's five capture
//! triggers, its `pg_trigger` oid, `xmin` and enabled mode, and each
//! capture function's comment keeps a hash of the source Trellis generated
//! (`sql::comment_text`). An install ([`Mode::Overwrite`]) writes the
//! trigger rows in its own transaction, with the triggers, so a crash
//! mid-install leaves both or neither; a widen or narrow ([`Mode::IfAbsent`])
//! changes no trigger, so it keeps the rows it finds and never absorbs a
//! disruption it didn't see. A table captured with no rows (its capture
//! restored from a dump; the rows are runtime state a dump leaves out) gets
//! them from the next pass.
//!
//! # What pauses
//!
//! The pass compares the live triggers and functions with the record
//! ([`findings`]) and pauses on any of:
//!
//! - **the table is gone** at that name: dropped, renamed away, moved to
//!   another schema ([`pause_readers_of_gone_table`]);
//! - **a recorded trigger is gone** from the table at that name: a swap, a
//!   drop and recreate, the triggers dropped;
//! - **a trigger's oid changed**: recreated by hand or by a schema tool;
//! - **a trigger isn't `ENABLE ALWAYS`**: disabled, `ORIGIN` (what
//!   `ENABLE TRIGGER ALL|USER` leaves) or `REPLICA`;
//! - **a trigger's `xmin` moved** with the same oid: altered by name;
//! - **a capture function's source differs from the hash its own comment
//!   records**: replaced by hand. A source that still matches its hash but
//!   isn't what this build generates is a function an older Trellis left; the
//!   ordinary widen replaces it, with no pause.
//!
//! The record of a restored database is re-recorded instead. A dump and
//! restore, or `pg_upgrade`, recreates every trigger under a new oid, so the
//! rows say which they are: the cluster's `system_identifier` and the oid of
//! `capture_triggers` itself, both written with each row. A stored value that
//! differs from the live one means the rows came from another database, and
//! the pass forgets them. New trigger oids under the same values mean the
//! triggers were recreated in this one: that pauses.
//!
//! Plain tables only: a partitioned root's tree has nodes that come and go
//! with partition maintenance (ADR-0017), and its own checks. A record for a
//! name that a partitioned root holds now is forgotten, not acted on.

use std::collections::BTreeMap;

use tokio_postgres::types::Type;
use tokio_postgres::{Client, GenericClient};

use super::CaptureError;
use super::columns::CaptureCatalog;
use super::sql::{self, CaptureEvent};

/// How [`record`] treats rows already there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// An install (or repair): the triggers are what Trellis just made them.
    Overwrite,
    /// A widen or narrow: no trigger changes, so a table with rows keeps
    /// them.
    IfAbsent,
}

/// Records `table`'s capture triggers as they are now, in the caller's
/// transaction (see the module doc). A table that is not a plain table gets
/// no rows, and loses any it had.
pub(crate) async fn record(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
    mode: Mode,
) -> Result<(), tokio_postgres::Error> {
    if mode == Mode::IfAbsent {
        let has: bool = client
            .query_typed(
                "select exists (select 1 from capture_triggers where table_name = $1)",
                &[(&table, Type::TEXT)],
            )
            .await?
            .first()
            .is_some_and(|row| row.get(0));
        if has {
            return Ok(());
        }
    } else {
        forget(client, table).await?;
    }
    let names: Vec<String> = CaptureEvent::ALL
        .into_iter()
        .map(|event| sql::trigger_name(schema, event))
        .collect();
    client
        .execute_typed(
            "insert into capture_triggers \
                 (table_name, trigger_name, trigger_oid, trigger_xmin, enabled, \
                  system_identifier, catalog_oid) \
             select $1, g.tgname::text, g.oid, g.xmin::text, g.tgenabled, \
                    (select system_identifier::text from pg_catalog.pg_control_system()), \
                    'capture_triggers'::pg_catalog.regclass::pg_catalog.oid \
             from pg_catalog.pg_trigger g \
             join pg_catalog.pg_class c on c.oid = g.tgrelid \
             where c.oid = pg_catalog.to_regclass($2) and c.relkind = 'r' \
               and g.tgname = any($3) and not g.tgisinternal \
             on conflict (table_name, trigger_name) do nothing",
            &[
                (&table, Type::TEXT),
                (&crate::defs::ddl::regclass_arg(table), Type::TEXT),
                (&names, Type::TEXT_ARRAY),
            ],
        )
        .await?;
    Ok(())
}

/// Forgets `table`'s trigger record: its capture is uninstalled, or a resume
/// asks for a fresh one.
pub(crate) async fn forget(
    client: &impl GenericClient,
    table: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute_typed(
            "delete from capture_triggers where table_name = $1",
            &[(&table, Type::TEXT)],
        )
        .await?;
    Ok(())
}

/// Forgets the trigger record of every table but `keep`.
pub(crate) async fn forget_except(
    client: &impl GenericClient,
    keep: &[&str],
) -> Result<(), tokio_postgres::Error> {
    client
        .execute_typed(
            "delete from capture_triggers where table_name <> all($1)",
            &[(&keep, Type::TEXT_ARRAY)],
        )
        .await?;
    Ok(())
}

/// One recorded or live capture trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Trigger {
    pub name: String,
    pub oid: u32,
    pub xmin: String,
    /// `pg_trigger.tgenabled`: `A` is `ENABLE ALWAYS`.
    pub enabled: String,
}

/// One live capture function: its source, and the hash its comment records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Function {
    pub name: String,
    pub source: String,
    /// `None` for a function whose comment records no hash.
    pub recorded_hash: Option<String>,
}

/// One way the live capture differs from what Trellis recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Finding {
    /// A recorded trigger isn't on the table at that name.
    Gone(String),
    /// A trigger has an oid other than the recorded one.
    Recreated(String),
    /// A trigger isn't `ENABLE ALWAYS`, with its mode.
    Mode(String, String),
    /// A trigger's `xmin` moved under the recorded oid.
    Altered(String),
    /// A capture function's source isn't the one its comment hashes.
    FunctionReplaced(String),
}

impl Finding {
    fn describe(&self) -> String {
        match self {
            Finding::Gone(t) => format!("the capture trigger {t} is gone"),
            Finding::Recreated(t) => {
                format!("the capture trigger {t} was dropped and created again")
            }
            Finding::Mode(t, mode) => format!(
                "the capture trigger {t} is {} (it must be ENABLE ALWAYS)",
                match mode.as_str() {
                    "D" => "disabled",
                    "O" => "enabled in ORIGIN mode",
                    "R" => "enabled in REPLICA mode",
                    _ => "not enabled ALWAYS",
                }
            ),
            Finding::Altered(t) => format!("the capture trigger {t} was altered"),
            Finding::FunctionReplaced(f) => {
                format!("the capture function {f} was replaced")
            }
        }
    }
}

/// What the live `triggers` and `functions` show against the `recorded`
/// triggers. Pure. A trigger is reported once, by the first thing wrong with
/// it: missing, recreated, not `ENABLE ALWAYS`, altered.
pub(crate) fn findings(
    recorded: &[Trigger],
    triggers: &[Trigger],
    functions: &[Function],
) -> Vec<Finding> {
    let live: BTreeMap<&str, &Trigger> = triggers.iter().map(|t| (t.name.as_str(), t)).collect();
    let mut found = Vec::new();
    for was in recorded {
        let Some(now) = live.get(was.name.as_str()) else {
            found.push(Finding::Gone(was.name.clone()));
            continue;
        };
        if now.oid != was.oid {
            found.push(Finding::Recreated(was.name.clone()));
        } else if now.enabled != "A" {
            found.push(Finding::Mode(was.name.clone(), now.enabled.clone()));
        } else if now.xmin != was.xmin {
            found.push(Finding::Altered(was.name.clone()));
        }
    }
    for function in functions {
        if let Some(hash) = &function.recorded_hash
            && *hash != sql::body_hash(&function.source)
        {
            found.push(Finding::FunctionReplaced(function.name.clone()));
        }
    }
    found
}

/// The `capture_failure` sentence for `found` on `table`.
fn disrupted_error(table: &str, found: &[Finding]) -> String {
    let what: Vec<String> = found.iter().map(Finding::describe).collect();
    format!(
        "Trellis's capture of {table} was changed outside Trellis: {}. Changes made to the table \
         meanwhile may be missing from this definition, so Trellis can't guarantee it is correct. \
         Resume the definition to reinstall capture and rebuild it, or drop the definition",
        what.join("; ")
    )
}

/// The `capture_failure` sentence for a definition whose `table` is gone.
fn gone_error(table: &str) -> String {
    format!(
        "the table {table} no longer exists at that name (it was dropped, renamed away, moved \
         to another schema, or replaced by something that isn't a table). Changes made to it meanwhile may be missing from this definition, \
         so Trellis can't guarantee it is correct. Put a table there again and resume the \
         definition to re-validate it against that table and rebuild it (a resume refuses while \
         the table is missing), or drop the definition"
    )
}

/// Pauses each definition that reads `table` and isn't paused for a capture
/// failure yet, with `error`.
async fn pause_readers(
    client: &mut Client,
    catalog: &CaptureCatalog,
    table: &str,
    error: &str,
) -> Result<(), CaptureError> {
    let readers = crate::staging::schema_change::unpaused_readers(catalog, table);
    if readers.is_empty() {
        return Ok(());
    }
    let txn = client.transaction().await?;
    for id in readers {
        if crate::defs::lifecycle::pause_for_disruption(&txn, id, table, error).await? {
            crate::instance_log::warn!(transform_id = id, table = %table, "definition paused: {error}");
        }
    }
    txn.commit().await?;
    Ok(())
}

/// The capture pass's check that `table`, which some definition reads, still
/// exists at that name as a table (plain or partitioned). When it doesn't,
/// pauses every definition that reads it and isn't paused yet (#999) and
/// returns `true`, and the caller leaves the table alone: its capture can't
/// be installed, and a table swapped in meanwhile is for a resume to
/// re-validate. A view, materialized view or foreign table put at the name
/// (a table renamed away behind a compatibility view, say) is no table: the
/// table is gone from the name all the same.
///
/// A partitioned source the pass has recorded is left to
/// [`super::partitions`], which tells a root renamed away from a gone one.
pub(crate) async fn pause_readers_of_gone_table(
    client: &mut Client,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<bool, CaptureError> {
    let exists: bool = client
        .query_typed(
            "select exists (select 1 from pg_catalog.pg_class c \
                 where c.oid = pg_catalog.to_regclass($1) and c.relkind in ('r', 'p'))",
            &[(&crate::defs::ddl::regclass_arg(table), Type::TEXT)],
        )
        .await?
        .first()
        .is_some_and(|row| row.get(0));
    if exists
        || super::partitions::recorded(&*client, table)
            .await?
            .is_some()
    {
        return Ok(false);
    }
    pause_readers(client, catalog, table, &gone_error(table)).await?;
    Ok(true)
}

/// Whether the rows of `capture_triggers` for `table` came from another
/// database: a stored `system_identifier` or `catalog_oid` that isn't the
/// live one.
async fn restored(client: &impl GenericClient, table: &str) -> Result<bool, CaptureError> {
    Ok(client
        .query_typed(
            "select exists (select 1 from capture_triggers \
                 where table_name = $1 \
                   and (system_identifier is distinct from \
                            (select system_identifier::text from pg_catalog.pg_control_system()) \
                        or catalog_oid is distinct from \
                            'capture_triggers'::pg_catalog.regclass::pg_catalog.oid))",
            &[(&table, Type::TEXT)],
        )
        .await?
        .first()
        .is_some_and(|row| row.get(0)))
}

/// Whether `table` is a plain table (`relkind = 'r'`).
async fn plain_table(client: &impl GenericClient, table: &str) -> Result<bool, CaptureError> {
    Ok(client
        .query_typed(
            "select exists (select 1 from pg_catalog.pg_class c \
                 where c.oid = pg_catalog.to_regclass($1) and c.relkind = 'r')",
            &[(&crate::defs::ddl::regclass_arg(table), Type::TEXT)],
        )
        .await?
        .first()
        .is_some_and(|row| row.get(0)))
}

async fn recorded_triggers(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<Trigger>, CaptureError> {
    Ok(client
        .query_typed(
            "select trigger_name, trigger_oid, trigger_xmin, enabled::text \
             from capture_triggers where table_name = $1 order by trigger_name",
            &[(&table, Type::TEXT)],
        )
        .await?
        .into_iter()
        .map(|row| Trigger {
            name: row.get(0),
            oid: row.get(1),
            xmin: row.get(2),
            enabled: row.get(3),
        })
        .collect())
}

/// The live capture triggers of the plain table `table`; empty for a table
/// that isn't one.
async fn live_triggers(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
) -> Result<Vec<Trigger>, CaptureError> {
    let names: Vec<String> = CaptureEvent::ALL
        .into_iter()
        .map(|event| sql::trigger_name(schema, event))
        .collect();
    Ok(client
        .query_typed(
            "select g.tgname::text, g.oid, g.xmin::text, g.tgenabled::text \
             from pg_catalog.pg_trigger g \
             join pg_catalog.pg_class c on c.oid = g.tgrelid \
             where c.oid = pg_catalog.to_regclass($1) and c.relkind = 'r' \
               and g.tgname = any($2) and not g.tgisinternal \
             order by g.tgname",
            &[
                (&crate::defs::ddl::regclass_arg(table), Type::TEXT),
                (&names, Type::TEXT_ARRAY),
            ],
        )
        .await?
        .into_iter()
        .map(|row| Trigger {
            name: row.get(0),
            oid: row.get(1),
            xmin: row.get(2),
            enabled: row.get(3),
        })
        .collect())
}

/// `table`'s capture functions that exist, with the hash each comment records.
async fn live_functions(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
) -> Result<Vec<Function>, CaptureError> {
    let names: Vec<String> = CaptureEvent::ALL
        .into_iter()
        .map(|event| sql::function_name(table, event))
        .collect::<Result<_, _>>()?;
    Ok(client
        .query_typed(
            "select p.proname::text, p.prosrc, \
                    case when d.description like '{\"trellis_capture\":1,%' \
                         then d.description::jsonb ->> 'body' end \
             from pg_catalog.pg_proc p \
             left join pg_catalog.pg_description d \
               on d.objoid = p.oid \
              and d.classoid = 'pg_catalog.pg_proc'::pg_catalog.regclass and d.objsubid = 0 \
             where p.pronamespace = (select oid from pg_catalog.pg_namespace where nspname = $1) \
               and p.pronargs = 0 and p.proname = any($2) \
             order by p.proname",
            &[(&schema, Type::TEXT), (&names, Type::TEXT_ARRAY)],
        )
        .await?
        .into_iter()
        .map(|row| Function {
            name: row.get(0),
            source: row.get(1),
            recorded_hash: row.get(2),
        })
        .collect())
}

/// The capture pass's check of `table`'s capture against what Trellis
/// recorded (see the module doc). Returns whether it found a disruption, in
/// which case it has paused the table's readers and the caller installs
/// nothing on the table.
///
/// With no record, a complete capture is recorded as it is, for the next pass
/// to compare; a record from another database is forgotten the same way.
pub(crate) async fn check(
    client: &mut Client,
    schema: &str,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<bool, CaptureError> {
    let mut recorded = recorded_triggers(&*client, table).await?;
    // A partitioned root (or anything but a plain table) holds the name: its
    // tree is `partitions`', and nothing recorded for a plain table applies.
    if !recorded.is_empty() && !plain_table(&*client, table).await? {
        forget(&*client, table).await?;
        return Ok(false);
    }
    if !recorded.is_empty() && restored(&*client, table).await? {
        forget(&*client, table).await?;
        recorded.clear();
    }
    if recorded.is_empty() {
        if matches!(
            super::install::installed(&*client, schema, table).await?,
            super::install::Installed::Complete { .. }
        ) {
            record(&*client, schema, table, Mode::Overwrite).await?;
        }
        return Ok(false);
    }
    let triggers = live_triggers(&*client, schema, table).await?;
    let functions = live_functions(&*client, schema, table).await?;
    let found = findings(&recorded, &triggers, &functions);
    if found.is_empty() {
        return Ok(false);
    }
    pause_readers(client, catalog, table, &disrupted_error(table, &found)).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trigger(name: &str, oid: u32, xmin: &str, enabled: &str) -> Trigger {
        Trigger {
            name: name.to_string(),
            oid,
            xmin: xmin.to_string(),
            enabled: enabled.to_string(),
        }
    }

    fn recorded() -> Vec<Trigger> {
        vec![
            trigger("t_a", 10, "100", "A"),
            trigger("t_b", 11, "100", "A"),
        ]
    }

    fn function(source: &str, hash_of: &str) -> Function {
        Function {
            name: "f".to_string(),
            source: source.to_string(),
            recorded_hash: Some(sql::body_hash(hash_of)),
        }
    }

    #[test]
    fn untouched_capture_finds_nothing() {
        assert_eq!(
            findings(&recorded(), &recorded(), &[function("body", "body")]),
            vec![]
        );
    }

    #[test]
    fn each_kind_of_disruption_is_found_once_per_trigger() {
        let found = findings(
            &recorded(),
            &[trigger("t_a", 99, "200", "D")],
            &[function("edited", "body")],
        );
        assert_eq!(
            found,
            vec![
                Finding::Recreated("t_a".into()),
                Finding::Gone("t_b".into()),
                Finding::FunctionReplaced("f".into()),
            ]
        );
        assert_eq!(
            findings(
                &recorded(),
                &[
                    trigger("t_a", 10, "100", "O"),
                    trigger("t_b", 11, "300", "A")
                ],
                &[]
            ),
            vec![
                Finding::Mode("t_a".into(), "O".into()),
                Finding::Altered("t_b".into()),
            ]
        );
    }

    #[test]
    fn a_function_with_no_recorded_hash_is_not_judged() {
        let f = Function {
            name: "f".into(),
            source: "anything".into(),
            recorded_hash: None,
        };
        assert_eq!(findings(&[], &[], &[f]), vec![]);
    }

    #[test]
    fn the_error_says_what_was_found_and_that_correctness_is_not_guaranteed() {
        let text = disrupted_error(
            "public.t",
            &[Finding::Mode("trellis_capture_insert".into(), "O".into())],
        );
        assert!(text.contains("ORIGIN"), "{text}");
        assert!(text.contains("can't guarantee"), "{text}");
        assert!(text.contains("Resume"), "{text}");
    }
}
