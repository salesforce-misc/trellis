//! `trellis status [--database-url <URL>]` — a one-shot, read-only snapshot
//! of what's registered against the configured database.
//!
//! Unlike `define`/`run`, this command never calls `.migrate()`. Those two
//! are already mutating operations (they create/register things), so folding
//! a schema migration into their connect path doesn't add a new kind of
//! surprise. `status` is meant to be safe to run at any time, including
//! against a database an operator doesn't expect to change — so on a
//! genuinely fresh/unmigrated database, the underlying Postgres "relation
//! does not exist" error is left to surface as-is rather than papering over
//! it by migrating on this command's behalf.
//!
//! What this prints today: every registered transform definition
//! ([`Trellis::definitions`]) — id, tables, `created_at`, and its whole-
//! keyspace lifecycle status (issue #55's `TransformStatus`:
//! `waiting_to_backfill`/`backfilling`/`live`/`quarantined`), plus, on a
//! line of its own, the failure of its source's backfill if that keeps
//! failing (`backfill_failure`, issue #461), on another why the drain
//! halted on it, if it did (`halt`, issue #663), and on another the page the
//! drain keeps failing on with nothing charged or paused, if one holds it
//! back (`drain_failure` from [`Trellis::status`], issue #817), and one per
//! join column of a relationship it reads that has no usable index
//! (`unindexed_joins`, issue #973), and one per node of its partitioned
//! source it is being corrected for (`correcting`, ADR-0017). Finer-grained per-`(transform,
//! column)` pause state (docs/decisions/0008-public-api-design.md's
//! decision 5 and
//! docs/decisions/0003-quarantine-storage-and-api.md) is implemented on the
//! engine (`Trellis::quarantined`/`quarantine_status`/`sample_quarantined`)
//! but not yet surfaced by this command — still a real gap, just a smaller
//! one than "no status at all."
//!
//! [`Trellis::poisoned_since`] *is* surfaced here, called with `UNIX_EPOCH`
//! as the watermark. Despite its name suggesting a point-in-time delta, the
//! underlying `poison` table (see `trellis/migrations/V71__poison_per_transform.sql`
//! and `trellis::staging::quarantine`) is a live marker of which
//! `(src_table, key)` pairs are *currently* held for which definition, not an append-only
//! log — rows are deleted on release, not just inserted on poison. So
//! `poisoned_since(UNIX_EPOCH)` returns exactly today's outstanding
//! quarantine set, which is precisely the "what's poisoned right now"
//! snapshot a status command wants; it isn't the unbounded historical log
//! its watermark-shaped signature might suggest.

use std::time::{SystemTime, UNIX_EPOCH};
use trellis::{
    Config, CorrectingNode, DefinitionSummary, DrainFailure, JoinBuildStep, JoinSide, JoinSidePart,
    JoinStatus, SourceIndexGap, Trellis, TrellisOptions, UnindexedJoin,
};

use super::release::shell_quote;

/// Help text for `trellis status -h`/`--help`, and prefixed to any
/// argument-parsing error so a mistake also shows correct usage.
pub const USAGE: &str = "\
Usage: trellis status [--database-url <URL>]

Prints every registered TRANSFORM definition, and a note on
what lifecycle/quarantine status isn't tracked yet (issue #55). Read-only:
unlike `define`/`run`, this does not apply pending migrations, so it fails
plainly against a database with no Trellis schema yet.

Options:
  -d, --database-url <URL>  Postgres connection string. May be given before
                             or after the subcommand name. Falls back to
                             TRELLIS_DATABASE_URL, then PGHOST/PGPORT/PGUSER/
                             PGPASSWORD/PGDATABASE, if omitted.
  -h, --help                 Print this help and exit.
";

/// A parsed `status` invocation. Empty on purpose: once
/// `--database-url`/`-d` has been pulled out of argv by the caller (see
/// `connection::extract_database_url`), `status` takes no other flags and no
/// positional arguments at all.
#[derive(Debug, PartialEq, Eq)]
pub struct Args {}

/// Parses the remaining args after `--database-url`/`-d` (and the `status`
/// subcommand name itself) have been stripped. `status` takes nothing else,
/// so any leftover argument is a usage error rather than something silently
/// ignored.
pub fn parse(args: &[String]) -> Result<Args, String> {
    if args.is_empty() {
        Ok(Args {})
    } else {
        Err(format!(
            "{USAGE}\nerror: trellis status takes no arguments, got {}: {:?}",
            args.len(),
            args
        ))
    }
}

/// Connects (without migrating — see the module doc comment for why),
/// prints the definition listing plus the "status not
/// implemented yet" note, then disconnects.
///
/// `shutdown` is called even if the listing failed, per [`Trellis::shutdown`]'s
/// "correct lifecycle call" contract — but the earlier error (the one an
/// operator actually needs to see) takes priority over a shutdown failure
/// when both occur.
pub async fn run(_args: Args, database_url: Option<String>) -> Result<String, String> {
    let config = Config::resolve(database_url).map_err(|err| err.to_string())?;
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .map_err(|err| err.to_string())?;

    let outcome = report(&trellis).await;
    let shutdown_outcome = trellis.shutdown().await.map_err(|err| err.to_string());

    let message = outcome?;
    shutdown_outcome?;
    Ok(message)
}

/// The note appended to every `status` listing, explaining the remaining gap
/// now that whole-transform lifecycle status (issue #55) is shown per
/// definition below. See the module doc comment for the full reasoning.
const STATUS_NOTE: &str = "\
Note: the status shown per definition above is whole-transform (issue #55). \
Finer-grained per-column pause state is not yet surfaced by this command, \
even though the engine tracks it.";

/// Builds the full human-readable status report: definitions, poisoned keys,
/// then [`STATUS_NOTE`].
async fn report(trellis: &Trellis) -> Result<String, String> {
    let definitions = trellis.definitions().await.map_err(|err| err.to_string())?;
    // `definitions()` doesn't carry a drain failure, the unindexed join
    // columns or the nodes under correction, so each definition's own status
    // is read for them, by the qualified target name `definitions()` lists.
    let mut listed = Vec::with_capacity(definitions.len());
    for def in definitions {
        let status = trellis
            .status(&def.target_table)
            .await
            .map_err(|err| err.to_string())?;
        let warnings = status.map_or_else(StatusWarnings::default, |status| StatusWarnings {
            drain_failure: status.drain_failure,
            unindexed_joins: status.unindexed_joins,
            correcting: status.correcting,
            source_index_gaps: status.source_index_gaps,
            join: status.join,
            paused_upstream_columns: status.paused_upstream_columns,
        });
        listed.push((def, warnings));
    }
    let poisoned = trellis
        .poisoned_since(UNIX_EPOCH)
        .await
        .map_err(|err| err.to_string())?;

    let mut out = String::new();
    out.push_str("Transform definitions:\n");
    out.push_str(&format_definitions(&listed));
    out.push('\n');
    out.push_str(&format_poisoned(&poisoned));
    out.push('\n');
    out.push_str(STATUS_NOTE);

    Ok(out)
}

fn format_poisoned(poisoned: &[trellis::PoisonEntry]) -> String {
    if poisoned.is_empty() {
        return "Quarantined source rows: none\n".to_string();
    }
    let mut out = format!("Quarantined source rows: {}\n", poisoned.len());
    // The transform, table and key are quoted for a POSIX shell where they
    // need it (`release::shell_quote`), so each pastes into `trellis
    // release` as the engine reports it (#842). The error is only read, so
    // it keeps Rust's escaping, which holds it on one line.
    for entry in poisoned {
        out.push_str(&format!(
            "  transform={} table={} key={} error={:?}\n",
            shell_quote(&entry.transform),
            shell_quote(&entry.src_table),
            shell_quote(&entry.key),
            entry.last_error
        ));
    }
    out.push_str(
        "  Fix the cause, then release a key with \
         `trellis release <transform> <table> <key>`, pasting each as shown \
         (quoted for the shell where it needs it).\n",
    );
    out
}

/// What a definition's [`Trellis::status`] reports that its summary doesn't.
#[derive(Debug, Default)]
struct StatusWarnings {
    drain_failure: Option<DrainFailure>,
    unindexed_joins: Vec<UnindexedJoin>,
    correcting: Vec<CorrectingNode>,
    source_index_gaps: Vec<SourceIndexGap>,
    join: Option<JoinStatus>,
    paused_upstream_columns: Vec<String>,
}

/// One line per definition, each followed by an indented line per thing
/// holding it back: a failing backfill, a halt, and the drain failure its
/// status reports, if any; then one per node it is being corrected for, and
/// one per source index it lacks.
fn format_definitions(definitions: &[(DefinitionSummary, StatusWarnings)]) -> String {
    if definitions.is_empty() {
        return "  no transform definitions registered\n".to_string();
    }
    definitions
        .iter()
        .map(|(def, warnings)| {
            let mut line = format!(
                "  target={} status={} created_at={}\n",
                def.target_table,
                def.status.as_str(),
                format_timestamp(def.created_at)
            );
            // Issues #461 and #616: a failing backfill is retried after a
            // backoff, narrowed to the key that fails it, or charged until
            // the definition pauses (#625 F4). While it fails, this line is
            // where the error, attempts and next attempt show; the status
            // alone still reads `backfilling`.
            if let Some(failure) = &def.backfill_failure {
                line.push_str(&format!(
                    "    backfill of {} failing: attempts={} next_attempt_at={} error={:?}\n",
                    failure.source_table,
                    failure.attempts,
                    format_timestamp(failure.next_attempt_at),
                    failure.last_error
                ));
            }
            // Issue #663: the drain halted on it, so it stays paused until
            // the cause is fixed and it is resumed.
            if let Some(halt) = &def.halt {
                line.push_str(&format!(
                    "    halted on {} at {}: error={:?}\n",
                    halt.source_table,
                    format_timestamp(halt.detected_at),
                    halt.error
                ));
            }
            // A page the drain keeps failing on holds the definition's
            // target back without pausing it, so its status alone can read
            // `live`. Every drain pass retries the page until the cause the
            // error names is fixed.
            if let Some(failure) = &warnings.drain_failure {
                line.push_str(&format_drain_failure(failure));
            }
            // ADR-0017: why a definition reads `backfilling` with no build
            // of its own asked for: a partition of its source was attached,
            // detached, dropped or truncated, and its keys are re-derived.
            for node in &warnings.correcting {
                line.push_str(&format_correcting(node));
            }
            // #973: a warning only. The definition reads the column
            // regardless; each read of it scans the table until the index
            // exists. See docs/recommendations.md.
            for join in &warnings.unindexed_joins {
                line.push_str(&format_unindexed_join(join));
            }
            // #1167: a source index the target's `self_check` pages by.
            for gap in &warnings.source_index_gaps {
                line.push_str(&format_source_index_gap(gap));
            }
            // #1208: why a `live` definition reads `catching_up`: it reads
            // a paused column of an upstream target, and resuming that column
            // writes through to it.
            if !warnings.paused_upstream_columns.is_empty() {
                line.push_str(&format!(
                    "    reads paused upstream column(s) {}; resuming them brings it back to live\n",
                    warnings.paused_upstream_columns.join(", ")
                ));
            }
            // #1112: a JOIN's hidden tables and the size of its partials.
            if let Some(join) = &warnings.join {
                line.push_str(&format_join(join));
            }
            line
        })
        .collect()
}

fn format_correcting(node: &CorrectingNode) -> String {
    let remaining = node
        .keys_remaining
        .map_or(String::new(), |keys| format!(" keys_remaining={keys}"));
    format!(
        "    correcting {} since={}{}\n",
        node.node,
        format_timestamp(node.since),
        remaining
    )
}

fn format_unindexed_join(join: &UnindexedJoin) -> String {
    format!(
        "    warning: relationship {} joins on {}.{}, which has no usable index; {}\n",
        join.relationship,
        join.table,
        join.column,
        join.fix()
    )
}

fn format_source_index_gap(gap: &SourceIndexGap) -> String {
    let columns = gap.columns.join(", ");
    if gap.opted_out {
        format!(
            "    warning: runs without a source index on {} ({columns}), as defined WITHOUT \
             SOURCE INDEX; self_check scans the source once per page; {}\n",
            gap.table,
            gap.fix()
        )
    } else {
        format!(
            "    warning: source index on {} ({columns}) is missing; self_check refuses \
             until it exists; {}\n",
            gap.table,
            gap.fix()
        )
    }
}

fn format_join(join: &JoinStatus) -> String {
    let mut out = String::new();
    if !join.hidden_tables.is_empty() {
        let tables: Vec<String> = join
            .hidden_tables
            .iter()
            .map(|t| format!("{}~{}rows/{}B", t.table, t.estimated_rows, t.bytes))
            .collect();
        out.push_str(&format!("    join tables {}\n", tables.join(" ")));
    }
    if let Some(step) = &join.building {
        out.push_str(&format!("    join build {}\n", format_join_step(step)));
    }
    if let Some(w) = &join.partials_warning {
        out.push_str(&format!(
            "    warning: P holds ~{} rows against ~{} in the fact table; \
             the GROUP BY barely compresses it\n",
            w.partials_rows, w.fact_rows
        ));
    }
    out
}

/// The step a `JOIN` build runs now or next ([`JoinStatus::building`]).
fn format_join_step(step: &JoinBuildStep) -> String {
    match step {
        JoinBuildStep::Whole => "both sides in one transaction".to_string(),
        JoinBuildStep::Final => "final step (T and the target)".to_string(),
        JoinBuildStep::Side { side, part } => {
            let side = match side {
                JoinSide::From => "FROM side",
                JoinSide::Joined => "JOIN side",
            };
            match part {
                JoinSidePart::Whole => format!("{side} whole"),
                JoinSidePart::Leaf(name) => format!("{side} leaf {name}"),
                JoinSidePart::Remainder => format!("{side} remainder"),
                JoinSidePart::Unroutable => {
                    format!("{side}: its partition tree can't be routed (see the backfill failure)")
                }
            }
        }
    }
}

fn format_drain_failure(failure: &DrainFailure) -> String {
    let sqlstate = failure
        .sqlstate
        .as_deref()
        .map_or(String::new(), |code| format!(" sqlstate={code}"));
    format!(
        "    drain failing on segment {} ({}): attempts={} since={} last_seen={}{} error={:?}\n",
        failure.seg_seq,
        failure.tables.join(", "),
        failure.attempts,
        format_timestamp(failure.since),
        format_timestamp(failure.last_seen),
        sqlstate,
        failure.error
    )
}

/// Formats a `created_at`/`poisoned_at`-style timestamp as `YYYY-MM-DD
/// HH:MM:SS UTC`. Hand-rolled off [`civil_from_days`] rather than pulling in
/// a date/time crate, matching this workspace's dependency-minimalism
/// convention — the CLI only ever needs to print a handful of these, never
/// parse or do arithmetic on them.
fn format_timestamp(time: SystemTime) -> String {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            let total_secs = duration.as_secs();
            let days = (total_secs / 86_400) as i64;
            let secs_of_day = total_secs % 86_400;
            let (year, month, day) = civil_from_days(days);
            let hour = secs_of_day / 3_600;
            let minute = (secs_of_day % 3_600) / 60;
            let second = secs_of_day % 60;
            format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02} UTC")
        }
        Err(_) => "(timestamp before the Unix epoch)".to_string(),
    }
}

/// Converts a day count since the Unix epoch (1970-01-01) into a
/// proleptic-Gregorian `(year, month, day)`. This is Howard Hinnant's public
/// domain `civil_from_days` algorithm (see
/// <http://howardhinnant.github.io/date_algorithms.html>) — a well-known,
/// dependency-free way to do this conversion without a date/time crate.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn no_args_parses_to_empty() {
        assert_eq!(parse(&[]).unwrap(), Args {});
    }

    #[test]
    fn stray_positional_argument_is_an_error() {
        let err = parse(&["bogus".to_string()]).unwrap_err();
        assert!(err.contains("takes no arguments"));
        assert!(err.contains("Usage: trellis status"));
    }

    #[test]
    fn multiple_stray_arguments_are_reported_together() {
        let args = vec!["one".to_string(), "two".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("got 2"));
    }

    #[test]
    fn a_join_build_step_is_printed_only_while_building() {
        let mut join = JoinStatus {
            hidden_tables: Vec::new(),
            partials_warning: None,
            building: None,
        };
        assert_eq!(format_join(&join), "");
        join.building = Some(JoinBuildStep::Side {
            side: JoinSide::From,
            part: JoinSidePart::Leaf("public.a1".to_string()),
        });
        assert_eq!(
            format_join(&join),
            "    join build FROM side leaf public.a1\n"
        );
        join.building = Some(JoinBuildStep::Final);
        assert_eq!(
            format_join(&join),
            "    join build final step (T and the target)\n"
        );
    }

    #[test]
    fn timestamp_at_unix_epoch() {
        assert_eq!(format_timestamp(UNIX_EPOCH), "1970-01-01 00:00:00 UTC");
    }

    #[test]
    fn timestamp_at_a_known_date() {
        // `date -u -d "2024-01-01 00:00:00" +%s` => 1704067200
        let time = UNIX_EPOCH + Duration::from_secs(1_704_067_200);
        assert_eq!(format_timestamp(time), "2024-01-01 00:00:00 UTC");
    }

    #[test]
    fn timestamp_with_time_of_day_component() {
        // `date -u -d "2000-03-01 12:34:56" +%s` => 951914096
        let time = UNIX_EPOCH + Duration::from_secs(951_914_096);
        assert_eq!(format_timestamp(time), "2000-03-01 12:34:56 UTC");
    }

    #[test]
    fn timestamp_before_epoch_does_not_panic() {
        let time = UNIX_EPOCH - Duration::from_secs(1);
        assert_eq!(format_timestamp(time), "(timestamp before the Unix epoch)");
    }

    #[test]
    fn empty_definitions_say_so_clearly() {
        assert_eq!(
            format_definitions(&[]),
            "  no transform definitions registered\n"
        );
    }

    #[test]
    fn a_definition_is_formatted_with_its_fields() {
        let def = DefinitionSummary {
            target_table: "order_totals".to_string(),
            status: trellis::TransformStatus::Live,
            created_at: UNIX_EPOCH,
            backfill_failure: None,
            halt: None,
        };
        let formatted = format_definitions(&[(def, StatusWarnings::default())]);
        assert!(formatted.contains("target=order_totals"));
        assert!(formatted.contains("status=live"));
        assert!(formatted.contains("1970-01-01 00:00:00 UTC"));
    }

    #[test]
    fn a_healthy_definition_is_one_line() {
        let def = DefinitionSummary {
            target_table: "order_totals".to_string(),
            status: trellis::TransformStatus::Live,
            created_at: UNIX_EPOCH,
            backfill_failure: None,
            halt: None,
        };
        let formatted = format_definitions(&[(def, StatusWarnings::default())]);
        assert_eq!(formatted.lines().count(), 1, "got {formatted:?}");
        assert!(!formatted.contains("backfill"), "got {formatted:?}");
    }

    #[test]
    fn a_failing_backfill_is_shown_under_its_definition() {
        // `date -u -d "2024-01-01 00:00:00" +%s` => 1704067200
        let next_attempt_at = UNIX_EPOCH + Duration::from_secs(1_704_067_200);
        let def = DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: trellis::TransformStatus::WaitingToBackfill,
            created_at: UNIX_EPOCH,
            backfill_failure: Some(trellis::BackfillFailure {
                source_table: "public.orders".to_string(),
                attempts: 3,
                last_error: "table \"public.orders\" has no primary key".to_string(),
                next_attempt_at,
            }),
            halt: None,
        };
        let formatted = format_definitions(&[(def, StatusWarnings::default())]);
        assert_eq!(
            formatted,
            "  target=public.order_totals \
             status=waiting_to_backfill created_at=1970-01-01 00:00:00 UTC\n    \
             backfill of public.orders failing: attempts=3 \
             next_attempt_at=2024-01-01 00:00:00 UTC \
             error=\"table \\\"public.orders\\\" has no primary key\"\n"
        );
    }

    #[test]
    fn a_halt_is_shown_under_its_definition() {
        // `date -u -d "2024-01-01 00:00:00" +%s` => 1704067200
        let detected_at = UNIX_EPOCH + Duration::from_secs(1_704_067_200);
        let def = DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: trellis::TransformStatus::Paused,
            created_at: UNIX_EPOCH,
            backfill_failure: None,
            halt: Some(trellis::CaptureFailure {
                kind: trellis::CaptureFailureKind::Halt,
                source_table: "public.orders".to_string(),
                columns: Vec::new(),
                error: "the drain halted: no primary key".to_string(),
                detected_at,
            }),
        };
        let formatted = format_definitions(&[(def, StatusWarnings::default())]);
        assert_eq!(
            formatted,
            "  target=public.order_totals \
             status=paused created_at=1970-01-01 00:00:00 UTC\n    \
             halted on public.orders at 2024-01-01 00:00:00 UTC: \
             error=\"the drain halted: no primary key\"\n"
        );
    }

    #[test]
    fn a_drain_failure_is_shown_under_its_definition() {
        // `date -u -d "2024-01-01 00:00:00" +%s` => 1704067200
        let since = UNIX_EPOCH + Duration::from_secs(1_704_067_200);
        let def = DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: trellis::TransformStatus::Live,
            created_at: UNIX_EPOCH,
            backfill_failure: None,
            halt: None,
        };
        let failure = DrainFailure {
            seg_seq: 17,
            tables: vec!["public.lines".to_string(), "public.orders".to_string()],
            error: "permission denied for function audit_hook".to_string(),
            sqlstate: Some("42501".to_string()),
            since,
            last_seen: since + Duration::from_secs(90),
            attempts: 5,
        };
        let formatted = format_definitions(&[(
            def,
            StatusWarnings {
                drain_failure: Some(failure),
                ..StatusWarnings::default()
            },
        )]);
        assert_eq!(
            formatted,
            "  target=public.order_totals \
             status=live created_at=1970-01-01 00:00:00 UTC\n    \
             drain failing on segment 17 (public.lines, public.orders): attempts=5 \
             since=2024-01-01 00:00:00 UTC last_seen=2024-01-01 00:01:30 UTC \
             sqlstate=42501 error=\"permission denied for function audit_hook\"\n"
        );
    }

    #[test]
    fn an_unindexed_join_is_shown_under_its_definition_with_its_fix() {
        let def = DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: trellis::TransformStatus::Live,
            created_at: UNIX_EPOCH,
            backfill_failure: None,
            halt: None,
        };
        let warnings = StatusWarnings {
            unindexed_joins: vec![UnindexedJoin {
                relationship: "items".to_string(),
                table: "public.order_items".to_string(),
                column: "order_id".to_string(),
            }],
            ..StatusWarnings::default()
        };
        assert_eq!(
            format_definitions(&[(def, warnings)]),
            "  target=public.order_totals \
             status=live created_at=1970-01-01 00:00:00 UTC\n    \
             warning: relationship items joins on public.order_items.order_id, \
             which has no usable index; \
             create an index on public.order_items (order_id)\n"
        );
    }

    #[test]
    fn a_missing_source_index_is_shown_with_its_fix_and_whether_it_opted_out() {
        let def = || DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: trellis::TransformStatus::Live,
            created_at: UNIX_EPOCH,
            backfill_failure: None,
            halt: None,
        };
        let gap = |opted_out| StatusWarnings {
            source_index_gaps: vec![SourceIndexGap {
                table: "public.orders".to_string(),
                columns: vec!["customer_id".to_string()],
                opted_out,
            }],
            ..StatusWarnings::default()
        };
        let head = "  target=public.order_totals status=live created_at=1970-01-01 00:00:00 UTC\n";
        assert_eq!(
            format_definitions(&[(def(), gap(false))]),
            format!(
                "{head}    warning: source index on public.orders (customer_id) is missing; \
                 self_check refuses until it exists; \
                 create index on \"public\".\"orders\" (\"customer_id\")\n"
            )
        );
        assert_eq!(
            format_definitions(&[(def(), gap(true))]),
            format!(
                "{head}    warning: runs without a source index on public.orders (customer_id), \
                 as defined WITHOUT SOURCE INDEX; self_check scans the source once per page; \
                 create index on \"public\".\"orders\" (\"customer_id\")\n"
            )
        );
    }

    #[test]
    fn each_node_under_correction_is_shown_under_its_definition() {
        let def = DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: trellis::TransformStatus::Backfilling,
            created_at: UNIX_EPOCH,
            backfill_failure: None,
            halt: None,
        };
        let warnings = StatusWarnings {
            correcting: vec![
                CorrectingNode {
                    node: "public.orders_2026".to_string(),
                    since: UNIX_EPOCH + Duration::from_secs(60),
                    keys_remaining: Some(1200),
                },
                CorrectingNode {
                    node: "public.orders_2027".to_string(),
                    since: UNIX_EPOCH + Duration::from_secs(90),
                    keys_remaining: None,
                },
            ],
            ..StatusWarnings::default()
        };
        assert_eq!(
            format_definitions(&[(def, warnings)]),
            "  target=public.order_totals \
             status=backfilling created_at=1970-01-01 00:00:00 UTC\n    \
             correcting public.orders_2026 since=1970-01-01 00:01:00 UTC keys_remaining=1200\n    \
             correcting public.orders_2027 since=1970-01-01 00:01:30 UTC\n"
        );
    }

    #[test]
    fn a_paused_upstream_column_is_named_under_the_catching_up_reader() {
        let def = DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: trellis::TransformStatus::CatchingUp,
            created_at: UNIX_EPOCH,
            backfill_failure: None,
            halt: None,
        };
        let warnings = StatusWarnings {
            paused_upstream_columns: vec!["t.a".to_string(), "t.b".to_string()],
            ..StatusWarnings::default()
        };
        assert_eq!(
            format_definitions(&[(def, warnings)]),
            "  target=public.order_totals \
             status=catching_up created_at=1970-01-01 00:00:00 UTC\n    \
             reads paused upstream column(s) t.a, t.b; resuming them brings it back to live\n"
        );
    }

    #[test]
    fn a_drain_failure_without_a_sqlstate_leaves_it_out() {
        let failure = DrainFailure {
            seg_seq: 3,
            tables: vec!["public.orders".to_string()],
            error: "records fail only together\nDETAIL: twice".to_string(),
            sqlstate: None,
            since: UNIX_EPOCH,
            last_seen: UNIX_EPOCH,
            attempts: 1,
        };
        let formatted = format_drain_failure(&failure);
        assert_eq!(formatted.lines().count(), 1, "got {formatted:?}");
        assert!(!formatted.contains("sqlstate"), "got {formatted:?}");
        assert!(
            formatted.contains(r#"error="records fail only together\nDETAIL: twice""#),
            "got {formatted:?}"
        );
    }

    #[test]
    fn a_multi_line_backfill_error_stays_on_its_line() {
        let def = DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: trellis::TransformStatus::WaitingToBackfill,
            created_at: UNIX_EPOCH,
            backfill_failure: Some(trellis::BackfillFailure {
                source_table: "public.orders".to_string(),
                attempts: 1,
                last_error: "db error: ERROR: permission denied for table orders\n\
                             DETAIL: role lacks SELECT\nHINT: grant it"
                    .to_string(),
                next_attempt_at: UNIX_EPOCH,
            }),
            halt: None,
        };
        let formatted = format_definitions(&[(def, StatusWarnings::default())]);
        assert_eq!(formatted.lines().count(), 2, "got {formatted:?}");
        assert!(
            formatted.contains(r"orders\nDETAIL: role lacks SELECT\nHINT: grant it"),
            "got {formatted:?}"
        );
    }

    #[test]
    fn note_mentions_issue_55() {
        assert!(STATUS_NOTE.contains("#55"));
    }

    #[test]
    fn no_poisoned_entries_say_so_clearly() {
        assert_eq!(format_poisoned(&[]), "Quarantined source rows: none\n");
    }

    #[test]
    fn a_poisoned_entry_is_formatted_with_its_fields() {
        use trellis::PoisonEntry;

        let entry = PoisonEntry {
            transform: "order_totals".to_string(),
            src_table: "orders".to_string(),
            key: "42".to_string(),
            last_error: "division by zero".to_string(),
            poisoned_at: UNIX_EPOCH,
        };
        let formatted = format_poisoned(std::slice::from_ref(&entry));
        assert!(formatted.contains("Quarantined source rows: 1"));
        assert!(formatted.contains("transform=order_totals"));
        assert!(formatted.contains("table=orders"));
        assert!(formatted.contains("key=42 "));
        assert!(formatted.contains("error=\"division by zero\""));
    }

    /// The `transform=`, `table=` and `key=` words of `entry`'s `status`
    /// line, pasted after `trellis release` and split by `shell` as an
    /// operator's shell would, and parsed by `release`.
    fn paste_into_release(
        shell: &str,
        entry: &trellis::PoisonEntry,
    ) -> super::super::release::Args {
        let formatted = format_poisoned(std::slice::from_ref(entry));
        let line = formatted
            .lines()
            .find(|line| line.starts_with("  transform="))
            .expect("the entry's line");
        assert!(!line.chars().any(char::is_control), "{line:?}");
        let words = &line.trim_start()[..line.trim_start().rfind(" error=").expect("error=")];

        // The shell prints each argument NUL-terminated.
        let output = std::process::Command::new(shell)
            .arg("-c")
            .arg(format!("printf '%s\\0' {words}"))
            .output()
            .unwrap_or_else(|err| panic!("run {shell}: {err}"));
        assert!(output.status.success(), "{output:?}");
        let argv: Vec<String> = String::from_utf8(output.stdout)
            .expect("utf-8")
            .split_terminator('\0')
            .zip(["transform=", "table=", "key="])
            .map(|(word, label)| {
                word.strip_prefix(label)
                    .unwrap_or_else(|| panic!("{word:?} starts with {label}"))
                    .to_string()
            })
            .collect();
        super::super::release::parse(&argv).expect("release takes the pasted words")
    }

    fn held(transform: &str, src_table: &str, key: &str) -> trellis::PoisonEntry {
        trellis::PoisonEntry {
            transform: transform.to_string(),
            src_table: src_table.to_string(),
            key: key.to_string(),
            last_error: "division by zero".to_string(),
            poisoned_at: UNIX_EPOCH,
        }
    }

    /// A held key that needs quoting round-trips from `status`'s listing to
    /// `release`'s arguments through a real POSIX shell (#842).
    #[test]
    fn a_held_key_pastes_from_status_into_release_unchanged() {
        for entry in [
            held(
                "order_totals",
                "public.orders",
                r#"it's a "key" \ with $HOME `and` \"#,
            ),
            held("order_totals", "public.orders", "=ls"),
            held("order totals", "public.my orders", "-7"),
        ] {
            assert_eq!(
                paste_into_release("sh", &entry),
                super::super::release::Args {
                    transform: entry.transform,
                    source_table: entry.src_table,
                    key: entry.key,
                }
            );
        }
    }

    /// A key with control characters in it, as every composite key has
    /// (its columns' separator, U+001F) and a NULL column's sentinel
    /// (U+0001) adds, is printed without them raw, and still round-trips:
    /// through bash, which reads the `$'...'` words they're written as
    /// (#842).
    #[test]
    fn a_composite_or_control_character_key_pastes_from_status_into_release_unchanged() {
        for key in [
            "1\u{1f}east",
            "\u{1}\u{1f}it's",
            "line\none\u{1b}[2J\u{85}é",
        ] {
            let entry = held("order_totals", "public.orders", key);
            assert_eq!(
                paste_into_release("bash", &entry),
                super::super::release::Args {
                    transform: entry.transform,
                    source_table: entry.src_table,
                    key: entry.key,
                }
            );
        }
    }
}
