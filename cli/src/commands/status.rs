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
//! `waiting_to_backfill`/`backfilling`/`live`/`quarantined`) — and every
//! relationship ([`Trellis::relationships`]). Finer-grained per-`(transform,
//! column)` pause state (docs/decisions/0008-public-api-design.md's
//! "Decision 5" and the amendment to
//! docs/decisions/0003-quarantine-storage-and-api.md) is implemented on the
//! engine (`Trellis::quarantined`/`quarantine_status`/`sample_quarantined`)
//! but not yet surfaced by this command — still a real gap, just a smaller
//! one than "no status at all."
//!
//! [`Trellis::poisoned_since`] *is* surfaced here, called with `UNIX_EPOCH`
//! as the watermark. Despite its name suggesting a point-in-time delta, the
//! underlying `poison` table (see `trellis/migrations/V13__quarantine.sql`
//! and `trellis::staging::quarantine`) is a live marker of which
//! `(src_table, key)` pairs are *currently* evicted, not an append-only
//! log — rows are deleted on release, not just inserted on poison. So
//! `poisoned_since(UNIX_EPOCH)` returns exactly today's outstanding
//! quarantine set, which is precisely the "what's poisoned right now"
//! snapshot a status command wants; it isn't the unbounded historical log
//! its watermark-shaped signature might suggest.

use std::time::{SystemTime, UNIX_EPOCH};
use trellis::{Config, DefinitionSummary, RelationshipSummary, Trellis, TrellisOptions};

/// Help text for `trellis status -h`/`--help`, and prefixed to any
/// argument-parsing error so a mistake also shows correct usage.
pub const USAGE: &str = "\
Usage: trellis status [--database-url <URL>]

Prints every registered TRANSFORM definition and RELATIONSHIP, and a note on
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
/// prints the definition/relationship listing plus the "status not
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

/// Builds the full human-readable status report: definitions, relationships,
/// then [`STATUS_NOTE`].
async fn report(trellis: &Trellis) -> Result<String, String> {
    let definitions = trellis.definitions().await.map_err(|err| err.to_string())?;
    let relationships = trellis
        .relationships()
        .await
        .map_err(|err| err.to_string())?;
    let poisoned = trellis
        .poisoned_since(UNIX_EPOCH)
        .await
        .map_err(|err| err.to_string())?;

    let mut out = String::new();
    out.push_str("Transform definitions:\n");
    out.push_str(&format_definitions(&definitions));
    out.push('\n');
    out.push_str("Relationships:\n");
    out.push_str(&format_relationships(&relationships));
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
    for entry in poisoned {
        out.push_str(&format!(
            "  table={} key={:?} error={:?}\n",
            entry.src_table, entry.key, entry.last_error
        ));
    }
    out
}

fn format_definitions(definitions: &[DefinitionSummary]) -> String {
    if definitions.is_empty() {
        return "  no transform definitions registered\n".to_string();
    }
    definitions
        .iter()
        .map(|def| {
            format!(
                "  id={} source={} target={} status={} created_at={}\n",
                def.id,
                def.source_table,
                def.target_table,
                def.status.as_str(),
                format_timestamp(def.created_at)
            )
        })
        .collect()
}

fn format_relationships(relationships: &[RelationshipSummary]) -> String {
    if relationships.is_empty() {
        return "  no relationships registered\n".to_string();
    }
    relationships
        .iter()
        .map(|rel| {
            format!(
                "  id={} name={:?} {}.{}.{} -> {}.{}.{} cardinality={} created_at={}\n",
                rel.id,
                rel.name,
                rel.from_schema,
                rel.from_table,
                rel.from_col,
                rel.to_schema,
                rel.to_table,
                rel.to_col,
                rel.cardinality,
                format_timestamp(rel.created_at)
            )
        })
        .collect()
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
    fn empty_relationships_say_so_clearly() {
        assert_eq!(format_relationships(&[]), "  no relationships registered\n");
    }

    #[test]
    fn a_definition_is_formatted_with_its_fields() {
        let def = DefinitionSummary {
            id: 7,
            target_table: "order_totals".to_string(),
            source_table: "orders".to_string(),
            source_version: 1,
            status: trellis::TransformStatus::Live,
            created_at: UNIX_EPOCH,
        };
        let formatted = format_definitions(std::slice::from_ref(&def));
        assert!(formatted.contains("id=7"));
        assert!(formatted.contains("source=orders"));
        assert!(formatted.contains("target=order_totals"));
        assert!(formatted.contains("status=live"));
        assert!(formatted.contains("1970-01-01 00:00:00 UTC"));
    }

    #[test]
    fn a_relationship_is_formatted_with_its_fields() {
        let rel = RelationshipSummary {
            id: 3,
            name: "posts_by_author".to_string(),
            from_schema: "shop".to_string(),
            from_table: "authors".to_string(),
            from_col: "id".to_string(),
            to_schema: "blog".to_string(),
            to_table: "posts".to_string(),
            to_col: "author_id".to_string(),
            cardinality: "to_many".to_string(),
            created_at: UNIX_EPOCH,
        };
        let formatted = format_relationships(std::slice::from_ref(&rel));
        assert!(formatted.contains("id=3"));
        assert!(formatted.contains("name=\"posts_by_author\""));
        assert!(formatted.contains("shop.authors.id -> blog.posts.author_id"));
        assert!(formatted.contains("cardinality=to_many"));
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
            src_table: "orders".to_string(),
            key: "42".to_string(),
            last_error: "division by zero".to_string(),
            poisoned_at: UNIX_EPOCH,
        };
        let formatted = format_poisoned(std::slice::from_ref(&entry));
        assert!(formatted.contains("Quarantined source rows: 1"));
        assert!(formatted.contains("table=orders"));
        assert!(formatted.contains("key=\"42\""));
        assert!(formatted.contains("error=\"division by zero\""));
    }
}
