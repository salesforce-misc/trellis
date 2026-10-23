//! `trellis apply <GRAMMAR>` — runs one statement of Trellis's grammar
//! against the configured database.
//!
//! One command for every definition-changing operation, because the engine
//! facade has one method for them: [`trellis::Trellis::apply`] takes the
//! statement text and the *parse* decides whether it defines, alters,
//! pauses, resumes, or drops a transform (issue #227, ADR-0012). All this
//! module decides is how to phrase the confirmation it prints.

use trellis::{Applied, Config, Trellis, TrellisOptions};

/// Help text for `trellis apply -h`/`--help`, and prefixed to any
/// argument-parsing error so a mistake also shows correct usage.
pub const USAGE: &str = "\
Usage: trellis apply [--database-url <URL>] <GRAMMAR>

Runs one Trellis statement. <GRAMMAR> is the full statement text as a single,
quoted shell argument. The statement forms:

  trellis apply 'TRANSFORM post_totals FROM posts SELECT count(*) AS n'

  trellis apply 'PAUSE TRANSFORM post_totals'
  trellis apply 'PAUSE TRANSFORM post_totals.n'
  trellis apply 'RESUME TRANSFORM post_totals'
  trellis apply 'RESUME TRANSFORM post_totals.n'
  trellis apply 'DROP TRANSFORM post_totals'

A transform is addressed by its target table, bare (post_totals) or
schema-qualified (public.post_totals); PAUSE and RESUME also take
<transform>.<column>, which names one calculated field and always takes the
bare transform. When a.b reads both ways, it is column b of transform a if a
declares a field b, and otherwise the transform registered as a.b.

PAUSE and DROP are idempotent. RESUME rebuilds by a fresh backfill rather than
catching up. DROP removes the target table's data too, and is refused (naming
the blockers) while another definition still chains off the subject; pause a
definition before dropping it.

`define` is accepted as a deprecated alias for `apply`.

Applies pending migrations first, so this works against a freshly created
database with no separate migrate step.

Options:
  -d, --database-url <URL>  Postgres connection string. May be given before
                             or after the subcommand name. Falls back to
                             TRELLIS_DATABASE_URL, then PGHOST/PGPORT/PGUSER/
                             PGPASSWORD/PGDATABASE, if omitted.
  -h, --help                 Print this help and exit.
";

/// A parsed `apply` invocation: just the statement text, once
/// `--database-url`/`-d` has already been pulled out of argv by the caller
/// (see `connection::extract_database_url`).
#[derive(Debug)]
pub struct Args {
    pub grammar: String,
}

/// Parses the remaining positional args after `--database-url`/`-d` (and the
/// subcommand name itself) have been stripped. Deliberately refuses to
/// reassemble multiple argv words into one statement — a caller who forgot to
/// quote their grammar would otherwise get it silently space-mangled rather
/// than a clear error.
pub fn parse(args: &[String]) -> Result<Args, String> {
    match args {
        [grammar] => Ok(Args {
            grammar: grammar.clone(),
        }),
        [] => Err(format!(
            "{USAGE}\nerror: missing required <GRAMMAR> argument"
        )),
        _ => Err(format!(
            "{USAGE}\nerror: expected exactly one <GRAMMAR> argument (quote it as a single shell \
             argument), got {}: {:?}",
            args.len(),
            args
        )),
    }
}

/// Connects, migrates, runs `args.grammar`, and disconnects, returning a
/// human-readable confirmation on success.
///
/// `shutdown` is called even if migration or the statement itself failed, per
/// [`Trellis::shutdown`]'s "correct lifecycle call" contract — but the
/// earlier error (the one an operator actually needs to see) takes priority
/// over a shutdown failure when both occur.
pub async fn run(args: Args, database_url: Option<String>) -> Result<String, String> {
    let config = Config::resolve(database_url).map_err(|err| err.to_string())?;
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .map_err(|err| err.to_string())?;

    let outcome = apply(&trellis, &args.grammar).await;
    let shutdown_outcome = trellis.shutdown().await.map_err(|err| err.to_string());

    let message = outcome?;
    shutdown_outcome?;
    Ok(message)
}

/// Applies migrations, then runs `grammar` through the one facade entrypoint
/// and describes what came back.
async fn apply(trellis: &Trellis, grammar: &str) -> Result<String, String> {
    trellis.migrate().await.map_err(|err| err.to_string())?;

    let applied = trellis
        .apply(grammar)
        .await
        .map_err(|err| err.to_string())?;
    Ok(describe(applied))
}

/// Phrases one [`Applied`] outcome as a line for an operator's terminal.
///
/// [`Applied`] is `#[non_exhaustive]` (a new statement form is a new variant),
/// so the catch-all arm is required rather than optional — it reports a plain
/// success for a statement this CLI build has no wording for yet, which is
/// better than refusing to print anything about work the engine already did.
fn describe(applied: Applied) -> String {
    match applied {
        Applied::TransformDefined(target_table) => {
            format!("registered transform {target_table}")
        }
        Applied::Paused => "paused".to_string(),
        Applied::Resumed { ref columns } if columns.is_empty() => {
            "resumed; it will rebuild by a fresh backfill".to_string()
        }
        Applied::Resumed { columns } => format!(
            "resumed {}: {}",
            if columns.len() == 1 {
                "1 column".to_string()
            } else {
                format!("{} columns", columns.len())
            },
            columns
                .iter()
                .map(|(transform, column)| format!("{transform}.{column}"))
                .collect::<Vec<_>>()
                .join(", "),
        ),
        Applied::Dropped => "dropped".to_string(),
        _ => "done".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_grammar_is_an_error() {
        let err = parse(&[]).unwrap_err();
        assert!(err.contains("missing required <GRAMMAR>"));
        assert!(err.contains("Usage: trellis apply"));
    }

    #[test]
    fn too_many_args_is_an_error() {
        let args = vec!["TRANSFORM".to_string(), "x FROM y".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("expected exactly one <GRAMMAR> argument"));
    }

    #[test]
    fn single_grammar_arg_parses() {
        let args = vec!["TRANSFORM x FROM y SELECT 1".to_string()];
        let parsed = parse(&args).unwrap();
        assert_eq!(parsed.grammar, "TRANSFORM x FROM y SELECT 1");
    }

    /// The help text has to name every statement form, since this is the only
    /// place an operator learns the grammar from the tool itself.
    #[test]
    fn usage_documents_every_statement_form() {
        for form in [
            "TRANSFORM post_totals FROM",
            "PAUSE TRANSFORM post_totals'",
            "PAUSE TRANSFORM post_totals.n",
            "RESUME TRANSFORM post_totals'",
            "RESUME TRANSFORM post_totals.n",
            "DROP TRANSFORM post_totals",
        ] {
            assert!(USAGE.contains(form), "USAGE is missing {form:?}");
        }
    }

    /// A whole-transform resume and a column resume read differently, because
    /// the second one has specific pairs to name (including any dependent
    /// un-cascaded with it).
    #[test]
    fn a_column_resume_names_the_pairs_it_resumed() {
        assert_eq!(
            describe(Applied::Resumed {
                columns: Vec::new()
            }),
            "resumed; it will rebuild by a fresh backfill"
        );
        assert_eq!(
            describe(Applied::Resumed {
                columns: vec![("order_totals".to_string(), "total".to_string())],
            }),
            "resumed 1 column: order_totals.total"
        );
        assert_eq!(
            describe(Applied::Resumed {
                columns: vec![
                    ("order_totals".to_string(), "total".to_string()),
                    ("order_report".to_string(), "grand".to_string()),
                ],
            }),
            "resumed 2 columns: order_totals.total, order_report.grand"
        );
    }

    #[test]
    fn pause_and_drop_report_plainly() {
        assert_eq!(describe(Applied::Paused), "paused");
        assert_eq!(describe(Applied::Dropped), "dropped");
    }
}
