//! `trellis dump` — writes a schema-only dump of the database that carries
//! its Trellis definitions, and none of Trellis's own state
//! ([ADR-0025](../../../docs/decisions/0025-schema-dumps-restore-definitions.md)):
//! `pg_dump --schema-only` with the arguments [`Trellis::schema_dump`]
//! gives, its output through [`Trellis::filter_schema_dump`], then the
//! appendix of definitions. `trellis restore` loads it into another
//! database. `--definitions-only` writes just the appendix, for a dump the
//! operator builds themselves.
//!
//! Read-only, like `status`: it never migrates, so it fails plainly against
//! a database with no Trellis schema.

use std::path::PathBuf;
use trellis::{Config, Trellis, TrellisOptions};

use crate::postgres_tools::{self, MIN_PG_DUMP_MAJOR};

/// Help text for `trellis dump -h`/`--help`, and prefixed to any
/// argument-parsing error so a mistake also shows correct usage.
pub const USAGE: &str = "\
Usage: trellis dump [--database-url <URL>] [--out <FILE>] [--definitions-only]

Writes a schema-only dump of the database that carries its Trellis
definitions: `pg_dump --schema-only --no-owner` of every schema but Trellis's
own, without the 1-1 targets' ledgers and the capture triggers on source
tables, then one line per definition (its source, target and JOIN table, its
text as written, its grammar version and its floor), each reader after its
upstream. `trellis restore` loads the file into an empty database and
restores every definition into its target. The same definitions give the
same appendix, whatever their history.

pg_dump is the first one on PATH; it must be version 15 or later, and no
older than the server. The definitions are read before and after it runs, and
the dump is redone (three times at most) when a define or drop committed in
between.

Options:
  -o, --out <FILE>          Write the dump to FILE (replaced whole, never
                             left half written) instead of stdout.
      --definitions-only     Write only the definitions, without running
                             pg_dump: append them to a `pg_dump
                             --schema-only` you run yourself with the
                             exclusions `docs/embedding.md` (Schema dumps)
                             lists.
  -d, --database-url <URL>  Postgres connection string. May be given before
                             or after the subcommand name. Falls back to
                             TRELLIS_DATABASE_URL, then PGHOST/PGPORT/PGUSER/
                             PGPASSWORD/PGDATABASE, if omitted. pg_dump gets
                             the same connection through its environment.
  -h, --help                 Print this help and exit.
";

/// How many times the dump is taken before giving up on a racing define.
const ATTEMPTS: usize = 3;

/// A parsed `dump` invocation.
#[derive(Debug, PartialEq, Eq)]
pub struct Args {
    pub out: Option<PathBuf>,
    pub definitions_only: bool,
}

/// Parses the remaining args after `--database-url`/`-d` (and the
/// subcommand name itself) have been stripped.
pub fn parse(args: &[String]) -> Result<Args, String> {
    let error = |message: String| format!("{USAGE}\nerror: {message}");
    let mut parsed = Args {
        out: None,
        definitions_only: false,
    };
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-o" | "--out" => {
                let Some(path) = args.next() else {
                    return Err(error(format!("{arg} requires a value, e.g. --out FILE")));
                };
                if parsed.out.is_some() {
                    return Err(error("--out may only be specified once".to_string()));
                }
                parsed.out = Some(PathBuf::from(path));
            }
            "--definitions-only" => parsed.definitions_only = true,
            other => return Err(error(format!("unrecognized argument {other:?}"))),
        }
    }
    Ok(parsed)
}

/// Connects, dumps, and disconnects. Returns what goes to stdout: the dump
/// itself, or a confirmation when it went to `--out`.
pub async fn run(args: Args, database_url: Option<String>) -> Result<String, String> {
    let config = Config::resolve(database_url).map_err(|err| err.to_string())?;
    if !args.definitions_only {
        check_pg_dump().await?;
    }
    let trellis = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .map_err(|err| err.to_string())?;
    let outcome = dump(&trellis, &config, args.definitions_only).await;
    let shutdown_outcome = trellis.shutdown().await.map_err(|err| err.to_string());
    let (text, definitions) = outcome?;
    shutdown_outcome?;

    match args.out {
        None => Ok(text),
        Some(path) => {
            write_whole(&path, &text)?;
            Ok(format!(
                "wrote the dump of {definitions} definition(s) to {}\n",
                path.display()
            ))
        }
    }
}

/// Refuses a `pg_dump` that isn't on PATH or is older than the oldest one
/// whose output the trigger filter is tested against.
async fn check_pg_dump() -> Result<(), String> {
    let mut command = std::process::Command::new("pg_dump");
    command.arg("--version");
    let output = postgres_tools::output("pg_dump", command)
        .await
        .map_err(|err| format!("{err}, or pass --definitions-only"))?;
    let line = String::from_utf8_lossy(&output.stdout);
    match postgres_tools::major_version(&line) {
        Some(major) if major >= MIN_PG_DUMP_MAJOR => Ok(()),
        Some(_) => Err(format!(
            "{} is too old: trellis dump needs pg_dump {MIN_PG_DUMP_MAJOR} or later; put a \
             newer one first on PATH, or pass --definitions-only",
            line.trim()
        )),
        None => Err(format!(
            "could not read pg_dump's version from {:?}",
            line.trim()
        )),
    }
}

/// The dump, and how many definitions it carries.
async fn dump(
    trellis: &Trellis,
    config: &Config,
    definitions_only: bool,
) -> Result<(String, usize), String> {
    let read = || async {
        trellis
            .schema_dump()
            .await
            .map_err(|err| format!("could not read the definitions: {err}"))
    };
    let count = |appendix: &str| {
        appendix
            .lines()
            .filter(|line| line.starts_with("insert into "))
            .count()
    };
    if definitions_only {
        let appendix = read().await?.appendix;
        let definitions = count(&appendix);
        return Ok((appendix, definitions));
    }
    for _ in 0..ATTEMPTS {
        let before = read().await?;
        let mut command = postgres_tools::command("pg_dump", config)?;
        command
            .args(["--schema-only", "--no-owner", "--no-password"])
            .args(&before.pg_dump_args);
        let output = postgres_tools::output("pg_dump", command).await?;
        if !output.status.success() {
            return Err(postgres_tools::failed("pg_dump", &output));
        }
        let after = read().await?;
        if after != before {
            continue;
        }
        let sql = String::from_utf8(output.stdout)
            .map_err(|_| "pg_dump's output is not UTF-8; dump a UTF-8 database".to_string())?;
        let mut text = postgres_tools::loadable(
            &trellis
                .filter_schema_dump(&sql)
                .map_err(|err| err.to_string())?,
        );
        if !text.is_empty() && !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str(&before.appendix);
        return Ok((text, count(&before.appendix)));
    }
    Err(format!(
        "the definitions changed during each of {ATTEMPTS} dumps: dump when nothing is \
         defining or dropping transforms"
    ))
}

/// Writes `text` to a sibling of `path` and renames it over `path`, so a
/// reader never sees it half written.
fn write_whole(path: &std::path::Path, text: &str) -> Result<(), String> {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(format!(".{}.tmp", std::process::id()));
    let temporary = PathBuf::from(temporary);
    std::fs::write(&temporary, text)
        .and_then(|()| std::fs::rename(&temporary, path))
        .map_err(|err| {
            let _ = std::fs::remove_file(&temporary);
            format!("could not write {}: {err}", path.display())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| arg.to_string()).collect()
    }

    #[test]
    fn no_args_dumps_everything_to_stdout() {
        assert_eq!(
            parse(&[]).unwrap(),
            Args {
                out: None,
                definitions_only: false
            }
        );
    }

    #[test]
    fn flags_parse_in_either_order() {
        for args in [
            ["-o", "x.sql", "--definitions-only"].as_slice(),
            ["--definitions-only", "--out", "x.sql"].as_slice(),
        ] {
            assert_eq!(
                parse(&strings(args)).unwrap(),
                Args {
                    out: Some(PathBuf::from("x.sql")),
                    definitions_only: true
                }
            );
        }
    }

    #[test]
    fn bad_flags_are_errors() {
        assert!(
            parse(&strings(&["--out"]))
                .unwrap_err()
                .contains("requires a value")
        );
        assert!(
            parse(&strings(&["-o", "a", "-o", "b"]))
                .unwrap_err()
                .contains("only be specified once")
        );
        assert!(
            parse(&strings(&["extra"]))
                .unwrap_err()
                .contains("unrecognized argument")
        );
    }
}
