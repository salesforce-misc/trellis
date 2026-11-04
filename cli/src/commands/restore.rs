//! `trellis restore <FILE>` — loads a dump `trellis dump` wrote into an
//! empty database, then runs `migrate`, which creates Trellis's schema and
//! restores each definition the dump queued into the target it created
//! ([ADR-0025](../../../docs/decisions/0025-schema-dumps-restore-definitions.md)).
//!
//! The load runs `psql`, because the file is `pg_dump`'s plain-text output,
//! which is a `psql` script: `pg_dump` 15.14, 16.10, 17.6 and later write
//! `\restrict` meta-commands into it, which only `psql` reads. It runs in one
//! transaction behind a check that the database is empty, so a file that
//! fails to load, or a database that isn't empty, leaves the database as it
//! was.

use std::io::Write as _;
use std::path::PathBuf;
use std::process::Stdio;
use trellis::{Config, Trellis, TrellisOptions};

use crate::postgres_tools;

/// Help text for `trellis restore -h`/`--help`, and prefixed to any
/// argument-parsing error so a mistake also shows correct usage.
pub const USAGE: &str = "\
Usage: trellis restore [--database-url <URL>] <FILE>

Loads FILE, a dump `trellis dump` wrote, into an empty database, then
creates Trellis's schema there and restores each definition the dump carries
into the target table it created, each reader after its upstream. Every
definition is restored unbuilt; `trellis run` builds it from the source rows.

The database must be empty: no schema but `public`, and nothing in `public`.
Create it first (`createdb`). The file is loaded with psql, the first one on
PATH, in one transaction: if any statement fails, or the database isn't
empty, nothing is loaded. A `CREATE SCHEMA public;` line in the file is run
as `CREATE SCHEMA IF NOT EXISTS public;`. pg_dump 15.14, 16.10, 17.6 and
later write `\\restrict` lines, which psql reads from those same releases on
(15.14 or a later 15, 16.10 or a later 16, 17.6 or a later 17, or 18); an
older psql stops at them and loads nothing.

A definition the restore refuses (a target_schema setting that resolves a
bare name elsewhere, or a target it can't adopt) fails the command naming it.
The definitions before it stay restored and the rest stay queued; fix the
cause, then any command that migrates (`trellis run`, `trellis apply`) goes on
with the rest.

Options:
  -d, --database-url <URL>  Postgres connection string of the database to
                             load into. May be given before or after the
                             subcommand name. Falls back to
                             TRELLIS_DATABASE_URL, then PGHOST/PGPORT/PGUSER/
                             PGPASSWORD/PGDATABASE, if omitted. psql gets the
                             same connection through its environment.
  -h, --help                 Print this help and exit.
";

/// Refuses to load into a database that has anything in it but an empty
/// `public` schema. Run first, in the load's transaction.
const EMPTY_DATABASE_CHECK: &str = "\
do $trellis_restore$ begin
  if exists (select from pg_catalog.pg_namespace n
             where n.nspname not in ('pg_catalog', 'information_schema', 'public')
               and n.nspname !~ '^pg_(toast|temp_|toast_temp_)')
     or exists (select from pg_catalog.pg_class c
                join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                where n.nspname = 'public')
     or exists (select from pg_catalog.pg_type t
                join pg_catalog.pg_namespace n on n.oid = t.typnamespace
                where n.nspname = 'public')
     or exists (select from pg_catalog.pg_proc p
                join pg_catalog.pg_namespace n on n.oid = p.pronamespace
                where n.nspname = 'public')
  then
    raise exception 'trellis restore loads only into an empty database: this one has a schema '
      'other than public, or something in public; create an empty one with createdb';
  end if;
end $trellis_restore$";

/// A parsed `restore` invocation.
#[derive(Debug, PartialEq, Eq)]
pub struct Args {
    pub file: PathBuf,
}

/// Parses the remaining args after `--database-url`/`-d` (and the
/// subcommand name itself) have been stripped: one file, after an optional
/// `--` that lets its name start with `-`.
pub fn parse(args: &[String]) -> Result<Args, String> {
    let args = match args {
        [first, rest @ ..] if first == "--" => rest,
        [first, ..] if first.starts_with('-') => {
            return Err(format!("{USAGE}\nerror: unrecognized argument {first:?}"));
        }
        _ => args,
    };
    match args {
        [file] => Ok(Args {
            file: PathBuf::from(file),
        }),
        _ => Err(format!(
            "{USAGE}\nerror: expected one <FILE>, got {} argument(s): {args:?}",
            args.len()
        )),
    }
}

/// Loads the file, migrates, and disconnects, returning a confirmation.
pub async fn run(args: Args, database_url: Option<String>) -> Result<String, String> {
    let sql = std::fs::read(&args.file)
        .map_err(|err| format!("could not read {}: {err}", args.file.display()))?;
    let sql = String::from_utf8(sql)
        .map_err(|_| format!("{} is not UTF-8, so it isn't a dump", args.file.display()))?;
    let config = Config::resolve(database_url).map_err(|err| err.to_string())?;
    check_queue_schema(&sql, &config)?;

    load(&config, postgres_tools::loadable(&sql)).await?;

    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .map_err(|err| format!("loaded {}, but {err}", args.file.display()))?;
    let outcome = restore(&trellis).await;
    let shutdown_outcome = trellis.shutdown().await.map_err(|err| err.to_string());
    let definitions =
        outcome.map_err(|err| format!("loaded {}, but {err}", args.file.display()))?;
    shutdown_outcome?;
    Ok(format!(
        "loaded {} and restored {definitions} definition(s); `trellis run` builds them",
        args.file.display()
    ))
}

/// Refuses a dump whose definitions are queued in another Trellis schema
/// than this instance's (`TRELLIS_SCHEMA`): `migrate` would never find
/// them. A file without a queue (the format written before it, which
/// carries the catalog rows) passes.
fn check_queue_schema(sql: &str, config: &Config) -> Result<(), String> {
    const QUEUE: &str = ".restore_queue (ordinal integer primary key";
    let ours = format!(
        "create table \"{}\"{QUEUE}",
        config.schema().replace('"', "\"\"")
    );
    let queued_elsewhere = sql
        .lines()
        .any(|line| line.starts_with("create table ") && line.contains(QUEUE))
        && !sql.lines().any(|line| line.starts_with(&ours));
    if queued_elsewhere {
        return Err(format!(
            "the dump queues its definitions in another Trellis schema than this instance's \
             ({:?}); set TRELLIS_SCHEMA to the schema of the instance it was dumped from",
            config.schema()
        ));
    }
    Ok(())
}

/// Runs `sql` through psql in one transaction, after the empty-database
/// check.
async fn load(config: &Config, sql: String) -> Result<(), String> {
    let mut command = postgres_tools::command("psql", config)?;
    command
        .args([
            "--no-psqlrc",
            "--quiet",
            "--no-password",
            "--single-transaction",
        ])
        .args([
            "--set",
            "ON_ERROR_STOP=1",
            "--command",
            EMPTY_DATABASE_CHECK,
        ])
        .args(["--file", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let output = tokio::task::spawn_blocking(move || {
        let mut child = command.spawn()?;
        let mut stdin = child.stdin.take().expect("piped stdin");
        // psql stops reading at the first error, which closes the pipe:
        // that failure is in its exit status and stderr, not here.
        let _ = stdin.write_all(sql.as_bytes());
        drop(stdin);
        child.wait_with_output()
    })
    .await
    .map_err(|err| format!("psql did not finish: {err}"))?
    .map_err(|err| postgres_tools::spawn_error("psql", &err))?;
    if !output.status.success() {
        return Err(format!(
            "{}; nothing was loaded",
            postgres_tools::failed("psql", &output)
        ));
    }
    Ok(())
}

/// `migrate`, which restores the queue, then how many definitions the
/// database has.
async fn restore(trellis: &Trellis) -> Result<usize, String> {
    trellis.migrate().await.map_err(|err| err.to_string())?;
    let definitions = trellis.definitions().await.map_err(|err| err.to_string())?;
    Ok(definitions.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| arg.to_string()).collect()
    }

    #[test]
    fn one_file_parses() {
        assert_eq!(
            parse(&strings(&["dump.sql"])).unwrap().file,
            PathBuf::from("dump.sql")
        );
        assert_eq!(
            parse(&strings(&["--", "-dump.sql"])).unwrap().file,
            PathBuf::from("-dump.sql")
        );
    }

    #[test]
    fn anything_but_one_file_is_an_error() {
        assert!(parse(&[]).unwrap_err().contains("expected one <FILE>"));
        assert!(
            parse(&strings(&["a.sql", "b.sql"]))
                .unwrap_err()
                .contains("expected one <FILE>")
        );
        assert!(
            parse(&strings(&["--bogus", "a.sql"]))
                .unwrap_err()
                .contains("unrecognized argument")
        );
    }

    #[test]
    fn a_queue_in_another_schema_is_refused() {
        let config = Config::with_schema("host=x", "trellis").unwrap();
        let queue = |schema: &str| {
            format!(
                "create table \"{schema}\".restore_queue (ordinal integer primary key, \
                 source_table text not null);\n"
            )
        };
        assert!(check_queue_schema(&queue("trellis"), &config).is_ok());
        assert!(check_queue_schema("CREATE TABLE public.t (id int);\n", &config).is_ok());
        let err = check_queue_schema(&queue("other"), &config).unwrap_err();
        assert!(err.contains("TRELLIS_SCHEMA"), "{err}");
    }
}
