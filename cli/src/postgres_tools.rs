//! Running the PostgreSQL client programs `dump` and `restore` need
//! (`pg_dump`, `psql`) against the database Trellis is configured for.
//!
//! The connection reaches the child through the libpq environment
//! ([`trellis::Config::libpq_environment`]), never its command line, where
//! another user of the host could read the password. Every `PG*` variable
//! the CLI inherited is cleared first, so the child connects exactly where
//! the resolved configuration says and nowhere an inherited variable points.

use std::process::{Command, Output, Stdio};
use trellis::Config;

/// The oldest `pg_dump` whose output `filter_schema_dump` is tested against
/// (ADR-0025).
pub const MIN_PG_DUMP_MAJOR: u32 = 15;

/// `program` with the connection of `config` in its environment, its
/// inherited `PG*` variables cleared, and no password prompt.
pub fn command(program: &str, config: &Config) -> Result<Command, String> {
    let mut command = Command::new(program);
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("PG") {
            command.env_remove(name);
        }
    }
    let vars = config.libpq_environment().map_err(|err| err.to_string())?;
    command.envs(vars);
    command.stdin(Stdio::null());
    Ok(command)
}

/// Runs `command` to completion off the async runtime, with its output
/// captured; `program` names it in the error when it can't be started.
pub async fn output(program: &'static str, mut command: Command) -> Result<Output, String> {
    tokio::task::spawn_blocking(move || command.output())
        .await
        .map_err(|err| format!("{program} did not finish: {err}"))?
        .map_err(|err| spawn_error(program, &err))
}

/// Why `program` couldn't be started, with what to do about it.
pub fn spawn_error(program: &str, err: &std::io::Error) -> String {
    if err.kind() == std::io::ErrorKind::NotFound {
        format!(
            "{program} is not on PATH: install the PostgreSQL client programs \
             ({MIN_PG_DUMP_MAJOR} or later, and no older than the server) and put their \
             directory first on PATH"
        )
    } else {
        format!("could not run {program}: {err}")
    }
}

/// `output`'s failure as an error naming `program`, with what it wrote to
/// stderr. Neither program puts a password in its messages.
pub fn failed(program: &str, output: &Output) -> String {
    format!(
        "{program} failed ({}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim_end()
    )
}

/// The major version in a `--version` line such as `pg_dump (PostgreSQL)
/// 17.11`, `pg_dump (PostgreSQL) 18beta1` or `pg_dump (PostgreSQL) 16.4
/// (Debian 16.4-1)`.
pub fn major_version(version_line: &str) -> Option<u32> {
    let version = version_line.split_once(") ")?.1.split_whitespace().next()?;
    let digits: String = version.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// `sql` loadable into a database that already has a `public` schema, as
/// every new database does: `pg_dump` 17 writes `CREATE SCHEMA public;`
/// when it dumps `public`, which fails there. The Rails and Ecto hooks make
/// the same change.
pub fn loadable(sql: &str) -> String {
    sql.split_inclusive('\n')
        .map(|line| {
            if line.trim_end_matches(['\n', '\r']) == "CREATE SCHEMA public;" {
                line.replacen(
                    "CREATE SCHEMA public;",
                    "CREATE SCHEMA IF NOT EXISTS public;",
                    1,
                )
            } else {
                line.to_string()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn major_version_reads_releases_and_prereleases() {
        assert_eq!(major_version("pg_dump (PostgreSQL) 17.11\n"), Some(17));
        assert_eq!(major_version("pg_dump (PostgreSQL) 18beta1"), Some(18));
        assert_eq!(
            major_version("pg_dump (PostgreSQL) 14.2 (Debian 14.2-1)"),
            Some(14)
        );
        assert_eq!(major_version("garbage"), None);
    }

    #[test]
    fn loadable_rewrites_only_the_public_schema_statement() {
        let sql = "CREATE SCHEMA public;\nCREATE SCHEMA app;\n-- CREATE SCHEMA public;\n";
        assert_eq!(
            loadable(sql),
            "CREATE SCHEMA IF NOT EXISTS public;\nCREATE SCHEMA app;\n-- CREATE SCHEMA public;\n"
        );
    }
}
