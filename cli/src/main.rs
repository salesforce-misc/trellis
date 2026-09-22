//! `trellis` — the operator CLI for the Trellis engine.
//!
//! A thin argv-parsing/dispatch layer over [`trellis::Trellis`]: this binary
//! exists so operators can register definitions, run the live pipeline, and
//! inspect status without writing Rust against the trellis crate directly.
//! No `clap` (or any arg-parsing crate) and no `thiserror`/`anyhow`, matching
//! the workspace's dependency-minimalism convention (see
//! `trellis/src/error.rs`'s doc comment) — argv parsing is hand-rolled and
//! small enough not to need a framework.
//!
//! Each subcommand lives in its own `commands::<name>` module; this file is
//! just the top-level usage/help and the match that dispatches to one.
//! Today that's `apply`, `run`, and `status`.

mod commands;
mod connection;

use std::process::ExitCode;

const USAGE: &str = "\
Usage: trellis <COMMAND> [OPTIONS]

Commands:
  apply <GRAMMAR>     Run one Trellis statement: TRANSFORM, RELATIONSHIP,
                       PAUSE, RESUME or DROP. (`define` is a deprecated alias.)
  run                 Run the live CDC/apply pipeline until interrupted.
  status              Print registered definitions/relationships and exit.

Options:
  -d, --database-url <URL>  Postgres connection string. May be given before
                             or after the subcommand name (e.g. both
                             `trellis --database-url <URL> apply ...` and
                             `trellis apply --database-url <URL> ...`
                             work). Falls back to TRELLIS_DATABASE_URL, then
                             PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE, if
                             omitted.
  -h, --help                 Print this help and exit.

Run `trellis <COMMAND> --help` for command-specific help.
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    run(args)
}

/// The testable core of `main`: takes argv (already stripped of the program
/// name) and returns the process exit code, printing usage/output/errors to
/// the appropriate stream along the way.
fn run(mut args: Vec<String>) -> ExitCode {
    if args.is_empty() {
        eprint!("{USAGE}");
        return ExitCode::FAILURE;
    }
    if wants_help(&args[..1]) {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
    }

    let database_url = match connection::extract_database_url(&mut args) {
        Ok(url) => url,
        Err(message) => {
            eprintln!("error: {message}");
            eprint!("{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    // `extract_database_url` only ever removes entries, and `args` was
    // non-empty above, but it could have removed the sole remaining entry if
    // that entry *was* the flag itself with no subcommand around it (e.g.
    // `trellis --database-url <URL>` with nothing else) — re-check.
    if args.is_empty() {
        eprint!("{USAGE}");
        return ExitCode::FAILURE;
    }

    let command = args.remove(0);
    match command.as_str() {
        // `define` predates the unified entrypoint (issue #227), when the
        // CLI had one command per typed facade method. Kept as an alias so an
        // operator's existing scripts and runbooks don't break over a rename.
        "apply" | "define" => run_apply(args, database_url),
        "run" => run_run(args, database_url),
        "status" => run_status(args, database_url),
        _ if wants_help(std::slice::from_ref(&command)) => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        other => {
            eprintln!("error: unknown command {other:?}");
            eprint!("{USAGE}");
            ExitCode::FAILURE
        }
    }
}

/// Whether any of `args` is a help flag (`-h`/`--help`).
fn wants_help(args: &[String]) -> bool {
    args.iter().any(|a| a == "-h" || a == "--help")
}

/// Dispatches `trellis apply`: handles `-h`/`--help` itself (so it works
/// without a database connection), otherwise parses the grammar argument and
/// runs it against a single-use tokio runtime.
fn run_apply(args: Vec<String>, database_url: Option<String>) -> ExitCode {
    if wants_help(&args) {
        print!("{}", commands::apply::USAGE);
        return ExitCode::SUCCESS;
    }

    let parsed = match commands::apply::parse(&args) {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("error: failed to start async runtime: {err}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(commands::apply::run(parsed, database_url)) {
        Ok(message) => {
            println!("{message}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

/// Dispatches `trellis run`: handles `-h`/`--help` itself (so it works
/// without a database connection), otherwise parses the flags and runs the
/// live pipeline until interrupted, on a single-use tokio runtime.
fn run_run(args: Vec<String>, database_url: Option<String>) -> ExitCode {
    if wants_help(&args) {
        print!("{}", commands::run::USAGE);
        return ExitCode::SUCCESS;
    }

    let parsed = match commands::run::parse(&args) {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("error: failed to start async runtime: {err}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(commands::run::run(parsed, database_url)) {
        Ok(message) => {
            println!("{message}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

/// Dispatches `trellis status`: handles `-h`/`--help` itself (so it works
/// without a database connection), otherwise parses the (empty) argv and
/// runs it against a single-use tokio runtime.
fn run_status(args: Vec<String>, database_url: Option<String>) -> ExitCode {
    if wants_help(&args) {
        print!("{}", commands::status::USAGE);
        return ExitCode::SUCCESS;
    }

    let parsed = match commands::status::parse(&args) {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("error: failed to start async runtime: {err}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(commands::status::run(parsed, database_url)) {
        Ok(message) => {
            println!("{message}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wants_help_detects_both_spellings() {
        assert!(wants_help(&["-h".to_string()]));
        assert!(wants_help(&["--help".to_string()]));
        assert!(!wants_help(&["apply".to_string()]));
        assert!(!wants_help(&[]));
    }

    #[test]
    fn no_args_is_a_failure() {
        assert_eq!(run(vec![]), ExitCode::FAILURE);
    }

    #[test]
    fn top_level_help_is_success() {
        assert_eq!(run(vec!["--help".to_string()]), ExitCode::SUCCESS);
        assert_eq!(run(vec!["-h".to_string()]), ExitCode::SUCCESS);
    }

    #[test]
    fn unknown_command_is_a_failure() {
        assert_eq!(run(vec!["frobnicate".to_string()]), ExitCode::FAILURE);
    }

    #[test]
    fn apply_help_is_success_without_a_database() {
        assert_eq!(
            run(vec!["apply".to_string(), "--help".to_string()]),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn apply_missing_grammar_is_a_failure() {
        assert_eq!(run(vec!["apply".to_string()]), ExitCode::FAILURE);
    }

    /// `define` is a deprecated alias for `apply` (issue #227) — it must keep
    /// reaching the same command rather than falling through to
    /// "unknown command".
    #[test]
    fn define_is_still_accepted_as_an_alias() {
        assert_eq!(
            run(vec!["define".to_string(), "--help".to_string()]),
            ExitCode::SUCCESS
        );
        assert_eq!(run(vec!["define".to_string()]), ExitCode::FAILURE);
    }

    #[test]
    fn run_help_is_success_without_a_database() {
        assert_eq!(
            run(vec!["run".to_string(), "--help".to_string()]),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn run_bad_flags_is_a_failure_without_a_database() {
        // A bad --drain-threads value must be rejected during argv parsing,
        // before any attempt to connect — otherwise this test would hang or
        // fail for the wrong reason in an environment with no database.
        assert_eq!(
            run(vec![
                "run".to_string(),
                "--drain-threads".to_string(),
                "banana".to_string()
            ]),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn run_bad_prometheus_bind_is_a_failure_without_a_database() {
        // A malformed --prometheus-bind value must be rejected during argv
        // parsing, before any attempt to connect or bind.
        assert_eq!(
            run(vec![
                "run".to_string(),
                "--prometheus-bind".to_string(),
                "banana".to_string()
            ]),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn run_conflicting_staging_flags_is_a_failure_without_a_database() {
        assert_eq!(
            run(vec![
                "run".to_string(),
                "--staging".to_string(),
                "--no-staging".to_string()
            ]),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn status_help_is_success_without_a_database() {
        assert_eq!(
            run(vec!["status".to_string(), "--help".to_string()]),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn status_stray_argument_is_a_failure_without_a_database() {
        // A stray positional argument must be rejected during argv parsing,
        // before any attempt to connect — otherwise this test would hang or
        // fail for the wrong reason in an environment with no database.
        assert_eq!(
            run(vec!["status".to_string(), "bogus".to_string()]),
            ExitCode::FAILURE
        );
    }
}
