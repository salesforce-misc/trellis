//! `trellis-testkit`: a throwaway Postgres for test suites that aren't
//! written in Rust (the Elixir and Ruby bindings, epic #140).
//!
//! It starts a [`TestCluster`], the same cluster the engine's own tests run
//! against (same `initdb` flags, same `postgres -c` settings, unix socket
//! only), creates one empty database on it, and prints one line of JSON on
//! stdout describing how to reach it:
//!
//! ```json
//! {"dsn":"host=/tmp/trellis-testkit-123-0/sock port=5432 user=postgres dbname=trellis_test",
//!  "host":"/tmp/trellis-testkit-123-0/sock","port":5432,"user":"postgres",
//!  "dbname":"trellis_test","data_dir":"/tmp/trellis-testkit-123-0/data",
//!  "log_file":"/tmp/trellis-testkit-123-0/postgres.log","pid":4242}
//! ```
//!
//! (on one line in the real output). `dsn` is a libpq keyword/value string,
//! which `trellis` itself and Ruby's `pg` gem (`PG.connect(dsn)`) take as-is.
//! `host` is the socket *directory*, which is what libpq's `host` means when
//! it starts with a `/`; Postgrex takes it as `socket_dir: host`. `pid` is
//! the postmaster's, and `log_file` is the server log, worth dumping when a
//! host suite fails. The database is empty: run the engine's migrations
//! through the binding under test, the way an embedder would.
//!
//! Then it holds the cluster until one of:
//!
//! - SIGTERM, SIGINT or SIGHUP;
//! - EOF on stdin. A host suite should spawn this with stdin on a pipe and
//!   close the pipe (or just exit) when it's done. The pipe closes when the
//!   host process dies for any reason, SIGKILL included, so a crashed test
//!   runner doesn't leave a cluster behind.
//!
//! and then stops the server and deletes the temp directory (data dir,
//! socket, log) before exiting 0. Diagnostics go to stderr; stdout carries
//! only the JSON line.
//!
//! A host that can't keep a pipe on stdin (a CI step that backgrounds this
//! with `&`, where stdin is `/dev/null`) passes `--ignore-stdin` and relies
//! on a signal alone.
//!
//! Don't SIGKILL this process: nothing can tear down then. The `postgres`
//! server outlives it (reparented to init) and keeps running, and testkit's
//! orphan reaper deliberately never touches a directory whose postmaster is
//! alive, so a later [`TestCluster`] won't clean it up either. Stop such a
//! leftover with `pg_ctl stop -D <data_dir>` (`data_dir` is in the JSON).
//! Closing stdin or sending SIGTERM is always enough.
//!
//! ```text
//! trellis-testkit [--dbname NAME] [--ignore-stdin]
//! ```
//!
//! # From a host suite
//!
//! Build it with `cargo build -p testkit --bin trellis-testkit` (it lands in
//! `target/debug/trellis-testkit`); it needs `initdb`, `postgres`, `pg_ctl`
//! and `createdb` on `PATH`, like the Rust tests do. Then, in the suite's
//! setup hook, spawn it with stdin and stdout on pipes and read one line.
//! Ruby, with the `pg` gem:
//!
//! ```ruby
//! testkit = IO.popen(["target/debug/trellis-testkit"], "r+")
//! info = JSON.parse(testkit.gets)
//! conn = PG.connect(info["dsn"])
//! # ... at exit:
//! testkit.close # closes its stdin, then waits for teardown
//! ```
//!
//! A forked child inherits the write end of that pipe, so EOF only arrives
//! once every process holding it has exited or closed it.
//!
//! Elixir, with Postgrex (a port's stdin closes when its owner exits):
//!
//! ```elixir
//! port = Port.open({:spawn_executable, "target/debug/trellis-testkit"},
//!                  [:binary, {:line, 65_536}])
//! info = receive do {^port, {:data, {:eol, line}}} -> Jason.decode!(line) end
//! {:ok, pg} = Postgrex.start_link(socket_dir: info["host"], port: info["port"],
//!                                 username: info["user"], database: info["dbname"])
//! ```

use std::io::Write;
use std::process::ExitCode;
use testkit::TestCluster;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::oneshot;

const USAGE: &str = "usage: trellis-testkit [--dbname NAME] [--ignore-stdin]

Starts a throwaway Postgres with the engine test suite's settings, creates
one empty database, prints a JSON line with its connection details on
stdout, and tears everything down on SIGTERM/SIGINT/SIGHUP or stdin EOF.

  --dbname NAME    name of the database to create: ASCII letters, digits
                   and underscores, at most 63 (default: trellis_test)
  --ignore-stdin   don't treat stdin EOF as the signal to tear down";

struct Args {
    dbname: String,
    watch_stdin: bool,
}

impl Args {
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut parsed = Args {
            dbname: "trellis_test".to_string(),
            watch_stdin: true,
        };
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--dbname" => {
                    parsed.dbname = args
                        .next()
                        .ok_or_else(|| "--dbname needs a value".to_string())?;
                    check_dbname(&parsed.dbname)?;
                }
                "--ignore-stdin" => parsed.watch_stdin = false,
                other => return Err(format!("unrecognized argument `{other}`")),
            }
        }
        Ok(parsed)
    }
}

/// Restricts `--dbname` to names that need no quoting. The printed `dsn` is
/// an unquoted libpq keyword/value string, so a space or a quote would make
/// it unparseable, and `createdb` would read a leading `-` as an option.
fn check_dbname(name: &str) -> Result<(), String> {
    let plain = name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    // 63 bytes is Postgres's identifier limit (NAMEDATALEN - 1); a longer
    // name would be silently truncated to something the JSON doesn't say.
    if name.is_empty() || name.len() > 63 || !plain {
        return Err(format!(
            "--dbname `{name}` must be 1 to 63 ASCII letters, digits or underscores"
        ));
    }
    Ok(())
}

/// A line on stderr that can't fail. Once a cluster exists, `eprintln!`
/// would panic if stderr is a pipe whose reader has gone (a host that
/// captured stderr and then died), and a second panic in teardown would
/// abort before the server is stopped.
fn note(message: std::fmt::Arguments) {
    let _ = writeln!(std::io::stderr(), "trellis-testkit: {message}");
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let args = match Args::parse(args.into_iter()) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("trellis-testkit: {message}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime")
        .block_on(run(args))
}

async fn run(args: Args) -> ExitCode {
    // Install the handlers before the (seconds-long) startup, so a signal
    // that lands mid-`initdb` is queued for the wait below rather than
    // killing the process with the default action and leaking the cluster.
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    let mut sighup = signal(SignalKind::hangup()).expect("install SIGHUP handler");
    let stdin_eof = args.watch_stdin.then(watch_stdin);

    let cluster = TestCluster::start();
    let dsn = cluster.create_database(&args.dbname);

    let info = serde_json::json!({
        "dsn": dsn,
        "host": cluster.socket_dir().display().to_string(),
        "port": cluster.port(),
        "user": "postgres",
        "dbname": args.dbname,
        "data_dir": cluster.data_dir().display().to_string(),
        "log_file": cluster.root().join("postgres.log").display().to_string(),
        "pid": cluster.server_pid(),
    });
    let mut stdout = std::io::stdout().lock();
    if let Err(error) = writeln!(stdout, "{info}").and_then(|()| stdout.flush()) {
        // Nobody is reading: whoever spawned us is gone.
        note(format_args!(
            "could not write connection info ({error}); tearing down"
        ));
        drop(cluster);
        return ExitCode::FAILURE;
    }
    drop(stdout);

    let reason = tokio::select! {
        _ = sigterm.recv() => "SIGTERM",
        _ = sigint.recv() => "SIGINT",
        _ = sighup.recv() => "SIGHUP",
        _ = async {
            match stdin_eof {
                Some(eof) => { let _ = eof.await; }
                None => std::future::pending().await,
            }
        } => "stdin closed",
    };
    note(format_args!("{reason}; tearing down"));
    // The handlers stay installed through teardown, so a second signal
    // (an impatient runner, a double Ctrl-C) can't cut it short.
    drop(cluster);
    ExitCode::SUCCESS
}

/// Reads (and discards) stdin on a plain thread until EOF or a read error,
/// then fires the returned receiver. A plain thread, not tokio's stdin: that
/// one reads on the runtime's blocking pool, whose shutdown would wait on a
/// read that may never return.
fn watch_stdin() -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel();
    std::thread::spawn(move || {
        let _ = std::io::copy(&mut std::io::stdin().lock(), &mut std::io::sink());
        let _ = tx.send(());
    });
    rx
}

#[cfg(test)]
mod tests {
    use super::Args;

    fn parse(args: &[&str]) -> Result<Args, String> {
        Args::parse(args.iter().map(|arg| arg.to_string()))
    }

    #[test]
    fn defaults_to_trellis_test_and_watching_stdin() {
        let args = parse(&[]).unwrap();
        assert_eq!(args.dbname, "trellis_test");
        assert!(args.watch_stdin);
    }

    #[test]
    fn takes_a_dbname_and_ignore_stdin() {
        let args = parse(&["--ignore-stdin", "--dbname", "app_test"]).unwrap();
        assert_eq!(args.dbname, "app_test");
        assert!(!args.watch_stdin);
    }

    #[test]
    fn rejects_a_missing_value_and_unknown_flags() {
        assert!(parse(&["--dbname"]).is_err());
        assert!(parse(&["--tcp"]).is_err());
    }

    #[test]
    fn rejects_a_dbname_the_dsn_or_createdb_would_misread() {
        // Each of these would print a `dsn` libpq can't parse back to the
        // same database, or reach `createdb` as an option.
        for name in ["has space", "a'b", "a\"b", "--ignore-stdin", "-V", ""] {
            assert!(parse(&["--dbname", name]).is_err(), "accepted {name:?}");
        }
        assert!(parse(&["--dbname", &"a".repeat(64)]).is_err());
        assert!(parse(&["--dbname", &"a".repeat(63)]).is_ok());
        assert!(parse(&["--dbname", "App_Test_2"]).is_ok());
    }
}
