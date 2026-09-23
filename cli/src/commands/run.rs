//! `trellis run [--staging|--no-staging] [--drain-threads N]` — runs the live
//! CDC/apply pipeline until interrupted.
//!
//! Unlike `define`, this command can't just call `.migrate()` right after
//! connecting: [`trellis::Trellis::connect`] with `staging: true` derives the
//! source-table set from the catalog *during* connect, before this module
//! ever gets a chance to run anything, and it errors immediately
//! (`TrellisError::NoDefinitions`) if no definitions are registered yet.
//! Migrating after that connect would be too late even if it created
//! definitions — which it doesn't; migrations only create/update schema, not
//! data. So there is no ordering of "connect once, then migrate" that helps
//! here: an operator must have already run `trellis apply` (against a
//! migrated database) before `run` will do anything useful, and that's a
//! real prerequisite, not an oversight.
//!
//! What `.migrate()` *is* still useful for here is the schema itself: on a
//! genuinely fresh database (no Trellis tables at all), the catalog query
//! `connect` runs to derive source tables would fail with a raw "relation
//! does not exist" error rather than the engine's own clear
//! `NoDefinitions` message. So this module opens a short-lived, no-options
//! connection first, migrates on it, and shuts it down — that guarantees the
//! catalog tables exist by the time the real `staging`/`drain_threads`
//! connect runs, so a no-definitions-yet database surfaces the engine's own
//! `NoDefinitions` message instead of a confusing SQL error, while a
//! genuinely fresh database doesn't require a separate `migrate` step any
//! more than `define` does.
//!
//! ## `--prometheus-bind`
//!
//! An operator running `trellis run` as a single conceptual client (even
//! though it houses a staging worker and drain threads, each with their own
//! Postgres connections) can optionally serve that same process's metrics
//! registry as a Prometheus scrape target, alongside the pipeline, by
//! passing `--prometheus-bind <ADDR>`. This replaces the old standalone
//! `trellis prometheus` subcommand (removed): that subcommand ran in its own
//! process, so the registry it served was always empty — nothing in that
//! process ever called into `trellis::staging`/`trellis::client` to record an
//! observation. Serving it from inside `run` instead means the registry
//! `render_prometheus()` reads is the *same* one this process's pipeline is
//! actually populating.
//!
//! There's deliberately no real HTTP parsing, matching the old standalone
//! command's approach — see [`handle_metrics_connection`]'s doc comment.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use trellis::metrics::Metrics;
use trellis::{Config, Trellis, TrellisOptions};

/// Help text for `trellis run -h`/`--help`, and prefixed to any
/// argument-parsing error so a mistake also shows correct usage.
pub const USAGE: &str = "\
Usage: trellis run [--staging|--no-staging] [--drain-threads N]
                    [--prometheus-bind <ADDR>] [--database-url <URL>]

Runs the live CDC/apply pipeline (staging worker and/or drain workers) until
interrupted with Ctrl-C. Requires at least one TRANSFORM or RELATIONSHIP
definition to already be registered (via `trellis apply`) against a
migrated database.

Options:
  --staging                  Run the CDC/staging worker. Default.
  --no-staging               Don't run the CDC/staging worker. Mutually
                             exclusive with --staging.
  --drain-threads <N>        Number of drain (application) worker threads to
                             run. Must be a non-negative integer. Default: 2.
                             Each worker borrows from a connection pool capped
                             at TRELLIS_POOL_MAX_SIZE (default 20); raise that
                             alongside a large --drain-threads, or acquiring a
                             connection fails after
                             TRELLIS_POOL_WAIT_TIMEOUT_SECS (default 30).
  --prometheus-bind <ADDR>  host:port to serve this process's metrics
                             registry as Prometheus text exposition on,
                             alongside the pipeline, until interrupted. Not
                             served unless given.
  -d, --database-url <URL>  Postgres connection string. May be given before
                             or after the subcommand name. Falls back to
                             TRELLIS_DATABASE_URL, then PGHOST/PGPORT/PGUSER/
                             PGPASSWORD/PGDATABASE, if omitted.
  -h, --help                 Print this help and exit.
";

/// A parsed `run` invocation, once `--database-url`/`-d` has already been
/// pulled out of argv by the caller (see `connection::extract_database_url`).
#[derive(Debug, PartialEq, Eq)]
pub struct Args {
    pub staging: bool,
    pub drain_threads: usize,
    pub prometheus_bind: Option<SocketAddr>,
}

impl Default for Args {
    /// Staging on, two drain threads, no metrics endpoint — a single-process,
    /// all-in-one setup that does something useful with no flags at all.
    fn default() -> Self {
        Self {
            staging: true,
            drain_threads: 2,
            prometheus_bind: None,
        }
    }
}

/// Parses `--staging`/`--no-staging`/`--drain-threads <N>`/
/// `--prometheus-bind <ADDR>`. Order-independent and each flag may appear at
/// most once; `--staging` and `--no-staging` together are a usage error
/// (rather than "last one wins") since silently picking one would surprise
/// whichever the operator meant.
pub fn parse(args: &[String]) -> Result<Args, String> {
    let mut staging: Option<bool> = None;
    let mut drain_threads: Option<usize> = None;
    let mut prometheus_bind: Option<SocketAddr> = None;
    let mut idx = 0;

    while idx < args.len() {
        match args[idx].as_str() {
            "--staging" => {
                if staging == Some(false) {
                    return Err(format!(
                        "{USAGE}\nerror: --staging and --no-staging are mutually exclusive"
                    ));
                }
                staging = Some(true);
                idx += 1;
            }
            "--no-staging" => {
                if staging == Some(true) {
                    return Err(format!(
                        "{USAGE}\nerror: --staging and --no-staging are mutually exclusive"
                    ));
                }
                staging = Some(false);
                idx += 1;
            }
            "--drain-threads" => {
                let Some(value) = args.get(idx + 1) else {
                    return Err(format!(
                        "{USAGE}\nerror: --drain-threads requires a value, e.g. --drain-threads 2"
                    ));
                };
                if drain_threads.is_some() {
                    return Err(format!(
                        "{USAGE}\nerror: --drain-threads may only be specified once"
                    ));
                }
                let parsed: usize = value.parse().map_err(|_| {
                    format!(
                        "{USAGE}\nerror: --drain-threads expects a non-negative integer, got {value:?}"
                    )
                })?;
                drain_threads = Some(parsed);
                idx += 2;
            }
            "--prometheus-bind" => {
                let Some(value) = args.get(idx + 1) else {
                    return Err(format!(
                        "{USAGE}\nerror: --prometheus-bind requires a value, e.g. --prometheus-bind 127.0.0.1:9464"
                    ));
                };
                if prometheus_bind.is_some() {
                    return Err(format!(
                        "{USAGE}\nerror: --prometheus-bind may only be specified once"
                    ));
                }
                let parsed: SocketAddr = value.parse().map_err(|_| {
                    format!(
                        "{USAGE}\nerror: --prometheus-bind expects a host:port socket address, got {value:?}"
                    )
                })?;
                prometheus_bind = Some(parsed);
                idx += 2;
            }
            other => {
                return Err(format!("{USAGE}\nerror: unrecognized argument {other:?}"));
            }
        }
    }

    let defaults = Args::default();
    Ok(Args {
        staging: staging.unwrap_or(defaults.staging),
        drain_threads: drain_threads.unwrap_or(defaults.drain_threads),
        prometheus_bind,
    })
}

/// Connects (migrating a throwaway connection first — see the module doc
/// comment for why), prints a startup message, blocks until Ctrl-C (while
/// also serving `--prometheus-bind`'s listener, if given), then shuts down
/// cleanly. Returns the shutdown message on success.
pub async fn run(args: Args, database_url: Option<String>) -> Result<String, String> {
    let config = Config::resolve(database_url).map_err(|err| err.to_string())?;

    // Migrate on a plain, no-background-work connection first. This isn't
    // "run migrations before doing anything" for its own sake — it exists so
    // that, on a genuinely fresh database, the staging connect below fails
    // with the engine's clear `NoDefinitions` message rather than a raw
    // "relation does not exist" from the catalog query `connect` runs
    // internally when `staging` is set. See the module doc comment.
    let migrator = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .map_err(|err| err.to_string())?;
    let migration_outcome = migrator.migrate().await.map_err(|err| err.to_string());
    let migrator_shutdown = migrator.shutdown().await.map_err(|err| err.to_string());
    migration_outcome?;
    migrator_shutdown?;

    let options = TrellisOptions {
        staging: args.staging,
        drain_threads: args.drain_threads,
        ..Default::default()
    };
    let trellis = Trellis::connect(config, options)
        .await
        .map_err(|err| err.to_string())?;

    let metrics_listener = match args.prometheus_bind {
        Some(bind) => match TcpListener::bind(bind).await {
            Ok(listener) => Some(listener),
            Err(err) => {
                let shutdown_outcome = trellis.shutdown().await;
                return Err(format!(
                    "failed to bind --prometheus-bind {bind}: {err}{}",
                    match shutdown_outcome {
                        Ok(()) => String::new(),
                        Err(shutdown_err) => {
                            format!("; additionally, shutdown failed: {shutdown_err}")
                        }
                    }
                ));
            }
        },
        None => None,
    };

    println!(
        "trellis running: staging={}, drain_threads={}",
        if args.staging { "on" } else { "off" },
        args.drain_threads
    );
    if let Some(listener) = &metrics_listener {
        let bound_addr = listener
            .local_addr()
            .map_err(|err| format!("failed to read bound address: {err}"))?;
        println!("prometheus metrics listening on http://{bound_addr}");
    }
    println!("press Ctrl-C to stop");

    if let Err(err) = wait_for_shutdown_signal(metrics_listener).await {
        // Failing to even install the signal handler is unusual, but we
        // must still release the connection/background workers we started
        // rather than leaking them.
        let shutdown_outcome = trellis.shutdown().await;
        return Err(format!(
            "{err}{}",
            match shutdown_outcome {
                Ok(()) => String::new(),
                Err(shutdown_err) => format!("; additionally, shutdown failed: {shutdown_err}"),
            }
        ));
    }

    println!("shutting down...");
    trellis.shutdown().await.map_err(|err| err.to_string())?;
    Ok("shut down cleanly".to_string())
}

/// Blocks until Ctrl-C. If `listener` is `Some`, also accepts and serves
/// metrics connections on it (each in its own spawned task) concurrently,
/// exactly like the old standalone `trellis prometheus` subcommand's serve
/// loop — the only difference is this loop shares the process with the
/// pipeline `run` above is already running.
async fn wait_for_shutdown_signal(listener: Option<TcpListener>) -> Result<(), String> {
    let Some(listener) = listener else {
        return tokio::signal::ctrl_c()
            .await
            .map_err(|err| format!("failed to listen for Ctrl-C: {err}"));
    };

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer_addr)) => {
                        tokio::spawn(async move {
                            if let Err(err) = handle_metrics_connection(stream).await {
                                eprintln!("trellis run: metrics connection error: {err}");
                            }
                        });
                    }
                    // A single failed accept (e.g. the peer reset before we
                    // finished accepting it) shouldn't take the whole
                    // listener down — log it and keep serving.
                    Err(err) => {
                        eprintln!("trellis run: metrics accept error: {err}");
                    }
                }
            }
            ctrl_c = tokio::signal::ctrl_c() => {
                return ctrl_c.map_err(|err| format!("failed to listen for Ctrl-C: {err}"));
            }
        }
    }
}

/// The largest number of request bytes a single metrics connection is
/// allowed to send before this gives up reading and just responds anyway.
/// Real requests (a scraper's bare `GET / HTTP/1.1` plus a few headers) are a
/// few hundred bytes at most; this cap just bounds memory/time spent on a
/// client that never sends a terminator, without needing a real HTTP parser
/// to know when the headers are "done".
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// How long a single metrics connection is allowed to spend before this
/// gives up reading its request and responds anyway — bounds a slow/silent
/// client's hold on a spawned task (though not on `accept`, since each
/// connection runs in its own task).
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// The `Content-Type` Prometheus's own text exposition format expects
/// (https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md).
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Drains (a bounded amount of) the request off `stream`, then writes back
/// this process's registry — rendered fresh for this request via
/// [`Metrics::render_prometheus`] — as a `200 OK` and closes the connection.
/// Any I/O error here is this single connection's problem, not the server's
/// — the caller logs and moves on rather than propagating anything that
/// would affect other connections.
///
/// There's deliberately no real HTTP parsing, matching the old standalone
/// `trellis prometheus` command's approach: a Prometheus scraper (or `curl`)
/// sends a bare `GET / HTTP/1.1` with a handful of headers and nothing else
/// worth reading, so this only drains enough to know the client is done
/// sending (or gives up under [`MAX_REQUEST_BYTES`]/[`READ_TIMEOUT`]) before
/// writing back a fixed-status, rendered-body response.
async fn handle_metrics_connection(mut stream: TcpStream) -> Result<(), String> {
    read_metrics_request(&mut stream).await?;
    let body = Metrics::new().render_prometheus();
    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: {PROMETHEUS_CONTENT_TYPE}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    // Bounded the same way `read_metrics_request` is: a client that stops
    // reading mid-response (TCP backpressure) shouldn't be able to pin this
    // connection's spawned task open indefinitely.
    tokio::time::timeout(READ_TIMEOUT, stream.write_all(response.as_bytes()))
        .await
        .map_err(|_| "timed out writing response".to_string())?
        .map_err(|err| format!("failed to write response: {err}"))?;
    stream
        .shutdown()
        .await
        .map_err(|err| format!("failed to close connection: {err}"))
}

/// Reads off `stream` until the request headers look complete (a
/// `\r\n\r\n` terminator has appeared), the client closes its write side, or
/// [`MAX_REQUEST_BYTES`]/[`READ_TIMEOUT`] is hit — whichever comes first.
/// There's no real HTTP parsing here (see [`handle_metrics_connection`]'s
/// doc comment): this only needs to avoid hanging on, or being wedged open
/// by, a client that never finishes sending.
async fn read_metrics_request(stream: &mut TcpStream) -> Result<(), String> {
    let drain = async {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 512];
        while buf.len() < MAX_REQUEST_BYTES {
            let n = stream
                .read(&mut chunk)
                .await
                .map_err(|err| format!("failed to read request: {err}"))?;
            if n == 0 {
                // Client closed its write side (or sent nothing) — nothing
                // more to drain.
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        Ok::<(), String>(())
    };

    match tokio::time::timeout(READ_TIMEOUT, drain).await {
        Ok(result) => result,
        // A client that never finishes sending headers within the timeout
        // still gets a response — we just stop waiting on it.
        Err(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_args_uses_defaults() {
        let parsed = parse(&[]).unwrap();
        assert_eq!(
            parsed,
            Args {
                staging: true,
                drain_threads: 2,
                prometheus_bind: None,
            }
        );
    }

    #[test]
    fn explicit_staging_flag() {
        let args = vec!["--staging".to_string()];
        let parsed = parse(&args).unwrap();
        assert!(parsed.staging);
    }

    #[test]
    fn no_staging_flag() {
        let args = vec!["--no-staging".to_string()];
        let parsed = parse(&args).unwrap();
        assert!(!parsed.staging);
        // drain_threads still defaults even though staging was overridden.
        assert_eq!(parsed.drain_threads, 2);
    }

    #[test]
    fn staging_and_no_staging_together_is_an_error() {
        let args = vec!["--staging".to_string(), "--no-staging".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("mutually exclusive"));

        let args = vec!["--no-staging".to_string(), "--staging".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("mutually exclusive"));
    }

    #[test]
    fn repeated_staging_flag_is_fine() {
        let args = vec!["--staging".to_string(), "--staging".to_string()];
        let parsed = parse(&args).unwrap();
        assert!(parsed.staging);
    }

    #[test]
    fn drain_threads_parses() {
        let args = vec!["--drain-threads".to_string(), "5".to_string()];
        let parsed = parse(&args).unwrap();
        assert_eq!(parsed.drain_threads, 5);
    }

    #[test]
    fn drain_threads_zero_is_valid() {
        let args = vec!["--drain-threads".to_string(), "0".to_string()];
        let parsed = parse(&args).unwrap();
        assert_eq!(parsed.drain_threads, 0);
    }

    #[test]
    fn drain_threads_missing_value_is_an_error() {
        let args = vec!["--drain-threads".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("requires a value"));
    }

    #[test]
    fn drain_threads_negative_is_an_error() {
        let args = vec!["--drain-threads".to_string(), "-1".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("non-negative integer"));
    }

    #[test]
    fn drain_threads_non_numeric_is_an_error() {
        let args = vec!["--drain-threads".to_string(), "banana".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("non-negative integer"));
    }

    #[test]
    fn drain_threads_specified_twice_is_an_error() {
        let args = vec![
            "--drain-threads".to_string(),
            "1".to_string(),
            "--drain-threads".to_string(),
            "2".to_string(),
        ];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("only be specified once"));
    }

    #[test]
    fn unrecognized_argument_is_an_error() {
        let args = vec!["--bogus".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("unrecognized argument"));
    }

    #[test]
    fn combined_flags_parse_in_either_order() {
        let args = vec![
            "--drain-threads".to_string(),
            "3".to_string(),
            "--no-staging".to_string(),
        ];
        let parsed = parse(&args).unwrap();
        assert_eq!(
            parsed,
            Args {
                staging: false,
                drain_threads: 3,
                prometheus_bind: None,
            }
        );
    }

    #[test]
    fn prometheus_bind_parses() {
        let args = vec![
            "--prometheus-bind".to_string(),
            "127.0.0.1:9464".to_string(),
        ];
        let parsed = parse(&args).unwrap();
        assert_eq!(
            parsed.prometheus_bind,
            Some("127.0.0.1:9464".parse().unwrap())
        );
    }

    #[test]
    fn prometheus_bind_missing_value_is_an_error() {
        let args = vec!["--prometheus-bind".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("requires a value"));
    }

    #[test]
    fn prometheus_bind_malformed_value_is_an_error() {
        for bad in ["banana", "127.0.0.1", "not-a-port:abc", ":9464"] {
            let args = vec!["--prometheus-bind".to_string(), bad.to_string()];
            let err = parse(&args).unwrap_err();
            assert!(
                err.contains("host:port socket address"),
                "expected a clear error for {bad:?}, got: {err}"
            );
        }
    }

    #[test]
    fn prometheus_bind_specified_twice_is_an_error() {
        let args = vec![
            "--prometheus-bind".to_string(),
            "127.0.0.1:9464".to_string(),
            "--prometheus-bind".to_string(),
            "127.0.0.1:9465".to_string(),
        ];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("only be specified once"));
    }

    /// End-to-end (within this process) check that `--prometheus-bind`'s
    /// listener really does serve this process's registry: records one
    /// observation directly against `trellis::metrics` (the same global
    /// registry `Metrics::render_prometheus` reads — no `Trellis`/Postgres
    /// connection needed for this), drives `handle_metrics_connection` over
    /// a real loopback socket, and checks the response is a `200 OK` with
    /// the Prometheus content type whose body contains that observation.
    #[tokio::test]
    async fn serves_the_rendered_registry_as_a_200_with_the_prometheus_content_type() {
        trellis::metrics::increment_changes_applied("run_cli_test_target", 1);

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral loopback port");
        let addr = listener.local_addr().expect("listener has a local address");
        tokio::spawn(async move {
            let (stream, _peer_addr) = listener.accept().await.expect("accept one connection");
            handle_metrics_connection(stream)
                .await
                .expect("handle_metrics_connection succeeds");
        });

        let mut stream = TcpStream::connect(addr)
            .await
            .expect("connect to the listener");
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("write the request");
        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .await
            .expect("read the whole response before the server closes the connection");
        let response = String::from_utf8(buf).expect("response is valid utf-8");

        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "expected a 200, got: {response}"
        );
        assert!(
            response.contains(&format!("Content-Type: {PROMETHEUS_CONTENT_TYPE}")),
            "missing the Prometheus content type header: {response}"
        );
        assert!(
            response.contains("trellis_changes_applied_total"),
            "response body missing the recorded metric: {response}"
        );
        assert!(
            response.contains("run_cli_test_target"),
            "response body missing the recorded label: {response}"
        );
    }
}
