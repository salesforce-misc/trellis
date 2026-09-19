//! `engine_subprocess` — issue #166's real, `SIGKILL`-able engine process.
//!
//! Not a general-purpose operator entrypoint (that's `cli`'s `trellis run` —
//! see `cli/src/commands/run.rs`): this is test-only infrastructure, built
//! specifically for `generative::backend::subprocess::SubprocessBackend` to
//! `Command::spawn()` and `testkit::crash::CrashGuard` to `SIGKILL`. It
//! deliberately does *not* reuse `trellis run`'s CLI: that command derives
//! its source-table set from the catalog at connect time (see its own
//! module doc comment) and exposes no way to pin a specific
//! slot/publication/`wake_channel`, both of which
//! `SubprocessBackend`/`ManualBackend` need explicit control over (issue
//! #188 — a replication slot name is cluster-wide, not database-scoped, so
//! tests sharing one Postgres cluster across isolated databases must be able
//! to give each backend its own). Configuration here is entirely
//! environment-variable driven, matching the pattern
//! `testkit::crash::CrashGuard`'s own doc comment calls for ("a binary...
//! re-invoked with an env var... telling it which operation to run").
//!
//! Required env vars: `TRELLIS_DSN`, `TRELLIS_STAGING_WORKER` (`"true"`/
//! `"false"`), `TRELLIS_APPLICATION_THREADS` (a `usize`), `TRELLIS_SLOT`,
//! `TRELLIS_PUBLICATION`, `TRELLIS_MAINTENANCE_INTERVAL_MS` (a `u64`),
//! `TRELLIS_READY_MARKER` (a path). Optional: `TRELLIS_SOURCE_TABLES`
//! (comma-separated `schema.table` names; only consulted when
//! `TRELLIS_STAGING_WORKER=true`, same as [`trellis::ClientOptions::source_tables`]
//! itself). Two more env vars are read directly by `trellis::staging::apply`'s
//! own test-only pause hook, never by this binary — see
//! `SubprocessBackend::spawn_engine`'s doc comment for why they only need to
//! be present in this process's environment, not parsed here:
//! `TRELLIS_TEST_PAUSE_TRIGGER`/`TRELLIS_TEST_PAUSE_MARKER`.

use std::time::Duration;

use trellis::{Client, ClientOptions};

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        eprintln!("engine_subprocess: missing required env var {name}");
        std::process::exit(2);
    })
}

fn main() {
    let dsn = required_env("TRELLIS_DSN");
    let staging_worker = match required_env("TRELLIS_STAGING_WORKER").as_str() {
        "true" => true,
        "false" => false,
        other => {
            eprintln!(
                "engine_subprocess: TRELLIS_STAGING_WORKER must be \"true\" or \"false\", got \
                 {other:?}"
            );
            std::process::exit(2);
        }
    };
    let application_threads: usize = required_env("TRELLIS_APPLICATION_THREADS")
        .parse()
        .unwrap_or_else(|err| {
            eprintln!("engine_subprocess: TRELLIS_APPLICATION_THREADS must be a usize: {err}");
            std::process::exit(2);
        });
    let source_tables: Vec<String> = std::env::var("TRELLIS_SOURCE_TABLES")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    let slot = required_env("TRELLIS_SLOT");
    let publication = required_env("TRELLIS_PUBLICATION");
    let maintenance_interval_ms: u64 = required_env("TRELLIS_MAINTENANCE_INTERVAL_MS")
        .parse()
        .unwrap_or_else(|err| {
            eprintln!("engine_subprocess: TRELLIS_MAINTENANCE_INTERVAL_MS must be a u64: {err}");
            std::process::exit(2);
        });
    let ready_marker = required_env("TRELLIS_READY_MARKER");

    let options = ClientOptions {
        staging_worker,
        application_threads,
        source_tables,
        slot,
        publication,
        maintenance_interval: Duration::from_millis(maintenance_interval_ms),
        ..Default::default()
    };

    println!(
        "engine_subprocess: starting (pid={}, staging_worker={staging_worker}, \
         application_threads={application_threads})",
        std::process::id()
    );
    // `Client::start` blocks synchronously until setup completes (or fails)
    // — see its own doc comment — so no runtime/`.await` is needed in this
    // `main` at all; `Client` spawns and owns its own dedicated thread
    // internally.
    // Never read again after this — `main` blocks forever below, so
    // `client` (and the background thread/tasks it owns) simply lives until
    // this process is killed. See the module doc comment for why there's no
    // graceful shutdown path here.
    let _client = match Client::start(dsn, options) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("engine_subprocess: failed to start client: {err}");
            std::process::exit(1);
        }
    };

    if let Err(err) = std::fs::write(&ready_marker, b"ready") {
        eprintln!("engine_subprocess: failed to write ready marker {ready_marker:?}: {err}");
        std::process::exit(1);
    }
    println!("engine_subprocess: ready");

    // Block forever. There is no graceful-shutdown path here on purpose —
    // `client.shutdown()` is never called: `SubprocessBackend`'s whole
    // lifecycle model is "let `CrashGuard` end this process" (`SIGKILL` for
    // a crash test; a plain kill+wait for ordinary teardown, both of which
    // bypass Rust destructors anyway — see `CrashGuard::kill`'s doc
    // comment). Parking the main thread is enough to keep every background
    // task `client` owns alive until that external kill arrives; `client`
    // itself is simply never dropped by this process's own control flow.
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
