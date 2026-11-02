//! The upgrade battery's library probe (`.github/scripts/upgrade-battery.sh`).
//!
//!   upgrade-battery-check self-check <target>...   audit each target, print one line each
//!   upgrade-battery-check dump-definitions         print `dump_definitions_sql`
//!
//! The database comes from TRELLIS_DATABASE_URL (or the PG* variables), as for
//! the CLI. The probe starts no worker: `self_check` runs on whichever
//! `trellis run` is attached to the database.

use std::time::{Duration, Instant};

use engine::{Config, SelfCheckJobState, SelfCheckMode, SelfCheckOutcome, Trellis, TrellisOptions};

const JOB_DEADLINE: Duration = Duration::from_secs(240);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(args).await {
        Ok(code) => code,
        Err(message) => {
            eprintln!("upgrade-battery-check: {message}");
            std::process::ExitCode::from(2)
        }
    }
}

async fn run(args: Vec<String>) -> Result<std::process::ExitCode, String> {
    let (command, rest) = args
        .split_first()
        .ok_or("usage: self-check <target>... | dump-definitions")?;
    let config = Config::resolve(None).map_err(|e| e.to_string())?;
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .map_err(|e| e.to_string())?;
    let result = match command.as_str() {
        "dump-definitions" => {
            let sql = trellis
                .dump_definitions_sql()
                .await
                .map_err(|e| e.to_string());
            sql.map(|sql| {
                print!("{sql}");
                std::process::ExitCode::SUCCESS
            })
        }
        "self-check" => self_check(&trellis, rest).await,
        other => Err(format!("unknown command {other:?}")),
    };
    let shutdown = trellis.shutdown().await.map_err(|e| e.to_string());
    let code = result?;
    shutdown?;
    Ok(code)
}

/// Starts a standard-mode job per target and prints its verdict. Exits 0 only
/// when every target's job ended `Converged` with no held key.
async fn self_check(
    trellis: &Trellis,
    targets: &[String],
) -> Result<std::process::ExitCode, String> {
    let mut clean = true;
    for target in targets {
        let job = trellis
            .self_check(target, SelfCheckMode::Standard, Duration::from_secs(60))
            .await
            .map_err(|e| format!("{target}: {e}"))?;
        let started = Instant::now();
        let finished = loop {
            let polled = trellis
                .self_check_job(job.id)
                .await
                .map_err(|e| format!("{target}: {e}"))?
                .ok_or_else(|| format!("{target}: job {} vanished", job.id))?;
            match polled.state {
                SelfCheckJobState::Queued | SelfCheckJobState::Running
                    if started.elapsed() < JOB_DEADLINE =>
                {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                _ => break polled,
            }
        };
        let (verdict, held, drain_failures, converged) = match &finished.report {
            Some(report) => (
                match &report.outcome {
                    SelfCheckOutcome::Converged => "converged".to_string(),
                    other => format!("{other:?}"),
                },
                report.held_keys.as_ref().map_or(0, |h| h.count),
                report.drain_failures.len(),
                matches!(report.outcome, SelfCheckOutcome::Converged),
            ),
            None => (format!("no-report error={:?}", finished.error), 0, 0, false),
        };
        println!(
            "self_check {target} state={:?} outcome={verdict} rows_compared={} held_keys={held} drain_failures={drain_failures}",
            finished.state, finished.rows_compared
        );
        clean &= converged && drain_failures == 0;
    }
    Ok(if clean {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::from(1)
    })
}
