//! `build-under-load --one-to-one --apply-latency`: how long each statement
//! `Trellis::apply` accepts takes to return on the loaded source while the
//! writers run (#666, #625 F8b; the user's Q8 bar is < 1 s each on the 100M
//! source), and how long an `ALTER TRANSFORM` and a column resume take until
//! their field is built.
//!
//! It runs once the scenario's 1-1 target (`agg_totals`, over `agg_src`) is
//! `live`, before the writers' `--duration-secs`, through a define-only
//! `Trellis` handle beside the running `Client`, one statement at a time:
//!
//! - on `agg_totals`: `ALTER ... ADD` a field, then `ALTER ... ALTER` it,
//!   `PAUSE` and `RESUME` it as a column, and `ALTER ... DROP` it, each
//!   rebuild waited out to `live` before the next statement (the `*_built`
//!   columns time the statement to `live`);
//! - a second 1-1 over the source (`apply_one`): define, `PAUSE`, `RESUME`,
//!   `PAUSE` again and `DROP` (a drop needs a paused definition), back to
//!   back, its build left running in between;
//! - an aggregate over the source (`apply_agg`): define, `PAUSE`, `DROP`;
//! - a to-one relationship from the source to a `--groups`-row table
//!   (`agg_grp`), then `DROP RELATIONSHIP`;
//! - with `--apply-latency-big-to-side`, also a relationship whose to-side is
//!   the loaded source itself (from a one-row `apply_parent`), then its
//!   `DROP`: declaring a to-one relationship seeds its parent projection
//!   from the whole to-side inside the call (`defs::catalog::
//!   ensure_relationship_projection_in_txn`), which milestone E (#624)
//!   replaces, so this measures that cost rather than gating on it.
//!
//! The added field is dropped again, so the scenario's oracle still compares
//! the target's own columns.

use std::time::{Duration, Instant};

use tokio_postgres::Client as RawClient;

use crate::streaming::disk_tier::json_escape;

/// How often a rebuild's wait reads the definition's status.
const BUILT_POLL: Duration = Duration::from_millis(100);

/// One timed `apply`.
#[derive(Debug, Clone)]
pub struct Timed {
    /// The statement's short name in the JSON (`alter_add`, `define_one`, ...).
    pub name: &'static str,
    pub statement: String,
    pub secs: f64,
    /// `Some(error)` when the statement failed.
    pub error: Option<String>,
}

/// What [`run`] measured.
#[derive(Debug, Clone, Default)]
pub struct ApplyAudit {
    pub statements: Vec<Timed>,
    /// `ALTER ... ADD` -> the definition `live` again (its field built).
    pub alter_add_built_secs: Option<f64>,
    /// `ALTER ... ALTER` -> `live` again.
    pub alter_alter_built_secs: Option<f64>,
    /// `RESUME <target>.<field>` -> `live` again.
    pub resume_column_built_secs: Option<f64>,
}

impl ApplyAudit {
    /// The slowest statement, and its time.
    pub fn slowest(&self) -> Option<&Timed> {
        self.statements
            .iter()
            .max_by(|a, b| a.secs.total_cmp(&b.secs))
    }

    pub fn to_json(&self) -> String {
        let statements: Vec<String> = self
            .statements
            .iter()
            .map(|t| {
                format!(
                    "{{\"name\":\"{}\",\"statement\":\"{}\",\"secs\":{:.4},\"error\":{}}}",
                    t.name,
                    json_escape(&t.statement),
                    t.secs,
                    t.error
                        .as_ref()
                        .map(|e| format!("\"{}\"", json_escape(e)))
                        .unwrap_or_else(|| "null".into())
                )
            })
            .collect();
        let opt = |v: Option<f64>| {
            v.map(|x| format!("{x:.3}"))
                .unwrap_or_else(|| "null".into())
        };
        format!(
            "{{\"max_secs\":{},\"slowest\":{},\"alter_add_built_secs\":{},\
             \"alter_alter_built_secs\":{},\"resume_column_built_secs\":{},\"statements\":[{}]}}",
            opt(self.slowest().map(|t| t.secs)),
            self.slowest()
                .map(|t| format!("\"{}\"", t.name))
                .unwrap_or_else(|| "null".into()),
            opt(self.alter_add_built_secs),
            opt(self.alter_alter_built_secs),
            opt(self.resume_column_built_secs),
            statements.join(","),
        )
    }

    pub fn human(&self) -> String {
        let each: Vec<String> = self
            .statements
            .iter()
            .map(|t| {
                format!(
                    "{} {:.3}s{}",
                    t.name,
                    t.secs,
                    if t.error.is_some() { " (failed)" } else { "" }
                )
            })
            .collect();
        let opt = |v: Option<f64>| {
            v.map(|x| format!("{x:.1}s"))
                .unwrap_or_else(|| "never".into())
        };
        format!(
            "apply: {}; ALTER ADD built after {}, ALTER ALTER after {}, column RESUME after {}",
            each.join(", "),
            opt(self.alter_add_built_secs),
            opt(self.alter_alter_built_secs),
            opt(self.resume_column_built_secs),
        )
    }
}

/// Times `statement` through `trellis.apply`.
async fn timed(trellis: &trellis::Trellis, name: &'static str, statement: &str) -> Timed {
    let started = Instant::now();
    let result = trellis.apply(statement).await;
    let secs = started.elapsed().as_secs_f64();
    let error = result.err().map(|e| e.to_string());
    match &error {
        None => eprintln!("apply-latency: {name} returned in {secs:.3}s"),
        Some(e) => eprintln!("apply-latency: {name} FAILED after {secs:.3}s: {e}"),
    }
    Timed {
        name,
        statement: statement.to_string(),
        secs,
        error,
    }
}

/// Waits until `target`'s definition reads `live` (its rebuild is done),
/// up to `deadline`; returns the seconds since `since`, or `None` at the
/// deadline.
async fn built(raw: &RawClient, target: &str, since: Instant, deadline: Duration) -> Option<f64> {
    loop {
        let status: Option<String> = raw
            .query_opt(
                "select status from transform_definitions \
                 where split_part(target_table, '.', 2) = $1",
                &[&target],
            )
            .await
            .expect("read the definition's status")
            .map(|row| row.get(0));
        if status.as_deref() == Some("live") {
            return Some(since.elapsed().as_secs_f64());
        }
        if since.elapsed() > deadline {
            eprintln!("apply-latency: {target} not live after {deadline:?} ({status:?})");
            return None;
        }
        tokio::time::sleep(BUILT_POLL).await;
    }
}

/// Runs the audit over `source` (`public.<source>`, `groups` distinct
/// `grp`s) and the live 1-1 `target` (see the module doc). `build_timeout`
/// bounds each rebuild's wait.
pub async fn run(
    dsn: &str,
    raw: &RawClient,
    source: &str,
    target: &str,
    groups: i32,
    big_to_side: bool,
    build_timeout: Duration,
) -> ApplyAudit {
    let trellis = trellis::Trellis::connect(
        trellis::Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis");
    raw.batch_execute(&format!(
        "create table public.agg_grp (id integer primary key, label text); \
         insert into public.agg_grp select g, 'g' || g from generate_series(0, {groups}) g; \
         create table public.apply_parent (id bigint primary key); \
         insert into public.apply_parent values (1);"
    ))
    .await
    .expect("create the relationship to-sides");

    let mut audit = ApplyAudit::default();

    let started = Instant::now();
    audit.statements.push(
        timed(
            &trellis,
            "alter_add",
            &format!("ALTER TRANSFORM {target} ADD amt + 1 AS amt1"),
        )
        .await,
    );
    audit.alter_add_built_secs = built(raw, target, started, build_timeout).await;

    let started = Instant::now();
    audit.statements.push(
        timed(
            &trellis,
            "alter_alter",
            &format!("ALTER TRANSFORM {target} ALTER amt1 AS amt + 2"),
        )
        .await,
    );
    audit.alter_alter_built_secs = built(raw, target, started, build_timeout).await;

    audit.statements.push(
        timed(
            &trellis,
            "pause_column",
            &format!("PAUSE TRANSFORM {target}.amt1"),
        )
        .await,
    );
    let started = Instant::now();
    audit.statements.push(
        timed(
            &trellis,
            "resume_column",
            &format!("RESUME TRANSFORM {target}.amt1"),
        )
        .await,
    );
    audit.resume_column_built_secs = built(raw, target, started, build_timeout).await;
    audit.statements.push(
        timed(
            &trellis,
            "alter_drop",
            &format!("ALTER TRANSFORM {target} DROP amt1"),
        )
        .await,
    );

    audit.statements.push(
        timed(
            &trellis,
            "define_one",
            &format!("TRANSFORM apply_one FROM public.{source} SELECT amt AS a"),
        )
        .await,
    );
    audit
        .statements
        .push(timed(&trellis, "pause", "PAUSE TRANSFORM apply_one").await);
    audit
        .statements
        .push(timed(&trellis, "resume", "RESUME TRANSFORM apply_one").await);
    audit
        .statements
        .push(timed(&trellis, "pause_again", "PAUSE TRANSFORM apply_one").await);
    audit
        .statements
        .push(timed(&trellis, "drop_one", "DROP TRANSFORM apply_one").await);

    audit.statements.push(
        timed(
            &trellis,
            "define_agg",
            &format!("TRANSFORM apply_agg FROM public.{source} GROUP BY grp SELECT SUM(amt) AS s"),
        )
        .await,
    );
    audit
        .statements
        .push(timed(&trellis, "pause_agg", "PAUSE TRANSFORM apply_agg").await);
    audit
        .statements
        .push(timed(&trellis, "drop_agg", "DROP TRANSFORM apply_agg").await);

    audit.statements.push(
        timed(
            &trellis,
            "define_relationship",
            &format!("RELATIONSHIP apply_rel FROM {source}.grp TO agg_grp.id"),
        )
        .await,
    );
    audit.statements.push(
        timed(
            &trellis,
            "drop_relationship",
            &format!("DROP RELATIONSHIP {source}.apply_rel"),
        )
        .await,
    );

    if big_to_side {
        audit.statements.push(
            timed(
                &trellis,
                "define_relationship_big_to_side",
                &format!("RELATIONSHIP apply_big FROM apply_parent.id TO {source}.id"),
            )
            .await,
        );
        audit.statements.push(
            timed(
                &trellis,
                "drop_relationship_big_to_side",
                "DROP RELATIONSHIP apply_parent.apply_big",
            )
            .await,
        );
    }

    trellis
        .shutdown()
        .await
        .expect("shut the define-only Trellis down");
    audit
}
