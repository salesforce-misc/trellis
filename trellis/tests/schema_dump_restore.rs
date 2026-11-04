//! An embedded database's schema dump carries the application's schema, the
//! target tables and one row per definition (`trellis::dump::schema_dump`),
//! and `migrate` on a database loaded from it restores each definition into
//! its dumped target (ADR-0025).
//!
//! Hand-driven and deterministic, as `dump_definitions_sql.rs` is: builds
//! and drains run through the engine functions the workers call.
//!
//! - the dump leaves Trellis's schema, the ledgers and the capture triggers
//!   out, loads with `psql`, and `migrate` restores every definition, which
//!   builds the same targets; a user's index and grant on a target survive
//!   (`a_restored_dump_builds_its_definitions`)
//! - two databases with the same definitions and different histories dump
//!   the same appendix (`the_appendix_is_the_same_for_the_same_definitions`)
//! - a different `target_schema` setting refuses and leaves the queue for
//!   the next `migrate` (`a_changed_target_schema_refuses_and_keeps_the_queue`)
//! - a row this release can't run is stored paused and unparsed, and so is a
//!   row reading its target (`a_row_this_release_cannot_run_is_stored_paused`)
//! - a release that meets the floor of a row stored paused rebuilds it on
//!   `RESUME`, but a 1-1 one, which has no ledger
//!   (`rows_restored_paused_resume_on_a_release_that_meets_their_floor`)
//! - a refusal part way leaves the rows before it restored and the rest
//!   queued, and the next `migrate` goes on from it
//!   (`a_refusal_part_way_restores_the_rows_before_it_and_the_next_migrate_goes_on`)

use std::process::Command;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
use trellis::{Config, Trellis, TrellisOptions};

const SCHEMA: &str = trellis::config::DEFAULT_SCHEMA;

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

async fn facade_with(config: Config) -> Trellis {
    Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect a define-only Trellis")
}

async fn facade(dsn: &str) -> Trellis {
    facade_with(Config::from_dsn(dsn.to_string()).expect("valid dsn")).await
}

/// Runs every queued build to completion by hand.
async fn build_everything(pool: &trellis::pool::Pool) {
    trellis::intake::harness::start_registered_builds(pool)
        .await
        .expect("dispatch registered definitions' builds");
    trellis::staging::build::settle_builds(pool).await;
}

/// Seals and drains the ring until nothing is pending.
async fn drain_to_quiescence(pool: &trellis::pool::Pool, client: &mut Client) {
    for _ in 0..16 {
        let outcome = seal::seal_phase1_waiting(client).await;
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "dump_test",
            1,
            "dump_test_wake",
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("the ring did not reach quiescence within 16 seal/drain rounds");
}

fn run(cmd: &mut Command, what: &str) -> String {
    let out = cmd.output().unwrap_or_else(|e| panic!("{what}: {e}"));
    assert!(
        out.status.success(),
        "{what} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf8")
}

async fn rows_json(raw: &Client, sql: &str) -> Vec<String> {
    raw.query(&format!("select j::text from ({sql}) q(j)"), &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}

async fn count(raw: &Client, sql: &str) -> i64 {
    raw.query_one(sql, &[]).await.expect("count").get(0)
}

const STATUSES: &str =
    "select target_table || ' ' || status from trellis.transform_definitions order by id";

/// The application's schema: an enum, a plain table, a joined table and a
/// partitioned one.
const SOURCE_DDL: &str = "\
    create type public.mood as enum ('calm', 'loud'); \
    create table public.cats (id bigint primary key, name text); \
    create table public.orders (id bigint primary key, g bigint, a bigint, cat_id bigint, \
                                m public.mood); \
    create table public.r (id int, region int, v bigint, primary key (id, region)) \
        partition by list (region); \
    create table public.r1 partition of public.r for values in (1); \
    create table public.r2 partition of public.r for values in (2)";

const SOURCE_ROWS: &str = "\
    insert into public.cats values (1, 'tom'), (2, 'kit'); \
    insert into public.orders values (1, 1, 10, 1, 'calm'), (2, 1, 20, 2, 'loud'), \
                                     (3, 2, 30, 1, 'calm'); \
    insert into public.r values (1, 1, 5), (2, 1, 6), (3, 2, 7)";

/// Every kind of definition: a 1-1 one (altered after its define, written
/// over two lines), an aggregate grouped by the enum chained off it, an
/// aggregate `JOIN`, a 1-1 `JOIN`, a `LEFT JOIN` and an aggregate over the
/// partitioned table. A user's index and grant on a target.
async fn define_everything(t: &Trellis, raw: &mut Client) {
    t.apply("TRANSFORM o1 FROM orders\n  SELECT g AS g, a + 1 AS b, m AS m")
        .await
        .expect("o1");
    build_everything(t.pool()).await;
    t.apply("ALTER TRANSFORM o1 ADD a * 2 AS twice")
        .await
        .expect("alter o1");
    build_everything(t.pool()).await;
    for statement in [
        "TRANSFORM tot FROM o1 GROUP BY g, m WITHOUT SOURCE INDEX \
         SELECT g AS g, m AS m, SUM(b) AS total",
        "TRANSFORM oc FROM orders JOIN cats ON orders.cat_id = cats.id GROUP BY cats.name \
         WITHOUT SOURCE INDEX SELECT cats.name AS name, COUNT(*) AS n",
        "TRANSFORM ol FROM orders LEFT JOIN cats ON orders.cat_id = cats.id GROUP BY orders.g \
         WITHOUT SOURCE INDEX SELECT orders.g AS g, COUNT(cats.id) AS n",
        "TRANSFORM ov FROM orders JOIN cats ON orders.cat_id = cats.id \
         SELECT cats.name AS who, orders.a * 2 AS twice",
        "TRANSFORM rt FROM r GROUP BY region WITHOUT SOURCE INDEX \
         SELECT region AS region, SUM(v) AS total",
    ] {
        t.apply(statement).await.expect(statement);
    }
    raw.batch_execute(
        "create index o1_by_b on public.o1 (b); \
         grant select on public.o1 to public",
    )
    .await
    .expect("a user's index and grant on a target");
}

/// The structure file an embedded client writes: `pg_dump` of the
/// application's schema with the dump's arguments, filtered, then the
/// appendix.
async fn structure_file(t: &Trellis, dsn: &str) -> String {
    let dump = t.schema_dump().await.expect("schema dump");
    let pg_dump = run(
        Command::new("pg_dump")
            .args(["--schema-only", "--no-owner", "--schema", "public"])
            .args(&dump.pg_dump_args)
            .arg(dsn),
        "pg_dump",
    );
    let filtered = t.filter_schema_dump(&pg_dump).expect("filter");
    format!("{filtered}{}", dump.appendix)
}

/// Loads `sql` into `dsn`'s empty database with `psql`, stopping at the
/// first error, as `db:schema:load` and `ecto.load` do.
fn load(dsn: &str, sql: &str) {
    run(
        Command::new("psql")
            .args(["-q", "-c", "drop schema public cascade"])
            .arg(dsn),
        "drop the empty database's public schema",
    );
    let dir = std::env::temp_dir().join(format!("schema_dump_restore_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file = dir.join(format!("structure_{}.sql", dsn.rsplit('=').next().unwrap()));
    std::fs::write(&file, sql).expect("write dump");
    run(
        Command::new("psql")
            .args(["-v", "ON_ERROR_STOP=1", "-q", "-f"])
            .arg(&file)
            .arg(dsn),
        "psql load",
    );
}

#[tokio::test]
async fn a_restored_dump_builds_its_definitions() {
    let cluster = TestCluster::start();
    let source = cluster.create_isolated_database().await;
    let mut raw = connect_raw(source.dsn()).await;
    raw.batch_execute(SOURCE_DDL).await.expect("source ddl");
    raw.batch_execute(SOURCE_ROWS).await.expect("source rows");
    let t = facade(source.dsn()).await;
    define_everything(&t, &mut raw).await;
    for _ in 0..3 {
        build_everything(t.pool()).await;
        trellis::client::reconcile_pass(&mut raw, t.pool(), SCHEMA)
            .await
            .expect("capture pass");
        drain_to_quiescence(t.pool(), &mut raw).await;
    }
    // The partitioned source's aggregate is restored like the rest; this
    // hand-driven harness doesn't settle its build (it pauses here without
    // a recorded cause), so the builds below are compared without it.
    let built = |statuses: &[String]| {
        statuses
            .iter()
            .filter(|s| !s.starts_with("public.rt "))
            .all(|s| s.ends_with(" live"))
    };
    let statuses = rows_json(&raw, STATUSES).await;
    assert!(built(&statuses), "{statuses:?}");
    let recorded = rows_json(
        &raw,
        "select jsonb_build_array(target_table, source_table, join_table, definition_text, \
                grammar_version, floor_version) \
         from trellis.transform_definitions order by target_table",
    )
    .await;
    assert_eq!(recorded.len(), 6);
    assert!(
        count(
            &raw,
            "select count(*) from pg_trigger where tgname like 'trellis_capture_%'"
        )
        .await
            > 0,
        "the source tables have capture triggers"
    );

    let structure = structure_file(&t, source.dsn()).await;
    assert!(!structure.contains("trellis_capture_"), "{structure}");
    assert!(!structure.contains("__ledger"), "{structure}");
    assert!(!structure.contains("trellis.cap_"), "{structure}");
    assert!(
        !structure.contains("CREATE TABLE trellis."),
        "no Trellis table is dumped"
    );
    let appendix = structure
        .split_once("-- Trellis definitions (")
        .expect("appendix")
        .1;
    assert_eq!(
        appendix
            .lines()
            .filter(|l| l.starts_with("insert into "))
            .count(),
        6,
        "one row per definition, one per line: {appendix}"
    );
    // A chain's upstream is queued before its reader.
    let o1_at = appendix
        .find("'public.o1', null,")
        .unwrap_or_else(|| panic!("o1 row: {appendix}"));
    let tot_at = appendix.find("'public.tot'").expect("tot row");
    assert!(o1_at < tot_at, "{appendix}");

    let loaded = cluster.create_empty_database().await;
    load(loaded.dsn(), &structure);
    let after = connect_raw(loaded.dsn()).await;
    let t2 = facade(loaded.dsn()).await;
    t2.migrate().await.expect("migrate restores the queue");
    assert_eq!(
        count(
            &after,
            "select (to_regclass('trellis.restore_queue') is null)::int::bigint"
        )
        .await,
        1,
        "the queue is dropped"
    );
    let restored = rows_json(
        &after,
        "select jsonb_build_array(target_table, source_table, join_table, definition_text, \
                grammar_version, floor_version) \
         from trellis.transform_definitions order by target_table",
    )
    .await;
    assert_eq!(
        restored, recorded,
        "text, identities, grammar and floor as recorded"
    );
    let statuses = rows_json(&after, STATUSES).await;
    assert!(
        statuses.iter().all(|s| s.ends_with(" waiting_to_backfill")),
        "{statuses:?}"
    );
    assert_eq!(
        count(
            &after,
            "select count(*) from pg_indexes where indexname = 'o1_by_b'"
        )
        .await,
        1,
        "the user's index on the adopted target survives"
    );
    assert_eq!(
        count(
            &after,
            "select count(*) from information_schema.role_table_grants \
             where table_schema = 'public' and table_name = 'o1' and grantee = 'PUBLIC'"
        )
        .await,
        1,
        "the user's grant survives"
    );
    // A second migrate finds no queue and changes nothing.
    t2.migrate().await.expect("migrate again");

    let mut worker = connect_raw(loaded.dsn()).await;
    trellis::client::reconcile_pass(&mut worker, t2.pool(), SCHEMA)
        .await
        .expect("reconcile pass on the restored database");
    after
        .batch_execute(SOURCE_ROWS)
        .await
        .expect("source rows after the restore");
    for _ in 0..3 {
        build_everything(t2.pool()).await;
        trellis::client::reconcile_pass(&mut worker, t2.pool(), SCHEMA)
            .await
            .expect("reconcile pass");
        drain_to_quiescence(t2.pool(), &mut worker).await;
    }
    let statuses = rows_json(&after, STATUSES).await;
    assert!(built(&statuses), "{statuses:?}");
    for target in ["o1", "tot", "oc", "ol", "ov"] {
        let sql = format!("select to_jsonb(t) from public.{target} t");
        let mut want = rows_json(&raw, &sql).await;
        assert!(!want.is_empty(), "the source's {target} has rows");
        want.sort();
        let mut got = rows_json(&after, &sql).await;
        got.sort();
        assert_eq!(got, want, "{target}");
    }
}

/// Every line of `appendix` starts the way `Trellis.StructureDump`'s
/// `@appendix_starts` says an appendix line does, so `append/2` over a file
/// that ends in this appendix replaces it.
fn every_line_is_one_the_elixir_client_strips(appendix: &str) {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../clients/elixir/lib/trellis/structure_dump.ex"
    );
    let source = std::fs::read_to_string(path).expect("read structure_dump.ex");
    let list = source
        .split_once("@appendix_starts [")
        .and_then(|(_, rest)| rest.split_once(']'))
        .expect("@appendix_starts list")
        .0;
    let starts: Vec<&str> = list.split('"').skip(1).step_by(2).collect();
    assert!(appendix.starts_with("-- Trellis definitions ("));
    for line in appendix.lines() {
        assert!(
            line.is_empty() || starts.iter().any(|start| line.starts_with(start)),
            "the Elixir client wouldn't strip this appendix line: {line}"
        );
    }
}

#[tokio::test]
async fn the_appendix_is_the_same_for_the_same_definitions() {
    let cluster = TestCluster::start();
    let first = cluster.create_isolated_database().await;
    let second = cluster.create_isolated_database().await;
    let mut raw_first = connect_raw(first.dsn()).await;
    let mut raw_second = connect_raw(second.dsn()).await;
    raw_first.batch_execute(SOURCE_DDL).await.expect("ddl");
    raw_second.batch_execute(SOURCE_DDL).await.expect("ddl");
    let t_first = facade(first.dsn()).await;
    let t_second = facade(second.dsn()).await;
    // The second database has another history: a definition defined and
    // dropped first, so every id differs.
    for statement in [
        "TRANSFORM gone FROM orders SELECT g AS g",
        "PAUSE TRANSFORM gone",
        "DROP TRANSFORM gone",
    ] {
        t_second.apply(statement).await.expect(statement);
    }
    define_everything(&t_first, &mut raw_first).await;
    define_everything(&t_second, &mut raw_second).await;
    let a = t_first.schema_dump().await.expect("dump");
    let b = t_second.schema_dump().await.expect("dump");
    assert_eq!(a, t_first.schema_dump().await.expect("dump again"));
    assert_eq!(a.appendix, b.appendix);
    assert_eq!(a.pg_dump_args, b.pg_dump_args);
    every_line_is_one_the_elixir_client_strips(&a.appendix);
    assert_eq!(
        a.pg_dump_args,
        vec![
            "--exclude-schema=\"trellis\"".to_string(),
            "--exclude-table=\"public\".\"o1__ledger\"".to_string(),
        ]
    );
}

/// Loads a dump of `o1` and `tot` (which reads `o1`'s target) into a fresh
/// database, its appendix changed by `edit` first, and returns the
/// database's DSN with the cluster that holds it.
async fn load_edited(
    cluster: &TestCluster,
    edit: impl Fn(&str) -> String,
) -> testkit::TestDatabase {
    let source = cluster.create_isolated_database().await;
    let raw = connect_raw(source.dsn()).await;
    raw.batch_execute(SOURCE_DDL).await.expect("ddl");
    let t = facade(source.dsn()).await;
    t.apply("TRANSFORM o1 FROM orders SELECT g AS g, a + 1 AS b, m AS m")
        .await
        .expect("o1");
    build_everything(t.pool()).await;
    for statement in [
        "TRANSFORM tot FROM o1 GROUP BY g, m WITHOUT SOURCE INDEX \
         SELECT g AS g, m AS m, SUM(b) AS total",
        "TRANSFORM oc FROM orders JOIN cats ON orders.cat_id = cats.id GROUP BY cats.name \
         WITHOUT SOURCE INDEX SELECT cats.name AS name, COUNT(*) AS n",
    ] {
        t.apply(statement).await.expect(statement);
    }
    let structure = edit(&structure_file(&t, source.dsn()).await);
    let loaded = cluster.create_empty_database().await;
    load(loaded.dsn(), &structure);
    loaded
}

#[tokio::test]
async fn a_changed_target_schema_refuses_and_keeps_the_queue() {
    let cluster = TestCluster::start();
    let loaded = load_edited(&cluster, str::to_string).await;
    let after = connect_raw(loaded.dsn()).await;
    after
        .batch_execute("create schema elsewhere")
        .await
        .expect("another target schema");
    let elsewhere = Config::from_dsn(loaded.dsn().to_string())
        .expect("dsn")
        .with_target_schema("elsewhere")
        .expect("target schema");
    let refused = facade_with(elsewhere)
        .await
        .migrate()
        .await
        .expect_err("a bare target resolves into another schema");
    let message = refused.to_string();
    assert!(
        message.contains("can't restore transform public.o1")
            && message.contains("elsewhere.o1")
            && message.contains("target_schema"),
        "{message}"
    );
    assert_eq!(refused.code(), trellis::ErrorCode::Validation);
    assert_eq!(
        count(&after, "select count(*) from trellis.restore_queue").await,
        3,
        "nothing restored, every row still queued"
    );
    assert_eq!(
        count(&after, "select count(*) from trellis.transform_definitions").await,
        0
    );
    // With the setting it was dumped with, the next migrate restores them.
    facade(loaded.dsn())
        .await
        .migrate()
        .await
        .expect("migrate with the dumped setting");
    assert_eq!(
        count(&after, "select count(*) from trellis.transform_definitions").await,
        3
    );
}

#[tokio::test]
async fn a_row_this_release_cannot_run_is_stored_paused() {
    let cluster = TestCluster::start();
    // o1's floor is above the loading release, and oc's text was written
    // under a grammar newer than it reads. tot reads o1's target.
    let loaded = load_edited(&cluster, |sql| {
        sql.lines()
            .map(|line| {
                if line.contains("'public.o1', null,") {
                    line.replace(", '0.2.0');", ", '9.9.9');")
                } else if line.contains("'public.oc'") {
                    line.replace(", 1, '0.2.0');", ", 99, '0.2.0');")
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    })
    .await;
    let after = connect_raw(loaded.dsn()).await;
    let config = Config::from_dsn(loaded.dsn().to_string())
        .expect("dsn")
        .with_binary_version("0.2.0");
    facade_with(config)
        .await
        .migrate()
        .await
        .expect("migrate stores what it can't run paused");
    let rows = rows_json(
        &after,
        "select jsonb_build_array(d.target_table, d.status, d.floor_version, d.grammar_version, \
                f.error) \
         from trellis.transform_definitions d \
         left join trellis.capture_failures f on f.transform_id = d.id order by d.target_table",
    )
    .await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert!(
        rows[0].contains("public.o1")
            && rows[0].contains("\"paused\"")
            && rows[0].contains("9.9.9")
            && rows[0].contains("its floor"),
        "{rows:?}"
    );
    assert!(
        rows[1].contains("public.oc")
            && rows[1].contains("\"paused\"")
            && rows[1].contains(", 99,")
            && rows[1].contains("it was written under grammar version 99")
            && rows[1].contains("roll forward"),
        "{rows:?}"
    );
    assert!(
        rows[2].contains("public.tot")
            && rows[2].contains("\"paused\"")
            && rows[2].contains("reads public.o1"),
        "{rows:?}"
    );
    assert_eq!(
        count(
            &after,
            "select (to_regclass('trellis.restore_queue') is null)::int::bigint"
        )
        .await,
        1,
        "the queue is dropped"
    );
}

/// A dump whose every row has a floor above the loading release: each is
/// stored paused, and a release that meets the floor rebuilds each on
/// `RESUME` but a 1-1 definition, which has no ledger and refuses
/// (ADR-0025, `docs/embedding.md`).
#[tokio::test]
async fn rows_restored_paused_resume_on_a_release_that_meets_their_floor() {
    let cluster = TestCluster::start();
    let source = cluster.create_isolated_database().await;
    let mut raw = connect_raw(source.dsn()).await;
    raw.batch_execute(SOURCE_DDL).await.expect("ddl");
    let t = facade(source.dsn()).await;
    define_everything(&t, &mut raw).await;
    let structure = structure_file(&t, source.dsn())
        .await
        .lines()
        .map(|line| line.replace(", '0.2.0');", ", '0.3.0');"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let loaded = cluster.create_empty_database().await;
    load(loaded.dsn(), &structure);
    let after = connect_raw(loaded.dsn()).await;
    let at = |version: &str| {
        Config::from_dsn(loaded.dsn().to_string())
            .expect("dsn")
            .with_binary_version(version)
    };
    facade_with(at("0.2.0"))
        .await
        .migrate()
        .await
        .expect("0.2.0 stores every row paused");
    let statuses = rows_json(&after, STATUSES).await;
    assert!(
        statuses.iter().all(|s| s.ends_with(" paused")),
        "{statuses:?}"
    );
    after.batch_execute(SOURCE_ROWS).await.expect("source rows");

    let forward = Trellis::connect(
        at("0.3.0"),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("connect 0.3.0");
    let err = forward
        .apply("RESUME TRANSFORM o1")
        .await
        .expect_err("a 1-1 definition restored paused has no ledger");
    assert!(err.to_string().contains("o1__ledger"), "{err}");
    let resumed = ["oc", "ol", "ov", "rt"];
    for target in resumed {
        forward
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .unwrap_or_else(|err| panic!("resume {target}: {err}"));
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    for target in resumed {
        loop {
            let status = forward
                .status(target)
                .await
                .expect("status")
                .expect("defined")
                .status;
            if status == trellis::TransformStatus::Live {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{target} never went live: {status:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
    let token = forward.watermark_token().await.expect("token");
    forward
        .await_converged(token, std::time::Duration::from_secs(60))
        .await
        .expect("converged");
    use trellis::staging::self_check::CheckPageExt;
    for target in resumed {
        let report = forward
            .self_check_to_end(
                target,
                trellis::SelfCheckMode::Strict,
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap_or_else(|err| panic!("self_check {target}: {err}"));
        assert_eq!(
            report.outcome,
            trellis::SelfCheckOutcome::Converged,
            "{target}"
        );
        assert!(
            count(&after, &format!("select count(*) from public.{target}")).await > 0,
            "{target} built rows"
        );
    }
    forward.shutdown().await.expect("shutdown");
}

/// A refusal part way through the queue: the rows before it stay restored
/// and leave the queue, and the next `migrate` goes on from the refused row
/// without defining them again.
#[tokio::test]
async fn a_refusal_part_way_restores_the_rows_before_it_and_the_next_migrate_goes_on() {
    let cluster = TestCluster::start();
    // tot's row records a target its text doesn't name.
    let loaded = load_edited(&cluster, |sql| {
        sql.replace("'public.tot',", "'public.not_tot',")
    })
    .await;
    let after = connect_raw(loaded.dsn()).await;
    let refused = facade(loaded.dsn())
        .await
        .migrate()
        .await
        .expect_err("tot's row doesn't match its text");
    assert!(
        refused
            .to_string()
            .contains("can't restore transform public.not_tot"),
        "{refused}"
    );
    assert_eq!(
        rows_json(&after, STATUSES).await,
        vec!["public.o1 waiting_to_backfill".to_string()],
        "o1, before it, is restored"
    );
    assert_eq!(
        rows_json(
            &after,
            "select to_jsonb(target_table) from trellis.restore_queue order by ordinal"
        )
        .await,
        vec![
            "\"public.not_tot\"".to_string(),
            "\"public.oc\"".to_string()
        ],
        "the refused row and the rest stay queued"
    );
    after
        .batch_execute(
            "update trellis.restore_queue set target_table = 'public.tot' \
             where target_table = 'public.not_tot'",
        )
        .await
        .expect("correct the row");
    facade(loaded.dsn())
        .await
        .migrate()
        .await
        .expect("the next migrate goes on from the refused row");
    let statuses = rows_json(&after, STATUSES).await;
    assert_eq!(statuses.len(), 3, "{statuses:?}");
    assert!(
        statuses.iter().all(|s| s.ends_with(" waiting_to_backfill")),
        "{statuses:?}"
    );
    assert_eq!(
        count(
            &after,
            "select (to_regclass('trellis.restore_queue') is null)::int::bigint"
        )
        .await,
        1,
        "the queue is dropped"
    );
}
