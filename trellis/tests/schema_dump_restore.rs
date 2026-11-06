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
//! - a target that carries what a user may add to any target (unique indexes
//!   beside the key, a deferrable unique and a check constraint, foreign keys
//!   both ways, a trigger) is restored with all of it, builds, and a row its
//!   unique index refuses holds its key
//!   (`a_restored_target_keeps_the_objects_a_user_added_and_builds`)
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
//! - an appendix loaded over a database that already holds the identical
//!   definitions dequeues each row and leaves the definition as it was
//!   (`an_appendix_over_identical_definitions_is_dequeued_and_changes_nothing`)
//! - a row whose target holds a definition of different text refuses, names
//!   that cause, and the rows before it are dequeued
//!   (`an_appendix_over_a_different_definition_refuses_and_names_the_cause`)
//! - a chain whose upstream already exists restores the rest, built or not
//!   (`a_chain_whose_upstream_already_exists_restores_the_rest`)
//! - a row differing only in its `JOIN` table or grammar version refuses
//!   too, and the way out the refusal names works end to end
//!   (`the_refusal_names_a_way_out_that_works_and_every_recorded_field_counts`)

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
    load_edited_keeping_the_appendix(cluster, edit).await.0
}

/// [`load_edited`], and the appendix of the file it loaded (the part after
/// `pg_dump`'s output), for appending to a database again.
async fn load_edited_keeping_the_appendix(
    cluster: &TestCluster,
    edit: impl Fn(&str) -> String,
) -> (testkit::TestDatabase, String) {
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
    let appendix = structure
        .split_once("-- Trellis definitions (")
        .expect("the file ends with the appendix")
        .1
        .to_string();
    (loaded, format!("-- Trellis definitions ({appendix}"))
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

/// The three definitions' `(target, id, text)`, in catalog order.
const DEFINITIONS: &str = "select jsonb_build_array(target_table, id, definition_text) \
     from trellis.transform_definitions order by id";

/// The queue's rows, in order, as their target.
const QUEUED: &str = "select to_jsonb(target_table) from trellis.restore_queue order by ordinal";

#[tokio::test]
async fn an_appendix_over_identical_definitions_is_dequeued_and_changes_nothing() {
    let cluster = TestCluster::start();
    let (loaded, appendix) = load_edited_keeping_the_appendix(&cluster, str::to_string).await;
    let after = connect_raw(loaded.dsn()).await;
    facade(loaded.dsn()).await.migrate().await.expect("migrate");
    let defined = rows_json(&after, DEFINITIONS).await;
    assert_eq!(defined.len(), 3, "{defined:?}");
    // Someone appends the appendix to a database that already has them.
    after.batch_execute(&appendix).await.expect("append");
    assert_eq!(
        count(&after, "select count(*) from trellis.restore_queue").await,
        3
    );
    // Twice: the second finds no queue.
    for _ in 0..2 {
        facade(loaded.dsn())
            .await
            .migrate()
            .await
            .expect("the identical definitions are not restored again");
        assert_eq!(
            rows_json(&after, DEFINITIONS).await,
            defined,
            "same ids and text: nothing was redefined"
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
}

#[tokio::test]
async fn an_appendix_over_a_different_definition_refuses_and_names_the_cause() {
    let cluster = TestCluster::start();
    let (loaded, appendix) = load_edited_keeping_the_appendix(&cluster, str::to_string).await;
    let after = connect_raw(loaded.dsn()).await;
    facade(loaded.dsn()).await.migrate().await.expect("migrate");
    let defined = rows_json(&after, DEFINITIONS).await;
    // oc's queued text differs from the one the database runs.
    let changed = appendix.replace("COUNT(*) AS n", "COUNT(*) AS m");
    assert_ne!(changed, appendix);
    after.batch_execute(&changed).await.expect("append");
    let refused = facade(loaded.dsn())
        .await
        .migrate()
        .await
        .expect_err("oc's queued text is not the one it runs");
    let message = refused.to_string();
    assert!(
        message.contains("can't restore transform public.oc")
            && message.contains("already has a transform into public.oc")
            && message.contains("COUNT(*) AS n")
            && message.contains("COUNT(*) AS m")
            && message.contains("delete the row from"),
        "{message}"
    );
    assert!(!message.contains("already exists"), "{message}");
    assert_eq!(refused.code(), trellis::ErrorCode::Validation);
    assert_eq!(
        rows_json(&after, QUEUED).await,
        vec!["\"public.oc\"".to_string()],
        "o1 and tot, identical, are dequeued; oc stays"
    );
    assert_eq!(
        rows_json(&after, DEFINITIONS).await,
        defined,
        "the running definition is untouched"
    );
    // It keeps refusing until the row goes; the remedy the message names works.
    facade(loaded.dsn())
        .await
        .migrate()
        .await
        .expect_err("still refused");
    after
        .batch_execute("delete from trellis.restore_queue where target_table = 'public.oc'")
        .await
        .expect("delete the row");
    facade(loaded.dsn())
        .await
        .migrate()
        .await
        .expect("migrate once the row is gone");
    assert_eq!(rows_json(&after, DEFINITIONS).await, defined);
    // The same text recorded against another source is not the same
    // definition either.
    let elsewhere = appendix.replace("'public.orders', 'public.oc'", "'public.cats', 'public.oc'");
    assert_ne!(elsewhere, appendix);
    after.batch_execute(&elsewhere).await.expect("append");
    let refused = facade(loaded.dsn())
        .await
        .migrate()
        .await
        .expect_err("oc's queued source is not the one it reads");
    assert!(
        refused
            .to_string()
            .contains("already has a transform into public.oc, defined from public.orders"),
        "{refused}"
    );
}

#[tokio::test]
async fn a_chain_whose_upstream_already_exists_restores_the_rest() {
    for built in [false, true] {
        let cluster = TestCluster::start();
        let loaded = load_edited(&cluster, str::to_string).await;
        let after = connect_raw(loaded.dsn()).await;
        // Restore only o1 (queue order: o1, tot, oc), keeping the rest of
        // the rows aside to queue again.
        after
            .batch_execute(
                "create table public.aside as table trellis.restore_queue; \
                 delete from trellis.restore_queue where target_table <> 'public.o1'",
            )
            .await
            .expect("set the later rows aside");
        let t = facade(loaded.dsn()).await;
        t.migrate().await.expect("restore o1");
        if built {
            build_everything(t.pool()).await;
        }
        let o1 = rows_json(
            &after,
            "select jsonb_build_array(id, status) from trellis.transform_definitions",
        )
        .await;
        assert_eq!(o1.len(), 1, "{o1:?}");
        // The whole queue again: o1 exists, tot reads it, oc is independent.
        after
            .batch_execute("create table trellis.restore_queue as table public.aside")
            .await
            .expect("queue every definition again");
        assert_eq!(
            rows_json(&after, QUEUED).await.first().map(String::as_str),
            Some("\"public.o1\"")
        );
        facade(loaded.dsn())
            .await
            .migrate()
            .await
            .expect("o1 is dequeued, tot and oc are restored");
        let statuses = rows_json(&after, STATUSES).await;
        assert_eq!(statuses.len(), 3, "built={built}: {statuses:?}");
        assert_eq!(
            statuses[1..],
            [
                "public.tot waiting_to_backfill".to_string(),
                "public.oc waiting_to_backfill".to_string()
            ],
            "built={built}"
        );
        assert_eq!(
            rows_json(
                &after,
                "select jsonb_build_array(id, status) from trellis.transform_definitions \
                 where target_table = 'public.o1'"
            )
            .await,
            o1,
            "o1 is the definition that was there"
        );
    }
}

/// The two ways out the refusal names, end to end: drop the existing
/// definition and define the queued text, after which the row is either
/// deleted by hand or dequeued as identical; and a row that differs only in
/// its `JOIN` table or its grammar version refuses as a different text does.
#[tokio::test]
async fn the_refusal_names_a_way_out_that_works_and_every_recorded_field_counts() {
    let cluster = TestCluster::start();
    let (loaded, appendix) = load_edited_keeping_the_appendix(&cluster, str::to_string).await;
    let after = connect_raw(loaded.dsn()).await;
    after.batch_execute(SOURCE_ROWS).await.expect("rows");
    let t = facade(loaded.dsn()).await;
    t.migrate().await.expect("migrate");
    build_everything(t.pool()).await;
    let defined = rows_json(&after, DEFINITIONS).await;

    // Only the JOIN table differs, then only the grammar version.
    for (what, from, to, named) in [
        (
            "join table",
            "'public.oc', 'public.cats',",
            "'public.oc', 'public.o1',",
            "different definition for it, from public.orders joined with public.o1",
        ),
        (
            "grammar version",
            "COUNT(*) AS n', 1,",
            "COUNT(*) AS n', 2,",
            "COUNT(*) AS n` (grammar version 2)",
        ),
    ] {
        let edited = appendix.replace(from, to);
        assert_ne!(edited, appendix, "{what}: {appendix}");
        after.batch_execute(&edited).await.expect("append");
        let refused = facade(loaded.dsn()).await.migrate().await.expect_err(what);
        assert!(
            refused
                .to_string()
                .contains("already has a transform into public.oc")
                && refused.to_string().contains(named),
            "{what}: {refused}"
        );
        assert_eq!(
            rows_json(&after, QUEUED).await,
            vec!["\"public.oc\"".to_string()],
            "{what}"
        );
        assert_eq!(rows_json(&after, DEFINITIONS).await, defined, "{what}");
        after
            .batch_execute("drop table trellis.restore_queue")
            .await
            .expect("drop the queue");
    }

    // The queued oc differs; use it instead of the running one, as the
    // message says, once deleting the row by hand and once leaving it.
    for (round, column) in ["m", "k"].into_iter().enumerate() {
        let changed = appendix.replace("COUNT(*) AS n", &format!("COUNT(*) AS {column}"));
        after.batch_execute(&changed).await.expect("append");
        let refused = t.migrate().await.expect_err("oc differs");
        let message = refused.to_string();
        assert!(message.contains("PAUSE and DROP TRANSFORM"), "{message}");
        let queued: String = after
            .query_one(
                "select definition_text from trellis.restore_queue \
                 where target_table = 'public.oc'",
                &[],
            )
            .await
            .expect("the queued text")
            .get(0);
        t.apply("PAUSE TRANSFORM oc").await.expect("pause oc");
        t.apply("DROP TRANSFORM oc").await.expect("drop oc");
        t.apply(&queued).await.expect("define the queued text");
        if round == 0 {
            after
                .batch_execute("delete from trellis.restore_queue where target_table = 'public.oc'")
                .await
                .expect("delete the row");
        }
        t.migrate()
            .await
            .expect("migrate once the queued text is defined");
        assert_eq!(
            count(
                &after,
                "select (to_regclass('trellis.restore_queue') is null)::int::bigint"
            )
            .await,
            1,
            "the queue is dropped"
        );
        build_everything(t.pool()).await;
        let statuses = rows_json(&after, STATUSES).await;
        assert_eq!(
            statuses,
            vec![
                "public.o1 live".to_string(),
                "public.tot live".to_string(),
                "public.oc live".to_string()
            ],
            "round {round}"
        );
        assert_eq!(
            count(
                &after,
                &format!("select sum({column})::bigint from public.oc")
            )
            .await,
            3,
            "oc is built from the queued text"
        );
    }
}

/// What a user added to `o1` after the fact: unique indexes beside the key
/// (plain, partial, expression), a deferrable unique and a `CHECK`
/// constraint, a foreign key to another table and another table's foreign
/// key to it, and an audit trigger.
const USER_OBJECTS: &str = "\
    create table public.groups (g bigint primary key); \
    create table public.o1_audit (id bigint, op text); \
    create function public.audit_o1() returns trigger language plpgsql as \
      $$ begin insert into public.o1_audit values (new.id, tg_op); return new; end $$; \
    create trigger o1_audit after insert or update on public.o1 \
      for each row execute function public.audit_o1(); \
    create unique index o1_b_unique_ix on public.o1 (b); \
    create unique index o1_g_for_big_b on public.o1 (g) where b > 25; \
    create unique index o1_double_b on public.o1 ((b * 2)); \
    alter table public.o1 add constraint o1_b_deferred unique (b) deferrable initially deferred; \
    alter table public.o1 add constraint o1_b_positive check (b > 0); \
    alter table public.o1 add constraint o1_g_fkey foreign key (g) references public.groups (g); \
    create table public.o1_notes (id bigint primary key, o1_id bigint references public.o1 (id))";

/// Everything on `o1` that the user added: its indexes (the key's included),
/// its constraints and the foreign keys referencing it (not PostgreSQL 18's
/// `NOT NULL` entries), and its triggers (not a foreign key's internal ones
/// or the capture triggers a reader would install).
const O1_USER_OBJECTS: &str = "\
    select indexdef from pg_indexes where schemaname = 'public' and tablename = 'o1' \
    union all \
    select conrelid::regclass || ' ' || conname || ' ' || pg_get_constraintdef(oid) \
      from pg_constraint \
      where (conrelid = 'public.o1'::regclass or confrelid = 'public.o1'::regclass) \
        and contype <> 'n' \
    union all \
    select pg_get_triggerdef(oid) from pg_trigger \
      where tgrelid = 'public.o1'::regclass and not tgisinternal \
        and tgname not like 'trellis_capture_%' \
    order by 1";

/// Seals what the capture pass staged and drains it as a drain worker does,
/// retrying a failing page until the keys it fails on are held and it
/// commits. Bounded, not a wait for convergence.
async fn drain_until_committed(pool: &trellis::pool::Pool, client: &mut Client) {
    let outcome = seal::seal_phase1_waiting(client).await;
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    for _ in 0..20 {
        match apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "dump_test",
            1,
            "dump_test_wake",
        )
        .await
        {
            Ok(Some(_)) => continue,
            Ok(None) => {
                retire_drained_segments(client).await.expect("retire");
                return;
            }
            Err(_) => {}
        }
    }
    panic!("the page never committed");
}

/// What a dump of a target the user added unique indexes, constraints,
/// foreign keys and a trigger to carries, and what `migrate` does with it:
/// the definition is adopted with every one of those objects, builds, and
/// the user's trigger and indexes act on what Trellis writes.
#[tokio::test]
async fn a_restored_target_keeps_the_objects_a_user_added_and_builds() {
    let cluster = TestCluster::start();
    let source = cluster.create_isolated_database().await;
    let raw = connect_raw(source.dsn()).await;
    raw.batch_execute("create table public.orders (id bigint primary key, g bigint, a bigint)")
        .await
        .expect("source ddl");
    let t = facade(source.dsn()).await;
    t.apply("TRANSFORM o1 FROM orders SELECT g AS g, a + 1 AS b")
        .await
        .expect("o1");
    build_everything(t.pool()).await;
    raw.batch_execute(USER_OBJECTS).await.expect("user objects");
    let objects = rows_json(&raw, O1_USER_OBJECTS).await;
    for name in [
        "o1_b_unique_ix",
        "o1_g_for_big_b",
        "o1_double_b",
        "o1_b_deferred",
        "o1_b_positive",
        "o1_g_fkey",
        "o1_notes_o1_id_fkey",
        "TRIGGER o1_audit",
    ] {
        assert!(
            objects.iter().any(|object| object.contains(name)),
            "the source's o1 has {name}: {objects:#?}"
        );
    }

    let structure = structure_file(&t, source.dsn()).await;
    let loaded = cluster.create_empty_database().await;
    load(loaded.dsn(), &structure);
    let mut after = connect_raw(loaded.dsn()).await;
    let t2 = facade(loaded.dsn()).await;
    t2.migrate()
        .await
        .expect("migrate adopts the dumped target");
    assert_eq!(
        rows_json(&after, STATUSES).await,
        ["public.o1 waiting_to_backfill"]
    );
    assert_eq!(rows_json(&after, O1_USER_OBJECTS).await, objects);

    // The restored database is empty: the application's rows arrive, and
    // the definition builds from them.
    trellis::client::reconcile_pass(&mut after, t2.pool(), SCHEMA)
        .await
        .expect("reconcile pass on the restored database");
    after
        .batch_execute(
            "insert into public.groups values (1), (2); \
             insert into public.orders values (1, 1, 10), (2, 1, 20), (3, 2, 30)",
        )
        .await
        .expect("source rows after the restore");
    for _ in 0..3 {
        build_everything(t2.pool()).await;
        trellis::client::reconcile_pass(&mut after, t2.pool(), SCHEMA)
            .await
            .expect("reconcile pass");
        drain_to_quiescence(t2.pool(), &mut after).await;
    }
    assert_eq!(
        rows_json(&after, STATUSES).await,
        ["public.o1 live"],
        "the definition is live"
    );
    assert_eq!(
        rows_json(&after, "select to_jsonb(t) from public.o1 t order by id").await,
        [
            r#"{"b": 11, "g": 1, "id": 1}"#,
            r#"{"b": 21, "g": 1, "id": 2}"#,
            r#"{"b": 31, "g": 2, "id": 3}"#
        ],
        "the target is what the source computes"
    );
    assert_eq!(
        count(
            &after,
            "select count(*) from public.o1_audit where op = 'INSERT'"
        )
        .await,
        3,
        "the user's trigger fired for what Trellis wrote"
    );
    assert_eq!(rows_json(&after, O1_USER_OBJECTS).await, objects);

    // A value key 2 has: the unique index refuses the write, the key is
    // held, and the definition keeps running.
    after
        .batch_execute("update public.orders set a = 20 where id = 3")
        .await
        .expect("a value another key has");
    trellis::client::reconcile_pass(&mut after, t2.pool(), SCHEMA)
        .await
        .expect("capture pass");
    drain_until_committed(t2.pool(), &mut after).await;
    assert_eq!(
        rows_json(&after, "select key from poison order by key").await,
        ["3"],
        "the refused row's key is held"
    );
    let status = t2
        .status("o1")
        .await
        .expect("read status")
        .expect("o1 is registered");
    assert_eq!(status.status, trellis::TransformStatus::Live, "{status:?}");
    assert!(status.drain_failure.is_none(), "{status:?}");
    assert_eq!(
        count(&after, "select b from public.o1 where id = 3").await,
        31,
        "the held key keeps its last value"
    );
}
