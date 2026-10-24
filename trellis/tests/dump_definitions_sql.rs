//! Issue #644: `dump_definitions_sql` appends a schema-only `pg_dump` with the
//! rows that say what is defined, the way Rails appends `schema_migrations`.
//!
//! The round trip is hand-driven and deterministic (#297): no live pipeline,
//! and the one build step the loaded database needs is run through the same
//! engine functions the drain worker calls.
//!
//! - the dump plus the appendix loads into a fresh database as "defined, not
//!   built", with every runtime table empty
//!   (`a_dump_loads_as_defined_not_built`)
//! - the staging worker's reconcile pass starts the loaded definitions'
//!   builds, which build from the source rows and get the same targets
//!   (`a_loaded_database_builds_its_definitions`)
//! - every table in the Trellis schema is classified
//!   (`every_trellis_table_is_classified`)
//! - a schema name with `%` or `'` in it dumps
//!   (`a_schema_name_with_percent_and_quote_dumps`)

use std::process::Command;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::defs::chunk_queue;
use trellis::dump::{CLASSIFICATION, Class};
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
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

async fn facade(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

/// Runs every queued build to completion by hand: the stand-in for a drain
/// worker (`pause_and_drop.rs`'s helper of the same name).
async fn build_everything(pool: &trellis::Pool) {
    trellis::intake::markers::discharge_registrations(pool)
        .await
        .expect("dispatch registered definitions' builds");
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, "dump_test", 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            trellis::staging::build::settle_builds(pool).await;
            trellis::intake::markers::discharge_registrations(pool)
                .await
                .expect("discharge the go-live catch-ups");
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                "dump_test",
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, "dump_test")
                .await
                .expect("finish_chunk");
        }
    }
}

/// Loads `schema_sql` plus the appendix into `dsn`'s empty database, the way
/// `psql -f structure.sql` does. The dump creates `public` itself.
fn load(dsn: &str, schema_sql: &str, appendix: &str) {
    run(
        Command::new("psql")
            .args(["-q", "-c", "drop schema public cascade"])
            .arg(dsn),
        "drop the empty database's public schema",
    );
    let file = tempfile_dir().join(format!("structure_{}.sql", dsn.rsplit('=').next().unwrap()));
    std::fs::write(&file, format!("{schema_sql}\n{appendix}")).expect("write dump");
    run(
        Command::new("psql")
            .args(["-v", "ON_ERROR_STOP=1", "-q", "-f"])
            .arg(&file)
            .arg(dsn),
        "psql load",
    );
}

/// Seals and drains the ring through the engine's own apply path until
/// nothing is pending: the stand-in for the maintenance loop and drain workers.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "dump_test",
            1,
            "dump_test_wake",
            &watermark,
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

const SOURCE_DDL: &str = "\
    create type public.mood as enum ('calm', 'loud'); \
    create table public.cats (id bigint primary key, name text); \
    create table public.orders (id bigint primary key, g bigint, a bigint, cat_id bigint, \
                                m public.mood); \
    insert into public.cats values (1, 'tom'), (2, 'kit'); \
    insert into public.orders values (1, 1, 10, 1, 'calm'), (2, 1, 20, 2, 'loud'), (3, 2, 30, 1, 'calm')";

/// A 1-1 transform, an aggregate chained off it, and a `JOIN` of `orders` to
/// `cats`, all built, with the capture triggers installed.
async fn define_and_build(t: &Trellis, raw: &mut Client) {
    raw.batch_execute(SOURCE_DDL).await.expect("source ddl");
    t.apply("TRANSFORM o1 FROM orders SELECT g AS g, a + 1 AS b, m AS m")
        .await
        .expect("o1");
    build_everything(t.pool()).await;
    t.apply("TRANSFORM tot FROM o1 GROUP BY g, m WITHOUT SOURCE INDEX SELECT g AS g, m AS m, SUM(b) AS total")
        .await
        .expect("tot");
    t.apply(
        "TRANSFORM oc FROM orders JOIN cats ON orders.cat_id = cats.id GROUP BY cats.name \
         WITHOUT SOURCE INDEX SELECT cats.name AS name, COUNT(*) AS n",
    )
    .await
    .expect("oc");
    build_everything(t.pool()).await;
    trellis::client::reconcile_pass(
        raw,
        t.pool(),
        SCHEMA,
        "dump_test_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("capture pass");
    build_everything(t.pool()).await;
    drain_to_quiescence(t.pool(), raw).await;
    build_everything(t.pool()).await;
}

/// `tot`'s group partials (the group basis's hidden table, `agg_<id>` in the
/// Trellis schema) are the aggregate's runtime state; they must exist in the
/// source for the "never dumped" assertions to mean anything.
async fn tot_partials_table(raw: &Client) -> String {
    let id = count(
        raw,
        "select id from transform_definitions where target_table = 'public.tot'",
    )
    .await;
    format!("{SCHEMA}.agg_{id}")
}

async fn tot_partial_rows(raw: &Client) -> i64 {
    let table = tot_partials_table(raw).await;
    count(raw, &format!("select count(*) from {table}")).await
}

/// The rows in `oc`'s hidden tables (`join_<id>_*` in the Trellis schema),
/// the `JOIN`'s runtime state.
async fn oc_hidden_rows(raw: &Client) -> i64 {
    let id = count(
        raw,
        "select id from transform_definitions where target_table = 'public.oc'",
    )
    .await;
    let tables: Vec<String> = raw
        .query(
            "select c.relname::text from pg_class c join pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = $1 and c.relname like $2 and c.relkind = 'r'",
            &[&SCHEMA, &format!("join\\_{id}\\_%")],
        )
        .await
        .expect("list oc's hidden tables")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert!(!tables.is_empty(), "oc has hidden tables");
    let mut rows = 0;
    for table in tables {
        rows += count(raw, &format!("select count(*) from {SCHEMA}.{table}")).await;
    }
    rows
}

/// The Elixir client replaces an earlier appendix in a structure file only
/// if every line from its marker starts with one of the prefixes it lists
/// (`@appendix_starts` in `Trellis.StructureDump`); a line it doesn't know
/// leaves the old appendix in place and the definitions load twice. The list
/// is read from the Elixir source, so it lives in one place.
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
    assert!(starts.len() > 3, "parsed {starts:?}");
    assert!(appendix.starts_with("-- Trellis definitions ("));
    for line in appendix.lines() {
        assert!(
            line.is_empty() || starts.iter().any(|start| line.starts_with(start)),
            "the Elixir client wouldn't strip this appendix line: {line}"
        );
    }
}

const STATUSES: &str =
    "select target_table || ' ' || status from transform_definitions order by id";

#[tokio::test]
async fn a_dump_loads_as_defined_not_built() {
    let cluster = TestCluster::start();
    let source = cluster.create_isolated_database().await;
    let mut raw = connect_raw(source.dsn()).await;
    let t = facade(source.dsn()).await;
    define_and_build(&t, &mut raw).await;
    assert!(
        tot_partial_rows(&raw).await > 0,
        "the source has runtime state"
    );
    assert!(
        oc_hidden_rows(&raw).await > 0,
        "the source has the JOIN's runtime state"
    );
    assert!(
        rows_json(&raw, STATUSES)
            .await
            .iter()
            .all(|s| s.ends_with(" live")),
        "{:?}",
        rows_json(&raw, STATUSES).await
    );

    let schema_sql = run(
        Command::new("pg_dump")
            .args([
                "--schema-only",
                "--no-owner",
                "--schema",
                SCHEMA,
                "--schema",
                "public",
            ])
            .arg(source.dsn()),
        "pg_dump",
    );
    let appendix = t.dump_definitions_sql().await.expect("dump definitions");
    every_line_is_one_the_elixir_client_strips(&appendix);

    let loaded = cluster.create_empty_database().await;
    load(loaded.dsn(), &schema_sql, &appendix);
    let after = connect_raw(loaded.dsn()).await;

    // The catalog rows match, apart from the build progress a define resets.
    for sql in [
        "select to_jsonb(t) - 'created_at' from schema_nodes t order by id",
        "select to_jsonb(t) - 'created_at' from schema_edges t order by id",
        "select to_jsonb(t) from source_table_versions t order by source_table",
        // #1242: a type's OID is the database's own; the name beside it loads.
        "select to_jsonb(t) - 'type_oid' from definition_key_types t order by transform_id, table_name, column_name",
        "select to_jsonb(t) - 'type_oid' from definition_types t order by transform_id, type_name",
        // #1241: an enum value's OID too; its label loads.
        "select to_jsonb(t) - 'type_oid' - 'value_oid' from definition_enum_values t \
         order by transform_id, label",
        "select to_jsonb(t) from trellis_instance t",
        "select to_jsonb(t) - 'applied_on' from refinery_schema_history t order by version",
        "select to_jsonb(t) - 'created_at' - 'status' - 'build' - 'build_seg' - 'build_marker_generation' - 'fuse_rearmed_at' from transform_definitions t order by id",
    ] {
        let want = rows_json(&raw, sql).await;
        assert!(!want.is_empty(), "{sql} has rows to compare");
        assert_eq!(rows_json(&after, sql).await, want, "{sql}");
    }

    // The enum's OID differs between the two databases, and the loaded
    // record holds the loaded database's (ADR-0020).
    let mood_oid = "select 'public.mood'::regtype::oid::bigint";
    assert_ne!(count(&raw, mood_oid).await, count(&after, mood_oid).await);
    assert_eq!(
        count(
            &after,
            "select count(*) from definition_types where type_oid = 'public.mood'::regtype"
        )
        .await,
        3,
        "each definition reading a column of the enum records the loaded database's OID"
    );
    assert_eq!(
        count(&after, "select count(*) from definition_types").await,
        3
    );
    // #1241: each recorded value resolves to the loaded database's value of
    // the same label, so a relabel there is found by OID.
    assert_eq!(
        count(
            &after,
            "select count(*) from definition_enum_values v \
             join pg_enum e on e.oid = v.value_oid \
             where e.enumtypid = 'public.mood'::regtype and v.type_oid = e.enumtypid \
               and e.enumlabel::text = v.label"
        )
        .await,
        count(&raw, "select count(*) from definition_enum_values").await,
        "every recorded enum value resolves by its label"
    );
    assert_eq!(
        count(&raw, "select count(*) from definition_enum_values").await,
        6,
        "two values for each definition reading the enum"
    );
    // `tot` groups by the enum: its key type's OID is the loaded one too.
    assert_eq!(
        count(
            &after,
            "select count(*) from definition_key_types where type_name = 'public.mood' \
             and type_oid = 'public.mood'::regtype"
        )
        .await,
        1
    );

    // Every definition is defined, not built.
    let statuses = rows_json(&after, STATUSES).await;
    assert_eq!(statuses.len(), 3);
    assert!(
        statuses.iter().all(|s| s.ends_with(" waiting_to_backfill")),
        "{statuses:?}"
    );
    // The build progress the source recorded is cleared, as a define leaves
    // it: the source's builds left it set, so the reset is what clears it.
    let progress = "select count(*) from transform_definitions \
                    where build is not null or build_seg is not null \
                       or build_marker_generation is not null or fuse_rearmed_at is not null";
    assert!(
        count(&raw, progress).await > 0,
        "the source has build progress"
    );
    assert_eq!(
        count(&after, progress).await,
        0,
        "build progress loads cleared"
    );

    // Every runtime table is empty, and the seed rows match a migrated database.
    for (table, class) in CLASSIFICATION {
        if matches!(class, Class::Runtime) {
            assert_eq!(
                count(&after, &format!("select count(*) from {SCHEMA}.{table}")).await,
                0,
                "{table} loads empty"
            );
        }
    }
    for t in ["o1", "tot", "oc"] {
        assert_eq!(
            count(&after, &format!("select count(*) from public.{t}")).await,
            0
        );
    }
    assert_eq!(
        count(&after, "select count(*) from public.o1__ledger").await,
        0
    );
    assert_eq!(
        tot_partial_rows(&after).await,
        0,
        "tot's partials load empty"
    );
    assert_eq!(
        oc_hidden_rows(&after).await,
        0,
        "oc's hidden tables load empty"
    );
    let fresh = cluster.create_isolated_database().await;
    let fresh = connect_raw(fresh.dsn()).await;
    for (table, class) in CLASSIFICATION {
        if matches!(class, Class::Seed { .. }) {
            let sql = format!("select to_jsonb(t) - 'created_at' from {SCHEMA}.{table} t");
            assert_eq!(
                rows_json(&after, &sql).await,
                rows_json(&fresh, &sql).await,
                "{table} loads its migration's seed row"
            );
        }
    }

    // The capture triggers came with the dump.
    let triggers = "select tgname::text from pg_trigger \
                    where tgrelid = 'public.orders'::regclass and not tgisinternal order by 1";
    let want = rows_json(&raw, triggers).await;
    assert!(!want.is_empty(), "the source has capture triggers");
    assert_eq!(rows_json(&after, triggers).await, want);

    // The migration history is current, so migrate does nothing.
    let t2 = facade(loaded.dsn()).await;
    t2.migrate().await.expect("migrate on the loaded database");
    assert_eq!(
        count(&after, "select count(*) from refinery_schema_history").await,
        count(&raw, "select count(*) from refinery_schema_history").await
    );

    // Identity sequences continue past the loaded rows.
    t2.apply("TRANSFORM later FROM cats SELECT name AS n")
        .await
        .expect("a define after the load");
    assert!(count(&after, "select max(id) from transform_definitions").await > 3);
    assert_eq!(
        count(&after, "select count(distinct id) from schema_nodes").await,
        count(&after, "select count(*) from schema_nodes").await
    );
}

#[tokio::test]
async fn a_loaded_database_builds_its_definitions() {
    let cluster = TestCluster::start();
    let source = cluster.create_isolated_database().await;
    let mut raw = connect_raw(source.dsn()).await;
    let t = facade(source.dsn()).await;
    define_and_build(&t, &mut raw).await;

    let schema_sql = run(
        Command::new("pg_dump")
            .args([
                "--schema-only",
                "--no-owner",
                "--schema",
                SCHEMA,
                "--schema",
                "public",
            ])
            .arg(source.dsn()),
        "pg_dump",
    );
    let appendix = t.dump_definitions_sql().await.expect("dump definitions");
    let loaded = cluster.create_empty_database().await;
    load(loaded.dsn(), &schema_sql, &appendix);
    let after = connect_raw(loaded.dsn()).await;
    after
        .batch_execute(
            "insert into public.cats values (1, 'tom'), (2, 'kit'); \
             insert into public.orders values (1, 1, 10, 1, 'calm'), (2, 1, 20, 2, 'loud'), (3, 2, 30, 1, 'calm')",
        )
        .await
        .expect("load source rows");

    let t2 = facade(loaded.dsn()).await;
    let mut worker = connect_raw(loaded.dsn()).await;
    // The staging worker's own reconcile pass, not the test harness, finds
    // the loaded definitions and starts their builds: the loaded catalog
    // has no `pending_backfill` markers, and the dump's capture triggers
    // must count as current capture.
    trellis::client::reconcile_pass(
        &mut worker,
        t2.pool(),
        SCHEMA,
        "dump_test_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass on the loaded database");
    let started = rows_json(&after, STATUSES).await;
    assert!(
        started.iter().all(|s| !s.ends_with(" waiting_to_backfill")),
        "the worker's reconcile pass starts every loaded build: {started:?}"
    );
    // The loaded rows reach the ring through the dump's capture triggers,
    // so the builds are followed by the drain the worker would run.
    for _ in 0..3 {
        build_everything(t2.pool()).await;
        drain_to_quiescence(t2.pool(), &mut worker).await;
    }

    let statuses = rows_json(&after, STATUSES).await;
    assert!(
        statuses.iter().all(|s| s.ends_with(" live")),
        "{statuses:?}"
    );
    for sql in [
        "select to_jsonb(t) from public.o1 t order by 1::text",
        "select to_jsonb(t) from public.tot t order by 1::text",
        "select to_jsonb(t) from public.oc t order by 1::text",
    ] {
        let want = rows_json(&raw, &sql.replace(" order by 1::text", "")).await;
        assert!(!want.is_empty(), "the source's {sql} has rows");
        let mut want = want;
        want.sort();
        let mut got = rows_json(&after, &sql.replace(" order by 1::text", "")).await;
        got.sort();
        assert_eq!(got, want, "{sql}");
    }
}

/// Two reads of unchanged definitions give the same SQL, even when a runtime
/// update (a build moving a status) has moved a row in the heap between them.
/// The Rails hook reads the definitions on both sides of its `pg_dump` and
/// redoes the dump when the reads differ, so heap order leaking into the SQL
/// would make a dump taken while a worker builds fail.
#[tokio::test]
async fn a_runtime_update_leaves_the_dump_unchanged() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    let t = facade(db.dsn()).await;
    raw.batch_execute(SOURCE_DDL).await.expect("source ddl");
    t.apply("TRANSFORM o1 FROM orders SELECT g AS g")
        .await
        .expect("o1");
    t.apply("TRANSFORM o2 FROM orders SELECT a AS a")
        .await
        .expect("o2");
    let before = t.dump_definitions_sql().await.expect("dump definitions");
    // A status the dump resets, written back to the first row: a new tuple,
    // after the second row in the heap.
    raw.batch_execute(
        "update transform_definitions set status = 'paused' where id = (select min(id) from transform_definitions); \
         update schema_nodes set is_source = is_source where id = (select min(id) from schema_nodes)",
    )
    .await
    .expect("move rows");
    let after = t.dump_definitions_sql().await.expect("dump definitions");
    assert_eq!(after, before);
}

/// A schema name is any identifier, `%` and `'` included, and neither may
/// reach a format template or a string literal unescaped.
#[tokio::test]
async fn a_schema_name_with_percent_and_quote_dumps() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let schema = "dump%s'test";
    let t = Trellis::connect(
        Config::with_schema(db.dsn().to_string(), schema).expect("valid schema"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    t.migrate().await.expect("migrate");
    let appendix = t.dump_definitions_sql().await.expect("dump definitions");
    assert!(
        appendix.contains(r#"insert into "dump%s'test"."refinery_schema_history""#),
        "{appendix}"
    );
    assert!(
        appendix.contains(r#"setval('"dump%s''test".ring_slot_mirror'"#),
        "{appendix}"
    );
}

#[tokio::test]
async fn every_trellis_table_is_classified() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    let mut actual: Vec<String> = rows_json(
        &raw,
        &format!(
            "select c.relname::text from pg_class c join pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = '{SCHEMA}' and c.relkind in ('r', 'p') order by 1"
        ),
    )
    .await;
    actual.sort();
    let mut classified: Vec<String> = CLASSIFICATION.iter().map(|(t, _)| t.to_string()).collect();
    classified.sort();
    assert_eq!(
        actual, classified,
        "every table in the Trellis schema is listed in trellis::dump::CLASSIFICATION as a \
         definition, a seed or runtime state, and no listed table is gone"
    );
}

fn tempfile_dir() -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("trellis_dump_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}
