//! A logical restore (`pg_dump`, `pg_restore`) of a database running Trellis:
//! the ordering state it brings back holds the original cluster's WAL
//! positions and transaction ids, so the capture pass pauses every definition
//! with the restore reason and discards the ring, and `RESUME` resets the
//! ordering state before its rebuild. The operations that keep the counters
//! (`pg_upgrade`, a physical copy, `CREATE DATABASE ... TEMPLATE`) pause
//! nothing.
//!
//! The original is a 1-1, a group-basis aggregate and a `JOIN`, all `live`,
//! in a cluster whose WAL and transaction counter were advanced before the
//! definitions applied anything, so their ledgers and groups hold positions
//! ahead of a fresh cluster's. Nothing polls for convergence; every step is an
//! explicit call.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::process::Command;
use std::time::{Duration, Instant};

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisOptions};

const SCHEMA: &str = "trellis";

async fn connect(dsn: &str) -> Client {
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

async fn definer(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a definer")
}

fn pool_for(dsn: &str) -> trellis::Pool {
    trellis::Pool::new(&Config::from_dsn(dsn.to_string()).expect("valid dsn")).expect("pool")
}

async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(raw, pool, SCHEMA)
        .await
        .expect("reconcile pass");
}

async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    let outcome = reconcile::reconcile(
        raw,
        SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(2),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "logical_restore_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "logical_restore_test",
            1,
            "trellis_logical_restore_test",
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

async fn run_backfill_chunks(pool: &trellis::Pool) {
    const CLAIMED_BY: &str = "logical_restore_backfill";
    trellis::staging::build::settle_builds(pool).await;
}

async fn statuses(raw: &Client) -> BTreeMap<String, TransformStatus> {
    raw.query(
        "select split_part(target_table, '.', 2), status from transform_definitions",
        &[],
    )
    .await
    .expect("read statuses")
    .into_iter()
    .map(|row| {
        (
            row.get(0),
            trellis::defs::StoredStatus::from_row(row.get(1)).into(),
        )
    })
    .collect()
}

async fn bring_live(raw: &mut Client, pool: &trellis::Pool) {
    for _ in 0..10 {
        full_pass(raw, pool).await;
        run_backfill_chunks(pool).await;
        drain_to_quiescence(pool, raw).await;
        if statuses(raw)
            .await
            .values()
            .all(|s| *s == TransformStatus::Live)
        {
            return;
        }
    }
    panic!("not every definition went live: {:?}", statuses(raw).await);
}

async fn rows(raw: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

async fn count(raw: &Client, sql: &str) -> i64 {
    raw.query_one(sql, &[]).await.expect(sql).get(0)
}

fn run(command: &mut Command) {
    let out = command.output().expect("run a client tool");
    assert!(
        out.status.success(),
        "{command:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

const TARGETS: [&str; 3] = ["one", "gsum", "jl"];

/// Each target against the query that defines it, over the sources now.
async fn assert_oracle(raw: &Client, when: &str, only: &[&str]) {
    for (target, actual, expected) in [
        (
            "one",
            "select id::text, v::text from public.one order by 1",
            "select id::text, v::text from public.s1 order by 1",
        ),
        (
            "gsum",
            "select grp::text, s::text, n::text from public.gsum order by 1",
            "select grp::text, sum(amt)::text, count(*)::text from public.g group by grp \
             order by 1",
        ),
        (
            "jl",
            "select k::text, n::text, sy::text from public.jl order by 1",
            "select ja.k::text, count(*)::text, sum(jb.y)::text from public.ja \
             left join public.jb on ja.k = jb.k group by ja.k order by 1",
        ),
    ] {
        if !only.contains(&target) {
            continue;
        }
        assert_eq!(
            rows(raw, actual).await,
            rows(raw, expected).await,
            "{when}: {target} diverged from its oracle"
        );
    }
}

/// Advances the cluster's WAL (about 24 MB) and burns transaction ids, so a
/// fresh cluster is behind both.
async fn advance_counters(raw: &Client) {
    raw.batch_execute(
        "select pg_logical_emit_message(true, 'pad', repeat('x', 1000000)) \
         from generate_series(1, 24)",
    )
    .await
    .expect("advance the WAL");
    raw.batch_execute(
        "do $$ begin for i in 1..3000 loop perform pg_current_xact_id(); commit; end loop; end $$",
    )
    .await
    .expect("advance the counters");
}

struct Original {
    db: TestDatabase,
    raw: Client,
    dump: std::path::PathBuf,
}

/// The original database: three live definitions whose entries hold positions
/// of an advanced cluster, a few changes pending in the ring, and a dump.
async fn original(cluster: &TestCluster) -> Original {
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    // The rows arrive through capture.
    raw.batch_execute(
        "create table public.s1 (id int primary key, v int); \
         create table public.g (id int primary key, grp int not null, amt int); \
         create table public.ja (id int primary key, k varchar(5), x int); \
         create table public.jb (id int primary key, k varchar(5), y int); \
         create index on public.g (grp); \
         create index on public.ja (k); \
         create index on public.jb (k)",
    )
    .await
    .expect("create the sources");
    for text in [
        "TRANSFORM one FROM public.s1 SELECT v AS v",
        "TRANSFORM gsum FROM public.g GROUP BY grp WITHOUT SOURCE INDEX SELECT grp AS grp, SUM(amt) AS s, \
         COUNT(*) AS n",
        "TRANSFORM jl FROM public.ja LEFT JOIN public.jb ON ja.k = jb.k \
         GROUP BY ja.k WITHOUT SOURCE INDEX SELECT ja.k AS k, COUNT(*) AS n, SUM(jb.y) AS sy",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool).await;
    raw.batch_execute(
        "insert into public.s1 select i, i * 10 from generate_series(1, 6) i; \
         insert into public.g select i, i % 3, i from generate_series(1, 8) i; \
         insert into public.ja values (1, 'a', 1), (2, 'a', 2), (3, 'b', 3), (4, 'c', 4); \
         insert into public.jb values (1, 'a', 5), (2, 'a', 10), (3, 'b', 15), (4, 'd', 20)",
    )
    .await
    .expect("seed");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_oracle(&raw, "the original, live", &TARGETS).await;

    // Changes applied at positions of the advanced cluster, a truncate among
    // them: its floor is the cluster's position.
    advance_counters(&raw).await;
    raw.batch_execute(
        "truncate public.s1; \
         insert into public.s1 select i, i * 10 from generate_series(1, 6) i; \
         update public.s1 set v = v + 1 where id <= 3; \
         update public.g set amt = amt + 100 where id <= 3; \
         update public.ja set x = x + 1 where id <= 2; \
         update public.jb set y = y + 1 where id = 1",
    )
    .await
    .expect("changes");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_oracle(&raw, "the original, after its changes", &TARGETS).await;
    assert!(
        count(
            &raw,
            "select count(*) from public.one__ledger where __applied_lsn is not null"
        )
        .await
            > 0
            && count(&raw, "select count(*) from ledger_truncate_floor").await > 0,
        "the 1-1 ledger and its floor hold the cluster's positions"
    );
    let (agg, join) = hidden_ids(&raw).await;
    // A group-basis rebuild stamps every group it recomputes with a
    // snapshot and the segment it read; the steady state leaves both null.
    raw.batch_execute(&format!(
        "update agg_{agg} set __basis = pg_current_snapshot(), __stamp = 1000"
    ))
    .await
    .expect("stamp the groups");
    assert!(
        count(
            &raw,
            &format!("select count(*) from agg_{agg} where __basis is not null")
        )
        .await
            > 0,
        "the group basis holds the cluster's snapshots"
    );
    assert!(
        count(&raw, &format!("select count(*) from join_{join}_p")).await > 0,
        "the JOIN's hidden tables hold state"
    );

    // Pending in the ring at dump time, with a copy of each staged change that
    // says something else (a stale row a restore must never apply).
    raw.batch_execute(
        "update public.s1 set v = 444 where id = 4; \
         update public.g set amt = 444 where id = 4; \
         update public.jb set y = 222 where id = 2",
    )
    .await
    .expect("pending changes");
    let slot: i16 = raw
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .unwrap()
        .get(0);
    raw.batch_execute(&format!(
        "insert into seg_{slot} (src_table, key, op, lsn, old_image, new_image, origin_lsn, \
                                 src_changed) \
         select src_table, key, op, lsn + 100000, old_image, \
                new_image || jsonb_build_object( \
                    coalesce((select 'v' where new_image ? 'v'), \
                             (select 'amt' where new_image ? 'amt'), 'y'), -1), \
                origin_lsn, src_changed \
         from seg_{slot}"
    ))
    .await
    .expect("stale copies");
    assert!(count(&raw, &format!("select count(*) from seg_{slot}")).await >= 6);

    let dump = cluster.root().join("original.dump");
    run(Command::new("pg_dump")
        .args(["-Fc", "-f"])
        .arg(&dump)
        .arg(db.dsn()));
    Original { db, raw, dump }
}

async fn ring_rows(raw: &Client) -> i64 {
    count(
        raw,
        "select (select count(*) from seg_0) + (select count(*) from seg_1) \
              + (select count(*) from seg_2) + (select count(*) from seg_3)",
    )
    .await
}

/// Everything is paused with the restore reason, the ring is empty, and the
/// records of the dump are forgotten.
async fn assert_paused_by_the_restore(raw: &Client, trellis: &Trellis) {
    for (target, status) in statuses(raw).await {
        assert_eq!(status, TransformStatus::Paused, "{target}");
    }
    for target in TARGETS {
        let status = trellis
            .status(target)
            .await
            .expect("status")
            .expect("defined");
        let failure = status.capture_failure.expect("a capture_failure");
        assert!(
            failure.error.contains("restored from a logical dump"),
            "{target}: {}",
            failure.error
        );
        assert_eq!(failure.kind.as_str(), "disruption", "{target}");
    }
    assert_eq!(
        ring_rows(raw).await,
        0,
        "no ring row survives the detection"
    );
}

/// The ids the group-basis aggregate's and the `JOIN`'s hidden tables are
/// named by.
async fn hidden_ids(raw: &Client) -> (i64, i64) {
    let id = |target: &str| {
        let sql =
            format!("select id from transform_definitions where target_table = 'public.{target}'");
        async move { raw.query_one(&sql, &[]).await.expect("id").get::<_, i64>(0) }
    };
    (id("gsum").await, id("jl").await)
}

/// The resumes of the group-basis aggregate and the `JOIN` reset their
/// ordering state before the rebuild: each drops and recreates its hidden
/// tables (the JOIN's lock table with its one lock row). Both builds run in
/// the passes that follow.
async fn resume_groups_and_check_the_reset(raw: &Client, trellis: &Trellis) {
    let (agg, join) = hidden_ids(raw).await;
    for target in ["gsum", "jl"] {
        trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .unwrap_or_else(|e| panic!("{target}: {e}"));
    }
    assert_eq!(
        count(raw, &format!("select count(*) from agg_{agg}")).await,
        0,
        "the group basis starts over, with no partials, basis or stamp"
    );
    for part in ["p", "m", "j", "t"] {
        assert_eq!(
            count(raw, &format!("select count(*) from join_{join}_{part}")).await,
            0,
            "the JOIN's {part} is recreated"
        );
    }
    assert_eq!(
        count(raw, &format!("select count(*) from join_{join}_l")).await,
        1,
        "the JOIN's lock table is recreated with its lock row"
    );
}

/// RESUME the 1-1, bring it live, then an update, a delete and an insert of
/// keys that existed before the dump: all apply, and the target is the
/// query's.
async fn resume_and_write(raw: &mut Client, trellis: &Trellis, pool: &trellis::Pool) {
    resume_groups_and_check_the_reset(raw, trellis).await;
    // The `JOIN`'s resume rebuilt both its tables, keeping its target:
    // zero diff, and later changes on either side apply, among them a flip.
    bring_live_only(raw, pool, "jl").await;
    assert_oracle(raw, "the JOIN after the resume", &["jl"]).await;
    raw.batch_execute(
        "update public.ja set x = x + 5 where id = 1; \
         update public.jb set y = y + 5 where id = 3; \
         delete from public.jb where id = 3; \
         insert into public.jb values (10, 'c', 7); \
         delete from public.ja where id = 2; \
         insert into public.ja values (10, 'd', 1)",
    )
    .await
    .expect("join writes");
    drain_to_quiescence(pool, raw).await;
    assert_oracle(raw, "the JOIN after writes", &["jl"]).await;
    trellis
        .apply("RESUME TRANSFORM one")
        .await
        .expect("resume one");
    bring_live_only(raw, pool, "one").await;
    bring_live_only(raw, pool, "gsum").await;
    assert_oracle(raw, "after the resume", &["one", "gsum"]).await;
    raw.batch_execute(
        "update public.s1 set v = 1000 where id = 1; \
         delete from public.s1 where id = 2; \
         insert into public.s1 values (10, 100); \
         update public.s1 set v = v + 1 where id = 4; \
         update public.g set amt = amt + 1000 where id <= 2; \
         delete from public.g where id = 3; \
         insert into public.g values (30, 7, 5)",
    )
    .await
    .expect("writes");
    drain_to_quiescence(pool, raw).await;
    assert_oracle(
        raw,
        "after writes to keys that existed before the dump",
        &["one", "gsum"],
    )
    .await;
    // `self_check` over the restarted registry (its page filter): the
    // slots the restore left with no segment count as applied, and the
    // comparison agrees with the oracle.
    use trellis::staging::self_check::{CheckPageExt, SelfCheckScope};
    let report = trellis
        .self_check_page(
            "one",
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            trellis::SelfCheckMode::Strict,
            Duration::from_secs(30),
        )
        .await
        .expect("self_check one");
    assert!(
        matches!(report.outcome, trellis::SelfCheckOutcome::Converged),
        "self_check after the resume: {:?}",
        report.outcome
    );
}

/// Like [`bring_live`], for the one definition `target`: the paused others
/// are left as they are.
async fn bring_live_only(raw: &mut Client, pool: &trellis::Pool, target: &str) {
    for _ in 0..10 {
        full_pass(raw, pool).await;
        run_backfill_chunks(pool).await;
        drain_to_quiescence(pool, raw).await;
        if statuses(raw).await[target] == TransformStatus::Live {
            return;
        }
    }
    panic!("{target} did not go live: {:?}", statuses(raw).await);
}

/// The repro: the restored database's cluster is behind the original's WAL and
/// transaction counter.
#[tokio::test]
async fn a_logical_restore_behind_the_original_pauses_everything_and_resume_converges() {
    let a = TestCluster::start();
    let original = original(&a).await;

    let b = TestCluster::start();
    let restored = b.create_empty_database().await;
    run(Command::new("pg_restore")
        .args(["--no-owner", "-d", restored.dsn()])
        .arg(&original.dump));
    let mut raw = connect(restored.dsn()).await;
    let trellis = definer(restored.dsn()).await;
    assert!(
        count(
            &raw,
            "select count(*) from public.one__ledger \
             where __applied_lsn > pg_current_wal_insert_lsn()"
        )
        .await
            > 0,
        "the restore is behind the ledger's positions"
    );
    assert!(
        ring_rows(&raw).await >= 6,
        "the dump holds the pending ring rows"
    );
    // A dump reads the slot mirror outside its snapshot: one taken during a
    // seal carries a mirror naming another slot than the pointer.
    raw.batch_execute(
        "select setval('ring_slot_mirror', (ring_slot + 2) % 4, true) from segment_pointer",
    )
    .await
    .expect("skew the slot mirror");

    capture_pass(&mut raw, &restored.pool).await;
    assert_paused_by_the_restore(&raw, &trellis).await;
    // Nothing rebuilds by itself, and a second pass finds nothing more.
    capture_pass(&mut raw, &restored.pool).await;
    assert_paused_by_the_restore(&raw, &trellis).await;

    // Writes between the detection and the resume. The insert is staged in
    // the active segment's slot: a write staged in another slot would sit
    // where no seal looks (the worker seals only a non-empty active
    // segment), and no change after the restore would ever apply. The
    // resume's rebuild covers both writes.
    raw.batch_execute(
        "update public.s1 set v = 555 where id = 5; insert into public.s1 values (20, 200)",
    )
    .await
    .expect("writes while paused");
    let slot: i16 = raw
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        (
            count(&raw, &format!("select count(*) from seg_{slot}")).await,
            ring_rows(&raw).await
        ),
        (1, 1),
        "the insert is staged in the active segment's slot"
    );

    resume_and_write(&mut raw, &trellis, &restored.pool).await;
    assert_eq!(
        rows(
            &raw,
            "select v::text from public.one where id in (5, 20) order by id"
        )
        .await,
        vec![vec![Some("555".to_string())], vec![Some("200".to_string())]]
    );
}

/// A restore into another database of the same cluster, whose counters only
/// move forward, pauses all the same and costs one resume; over the original
/// itself with `--clean` the same.
#[tokio::test]
async fn a_logical_restore_into_the_same_cluster_pauses_then_resume_converges() {
    let a = TestCluster::start();
    let original = original(&a).await;

    let other = a.create_empty_database().await;
    run(Command::new("pg_restore")
        .args(["--no-owner", "-d", other.dsn()])
        .arg(&original.dump));
    let mut raw = connect(other.dsn()).await;
    let trellis = definer(other.dsn()).await;
    capture_pass(&mut raw, &other.pool).await;
    assert_paused_by_the_restore(&raw, &trellis).await;
    resume_and_write(&mut raw, &trellis, &other.pool).await;

    // Over the original itself.
    let mut over = connect(original.db.dsn()).await;
    run(Command::new("pg_restore")
        .args([
            "--clean",
            "--if-exists",
            "--no-owner",
            "-d",
            original.db.dsn(),
        ])
        .arg(&original.dump));
    let trellis = definer(original.db.dsn()).await;
    capture_pass(&mut over, &original.db.pool).await;
    assert_paused_by_the_restore(&over, &trellis).await;
    resume_and_write(&mut over, &trellis, &original.db.pool).await;
}

/// `pg_upgrade` keeps every oid and changes the cluster's identifier: the
/// record is re-recorded and nothing pauses, nothing is discarded.
#[tokio::test]
async fn a_changed_system_identifier_with_the_same_catalog_oid_pauses_nothing() {
    let a = TestCluster::start();
    let mut original = original(&a).await;
    let before = ring_rows(&original.raw).await;
    original
        .raw
        .batch_execute("update capture_triggers set system_identifier = '1'")
        .await
        .unwrap();
    capture_pass(&mut original.raw, &original.db.pool).await;
    capture_pass(&mut original.raw, &original.db.pool).await;
    for (target, status) in statuses(&original.raw).await {
        assert_eq!(status, TransformStatus::Live, "{target}");
    }
    assert_eq!(ring_rows(&original.raw).await, before, "the ring is kept");
    assert_eq!(
        count(
            &original.raw,
            "select count(*) from capture_triggers where system_identifier = '1'"
        )
        .await,
        0,
        "re-recorded under the live identifier"
    );
}

/// Operations that keep the oids, the WAL and the xid counter pause nothing: a
/// cold copy of the cluster (a physical restore) and `CREATE DATABASE ...
/// TEMPLATE`.
#[tokio::test]
async fn a_physical_copy_and_a_template_database_pause_nothing() {
    let a = TestCluster::start();
    let original = original(&a).await;

    // `CREATE DATABASE ... TEMPLATE` needs the template idle.
    let admin = connect(&a.database_dsn("postgres")).await;
    admin
        .execute(
            "select pg_terminate_backend(pid) from pg_stat_activity \
             where datname = $1 and pid <> pg_backend_pid()",
            &[&original.db.name()],
        )
        .await
        .unwrap();
    admin
        .batch_execute(&format!(
            "create database lr_template template \"{}\"",
            original.db.name()
        ))
        .await
        .unwrap();
    let dsn = a.database_dsn("lr_template");
    let mut raw = connect(&dsn).await;
    let pool = pool_for(&dsn);
    let ring = ring_rows(&raw).await;
    capture_pass(&mut raw, &pool).await;
    capture_pass(&mut raw, &pool).await;
    for (target, status) in statuses(&raw).await {
        assert_eq!(status, TransformStatus::Live, "template: {target}");
    }
    assert_eq!(ring_rows(&raw).await, ring);

    // A cold copy of the whole cluster, as a base backup restores it.
    let backup = a.cold_backup();
    let copy = TestCluster::from_backup(&backup);
    let dsn = copy.database_dsn(original.db.name());
    let mut raw = connect(&dsn).await;
    let pool = pool_for(&dsn);
    capture_pass(&mut raw, &pool).await;
    capture_pass(&mut raw, &pool).await;
    for (target, status) in statuses(&raw).await {
        assert_eq!(status, TransformStatus::Live, "physical copy: {target}");
    }
    assert_eq!(ring_rows(&raw).await, ring);
    // And its ordering state is valid: the pending changes apply, the stale
    // copy (a later change to the key, here) last.
    drain_to_quiescence(&pool, &mut raw).await;
    assert_eq!(
        rows(&raw, "select v::text from public.one where id = 4").await,
        vec![vec![Some("-1".to_string())]]
    );
}

/// A drain worker's gate before its first claim. With the restore
/// signal set and a sealed segment holding a ring row, the gate reports
/// "wait" while a claim would still hand the segment out (without the gate a
/// drain-only worker applies the pre-detection row); once the staging pass
/// has responded, the gate reports "go". With no stale record it is open at
/// once.
#[tokio::test]
async fn a_drain_worker_waits_for_the_restore_response_before_its_first_claim() {
    let a = TestCluster::start();
    let mut original = original(&a).await;
    let pool = &original.db.pool;

    assert!(
        !trellis::client::restore_gate_closed(pool).await,
        "no stale record: go at once"
    );

    // A sealed segment holding a ring row.
    let outcome = trellis::staging::seal::seal_phase1(&mut original.raw)
        .await
        .expect("seal phase 1");
    trellis::staging::seal::seal_phase2(&original.raw, outcome.sealed_seg_seq, "gate_test")
        .await
        .expect("seal phase 2");
    assert!(ring_rows(&original.raw).await > 0, "the ring holds rows");

    // The restore signal: a stored oid that isn't the live one.
    original
        .raw
        .batch_execute(
            "update capture_triggers set catalog_oid = (catalog_oid::bigint + 1000)::oid",
        )
        .await
        .unwrap();
    assert!(
        trellis::client::restore_gate_closed(pool).await,
        "the signal is set: wait"
    );
    let claimable = trellis::staging::next_claimable_segments(&**pool.get().await.unwrap(), 8)
        .await
        .expect("claimable");
    assert!(
        !claimable.is_empty(),
        "without the gate the first claim hands out the pre-detection segment"
    );

    capture_pass(&mut original.raw, pool).await;
    assert!(
        !trellis::client::restore_gate_closed(pool).await,
        "the staging pass responded: go"
    );
    assert_eq!(
        ring_rows(&original.raw).await,
        0,
        "the response emptied the ring"
    );
}
