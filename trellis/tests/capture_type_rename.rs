//! A type renamed or moved to another schema after define is followed in
//! place (issues #1204, #1242).
//!
//! `ALTER TYPE tone RENAME TO tone2` and `ALTER TYPE tone SET SCHEMA other`
//! change no value and touch nothing Trellis created, but Trellis casts
//! staged values by the type's name as it recorded it. Each definition
//! records the OID of every type it names beside the name (ADR-0020). The
//! staging worker's capture pass finds a recorded OID whose `schema.typname`
//! is not the recorded one, re-records the name wherever Trellis persists it,
//! and releases the keys held for the failed cast (`42704`). Nothing pauses,
//! rebuilds or reinstalls, whenever the rename happens.
//!
//! Every test is stepped by hand: writes, then explicit capture passes and
//! drains. Nothing polls for convergence (#297).

use std::process::Command;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisOptions};

const SCHEMA: &str = "trellis";

const TARGETS: [&str; 5] = ["tone_pass", "per_tone", "kind_v", "by_tone_join", "chained"];

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

/// One whole staging-worker pass: capture, markers, discharge, and the
/// release of the keys a re-type asked for.
async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(
        raw,
        pool,
        SCHEMA,
        "capture_type_rename_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
}

/// The capture half of one pass only. Asserts no table failed.
async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    let outcome = reconcile::reconcile(
        raw,
        SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "capture_type_rename_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "capture_type_rename_test",
            1,
            "trellis_capture_type_rename_test",
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

/// Drains the active segment, whose rows include keys whose write fails:
/// each failed drain charges the key a death, until it is held and the
/// drain goes past it. Bounded attempts, not a timed wait.
async fn drain_past_failures(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{apply, retire_drained_segments, seal};
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "capture_type_rename_wake")
        .await
        .expect("seal phase 2");
    let mut drained = false;
    for _ in 0..16 {
        match apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "capture_type_rename_test",
            1,
            "trellis_capture_type_rename_test",
        )
        .await
        {
            Ok(Some(_)) | Err(_) => {}
            Ok(None) => {
                drained = true;
                break;
            }
        }
    }
    assert!(drained, "the segment did not drain within 16 attempts");
    retire_drained_segments(client)
        .await
        .expect("retire drained segments");
}

async fn run_backfill_chunks(pool: &trellis::Pool) {
    use trellis::defs::chunk_queue;
    const CLAIMED_BY: &str = "capture_type_rename_backfill";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            trellis::staging::build::settle_builds(pool).await;
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                CLAIMED_BY,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

async fn status(raw: &Client, target: &str) -> TransformStatus {
    let text: String = raw
        .query_one(
            "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0);
    TransformStatus::from_persisted(&text).expect("known status")
}

/// Alternates staging-worker passes and drains until every one of
/// `targets` is live: a bounded number of explicit steps, not a timed wait.
async fn bring_live(raw: &mut Client, pool: &trellis::Pool, targets: &[&str]) {
    for _ in 0..8 {
        full_pass(raw, pool).await;
        run_backfill_chunks(pool).await;
        drain_to_quiescence(pool, raw).await;
        let mut all_live = true;
        for target in targets {
            all_live &= status(raw, target).await == TransformStatus::Live;
        }
        if all_live {
            return;
        }
    }
    for target in targets {
        eprintln!("{target}: {:?}", status(raw, target).await);
    }
    panic!("{targets:?} did not all go live within 8 pass/drain rounds");
}

async fn rows(raw: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    raw.query(sql, &[])
        .await
        .expect(sql)
        .into_iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

async fn held_keys(raw: &Client) -> i64 {
    raw.query_one("select count(*) from poison", &[])
        .await
        .expect("count held keys")
        .get(0)
}

/// Asserts every target is live and equals what the source says: zero
/// difference to the oracle.
async fn assert_converged(raw: &Client) {
    for target in TARGETS {
        assert_eq!(status(raw, target).await, TransformStatus::Live, "{target}");
    }
    assert_eq!(held_keys(raw).await, 0, "no key is held");
    let pairs = [
        (
            "select id::text, tone::text from public.tone_pass order by id",
            "select id::text, tone::text from public.items order by id",
        ),
        (
            "select tone::text, n::text from public.per_tone where n is not null \
             order by tone::text",
            "select tone::text, count(*)::text from public.items group by tone \
             order by tone::text",
        ),
        (
            "select tone::text, n::text from public.chained where n is not null \
             order by tone::text",
            "select tone::text, count(*)::text from public.items group by tone \
             order by tone::text",
        ),
        (
            "select tone::text, v::text from public.kind_v order by tone::text",
            "select tone::text, v::text from public.kinds order by tone::text",
        ),
        (
            "select tone::text, n::text from public.by_tone_join where n is not null \
             order by tone::text",
            "select i.tone::text, count(*)::text from public.items i \
             join public.notes n on n.item_id = i.id group by i.tone order by i.tone::text",
        ),
    ];
    for (target, oracle) in pairs {
        assert_eq!(rows(raw, target).await, rows(raw, oracle).await, "{target}");
    }
}

/// An enum `tone` read by five live definitions: a 1-1 passthrough, a
/// `GROUP BY` aggregate, a 1-1 whose source key is a `tone`, a JOIN grouped
/// by it, and an aggregate over the passthrough's target (a table no capture
/// trigger watches, so no baseline of its own).
async fn setup(dsn: &str, raw: &mut Client, pool: &trellis::Pool) -> Trellis {
    raw.batch_execute(
        "create schema other; \
         create type public.tone as enum ('calm', 'loud', 'brisk'); \
         create table public.items (id int primary key, tone tone, n int); \
         create table public.kinds (tone tone primary key, v int); \
         create table public.notes (id int primary key, item_id int); \
         create index on public.items (tone); create index on public.notes (item_id);",
    )
    .await
    .expect("seed");
    let trellis = definer(dsn).await;
    for text in [
        "TRANSFORM tone_pass FROM public.items SELECT tone AS tone",
        "TRANSFORM per_tone FROM public.items GROUP BY tone WITHOUT SOURCE INDEX SELECT tone AS tone, COUNT(*) AS n",
        "TRANSFORM kind_v FROM public.kinds SELECT v AS v",
        "TRANSFORM by_tone_join FROM public.items JOIN public.notes ON items.id = notes.item_id \
         GROUP BY items.tone WITHOUT SOURCE INDEX SELECT items.tone AS tone, COUNT(*) AS n",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(raw, pool, &TARGETS[..4]).await;
    raw.batch_execute("create index on public.tone_pass (tone)")
        .await
        .expect("index the pass target");
    let chained = "TRANSFORM chained FROM public.tone_pass GROUP BY tone \
                  WITHOUT SOURCE INDEX SELECT tone AS tone, COUNT(*) AS n";
    trellis.apply(chained).await.expect(chained);
    bring_live(raw, pool, &TARGETS).await;
    // The sources start empty: the rows arrive through capture.
    raw.batch_execute(
        "insert into public.items values (1, 'calm', 1), (2, 'loud', 2), (3, 'calm', 3); \
         insert into public.kinds values ('calm', 10), ('loud', 20); \
         insert into public.notes values (1, 1), (2, 2), (3, 3);",
    )
    .await
    .expect("seed");
    full_pass(raw, pool).await;
    drain_to_quiescence(pool, raw).await;
    assert_converged(raw).await;
    trellis
}

/// Writes that touch every definition: a new item with a new tone, a re-tone,
/// a new kind and a new note. `tone` is the type's name at the time.
async fn write(raw: &Client, tone_type: &str, round: i32) {
    let (a, b) = if round == 1 {
        ("brisk", "loud")
    } else {
        ("calm", "brisk")
    };
    raw.batch_execute(&format!(
        "insert into public.items values ({}, '{a}'::{tone_type}, 7); \
         update public.items set tone = '{b}'::{tone_type} where id = 2; \
         insert into public.kinds values ('{a}'::{tone_type}, {}) \
           on conflict (tone) do update set v = excluded.v; \
         insert into public.notes values ({}, {});",
        10 * round,
        100 * round,
        10 * round,
        10 * round,
    ))
    .await
    .expect("write");
}

/// The recorded type names of the enum, anywhere Trellis persists them.
async fn recorded_names(raw: &Client) -> Vec<String> {
    let mut names: Vec<String> = rows(
        raw,
        "select distinct v from ( \
           select e.value as v from transform_definitions d, \
                  jsonb_each_text(d.source_columns) e \
           union all select e.value from transform_definitions d, \
                  jsonb_each_text(d.join_columns) e \
           union all select type_name from definition_key_types \
           union all select type_name from capture_baselines) t \
         where v like '%tone%'",
    )
    .await
    .into_iter()
    .map(|row| row[0].clone().expect("name"))
    .collect();
    names.sort();
    names
}

/// The type is renamed or moved with writes before and after the change and
/// before and after the pass that follows it. The drain between the change
/// and the pass fails to cast by the old name and holds its keys; the pass
/// re-records the name and releases them.
async fn follows(alter: &str, tone_type: &str, recorded: &[&str]) {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    assert!(
        recorded_names(&raw)
            .await
            .iter()
            .all(|n| n.contains("public.tone")),
        "{:?}",
        recorded_names(&raw).await
    );

    write(&raw, "tone", 1).await;
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_converged(&raw).await;

    raw.batch_execute(alter).await.expect("alter the type");
    write(&raw, tone_type, 2).await;
    // Before the pass, the drain casts by a name that no longer exists.
    drain_past_failures(&db.pool, &mut raw).await;
    assert!(
        held_keys(&raw).await > 0,
        "the failed casts hold their keys"
    );

    capture_pass(&mut raw, &db.pool).await;
    for target in TARGETS {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
        let reported = trellis.status(target).await.expect("status").expect(target);
        assert!(reported.capture_failure.is_none(), "{target}");
    }
    assert_eq!(recorded_names(&raw).await, recorded);

    // The release is the discharge half of a pass.
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_converged(&raw).await;

    // Writes after the pass cast by the new name.
    write(&raw, tone_type, 3).await;
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_converged(&raw).await;
}

#[tokio::test]
async fn a_renamed_type_is_followed_in_place_by_every_reader() {
    follows(
        "alter type public.tone rename to tone2",
        "tone2",
        &["enum:public.tone2", "public.tone2"],
    )
    .await;
}

#[tokio::test]
async fn a_type_moved_to_another_schema_is_followed_in_place_by_every_reader() {
    follows(
        "alter type public.tone set schema other",
        "other.tone",
        &["enum:other.tone", "other.tone"],
    )
    .await;
}

/// A pass with no baseline (a restore forgets them) and a column retyped
/// meanwhile: the pass records a baseline and re-records nothing.
#[tokio::test]
async fn an_unchanged_name_is_never_re_recorded() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let _trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let before = recorded_names(&raw).await;
    raw.batch_execute(
        "delete from capture_baselines; delete from retype_releases; \
         alter table public.items alter column n type bigint",
    )
    .await
    .expect("simulate a restore");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let releases: i64 = raw
        .query_one("select count(*) from retype_releases", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(releases, 0, "nothing was re-recorded");
    assert_eq!(recorded_names(&raw).await, before);
    for target in TARGETS {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }
}

/// `ALTER TYPE ... RENAME VALUE` after the writes drained is no rename: the
/// labels Trellis stored no longer name a value (#1241), so every reader of
/// the type pauses, the chained one over the passthrough's copy included,
/// and `RESUME` rebuilds each (`capture_enum_relabel.rs` has each shape).
#[tokio::test]
async fn a_relabelled_value_pauses_every_reader_and_resume_rebuilds() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;

    write(&raw, "tone", 1).await;
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    raw.batch_execute("alter type public.tone rename value 'brisk' to 'zippy'")
        .await
        .expect("relabel");
    capture_pass(&mut raw, &db.pool).await;
    for target in TARGETS {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Paused,
            "{target}"
        );
        let reported = trellis.status(target).await.expect("status").expect(target);
        let reason = reported
            .capture_failure
            .map(|f| f.error)
            .unwrap_or_default();
        assert!(
            reason.starts_with("enum value 'brisk' of type public.tone was renamed to 'zippy'"),
            "{target}: {reason}"
        );
    }
    for target in TARGETS {
        trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .expect(target);
    }
    bring_live(&mut raw, &db.pool, &TARGETS).await;
    assert_converged(&raw).await;
}

async fn source_column_token(raw: &Client, target: &str, column: &str) -> String {
    raw.query_one(
        "select source_columns ->> $2 from transform_definitions \
         where split_part(target_table, '.', 2) = $1",
        &[&target, &column],
    )
    .await
    .expect("read the recorded token")
    .get(0)
}

/// The type is renamed and a new type is created at its old name, read by a
/// definition defined before the pass that follows the rename (the first
/// half of the usual migration that drops an enum label). The pass
/// re-records the old name only for the columns of the renamed type: the
/// new definition keeps casting by the name its own type has.
#[tokio::test]
async fn a_new_type_at_the_old_name_keeps_its_own_readers() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create type public.tone as enum ('calm', 'loud'); \
         create table public.items (id int primary key, tone tone);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM tone_pass FROM public.items SELECT tone AS tone")
        .await
        .expect("define tone_pass");
    bring_live(&mut raw, &db.pool, &["tone_pass"]).await;
    raw.batch_execute("insert into public.items values (1, 'calm'), (2, 'loud')")
        .await
        .expect("write");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    raw.batch_execute(
        "alter type public.tone rename to tone_old; \
         create type public.tone as enum ('x', 'y'); \
         create table public.fresh (id int primary key, tone public.tone);",
    )
    .await
    .expect("rename, and create a new type at the old name");
    trellis
        .apply("TRANSFORM fresh_pass FROM public.fresh SELECT tone AS tone")
        .await
        .expect("define fresh_pass");
    bring_live(&mut raw, &db.pool, &["tone_pass", "fresh_pass"]).await;
    assert_eq!(
        source_column_token(&raw, "tone_pass", "tone").await,
        "enum:public.tone_old"
    );
    assert_eq!(
        source_column_token(&raw, "fresh_pass", "tone").await,
        "enum:public.tone"
    );

    raw.batch_execute(
        "insert into public.fresh values (1, 'x'), (2, 'y'); \
         insert into public.items values (3, 'loud'); \
         update public.items set tone = 'loud' where id = 1;",
    )
    .await
    .expect("write");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(held_keys(&raw).await, 0, "no key is held");
    for target in ["tone_pass", "fresh_pass"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }
    for (target, source) in [("tone_pass", "items"), ("fresh_pass", "fresh")] {
        assert_eq!(
            rows(
                &raw,
                &format!("select id::text, tone::text from public.{target} order by id")
            )
            .await,
            rows(
                &raw,
                &format!("select id::text, tone::text from public.{source} order by id")
            )
            .await,
            "{target}"
        );
    }
}

/// A partitioned source (an aggregate grouped by its partition key: define
/// refuses a 1-1 from one, and an enum partition key): the
/// baseline records each leaf, and the rename is followed for the root's
/// readers as for a plain table's.
#[tokio::test]
async fn a_renamed_type_of_a_partitioned_source_is_followed_in_place() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create type public.tone as enum ('calm', 'loud', 'brisk'); \
         create table public.items (g int not null, id int not null, tone tone, \
             primary key (g, id)) partition by range (g); \
         create table public.items_p0 partition of public.items for values from (0) to (8); \
         create table public.items_p1 partition of public.items for values from (8) to (16); \
         create index on public.items (g, tone);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    let text = "TRANSFORM per_tone FROM public.items GROUP BY g, tone \
               WITHOUT SOURCE INDEX SELECT g AS g, tone AS tone, COUNT(*) AS n";
    trellis.apply(text).await.expect(text);
    bring_live(&mut raw, &db.pool, &["per_tone"]).await;
    raw.batch_execute("insert into public.items values (1, 1, 'calm'), (9, 2, 'loud')")
        .await
        .expect("write");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    raw.batch_execute(
        "alter type public.tone rename to tone2; \
         insert into public.items values (1, 3, 'brisk'), (9, 4, 'calm'); \
         update public.items set tone = 'brisk' where id = 1;",
    )
    .await
    .expect("rename and write");
    drain_past_failures(&db.pool, &mut raw).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "per_tone").await, TransformStatus::Live);
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(held_keys(&raw).await, 0, "no key is held");
    assert_eq!(
        rows(
            &raw,
            "select g::text, tone::text, n::text from public.per_tone where n is not null \
             order by g, tone::text"
        )
        .await,
        rows(
            &raw,
            "select g::text, tone::text, count(*)::text from public.items group by g, tone \
             order by g, tone::text"
        )
        .await,
    );
}

/// A definition paused by an operator when the type is renamed: the pass
/// re-records its names too, and `RESUME`, before or after that pass,
/// re-validates it against the new name.
#[tokio::test]
async fn a_paused_reader_resumes_after_a_rename() {
    for resume_first in [false, true] {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut raw = connect(db.dsn()).await;
        raw.batch_execute(
            "create type public.tone as enum ('calm', 'loud'); \
             create table public.items (id int primary key, tone tone); \
             create table public.kinds (tone tone primary key, v int);",
        )
        .await
        .expect("seed");
        let trellis = definer(db.dsn()).await;
        for text in [
            "TRANSFORM tone_pass FROM public.items SELECT tone AS tone",
            "TRANSFORM kind_v FROM public.kinds SELECT v AS v",
        ] {
            trellis.apply(text).await.expect(text);
        }
        bring_live(&mut raw, &db.pool, &["tone_pass", "kind_v"]).await;
        raw.batch_execute(
            "insert into public.items values (1, 'calm'), (2, 'loud'); \
             insert into public.kinds values ('calm', 1)",
        )
        .await
        .expect("write");
        full_pass(&mut raw, &db.pool).await;
        drain_to_quiescence(&db.pool, &mut raw).await;

        for text in ["PAUSE TRANSFORM tone_pass", "PAUSE TRANSFORM kind_v"] {
            trellis.apply(text).await.expect(text);
        }
        raw.batch_execute(
            "alter type public.tone rename to tone2; \
             insert into public.items values (3, 'loud'); \
             update public.items set tone = 'loud' where id = 1; \
             insert into public.kinds values ('loud', 2); update public.kinds set v = 3;",
        )
        .await
        .expect("rename and write");
        if resume_first {
            for text in ["RESUME TRANSFORM tone_pass", "RESUME TRANSFORM kind_v"] {
                trellis.apply(text).await.expect(text);
            }
            capture_pass(&mut raw, &db.pool).await;
        } else {
            capture_pass(&mut raw, &db.pool).await;
            for text in ["RESUME TRANSFORM tone_pass", "RESUME TRANSFORM kind_v"] {
                trellis.apply(text).await.expect(text);
            }
        }
        bring_live(&mut raw, &db.pool, &["tone_pass", "kind_v"]).await;
        assert_eq!(held_keys(&raw).await, 0, "no key is held");
        assert_eq!(
            rows(
                &raw,
                "select id::text, tone::text from public.tone_pass order by id"
            )
            .await,
            rows(
                &raw,
                "select id::text, tone::text from public.items order by id"
            )
            .await,
            "resume first: {resume_first}"
        );
        assert_eq!(
            rows(
                &raw,
                "select tone::text, v::text from public.kind_v order by tone::text"
            )
            .await,
            rows(
                &raw,
                "select tone::text, v::text from public.kinds order by tone::text"
            )
            .await,
            "resume first: {resume_first}"
        );
    }
}

/// What each definition recorded for the enum, by OID: `definition_types`
/// and `definition_key_types`, as `(target, oid, name)` rows.
async fn recorded_oids(raw: &Client) -> Vec<Vec<Option<String>>> {
    rows(
        raw,
        "select split_part(d.target_table, '.', 2), r.type_oid::text, r.type_name \
         from transform_definitions d \
         join (select transform_id, type_oid, type_name from definition_types \
               union all select transform_id, type_oid, type_name from definition_key_types \
               where type_name not like 'pg_catalog.%') r on r.transform_id = d.id \
         order by 1, 2, 3",
    )
    .await
}

async fn type_oid(raw: &Client, name: &str) -> String {
    raw.query_one(&format!("select '{name}'::regtype::oid::text"), &[])
        .await
        .expect("read a type's oid")
        .get(0)
}

async fn capture_failures(raw: &Client) -> Vec<Vec<Option<String>>> {
    rows(
        raw,
        "select d.target_table, f.error from capture_failures f \
         join transform_definitions d on d.id = f.transform_id order by 1",
    )
    .await
}

/// Each target against its oracle, for a 1-1 passthrough `tone_pass` of
/// `items` and a `GROUP BY tone` count `per_tone`.
async fn assert_pass_and_counts(raw: &Client) {
    for target in ["tone_pass", "per_tone"] {
        assert_eq!(status(raw, target).await, TransformStatus::Live, "{target}");
    }
    assert_eq!(held_keys(raw).await, 0, "no key is held");
    assert_eq!(
        rows(
            raw,
            "select id::text, tone::text from public.tone_pass order by id"
        )
        .await,
        rows(
            raw,
            "select id::text, tone::text from public.items order by id"
        )
        .await,
    );
    assert_eq!(
        rows(
            raw,
            "select tone::text, n::text from public.per_tone where n is not null \
             order by tone::text"
        )
        .await,
        rows(
            raw,
            "select tone::text, count(*)::text from public.items group by tone \
             order by tone::text"
        )
        .await,
    );
}

const PASS_AND_COUNTS: [&str; 2] = [
    "TRANSFORM tone_pass FROM public.items SELECT tone AS tone",
    "TRANSFORM per_tone FROM public.items GROUP BY tone WITHOUT SOURCE INDEX SELECT tone AS tone, COUNT(*) AS n",
];

/// #1242 window 1: the type is renamed between define and the pass that
/// installs the source's capture, so the baseline that pass records already
/// has the new name. The definitions' own record still has the type's OID
/// beside the old name: the first pass follows the rename, nothing pauses,
/// nothing is held, and the targets equal their oracles.
#[tokio::test]
async fn a_rename_before_the_first_pass_is_followed() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create type public.tone as enum ('calm', 'loud', 'brisk'); \
         create table public.items (id int primary key, tone tone, n int); \
         create index on public.items (tone); \
         insert into public.items values (1, 'calm', 1), (2, 'loud', 2), (3, 'calm', 3);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in PASS_AND_COUNTS {
        trellis.apply(text).await.expect(text);
    }
    let oid = type_oid(&raw, "public.tone").await;
    raw.batch_execute("alter type public.tone rename to tone2")
        .await
        .expect("rename before any pass");

    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        capture_failures(&raw).await,
        Vec::<Vec<Option<String>>>::new()
    );
    for row in recorded_oids(&raw).await {
        assert_eq!(row[1].as_deref(), Some(oid.as_str()), "{row:?}");
        assert_eq!(row[2].as_deref(), Some("public.tone2"), "{row:?}");
    }
    assert_eq!(
        source_column_token(&raw, "tone_pass", "tone").await,
        "enum:public.tone2"
    );

    bring_live(&mut raw, &db.pool, &["tone_pass", "per_tone"]).await;
    assert_pass_and_counts(&raw).await;
    raw.batch_execute(
        "insert into public.items values (4, 'brisk', 4); \
         update public.items set tone = 'brisk' where id = 1; \
         delete from public.items where id = 2;",
    )
    .await
    .expect("write");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_pass_and_counts(&raw).await;
    assert_eq!(
        capture_failures(&raw).await,
        Vec::<Vec<Option<String>>>::new()
    );
}

/// #1242 window 2: a chained reader over an upstream paused for a capture
/// failure. The paused upstream no longer counts toward its source's
/// captured columns, and no other captured table has the type, so no
/// capture baseline records it; the chained definition reads the upstream's
/// target, which has none. Its own record still has the type's OID: the
/// rename is followed, with no misleading re-render pause.
#[tokio::test]
async fn a_rename_is_followed_for_a_chained_reader_of_a_paused_upstream() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create type public.tone as enum ('calm', 'loud', 'brisk'); \
         create table public.items (id int primary key, tone tone, x int);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    let upstream = "TRANSFORM tone_pass FROM public.items SELECT tone AS tone, x AS x";
    trellis.apply(upstream).await.expect(upstream);
    bring_live(&mut raw, &db.pool, &["tone_pass"]).await;
    raw.batch_execute("create index on public.tone_pass (tone)")
        .await
        .expect("index the upstream's target");
    let chained = "TRANSFORM chained FROM public.tone_pass GROUP BY tone \
                  WITHOUT SOURCE INDEX SELECT tone AS tone, COUNT(*) AS n";
    trellis.apply(chained).await.expect(chained);
    bring_live(&mut raw, &db.pool, &["tone_pass", "chained"]).await;
    raw.batch_execute(
        "insert into public.items values (1, 'calm', 1), (2, 'loud', 2), (3, 'calm', 3)",
    )
    .await
    .expect("write");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    // The upstream pauses for a column it reads, dropped.
    raw.batch_execute("alter table public.items drop column x")
        .await
        .expect("drop a read column");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "tone_pass").await, TransformStatus::Paused);
    capture_pass(&mut raw, &db.pool).await;
    let oid = type_oid(&raw, "public.tone").await;
    let baselines: i64 = raw
        .query_one(
            &format!("select count(*) from capture_baselines where type_oid = {oid}"),
            &[],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(baselines, 0, "no capture baseline records the type");

    raw.batch_execute("alter type public.tone rename to tone2")
        .await
        .expect("rename");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "chained").await, TransformStatus::Live);
    let failures = capture_failures(&raw).await;
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(failures[0][0].as_deref(), Some("public.tone_pass"));
    for row in recorded_oids(&raw).await {
        assert_eq!(row[1].as_deref(), Some(oid.as_str()), "{row:?}");
        assert_eq!(row[2].as_deref(), Some("public.tone2"), "{row:?}");
    }
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "chained").await, TransformStatus::Live);
    assert_eq!(held_keys(&raw).await, 0, "no key is held");
    assert_eq!(
        rows(
            &raw,
            "select tone::text, n::text from public.chained where n is not null \
             order by tone::text"
        )
        .await,
        rows(
            &raw,
            "select tone::text, count(*)::text from public.tone_pass group by tone \
             order by tone::text"
        )
        .await,
    );
}

fn run(command: &mut Command) {
    let out = command.output().expect("run a client tool");
    assert!(
        out.status.success(),
        "{command:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// #1242, a logical restore: every OID is new. The pass pauses every
/// definition for the restore and compares none of their recorded OIDs, even
/// one that names another type here (a decoy, standing in for the original
/// database's OID taken by an unrelated type). `RESUME` records the new OIDs,
/// and a later rename is followed.
#[tokio::test]
async fn a_logical_restore_is_re_recorded_by_resume_and_a_later_rename_followed() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create type public.tone as enum ('calm', 'loud', 'brisk'); \
         create table public.items (id int primary key, tone tone, n int); \
         create index on public.items (tone);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in PASS_AND_COUNTS {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool, &["tone_pass", "per_tone"]).await;
    raw.batch_execute("insert into public.items values (1, 'calm', 1), (2, 'loud', 2)")
        .await
        .expect("write");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    let original_oid = type_oid(&raw, "public.tone").await;
    let dump = cluster.root().join("type_identity.dump");
    run(Command::new("pg_dump")
        .args(["-Fc", "-f"])
        .arg(&dump)
        .arg(db.dsn()));

    let restored = cluster.create_empty_database().await;
    run(Command::new("pg_restore")
        .args(["--no-owner", "-d", restored.dsn()])
        .arg(&dump));
    let mut raw = connect(restored.dsn()).await;
    let trellis = definer(restored.dsn()).await;
    let oid = type_oid(&raw, "public.tone").await;
    assert_ne!(oid, original_oid, "a restore gives the type a new OID");
    // The original's OID for `per_tone`'s record names an unrelated type here.
    raw.batch_execute(
        "create type public.decoy as enum ('x'); \
         update definition_types set type_oid = 'public.decoy'::regtype \
         where transform_id = (select id from transform_definitions \
                               where target_table = 'public.per_tone'); \
         update definition_key_types set type_oid = 'public.decoy'::regtype \
         where type_name = 'public.tone'",
    )
    .await
    .expect("a decoy at a recorded OID");
    let before = recorded_oids(&raw).await;

    capture_pass(&mut raw, &restored.pool).await;
    capture_pass(&mut raw, &restored.pool).await;
    for target in ["tone_pass", "per_tone"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Paused,
            "{target}"
        );
        let failure = trellis
            .status(target)
            .await
            .expect("status")
            .expect(target)
            .capture_failure
            .expect("a capture failure");
        assert!(
            failure.error.contains("restored from a logical dump"),
            "{target}: {}",
            failure.error
        );
    }
    assert_eq!(
        recorded_oids(&raw).await,
        before,
        "nothing compared a restored definition's OIDs"
    );
    let releases: i64 = raw
        .query_one("select count(*) from retype_releases", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(releases, 0, "nothing was re-recorded");

    for target in ["tone_pass", "per_tone"] {
        trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .unwrap_or_else(|e| panic!("{target}: {e}"));
    }
    let recorded = recorded_oids(&raw).await;
    assert!(!recorded.is_empty());
    for row in &recorded {
        assert_eq!(row[1].as_deref(), Some(oid.as_str()), "{row:?}");
        assert_eq!(row[2].as_deref(), Some("public.tone"), "{row:?}");
    }
    bring_live(&mut raw, &restored.pool, &["tone_pass", "per_tone"]).await;
    assert_pass_and_counts(&raw).await;

    raw.batch_execute(
        "alter type public.tone rename to tone2; \
         insert into public.items values (3, 'brisk', 3); \
         update public.items set tone = 'brisk' where id = 1;",
    )
    .await
    .expect("rename and write");
    full_pass(&mut raw, &restored.pool).await;
    assert_eq!(
        capture_failures(&raw).await,
        Vec::<Vec<Option<String>>>::new()
    );
    full_pass(&mut raw, &restored.pool).await;
    drain_to_quiescence(&restored.pool, &mut raw).await;
    assert_pass_and_counts(&raw).await;
    for row in recorded_oids(&raw).await {
        assert_eq!(row[2].as_deref(), Some("public.tone2"), "{row:?}");
    }
}

/// #1242, a simulated `pg_upgrade`: the cluster's identifier changes and
/// every OID and name stays. Nothing is re-recorded and nothing pauses.
#[tokio::test]
async fn a_simulated_pg_upgrade_re_records_nothing() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let _trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let oids = recorded_oids(&raw).await;
    let names = recorded_names(&raw).await;
    assert!(!oids.is_empty());
    raw.batch_execute("update capture_triggers set system_identifier = '1'")
        .await
        .expect("simulate pg_upgrade");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let releases: i64 = raw
        .query_one("select count(*) from retype_releases", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(releases, 0, "nothing was re-recorded");
    assert_eq!(recorded_oids(&raw).await, oids);
    assert_eq!(recorded_names(&raw).await, names);
    write(&raw, "tone", 1).await;
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_converged(&raw).await;
}

/// A rename while another transaction holds a reader's definition row past
/// the pass's `lock_timeout` (a long build chunk, an operator's open
/// transaction): the pass goes on without following it, pausing nothing, and
/// the next pass after the lock is released follows it.
#[tokio::test]
async fn a_rename_blocked_by_a_locked_definition_is_followed_by_a_later_pass() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create type public.tone as enum ('calm', 'loud', 'brisk'); \
         create table public.items (id int primary key, tone tone, n int); \
         create index on public.items (tone); \
         insert into public.items values (1, 'calm', 1), (2, 'loud', 2);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in PASS_AND_COUNTS {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool, &["tone_pass", "per_tone"]).await;

    let mut holder = connect(db.dsn()).await;
    let lock = holder.transaction().await.expect("begin");
    lock.execute(
        "select 1 from transform_definitions where target_table = 'public.per_tone' \
         for key share",
        &[],
    )
    .await
    .expect("hold the definition row");
    raw.batch_execute(
        "alter type public.tone rename to tone2; set lock_timeout = '200ms'; \
         insert into public.items values (3, 'brisk', 3);",
    )
    .await
    .expect("rename");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        capture_failures(&raw).await,
        Vec::<Vec<Option<String>>>::new()
    );
    assert_eq!(
        source_column_token(&raw, "tone_pass", "tone").await,
        "enum:public.tone",
        "not followed while the row is locked"
    );
    lock.rollback().await.expect("release");
    raw.batch_execute("reset lock_timeout")
        .await
        .expect("reset");

    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        source_column_token(&raw, "tone_pass", "tone").await,
        "enum:public.tone2"
    );
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_pass_and_counts(&raw).await;
}
