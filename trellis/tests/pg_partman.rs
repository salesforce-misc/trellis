//! pg_partman driving partition maintenance under real Trellis capture
//! (#959, X12-8 of epic #897).
//!
//! **Opt-in tier.** Every test here is `#[ignore]`d: the default `verify` and
//! CI never need pg_partman. Run them on a machine that has it built for the
//! `postgres` on `PATH` (PG 15 or newer; the native partitioning pg_partman 5
//! uses):
//!
//! ```text
//! # once per PG version: a PGXS build into that version's own tree (no
//! # Trellis code needs superuser; the test cluster is ours). `make clean`
//! # between versions, or the next install reuses objects built against the
//! # previous version's headers. With asdf's Postgres:
//! git clone --branch v5.2.4 https://github.com/pgpartman/pg_partman
//! for v in 15.19 16.15 17.11 18.6; do
//!   make -C pg_partman clean
//!   make -C pg_partman PG_CONFIG=$HOME/.asdf/installs/postgres/$v/bin/pg_config install
//! done
//!
//! ASDF_POSTGRES_VERSION=17.11 \
//!   cargo test -p trellis --test pg_partman -- --ignored --test-threads 3
//! ```
//!
//! `ASDF_POSTGRES_VERSION=15.19` (and so on) picks the server. Each case
//! drives one pg_partman mode through its documented maintenance (premake,
//! `run_maintenance`, `partition_data_*`, retention by detach and by drop,
//! writers through the parent and straight to a leaf), runs Trellis's
//! reconcile pass after every step, and compares both readers with a group-by
//! of the source. Nothing paused and no diff is the expected outcome (ADR-0017:
//! maintenance of every routine kind pauses nothing). A step that pg_partman
//! itself refuses is printed and carried on from, not failed.
//!
//! Passes are called directly in bounded rounds; only the background-worker
//! case waits on the clock, because the worker's schedule is the thing under
//! test.

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
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

/// Panics, naming the build, when the `postgres` on `PATH` has no pg_partman.
fn require_partman() {
    let out = std::process::Command::new("pg_config")
        .arg("--pkglibdir")
        .output()
        .expect("run pg_config");
    let dir = String::from_utf8(out.stdout).unwrap();
    assert!(
        std::path::Path::new(dir.trim())
            .join("pg_partman_bgw.so")
            .exists(),
        "pg_partman is not installed for this postgres ({}); see this file's module doc",
        dir.trim()
    );
}

/// One pg_partman mode: the parent, how it is partitioned and which key
/// values land where.
#[derive(Clone)]
struct Mode {
    name: &'static str,
    /// The partition key column(s), and the readers' group key (it holds every
    /// partition key column).
    group: &'static str,
    /// SQL run after `create_parent` and before any row exists.
    post_create: &'static str,
    /// `create table public.ev (...)`, partitioned by range or list on `key`.
    parent_ddl: &'static str,
    /// The `create_parent` call (arguments after the parent table).
    create_parent: &'static str,
    /// SQL for a key from the integer expression `{i}`.
    key_of: &'static str,
    /// `i` values that land in leaves `create_parent` made.
    near: (i64, i64),
    /// `i` values that land in the DEFAULT until the premake is raised.
    mid: (i64, i64),
    /// `i` values that stay in the DEFAULT whatever the premake.
    far: Option<(i64, i64)>,
    /// The premake the maintenance step raises to.
    bump: u32,
    /// `part_config.retention` values for the detach and the drop step.
    keep_detach: &'static str,
    keep_drop: &'static str,
    /// `partition_data_*` call arguments after the parent table.
    move_fn: &'static str,
}

fn modes() -> Vec<Mode> {
    let time = Mode {
        name: "time_daily",
        group: "k",
        post_create: "select 1",
        parent_ddl: "create table public.ev (id bigserial, k date not null, a int, b int, \
                     primary key (k, id)) partition by range (k)",
        create_parent: "'k', '1 day', p_premake := 3, \
                        p_start_partition := (current_date - 5)::text",
        key_of: "(current_date + ({i}))",
        near: (-4, 3),
        mid: (5, 7),
        far: Some((20, 22)),
        bump: 8,
        keep_detach: "4 days",
        keep_drop: "2 days",
        move_fn: "partition_data_time",
    };
    let nodefault = Mode {
        name: "time_no_default",
        create_parent: "'k', '1 day', p_premake := 3, p_default_table := false, \
                        p_start_partition := (current_date - 5)::text",
        far: None,
        ..time.clone()
    };
    let id_range = Mode {
        name: "id_range",
        group: "k",
        post_create: "select 1",
        parent_ddl: "create table public.ev (id bigserial, k bigint not null, a int, b int, \
                     primary key (k, id)) partition by range (k)",
        create_parent: "'k', '100', p_premake := 3, p_start_partition := '0'",
        key_of: "({i} * 10)",
        near: (0, 39),
        mid: (40, 69),
        far: Some((500, 520)),
        bump: 8,
        keep_detach: "300",
        keep_drop: "200",
        move_fn: "partition_data_id",
    };
    let id_list = Mode {
        name: "id_list",
        group: "k",
        post_create: "select 1",
        parent_ddl: "create table public.ev (id bigserial, k bigint not null, a int, b int, \
                     primary key (k, id)) partition by list (k)",
        create_parent: "'k', '1', p_type := 'list', p_premake := 4, p_start_partition := '0'",
        key_of: "({i})",
        near: (0, 4),
        mid: (5, 7),
        far: Some((100, 102)),
        bump: 9,
        keep_detach: "6",
        keep_drop: "4",
        move_fn: "partition_data_id",
    };
    let epoch = Mode {
        name: "epoch_seconds",
        group: "k",
        post_create: "select 1",
        parent_ddl: "create table public.ev (id bigserial, k bigint not null, a int, b int, \
                     primary key (k, id)) partition by range (k)",
        create_parent: "'k', '1 day', p_epoch := 'seconds', p_premake := 3, \
                        p_start_partition := (current_date - 5)::text",
        key_of: "extract(epoch from (current_date + ({i})))::bigint",
        near: (-4, 3),
        mid: (5, 7),
        far: Some((20, 22)),
        bump: 8,
        keep_detach: "4 days",
        keep_drop: "2 days",
        move_fn: "partition_data_time",
    };
    let sub = Mode {
        name: "time_then_id_subpartitions",
        group: "k, id",
        post_create: "select partman.create_sub_parent('public.ev', 'id', '100', \
                      p_declarative_check := 'yes', p_premake := 2, p_start_partition := '0')",
        // Rows are written by id, which the sequence hands out: the keys that
        // matter are the day's.
        ..time.clone()
    };
    vec![time, nodefault, id_range, id_list, epoch, sub]
}

impl Mode {
    fn key(&self, i: &str) -> String {
        self.key_of.replace("{i}", i)
    }

    /// An `insert` of `lo..=hi` through `table`.
    fn insert(&self, table: &str, (lo, hi): (i64, i64)) -> String {
        format!(
            "insert into {table} (k, a, b) select {}, i % 5, i from generate_series({lo}, {hi}) i",
            self.key("i")
        )
    }
}

/// One whole staging-worker pass: capture, markers, discharge.
async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(raw, pool, SCHEMA, "pg_partman_wake", Duration::from_secs(5))
        .await
        .expect("reconcile pass");
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    drain(pool, client, 16, true).await;
}

/// Up to `rounds` seal/drain rounds; with `strict`, panics unless the ring
/// ends quiet (a writer running alongside never lets it).
async fn drain(pool: &trellis::Pool, client: &mut Client, rounds: usize, strict: bool) {
    use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
    let watermark = StagedWatermark::saturated();
    for _ in 0..rounds {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "pg_partman_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "pg_partman_test",
            1,
            "trellis_capture_missed_ddl_test",
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
    assert!(
        !strict,
        "the ring did not reach quiescence within {rounds} seal/drain rounds"
    );
}

async fn run_backfill_chunks(pool: &trellis::Pool) {
    use trellis::defs::chunk_queue;
    const CLAIMED_BY: &str = "pg_partman_backfill";
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

async fn rows(raw: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

fn texts(group: &str) -> String {
    group
        .split(", ")
        .map(|c| format!("{c}::text"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Both readers against a group-by of the source: empty when they agree.
async fn oracle_diff(raw: &Client, group: &str) -> String {
    let mut out = String::new();
    for (name, target, source) in [
        (
            "s",
            format!(
                "select {}, total::text from public.s order by 1",
                texts(group)
            ),
            format!(
                "select {}, sum(b)::text from public.ev group by {group} order by 1",
                texts(group)
            ),
        ),
        (
            "r",
            format!(
                "select {}, a::text, n::text from public.r order by 1, 2, 3",
                texts(group)
            ),
            format!(
                "select {}, a::text, count(*)::text from public.ev group by {group}, a order by 1, 2, 3",
                texts(group)
            ),
        ),
    ] {
        let (mut t, mut s) = (rows(raw, &target).await, rows(raw, &source).await);
        t.sort();
        s.sort();
        if t != s {
            let missing: Vec<_> = s.iter().filter(|r| !t.contains(r)).collect();
            let extra: Vec<_> = t.iter().filter(|r| !s.contains(r)).collect();
            let _ = write!(
                out,
                "reader {name}: {} source groups, {} target; source-only {missing:?}, target-only {extra:?}; ",
                s.len(),
                t.len()
            );
        }
    }
    out
}

struct Harness {
    _cluster: TestCluster,
    db: testkit::TestDatabase,
    raw: Client,
    app: Client,
    trellis: Trellis,
    mode: Mode,
    failures: Vec<String>,
}

impl Harness {
    /// A cluster (with the worker preloaded when `bgw`), the extension, the
    /// partitioned source, two live readers over it.
    async fn start(mode: Mode, bgw: bool) -> Self {
        require_partman();
        trellis::defs::partition_tree::open_gate_for_tests();
        let cluster = if bgw {
            TestCluster::start_with_settings(&[
                "shared_preload_libraries=pg_partman_bgw",
                "pg_partman_bgw.interval=1",
                "pg_partman_bgw.role=postgres",
                "pg_partman_bgw.analyze=off",
                "pg_partman_bgw.jobmon=off",
            ])
        } else {
            TestCluster::start()
        };
        let db = cluster.create_isolated_database().await;
        let raw = connect(db.dsn()).await;
        let app = connect(db.dsn()).await;
        let trellis = definer(db.dsn()).await;
        raw.batch_execute("create schema partman; create extension pg_partman schema partman")
            .await
            .expect("create extension pg_partman");
        raw.batch_execute(mode.parent_ddl).await.expect("parent");
        raw.batch_execute(&format!(
            "select partman.create_parent('public.ev', {})",
            mode.create_parent
        ))
        .await
        .expect("create_parent");
        raw.batch_execute(mode.post_create)
            .await
            .expect("post_create");
        // Rows in the premade leaves before the readers exist: their build
        // reads them.
        raw.batch_execute(&mode.insert("public.ev", mode.near))
            .await
            .expect("seed");
        let aliased = mode
            .group
            .split(", ")
            .map(|c| format!("{c} AS {c}"))
            .collect::<Vec<_>>()
            .join(", ");
        for text in [
            format!(
                "TRANSFORM r FROM public.ev GROUP BY {g}, a SELECT {aliased}, a AS a, COUNT(*) AS n",
                g = mode.group
            ),
            format!(
                "TRANSFORM s FROM public.ev GROUP BY {g} SELECT {aliased}, SUM(b) AS total",
                g = mode.group
            ),
        ] {
            trellis.apply(&text).await.expect(&text);
        }
        let mut h = Self {
            _cluster: cluster,
            db,
            raw,
            app,
            trellis,
            mode,
            failures: Vec::new(),
        };
        for _ in 0..10 {
            full_pass(&mut h.raw, &h.db.pool).await;
            run_backfill_chunks(&h.db.pool).await;
            drain_to_quiescence(&h.db.pool, &mut h.raw).await;
            if h.statuses()
                .await
                .iter()
                .all(|s| *s == TransformStatus::Live)
            {
                break;
            }
        }
        h
    }

    async fn statuses(&self) -> Vec<TransformStatus> {
        let mut out = Vec::new();
        for r in ["r", "s"] {
            let st = self.trellis.status(r).await.unwrap().expect("reader");
            out.push(st.status);
        }
        out
    }

    /// Passes, builds and drains in bounded rounds until the readers match
    /// the source; the diff left after `rounds` rounds (empty when exact),
    /// and every status/capture failure seen.
    async fn settle(&mut self, rounds: usize) -> (usize, String) {
        let mut diff = String::new();
        for round in 1..=rounds {
            full_pass(&mut self.raw, &self.db.pool).await;
            run_backfill_chunks(&self.db.pool).await;
            drain_to_quiescence(&self.db.pool, &mut self.raw).await;
            diff = oracle_diff(&self.raw, self.mode.group).await;
            if diff.is_empty() {
                return (round, diff);
            }
        }
        (rounds, diff)
    }

    async fn paused(&self) -> Vec<String> {
        let mut out = Vec::new();
        for r in ["r", "s"] {
            let st = self.trellis.status(r).await.unwrap().expect("reader");
            if st.status != TransformStatus::Live || st.capture_failure.is_some() {
                out.push(format!(
                    "{r}: {:?} {:?}",
                    st.status,
                    st.capture_failure.map(|f| f.error)
                ));
            }
        }
        out
    }

    /// Runs `sql` on the app connection; a pg_partman refusal is noted.
    async fn run(&self, step: &str, sql: &str) -> bool {
        match self.app.batch_execute(sql).await {
            Ok(()) => true,
            Err(e) => {
                let why = e.as_db_error().map_or_else(
                    || e.to_string(),
                    |d| format!("{} ({})", d.message(), d.code().code()),
                );
                println!("[{}] {step}: refused: {why}", self.mode.name);
                false
            }
        }
    }

    /// Settles and checks one step; records what broke.
    async fn check(&mut self, step: &str) {
        let (rounds, diff) = self.settle(6).await;
        let paused = self.paused().await;
        let shape = rows(
            &self.raw,
            "select (select count(*) from pg_inherits where inhparent = 'public.ev'::regclass)::text, \
                    (select count(*) from public.ev)::text, \
                    (select count(*) from public.ev where tableoid::regclass::text like '%default')::text, \
                    (select count(*) from pg_partition_tree('public.ev'))::text",
        )
        .await;
        let shape = shape
            .first()
            .map(|r| r.iter().flatten().cloned().collect::<Vec<_>>());
        println!(
            "[{}] {step}: nodes/rows/default/tree {shape:?}, rounds {rounds}, diff {:?}, paused {paused:?}",
            self.mode.name,
            if diff.is_empty() { "none" } else { &diff }
        );
        if !diff.is_empty() {
            self.failures
                .push(format!("[{}] {step}: diverged: {diff}", self.mode.name));
        }
        if !paused.is_empty() {
            self.failures
                .push(format!("[{}] {step}: paused: {paused:?}", self.mode.name));
        }
    }

    /// The leaf holding `i`'s key, by name.
    async fn leaf_of(&self, i: i64) -> Option<String> {
        let key = self.mode.key(&i.to_string());
        let r = rows(
            &self.raw,
            &format!("select tableoid::regclass::text from public.ev where k = {key} limit 1"),
        )
        .await;
        r.first().and_then(|r| r[0].clone())
    }
}

/// A writer loop for `iters` operations; statements that fail (a leaf
/// removed, a lock timeout) are counted, not fatal.
async fn writer(dsn: String, ops: Vec<String>, iters: usize) -> usize {
    let c = connect(&dsn).await;
    let mut failed = 0;
    for n in 0..iters {
        if c.batch_execute(&ops[n % ops.len()]).await.is_err() {
            failed += 1;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    failed
}

async fn run_mode(mode: Mode) {
    let name = mode.name;
    let mut h = Harness::start(mode, false).await;
    h.check("live after create_parent + seed").await;

    // Writes through the parent and straight to a leaf.
    let near = h.mode.near;
    let leaf = h.leaf_of(near.0 + 1).await.expect("a leaf with rows");
    let key0 = h.mode.key(&(near.0 + 1).to_string());
    let sql = format!(
        "{}; {}; update public.ev set b = b + 100 where a = 2; \
         insert into {leaf} (k, a, b) values ({key0}, 9, 9); \
         update {leaf} set b = b + 1; delete from {leaf} where b % 2 = 0",
        h.mode.insert("public.ev", near),
        h.mode.insert("public.ev", (near.0, near.0 + 2)),
    );
    h.run("writes via parent and leaf", &sql).await;
    h.check("writes via parent and leaf").await;

    // Rows that no leaf holds go to the DEFAULT, or are refused without one.
    let mid = h.mode.mid;
    let mut default_sql = h.mode.insert("public.ev", mid);
    if let Some(far) = h.mode.far {
        default_sql = format!("{default_sql}; {}", h.mode.insert("public.ev", far));
    }
    let wrote_default = h.run("rows beyond the premake", &default_sql).await;
    h.check("rows beyond the premake").await;

    // Maintenance with the premake raised: new leaves, over a DEFAULT that
    // holds rows in their range.
    let bump = h.mode.bump;
    h.run(
        "raise the premake",
        &format!(
            "update partman.part_config set premake = {bump} where parent_table = 'public.ev'"
        ),
    )
    .await;
    let maintained = h
        .run(
            "run_maintenance",
            "select partman.run_maintenance('public.ev')",
        )
        .await;
    h.check("run_maintenance with rows in the DEFAULT").await;

    if wrote_default && h.mode.mid.0 <= h.mode.mid.1 {
        // The documented fix: move the DEFAULT's rows into the new leaves.
        let mv = h.mode.move_fn;
        let moved = h
            .run(
                "partition_data",
                &format!("select partman.{mv}('public.ev', p_batch_count := 20)"),
            )
            .await;
        h.check(&format!("{mv} moves DEFAULT rows (ok: {moved})"))
            .await;
        if !maintained {
            h.run(
                "run_maintenance again",
                "select partman.run_maintenance('public.ev')",
            )
            .await;
            h.check("run_maintenance after the move").await;
        }
    }

    // Writes straight to a leaf pg_partman created after the readers were
    // defined: only the pass's adoption of the leaf captures them.
    if wrote_default {
        match h.leaf_of(h.mode.mid.0).await {
            Some(new_leaf) if !new_leaf.ends_with("default") => {
                let key = h.mode.key(&h.mode.mid.0.to_string());
                h.run(
                    "writes straight to a leaf maintenance created",
                    &format!(
                        "insert into {new_leaf} (k, a, b) values ({key}, 3, 7); \
                         update {new_leaf} set b = b + 1 where a = 1; \
                         delete from {new_leaf} where a = 3"
                    ),
                )
                .await;
                h.check("writes straight to a leaf maintenance created")
                    .await;
            }
            other => h.failures.push(format!(
                "[{name}] no new leaf holds the moved rows: {other:?}"
            )),
        }
    }

    // Concurrent writers (through the parent, and straight to a leaf) while
    // maintenance creates leaves and the reconcile pass runs.
    let leaf = h.leaf_of(near.0 + 1).await.unwrap_or(leaf);
    let parent_ops = vec![
        h.mode.insert("public.ev", (near.0, near.1)),
        format!(
            "update public.ev set b = b + 1 where k = {}",
            h.mode.key(&format!(
                "{} + (random() * {})::int",
                near.0,
                near.1 - near.0
            ))
        ),
        format!(
            "delete from public.ev where k = {} and a = 4",
            h.mode.key(&format!(
                "{} + (random() * {})::int",
                near.0,
                near.1 - near.0
            ))
        ),
    ];
    let leaf_ops = vec![
        format!("insert into {leaf} (k, a, b) values ({key0}, 1, 7)"),
        format!("update {leaf} set b = b + 1 where a = 1"),
        format!("delete from {leaf} where b % 3 = 0"),
    ];
    let w1 = tokio::spawn(writer(h.db.dsn().to_string(), parent_ops, 150));
    let w2 = tokio::spawn(writer(h.db.dsn().to_string(), leaf_ops, 150));
    for extra in 1..=3 {
        h.run(
            "raise the premake while writers run",
            &format!(
                "update partman.part_config set premake = {} where parent_table = 'public.ev'",
                bump + extra
            ),
        )
        .await;
        h.run(
            "run_maintenance while writers run",
            "select partman.run_maintenance('public.ev')",
        )
        .await;
        full_pass(&mut h.raw, &h.db.pool).await;
    }
    let (f1, f2) = (w1.await.unwrap(), w2.await.unwrap());
    println!("[{name}] concurrent writers: {f1} and {f2} statements refused");
    h.check("concurrent writers during maintenance").await;

    // Retention: detach (keep the table), then drop. Rows written to a
    // detached table afterwards are not the source's.
    h.run(
        "retention keeps the table",
        &format!(
            "update partman.part_config set retention = '{}', retention_keep_table = true \
             where parent_table = 'public.ev'",
            h.mode.keep_detach
        ),
    )
    .await;
    h.run(
        "run_maintenance (detach)",
        "select partman.run_maintenance('public.ev')",
    )
    .await;
    // Writes to a detached leaf are not the source's: before the next pass
    // (the capture functions' own guard) and after it.
    let detached = rows(
        &h.raw,
        "select c.oid::regclass::text from pg_class c \
         where c.relkind in ('r', 'p') and c.relispartition = false and c.relname like 'ev\\_p%' \
         order by 1 limit 1",
    )
    .await;
    match detached.first().and_then(|r| r[0].clone()) {
        Some(d) => {
            let write = format!("update {d} set b = b + 5; delete from {d} where a = 0");
            h.run("write to a detached table before a pass", &write)
                .await;
            h.check("retention detaches the oldest leaves, one written before a pass")
                .await;
            h.run("write to a detached table after a pass", &write)
                .await;
            h.check("writes to a detached leaf after a pass").await;
        }
        None => h
            .failures
            .push(format!("[{name}] retention detached no leaf")),
    }
    h.run(
        "retention drops",
        &format!(
            "update partman.part_config set retention = '{}', retention_keep_table = false \
             where parent_table = 'public.ev'",
            h.mode.keep_drop
        ),
    )
    .await;
    h.run(
        "run_maintenance (drop)",
        "select partman.run_maintenance('public.ev')",
    )
    .await;
    h.check("retention drops leaves").await;

    // Maintenance in the same transaction as writes through the parent.
    let in_tx = format!(
        "begin; {}; select partman.run_maintenance('public.ev'); {}; commit",
        h.mode.insert("public.ev", near),
        h.mode.insert("public.ev", near),
    );
    h.run("writes and maintenance in one transaction", &in_tx)
        .await;
    h.check("writes and maintenance in one transaction").await;

    assert!(h.failures.is_empty(), "{:#?}", h.failures);
}

#[tokio::test]
#[ignore = "needs pg_partman; see the module doc"]
async fn partman_time_daily() {
    run_mode(modes().remove(0)).await;
}

#[tokio::test]
#[ignore = "needs pg_partman; see the module doc"]
async fn partman_time_without_a_default() {
    run_mode(modes().remove(1)).await;
}

#[tokio::test]
#[ignore = "needs pg_partman; see the module doc"]
async fn partman_id_range() {
    run_mode(modes().remove(2)).await;
}

#[tokio::test]
#[ignore = "needs pg_partman; see the module doc"]
async fn partman_id_list() {
    run_mode(modes().remove(3)).await;
}

#[tokio::test]
#[ignore = "needs pg_partman; see the module doc"]
async fn partman_epoch_seconds() {
    run_mode(modes().remove(4)).await;
}

#[tokio::test]
#[ignore = "needs pg_partman; see the module doc"]
async fn partman_time_then_id_subpartitions() {
    run_mode(modes().remove(5)).await;
}

/// The worker's cluster: preloaded, pointed at the case's database.
async fn start_with_worker(mode: Mode) -> Harness {
    let h = Harness::start(mode, true).await;
    let name = h.db.name().to_string();
    h.raw
        .batch_execute(&format!(
            "alter system set pg_partman_bgw.dbname = '{name}'"
        ))
        .await
        .expect("point the worker at the database");
    h.raw
        .batch_execute("select pg_reload_conf()")
        .await
        .expect("reload");
    h
}

async fn leaf_count(raw: &Client) -> String {
    rows(
        raw,
        "select count(*)::text from pg_inherits where inhparent = 'public.ev'::regclass",
    )
    .await[0][0]
        .clone()
        .unwrap()
}

/// What the worker did, from the server log: runs started and runs that
/// failed.
fn worker_log(h: &Harness) -> (usize, usize) {
    let log = std::fs::read_to_string(h._cluster.root().join("postgres.log")).unwrap_or_default();
    (
        log.matches("dynamic background worker initialized").count(),
        log.matches("exited with exit code 1").count(),
    )
}

/// `pg_partman_bgw` runs `run_maintenance_proc` on its own schedule (1 s
/// here) while Trellis's pass runs and writers insert through the parent and
/// straight to a leaf: once the worker has changed the leaves (its premake
/// and retention), the readers match the source with no operator step, and
/// again after further runs.
#[tokio::test]
#[ignore = "needs pg_partman; see the module doc"]
async fn partman_background_worker() {
    let mut h = start_with_worker(modes().remove(0)).await;
    h.check("live").await;
    let near = h.mode.near;
    let leaves_before = leaf_count(&h.raw).await;
    // Work for the worker: a higher premake, and a retention that drops.
    h.run(
        "configure the worker's work",
        "update partman.part_config set premake = 8, retention = '3 days', \
         retention_keep_table = false where parent_table = 'public.ev'",
    )
    .await;
    let leaf = h.leaf_of(near.0 + 2).await.expect("a leaf");
    let key = h.mode.key(&(near.0 + 2).to_string());
    let parent_ops = vec![
        h.mode.insert("public.ev", (near.0 + 2, near.1)),
        "update public.ev set b = b + 1 where a = 1".to_string(),
        "delete from public.ev where a = 4 and b % 2 = 0".to_string(),
    ];
    let leaf_ops = vec![
        format!("insert into {leaf} (k, a, b) values ({key}, 1, 7)"),
        format!("update {leaf} set b = b + 1 where a = 1"),
    ];
    let w1 = tokio::spawn(writer(h.db.dsn().to_string(), parent_ops, 300));
    let w2 = tokio::spawn(writer(h.db.dsn().to_string(), leaf_ops, 300));
    let start = Instant::now();
    let mut worker_done = None;
    while start.elapsed() < Duration::from_secs(60) {
        full_pass(&mut h.raw, &h.db.pool).await;
        run_backfill_chunks(&h.db.pool).await;
        drain(&h.db.pool, &mut h.raw, 1, false).await;
        if worker_done.is_none() && leaf_count(&h.raw).await != leaves_before {
            worker_done = Some(start.elapsed());
        }
        if worker_done.is_some() && w1.is_finished() && w2.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let (f1, f2) = (w1.await.unwrap(), w2.await.unwrap());
    let (runs, failed) = worker_log(&h);
    println!(
        "[bgw] leaves {leaves_before} -> {}; worker changed them after {worker_done:?}; \
         {runs} runs, {failed} failed; writers refused {f1} and {f2}",
        leaf_count(&h.raw).await
    );
    assert!(worker_done.is_some(), "the worker never changed the leaves");
    h.check("after the worker ran, no operator step").await;
    let more = leaf_count(&h.raw).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    h.check("after further worker runs").await;
    println!("[bgw] leaves {more} -> {}", leaf_count(&h.raw).await);
    assert!(h.failures.is_empty(), "{:#?}", h.failures);
}

/// The worker beside rows in the DEFAULT that its new leaves would cover:
/// every run fails on `ATTACH` (pg_partman's own refusal, with no operator
/// step that clears it), and Trellis stays exact meanwhile.
#[tokio::test]
#[ignore = "needs pg_partman; see the module doc"]
async fn partman_background_worker_blocked_by_rows_in_the_default() {
    let mut h = start_with_worker(modes().remove(0)).await;
    h.check("live").await;
    let leaves_before = leaf_count(&h.raw).await;
    let mid = h.mode.mid;
    h.run("rows beyond the premake", &h.mode.insert("public.ev", mid))
        .await;
    h.run(
        "raise the premake",
        "update partman.part_config set premake = 8 where parent_table = 'public.ev'",
    )
    .await;
    // Passes until the worker has failed on the DEFAULT's rows three times,
    // against a deadline rather than a fixed number of ticks, so a loaded
    // box only waits longer.
    let refusals = |h: &Harness| {
        std::fs::read_to_string(h._cluster.root().join("postgres.log"))
            .unwrap_or_default()
            .matches("for default partition \"ev_default\" would be violated")
            .count()
    };
    let start = Instant::now();
    let mut tick = 0;
    while refusals(&h) < 3 && start.elapsed() < Duration::from_secs(60) {
        full_pass(&mut h.raw, &h.db.pool).await;
        run_backfill_chunks(&h.db.pool).await;
        drain_to_quiescence(&h.db.pool, &mut h.raw).await;
        println!(
            "[bgw-blocked] tick {tick}: {:?}",
            oracle_diff(&h.raw, h.mode.group).await
        );
        tick += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let (runs, failed) = worker_log(&h);
    println!(
        "[bgw-blocked] leaves {leaves_before} -> {}; {runs} runs, {failed} failed, \
         {} refused on the DEFAULT's rows",
        leaf_count(&h.raw).await,
        refusals(&h)
    );
    h.check("beside a worker that cannot attach").await;
    assert!(
        refusals(&h) >= 3,
        "the worker did not fail on the DEFAULT's rows three times in 60 s"
    );
    assert_eq!(leaf_count(&h.raw).await, leaves_before);
    assert!(h.failures.is_empty(), "{:#?}", h.failures);
}

/// X12-8d: does pg_partman's DEFAULT hit the accepted "DEFAULT plus
/// short-lived leaf" gap? Retention drops leaves that lived through many
/// passes, so the realistic case is a leaf the pass recorded. The case closest
/// to the gap that pg_partman builds is a brand-new leaf (premake raised),
/// written through the parent, applied by the staging worker and dropped
/// before any pass saw it. It converges because the same `run_maintenance`
/// made the leaf's neighbours too, and they stay: the next pass sees a tree
/// that changed. The gap needs the tree to look the same at both passes (one
/// leaf created and removed in between, by hand), which pg_partman's
/// maintenance does not do.
#[tokio::test]
#[ignore = "needs pg_partman; see the module doc"]
async fn partman_default_beside_short_lived_and_retired_leaves() {
    let mut h = Harness::start(modes().remove(0), false).await;
    h.check("live").await;

    // Realistic: the oldest leaf lived through the passes above, takes a
    // write, and retention drops it after a pass has seen the write.
    let old = h.mode.near.0;
    h.run(
        "a write to the oldest leaf",
        &h.mode.insert("public.ev", (old, old)),
    )
    .await;
    h.check("a write to the oldest leaf, then a pass").await;
    h.run(
        "retention drops",
        "update partman.part_config set retention = '3 days', retention_keep_table = false \
         where parent_table = 'public.ev'",
    )
    .await;
    h.run(
        "run_maintenance",
        "select partman.run_maintenance('public.ev')",
    )
    .await;
    h.check("a leaf that lived through a pass is dropped").await;

    // Worst case: a new leaf no pass has seen.
    h.run(
        "raise the premake",
        "update partman.part_config set premake = 12 where parent_table = 'public.ev'",
    )
    .await;
    h.run(
        "run_maintenance",
        "select partman.run_maintenance('public.ev')",
    )
    .await;
    let key9 = h.mode.key("9");
    let leaf = rows(
        &h.raw,
        &format!(
            "with w as (insert into public.ev (k, a, b) values ({key9}, 1, 1) \
             returning tableoid::regclass::text t) select t from w"
        ),
    )
    .await;
    let leaf = leaf[0][0].clone().expect("a leaf");
    // The staging worker applies the write (it drains on its own clock, the
    // reconcile pass on another) before the leaf goes.
    drain_to_quiescence(&h.db.pool, &mut h.raw).await;
    println!(
        "[gap] a brand-new leaf {leaf} took a write; before the drop: {:?}",
        oracle_diff(&h.raw, h.mode.group).await
    );
    h.run(
        "detach",
        &format!("alter table public.ev detach partition {leaf}"),
    )
    .await;
    h.run("drop", &format!("drop table {leaf}")).await;
    h.check("a brand-new leaf, written and applied, is dropped before any pass")
        .await;
    assert!(h.failures.is_empty(), "{:#?}", h.failures);
}

/// `undo_partition` empties the set into a plain table: each leaf's rows move
/// out and the leaf is detached, one batch at a time, with a writer on.
#[tokio::test]
#[ignore = "needs pg_partman; see the module doc"]
async fn partman_undo_partition() {
    let mut h = Harness::start(modes().remove(0), false).await;
    h.check("live").await;
    h.run(
        "a flat target",
        "create table public.ev_flat (like public.ev including defaults)",
    )
    .await;
    let near = h.mode.near;
    let ops = vec![
        h.mode.insert("public.ev", near),
        "update public.ev set b = b + 1 where a = 1".to_string(),
    ];
    let w = tokio::spawn(writer(h.db.dsn().to_string(), ops, 100));
    for batch in 0..4 {
        h.run(
            "undo one batch",
            "call partman.undo_partition_proc('public.ev', p_target_table := 'public.ev_flat', \
             p_loop_count := 2, p_wait := 0)",
        )
        .await;
        h.check(&format!("undo_partition batch {batch}")).await;
    }
    println!("[undo] writer refused {}", w.await.unwrap());
    h.check("after undo_partition").await;
    assert!(h.failures.is_empty(), "{:#?}", h.failures);
}
