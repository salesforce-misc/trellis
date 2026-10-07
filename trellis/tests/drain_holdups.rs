//! Drain holdups (#817): a page the drain keeps failing on while charging no
//! key and pausing no definition is recorded in `drain_holdups`, reported by
//! `status` on every unfrozen definition reading a table on the page and by
//! `self_check` for the instance, and cleared by the transaction that
//! commits the page. A page that fails at COMMIT, on a deferred constraint an
//! application put on a target (#856), is classified like any other: retried,
//! charged to its key, or held up.
//!
//! Trellis runs as a non-superuser login role, `holdup_trellis`, owning the
//! application's tables, and the drain as a second one, `holdup_worker`,
//! granted what it uses, so a column grant can be taken away from the drain
//! alone (capture's functions run as the tables' owner and keep reading).
//! Nothing polls for convergence (#297): every seal, capture pass and drain
//! is stepped by hand, one drain call at a time.

use std::time::{Duration, Instant, SystemTime};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::defs::TransformStatus;
use trellis::intake::markers;
use trellis::staging::{StagedWatermark, apply, quarantine, seal};

const SCHEMA: &str = trellis::config::DEFAULT_SCHEMA;

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

struct Instance {
    db: testkit::TestDatabase,
    admin: Client,
    raw: Client,
    pool: trellis::Pool,
    trellis: trellis::Trellis,
}

/// `public.p` (3 rows), `public.c` (6 rows, `pid` naming a `p`) and
/// `public.q` (6 rows), owned by `holdup_trellis`, which runs Trellis;
/// `c.pid` related to `p.id` as `parent`, and `c.id` to `q.id` as `twin`;
/// and `ddl`'s definitions, live, with nothing left to drain.
async fn instance(cluster: &TestCluster, ddl: &[&str]) -> Instance {
    let db = cluster.create_empty_database().await;
    let admin = connect(db.dsn()).await;
    admin
        .batch_execute(&format!(
            "create role holdup_trellis login; \
             grant create on database \"{}\" to holdup_trellis; \
             grant create on schema public to holdup_trellis; \
             create table public.p (id int primary key, name text); \
             create table public.c (id int primary key, pid int, amount int); \
             insert into public.p select i, 'n' || i from generate_series(1, 3) i; \
             insert into public.c select i, 1 + i % 3, i from generate_series(1, 6) i; \
             create table public.q (id int primary key, v int); \
             insert into public.q select i, i from generate_series(1, 6) i; \
             alter table public.p owner to holdup_trellis; \
             alter table public.c owner to holdup_trellis; \
             alter table public.q owner to holdup_trellis;",
            db.name()
        ))
        .await
        .expect("a login role owning the application's tables");
    let dsn = db.dsn().replace("user=postgres", "user=holdup_trellis");
    let config = trellis::Config::with_schema(dsn.clone(), SCHEMA).expect("valid config");
    let pool = trellis::Pool::new(&config).expect("pool");
    trellis::migrate(&pool, &config).await.expect("migrate");
    let mut raw = connect(&dsn).await;
    raw.batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    let trellis = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("connect");
    for relationship in [
        "RELATIONSHIP parent FROM c.pid TO p.id",
        "RELATIONSHIP twin FROM c.id TO q.id",
    ] {
        trellis.apply(relationship).await.expect(relationship);
    }
    for statement in ddl {
        trellis.apply(statement).await.expect(statement);
    }
    capture_pass(&mut raw, &pool).await;
    markers::settle_registrations(&pool).await;
    // Drain what going live staged, so each test's pages hold its own
    // changes alone.
    let seg = seal(&mut raw).await;
    drain(&pool, seg)
        .await
        .expect("drain what going live staged");
    let it = Instance {
        db,
        admin,
        raw,
        pool,
        trellis,
    };
    for target in targets(ddl) {
        assert_eq!(status(&it, target).await.status, TransformStatus::Live);
    }
    it
}

/// The bare targets `ddl` defines.
fn targets<'a>(ddl: &[&'a str]) -> Vec<&'a str> {
    ddl.iter()
        .map(|statement| statement.split_whitespace().nth(1).expect("a target"))
        .collect()
}

/// The capture half of one staging-worker pass.
async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    let outcome = trellis::capture::reconcile::reconcile(
        raw,
        SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

/// A drain role, `holdup_worker`, holding every privilege the drain uses on
/// Trellis's schema and the application's tables, but on `public.c` only
/// `SELECT`, column by column. Returns a pool logged in as it, and one whose
/// sessions also run with `options`.
async fn worker(it: &Instance, options: &str) -> trellis::Pool {
    it.admin
        .batch_execute(&format!(
            "do $$ begin \
               if not exists (select from pg_roles where rolname = 'holdup_worker') then \
                 create role holdup_worker login; \
               end if; \
             end $$; \
             grant usage on schema {SCHEMA}, public to holdup_worker; \
             grant all on all tables in schema {SCHEMA} to holdup_worker; \
             grant all on all sequences in schema {SCHEMA} to holdup_worker; \
             grant all on all tables in schema public to holdup_worker; \
             revoke all on public.c from holdup_worker; \
             grant select (id, pid, amount) on public.c to holdup_worker;"
        ))
        .await
        .expect("a drain role");
    let mut dsn = it.db.dsn().replace("user=postgres", "user=holdup_worker");
    if !options.is_empty() {
        dsn = format!("{dsn} options='{options}'");
    }
    let config = trellis::Config::with_schema(dsn, SCHEMA).expect("valid config");
    trellis::Pool::new(&config).expect("pool")
}

/// Seals the ring, both phases, and returns the sealed segment.
async fn seal(raw: &mut Client) -> i64 {
    let sealed = seal::seal_phase1(raw).await.expect("seal phase 1");
    seal::seal_phase2(raw, sealed.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    sealed.sealed_seg_seq
}

/// One drain call over `seg_seq` as `pool`'s role: the drain's error, if it
/// surfaced one.
async fn drain(pool: &trellis::Pool, seg_seq: i64) -> Result<(), String> {
    apply::drain_once(
        pool,
        seg_seq,
        "holdup_test",
        1,
        "trellis_holdup_test",
        &StagedWatermark::saturated(),
    )
    .await
    .map(|_| ())
    .map_err(|err| err.to_string())
}

async fn status(it: &Instance, target: &str) -> trellis::DefinitionStatus {
    it.trellis
        .status(target)
        .await
        .expect("status")
        .unwrap_or_else(|| panic!("{target} is registered"))
}

/// One `drain_holdups` row, as the superuser reads it.
#[derive(Debug)]
struct Row {
    seg_seq: i64,
    buckets: Vec<i16>,
    tables: Vec<String>,
    sqlstate: Option<String>,
    since: SystemTime,
    attempts: i32,
}

async fn holdups(admin: &Client) -> Vec<Row> {
    admin
        .query(
            &format!(
                "select seg_seq, buckets, tables, sqlstate, since, attempts \
                 from {SCHEMA}.drain_holdups order by seg_seq"
            ),
            &[],
        )
        .await
        .expect("read the holdups")
        .iter()
        .map(|row| Row {
            seg_seq: row.get(0),
            buckets: row.get(1),
            tables: row.get(2),
            sqlstate: row.get(3),
            since: row.get(4),
            attempts: row.get(5),
        })
        .collect()
}

/// Every key the quarantine has charged or holds.
async fn charged(admin: &Client) -> i64 {
    admin
        .query_one(
            &format!(
                "select (select count(*) from {SCHEMA}.poison) \
                      + (select count(*) from {SCHEMA}.key_deaths)"
            ),
            &[],
        )
        .await
        .expect("count charged keys")
        .get(0)
}

const READERS: [&str; 4] = [
    // Reads `c`, and `p` through `parent`.
    "TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name",
    // Reads `p`.
    "TRANSFORM p_copy FROM public.p SELECT name AS name",
    // Reads `c` alone: `twin` is declared on `c`, but it doesn't read through it.
    "TRANSFORM c_copy FROM public.c SELECT amount AS amount",
    // Reads `q` alone.
    "TRANSFORM q_copy FROM public.q SELECT v AS v",
];

/// Takes `SELECT` on `c.pid` from the drain's role, keeping it on `c`'s
/// other columns, and renames `p` row `id`. `c_named` reads that name, so
/// its page holds the change to `p` and a recompute of each `c` row naming
/// it, whose re-read of `c` Postgres refuses. The catalog can't pin the
/// refusal on `c`: a column grant counts as holding the privilege.
async fn refuse_a_column_and_rename(it: &Instance, id: i32) {
    it.admin
        .batch_execute("revoke select (pid) on public.c from holdup_worker")
        .await
        .expect("take a column grant away");
    it.admin
        .execute(
            "update public.p set name = 'renamed' || id where id = $1",
            &[&id],
        )
        .await
        .expect("write the to-side");
}

/// The issue's first two cases. A refused read the catalog can't pin on a
/// table pauses nothing, so the drain records a holdup. `status` reports it
/// on each reader of the page's tables, `c` and `p`, and not on `q_copy`,
/// which reads neither, and `self_check` reports it for the instance.
/// Nothing is paused or charged. Another failed pass counts an attempt,
/// keeping `since`. Restoring the grant, the next drain commits the page and
/// the holdup goes with it.
#[tokio::test]
async fn an_unpinned_refusal_is_a_holdup_its_readers_report_until_the_page_commits() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, &READERS).await;
    let worker = worker(&it, "").await;
    refuse_a_column_and_rename(&it, 2).await;
    let seg = seal(&mut it.raw).await;
    let err = drain(&worker, seg)
        .await
        .expect_err("the refused read surfaces");
    assert!(err.contains("permission denied"), "{err}");

    let page = vec!["public.c".to_string(), "public.p".to_string()];
    let rows = holdups(&it.admin).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].seg_seq, seg);
    assert_eq!(rows[0].tables, page);
    assert_eq!(rows[0].sqlstate.as_deref(), Some("42501"));
    assert_eq!(rows[0].attempts, 1);
    assert!(!rows[0].buckets.is_empty());

    for reader in ["c_named", "p_copy", "c_copy"] {
        let reported = status(&it, reader).await;
        assert_eq!(
            reported.status,
            TransformStatus::Live,
            "{reader} isn't paused"
        );
        assert!(reported.capture_failure.is_none(), "{reader}: {reported:?}");
        let failure = reported
            .drain_failure
            .unwrap_or_else(|| panic!("{reader} reports the holdup"));
        assert_eq!(failure.seg_seq, seg);
        assert_eq!(failure.tables, page);
        assert_eq!(failure.sqlstate.as_deref(), Some("42501"));
        assert_eq!(failure.attempts, 1);
        assert!(failure.error.contains("permission denied"), "{failure:?}");
        assert_eq!(failure.since, rows[0].since);
    }
    let unaffected = status(&it, "q_copy").await;
    assert_eq!(
        unaffected.drain_failure, None,
        "q_copy reads neither c nor p"
    );
    assert_eq!(charged(&it.admin).await, 0, "no key was charged");

    let report = it
        .trellis
        .self_check(
            "q_copy",
            trellis::SelfCheckScope {
                after: None,
                limit: 100,
            },
            trellis::SelfCheckMode::Strict,
            Duration::from_secs(1),
        )
        .await
        .expect("self_check");
    assert_eq!(
        report
            .drain_failures
            .iter()
            .map(|failure| failure.seg_seq)
            .collect::<Vec<_>>(),
        vec![seg],
        "self_check reports the instance's holdups, whichever definition it audits"
    );

    drain(&worker, seg)
        .await
        .expect_err("the refused read surfaces again");
    let again = holdups(&it.admin).await;
    assert_eq!(again.len(), 1, "{again:?}");
    assert_eq!(again[0].attempts, 2);
    assert_eq!(again[0].since, rows[0].since, "since is the first failure");

    it.admin
        .batch_execute("grant select (pid) on public.c to holdup_worker")
        .await
        .expect("restore the grant");
    drain(&worker, seg)
        .await
        .expect("the page commits once the grant is back");
    assert!(holdups(&it.admin).await.is_empty());
    for reader in READERS.map(|ddl| targets(&[ddl])[0]) {
        let reported = status(&it, reader).await;
        assert_eq!(reported.drain_failure, None, "{reader}");
        assert_eq!(reported.status, TransformStatus::Live, "{reader}");
    }
    assert_eq!(charged(&it.admin).await, 0);
    it.trellis.shutdown().await.expect("shutdown");
}

/// Two pages, in two segments, fail: two holdups, one per segment, and
/// `status` reports the oldest. Each page's commit clears its own and leaves
/// the other's.
#[tokio::test]
async fn each_failing_page_has_its_own_holdup_cleared_by_its_own_commit() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, &READERS[..1]).await;
    let worker = worker(&it, "").await;
    let mut segs = Vec::new();
    for id in [1, 2] {
        refuse_a_column_and_rename(&it, id).await;
        segs.push(seal(&mut it.raw).await);
    }
    for &seg in &segs {
        let err = drain(&worker, seg).await.expect_err("refused");
        assert!(err.contains("permission denied"), "{err}");
    }
    let rows = holdups(&it.admin).await;
    assert_eq!(
        rows.iter().map(|row| row.seg_seq).collect::<Vec<_>>(),
        segs,
        "{rows:?}"
    );
    assert_eq!(
        status(&it, "c_named")
            .await
            .drain_failure
            .map(|f| f.seg_seq),
        Some(segs[0]),
        "status reports the oldest"
    );

    it.admin
        .batch_execute("grant select (pid) on public.c to holdup_worker")
        .await
        .expect("restore the grant");
    drain(&worker, segs[0])
        .await
        .expect("the first page commits");
    assert_eq!(
        holdups(&it.admin)
            .await
            .iter()
            .map(|row| row.seg_seq)
            .collect::<Vec<_>>(),
        vec![segs[1]],
        "the second page's holdup stays until its own page commits"
    );
    assert_eq!(
        status(&it, "c_named")
            .await
            .drain_failure
            .map(|f| f.seg_seq),
        Some(segs[1])
    );
    drain(&worker, segs[1])
        .await
        .expect("the second page commits");
    assert!(holdups(&it.admin).await.is_empty());
    assert_eq!(status(&it, "c_named").await.drain_failure, None);
    it.trellis.shutdown().await.expect("shutdown");
}

/// Who `status` reports a holdup on, from hand-written rows: every
/// definition reading a table on the page as its source, or through a
/// relationship its fields name, unless it is frozen. A relationship
/// declared on a definition's source that it doesn't read through doesn't
/// count.
#[tokio::test]
async fn status_reports_a_holdup_on_each_unfrozen_reader_of_its_tables() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, &READERS).await;
    let seg = seal(&mut it.raw).await;
    let holdup = |tables: &str| {
        format!(
            "delete from {SCHEMA}.drain_holdups; \
             insert into {SCHEMA}.drain_holdups \
               (seg_seq, buckets, tables, error, sqlstate, since, last_seen, attempts) \
             values ({seg}, '{{0}}', '{tables}', 'it fails', null, now(), now(), 3)"
        )
    };
    let reporting = async |it: &Instance| {
        let mut reporting = Vec::new();
        for reader in READERS.map(|ddl| targets(&[ddl])[0]) {
            if let Some(failure) = status(it, reader).await.drain_failure {
                assert_eq!(failure.seg_seq, seg);
                assert_eq!(failure.attempts, 3);
                assert_eq!(failure.sqlstate, None);
                assert_eq!(failure.error, "it fails");
                reporting.push(reader);
            }
        }
        reporting
    };

    it.admin
        .batch_execute(&holdup("{public.p}"))
        .await
        .expect("hold a page of p");
    assert_eq!(
        reporting(&it).await,
        vec!["c_named", "p_copy"],
        "p's reader, and c_named through parent"
    );
    it.admin
        .batch_execute(&holdup("{public.q}"))
        .await
        .expect("hold a page of q");
    assert_eq!(
        reporting(&it).await,
        vec!["q_copy"],
        "c_copy and c_named don't read through twin"
    );
    it.admin
        .batch_execute(&holdup("{public.c}"))
        .await
        .expect("hold a page of c");
    assert_eq!(reporting(&it).await, vec!["c_named", "c_copy"]);

    it.trellis
        .apply("PAUSE TRANSFORM c_copy")
        .await
        .expect("pause c_copy");
    assert_eq!(
        reporting(&it).await,
        vec!["c_named"],
        "a paused definition applies nothing, so it isn't held back"
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// Records that fail only together: two keys of `c` set to one amount, which
/// a unique index on `c_copy`'s target refuses. Each key applies alone, so
/// isolation reproduces nothing and charges no key, and the drain records a
/// holdup that `c_copy` reports.
#[tokio::test]
async fn records_failing_only_together_are_a_holdup() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, &READERS[2..]).await;
    it.admin
        .batch_execute(
            "create unique index c_copy_amount on public.c_copy (amount); \
             update public.c set amount = 100 where id in (1, 2)",
        )
        .await
        .expect("a unique amount, and two keys that collide on it");
    let seg = seal(&mut it.raw).await;
    let err = drain(&it.pool, seg)
        .await
        .expect_err("the page fails while both keys are in it");
    assert!(err.contains("c_copy_amount"), "{err}");

    let rows = holdups(&it.admin).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].sqlstate.as_deref(), Some("23505"));
    assert_eq!(rows[0].tables, vec!["public.c".to_string()]);
    assert_eq!(charged(&it.admin).await, 0, "isolation charged nothing");
    let reported = status(&it, "c_copy").await;
    assert_eq!(reported.status, TransformStatus::Live);
    assert_eq!(reported.drain_failure.map(|f| f.seg_seq), Some(seg));
    it.trellis.shutdown().await.expect("shutdown");
}

/// A transient failure records nothing: retrying it is the expected
/// behaviour. Here every statement of the drain's sessions times out after
/// 300 ms, and a lock held on `c_copy`'s target keeps its write waiting past
/// that, so each attempt fails with a statement timeout until the retries
/// run out.
#[tokio::test]
async fn a_transient_failure_records_no_holdup() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, &READERS[2..]).await;
    let impatient = worker(&it, "-c statement_timeout=300").await;
    it.admin
        .batch_execute("update public.c set amount = 100 where id = 1")
        .await
        .expect("write the source");
    let seg = seal(&mut it.raw).await;

    let holder = connect(it.db.dsn()).await;
    holder
        .batch_execute("begin; lock table public.c_copy in access exclusive mode")
        .await
        .expect("hold the target's lock");
    let err = drain(&impatient, seg)
        .await
        .expect_err("every attempt times out");
    assert!(err.contains("statement timeout"), "{err}");
    assert!(holdups(&it.admin).await.is_empty());
    holder.batch_execute("rollback").await.expect("release");

    drain(&impatient, seg).await.expect("the page commits");
    assert!(holdups(&it.admin).await.is_empty());
    assert_eq!(status(&it, "c_copy").await.drain_failure, None);
    it.trellis.shutdown().await.expect("shutdown");
}

/// The key-by-key rows the quarantine charged: `(key, deaths)`.
async fn deaths(admin: &Client) -> Vec<(String, i32)> {
    admin
        .query(
            &format!("select key, deaths from {SCHEMA}.key_deaths order by key"),
            &[],
        )
        .await
        .expect("read the charged keys")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// `c_copy`'s amount for `id`.
async fn copied_amount(admin: &Client, id: i32) -> i32 {
    admin
        .query_one("select amount from public.c_copy where id = $1", &[&id])
        .await
        .expect("read the target")
        .get(0)
}

/// A deferred constraint an application put on a target fails at COMMIT
/// (#856). A deferrable foreign key from `c_copy.amount` to `q.id`, and a
/// page with two keys of `c`: key 3 names no `q`, key 4 names one. The
/// drain classifies the COMMIT's failure like any other, and isolation, whose
/// probes check deferred constraints before rolling back, charges key 3 alone
/// on the first pass; nothing is held up. Once key 3 reaches the death
/// threshold it is poisoned, and the page commits without it.
#[tokio::test]
async fn a_deferred_constraint_failing_at_commit_is_charged_to_its_key() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, &READERS[2..]).await;
    it.admin
        .batch_execute(
            "alter table public.c_copy add constraint c_copy_amount_q \
               foreign key (amount) references public.q (id) deferrable initially deferred; \
             update public.c set amount = 99 where id = 3; \
             update public.c set amount = 2 where id = 4",
        )
        .await
        .expect("a deferred foreign key, a key that violates it and one that doesn't");
    let seg = seal(&mut it.raw).await;

    let err = drain(&it.pool, seg)
        .await
        .expect_err("the page fails at COMMIT");
    assert!(err.contains("c_copy_amount_q"), "{err}");
    assert_eq!(
        deaths(&it.admin).await,
        vec![("3".to_string(), 1)],
        "isolation reproduced the deferred failure on key 3 alone"
    );
    assert!(
        holdups(&it.admin).await.is_empty(),
        "a charged key isn't a holdup"
    );
    assert_eq!(status(&it, "c_copy").await.drain_failure, None);

    for pass in 2..quarantine::DEFAULT_DEATH_THRESHOLD {
        drain(&it.pool, seg)
            .await
            .expect_err("each pass charges key 3 again");
        assert_eq!(deaths(&it.admin).await, vec![("3".to_string(), pass)]);
    }
    drain(&it.pool, seg)
        .await
        .expect("key 3 reaches the threshold and the page commits without it");
    assert_eq!(copied_amount(&it.admin, 4).await, 2, "key 4 applied");
    assert_eq!(copied_amount(&it.admin, 3).await, 3, "key 3 didn't");
    assert!(holdups(&it.admin).await.is_empty());
    it.trellis.shutdown().await.expect("shutdown");
}

/// Records failing only together at COMMIT (#856): two keys of `c` set to one
/// amount, which a deferrable unique constraint on `c_copy` refuses when the
/// page commits. Each key passes alone, deferred check included, so
/// isolation charges nothing and the drain records a holdup.
#[tokio::test]
async fn records_failing_only_together_at_commit_are_a_holdup() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, &READERS[2..]).await;
    it.admin
        .batch_execute(
            "alter table public.c_copy add constraint c_copy_amount \
               unique (amount) deferrable initially deferred; \
             update public.c set amount = 100 where id in (1, 2)",
        )
        .await
        .expect("a deferred unique amount, and two keys that collide on it");
    let seg = seal(&mut it.raw).await;
    let err = drain(&it.pool, seg)
        .await
        .expect_err("the page fails at COMMIT while both keys are in it");
    assert!(err.contains("c_copy_amount"), "{err}");

    let rows = holdups(&it.admin).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].seg_seq, seg);
    assert_eq!(rows[0].sqlstate.as_deref(), Some("23505"));
    assert_eq!(rows[0].tables, vec!["public.c".to_string()]);
    assert_eq!(charged(&it.admin).await, 0, "isolation charged nothing");
    assert_eq!(
        status(&it, "c_copy").await.drain_failure.map(|f| f.seg_seq),
        Some(seg)
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// A transient failure at COMMIT is retried (#856): a deferred constraint
/// trigger on `c_copy` raises a serialization failure the first time it
/// fires, counted by a sequence, which a rollback doesn't take back. One
/// drain call retries the page, and it commits.
#[tokio::test]
async fn a_transient_failure_at_commit_is_retried() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, &READERS[2..]).await;
    it.admin
        .batch_execute(
            "create sequence public.commit_attempts; \
             grant usage on sequence public.commit_attempts to holdup_trellis; \
             create function public.fail_first_commit() returns trigger \
               language plpgsql as $$ \
               begin \
                 if nextval('public.commit_attempts') = 1 then \
                   raise exception 'the first commit fails' using errcode = '40001'; \
                 end if; \
                 return null; \
               end $$; \
             create constraint trigger c_copy_fail_first_commit \
               after insert or update or delete on public.c_copy \
               deferrable initially deferred \
               for each row execute function public.fail_first_commit(); \
             update public.c set amount = 100 where id = 1",
        )
        .await
        .expect("a deferred trigger failing the first commit");
    let seg = seal(&mut it.raw).await;

    drain(&it.pool, seg)
        .await
        .expect("the drain retries the page past the failed commit");
    let fired: i64 = it
        .admin
        .query_one("select last_value from public.commit_attempts", &[])
        .await
        .expect("read the sequence")
        .get(0);
    assert!(
        fired >= 2,
        "the first commit failed and a retry fired it again"
    );
    assert_eq!(copied_amount(&it.admin, 1).await, 100);
    assert!(holdups(&it.admin).await.is_empty());
    assert_eq!(charged(&it.admin).await, 0);
    it.trellis.shutdown().await.expect("shutdown");
}
