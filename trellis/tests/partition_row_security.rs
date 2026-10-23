//! #958 (X12-7f): row-level security on a partitioned source.
//!
//! Trellis runs as a non-superuser login role that owns the tree (a superuser
//! bypasses RLS). Postgres applies a table's own policies only when the table
//! is named in the query: a read through the root applies the root's and not
//! a leaf's. So:
//!
//! - a policy that applies to the role on the **root** is the plain-table
//!   case: the capture pass pauses the readers, naming the root;
//! - a policy on a **leaf** alone hides nothing from the row flow, whose
//!   reads (the drain's, the capture functions' live re-read, a build's) all
//!   go through the root;
//! - the correction engine enumerates a node's own keys by reading the node
//!   (it must read a detached or truncated one), and those reads run with
//!   `row_security = off` (#766): a leaf policy that applies makes the
//!   leaf's own correction fail loudly instead of skipping the hidden keys,
//!   the failure is recorded, and the correction completes once the role is
//!   exempt. Nothing ever counts a hidden row as gone.
//!
//! Every aggregate here is on the group basis (the dev knob), seeded through
//! capture. Nothing polls for convergence (#297).

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::defs::TransformStatus;
use trellis::defs::chunk_queue;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::partition_correction;
use trellis::staging::self_check::CheckPageExt;
use trellis::staging::{StagedWatermark, apply, seal};
use trellis::{SelfCheckOutcome, Trellis};

const SCHEMA: &str = trellis::config::DEFAULT_SCHEMA;

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

struct Instance {
    _db: testkit::TestDatabase,
    admin: Client,
    raw: Client,
    pool: trellis::Pool,
    trellis: Trellis,
}

/// `public.items (id, g, v)` range-partitioned on `g` into `items_p0` (0-7)
/// and `items_p1` (8-15), all owned by the login role `rls_trellis` that
/// Trellis runs as, an aggregate over it live, and 160 rows seeded through
/// capture.
async fn instance(cluster: &TestCluster) -> Instance {
    trellis::dev::staging::set_group_basis(true);
    let db = cluster.create_empty_database().await;
    let admin = connect(db.dsn()).await;
    admin
        .batch_execute(&format!(
            "create role rls_trellis login; \
             grant create on database \"{}\" to rls_trellis; \
             grant create on schema public to rls_trellis; \
             create table public.items (id int not null, g int not null, v int not null, \
                 primary key (g, id)) partition by range (g); \
             create table public.items_p0 partition of public.items for values from (0) to (8); \
             create table public.items_p1 partition of public.items for values from (8) to (16); \
             alter table public.items owner to rls_trellis; \
             alter table public.items_p0 owner to rls_trellis; \
             alter table public.items_p1 owner to rls_trellis;",
            db.name()
        ))
        .await
        .expect("a login role owning the tree");
    let dsn = db.dsn().replace("user=postgres", "user=rls_trellis");
    let config = trellis::Config::with_schema(dsn.clone(), SCHEMA).expect("valid config");
    let pool = trellis::Pool::new(&config).expect("pool");
    trellis::migrate(&pool, &config)
        .await
        .expect("migrate as the login role");
    let raw = connect(&dsn).await;
    raw.batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    let trellis = Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("connect as the login role");
    trellis
        .apply("TRANSFORM agg FROM public.items GROUP BY g SELECT COUNT(*) AS n, SUM(v) AS total")
        .await
        .expect("define");
    let mut it = Instance {
        _db: db,
        admin,
        raw,
        pool,
        trellis,
    };
    it.pass().await;
    trellis::intake::markers::settle_registrations(&it.pool).await;
    it.exec("insert into public.items select i, i % 16, i from generate_series(1, 160) i")
        .await;
    it.settle().await.expect("seed");
    assert_eq!(it.wrong_groups().await, Vec::<String>::new());
    it
}

impl Instance {
    /// Runs `sql` as the superuser, for DDL and for application writes.
    async fn exec(&self, sql: &str) {
        self.admin
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// The capture half of one staging-worker pass.
    async fn pass(&mut self) {
        let desired = trellis::defs::tables_to_capture(&self.pool)
            .await
            .expect("tables to capture");
        let outcome = trellis::capture::reconcile::reconcile(
            &mut self.raw,
            SCHEMA,
            &desired,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("capture pass");
        assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    }

    async fn status(&self) -> trellis::DefinitionStatus {
        self.trellis
            .status("agg")
            .await
            .expect("status")
            .expect("agg is registered")
    }

    /// The groups whose target row differs from a `GROUP BY` the superuser
    /// reads, as `group: have vs want`.
    async fn wrong_groups(&self) -> Vec<String> {
        self.admin
            .query(
                "with want as (select g, count(*) n, sum(v) total from public.items group by g), \
                      have as (select g, n, total from public.agg) \
                 select coalesce(w.g, h.g)::text || ': ' || coalesce(h.n::text, '-') \
                        || ' vs ' || coalesce(w.n::text, '-') \
                 from want w full join have h on h.g = w.g \
                 where w.n is distinct from h.n or w.total is distinct from h.total \
                 order by coalesce(w.g, h.g)",
                &[],
            )
            .await
            .expect("oracle")
            .iter()
            .map(|row| row.get(0))
            .collect()
    }

    /// One seal, a drain of it and the correction engine, repeated until the
    /// pipeline is quiet. The first error stops it.
    async fn settle(&mut self) -> Result<(), String> {
        for _ in 0..16 {
            self.drain_sealed().await?;
            trellis::staging::retire_drained_segments(&mut self.raw)
                .await
                .map_err(|e| e.to_string())?;
            trellis::staging::collect_tombstones(&mut self.raw)
                .await
                .map_err(|e| e.to_string())?;
            self.run_corrections().await?;
            if !trellis::staging::has_pending(&self.raw)
                .await
                .map_err(|e| e.to_string())?
            {
                return Ok(());
            }
        }
        Err("no quiescence in 16 rounds".to_string())
    }

    async fn run_corrections(&self) -> Result<partition_correction::Outcome, String> {
        partition_correction::run_pending(&self.pool, &partition_correction::Options::default())
            .await
            .map_err(|e| format!("{e:?}"))
    }

    /// Claims and runs every claimable correction row once, as a drain worker
    /// does: a row that fails is given up through the chunk queue, which
    /// records the error `Trellis::status` reports.
    async fn worker_turns(&self) {
        let options = WorkerOptions {
            chunk_rows: build::DEFAULT_CHUNK_ROWS,
            drain_batch_cap: usize::MAX,
            heartbeat_interval: Duration::from_secs(5),
            reclaim_ttl: Duration::from_secs(60),
        };
        loop {
            let claimed = {
                let client = self.pool.get().await.expect("pool");
                chunk_queue::claim_chunks_of(
                    &**client,
                    "worker",
                    1,
                    &[chunk_queue::KIND_CORRECTION],
                )
                .await
                .expect("claim")
            };
            let Some(chunk) = claimed.into_iter().next() else {
                return;
            };
            build::run_claimed(&self.pool, &chunk, "worker", &options).await;
        }
    }

    async fn drain_sealed(&mut self) -> Result<(), String> {
        let sealed = seal::seal_phase1(&mut self.raw)
            .await
            .map_err(|e| e.to_string())?;
        seal::seal_phase2(&self.raw, sealed.sealed_seg_seq, "wake")
            .await
            .map_err(|e| e.to_string())?;
        let watermark = StagedWatermark::saturated();
        for _ in 0..16 {
            match apply::drain_once(
                &self.pool,
                sealed.sealed_seg_seq,
                "partition_row_security",
                1,
                "trellis_partition_row_security",
                &watermark,
            )
            .await
            {
                Ok(Some(_)) => {}
                Ok(None) => return Ok(()),
                Err(err) => return Err(format!("{err:?}")),
            }
        }
        Err("a drain didn't finish in 16 turns".to_string())
    }
}

/// A policy on the root: enabled without `FORCE`, the owner is exempt and
/// nothing changes; forced, it applies to the role, the next pass pauses the
/// reader naming the root, and a leaf dropped meanwhile is not corrected
/// under a wrong read.
#[tokio::test]
async fn a_policy_on_the_root_is_the_plain_table_case() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.exec(
        "alter table public.items enable row level security; \
         create policy even_only on public.items using (id % 2 = 0)",
    )
    .await;
    it.pass().await;
    assert_eq!(it.status().await.status, TransformStatus::Live);
    it.exec("update public.items set v = v + 1 where id % 3 = 0")
        .await;
    it.settle().await.expect("an exempt owner drains");
    assert_eq!(it.wrong_groups().await, Vec::<String>::new());

    it.exec("alter table public.items force row level security")
        .await;
    it.pass().await;
    let status = it.status().await;
    assert_eq!(status.status, TransformStatus::Paused);
    let failure = status.capture_failure.expect("the reason");
    assert_eq!(failure.source_table, "public.items");
    assert!(
        failure.error.contains("FORCE ROW LEVEL SECURITY") && failure.error.contains("resume"),
        "{failure:?}"
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// A policy on a leaf alone, forced on the role: every read of the row flow
/// goes through the root, so nothing is hidden. Writes through the parent
/// (insert, update, delete, and a write nested in the same statement, which
/// makes capture re-read) and the correction of another leaf converge, the
/// capture pass pauses nothing and `self_check` finds nothing.
#[tokio::test]
async fn a_policy_on_a_leaf_alone_hides_nothing_from_the_row_flow() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.exec(
        "alter table public.items_p1 enable row level security, force row level security; \
         create policy even_only on public.items_p1 using (id % 2 = 0)",
    )
    .await;
    it.pass().await;
    assert_eq!(it.status().await.status, TransformStatus::Live);
    assert_eq!(it.status().await.capture_failure, None);

    // The hidden rows: odd ids in the leaf. A nested update in the same
    // statement makes the insert's capture re-read each row it wrote.
    it.exec(
        "create function public.bump() returns trigger language plpgsql as $$ begin \
             update public.items set v = v + 1 where g = new.g and id = new.id; return null; \
         end $$; \
         create trigger bump after insert on public.items for each row execute function public.bump(); \
         insert into public.items values (601, 9, 1), (602, 9, 1), (603, 2, 1), (604, 2, 1); \
         drop trigger bump on public.items; \
         update public.items set v = v + 10 where g in (9, 10); \
         delete from public.items where id in (17, 33, 49, 50)",
    )
    .await;
    it.settle()
        .await
        .expect("the row flow reads through the root");
    assert_eq!(it.wrong_groups().await, Vec::<String>::new());

    // The other leaf's correction does not read the policy's leaf.
    it.exec("drop table public.items_p0").await;
    it.pass().await;
    it.settle().await.expect("the dropped leaf's correction");
    assert_eq!(it.wrong_groups().await, Vec::<String>::new());
    assert_eq!(it.status().await.status, TransformStatus::Live);

    match it
        .trellis
        .self_check_to_end(
            "agg",
            trellis::SelfCheckMode::Strict,
            Duration::from_secs(30),
        )
        .await
        .expect("self_check")
        .outcome
    {
        SelfCheckOutcome::Converged => {}
        other => panic!("{other:?}"),
    }
    it.trellis.shutdown().await.expect("shutdown");
}

/// The same leaf policy, and the leaf truncated: the correction enumerates
/// the leaf's own keys, and its read raises (`row_security = off`, #766)
/// where it would otherwise have skipped the hidden keys and left their
/// groups counted. The failure is recorded, the groups stay as they were,
/// and once the role is exempt the retry corrects them.
#[tokio::test]
async fn a_policy_on_a_leaf_makes_that_leafs_own_correction_fail_loudly() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.exec(
        "alter table public.items_p1 enable row level security, force row level security; \
         create policy even_only on public.items_p1 using (id % 2 = 0); \
         truncate public.items_p1",
    )
    .await;
    it.pass().await;
    // The seal queues the truncate's correction, the staging worker
    // schedules it and a drain worker runs it.
    it.drain_sealed().await.expect("the truncate drains");
    partition_correction::schedule(&it.pool)
        .await
        .expect("schedule");
    it.worker_turns().await;
    let wrong = it.wrong_groups().await;
    assert_eq!(wrong.len(), 8, "the leaf's eight groups stay: {wrong:?}");
    let status = it.status().await;
    assert_eq!(status.status, TransformStatus::Backfilling);
    let failure = status.backfill_failure.expect("the failure is recorded");
    assert!(
        failure.last_error.contains("row-level security policy")
            && failure.last_error.contains("items_p1"),
        "the failure names the policy and the leaf: {failure:?}"
    );

    // Exempt the role; the retry reads the whole leaf.
    it.exec("alter table public.items_p1 no force row level security")
        .await;
    // The failed row waits out a backoff; the test ends it.
    it.exec(&format!(
        "update {SCHEMA}.backfill_chunks set next_attempt_at = null where kind = 'correction'"
    ))
    .await;
    it.worker_turns().await;
    it.settle().await.expect("the retry's deltas fold");
    assert_eq!(it.wrong_groups().await, Vec::<String>::new());
    assert_eq!(it.status().await.status, TransformStatus::Live);
    it.trellis.shutdown().await.expect("shutdown");
}
