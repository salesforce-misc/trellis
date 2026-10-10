//! A chunked build over a text-keyed source whose key column is re-collated
//! while the build runs (issue #769).
//!
//! A build's chunks are `(lo, hi]` ranges of the source's primary key, stored
//! as text in `backfill_chunks.lo/hi` and compared against the key column by
//! later transactions. `alter column … type text collate …` between two of
//! them changes how the column orders, so the ranges planned under the old
//! order no longer partition the source under the new one: some keys fall in
//! no remaining range. The test database's default collation is ICU `en-US`
//! (`testkit::cluster`), which orders `k0001 < K0002 < k0003`; `"C"` orders
//! every `K…` before every `k…`.
//!
//! Every test steps the engine by hand, as `rederive_build.rs` does: no
//! background worker runs and nothing waits for convergence (#297).

use std::time::Duration;

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::chunk_queue;
use trellis::defs::{Statement, ValueType, alter_transform, parse_statement};
use trellis::staging::build::one_to_one::{self, OneToOnePlan};
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments};

const WAKE: &str = "key_collation_wake";

const ONE: &str = "TRANSFORM one FROM public.src SELECT g AS g, v + v AS dbl";
const ONE_ACTUAL: &str = "select (id, g, dbl)::text from public.one order by id collate \"C\"";
const ONE_EXPECTED: &str = "select (id, g, v + v)::text from public.src order by id collate \"C\"";

const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n";
const AGG_ACTUAL: &str = "select (g, total, n)::text from public.agg order by g";
const AGG_EXPECTED: &str =
    "select (g, sum(v), count(*))::text from public.src group by g order by g";

/// Ten rows per chunk.
const OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 10,
    drain_batch_cap: 100_000,
    heartbeat_interval: Duration::from_secs(1),
    reclaim_ttl: Duration::from_secs(30),
};

const MAX_STEPS: usize = 2_000;

struct Fixture {
    db: TestDatabase,
    raw: Client,
    _cluster: TestCluster,
}

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

impl Fixture {
    /// `public.src (id text, g, v)` with 200 rows keyed `k0001`, `K0002`,
    /// `k0003`, … (lower case for an odd number, upper case for an even
    /// one), under the database's default collation, and `definition`
    /// registered over it.
    async fn new(definition: &str) -> Self {
        Self::with_key("", definition).await
    }

    /// [`Fixture::new`] with `key` (`collate "C"`, say) on the key column.
    async fn with_key(key: &str, definition: &str) -> Self {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = connect(db.dsn()).await;
        raw.batch_execute(&format!(
            "create table public.src (id text {key} primary key, g integer, v bigint); \
             insert into public.src \
             select case when i % 2 = 0 then 'K' else 'k' end || lpad(i::text, 4, '0'), \
                    i % 7, i \
             from generate_series(1, 200) i"
        ))
        .await
        .expect("seed the source");
        // The source's own types: an `ALTER TRANSFORM` refuses to build a
        // field over a column recorded as another type than the source has.
        let columns = [
            ("id".to_string(), ValueType::Text),
            ("g".to_string(), ValueType::Integer(trellis::IntWidth::Int4)),
            ("v".to_string(), ValueType::Integer(trellis::IntWidth::Int8)),
        ]
        .into_iter()
        .collect();
        trellis::defs::install_definition(&db.pool, definition, &columns, "public")
            .await
            .expect("register the definition");
        Self {
            db,
            raw,
            _cluster: cluster,
        }
    }

    async fn pass(&mut self) {
        trellis::client::reconcile_pass(
            &mut self.raw,
            &self.db.pool,
            DEFAULT_SCHEMA,
            WAKE,
            Duration::from_secs(5),
        )
        .await
        .expect("reconcile pass");
    }

    /// Claims and runs one plan job or chunk by hand. Returns whether there
    /// was one.
    async fn run_one_chunk(&self) -> bool {
        let pool = &self.db.pool;
        let claimed = {
            let client = pool.get().await.expect("pool");
            chunk_queue::claim_chunks_of(
                &**client,
                "hand",
                1,
                &[chunk_queue::KIND_PLAN, chunk_queue::KIND_REDERIVE],
            )
            .await
            .expect("claim")
        };
        let Some(chunk) = claimed.into_iter().next() else {
            return false;
        };
        build::run_claimed(pool, &chunk, "hand", &OPTIONS).await;
        true
    }

    async fn drain(&mut self) {
        let watermark = StagedWatermark::saturated();
        for _ in 0..16 {
            trellis::staging::seal_if_active_nonempty(&mut self.raw, WAKE)
                .await
                .expect("seal");
            while let Some(seg) = apply::next_claimable_segment(&self.raw)
                .await
                .expect("next claimable segment")
            {
                apply::drain_once(&self.db.pool, seg, "drainer", 1, WAKE, &watermark)
                    .await
                    .expect("drain_once");
            }
            retire_drained_segments(&mut self.raw)
                .await
                .expect("retire drained segments");
            if !has_pending(&self.raw).await.expect("has_pending") {
                return;
            }
        }
        panic!("the ring did not quiesce within 16 seal/drain rounds");
    }

    /// Runs build steps until one does no work.
    async fn finish(&mut self) {
        for _ in 0..MAX_STEPS {
            self.drain().await;
            let step = build::work_once(
                &self.db.pool,
                "worker",
                &OPTIONS,
                &mut build::MergeFailures::default(),
            )
            .await
            .expect("build step");
            if !step.progressed() {
                return;
            }
        }
        panic!("the build did not finish within {MAX_STEPS} steps");
    }

    async fn status(&self, target: &str) -> String {
        self.raw
            .query_one(
                "select status from transform_definitions \
                 where split_part(target_table, '.', 2) = $1",
                &[&target],
            )
            .await
            .expect("read the status")
            .get(0)
    }

    async fn rows(&self, sql: &str) -> Vec<String> {
        self.raw
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .into_iter()
            .map(|row| row.get(0))
            .collect()
    }

    async fn count(&self, sql: &str) -> i64 {
        self.raw
            .query_one(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .get(0)
    }

    /// Re-collates the source's key column, rewriting the table and its
    /// primary-key index, then lets the staging worker see the change.
    async fn recollate_key(&mut self, collation: &str) {
        self.raw
            .batch_execute(&format!(
                "alter table public.src alter column id type text collate \"{collation}\""
            ))
            .await
            .expect("re-collate the key");
        self.pass().await;
    }

    /// Starts the build, runs its plan job and its first chunk under the
    /// default collation, re-collates the key to `"C"`, and finishes it.
    async fn build_across_a_recollation(&mut self, target: &str) {
        self.pass().await;
        assert_eq!(self.status(target).await, "backfilling");
        assert!(self.run_one_chunk().await, "the plan job");
        assert_eq!(
            self.count("select count(*) from backfill_chunks where kind = 'rederive'")
                .await,
            20,
            "200 rows in chunks of 10, all planned in one batch"
        );
        assert!(self.run_one_chunk().await, "the first chunk");
        self.recollate_key("C").await;
        self.finish().await;
        assert_eq!(self.status(target).await, "live");
    }
}

/// A 1-1 Re-derive build whose remaining chunks run after the key is
/// re-collated still writes every source row.
#[tokio::test]
async fn a_one_to_one_build_survives_its_key_being_recollated() {
    let mut f = Fixture::new(ONE).await;
    f.build_across_a_recollation("one").await;
    assert_eq!(
        f.rows(ONE_ACTUAL).await,
        f.rows(ONE_EXPECTED).await,
        "the 1-1 target equals its source"
    );
}

/// An aggregate Re-derive build whose remaining chunks run after the key is
/// re-collated still counts every source row once.
#[tokio::test]
async fn an_aggregate_build_survives_its_key_being_recollated() {
    let mut f = Fixture::new(AGG).await;
    f.build_across_a_recollation("agg").await;
    assert_eq!(
        f.rows(AGG_ACTUAL).await,
        f.rows(AGG_EXPECTED).await,
        "the target equals a from-scratch GROUP BY over the source"
    );
}

/// A field build on a source keyed under a collation other than the
/// database's default writes the field on every row. The chunk reads the
/// target by the same `(lo, hi]` range as the source; when the target's key
/// was created under the default collation, that range held other keys on
/// the target than on the source, and the field build left the rest null.
#[tokio::test]
async fn a_field_build_over_a_c_collated_key_writes_every_row() {
    let mut f = Fixture::with_key(r#"collate "C""#, ONE).await;
    f.pass().await;
    f.finish().await;
    assert_eq!(f.status("one").await, "live");
    assert_eq!(f.rows(ONE_ACTUAL).await, f.rows(ONE_EXPECTED).await);
    assert_eq!(
        f.rows(
            "select collation_name::text from information_schema.columns \
             where table_schema = 'public' and table_name = 'one' and column_name = 'id'"
        )
        .await,
        vec!["C".to_string()],
        "the target's key keeps the source key's collation"
    );

    let Statement::AlterTransform(alter) =
        parse_statement("ALTER TRANSFORM one ADD v + v + v AS tri").expect("parse the ALTER")
    else {
        panic!("expected an ALTER TRANSFORM statement");
    };
    alter_transform(&f.db.pool, &alter)
        .await
        .expect("add a field");
    f.pass().await;
    f.finish().await;
    assert_eq!(f.status("one").await, "live");
    assert_eq!(
        f.rows(r#"select (id, tri)::text from public.one order by id collate "C""#)
            .await,
        f.rows(r#"select (id, v + v + v)::text from public.src order by id collate "C""#)
            .await,
        "the field build wrote every row"
    );
}

/// The `explain` of a 1-1 chunk's statement over a text key collated
/// `key`, after its first chunk: no table is read by a sequential scan.
/// Naming the key's collation in the range predicate keeps the key indexes
/// usable; naming any other collation (`"C"` over an `en-US` key) can't use
/// them.
async fn chunk_plan(key: &str) -> String {
    let mut f = Fixture::with_key(key, ONE).await;
    f.pass().await;
    assert!(f.run_one_chunk().await, "the plan job");
    assert!(f.run_one_chunk().await, "the first chunk");
    f.raw
        .batch_execute("analyze public.src, public.one, public.one__ledger")
        .await
        .expect("analyze");
    let plan = OneToOnePlan::load(&f.db.pool, "one")
        .await
        .expect("load the plan")
        .expect("a 1-1 plan");
    let mut client = f.db.pool.get().await.expect("pool");
    let txn = client.transaction().await.expect("begin");
    let explained = one_to_one::explain_chunk(&txn, &plan, Some("K0010"), "K0020", &["k0011"])
        .await
        .expect("explain the chunk statement");
    txn.rollback().await.expect("roll back");
    explained
}

fn seq_scans(explained: &str) -> Vec<&str> {
    explained
        .lines()
        .filter(|line| line.contains("Seq Scan"))
        .collect()
}

#[tokio::test]
async fn a_chunk_over_a_default_collated_key_reads_by_index() {
    let explained = chunk_plan("").await;
    assert_eq!(seq_scans(&explained), Vec::<&str>::new(), "{explained}");
    assert!(explained.contains("src_pkey"), "{explained}");
    assert!(explained.contains("one_pkey"), "{explained}");
}

#[tokio::test]
async fn a_chunk_over_a_c_collated_key_reads_by_index() {
    let explained = chunk_plan(r#"collate "C""#).await;
    assert_eq!(seq_scans(&explained), Vec::<&str>::new(), "{explained}");
    assert!(explained.contains("src_pkey"), "{explained}");
    assert!(explained.contains("one_pkey"), "{explained}");
}

/// A build whose recorded collation is dropped mid-build (the key
/// re-collated to `"C"`, then `drop collation`) can't read its remaining
/// ranges in the order it planned them. Its chunks fail, are charged, and
/// pause the definition: it doesn't loop, and doesn't read in another order.
/// A resume plans a fresh build under the key's collation now, which
/// finishes. An aggregate, because a 1-1 target's key keeps the source key's
/// collation, which the `drop collation` then refuses to drop.
#[tokio::test]
async fn a_build_whose_recorded_collation_is_dropped_pauses_and_resumes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create collation public.keyed (provider = icu, locale = 'en-US'); \
         create table public.src (id text collate public.keyed primary key, g integer, \
                                  v bigint); \
         insert into public.src \
         select case when i % 2 = 0 then 'K' else 'k' end || lpad(i::text, 4, '0'), \
                i % 7, i \
         from generate_series(1, 200) i",
    )
    .await
    .expect("seed the source");
    let columns = [
        ("id".to_string(), ValueType::Text),
        ("g".to_string(), ValueType::Numeric),
        ("v".to_string(), ValueType::Numeric),
    ]
    .into_iter()
    .collect();
    trellis::defs::install_definition(&db.pool, AGG, &columns, "public")
        .await
        .expect("register the definition");
    let mut f = Fixture {
        db,
        raw,
        _cluster: cluster,
    };
    f.pass().await;
    assert!(f.run_one_chunk().await, "the plan job");
    assert!(f.run_one_chunk().await, "the first chunk");
    f.recollate_key("C").await;
    f.raw
        .batch_execute("drop collation public.keyed")
        .await
        .expect("drop the recorded collation");

    for _ in 0..MAX_STEPS {
        if f.status("agg").await != "backfilling" {
            break;
        }
        f.raw
            .batch_execute("update backfill_chunks set next_attempt_at = now()")
            .await
            .expect("skip the backoff");
        assert!(f.run_one_chunk().await, "a chunk to run while backfilling");
    }
    assert_eq!(f.status("agg").await, "paused");
    let errors = f
        .rows("select last_error from backfill_chunks where last_error is not null")
        .await;
    assert!(
        errors.iter().any(|e| e.contains("keyed")),
        "the failure names the dropped collation: {errors:?}"
    );

    trellis::staging::quarantine::resume_transform(&f.db.pool, "agg")
        .await
        .expect("resume");
    f.pass().await;
    f.finish().await;
    assert_eq!(f.status("agg").await, "live");
    assert_eq!(f.rows(AGG_ACTUAL).await, f.rows(AGG_EXPECTED).await);
}
