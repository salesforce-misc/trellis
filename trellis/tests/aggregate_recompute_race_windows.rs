//! Issue #321: an aggregate's forced recompute double counts a concurrent
//! source commit.
//!
//! A group on the forced path (`apply_aggregate::apply_forced_groups_bulk`)
//! is re-derived from the *live* source in Phase 3. Nothing records what that
//! read saw. A source commit that was visible to it, but whose own CDC delta
//! commits to the target afterwards, is counted twice: once by the live read,
//! once by the delta. Each test below drives one window in which that
//! happens, and asserts the target against a from-scratch `GROUP BY` over the
//! source:
//!
//! - **W1, later batch** (the issue as filed): the commit's CDC seals into a
//!   batch after the recompute's. Its variant commits before the seal but
//!   stages after it, the wider window from the issue's first comment.
//! - **W2, earlier batch drained later**: the CDC sits in an earlier batch
//!   that a second worker applies after the recompute's batch.
//! - **W3, same batch, different bucket**: a split batch puts the image-less
//!   recompute and the image-bearing change in different buckets, drained by
//!   different workers.
//! - **W4, grain migration**: the recompute's key moves to another group
//!   between Phase 2 (which picked the group) and Phase 3 (which re-derives
//!   it), so the old group loses the row twice.
//! - **W5, extinction**: the delta path's existence probe deletes a group a
//!   concurrent delete emptied; the group is then recreated, and the delete's
//!   own delta lands on the recreated group.
//! - **Chained W1**: the aggregate reads a 1-1 target that is also a
//!   relationship endpoint, published (today by the engine, under #315's
//!   exception; here by the test itself, see the test) so both the seam's
//!   `Recompute` and the target's own CDC reach the ring.
//!
//! Everything is driven by hand, as `intermediate_hop_cdc.rs` does: `pgoutput`
//! bytes are read off a second logical slot and fed to a real
//! [`intake::Intake`] exactly when a test says so, and seal and claim are
//! explicit, so no test polls for convergence (#297). The one wait (W4's)
//! waits for a backend to block on a row lock the test holds, which is a
//! forced interleaving, not a race.
//!
//! The fix is the recompute horizon (`apply_aggregate::apply_aggregate_target`):
//! a forced recompute stamps each group row with the WAL insert position
//! read after its live read, a live read that deletes a group row raises
//! the target's extinct horizon the same way, and a delta whose earliest
//! image-bearing commit is at or below its group's horizon re-derives the
//! group instead of applying. Every test here failed before it with the
//! double count described on it. The last two pin parts of the mechanism the
//! five windows don't reach on their own: a row the delta path creates
//! inheriting the extinct horizon, and the fold comparing a telescoped
//! delta by its *earliest* commit. Then come issue #322's definition-time
//! enumeration, which the same rule closes, two cases found in review (an
//! extinction with no row to delete, and a delta that reaches the aggregate
//! through a relationship's reverse fast path), and a seam-style writer whose
//! ordering token is taken before it commits (#375's direction 1).

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use pgwire_replication::{Lsn, ReplicationEvent};
use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{create_relationship, install_definition};
use trellis::intake::{self, spill};
use trellis::staging::{
    CdcOp, MIN_ROWS_TO_SPLIT, SEG_BUCKETS, StagedChange, StagedWatermark, apply, has_pending,
    retire_drained_segments,
};

const PUBLICATION: &str = "race_pub";
/// The slot [`intake::Intake::connect`] requires to exist and be healthy.
/// Nothing reads its stream: the test feeds intake from [`BYTES_SLOT`].
const INTAKE_SLOT: &str = "race_intake";
/// The slot the test reads `pgoutput` bytes from.
const BYTES_SLOT: &str = "race_bytes";
const WAKE: &str = "race_wake";
const SRC: &str = "public.src";

/// The source every test uses: `id` is the key, `g` the group, `v` the summed
/// value. `REPLICA IDENTITY FULL` so an update or delete carries the old
/// image the aggregate subtracts.
const SRC_DDL: &str = "create table public.src (id integer primary key, g integer, v numeric); \
                       alter table public.src replica identity full";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n";

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'"
        ))
        .await
        .expect("set search_path");
    client
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

fn be_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes[..8].try_into().expect("8 bytes"))
}

/// Turns one `pgoutput` protocol-v1 message into the event the replication
/// transport would have handed intake (copied from `intermediate_hop_cdc.rs`).
fn to_event(data: &[u8]) -> ReplicationEvent {
    match data[0] {
        b'B' => ReplicationEvent::Begin {
            final_lsn: Lsn::from(be_u64(&data[1..])),
            commit_time_micros: be_u64(&data[9..]) as i64,
            xid: u32::from_be_bytes(data[17..21].try_into().expect("4 bytes")),
        },
        b'C' => ReplicationEvent::Commit {
            lsn: Lsn::from(be_u64(&data[2..])),
            end_lsn: Lsn::from(be_u64(&data[10..])),
            commit_time_micros: be_u64(&data[18..]) as i64,
        },
        b'M' => {
            let transactional = data[1] & 1 == 1;
            let lsn = Lsn::from(be_u64(&data[2..]));
            let rest = &data[10..];
            let nul = rest
                .iter()
                .position(|b| *b == 0)
                .expect("prefix terminator");
            let prefix = String::from_utf8(rest[..nul].to_vec()).expect("utf-8 prefix");
            let body = &rest[nul + 1..];
            let len = u32::from_be_bytes(body[..4].try_into().expect("4 bytes")) as usize;
            ReplicationEvent::Message {
                transactional,
                lsn,
                prefix,
                content: Bytes::copy_from_slice(&body[4..4 + len]),
            }
        }
        _ => ReplicationEvent::XLogData {
            wal_start: Lsn::from(0),
            wal_end: Lsn::from(0),
            server_time_micros: 0,
            data: Bytes::copy_from_slice(data),
        },
    }
}

/// One definition-time step, run in order by [`Harness::start`].
enum Setup<'a> {
    Transform(&'a str),
    Relationship(&'a str),
    Sql(&'a str),
    /// Adds a table to the test's own publication on top of whatever the
    /// engine's [`trellis::defs::publication_tables`] lists, so a test that
    /// needs a table's CDC does not depend on the engine choosing to publish
    /// it.
    Publish(&'a str),
}

/// A source, the definitions over it, and a real [`intake::Intake`] fed by
/// hand from [`BYTES_SLOT`].
struct Harness {
    raw: Client,
    intake: intake::Intake,
    db: testkit::TestDatabase,
    /// The aggregate target the test checks.
    agg: &'static str,
    _cluster: TestCluster,
}

impl Harness {
    /// Runs `setup_sql` (which creates and seeds the source), then `steps`,
    /// then creates the publication and both slots. Rows seeded by
    /// `setup_sql` reach the aggregate through its initial build, not CDC.
    async fn start(setup_sql: &str, steps: &[Setup<'_>], agg: &'static str) -> Self {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = connect_raw(db.dsn()).await;
        raw.batch_execute(setup_sql).await.expect("setup sql");

        let columns = numeric_columns(&["id", "g", "v"]);
        let mut also_published = Vec::new();
        for step in steps {
            match step {
                Setup::Transform(text) => {
                    install_definition(&db.pool, text, &columns, "public")
                        .await
                        .unwrap_or_else(|e| panic!("install {text}: {e}"));
                }
                Setup::Relationship(text) => {
                    create_relationship(&db.pool, text)
                        .await
                        .unwrap_or_else(|e| panic!("create {text}: {e}"));
                }
                Setup::Sql(sql) => raw.batch_execute(sql).await.expect("setup step sql"),
                Setup::Publish(table) => also_published.push(table.to_string()),
            }
        }

        let mut published = trellis::defs::publication_tables(&db.pool)
            .await
            .expect("publication_tables");
        for table in also_published {
            if !published.contains(&table) {
                published.push(table);
            }
        }
        raw.batch_execute(&format!(
            "create publication {PUBLICATION} for table {}",
            published.join(", ")
        ))
        .await
        .expect("create publication");
        for slot in [INTAKE_SLOT, BYTES_SLOT] {
            raw.query_one(
                "select slot_name from pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .await
            .expect("create slot");
        }
        raw.execute(
            "insert into replication_progress (slot_name, confirmed_lsn) values ($1, $2)",
            &[&INTAKE_SLOT, &PgLsn::from(0)],
        )
        .await
        .expect("seed replication_progress");

        let config = intake::IntakeConfig {
            dsn: db.dsn().to_string(),
            schema: DEFAULT_SCHEMA.to_string(),
            host: db.socket_dir().display().to_string(),
            port: db.port(),
            user: "postgres".to_string(),
            password: String::new(),
            database: db.name().to_string(),
            slot: INTAKE_SLOT.to_string(),
            publication: PUBLICATION.to_string(),
            wake_channel: WAKE.to_string(),
            spill_threshold: spill::DEFAULT_SPILL_THRESHOLD,
            hard_cap: spill::DEFAULT_HARD_CAP,
            group_commit: None,
        };
        let intake = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
            .await
            .expect("connect intake");

        Self {
            raw,
            intake,
            db,
            agg,
            _cluster: cluster,
        }
    }

    /// Runs `sql` as its own committed source transaction. Intake does not
    /// see it until the next [`Harness::feed`].
    async fn commit(&self, sql: &str) {
        self.raw
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// Consumes everything committed on [`BYTES_SLOT`] since the last call and
    /// feeds it to intake, which stages it into the active segment.
    async fn feed(&mut self) {
        let rows = self
            .raw
            .query(
                "select data from pg_logical_slot_get_binary_changes($1, null, null, \
                 'proto_version', '1', 'publication_names', $2, 'messages', 'true')",
                &[&BYTES_SLOT, &PUBLICATION],
            )
            .await
            .expect("read pgoutput bytes");
        for row in rows {
            let data: Vec<u8> = row.get(0);
            self.intake
                .handle_event(to_event(&data))
                .await
                .expect("intake handles the event");
        }
    }

    /// Appends one bare, image-less `Recompute` per key of [`SRC`], the shape
    /// every catch-up enumeration and marker stages, in its own transaction.
    async fn append_recomputes(&mut self, keys: &[i32]) {
        let changes: Vec<StagedChange> = keys
            .iter()
            .map(|k| StagedChange::Recompute {
                src_table: SRC.to_string(),
                key: k.to_string(),
                hop_gen: 0,
                group_key: None,
                src_changed: None,
                prior_image: None,
            })
            .collect();
        let txn = self.raw.transaction().await.expect("begin append");
        trellis::staging::append(&txn, &changes)
            .await
            .expect("append recomputes");
        txn.commit().await.expect("commit append");
    }

    /// Seals the active segment and returns its `seg_seq`.
    async fn seal(&mut self) -> i64 {
        trellis::staging::seal_if_active_nonempty(&mut self.raw, WAKE)
            .await
            .expect("seal")
            .expect("the active segment holds rows, so it seals")
            .sealed_seg_seq
    }

    /// The distinct ring `op`s sealed segment `seg_seq` holds for
    /// `src_table`, read before the segment drains.
    async fn staged_ops(&self, seg_seq: i64, src_table: &str) -> Vec<String> {
        let ring_slot: i16 = self
            .raw
            .query_one(
                "select ring_slot from segments where seg_seq = $1",
                &[&seg_seq],
            )
            .await
            .expect("read ring_slot")
            .get(0);
        self.raw
            .query(
                &format!(
                    "select distinct op from seg_{ring_slot} where src_table = $1 order by op"
                ),
                &[&src_table],
            )
            .await
            .expect("read staged ops")
            .into_iter()
            .map(|row| row.get(0))
            .collect()
    }

    /// One `drain_once` against a specific segment, as worker `worker`.
    async fn drain(&self, seg_seq: i64, worker: &str, live_workers: i64) {
        let outcome = apply::drain_once(
            &self.db.pool,
            seg_seq,
            worker,
            live_workers,
            WAKE,
            &StagedWatermark::saturated(),
        )
        .await
        .expect("drain_once");
        assert!(
            outcome.is_some(),
            "{worker} must claim at least one bucket of segment {seg_seq}"
        );
    }

    /// Seals and drains until nothing is pending, each round its own batch.
    async fn settle(&mut self) {
        let watermark = StagedWatermark::saturated();
        for _ in 0..16 {
            trellis::staging::seal_if_active_nonempty(&mut self.raw, WAKE)
                .await
                .expect("seal");
            while let Some(seg) = apply::next_claimable_segment(&self.raw)
                .await
                .expect("next claimable segment")
            {
                apply::drain_once(&self.db.pool, seg, "settle", 1, WAKE, &watermark)
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
        panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
    }

    /// The aggregate target, `group -> (total, n)`.
    async fn target(&self) -> BTreeMap<String, (Option<String>, Option<String>)> {
        let sql = format!(
            "select trim_scale(g::numeric)::text, trim_scale(total::numeric)::text, \
             trim_scale(n::numeric)::text from public.{}",
            self.agg
        );
        self.groups(&sql).await
    }

    /// The same aggregate recomputed from scratch over the source.
    async fn oracle(&self) -> BTreeMap<String, (Option<String>, Option<String>)> {
        self.groups(
            "select trim_scale(g::numeric)::text, trim_scale(sum(v))::text, \
             trim_scale(count(*)::numeric)::text from public.src group by g",
        )
        .await
    }

    async fn groups(&self, sql: &str) -> BTreeMap<String, (Option<String>, Option<String>)> {
        self.raw
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .into_iter()
            .map(|row| (row.get(0), (row.get(1), row.get(2))))
            .collect()
    }

    async fn assert_matches_oracle(&self, context: &str) {
        let target = self.target().await;
        let oracle = self.oracle().await;
        assert_eq!(
            target, oracle,
            "{context}: the aggregate (left) must equal a from-scratch GROUP BY over the source \
             (right), as group -> (total, n)"
        );
    }

    async fn finish(self) {
        // Intake's own stream was never read, so its walsender would hold up
        // cluster shutdown waiting for a flush confirmation that never comes.
        self.raw
            .execute(
                "select pg_terminate_backend(active_pid) from pg_replication_slots \
                 where slot_name = $1 and active_pid is not null",
                &[&INTAKE_SLOT],
            )
            .await
            .expect("terminate intake's walsender");
    }
}

/// The bucket a ring row for `key` of [`SRC`] lands in once a batch is split
/// into [`SEG_BUCKETS`], using the ring's own `route` expression.
async fn bucket_of(raw: &Client, key: i32) -> i64 {
    raw.query_one(
        "select (hashtextextended($1 || E'\\x1f' || $2, 0) & 2147483647) % $3",
        &[&SRC, &key.to_string(), &SEG_BUCKETS],
    )
    .await
    .expect("compute bucket")
    .get(0)
}

/// W1, the issue as filed: a forced recompute of group 1 seals into batch k,
/// a source commit C lands in group 1 afterwards, batch k's live read counts
/// C, and then C's own CDC delta seals into batch k+1 and counts it again.
#[tokio::test]
async fn w1_later_batch_double_counts_a_concurrent_commit() {
    let mut h = Harness::start(
        &format!("{SRC_DDL}; insert into public.src values (1, 1, 10)"),
        &[Setup::Transform(AGG)],
        "agg",
    )
    .await;
    h.assert_matches_oracle("after the initial build").await;

    h.append_recomputes(&[1]).await;
    let k = h.seal().await;

    // C: a new key in the recomputed group, committed after batch k sealed.
    h.commit("insert into public.src values (2, 1, 5)").await;

    h.drain(k, "worker-1", 1).await;
    assert_eq!(
        h.target().await,
        h.oracle().await,
        "batch k's forced recompute reads live state, so it already counts C"
    );

    h.feed().await;
    let k1 = h.seal().await;
    h.drain(k1, "worker-1", 1).await;
    h.assert_matches_oracle("C's delta in batch k+1 must not count C a second time")
        .await;

    h.finish().await;
}

/// W1's wider window (the issue's first comment): C commits *before* batch
/// k seals, but intake has not staged it yet, so its CDC still lands after
/// batch k. The window starts at intake's staged position, not at the seal.
#[tokio::test]
async fn w1_later_batch_double_counts_a_commit_made_before_the_seal_but_staged_after() {
    let mut h = Harness::start(
        &format!("{SRC_DDL}; insert into public.src values (1, 1, 10)"),
        &[Setup::Transform(AGG)],
        "agg",
    )
    .await;

    h.append_recomputes(&[1]).await;
    h.commit("insert into public.src values (2, 1, 5)").await;
    let k = h.seal().await;

    h.drain(k, "worker-1", 1).await;
    assert_eq!(
        h.target().await,
        h.oracle().await,
        "batch k's forced recompute reads live state, so it already counts C"
    );

    h.feed().await;
    let k1 = h.seal().await;
    h.drain(k1, "worker-1", 1).await;
    h.assert_matches_oracle(
        "C committed before the seal but staged after it must still be counted once",
    )
    .await;

    h.finish().await;
}

/// W2: C's delta is staged into batch j, and a forced recompute of the same
/// group into a later batch k. Batches are not drained in order, so worker 1
/// applies k first (counting C from live state) and worker 2 then applies j.
#[tokio::test]
async fn w2_earlier_batch_drained_later_double_counts_its_delta() {
    let mut h = Harness::start(
        &format!("{SRC_DDL}; insert into public.src values (1, 1, 10)"),
        &[Setup::Transform(AGG)],
        "agg",
    )
    .await;

    h.commit("insert into public.src values (2, 1, 5)").await;
    h.feed().await;
    let j = h.seal().await;

    h.append_recomputes(&[1]).await;
    let k = h.seal().await;
    assert!(j < k, "C's batch must be the earlier one");

    h.drain(k, "worker-1", 1).await;
    assert_eq!(
        h.target().await,
        h.oracle().await,
        "batch k's forced recompute reads live state, so it already counts C"
    );

    h.drain(j, "worker-2", 1).await;
    h.assert_matches_oracle("batch j's delta, applied after batch k, must not count C again")
        .await;

    h.finish().await;
}

/// W3: one batch, split into buckets. Key A's image-less recompute and key
/// B's image-bearing insert are in the same group but route to different
/// buckets, so two workers each claim one of them. Worker 2's plan never
/// marks the group forced, so it adds B's delta on top of worker 1's live
/// re-derive, which already counted B.
#[tokio::test]
async fn w3_same_batch_different_bucket_double_counts_a_change() {
    let mut h = Harness::start(SRC_DDL, &[Setup::Transform(AGG)], "agg").await;

    // With 8 buckets and `live_workers = 2`, the first claim takes buckets
    // 0-3 and the second takes half of what is left, 4-5. Pick A for the
    // first and B for the second.
    let mut a = None;
    let mut b = None;
    for id in 1..1000 {
        let bucket = bucket_of(&h.raw, id).await;
        if a.is_none() && bucket < 4 {
            a = Some(id);
        } else if b.is_none() && (4..6).contains(&bucket) {
            b = Some(id);
        }
        if a.is_some() && b.is_some() {
            break;
        }
    }
    let (a, b) = (
        a.expect("a key in buckets 0-3"),
        b.expect("a key in buckets 4-5"),
    );

    h.commit(&format!("insert into public.src values ({a}, 1, 10)"))
        .await;
    h.feed().await;
    h.settle().await;
    h.assert_matches_oracle("after A is counted").await;

    // C: B's insert into A's group, staged as CDC.
    h.commit(&format!("insert into public.src values ({b}, 1, 5)"))
        .await;
    h.feed().await;
    // A's recompute, plus enough recomputes of keys that don't exist to
    // split the batch. They re-read nothing, so they touch no group.
    let mut keys = vec![a];
    keys.extend(1_000_000..1_000_000 + MIN_ROWS_TO_SPLIT as i32 + 44);
    h.append_recomputes(&keys).await;
    let s = h.seal().await;

    let bucket_count: i16 = h
        .raw
        .query_one(
            "select bucket_count from segments where seg_seq = $1",
            &[&s],
        )
        .await
        .expect("read bucket_count")
        .get(0);
    assert_eq!(i64::from(bucket_count), SEG_BUCKETS, "the batch must split");
    // The routes actually stored on the ring rows, not just the prediction.
    let ring_slot: i16 = h
        .raw
        .query_one("select ring_slot from segments where seg_seq = $1", &[&s])
        .await
        .expect("read ring_slot")
        .get(0);
    let staged: BTreeMap<String, (String, i64)> = h
        .raw
        .query(
            &format!(
                "select key, op, route % $2 from seg_{ring_slot} \
                 where src_table = $1 and key in ($3, $4)"
            ),
            &[&SRC, &SEG_BUCKETS, &a.to_string(), &b.to_string()],
        )
        .await
        .expect("read staged routes")
        .into_iter()
        .map(|row| (row.get(0), (row.get(1), row.get(2))))
        .collect();
    assert_eq!(
        staged,
        BTreeMap::from([
            (
                a.to_string(),
                ("recompute".to_string(), bucket_of(&h.raw, a).await)
            ),
            (
                b.to_string(),
                ("insert".to_string(), bucket_of(&h.raw, b).await)
            ),
        ]),
        "A's recompute and B's insert must sit in the batch, in different workers' buckets"
    );

    h.drain(s, "worker-1", 2).await;
    assert_eq!(
        h.target().await,
        h.oracle().await,
        "worker 1 holds A's bucket, and its forced recompute already counts B"
    );
    h.drain(s, "worker-2", 2).await;
    // Whatever buckets are left (only fillers) go to a third claim.
    h.settle().await;
    h.assert_matches_oracle("worker 2's delta for B must not count B again")
        .await;

    h.finish().await;
}

/// W4: Phase 2 reads key A live and puts its recompute on group 1. Before
/// Phase 3 runs, a commit moves A to group 2. Phase 3 re-derives group 1
/// from live state, already without A. The move's own CDC delta then
/// subtracts A from group 1 a second time.
///
/// Phase 2 and Phase 3 are split without a hook: the test holds a
/// `FOR UPDATE` lock on group 1's target row. Phase 2 only reads, so it runs
/// through; Phase 3's ascending pre-lock (`apply_aggregate_target`) blocks on
/// it. Once the drain's backend is seen blocked on the test's connection,
/// the test commits the move and releases the lock.
#[tokio::test]
async fn w4_grain_migration_between_phase_2_and_phase_3_double_subtracts_the_old_group() {
    let mut h = Harness::start(
        &format!("{SRC_DDL}; insert into public.src values (1, 1, 10), (3, 1, 7)"),
        &[Setup::Transform(AGG)],
        "agg",
    )
    .await;
    h.assert_matches_oracle("after the initial build").await;

    h.append_recomputes(&[1]).await;
    let k = h.seal().await;

    let holder = connect_raw(h.db.dsn()).await;
    let holder_pid: i32 = holder
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("holder pid")
        .get(0);
    holder
        .batch_execute("begin; select 1 from public.agg where g = 1 for update")
        .await
        .expect("lock group 1's target row");

    let drain = h.drain(k, "worker-1", 1);
    let interleave = async {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let blocked: bool = h
                .raw
                .query_one(
                    "select exists (select 1 from pg_stat_activity \
                     where $1 = any(pg_blocking_pids(pid)))",
                    &[&holder_pid],
                )
                .await
                .expect("poll pg_stat_activity")
                .get(0);
            if blocked {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the drain never blocked on group 1's target row"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        h.commit("update public.src set g = 2 where id = 1").await;
        holder
            .batch_execute("rollback")
            .await
            .expect("release group 1's target row");
    };
    tokio::join!(drain, interleave);
    assert_eq!(
        h.target().await,
        BTreeMap::from([(
            "1".to_string(),
            (Some("7".to_string()), Some("1".to_string()))
        )]),
        "Phase 3 re-derived group 1 after the move committed, so it already lost A"
    );

    h.feed().await;
    h.settle().await;
    h.assert_matches_oracle("group 1 must lose A once, and group 2 must gain it once")
        .await;

    h.finish().await;
}

/// W5: group 5's only row r is deleted. Before that delete's CDC is staged,
/// a delta for group 5 (an earlier update of r) drains, and its existence
/// probe finds the group empty and deletes the group's row, which already
/// accounts for the delete. A new row r' then recreates group 5, and the
/// delete's delta lands on the recreated group, subtracting r a second time.
#[tokio::test]
async fn w5_extinct_then_recreated_group_double_subtracts_the_absorbed_delete() {
    let mut h = Harness::start(
        &format!("{SRC_DDL}; insert into public.src values (1, 5, 10)"),
        &[Setup::Transform(AGG)],
        "agg",
    )
    .await;
    h.assert_matches_oracle("after the initial build").await;

    // A delta-path change for group 5, staged.
    h.commit("update public.src set v = 11 where id = 1").await;
    h.feed().await;
    // r's delete, committed but not yet staged.
    h.commit("delete from public.src where id = 1").await;
    let k = h.seal().await;
    h.drain(k, "worker-1", 1).await;
    assert_eq!(
        h.target().await,
        h.oracle().await,
        "the update's existence probe sees group 5 empty and deletes its row"
    );

    // r' recreates group 5; the delete's CDC stages alongside it.
    h.commit("insert into public.src values (3, 5, 4)").await;
    h.feed().await;
    let k1 = h.seal().await;
    h.drain(k1, "worker-1", 1).await;
    h.assert_matches_oracle("group 5 must be f(r'), not f(r') - f(r)")
        .await;

    h.finish().await;
}

/// Chained W1: `h3` aggregates `h1`, a 1-1 target of `src` that is also a
/// relationship endpoint. A write to `h1` reaches `h3` twice: as the seam's
/// image-less `Recompute` (staged in the apply that wrote `h1`) and as `h1`'s
/// own CDC (staged when intake decodes that apply). The `Recompute` drains
/// first and re-derives the group from live `h1`, which already holds the
/// write; the CDC delta then adds it again. This is W1 with intake's lag
/// behind the apply as the window, which is structural rather than a race.
///
/// # What this pins, and what it leans on
///
/// Two separate things meet here, and only the second is under test:
///
/// 1. **The double feed.** `h1`'s CDC exists only because `h1` is published.
///    Today the engine publishes it itself (#315 keeps a relationship
///    endpoint in [`trellis::defs::publication_tables`]). Once #375's
///    direction 1 lands, the seam is the only feed for every target the
///    instance owns and the engine unpublishes `h1`; the double feed then
///    survives only in direction 1's transition window, where `h1` CDC
///    already in the slot before the `ALTER PUBLICATION ... DROP TABLE`
///    still arrives.
/// 2. **The recompute horizon** (the regression this pins): the
///    `Recompute`'s forced re-derive stamps group 1's horizon after its live
///    read, and `h1`'s CDC delta, whose commit that read already saw, lands
///    at or below it and re-derives instead of adding.
///
/// So the test publishes `h1` itself ([`Setup::Publish`]) instead of relying
/// on the engine's list, and it keeps producing the double feed after
/// direction 1. It also asserts the shape of both feeds as it goes: an
/// image-less `recompute` for `h1` from the seam, then an image-bearing
/// `insert` from CDC. If the seam's row for an aggregate reader becomes
/// image-bearing (#375's caution 3 leaves that open), the first of those
/// fails on purpose. Two image-bearing rows for one write are two deltas with
/// no forced re-derive between them, which no horizon absorbs, so this test
/// would no longer be pinning the horizon and needs revisiting, not
/// loosening.
#[tokio::test]
async fn chained_w1_aggregate_over_a_published_relationship_endpoint_double_counts() {
    let mut h = Harness::start(
        &format!(
            "{SRC_DDL}; \
             create table public.labels (id integer primary key, name text); \
             alter table public.labels replica identity full; \
             insert into public.labels values (1, 'one')"
        ),
        &[
            Setup::Transform("TRANSFORM h1 FROM public.src SELECT g AS g, v AS v"),
            Setup::Sql("alter table public.h1 replica identity full"),
            Setup::Relationship("RELATIONSHIP label FROM h1.g TO labels.id"),
            Setup::Transform(
                "TRANSFORM h3 FROM public.h1 GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n",
            ),
            // The double feed's second half, published by the test rather
            // than left to the engine: see this test's doc comment.
            Setup::Publish("public.h1"),
        ],
        "h3",
    )
    .await;
    // The source starts empty: a seeded row would put `h1` on the chunk-queue
    // build, and `h3` can only be installed over a `live` `h1`.
    //
    // C: a new source row in group 1. Its drain writes `h1` and stages the
    // seam's `Recompute` for `h3` into the next batch.
    h.commit("insert into public.src values (2, 1, 5)").await;
    h.feed().await;
    let k0 = h.seal().await;
    h.drain(k0, "worker-1", 1).await;

    let k = h.seal().await;
    assert_eq!(
        h.staged_ops(k, "public.h1").await,
        ["recompute"],
        "feed 1: the seam stages an image-less recompute of h1 for h3"
    );
    h.drain(k, "worker-1", 1).await;
    assert_eq!(
        h.target().await,
        h.oracle().await,
        "the seam's recompute re-derives group 1 from live h1, which already holds C"
    );

    // Intake now decodes the apply that wrote `h1`, so `h1`'s own CDC stages.
    h.feed().await;
    let k1 = h.seal().await;
    assert_eq!(
        h.staged_ops(k1, "public.h1").await,
        ["insert"],
        "feed 2: h1's own CDC carries the same write as an image-bearing insert"
    );
    h.settle().await;
    h.assert_matches_oracle("h1's CDC delta must not count C a second time")
        .await;

    h.finish().await;
}

/// W5 with the recreating insert drained first: the delete's delta sits in
/// batch j and the insert that recreates group 5 in a later batch k, which a
/// second worker applies first. The insert creates group 5's row through the
/// delta path, so that row has to inherit the extinct horizon; otherwise the
/// delete's delta, applied afterwards, finds a row with no horizon and
/// subtracts r a second time.
#[tokio::test]
async fn w5_a_group_recreated_by_a_delta_inherits_the_extinct_horizon() {
    let mut h = Harness::start(
        &format!("{SRC_DDL}; insert into public.src values (1, 5, 10)"),
        &[Setup::Transform(AGG)],
        "agg",
    )
    .await;

    h.commit("update public.src set v = 11 where id = 1").await;
    h.feed().await;
    h.commit("delete from public.src where id = 1").await;
    let k0 = h.seal().await;
    h.drain(k0, "worker-1", 1).await;
    assert_eq!(
        h.target().await,
        BTreeMap::new(),
        "the update's existence probe sees group 5 empty and deletes its row"
    );

    // The delete's CDC in batch j, r' in a later batch k.
    h.feed().await;
    let j = h.seal().await;
    h.commit("insert into public.src values (3, 5, 4)").await;
    h.feed().await;
    let k = h.seal().await;

    h.drain(k, "worker-1", 1).await;
    h.assert_matches_oracle("r' recreates group 5 as a delta")
        .await;
    h.drain(j, "worker-2", 1).await;
    h.assert_matches_oracle("the delete's delta must not subtract r from the recreated group")
        .await;

    h.finish().await;
}

/// W5 with no row to delete: group 5 has no target row when an insert's
/// delta drains, and the existence probe finds the group already emptied by
/// a delete whose CDC hasn't staged yet. The probe drops the insert's delta,
/// which accounts for the delete, but deletes nothing, since there is no row.
/// The extinct horizon has to rise anyway: once r' recreates group 5, the
/// delete's delta would otherwise land on it and subtract r, which was never
/// added.
#[tokio::test]
async fn w5_an_absorbed_delete_with_no_row_to_remove_still_raises_the_extinct_horizon() {
    let mut h = Harness::start(SRC_DDL, &[Setup::Transform(AGG)], "agg").await;

    // r's insert, staged; r's delete, committed but not yet staged.
    h.commit("insert into public.src values (1, 5, 10)").await;
    h.feed().await;
    h.commit("delete from public.src where id = 1").await;
    let k = h.seal().await;
    h.drain(k, "worker-1", 1).await;
    assert_eq!(
        h.target().await,
        BTreeMap::new(),
        "the insert's existence probe sees group 5 empty, so it creates no row"
    );

    // r' recreates group 5; the delete's CDC stages alongside it.
    h.commit("insert into public.src values (3, 5, 4)").await;
    h.feed().await;
    let k1 = h.seal().await;
    h.drain(k1, "worker-1", 1).await;
    h.assert_matches_oracle("group 5 must be f(r'), not f(r') - f(r)")
        .await;

    h.finish().await;
}

/// W1 with a second commit to C's key after the recompute: both of C's
/// changes fold into one delta whose *latest* commit is above group 1's
/// horizon but whose earliest (C itself) is below it. The delta telescopes
/// both, so it must be judged by its earliest commit and re-derive the
/// group; judging it by its latest would apply it and count C twice.
#[tokio::test]
async fn w1_a_telescoped_delta_is_judged_by_its_earliest_commit() {
    let mut h = Harness::start(
        &format!("{SRC_DDL}; insert into public.src values (1, 1, 10)"),
        &[Setup::Transform(AGG)],
        "agg",
    )
    .await;

    h.append_recomputes(&[1]).await;
    let k = h.seal().await;
    h.commit("insert into public.src values (2, 1, 5)").await;
    h.drain(k, "worker-1", 1).await;
    assert_eq!(
        h.target().await,
        h.oracle().await,
        "batch k's forced recompute reads live state, so it already counts C"
    );

    // After the recompute: not absorbed.
    h.commit("update public.src set v = 7 where id = 2").await;
    h.feed().await;
    let k1 = h.seal().await;
    h.drain(k1, "worker-1", 1).await;
    h.assert_matches_oracle("C's insert-then-update delta must count key 2 once")
        .await;

    h.finish().await;
}

/// Issue #322, closed by the same rule: an aggregate defined through the
/// ring path enumerates its source inline, as image-less recomputes, while a
/// commit C made just before the definition is still unstaged. The
/// enumeration's forced recompute counts C from live state, and C's own CDC
/// then arrives as a delta at or below the group's horizon, so it re-derives
/// the group rather than counting C again. No intake wait is involved.
#[tokio::test]
async fn issue_322_definition_time_enumeration_does_not_double_count_a_pre_define_commit() {
    // A 1-1 over the (empty) source puts it in the publication; the
    // aggregate under test is defined later, through the ring path.
    let mut h = Harness::start(
        SRC_DDL,
        &[Setup::Transform(
            "TRANSFORM mirror FROM public.src SELECT g AS g, v AS v",
        )],
        "agg",
    )
    .await;
    h.commit("insert into public.src values (1, 1, 10)").await;
    h.feed().await;
    h.settle().await;

    // C, committed before the definition and not yet staged.
    h.commit("insert into public.src values (2, 1, 5)").await;
    let columns = numeric_columns(&["id", "g", "v"]);
    trellis::defs::create_definition(&h.db.pool, AGG, &columns)
        .await
        .expect("create the aggregate through the ring path");
    let def = trellis::defs::parse(AGG).expect("parse the aggregate");
    trellis::defs::create_aggregate_target_table(&h.db.pool, &def, "public", &columns)
        .await
        .expect("create the aggregate's target table");
    let k = h.seal().await;
    h.drain(k, "worker-1", 1).await;
    h.assert_matches_oracle("the enumeration's forced recompute already counts C")
        .await;

    h.feed().await;
    h.settle().await;
    h.assert_matches_oracle("C's delta must not count it a second time")
        .await;

    h.finish().await;
}

/// W1 through a relationship: the aggregate sums a to-one parent's column.
/// A forced recompute of group 1 joins the parent live, so it already counts
/// the parent update P. P's own CDC then reaches the aggregate through the
/// relationship reverse fast path, which diffs every from-side row's
/// contribution under P's old and new images. That delta has to be judged
/// against group 1's horizon by P's LSN like any other, or it adds P's
/// change a second time.
#[tokio::test]
async fn w1_a_relationship_reverse_delta_is_judged_against_the_horizon() {
    let mut h = Harness::start(
        &format!(
            "{SRC_DDL}; \
             create table public.parents (id integer primary key, w numeric); \
             alter table public.parents replica identity full; \
             insert into public.parents values (1, 3); \
             insert into public.src values (1, 1, 1)"
        ),
        &[
            Setup::Relationship("RELATIONSHIP parent FROM src.g TO parents.id"),
            Setup::Transform(
                "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(parent.w) AS total, COUNT(*) AS n",
            ),
        ],
        "agg",
    )
    .await;
    let oracle_sql = "select trim_scale(s.g::numeric)::text, trim_scale(sum(p.w))::text, \
                      trim_scale(count(*)::numeric)::text from public.src s \
                      left join public.parents p on p.id = s.g group by s.g";
    assert_eq!(
        h.target().await,
        h.groups(oracle_sql).await,
        "initial build"
    );

    h.append_recomputes(&[1]).await;
    let k = h.seal().await;
    h.commit("update public.parents set w = 4 where id = 1")
        .await;
    h.drain(k, "worker-1", 1).await;
    assert_eq!(
        h.target().await,
        h.groups(oracle_sql).await,
        "the forced recompute reads the parent change live"
    );

    h.feed().await;
    h.settle().await;
    assert_eq!(
        h.target().await,
        h.groups(oracle_sql).await,
        "the parent change's reverse delta must not count it again"
    );
    h.finish().await;
}

/// A group row's recompute horizon (`ddl::RECOMPUTE_LSN_COLUMN`).
async fn horizon_of(raw: &Client, group: i32) -> PgLsn {
    raw.query_one(
        "select __trellis_recompute_lsn from public.agg where g = $1::integer",
        &[&group],
    )
    .await
    .expect("read the group's recompute horizon")
    .get(0)
}

/// One seam-style write, in the shape #375's direction 1 gives the target
/// mutation seam: inside the writer's own transaction, insert the row, read
/// `pg_current_wal_insert_lsn()` as the ordering token *after* the write's
/// row lock, and stage an image-bearing `Cdc` row carrying that token as its
/// `lsn`. The token is below the transaction's commit LSN, unlike a CDC
/// row's `end_lsn`. Returns the token; the caller decides when to commit.
async fn seam_write(txn: &tokio_postgres::Transaction<'_>, id: i32, g: i32, v: i32) -> PgLsn {
    txn.batch_execute(&format!("insert into public.src values ({id}, {g}, {v})"))
        .await
        .expect("seam writer's write");
    let token: PgLsn = txn
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await
        .expect("read the seam token")
        .get(0);
    trellis::staging::append(
        txn,
        &[StagedChange::Cdc {
            src_table: SRC.to_string(),
            key: id.to_string(),
            op: CdcOp::Insert,
            lsn: Some(token),
            old_image: None,
            new_image: Some(format!(r#"{{"id":"{id}","g":"{g}","v":"{v}"}}"#)),
            origin_lsn: None,
            src_changed: None,
            hop_gen: 0,
            group_key: None,
        }],
    )
    .await
    .expect("stage the seam row");
    token
}

/// #375's direction 1 makes the target mutation seam stage CDC-shaped rows
/// whose `lsn` is an ordering token read *before* the writer commits, not a
/// commit `end_lsn`. For the horizon, all that matters is that the token is
/// no later than the writer's commit (reading it after the row locks is what
/// orders one key's rows in the fold, a separate concern):
///
/// - token above the horizon H: the token was read after H, so the writer
///   committed after H, and the live read behind H could not see it. The
///   delta applies.
/// - token at or below H: the writer may or may not have been visible to
///   the read, so the group re-derives, which is right either way.
///
/// Three seam-style writers ([`seam_write`]), one per group, straddle one
/// batch of forced recomputes of groups 1, 2 and 3. The test never feeds
/// intake, so each write's only feed is its own seam row, as for an
/// unpublished target under direction 1:
///
/// - **absorbed** (group 1): token and commit before the recompute's read.
///   The read counts it; its delta must not count it again.
/// - **straddling** (group 2): token before the read, commit after. The
///   read does not see it, yet its token is at or below H. The group must
///   re-derive, not skip: this is the case a pre-commit token makes common.
/// - **after** (group 3): token read after the recompute committed, so above
///   H. It applies as a plain delta, leaving the group's horizon alone.
///
/// Each premise (token against H, what the read saw) is asserted, and each
/// group's horizon afterwards shows which path it took.
#[tokio::test]
async fn a_seam_writer_with_a_pre_commit_token_straddling_a_forced_recompute() {
    let mut h = Harness::start(
        &format!("{SRC_DDL}; insert into public.src values (1, 1, 10), (3, 2, 20), (5, 3, 30)"),
        &[Setup::Transform(AGG)],
        "agg",
    )
    .await;
    h.assert_matches_oracle("after the initial build").await;

    h.append_recomputes(&[1, 3, 5]).await;
    let k = h.seal().await;

    // Absorbed: token and commit both before batch k's read.
    let mut absorbed = connect_raw(h.db.dsn()).await;
    let txn = absorbed.transaction().await.expect("begin absorbed writer");
    let absorbed_token = seam_write(&txn, 2, 1, 5).await;
    txn.commit().await.expect("commit absorbed writer");

    // Straddling: token now, commit only after batch k's read.
    let mut straddling = connect_raw(h.db.dsn()).await;
    let straddling_txn = straddling
        .transaction()
        .await
        .expect("begin straddling writer");
    let straddling_token = seam_write(&straddling_txn, 4, 2, 7).await;

    h.drain(k, "worker-1", 1).await;
    let horizon = [
        horizon_of(&h.raw, 1).await,
        horizon_of(&h.raw, 2).await,
        horizon_of(&h.raw, 3).await,
    ];
    assert_eq!(
        h.target().await,
        BTreeMap::from([
            (
                "1".to_string(),
                (Some("15".to_string()), Some("2".to_string()))
            ),
            (
                "2".to_string(),
                (Some("20".to_string()), Some("1".to_string()))
            ),
            (
                "3".to_string(),
                (Some("30".to_string()), Some("1".to_string()))
            ),
        ]),
        "batch k's live read counts the absorbed writer and not the uncommitted straddling one"
    );
    assert!(
        absorbed_token <= horizon[0],
        "premise: the absorbed writer's token ({absorbed_token}) is at or below group 1's \
         horizon ({})",
        horizon[0]
    );
    assert!(
        straddling_token <= horizon[1],
        "premise: the straddling writer's token ({straddling_token}) is at or below group 2's \
         horizon ({}) although the read did not see it",
        horizon[1]
    );

    straddling_txn
        .commit()
        .await
        .expect("commit straddling writer");

    // After: token read once batch k has committed.
    let mut after = connect_raw(h.db.dsn()).await;
    let txn = after.transaction().await.expect("begin after writer");
    let after_token = seam_write(&txn, 6, 3, 9).await;
    txn.commit().await.expect("commit after writer");
    assert!(
        after_token > horizon[2],
        "premise: the after writer's token ({after_token}) is above group 3's horizon ({})",
        horizon[2]
    );

    let k1 = h.seal().await;
    assert_eq!(
        h.staged_ops(k1, SRC).await,
        ["insert"],
        "the batch holds the seam rows and nothing else for src"
    );
    h.drain(k1, "worker-1", 1).await;
    h.assert_matches_oracle(
        "each seam write counts once: absorbed not twice, straddling not zero times",
    )
    .await;

    assert!(
        horizon_of(&h.raw, 1).await > horizon[0],
        "group 1's delta was at or below its horizon, so the group re-derived"
    );
    assert!(
        horizon_of(&h.raw, 2).await > horizon[1],
        "group 2's delta was at or below its horizon, so the group re-derived rather than \
         skipping the write its read never saw"
    );
    assert_eq!(
        horizon_of(&h.raw, 3).await,
        horizon[2],
        "group 3's delta was above its horizon, so it applied as a delta"
    );

    h.finish().await;
}
