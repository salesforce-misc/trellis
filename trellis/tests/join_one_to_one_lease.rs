//! Two instances and a 1-1 `JOIN` fan-out that outlives its claim (ADR-0021,
//! #1514). A `b` change recomputes every `a` row on its key in one page
//! transaction, up to `TRELLIS_ONE_TO_ONE_JOIN_MAX_FANOUT` rows, holding the
//! key's locks; a fan-out of a million rows can run longer than the claim
//! lease (`liveness::DEFAULT_RECLAIM_TTL`, 30 s) if the worker's heartbeat
//! stalls. Locks give correctness: a second instance that reclaims the
//! bucket queues behind the first's page, the first fails its claim check
//! when it reaches it, and nothing it wrote commits.
//!
//! The scenarios shrink the fan-out to a cap of 25 rows and play the lease by
//! hand: aging a claim and sweeping it is what a stalled worker looks like
//! once the TTL passed (`reclaim_stale`, as `drain_paging.rs` does), and the
//! heartbeat daemon runs for real against a short interval. Nothing sleeps:
//! the only waits are on a concrete condition with a bound.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::{set_enabled, set_max_fanout};
use trellis::integer::IntWidth;
use trellis::staging::ApplyError;
use trellis::staging::interleave::PausePoint;
use trellis::staging::{HeartbeatDaemon, HeartbeatDaemonConfig, liveness};

const DDL: &str = "create table public.a (id integer primary key, k integer, x integer); \
                   create table public.b (id integer primary key, k integer, w integer); \
                   create unique index on public.b (k); \
                   create index on public.a (k)";

/// The fan-out cap these tests run with: key 10 holds exactly this many `a`
/// rows.
const CAP: i64 = 25;

/// The dev knob and the fan-out cap are process-wide, so these tests run one
/// at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Left,
    Inner,
}

const KINDS: [Kind; 2] = [Kind::Left, Kind::Inner];

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Left => "jl",
            Kind::Inner => "ji",
        }
    }

    fn target(self) -> String {
        format!("public.{}", self.name())
    }

    fn word(self) -> &'static str {
        match self {
            Kind::Left => "LEFT",
            Kind::Inner => "INNER",
        }
    }
}

/// `kind` over `CAP` rows of `a` on key 10, one on key 20, and `b` rows on
/// both keys, built and live, with the fan-out cap at `CAP`.
async fn start(kind: Kind) -> Driver {
    set_enabled(true);
    set_max_fanout(CAP as u64);
    let seed = format!(
        "insert into public.a select i, 10, i from generate_series(1, {CAP}) i; \
         insert into public.a values (100, 20, 100); \
         insert into public.b values (1, 10, 100), (2, 20, 200)"
    );
    Driver::start(
        &format!("{DDL}; {seed}"),
        &[("k", int4()), ("x", int4())],
        &[&format!(
            "TRANSFORM {} FROM public.a {} JOIN public.b ON a.k = b.k \
             SELECT a.x + COALESCE(b.w, 0) AS total",
            kind.name(),
            kind.word()
        )],
        &["public.a", "public.b"],
    )
    .await
}

/// The cap is a process-wide setting: every test puts it back.
struct CapGuard;

impl Drop for CapGuard {
    fn drop(&mut self) {
        set_max_fanout(0);
    }
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Asserts `kind`'s target equals the live join, `M` holds `b`'s rows and
/// `self_check` is clean.
async fn assert_exact(d: &mut Driver, kind: Kind, step: &str) {
    d.settle().await;
    let actual = d
        .rows(&format!(
            "select id, total from {} order by id",
            kind.target()
        ))
        .await;
    let expected = d
        .rows(&format!(
            "select a.id, a.x + coalesce(b.w, 0) from public.a a {} join public.b b \
             on a.k = b.k order by a.id",
            kind.word()
        ))
        .await;
    assert_eq!(
        actual, expected,
        "{kind:?} target (left) differs from the live join (right) after {step}"
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &kind.target())
        .await
        .expect("self_check");
    assert!(diverging.is_empty(), "{kind:?} after {step}: {diverging:?}");
    let id: i64 = d
        .ctl
        .query_one(
            "select id from transform_definitions where target_table = $1",
            &[&kind.target()],
        )
        .await
        .expect("the definition")
        .get(0);
    let m = d
        .rows(&format!(
            "select __from_key, k, w from {DEFAULT_SCHEMA}.join1_{id}_m \
             where not __tombstone order by 1"
        ))
        .await;
    let b = d
        .rows("select id::text, k, w from public.b order by 1")
        .await;
    assert_eq!(m, b, "{kind:?} M (left) differs from b after {step}");
    let status: String = d
        .ctl
        .query_one(
            "select status::text from transform_definitions where target_table = $1",
            &[&kind.target()],
        )
        .await
        .expect("the definition")
        .get(0);
    assert_eq!(status, "live", "{kind:?} after {step}");
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
}

/// Ages `seg_seq`'s claims past the TTL and sweeps them, as the maintenance
/// loop does for a worker whose heartbeat stalled.
async fn expire_claims(d: &Driver, seg_seq: i64) -> u64 {
    d.ctl
        .execute(
            "update seg_claims set claimed_at = now() - interval '1 hour' where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("age the claims");
    liveness::reclaim_stale(&d.ctl, Duration::from_secs(30))
        .await
        .expect("reclaim stale claims")
}

/// The transaction id a backend writes under.
async fn backend_xid(d: &Driver, pid: i32) -> String {
    d.ctl
        .query_one(
            "select backend_xid::text from pg_stat_activity where pid = $1",
            &[&pid],
        )
        .await
        .expect("read pg_stat_activity")
        .get::<_, Option<String>>(0)
        .expect("the frozen page has written, so it holds an xid")
}

/// A fan-out of exactly the cap whose worker loses its claim to the TTL
/// sweep while the page is frozen before the fan-out, holding the key's
/// locks. The second instance claims the freed bucket and queues behind the
/// page's locks; when the first goes on it writes its 25 rows, fails its
/// claim check and rolls them back (`ClaimLost`), and the second applies the
/// change once. No row carries the first page's transaction, so none of its
/// writes committed, and the target is exact.
#[tokio::test]
async fn a_fan_out_that_outlives_its_claim_is_applied_once_by_the_next_instance() {
    let _serial = SERIAL.lock().await;
    let _cap = CapGuard;
    for kind in KINDS {
        let mut d = start(kind).await;
        write(&d, "update public.b set w = 500 where id = 1").await;
        let batch = d.seal().await;
        let target = kind.target();
        let mut first = d
            .drain_frozen(batch, "w1", &[(PausePoint::BeforeJoin1Recompute, &target)])
            .await;
        let reached = first.reached(PausePoint::BeforeJoin1Recompute).await;
        let first_xid = backend_xid(&d, reached.backend_pid).await;
        let held: i64 = d
            .ctl
            .query_one(
                "select count(*) from seg_claims where seg_seq = $1 and claimed_by = 'w1'",
                &[&batch],
            )
            .await
            .expect("count the claims")
            .get(0);
        assert_eq!(held, 1, "{kind:?}: the first worker holds the batch");

        assert!(
            expire_claims(&d, batch).await > 0,
            "{kind:?}: a claim not held under a row lock is swept once it is stale"
        );
        let second = d.drain_frozen(batch, "w2", &[]).await;
        d.wait_blocked_behind(reached.backend_pid).await;
        d.release(&mut first, PausePoint::BeforeJoin1Recompute)
            .await;
        let lost = first.finish_result().await;
        assert!(
            matches!(lost, Err(ApplyError::ClaimLost)),
            "{kind:?}: the stale worker's page must fail its claim check: {lost:?}"
        );
        second.finish().await;

        let from_the_first: i64 = d
            .ctl
            .query_one(
                &format!("select count(*) from {target} where xmin::text = $1"),
                &[&first_xid],
            )
            .await
            .expect("count the rows the first page wrote")
            .get(0);
        assert_eq!(
            from_the_first, 0,
            "{kind:?}: nothing the first page wrote committed"
        );
        let written_by: Vec<String> = d
            .rows(&format!(
                "select distinct xmin::text from {target} where id <= {CAP} order by 1"
            ))
            .await;
        assert_eq!(
            written_by.len(),
            1,
            "{kind:?}: the {CAP} rows of the fan-out were written by one transaction: {written_by:?}"
        );
        assert_exact(&mut d, kind, "the second instance's apply").await;
        let totals = d
            .rows(&format!(
                "select count(*) from {target} where id <= {CAP} and total = id + 500"
            ))
            .await;
        assert_eq!(
            totals,
            [format!("({CAP})")],
            "{kind:?}: each row has x + 500"
        );
    }
}

/// The same frozen fan-out with its worker's heartbeat alive: the daemon
/// refreshes the claim of a page that holds no row lock on it, so the sweep
/// finds nothing stale and the second instance claims nothing (it returns,
/// leaving the batch to the first). The first then finishes the fan-out and
/// the target is exact. The claim's age is set back an hour first, so the
/// daemon's refresh is the one thing that can bring it current, and the test
/// waits (bounded) for exactly that.
#[tokio::test]
async fn a_fan_out_whose_heartbeat_is_alive_keeps_its_claim_from_the_next_instance() {
    let _serial = SERIAL.lock().await;
    let _cap = CapGuard;
    for kind in KINDS {
        let mut d = start(kind).await;
        write(&d, "update public.b set w = 500 where id = 1").await;
        let batch = d.seal().await;
        let target = kind.target();
        let mut first = d
            .drain_frozen(batch, "w1", &[(PausePoint::BeforeJoin1Recompute, &target)])
            .await;
        first.reached(PausePoint::BeforeJoin1Recompute).await;
        d.ctl
            .execute(
                "update seg_claims set claimed_at = now() - interval '1 hour' \
                 where seg_seq = $1",
                &[&batch],
            )
            .await
            .expect("age the claim");
        let daemon = HeartbeatDaemon::spawn(
            d.db.dsn().to_string(),
            DEFAULT_SCHEMA,
            HeartbeatDaemonConfig {
                interval: Duration::from_millis(50),
                idle_timeout: Duration::from_secs(60),
            },
        );
        daemon.register(batch, "w1").await;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let fresh: bool = d
                .ctl
                .query_one(
                    "select bool_and(claimed_at > now() - interval '1 minute') \
                     from seg_claims where seg_seq = $1 and claimed_by = 'w1'",
                    &[&batch],
                )
                .await
                .expect("read the claim")
                .get(0);
            if fresh {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{kind:?}: the daemon never refreshed the frozen page's claim"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            liveness::reclaim_stale(&d.ctl, Duration::from_secs(30))
                .await
                .expect("reclaim stale claims"),
            0,
            "{kind:?}: a claim the heartbeat keeps is not stale"
        );
        let second = d.drain_frozen(batch, "w2", &[]).await;
        assert!(
            second.finish().await.is_none(),
            "{kind:?}: the second instance has no bucket to claim"
        );

        d.release(&mut first, PausePoint::BeforeJoin1Recompute)
            .await;
        let outcome = first.finish().await;
        assert!(
            outcome.is_some_and(|outcome| outcome.batch_drained),
            "{kind:?}: the first instance finishes the batch"
        );
        daemon.deregister(batch, "w1").await;
        assert_exact(&mut d, kind, "the first instance's apply").await;
        let totals = d
            .rows(&format!(
                "select count(*) from {target} where id <= {CAP} and total = id + 500"
            ))
            .await;
        assert_eq!(
            totals,
            [format!("({CAP})")],
            "{kind:?}: each row has x + 500"
        );
    }
}
