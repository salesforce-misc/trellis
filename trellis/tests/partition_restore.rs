//! ADR-0017 P8 (issue #1067, decided on #1052): a dump and restore of a
//! partitioned source's recorded state pauses every definition that reads
//! it, and nothing is routed from the recorded state.
//!
//! A restore is faked deterministically: the stored oid of
//! `partition_sources` is changed under the same `system_identifier` (a dump
//! restored into the same cluster), or the identifier is changed and every
//! recorded node oid moved (a restore into another cluster). Each pass, the
//! correction engine and the merger are called by hand (#297).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::capture::partitions;
use trellis::capture::reconcile::PassOutcome;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::staging::build;
use trellis::staging::partition_correction::{self, Options};

const SOURCE: &str = "public.items";
const PLAIN: &str = "public.plain";

/// `items` hash-partitioned on `g` into four leaves, and a plain table
/// `plain` beside it; 160 rows in each.
const DDL: &str = "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by hash (g); \
     create table public.items_p0 partition of public.items for values with (modulus 4, remainder 0); \
     create table public.items_p1 partition of public.items for values with (modulus 4, remainder 1); \
     create table public.items_p2 partition of public.items for values with (modulus 4, remainder 2); \
     create table public.items_p3 partition of public.items for values with (modulus 4, remainder 3); \
     insert into public.items select i, i % 16, i from generate_series(1, 160) i; \
     create table public.plain (id integer primary key, g integer not null, v integer not null); \
     insert into public.plain select i, i % 16, i from generate_series(1, 160) i;";

struct Fx {
    d: Driver,
}

impl Fx {
    async fn start() -> Fx {
        let d = Driver::start(
            DDL,
            &[
                ("id", ValueType::Numeric),
                ("g", ValueType::Numeric),
                ("v", ValueType::Numeric),
            ],
            &[
                "TRANSFORM agg FROM public.items GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n",
                "TRANSFORM plain_agg FROM public.plain GROUP BY g SELECT SUM(v) AS total, \
                 COUNT(*) AS n",
            ],
            &[SOURCE, PLAIN],
        )
        .await;
        let mut fx = Fx { d };
        fx.pass().await;
        assert_eq!(fx.queued().await, 0);
        assert!(fx.wrong_groups("items", "agg").await.is_empty());
        fx
    }

    async fn pass(&mut self) -> PassOutcome {
        let desired = trellis::defs::tables_to_capture(self.d.pool())
            .await
            .expect("tables to capture");
        let outcome = trellis::capture::reconcile::reconcile(
            &mut self.d.ctl,
            DEFAULT_SCHEMA,
            &desired,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("pass");
        assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
        outcome
    }

    async fn full_pass(&mut self) {
        let pool = self.d.pool().clone();
        trellis::client::reconcile_pass(
            &mut self.d.ctl,
            &pool,
            DEFAULT_SCHEMA,
            drain_driver::WAKE,
            Duration::from_secs(2),
        )
        .await
        .expect("reconcile pass");
    }

    async fn settle_builds(&mut self) {
        self.d.settle().await;
        build::settle_builds(self.d.pool()).await;
    }

    /// Every row of the queue, whatever the engine may see of it.
    async fn queued(&self) -> i64 {
        self.d
            .ctl
            .query_one(
                "select count(*) from partition_corrections where table_name = $1",
                &[&SOURCE],
            )
            .await
            .expect("the queue")
            .get(0)
    }

    async fn status(&self, target: &str) -> String {
        self.d
            .ctl
            .query_one(
                "select status from transform_definitions \
                 where split_part(target_table, '.', 2) = $1",
                &[&target],
            )
            .await
            .expect("status")
            .get(0)
    }

    async fn capture_failure(&self, target: &str) -> Option<String> {
        self.d
            .ctl
            .query_opt(
                "select f.error from capture_failures f \
                 join transform_definitions d on d.id = f.transform_id \
                 where split_part(d.target_table, '.', 2) = $1",
                &[&target],
            )
            .await
            .expect("capture failure")
            .and_then(|r| r.get(0))
    }

    async fn recorded(&self) -> bool {
        partitions::recorded(&self.d.ctl, SOURCE)
            .await
            .expect("recorded")
            .is_some()
    }

    /// The groups of `target` that differ from a `GROUP BY` over `source`.
    async fn wrong_groups(&self, source: &str, target: &str) -> Vec<String> {
        self.d
            .ctl
            .query(
                &format!(
                    "with want as (select g, sum(v) as total, count(*) as n \
                                   from public.{source} group by g), \
                          have as (select g, total, n from public.{target}) \
                     select coalesce(w.g, h.g)::text from want w full join have h on h.g = w.g \
                     where w.total is distinct from h.total or w.n is distinct from h.n \
                     order by 1"
                ),
                &[],
            )
            .await
            .expect("oracle")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    async fn exec(&self, sql: &str) {
        self.d
            .ctl
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// Drops a leaf and lets the pass queue its correction, which the engine
    /// doesn't run: the state a dump taken now would hold.
    async fn queue_a_correction(&mut self) {
        self.exec("drop table public.items_p1").await;
        self.pass().await;
        self.d.settle().await;
        assert_eq!(self.queued().await, 1);
        assert!(!self.wrong_groups("items", "agg").await.is_empty());
    }

    /// The restore's response, and a resume that rebuilds to the oracle.
    async fn assert_restore_pauses_then_resume_rebuilds(&mut self) {
        // The engine runs nothing queued from the restored state, even before
        // a pass has looked.
        let outcome = partition_correction::run_pending(self.d.pool(), &Options::default())
            .await
            .expect("run the pending corrections");
        assert_eq!(outcome.completed, 0);
        assert_eq!(outcome.keys, 0);
        assert!(
            partitions::pending_corrections(&self.d.ctl, SOURCE)
                .await
                .expect("the queue")
                .is_empty(),
            "a restored source's corrections are hidden from the engine"
        );

        let outcome = self.pass().await;
        assert_eq!(outcome.partition_deep_checks, [SOURCE]);
        assert!(outcome.partition_sweeps.is_empty(), "no sweep on a restore");
        assert_eq!(self.status("agg").await, "paused");
        let failure = self
            .capture_failure("agg")
            .await
            .expect("a capture failure");
        assert!(
            failure.contains("restored from a logical dump"),
            "{failure}"
        );
        assert_eq!(self.queued().await, 0, "the queued correction is gone");
        // The restored record is forgotten: what the same pass recorded since
        // (a restore the catalog oid shows is answered before the pass looks
        // at the source) is the live tree's.
        let foreign: i64 = self
            .d
            .ctl
            .query_one(
                "select count(*) from partition_sources \
                 where catalog_oid <> 'partition_sources'::regclass::oid \
                    or system_identifier <> \
                       (select system_identifier::text from pg_control_system())",
                &[],
            )
            .await
            .expect("count the foreign records")
            .get(0);
        assert_eq!(foreign, 0, "the restored record is forgotten");
        // Every definition of the database pauses, whatever its source.
        assert_eq!(self.status("plain_agg").await, "paused");
        assert!(self.capture_failure("plain_agg").await.is_some());

        // The next pass records the live tree as a first sight and queues
        // nothing; the definition stays paused.
        self.pass().await;
        assert!(self.recorded().await);
        assert_eq!(self.queued().await, 0);
        assert_eq!(self.status("agg").await, "paused");
        let outcome = partition_correction::run_pending(self.d.pool(), &Options::default())
            .await
            .expect("run the pending corrections");
        assert_eq!(outcome.keys, 0);

        for target in ["agg", "plain_agg"] {
            trellis::staging::quarantine::resume_transform(self.d.pool(), target)
                .await
                .expect("resume");
        }
        for _ in 0..3 {
            self.full_pass().await;
            self.settle_builds().await;
        }
        assert_eq!(self.status("agg").await, "live");
        assert_eq!(self.status("plain_agg").await, "live");
        assert_eq!(self.capture_failure("agg").await, None);
        assert_eq!(
            self.wrong_groups("items", "agg").await,
            Vec::<String>::new()
        );
        // Recorded afresh, so nothing pauses it again.
        let outcome = self.pass().await;
        assert!(outcome.partition_deep_checks.is_empty());
        assert_eq!(self.status("agg").await, "live");
        assert_eq!(
            self.wrong_groups("plain", "plain_agg").await,
            Vec::<String>::new()
        );
    }
}

/// #1052: a dump restored into the same cluster keeps the
/// `system_identifier` but recreates `partition_sources`, so the stored
/// catalog oid no longer names it.
#[tokio::test]
async fn a_changed_catalog_oid_under_the_same_identifier_is_a_restore() {
    let mut fx = Fx::start().await;
    let live: i64 =
        fx.d.ctl
            .query_one(
                "select catalog_oid::bigint from partition_sources where table_name = $1 \
               and catalog_oid = 'partition_sources'::regclass",
                &[&SOURCE],
            )
            .await
            .expect("the catalog oid is recorded")
            .get(0);
    assert!(live > 0);
    fx.queue_a_correction().await;
    fx.exec("update partition_sources set catalog_oid = 1")
        .await;
    fx.assert_restore_pauses_then_resume_rebuilds().await;
}

/// A correction queued before a `pg_upgrade` is valid: the engine doesn't see
/// it while the stored identifier differs, the pass classifies the change as
/// `pg_upgrade` and records the new identifier, and from then on the
/// correction runs to the oracle without pausing anything.
#[tokio::test]
async fn a_correction_queued_before_a_pg_upgrade_runs_once_the_pass_rerecords() {
    let mut fx = Fx::start().await;
    fx.queue_a_correction().await;
    fx.exec(
        "update partition_sources set system_identifier = '1', nodes = jsonb_set(nodes, \
             '{nodes}', (select jsonb_agg(case when n ? 'inherits_xmin' \
                 and n->'inherits_xmin' <> 'null'::jsonb \
                 then jsonb_set(n, '{inherits_xmin}', '\"1\"') else n end) \
             from jsonb_array_elements(nodes->'nodes') n))",
    )
    .await;
    assert!(
        partitions::pending_corrections(&fx.d.ctl, SOURCE)
            .await
            .expect("the queue")
            .is_empty(),
        "hidden until the pass has classified the change"
    );
    let outcome = fx.pass().await;
    assert_eq!(outcome.partition_deep_checks, [SOURCE]);
    assert_eq!(fx.queued().await, 1, "the upgrade queues nothing new");
    assert_eq!(
        partitions::pending_corrections(&fx.d.ctl, SOURCE)
            .await
            .expect("the queue")
            .len(),
        1,
        "visible once the pass recorded the live identifier"
    );
    for _ in 0..3 {
        fx.full_pass().await;
        fx.settle_builds().await;
    }
    assert_eq!(fx.status("agg").await, "live");
    assert_eq!(fx.capture_failure("agg").await, None);
    assert_eq!(fx.queued().await, 0, "the correction ran");
    assert_eq!(fx.wrong_groups("items", "agg").await, Vec::<String>::new());
}

/// A restore into another cluster: a new identifier, and the recorded oids
/// name nothing live.
#[tokio::test]
async fn a_changed_identifier_with_new_oids_is_a_restore() {
    let mut fx = Fx::start().await;
    fx.queue_a_correction().await;
    fx.exec(
        "update partition_sources set system_identifier = '1', nodes = jsonb_set(nodes, \
             '{nodes}', (select jsonb_agg(jsonb_set(n, '{oid}', \
                 to_jsonb((n->>'oid')::bigint + 100000))) \
             from jsonb_array_elements(nodes->'nodes') n))",
    )
    .await;
    fx.assert_restore_pauses_then_resume_rebuilds().await;
}

/// A root dropped and recreated, or swapped in by renames, under the same
/// identifier and catalog oid is no restore: it is table maintenance (#1200),
/// so the pass pauses the readers with the changed-outside-Trellis wording
/// and corrects nothing.
#[tokio::test]
async fn a_recreated_or_swapped_root_is_no_restore() {
    for swap in [false, true] {
        let mut fx = Fx::start().await;
        let replace = "create table public.items_new (id integer not null, \
                 g integer not null, v integer not null, primary key (g, id)) \
                 partition by hash (g); \
             create table public.items_new0 partition of public.items_new \
                 for values with (modulus 2, remainder 0); \
             create table public.items_new1 partition of public.items_new \
                 for values with (modulus 2, remainder 1); \
             insert into public.items_new select i, i % 16, i from generate_series(1, 40) i;";
        if swap {
            fx.exec(&format!(
                "{replace} alter table public.items rename to items_old; \
                 alter table public.items_new rename to items;"
            ))
            .await;
        } else {
            fx.exec(&format!(
                "drop table public.items; {replace} \
                 alter table public.items_new rename to items;"
            ))
            .await;
        }
        fx.pass().await;
        assert_eq!(fx.status("agg").await, "paused", "swap {swap}");
        let failure = fx.capture_failure("agg").await.expect("a reason");
        assert!(
            failure.contains("not the one Trellis captured") && !failure.contains("restored"),
            "swap {swap}: {failure}"
        );
        assert_eq!(fx.queued().await, 0, "swap {swap}: nothing is corrected");
    }
}
