//! Deterministic interleavings of the drain on `JOIN` targets (#1108 J3,
//! ADR-0019), ported from #1091 J1's model scenarios and its review's RV
//! cases.
//!
//! Every test turns the group-basis dev knob on before its driver starts:
//! a `JOIN` applies only behind it. Rows arrive through capture, except the
//! tests of the build at the end of the file, which define over tables that
//! hold rows.
//!
//! Each scenario writes its source changes, seals them into batches, and
//! drives the pages by hand (`support/drain_driver.rs`): one page frozen at a
//! [`PausePoint`] of its `JOIN` apply while another runs, which either
//! queues behind the frozen page (asserted with `pg_blocking_pids`) or runs
//! through. Nothing sleeps or polls for convergence (#297). After each
//! scenario the pipeline settles and every target equals its `JOIN …
//! GROUP BY` query, and the hidden structures their invariants: `P` and `M`
//! equal each side's `GROUP BY`, `J.matches = Σ M.n`, and `T = Σ_k P ⊗ M`.
//!
//! Scenarios of one family share a database: each uses its own join key,
//! and its own `ga`/`gb` values where it creates groups, so a scenario's
//! pages share locks only with each other. The families run on a
//! single-column key (`a.k = b.k`) and on the two-column `(t, k)` key
//! (`a.t = b.t AND a.k = b.k`), where decoy rows share a scenario's `k` but
//! not its `t`; on the single-column key the decoys sit on a key of their
//! own, so they never keep a scenario's key matched.
//!
//! A test is named for the J1 family it ports:
//!
//! - 1, a fact write against a dimension row moving groups: `family1_…`.
//! - 2, matched/unmatched flips and a key's first sight: `family2_…`.
//! - 3, two dimension pages on one key: `family3_…`.
//! - 4 and 5, duplicate dimension rows and `NULL` keys and values:
//!   `families4_5_…` and `a_null_key_takes_no_j_row_or_lock`.
//! - 6, key changes, sorted `J` locks and deadlock probes: `family6_…`.
//! - 7, out-of-order pages: `family7_out_of_order_pages`.
//! - 8, a `P` or `M` cell's recompute against a page of the other side:
//!   `family8_…`. The recompute is an image-less change's page; a held
//!   key's release is the same recompute in a transaction of its own
//!   (`a_release_takes_the_j_lock_against_the_other_sides_pages`).
//! - 9, chunked builds of `P` and `M` under writes: `family9_…`. The engine
//!   builds both sides in one transaction, in a fixed order, or per side and
//!   leaf when a side is partitioned, so J1's permuted chunk orders and its
//!   chunk-against-chunk races have no counterpart. `family9_…` races pages
//!   of both sides against a build frozen at each point, a build against a
//!   page frozen at each point, and recomputes between two steps of a
//!   build; the tests from "the build" on cover one side's writes
//!   (`build_pages_around_a_build_…`), the lock statements and the steps of
//!   a build with a partitioned side.
//! - 10, `TRUNCATE` of either side: `family10_…`.
//! - 11, savepoints, rolled-back sources and two-phase commit:
//!   `family11_…` and `a_j_row_inserted_by_a_rolled_back_page_leaves_nothing`.
//! - 12, mixed pages: `family12_…`.
//! - 13, tombstone collection before stale pages drain: `family13_…`.
//! - 14, `T` in the page's own transaction: `family14_…`.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::{Driver, RunningDrain};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::interleave::PausePoint;
use trellis::{JoinBuildStep, JoinSide, JoinSidePart};

const DDL: &str = "create table public.a (id integer primary key, ga integer, t integer, \
                       k integer, x integer); \
                   create table public.b (id integer primary key, t integer, k integer, \
                       gb integer, y integer); \
                   create index on public.a (t, k); create index on public.a (k); \
                   create index on public.b (t, k); create index on public.b (k)";

/// The points of a page's `JOIN` apply, in order.
const POINTS: [PausePoint; 7] = [
    PausePoint::BeforeJoinLock,
    PausePoint::AfterJoinKeyInsert,
    PausePoint::AfterJoinLock,
    PausePoint::AfterEntryLock,
    PausePoint::AfterJoinTargetLock,
    PausePoint::AfterGroupUpsert,
    PausePoint::BeforeCommit,
];

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

/// The join key: `a.k = b.k`, or the pair `(t, k)`.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Key {
    Single,
    Pair,
}

impl Key {
    fn on(self) -> &'static str {
        match self {
            Key::Single => "a.k = b.k",
            Key::Pair => "a.t = b.t AND a.k = b.k",
        }
    }

    fn oracle_on(self) -> &'static str {
        match self {
            Key::Single => "a.k = b.k",
            Key::Pair => "a.t = b.t and a.k = b.k",
        }
    }

    /// The hidden tables' join-key columns, in `ON` order.
    fn columns(self) -> &'static [&'static str] {
        match self {
            Key::Single => &["k"],
            Key::Pair => &["t", "k"],
        }
    }
}

/// One `JOIN` target: J1's six kinds.
#[derive(Debug, Clone, Copy)]
struct Kind {
    name: &'static str,
    left: bool,
    ga: bool,
    gb: bool,
}

const L: Kind = Kind {
    name: "l",
    left: true,
    ga: true,
    gb: true,
};
const I: Kind = Kind {
    name: "i",
    left: false,
    ga: true,
    gb: true,
};
const LB: Kind = Kind {
    name: "lb",
    left: true,
    ga: false,
    gb: true,
};
const IB: Kind = Kind {
    name: "ib",
    left: false,
    ga: false,
    gb: true,
};
const LA: Kind = Kind {
    name: "la",
    left: true,
    ga: true,
    gb: false,
};
const IA: Kind = Kind {
    name: "ia",
    left: false,
    ga: true,
    gb: false,
};
const SIX: [Kind; 6] = [L, I, LB, IB, LA, IA];

impl Kind {
    fn groups(self) -> Vec<&'static str> {
        let mut groups = Vec::new();
        if self.ga {
            groups.push("ga");
        }
        if self.gb {
            groups.push("gb");
        }
        groups
    }

    fn qualified_groups(self) -> Vec<String> {
        self.groups()
            .iter()
            .map(|g| format!("{}.{g}", if *g == "ga" { "a" } else { "b" }))
            .collect()
    }

    fn definition(self, key: Key) -> String {
        let qualified = self.qualified_groups();
        let fields: Vec<String> = qualified
            .iter()
            .zip(self.groups())
            .map(|(q, g)| format!("{q} AS {g}"))
            .collect();
        format!(
            "TRANSFORM {} FROM public.a {} public.b ON {} GROUP BY {} \
            WITHOUT SOURCE INDEX SELECT {}, COUNT(*) AS n, COUNT(a.x) AS cx, SUM(a.x) AS sx, \
             COUNT(b.y) AS cy, SUM(b.y) AS sy",
            self.name,
            if self.left { "LEFT JOIN" } else { "JOIN" },
            key.on(),
            qualified.join(", "),
            fields.join(", ")
        )
    }

    fn actual(self) -> String {
        let groups = self.groups().join(", ");
        format!(
            "select {groups}, n, cx, sx, cy, sy from public.{} order by {groups}",
            self.name
        )
    }

    fn expected(self, key: Key) -> String {
        let groups = self.qualified_groups().join(", ");
        format!(
            "select {groups}, count(*), count(a.x), sum(a.x), count(b.y), sum(b.y) \
             from public.a {} public.b on {} group by {groups} order by {groups}",
            if self.left { "left join" } else { "join" },
            key.oracle_on()
        )
    }

    fn target(self) -> String {
        format!("public.{}", self.name)
    }
}

/// A driver with `kinds` defined over the empty `a` and `b`.
struct Lab {
    d: Driver,
    key: Key,
    kinds: Vec<Kind>,
}

async fn lab(key: Key, kinds: &[Kind]) -> Lab {
    lab_with_defaults(key, kinds, &[]).await
}

async fn lab_with_defaults(key: Key, kinds: &[Kind], defaults: &[(&str, &str)]) -> Lab {
    trellis::dev::staging::set_group_basis(true);
    let definitions: Vec<String> = kinds.iter().map(|k| k.definition(key)).collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let d = Driver::start_with_database_defaults(
        defaults,
        DDL,
        &[
            ("id", int4()),
            ("ga", int4()),
            ("t", int4()),
            ("k", int4()),
            ("x", int4()),
        ],
        &[],
        &definitions,
        &["public.a", "public.b"],
    )
    .await;
    Lab {
        d,
        key,
        kinds: kinds.to_vec(),
    }
}

impl Lab {
    async fn write(&self, sql: &str) {
        self.d.user().await.batch_execute(sql).await.expect(sql);
    }

    /// Writes `sql` and drains it as one sealed batch.
    async fn step(&mut self, sql: &str) {
        self.write(sql).await;
        let batch = self.d.seal().await;
        self.d.drain(batch, "step").await;
    }

    /// Writes `sql` and seals it into its own batch, undrained.
    async fn sealed(&mut self, sql: &str) -> i64 {
        self.write(sql).await;
        self.d.seal().await
    }

    async fn scalar(&self, sql: &str) -> String {
        let rows = self.d.rows(sql).await;
        assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
        rows[0].trim_matches(|c| c == '(' || c == ')').to_string()
    }

    /// Definition `kind`'s hidden table `part` (`p`, `m`, `j`, `t`),
    /// qualified.
    async fn hidden(&self, kind: Kind, part: &str) -> String {
        let id = self
            .scalar(&format!(
                "select id from transform_definitions where target_table = 'public.{}'",
                kind.name
            ))
            .await;
        format!("{DEFAULT_SCHEMA}.join_{id}_{part}")
    }

    /// The invariants that hold after every committed page (J1's `I_T` and
    /// `J` checks), on each target: `J.matches = Σ M.n`, every cell's key
    /// has a `J` row, and `T`'s hidden partials equal `Σ_k P ⊗ M` over the
    /// applied `P`, `M` and `J`.
    async fn check_applied(&self, context: &str) {
        let keys = self.key.columns();
        let eq = |x: &str, y: &str| -> String {
            keys.iter()
                .map(|c| format!("{x}.{c} = {y}.{c}"))
                .collect::<Vec<_>>()
                .join(" and ")
        };
        let any_null = |x: &str| -> String {
            keys.iter()
                .map(|c| format!("{x}.{c} is null"))
                .collect::<Vec<_>>()
                .join(" or ")
        };
        for kind in &self.kinds {
            let (p, m, j, t) = (
                self.hidden(*kind, "p").await,
                self.hidden(*kind, "m").await,
                self.hidden(*kind, "j").await,
                self.hidden(*kind, "t").await,
            );
            let matches = format!(
                "select q::text from {j} q \
                 where q.__matches <> (select coalesce(sum(__n), 0) from {m} m where {}) \
                 union all select m::text from {m} m \
                 where m.__n <> 0 and not exists (select 1 from {j} q where {}) \
                 union all select p::text from {p} p \
                 where not ({}) and not exists (select 1 from {j} q where {})",
                eq("m", "q"),
                eq("m", "q"),
                any_null("p"),
                eq("p", "q"),
            );
            assert_eq!(
                self.d.rows(&matches).await,
                Vec::<String>::new(),
                "{context}: {}'s J rows differ from Σ M.n, or a cell has no J row",
                kind.name
            );
            let groups = kind.groups();
            let matched: Vec<String> = groups
                .iter()
                .map(|g| {
                    if *g == "ga" {
                        "p.ga".to_string()
                    } else {
                        "m.gb".to_string()
                    }
                })
                .collect();
            let unmatched: Vec<String> = groups
                .iter()
                .map(|g| {
                    if *g == "ga" {
                        "p.ga".to_string()
                    } else {
                        "null::integer".to_string()
                    }
                })
                .collect();
            let mut df = format!(
                "select {}, p.__n * m.__n as n, p.__count_1 * m.__n as cx, \
                        p.__sum_1 * m.__n as sx, p.__n * m.__count_1 as cy, \
                        p.__n * m.__sum_1 as sy \
                 from {p} p join {m} m on {}",
                matched.join(", "),
                eq("p", "m")
            );
            if kind.left {
                df.push_str(&format!(
                    " union all select {}, p.__n, p.__count_1, p.__sum_1, 0, 0 \
                     from {p} p left join {j} q on {} \
                     where ({}) or coalesce(q.__matches, 0) = 0",
                    unmatched.join(", "),
                    eq("q", "p"),
                    any_null("p")
                ));
            }
            let g: Vec<String> = groups.iter().map(|g| (*g).to_string()).collect();
            let g = g.join(", ");
            let sigma = format!(
                "select {g}, sum(n), sum(cx), sum(sx), sum(cy), sum(sy) from ({df}) df \
                 group by {g} \
                 having not (sum(n) = 0 and sum(cx) = 0 and sum(sx) = 0 and sum(cy) = 0 \
                             and sum(sy) = 0) \
                 order by {g}"
            );
            let hidden_t = format!(
                "select {g}, __n, __count_1, __sum_1, __count_2, __sum_2 from {t} \
                 where not (__n = 0 and __count_1 = 0 and __sum_1 = 0 and __count_2 = 0 \
                            and __sum_2 = 0) \
                 order by {g}"
            );
            assert_eq!(
                self.d.rows(&hidden_t).await,
                self.d.rows(&sigma).await,
                "{context}: {}'s T (left) differs from Σ P ⊗ M (right)",
                kind.name
            );
        }
    }

    /// Settles the pipeline, then [`Self::check_applied`], every target
    /// against its `JOIN … GROUP BY`, and `P`/`M` against each side's
    /// `GROUP BY`.
    async fn check(&mut self, context: &str) {
        self.d.settle().await;
        self.check_applied(context).await;
        let keys = self.key.columns().join(", ");
        for kind in &self.kinds {
            assert_eq!(
                self.d.rows(&kind.actual()).await,
                self.d.rows(&kind.expected(self.key)).await,
                "{context}: target {} (left) differs from its JOIN (right)",
                kind.name
            );
            let p = self.hidden(*kind, "p").await;
            let m = self.hidden(*kind, "m").await;
            let p_group = if kind.ga { "ga, " } else { "" };
            let m_group = if kind.gb { "gb, " } else { "" };
            let not_null: Vec<String> = self
                .key
                .columns()
                .iter()
                .map(|c| format!("{c} is not null"))
                .collect();
            for (actual, expected) in [
                (
                    format!(
                        "select {p_group}{keys}, __n, __count_1, __sum_1 from {p} \
                         where __n <> 0 or __count_1 <> 0 or __sum_1 <> 0 \
                         order by {p_group}{keys}"
                    ),
                    format!(
                        "select {p_group}{keys}, count(*), count(x), coalesce(sum(x), 0) \
                         from public.a group by {p_group}{keys} order by {p_group}{keys}"
                    ),
                ),
                (
                    format!(
                        "select {m_group}{keys}, __n, __count_1, __sum_1 from {m} \
                         where __n <> 0 or __count_1 <> 0 or __sum_1 <> 0 \
                         order by {m_group}{keys}"
                    ),
                    format!(
                        "select {m_group}{keys}, count(*), count(y), coalesce(sum(y), 0) \
                         from public.b where {} group by {m_group}{keys} \
                         order by {m_group}{keys}",
                        not_null.join(" and ")
                    ),
                ),
            ] {
                assert_eq!(
                    self.d.rows(&actual).await,
                    self.d.rows(&expected).await,
                    "{context}: {}'s hidden cells (left) differ from the source (right)",
                    kind.name
                );
            }
        }
        assert_eq!(self.d.deadlocks_logged(), Vec::<String>::new(), "{context}");
    }

    /// Whether `other` queued behind backend `pid` (`true`) or ran to its
    /// end (`false`). Both are states the scenario forced; this only waits
    /// for the second drain (or build) to reach one of them.
    async fn queued_or_done<T>(&self, other: &drain_driver::Running<T>, pid: i32) -> bool {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if other.is_finished() {
                return false;
            }
            let blocked: bool = self
                .d
                .ctl
                .query_one(
                    "select exists (select 1 from pg_stat_activity \
                     where $1 = any(pg_blocking_pids(pid)))",
                    &[&pid],
                )
                .await
                .expect("read pg_stat_activity")
                .get(0);
            if blocked {
                return true;
            }
            assert!(
                Instant::now() < deadline,
                "the second page neither queued nor finished within 60 s"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Cancels the page frozen at `point` (backend `pid`) and holds `kind`'s
    /// lock row `FOR UPDATE` until the page's retry queues behind it, so the
    /// caller sees what the rolled-back attempt left. Returns the holder,
    /// which the caller commits to let the retry go.
    async fn cancel_and_hold(
        &self,
        kind: Kind,
        frozen: &mut RunningDrain,
        point: PausePoint,
        pid: i32,
    ) -> tokio_postgres::Client {
        let lock = self.hidden(kind, "l").await;
        let holder = self.d.user().await;
        let holder_pid: i32 = holder
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        let holding = tokio::spawn(async move {
            holder
                .batch_execute(&format!("begin; select 1 from {lock} for update"))
                .await
                .expect("hold the lock row");
            holder
        });
        self.d.wait_blocked_behind(pid).await;
        self.d
            .ctl
            .execute("select pg_cancel_backend($1)", &[&pid])
            .await
            .expect("cancel the page");
        let holder = holding.await.expect("holder");
        self.d.release(frozen, point).await;
        self.d.wait_blocked_behind(holder_pid).await;
        holder
    }

    /// One race: `x` and `y` are sealed, in that order, into two batches;
    /// the page of batch `paused` (0: `x`'s, 1: `y`'s) is frozen at `point`
    /// of the first target while the other drains; then both finish.
    /// `queues` is whether the second page must queue behind the first
    /// (`None`: either). With no point, the two drain one after the other,
    /// `paused`'s first.
    async fn race(
        &mut self,
        context: &str,
        x: &str,
        y: &str,
        paused: usize,
        point: Option<PausePoint>,
        queues: Option<bool>,
    ) {
        let batches = [self.sealed(x).await, self.sealed(y).await];
        let (first, second) = (batches[paused], batches[1 - paused]);
        let Some(point) = point else {
            self.d.drain(first, "x").await;
            self.check_applied(context).await;
            self.d.drain(second, "y").await;
            self.check(context).await;
            return;
        };
        let target = self.kinds[0].target();
        let mut frozen = self
            .d
            .drain_frozen(first, "x", &[(point, target.as_str())])
            .await;
        let reached = frozen.reached(point).await;
        let other = self.d.drain_frozen(second, "y", &[]).await;
        let queued = self.queued_or_done(&other, reached.backend_pid).await;
        if let Some(expected) = queues {
            assert_eq!(queued, expected, "{context}: did the second page queue?");
        }
        if !queued {
            // It committed while the first was frozen: the invariants hold
            // over what it applied.
            self.check_applied(context).await;
        }
        self.d.release(&mut frozen, point).await;
        frozen.finish().await;
        other.finish().await;
        self.check(context).await;
    }
}

/// Whether a page frozen at `point` holds the `J` lock of a key it shares
/// with the other page: from its sorted `J` lock on. `new_key`: the key's
/// `J` row is the frozen page's to create, so its insert already holds it.
fn holds_j(point: PausePoint, new_key: bool) -> bool {
    match point {
        PausePoint::BeforeJoinLock => false,
        PausePoint::AfterJoinKeyInsert => new_key,
        _ => true,
    }
}

/// Every schedule of a race: both sequential orders, and each page frozen
/// at every point, with whether the other must queue. `shared`: the pages
/// name a common join key; `new_key`: its `J` row doesn't exist before
/// them.
fn schedules(shared: bool, new_key: bool) -> Vec<(usize, Option<PausePoint>, Option<bool>)> {
    let mut out = vec![(0, None, None), (1, None, None)];
    for paused in [0, 1] {
        for point in POINTS {
            out.push((paused, Some(point), Some(shared && holds_j(point, new_key))));
        }
    }
    out
}

/// `template` for scenario `n` on `key`: `{k}` is its join key's values
/// `(t, k)`, `(n, n)`; `{dk}` a decoy key that never matches `{k}`: on the
/// pair, `(n + 1000, n)`, which shares `{k}`'s `k` but not its `t`; on the
/// single-column key, `(n + 1000, n + 1000)`, since a decoy dimension row on
/// `{k}`'s `k` would keep the key matched and hide every flip; `{k2}` a
/// second key `(n, n + 5000)`; `{n}` is `n`; `{g1}`…`{g4}` its `ga`/`gb`
/// values `10n + i`; `{id}` its id base `100n`, which rows add to.
fn fill(template: &str, n: i32, key: Key) -> String {
    let decoy_k = match key {
        Key::Single => n + 1000,
        Key::Pair => n,
    };
    template
        .replace("{k}", &format!("{n}, {n}"))
        .replace("{dk}", &format!("{}, {decoy_k}", n + 1000))
        .replace("{k2}", &format!("{n}, {}", n + 5000))
        .replace("{n}", &n.to_string())
        .replace("{g1}", &(10 * n + 1).to_string())
        .replace("{g2}", &(10 * n + 2).to_string())
        .replace("{g3}", &(10 * n + 3).to_string())
        .replace("{g4}", &(10 * n + 4).to_string())
        .replace("{id}", &(100 * n).to_string())
}

/// The decoys every scenario's setup adds: a fact and a dimension row on
/// `{dk}`.
const DECOYS: &str = "insert into public.a values ({id} + 90, {g1}, {dk}, 1); \
                      insert into public.b values ({id} + 90, {dk}, {g4}, 1000)";

/// Runs `races` (name, setup, x, y, shared, new_key) under every schedule,
/// each scenario on its own key.
async fn run_races(lab: &mut Lab, races: &[(&str, &str, &str, &str, bool, bool)]) {
    let mut n = 0;
    for (name, setup, x, y, shared, new_key) in races {
        for (paused, point, queues) in schedules(*shared, *new_key) {
            n += 1;
            if !setup.is_empty() {
                lab.step(&fill(setup, n, lab.key)).await;
            }
            lab.race(
                &format!(
                    "{:?}/{}: {name}, page {paused} frozen at {point:?}",
                    lab.key, lab.kinds[0].name
                ),
                &fill(x, n, lab.key),
                &fill(y, n, lab.key),
                paused,
                point,
                queues,
            )
            .await;
        }
    }
}

// ------------------------------------------------------------- family 1

/// J1 family 1: a fact write against its key's dimension row moving
/// `gb1 -> gb2`, in both source orders and both page orders, each page
/// frozen at every point.
async fn fact_vs_dimension_move(key: Key, kind: Kind) {
    let mut lab = lab(key, &[kind]).await;
    let setup = format!(
        "insert into public.a values ({{id}} + 1, {{g1}}, {{k}}, 10), \
                                     ({{id}} + 2, {{g2}}, {{k}}, null); \
         insert into public.b values ({{id}} + 1, {{k}}, {{g1}}, 100); {DECOYS}"
    );
    let mv = "update public.b set gb = {g2} where id = {id} + 1";
    let facts = [
        (
            "ins-same-ga",
            "insert into public.a values ({id} + 3, {g1}, {k}, 5)",
        ),
        (
            "ins-new-ga",
            "insert into public.a values ({id} + 3, {g3}, {k}, 5)",
        ),
        ("upd-x", "update public.a set x = 7 where id = {id} + 1"),
        ("del", "delete from public.a where id = {id} + 1"),
        (
            "upd-ga",
            "update public.a set ga = {g3} where id = {id} + 1",
        ),
    ];
    let mut races = Vec::new();
    let names: Vec<String> = facts
        .iter()
        .flat_map(|(name, _)| [format!("{name} a>b"), format!("{name} b>a")])
        .collect();
    for (i, (_, fact)) in facts.iter().enumerate() {
        races.push((
            names[2 * i].as_str(),
            setup.as_str(),
            *fact,
            mv,
            true,
            false,
        ));
        races.push((
            names[2 * i + 1].as_str(),
            setup.as_str(),
            mv,
            *fact,
            true,
            false,
        ));
    }
    run_races(&mut lab, &races).await;
}

#[tokio::test]
async fn family1_fact_vs_dimension_move_left() {
    fact_vs_dimension_move(Key::Single, L).await;
}

#[tokio::test]
async fn family1_fact_vs_dimension_move_inner() {
    fact_vs_dimension_move(Key::Single, I).await;
}

#[tokio::test]
async fn family1_fact_vs_dimension_move_pair_key() {
    fact_vs_dimension_move(Key::Pair, L).await;
}

// ------------------------------------------------------------- family 2

/// J1 family 2: a key's matched/unmatched flips against fact writes on it
/// (the first dimension row, one with a `NULL` `gb`, and the delete of the
/// only one), and the first sight of a key by both sides, whose `J` row
/// both pages create.
async fn flips(key: Key, kind: Kind) {
    let mut lab = lab(key, &[kind]).await;
    let unmatched = format!(
        "insert into public.a values ({{id}} + 1, {{g1}}, {{k}}, 10), \
                                     ({{id}} + 2, {{g2}}, {{k}}, null); {DECOYS}"
    );
    let matched =
        format!("{unmatched}; insert into public.b values ({{id}} + 1, {{k}}, {{g1}}, 100)");
    let dims = [
        (
            "ins-match",
            unmatched.as_str(),
            "insert into public.b values ({id} + 2, {k}, {g2}, 5)",
        ),
        (
            "ins-match-nullgb",
            unmatched.as_str(),
            "insert into public.b values ({id} + 2, {k}, null, 5)",
        ),
        (
            "del-only",
            matched.as_str(),
            "delete from public.b where id = {id} + 1",
        ),
    ];
    let facts = [
        (
            "ins-same-ga",
            "insert into public.a values ({id} + 3, {g1}, {k}, 5)",
        ),
        (
            "ins-new-ga",
            "insert into public.a values ({id} + 3, {g3}, {k}, 5)",
        ),
        ("upd-x", "update public.a set x = 7 where id = {id} + 1"),
        ("del", "delete from public.a where id = {id} + 1"),
    ];
    let mut names = Vec::new();
    for (dn, _, _) in &dims {
        for (fname, _) in &facts {
            names.push(format!("{dn} vs {fname}"));
        }
    }
    let mut races = Vec::new();
    let mut i = 0;
    for (_, setup, dim) in &dims {
        for (_, fact) in &facts {
            races.push((names[i].as_str(), *setup, *fact, *dim, true, false));
            i += 1;
        }
    }
    races.push((
        "first sight a>b",
        "",
        "insert into public.a values ({id} + 1, {g1}, {k}, 10)",
        "insert into public.b values ({id} + 1, {k}, {g2}, 100)",
        true,
        true,
    ));
    races.push((
        "first sight b>a",
        "",
        "insert into public.b values ({id} + 1, {k}, {g2}, 100)",
        "insert into public.a values ({id} + 1, {g1}, {k}, 10)",
        true,
        true,
    ));
    run_races(&mut lab, &races).await;
}

#[tokio::test]
async fn family2_flips_left() {
    flips(Key::Single, L).await;
}

#[tokio::test]
async fn family2_flips_inner() {
    flips(Key::Single, I).await;
}

#[tokio::test]
async fn family2_flips_pair_key() {
    flips(Key::Pair, L).await;
}

// ------------------------------------------------------------- family 3

/// J1 family 3: two dimension pages on one key that each cross zero from a
/// stale view (insert + insert, delete + insert, delete + delete, update +
/// insert, equal-`gb` duplicates), over facts on the key.
async fn two_dimension_pages(key: Key, kind: Kind) {
    let mut lab = lab(key, &[kind]).await;
    let facts = format!(
        "insert into public.a values ({{id}} + 1, {{g1}}, {{k}}, 10), \
                                     ({{id}} + 2, {{g2}}, {{k}}, null); {DECOYS}"
    );
    let one = format!("{facts}; insert into public.b values ({{id}} + 1, {{k}}, {{g1}}, 100)");
    let two = format!("{one}; insert into public.b values ({{id}} + 2, {{k}}, {{g2}}, 7)");
    let races = [
        (
            "ins+ins",
            facts.as_str(),
            "insert into public.b values ({id} + 1, {k}, {g1}, 100)",
            "insert into public.b values ({id} + 2, {k}, {g2}, 7)",
            true,
            false,
        ),
        (
            "del+ins",
            one.as_str(),
            "delete from public.b where id = {id} + 1",
            "insert into public.b values ({id} + 2, {k}, {g2}, 7)",
            true,
            false,
        ),
        (
            "del+del",
            two.as_str(),
            "delete from public.b where id = {id} + 1",
            "delete from public.b where id = {id} + 2",
            true,
            false,
        ),
        (
            "upd+ins",
            one.as_str(),
            "update public.b set gb = {g3} where id = {id} + 1",
            "insert into public.b values ({id} + 2, {k}, {g2}, 7)",
            true,
            false,
        ),
        (
            "equal-gb duplicates",
            facts.as_str(),
            "insert into public.b values ({id} + 1, {k}, {g1}, 100)",
            "insert into public.b values ({id} + 2, {k}, {g1}, 100)",
            true,
            false,
        ),
    ];
    run_races(&mut lab, &races).await;
}

#[tokio::test]
async fn family3_two_dimension_pages_left() {
    two_dimension_pages(Key::Single, L).await;
}

#[tokio::test]
async fn family3_two_dimension_pages_pair_key() {
    two_dimension_pages(Key::Pair, L).await;
}

// ------------------------------------------------------- families 4 and 5

/// J1 families 4 and 5, one page at a time over all six kinds at once:
/// duplicate dimension rows (`M.n` of 2 and 3, deleting them one by one,
/// `y` to `NULL`, three in one statement), and `NULL`s: `NULL` join keys on
/// either side (on the pair key, a `NULL` in one column), `NULL` groups and
/// values, a matched dimension row with a `NULL` `gb` beside the unmatched
/// facts of another key, and keys moving to and from `NULL`.
async fn duplicates_and_nulls(key: Key) {
    let mut lab = lab(key, &SIX).await;
    for sql in [
        // Family 4.
        "insert into public.a values (1, 1, 1, 1, 10), (2, 2, 1, 1, null)",
        "insert into public.b values (1, 1, 1, 1, 100), (2, 1, 1, 1, 50)",
        "insert into public.b values (3, 1, 1, 1, null)",
        "delete from public.b where id = 2",
        "update public.b set y = null where id = 1",
        "delete from public.b where id in (1, 3)",
        "insert into public.b values (4, 1, 1, 2, 1), (5, 1, 1, 2, 2), (6, 1, 1, 2, 3)",
        // Family 5.
        "insert into public.a values (3, 1, 1, null, 7), (4, 1, null, 1, 8), (5, null, 2, 2, 9)",
        "insert into public.b values (7, 1, null, 3, 4), (8, null, 1, 3, 5), (9, 2, 2, null, null)",
        "insert into public.a values (6, 3, 3, 3, 1); insert into public.b values (10, 9, 9, null, 1)",
        "update public.a set k = null where id = 1",
        "update public.a set k = 1 where id = 3; update public.b set k = null where id = 4",
        "update public.a set t = null where id = 2; update public.b set t = 1 where id = 8",
        "update public.b set k = 3, t = 3 where id = 10",
        "update public.a set ga = null, x = null where id = 6",
        "delete from public.a where id in (1, 5); delete from public.b where id = 9",
    ] {
        lab.step(sql).await;
        lab.check(&format!("{key:?}: after {sql}")).await;
    }
}

#[tokio::test]
async fn families4_5_duplicates_and_nulls() {
    duplicates_and_nulls(Key::Single).await;
}

#[tokio::test]
async fn families4_5_duplicates_and_nulls_pair_key() {
    duplicates_and_nulls(Key::Pair).await;
}

/// J1 family 5: a page whose every key has a `NULL` part creates no `J` row
/// and takes no `J` lock, so a page of another key never queues behind it.
#[tokio::test]
async fn a_null_key_takes_no_j_row_or_lock() {
    let mut lab = lab(Key::Pair, &[L]).await;
    lab.step("insert into public.b values (1, 1, 1, 1, 100)")
        .await;
    lab.race(
        "null key",
        "insert into public.a values (1, 1, null, 1, 10), (2, 1, 1, null, 5)",
        "insert into public.a values (3, 1, 1, 1, 1); update public.b set y = 1 where id = 1",
        0,
        Some(PausePoint::BeforeCommit),
        Some(false),
    )
    .await;
    let j = lab.hidden(L, "j").await;
    assert_eq!(
        lab.d.rows(&format!("select t, k from {j}")).await,
        ["(1,1)"],
        "only the non-null key has a J row"
    );
}

// ------------------------------------------------------------- family 6

/// J1 family 6: key changes (a fact's, a dimension row's, both) racing the
/// other side on the new key, and two pages locking the same two keys
/// named in opposite orders, which queue and never deadlock.
async fn key_changes(key: Key) {
    let mut lab = lab(key, &[L]).await;
    let setup = format!(
        "insert into public.a values ({{id}} + 1, {{g1}}, {{k}}, 10), \
                                     ({{id}} + 2, {{g2}}, {{k2}}, 3); \
         insert into public.b values ({{id}} + 1, {{k}}, {{g1}}, 100), \
                                     ({{id}} + 2, {{k2}}, {{g2}}, 1); {DECOYS}"
    );
    let races = [
        (
            "fact key change vs dimension insert",
            setup.as_str(),
            "update public.a set k = {n} + 5000 where id = {id} + 1",
            "insert into public.b values ({id} + 3, {k2}, {g3}, 7)",
            true,
            false,
        ),
        (
            "dimension key change vs fact insert",
            setup.as_str(),
            "update public.b set k = {n} + 5000 where id = {id} + 1",
            "insert into public.a values ({id} + 3, {g3}, {k}, 4)",
            true,
            false,
        ),
        (
            "both key changes",
            setup.as_str(),
            "update public.a set k = {n} + 5000 where id = {id} + 1",
            "update public.b set k = {n} where id = {id} + 2",
            true,
            false,
        ),
        (
            "opposite key orders",
            setup.as_str(),
            "update public.a set x = x + 1 where id = {id} + 2; \
             update public.a set x = x + 1 where id = {id} + 1",
            "update public.b set y = y + 1 where id = {id} + 1; \
             update public.b set y = y + 1 where id = {id} + 2",
            true,
            false,
        ),
    ];
    run_races(&mut lab, &races).await;
}

#[tokio::test]
async fn family6_key_changes_pair_key() {
    key_changes(Key::Pair).await;
}

#[tokio::test]
async fn family6_key_changes() {
    key_changes(Key::Single).await;
}

/// J1 family 6 (and #1043's R7): one batch of 900 changes over four keys,
/// of both tables, split into buckets and drained by eight workers at once.
/// Every page locks most keys: none deadlocks, and the result is exact.
#[tokio::test]
async fn family6_eight_concurrent_pages() {
    let mut lab = lab(Key::Single, &[L, I]).await;
    lab.step(
        "insert into public.a select g, g % 3, 0, g % 4, g from generate_series(1, 400) g; \
         insert into public.b select g, 0, g % 4, g % 2, g from generate_series(1, 200) g",
    )
    .await;
    for round in [
        "update public.a set x = x + 1, k = (k + 1) % 4; \
         update public.b set y = y + 1, gb = 1 - gb",
        "update public.a set ga = (ga + 1) % 3 where id % 2 = 0; \
         delete from public.b where id % 3 = 0; \
         insert into public.b select g, 0, g % 4, null, g from generate_series(201, 400) g",
    ] {
        let batch = lab.sealed(round).await;
        let mut drains = Vec::new();
        for i in 0..8 {
            drains.push(lab.d.drain_share(batch, &format!("w{i}"), 8).await);
        }
        for drain in drains {
            drain.finish().await;
        }
        lab.check(round).await;
    }
}

// ------------------------------------------------------------- family 7

/// J1 family 7: out-of-order pages. Every ordered pair of seven pages on
/// one key (three of `a`, three of `b`, one mixed) is sealed in order and
/// applied in reverse (two undrained batches are the most the ring holds),
/// with the invariants checked after every page, over all six kinds: 42
/// orders.
#[tokio::test]
async fn family7_out_of_order_pages() {
    let mut lab = lab(Key::Single, &SIX).await;
    let pages = [
        "insert into public.a values ({id} + 2, {g2}, {k}, 5)",
        "update public.a set ga = {g3}, x = 1 where id = {id} + 1",
        "delete from public.a where id = {id} + 1",
        "delete from public.b where id = {id} + 1",
        "insert into public.b values ({id} + 2, {k}, {g2}, 7), ({id} + 3, {k}, null, 1)",
        "update public.b set gb = {g4} where id = {id} + 1",
        "insert into public.a values ({id} + 4, {g1}, {k}, 3); \
         update public.b set y = y + 1 where id = {id} + 1",
    ];
    let mut n = 0;
    for (i, first) in pages.iter().enumerate() {
        for (j, second) in pages.iter().enumerate() {
            if i == j {
                continue;
            }
            n += 1;
            lab.step(&fill(
                "insert into public.a values ({id} + 1, {g1}, {k}, 10); \
                 insert into public.b values ({id} + 1, {k}, {g1}, 100)",
                n,
                lab.key,
            ))
            .await;
            let earlier = lab.sealed(&fill(first, n, lab.key)).await;
            let later = lab.sealed(&fill(second, n, lab.key)).await;
            let context = format!("page {j} applied before page {i}");
            assert!(
                lab.d.drain(later, "x").await.is_some(),
                "{context}: the later batch drained first"
            );
            lab.check_applied(&context).await;
            lab.d.drain(earlier, "x").await;
            lab.check(&context).await;
        }
    }
}

// --------------------------------------------------------- families 11, 14

/// J1 family 11: source transactions with savepoints (a partial rollback,
/// nested, released) and one rolled back whole, which leaves no ring row
/// and no `J` row for its key.
#[tokio::test]
async fn family11_savepoints_and_rolled_back_sources() {
    let mut lab = lab(Key::Pair, &[L, I]).await;
    for sql in [
        "begin; insert into public.a values (1, 1, 1, 1, 10); savepoint s; \
         insert into public.b values (1, 1, 1, 1, 100); rollback to savepoint s; \
         insert into public.b values (2, 1, 1, 2, 5); commit",
        "begin; savepoint s1; update public.a set x = 20 where id = 1; savepoint s2; \
         delete from public.b where id = 2; rollback to savepoint s2; release savepoint s1; \
         insert into public.b values (3, 1, 1, 3, 7); commit",
        "begin; insert into public.a values (9, 1, 9, 9, 1); \
         insert into public.b values (9, 9, 9, 1, 1); rollback",
    ] {
        lab.step(sql).await;
        lab.check(sql).await;
    }
    let j = lab.hidden(L, "j").await;
    assert_eq!(
        lab.d
            .rows(&format!("select t, k from {j} order by t, k"))
            .await,
        ["(1,1)"],
        "the rolled-back key has no J row"
    );
}

/// J1 family 11's two-phase commit: a source transaction that changes both
/// sides of a key is prepared, then committed or rolled back, with or
/// without a recompute of the key's cells (image-less changes, drained)
/// while it is prepared. The prepared transaction is in progress in the
/// recompute's snapshot, so the recompute's basis doesn't count it, and its
/// pages apply once it commits; rolled back, it leaves nothing.
#[tokio::test]
async fn family11_two_phase_commit() {
    const PREPARED: &str = "insert into public.a values ({id} + 5, {g3}, {k}, 5); \
                            update public.b set gb = {g2} where id = {id} + 1; \
                            insert into public.b values ({id} + 5, {k}, {g3}, 5)";
    let mut lab = lab(Key::Pair, &[L, I]).await;
    let mut n = 0;
    for commit in [true, false] {
        for recompute in [false, true] {
            n += 1;
            let context = format!("commit: {commit}, a recompute while prepared: {recompute}");
            lab.step(&fill(SEED, n, lab.key)).await;
            let gid = format!("family11_{n}");
            lab.write(&format!(
                "begin; {}; prepare transaction '{gid}'",
                fill(PREPARED, n, lab.key)
            ))
            .await;
            if recompute {
                let row = (100 * n + 1).to_string();
                lab.d
                    .stage_recomputes_with_prior("public.a", &[(row.as_str(), None)])
                    .await;
                lab.d
                    .stage_recomputes_with_prior("public.b", &[(row.as_str(), None)])
                    .await;
                let recomputed = lab.d.seal().await;
                lab.d.drain(recomputed, "recompute").await;
                lab.check_applied(&context).await;
            }
            let end = if commit { "commit" } else { "rollback" };
            lab.write(&format!("{end} prepared '{gid}'")).await;
            let batch = lab.d.seal().await;
            lab.d.drain(batch, "prepared").await;
            lab.check(&context).await;
        }
    }
}

/// The decision on #1108: a `J` row a page inserts for a key no row had,
/// in a page that rolls back, leaves nothing behind; the page's retry
/// creates it.
#[tokio::test]
async fn a_j_row_inserted_by_a_rolled_back_page_leaves_nothing() {
    let mut lab = lab(Key::Pair, &[L]).await;
    let batch = lab
        .sealed(
            "insert into public.a values (1, 1, 1, 1, 10); \
                 insert into public.b values (1, 1, 1, 2, 100)",
        )
        .await;
    let point = PausePoint::AfterJoinKeyInsert;
    let mut frozen = lab.d.drain_frozen(batch, "x", &[(point, "public.l")]).await;
    let reached = frozen.reached(point).await;
    let j = lab.hidden(L, "j").await;
    let holder = lab
        .cancel_and_hold(L, &mut frozen, point, reached.backend_pid)
        .await;
    assert_eq!(
        lab.d.rows(&format!("select t, k from {j}")).await,
        Vec::<String>::new(),
        "the rolled-back attempt's J row is gone"
    );
    holder.batch_execute("commit").await.expect("commit");
    frozen.finish().await;
    lab.check("after the rollback").await;
    assert_eq!(
        lab.d
            .rows(&format!("select t, k, __matches from {j}"))
            .await,
        ["(1,1,1)"]
    );
}

/// J1 family 14: `T` is written in the page's own transaction. Frozen just
/// before its commit, the page has written nothing another session sees;
/// cancelled there, it leaves nothing, and its retry applies it whole.
#[tokio::test]
async fn family14_t_in_the_pages_own_transaction() {
    let mut lab = lab(Key::Single, &[L]).await;
    lab.step(
        "insert into public.a values (1, 1, 0, 1, 10); \
         insert into public.b values (1, 0, 1, 1, 100)",
    )
    .await;
    let (p, m, j, t) = (
        lab.hidden(L, "p").await,
        lab.hidden(L, "m").await,
        lab.hidden(L, "j").await,
        lab.hidden(L, "t").await,
    );
    let state = format!(
        "select (select array_agg(x::text order by x::text) from {p} x) \
              || (select array_agg(x::text order by x::text) from {m} x) \
              || (select array_agg(x::text order by x::text) from {j} x) \
              || (select array_agg(x::text order by x::text) from {t} x) \
              || (select array_agg(x::text order by x::text) from public.l x)"
    );
    for cancel in [true, false] {
        let before = lab.d.rows(&state).await;
        let batch = lab
            .sealed(
                "insert into public.a values (2, 2, 0, 1, 5); \
                     update public.b set gb = 2 where id = 1",
            )
            .await;
        let point = PausePoint::BeforeCommit;
        let mut frozen = lab.d.drain_frozen(batch, "x", &[(point, "public.l")]).await;
        let reached = frozen.reached(point).await;
        assert_eq!(
            lab.d.rows(&state).await,
            before,
            "nothing shows before the commit"
        );
        if cancel {
            let holder = lab
                .cancel_and_hold(L, &mut frozen, point, reached.backend_pid)
                .await;
            assert_eq!(
                lab.d.rows(&state).await,
                before,
                "the rollback left nothing"
            );
            holder.batch_execute("commit").await.expect("commit");
            frozen.finish().await;
            lab.check("after the retry").await;
            lab.step("delete from public.a where id = 2; update public.b set gb = 1")
                .await;
        } else {
            lab.d.release(&mut frozen, point).await;
            frozen.finish().await;
            lab.check("after the commit").await;
        }
    }
}

// ------------------------------------------------------------ family 12

/// J1 family 12: mixed pages (rows of both tables in one page) against pure
/// pages of either side and against other mixed pages on the same key, and
/// two mixed pages locking two keys with their sides swapped.
async fn mixed_pages(key: Key) {
    let mut lab = lab(key, &[L]).await;
    let setup = format!(
        "insert into public.a values ({{id}} + 1, {{g1}}, {{k}}, 10), \
                                     ({{id}} + 2, {{g2}}, {{k2}}, 3); \
         insert into public.b values ({{id}} + 1, {{k}}, {{g1}}, 100), \
                                     ({{id}} + 2, {{k2}}, {{g2}}, 1); {DECOYS}"
    );
    let mixed = "insert into public.a values ({id} + 3, {g3}, {k}, 5); \
                 update public.b set gb = {g2} where id = {id} + 1";
    let races = [
        (
            "mixed vs a",
            setup.as_str(),
            mixed,
            "update public.a set x = 1 where id = {id} + 1",
            true,
            false,
        ),
        (
            "mixed vs b",
            setup.as_str(),
            mixed,
            "delete from public.b where id = {id} + 1",
            true,
            false,
        ),
        (
            "mixed vs mixed",
            setup.as_str(),
            mixed,
            "delete from public.a where id = {id} + 1; \
             insert into public.b values ({id} + 3, {k}, null, 2)",
            true,
            false,
        ),
        (
            "swapped sides over two keys",
            setup.as_str(),
            "update public.a set x = 2 where id = {id} + 1; \
             update public.b set y = 2 where id = {id} + 2",
            "update public.b set y = 3 where id = {id} + 1; \
             update public.a set x = 3 where id = {id} + 2",
            true,
            false,
        ),
        (
            "a mixed page's key moves",
            setup.as_str(),
            "update public.a set k = {n} + 5000 where id = {id} + 1; \
             update public.b set k = {n} where id = {id} + 2",
            "insert into public.b values ({id} + 3, {k2}, {g4}, 9)",
            true,
            false,
        ),
    ];
    run_races(&mut lab, &races).await;
}

#[tokio::test]
async fn family12_mixed_pages() {
    mixed_pages(Key::Single).await;
}

#[tokio::test]
async fn family12_mixed_pages_pair_key() {
    mixed_pages(Key::Pair).await;
}

// ---------------------------------------------------------------- review

/// RV1 (#1091's review): a fact in a new `ga` against a second dimension
/// row in a new `gb` on a matched key. The two pages' `T` groups are
/// disjoint, so only the `J` lock orders them; without it the cross term
/// `ΔP ⊗ ΔM` is lost. RV1b: the same on a key whose `J` row exists with no
/// match and whose only fact left, so its `P` cell is zero.
async fn rv1(key: Key, kind: Kind) {
    let mut lab = lab(key, &[kind]).await;
    let races = [
        (
            "RV1",
            "insert into public.a values ({id} + 1, {g1}, {k}, 10); \
             insert into public.b values ({id} + 1, {k}, {g1}, 100)",
            "insert into public.a values ({id} + 2, {g2}, {k}, 5)",
            "insert into public.b values ({id} + 2, {k}, {g2}, 50)",
            true,
            false,
        ),
        (
            "RV1b",
            "insert into public.a values ({id} + 1, {g1}, {k}, 10); \
             delete from public.a where id = {id} + 1",
            "insert into public.a values ({id} + 2, {g2}, {k}, 5)",
            "insert into public.b values ({id} + 2, {k}, {g2}, 50)",
            true,
            false,
        ),
    ];
    run_races(&mut lab, &races).await;
}

#[tokio::test]
async fn rv1_disjoint_t_groups_left() {
    rv1(Key::Single, L).await;
}

#[tokio::test]
async fn rv1_disjoint_t_groups_inner() {
    rv1(Key::Single, I).await;
}

#[tokio::test]
async fn rv1_disjoint_t_groups_pair_key() {
    rv1(Key::Pair, L).await;
}

/// RV2: pages read the other side after their `J` lock in a statement of
/// their own, so they must run under `READ COMMITTED`, whatever the
/// database's default: here `REPEATABLE READ`. A fact page queued on `J`
/// behind a dimension page reads that page's `M` once it commits.
#[tokio::test]
async fn rv2_pages_read_committed_whatever_the_default() {
    let mut lab = lab_with_defaults(
        Key::Single,
        &[L],
        &[("default_transaction_isolation", "repeatable read")],
    )
    .await;
    let races = [(
        "RV2",
        "insert into public.a values ({id} + 1, {g1}, {k}, 10); \
         insert into public.b values ({id} + 1, {k}, {g1}, 100)",
        "update public.b set gb = {g2} where id = {id} + 1; \
         insert into public.b values ({id} + 2, {k}, {g3}, 1)",
        "insert into public.a values ({id} + 2, {g2}, {k}, 5)",
        true,
        false,
    )];
    run_races(&mut lab, &races).await;
}

// ------------------------------------------------------------- the rest

/// #1096 decision 1: expression arguments are per-side partials and
/// composed fields are computed from `T`'s partials; `numeric(p,s)` keeps
/// Postgres's scale through rows leaving a group, and a group shows only
/// while its row count is above 0.
#[tokio::test]
async fn composed_fields_expression_arguments_and_numeric_scale() {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(
        "create table public.o (id integer primary key, cid integer, qty integer, \
                                amount numeric(10, 2)); \
         create table public.c (id integer primary key, region text, weight integer); \
         create index on public.o (cid)",
        &[
            ("id", int4()),
            ("cid", int4()),
            ("qty", int4()),
            ("amount", ValueType::Numeric),
        ],
        &[
            "TRANSFORM r FROM public.o LEFT JOIN public.c ON o.cid = c.id GROUP BY c.region \
          WITHOUT SOURCE INDEX SELECT c.region AS region, COUNT(*) AS n, SUM(o.amount) AS revenue, \
                  AVG(o.amount) AS mean, SUM(o.qty + 1) AS q1, COUNT(c.weight) AS w, \
                  COALESCE(SUM(c.weight), 0) + COUNT(o.qty) AS mixed, \
                  SUM(o.qty) > COUNT(*) AS many",
        ],
        &["public.o", "public.c"],
    )
    .await;
    let actual = "select region, n, revenue::text, mean::text, q1, w, mixed::text, many \
                  from public.r order by region";
    let expected = "select c.region, count(*), sum(o.amount)::text, avg(o.amount)::text, \
                           sum(o.qty + 1), count(c.weight), \
                           (coalesce(sum(c.weight), 0) + count(o.qty))::text, \
                           sum(o.qty) > count(*) \
                    from public.o left join public.c on o.cid = c.id \
                    group by c.region order by c.region";
    for sql in [
        "insert into public.o values (1, 1, 2, 1.5), (2, 1, null, 0.25), (3, 2, 4, null), \
                                     (4, null, 1, 3)",
        "insert into public.c values (1, 'emea', 3), (2, 'apac', null)",
        "delete from public.o where id = 2",
        "update public.c set region = 'apac' where id = 1",
        "update public.o set amount = 3 where id = 1; delete from public.c where id = 2",
        "delete from public.o where cid = 1",
    ] {
        d.user().await.batch_execute(sql).await.expect(sql);
        let batch = d.seal().await;
        d.drain(batch, "x").await;
        assert_eq!(d.rows(actual).await, d.rows(expected).await, "after {sql}");
    }
}

// ------------------------------------------------------------ family 10

/// J1 family 10's seed on scenario `n`'s key: two facts and a dimension row
/// matching them, and the decoys.
const SEED: &str = "insert into public.a values ({id} + 1, {g1}, {k}, 10), \
                                         ({id} + 2, {g2}, {k}, null); \
                    insert into public.b values ({id} + 1, {k}, {g1}, 100); \
                    insert into public.a values ({id} + 90, {g1}, {dk}, 1); \
                    insert into public.b values ({id} + 90, {dk}, {g4}, 1000)";

/// Every schedule of a race between a `TRUNCATE` page and another page of
/// the pair: both sequential orders, and each page frozen at every point.
/// Every page holds the pair's lock row from its first point on, `FOR
/// SHARE`, and the truncate's page `FOR UPDATE`, so the other always queues.
fn truncate_schedules() -> Vec<(usize, Option<PausePoint>, Option<bool>)> {
    let mut out = vec![(0, None, None), (1, None, None)];
    for paused in [0, 1] {
        for point in POINTS {
            out.push((paused, Some(point), Some(true)));
        }
    }
    out
}

/// Runs `races` (name, x, y) under every [`truncate_schedules`], each on a
/// key of its own seeded with [`SEED`].
async fn run_truncate_races(lab: &mut Lab, races: &[(&str, &str, &str)]) {
    let mut n = 0;
    for (name, x, y) in races {
        for (paused, point, queues) in truncate_schedules() {
            n += 1;
            lab.step(&fill(SEED, n, lab.key)).await;
            lab.race(
                &format!(
                    "{:?}/{}: {name}, page {paused} frozen at {point:?}",
                    lab.key, lab.kinds[0].name
                ),
                &fill(x, n, lab.key),
                &fill(y, n, lab.key),
                paused,
                point,
                queues,
            )
            .await;
        }
    }
}

/// J1 family 10: a `TRUNCATE` of either side against a page of the other
/// side, of its own side, and a truncate and an insert in one transaction,
/// in both batch orders and both page orders, each page frozen at every
/// point. A page of the truncated side from an earlier batch that applies
/// after the truncate's page is refused by the side's floor.
async fn truncate_races(key: Key, kind: Kind) {
    let mut lab = lab(key, &[kind]).await;
    run_truncate_races(
        &mut lab,
        &[
            (
                "truncate a vs b",
                "truncate public.a",
                "update public.b set gb = {g3} where id = {id} + 1",
            ),
            (
                "truncate b vs a",
                "truncate public.b",
                "insert into public.a values ({id} + 3, {g1}, {k}, 5)",
            ),
            (
                "a vs truncate a",
                "update public.a set x = 99 where id = {id} + 1; \
                 insert into public.a values ({id} + 3, {g3}, {k}, 5)",
                "truncate public.a",
            ),
            (
                "b vs truncate b",
                "insert into public.b values ({id} + 3, {k}, {g2}, 5)",
                "truncate public.b",
            ),
            (
                "truncate a and insert vs b",
                "begin; truncate public.a; \
                 insert into public.a values ({id} + 3, {g1}, {k}, 5), \
                                             ({id} + 4, {g2}, null, null, 6); commit",
                "update public.b set gb = {g3} where id = {id} + 1",
            ),
            (
                "truncate b and insert vs a",
                "begin; truncate public.b; \
                 insert into public.b values ({id} + 3, {k}, {g4}, 5), \
                                             ({id} + 4, null, null, {g2}, 6); commit",
                "insert into public.a values ({id} + 3, {g2}, {k}, 1)",
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn family10_truncate_races_left() {
    truncate_races(Key::Single, L).await;
}

#[tokio::test]
async fn family10_truncate_races_inner() {
    truncate_races(Key::Single, I).await;
}

#[tokio::test]
async fn family10_truncate_races_pair_key() {
    truncate_races(Key::Pair, L).await;
}

/// J1 family 10, sequential, on all six kinds: both sides truncated in either
/// order and then refilled, and a `TRUNCATE b` with every key matched, after
/// which `LEFT` targets hold each fact under a `NULL` `gb`, `INNER` ones
/// nothing, and every `J.matches` is 0 while the `J` rows stay.
#[tokio::test]
async fn family10_truncates_on_all_six_kinds() {
    let mut lab = lab(Key::Single, &SIX).await;
    let mut n = 0;
    for order in [
        "truncate public.a; truncate public.b",
        "truncate public.b; truncate public.a",
    ] {
        n += 1;
        lab.step(&fill(SEED, n, lab.key)).await;
        lab.step(&format!(
            "{order}; {}",
            fill(
                "insert into public.a values ({id} + 9, {g1}, {k}, 5); \
                 insert into public.b values ({id} + 9, {k}, {g4}, 5)",
                n,
                lab.key
            )
        ))
        .await;
        lab.check(order).await;
    }
    n += 1;
    lab.step(&fill(SEED, n, lab.key)).await;
    let j_rows = lab
        .d
        .rows(&format!(
            "select count(*) from {}",
            lab.hidden(L, "j").await
        ))
        .await;
    lab.step("truncate public.b").await;
    lab.check("truncate b with every key matched").await;
    for kind in SIX {
        let j = lab.hidden(kind, "j").await;
        assert_eq!(
            lab.d
                .rows(&format!("select coalesce(sum(__matches), 0) from {j}"))
                .await,
            ["(0)"],
            "{}: every J.matches is 0",
            kind.name
        );
    }
    assert_eq!(
        lab.d
            .rows(&format!(
                "select count(*) from {}",
                lab.hidden(L, "j").await
            ))
            .await,
        j_rows,
        "J rows are never collected"
    );
    for kind in [I, IB, IA] {
        assert_eq!(
            lab.d
                .rows(&format!("select count(*) from {}", kind.target()))
                .await,
            ["(0)"],
            "{} is empty",
            kind.name
        );
    }
    assert_eq!(
        lab.d
            .rows("select count(*) from public.l where gb is not null")
            .await,
        ["(0)"],
        "every LEFT group is unmatched"
    );
}

/// J1 family 10: a page of the truncated side from before the truncate,
/// applied after the truncate's page (the drain barrier never lets it; the
/// driver claims it out of order on purpose), is refused by the side's
/// floor, and a page of the other side applies.
#[tokio::test]
async fn family10_a_stale_page_after_the_truncate_is_refused() {
    let mut lab = lab(Key::Single, &[L, I]).await;
    for (n, (table, stale)) in [
        (
            "public.a",
            "insert into public.a values ({id} + 9, {g1}, {k}, 5); \
             update public.a set x = 99 where id = {id} + 1",
        ),
        (
            "public.b",
            "insert into public.b values ({id} + 9, {k}, {g4}, 5); \
             update public.b set y = 99 where id = {id} + 1",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let n = n as i32 + 1;
        lab.step(&fill(SEED, n, lab.key)).await;
        let earlier = lab.sealed(&fill(stale, n, lab.key)).await;
        let truncate = lab.sealed(&format!("truncate {table}")).await;
        lab.d.drain(truncate, "x").await;
        lab.check_applied(table).await;
        lab.d.drain(earlier, "y").await;
        lab.check(table).await;
    }
}

/// The mixed-page barrier (J1's `S10.mixed-page-barrier`): a `TRUNCATE a`,
/// then an insert into `a` and a change of `b` in one later batch, whose
/// page is a mixed one. Nothing past the truncate's batch is claimable
/// until it drains, so the mixed page can't apply its insert first and have
/// the truncate wipe it.
#[tokio::test]
async fn family10_a_mixed_page_waits_for_an_earlier_truncate() {
    let mut lab = lab(Key::Single, &[L, I]).await;
    lab.step(&fill(SEED, 1, lab.key)).await;
    let truncate = lab.sealed("truncate public.a").await;
    let mixed = lab
        .sealed(&fill(
            "insert into public.a values ({id} + 9, {g1}, {k}, 5); \
             update public.b set gb = {g3} where id = {id} + 1",
            1,
            lab.key,
        ))
        .await;
    let claimable = || async {
        trellis::staging::apply::next_claimable_segments(&lab.d.ctl, 1_000_000)
            .await
            .expect("next claimable segments")
    };
    assert_eq!(claimable().await, [truncate], "the mixed page waits");
    lab.d.drain(truncate, "x").await;
    assert_eq!(claimable().await, [mixed]);
    lab.d.drain(mixed, "x").await;
    lab.check("the mixed page after the truncate").await;
    assert_eq!(
        lab.d.rows("select count(*) from public.a").await,
        ["(1)"],
        "the insert survived"
    );
}

// ------------------------------------------- image-less changes and release

/// Writes `sql` to `table` with its capture triggers off: a change Trellis
/// never sees, which only a recompute repairs.
async fn uncaptured(lab: &Lab, table: &str, sql: &str) {
    lab.write(&format!(
        "alter table {table} disable trigger all; {sql}; alter table {table} enable trigger all"
    ))
    .await;
}

/// An image-less change (a `Recompute`) on either side recomputes the cells
/// it names: its key's current row's and its prior-image hint's, from the
/// side's source, and carries the change into `T` under the `J` locks. Here
/// it repairs changes capture never saw: a fact moved to another `ga` and
/// one deleted, and the key's only dimension row deleted, which flips the
/// key unmatched.
#[tokio::test]
async fn an_image_less_change_recomputes_the_cells_it_names() {
    for kind in [L, I] {
        let mut lab = lab(Key::Single, &[kind]).await;
        lab.step(&fill(SEED, 1, lab.key)).await;
        uncaptured(
            &lab,
            "public.a",
            "update public.a set ga = 13, x = 7 where id = 101; delete from public.a where id = 102",
        )
        .await;
        lab.d
            .stage_recomputes_with_prior(
                "public.a",
                &[
                    (
                        "101",
                        Some(r#"{"id": 101, "ga": 11, "t": 1, "k": 1, "x": 10}"#.to_string()),
                    ),
                    (
                        "102",
                        Some(r#"{"id": 102, "ga": 12, "t": 1, "k": 1, "x": null}"#.to_string()),
                    ),
                ],
            )
            .await;
        let batch = lab.d.seal().await;
        lab.d.drain(batch, "x").await;
        lab.check(&format!("{}: the a side's recompute", kind.name))
            .await;
        uncaptured(&lab, "public.b", "delete from public.b where id = 101").await;
        lab.d
            .stage_recomputes_with_prior(
                "public.b",
                &[(
                    "101",
                    Some(r#"{"id": 101, "t": 1, "k": 1, "gb": 11, "y": 100}"#.to_string()),
                )],
            )
            .await;
        let batch = lab.d.seal().await;
        lab.d.drain(batch, "x").await;
        lab.check(&format!("{}: the b side's recompute", kind.name))
            .await;
    }
}

impl Lab {
    /// Holds `key` of `table` for every target, as an eviction would: from
    /// now on every page parks its changes for them.
    async fn hold(&self, table: &str, key: &str) {
        self.d
            .ctl
            .execute(
                "insert into poison (transform_id, src_table, key, last_error) \
                 select id, $1, $2, 'test' from transform_definitions",
                &[&table, &key],
            )
            .await
            .expect("hold the key");
    }

    /// The cells `L`'s held row of `key` names, as `ga/k`, sorted.
    async fn held_cells(&self, key: &str) -> Vec<String> {
        self.d
            .rows(&format!(
                "select (g::jsonb ->> 'ga') || '/' || (g::jsonb ->> 'k') \
                 from poison_held h join transform_definitions d on d.id = h.transform_id, \
                      unnest(h.agg_groups) g \
                 where h.key = '{key}' and d.target_table = 'public.l' order by 1"
            ))
            .await
    }

    /// Releases `key` of `table` for every target, then checks each one
    /// against its `JOIN` before anything drains: the release's recompute,
    /// not the `Recompute` it stages, is what repairs it.
    async fn release(&mut self, table: &str, key: &str, context: &str) {
        for kind in self.kinds.clone() {
            trellis::staging::release_key(self.d.pool(), kind.name, table, key)
                .await
                .expect("release the key");
        }
        self.check_released(context).await;
    }

    /// Checks every target against its `JOIN` before anything drains, then
    /// settles and checks again: the release's own recompute, not the
    /// `Recompute` it stages, is what must repair them.
    async fn check_released(&mut self, context: &str) {
        self.check_applied(context).await;
        for kind in &self.kinds {
            assert_eq!(
                self.d.rows(&kind.actual()).await,
                self.d.rows(&kind.expected(self.key)).await,
                "{context}: target {} (left) differs from its JOIN (right) after the release",
                kind.name
            );
        }
        self.check(context).await;
    }
}

/// A key held for a `JOIN` parks its changes, keeping the cells of their
/// images (`agg_groups`); its release recomputes every one of them and the
/// key's current row's, on either side, and carries the change into `T`.
/// The `a` key moves through three `ga`s and to another join key while
/// held; the `b` key's moves flip its old key unmatched and its new one
/// matched.
#[tokio::test]
async fn a_release_recomputes_every_cell_the_held_key_could_owe() {
    let mut lab = lab(Key::Single, &SIX).await;
    lab.step(&fill(SEED, 1, lab.key)).await;
    lab.step(&fill(
        "insert into public.b values ({id} + 2, {k2}, {g2}, 7)",
        1,
        lab.key,
    ))
    .await;
    lab.hold("public.a", "101").await;
    lab.step("update public.a set ga = 13 where id = 101").await;
    lab.step("update public.a set ga = 14, x = 3 where id = 101")
        .await;
    lab.step("update public.a set k = 5001 where id = 101")
        .await;
    assert_eq!(
        lab.held_cells("101").await,
        ["(11/1)", "(13/1)", "(14/1)", "(14/5001)"],
        "the held row names every cell the parked changes touched"
    );
    lab.release("public.a", "101", "the a key's release").await;
    lab.hold("public.b", "101").await;
    lab.step("update public.b set k = 5001 where id = 101")
        .await;
    lab.step("update public.b set gb = 13 where id = 101").await;
    lab.release("public.b", "101", "the b key's release").await;
}

/// The automatic retry of a held key releases it the same way.
#[tokio::test]
async fn the_automatic_retry_recomputes_the_held_keys_cells() {
    let mut lab = lab(Key::Pair, &[L, I]).await;
    lab.step(&fill(SEED, 1, lab.key)).await;
    lab.hold("public.b", "101").await;
    lab.step(
        "update public.b set gb = 13, y = 3 where id = 101; delete from public.a where id = 101",
    )
    .await;
    for kind in [L, I] {
        let poisoned_at: std::time::SystemTime = lab
            .d
            .ctl
            .query_one(
                "select p.poisoned_at from poison p \
                 join transform_definitions d on d.id = p.transform_id \
                 where p.key = '101' and d.target_table = $1",
                &[&kind.target()],
            )
            .await
            .expect("poisoned_at")
            .get(0);
        trellis::staging::quarantine::retry_held_key(
            lab.d.pool(),
            kind.name,
            "public.b",
            "101",
            poisoned_at,
        )
        .await
        .expect("retry the key");
    }
    lab.check_released("after the retry").await;
}

/// A release is a `J`-locked writer: frozen at each point of its write, a
/// page of the other side on the same key queues behind it once it holds
/// the key's `J` row, and runs through before that; either way both apply
/// exactly (JR1, JR2).
#[tokio::test]
async fn a_release_takes_the_j_lock_against_the_other_sides_pages() {
    let mut lab = lab(Key::Single, &[L]).await;
    let mut n = 0;
    for point in &POINTS[..6] {
        n += 1;
        lab.step(&fill(SEED, n, lab.key)).await;
        let key = (100 * n + 1).to_string();
        lab.hold("public.a", &key).await;
        lab.step(&fill(
            "update public.a set ga = {g3}, x = 4 where id = {id} + 1",
            n,
            lab.key,
        ))
        .await;
        let other = lab
            .sealed(&fill(
                "update public.b set gb = {g2} where id = {id} + 1; \
                 insert into public.b values ({id} + 3, {k}, {g4}, 1)",
                n,
                lab.key,
            ))
            .await;
        let context = format!("release frozen at {point:?}");
        let release_key = key.clone();
        let mut frozen = lab
            .d
            .run_frozen(&[(*point, "public.l")], move |pool| async move {
                trellis::staging::release_key(&pool, "l", "public.a", &release_key).await
            })
            .await;
        let reached = frozen.reached(*point).await;
        let page = lab.d.drain_frozen(other, "y", &[]).await;
        let queued = lab.queued_or_done(&page, reached.backend_pid).await;
        assert_eq!(
            queued,
            holds_j(*point, false),
            "{context}: did the page queue?"
        );
        lab.d.release(&mut frozen, *point).await;
        frozen.finish().await;
        page.finish().await;
        lab.check(&context).await;
    }
}

// ------------------------------------------------------------- family 8

/// When J1 family 8's late change to the recomputed row commits, relative
/// to the recompute: before either page starts (`Pre`); while one of the two
/// pages is frozen (`Mid`); after both committed (`Post`); or in a
/// transaction opened before either page starts and committed after both,
/// so it is in progress at the recompute's snapshot (`Xip`). Its own page
/// drains after both, whichever it is.
#[derive(Debug, Clone, Copy)]
enum Late {
    Pre,
    Mid,
    Post,
    Xip,
}

/// One side's recompute in a J1 family 8 scenario (templates for [`fill`]):
/// the change capture never sees, which the recompute repairs; whether the
/// recompute carries the row's prior image (the row is gone, so only the
/// hint names its cell); the late change of the recomputed cell; and a page
/// of the other side on the same key.
struct Recompute {
    name: &'static str,
    table: &'static str,
    drift: &'static str,
    prior: bool,
    late: &'static str,
    other: &'static str,
}

const OTHER_A: &str = "update public.a set ga = {g3}, x = x + 1 where id = {id} + 2; \
                       insert into public.a values ({id} + 3, {g4}, {k}, 2)";

const RECOMPUTES: [Recompute; 3] = [
    Recompute {
        name: "a P cell's recompute against a b page",
        table: "public.a",
        drift: "update public.a set x = x + 5 where id = {id} + 1",
        prior: false,
        late: "update public.a set x = x + 100 where id = {id} + 1",
        other: "update public.b set gb = {g2}, y = y + 1 where id = {id} + 1",
    },
    Recompute {
        name: "an M cell's recompute against an a page",
        table: "public.b",
        drift: "update public.b set y = y + 5 where id = {id} + 1",
        prior: false,
        late: "update public.b set y = y + 100 where id = {id} + 1",
        other: OTHER_A,
    },
    Recompute {
        name: "an M recompute that crosses matches to 0, against an a page",
        table: "public.b",
        drift: "delete from public.b where id = {id} + 1",
        prior: true,
        late: "insert into public.b values ({id} + 4, {k}, {g1}, 7)",
        other: OTHER_A,
    },
];

impl Recompute {
    /// Stages the recompute of scenario `n`'s row `{id} + 1` and seals it
    /// into a batch of its own.
    async fn sealed(&self, lab: &mut Lab, n: i32) -> i64 {
        let row = (100 * n + 1).to_string();
        // SEED's row `{id} + 1` of `b`, the only one of its key.
        let prior = self.prior.then(|| {
            format!(
                r#"{{"id": {row}, "t": {n}, "k": {n}, "gb": {}, "y": 100}}"#,
                10 * n + 1
            )
        });
        lab.d
            .stage_recomputes_with_prior(self.table, &[(row.as_str(), prior)])
            .await;
        lab.d.seal().await
    }
}

/// One J1 family 8 scenario on key `n`: `r`'s recompute and a page of the
/// other side on the same key, one frozen at `point` while the other
/// drains (queued behind it once it holds the key's `J` row), with the late
/// change committed at `late`.
async fn recompute_race(
    lab: &mut Lab,
    n: i32,
    r: &Recompute,
    late: Late,
    recompute_frozen: bool,
    point: PausePoint,
) {
    let context = format!(
        "{:?}/{}: {}, late change {late:?}, {} frozen at {point:?}",
        lab.key,
        lab.kinds[0].name,
        r.name,
        if recompute_frozen {
            "the recompute"
        } else {
            "the other side's page"
        }
    );
    let key = lab.key;
    lab.step(&fill(SEED, n, key)).await;
    uncaptured(lab, r.table, &fill(r.drift, n, key)).await;
    let recompute = r.sealed(lab, n).await;
    let other = lab.sealed(&fill(r.other, n, key)).await;
    let xip = match late {
        Late::Pre => {
            lab.write(&fill(r.late, n, key)).await;
            None
        }
        Late::Xip => {
            let client = lab.d.user().await;
            client
                .batch_execute(&format!("begin; {}", fill(r.late, n, key)))
                .await
                .expect("open the late transaction");
            Some(client)
        }
        _ => None,
    };
    let (first, second) = if recompute_frozen {
        (recompute, other)
    } else {
        (other, recompute)
    };
    let target = lab.kinds[0].target();
    let mut frozen = lab
        .d
        .drain_frozen(first, "x", &[(point, target.as_str())])
        .await;
    let reached = frozen.reached(point).await;
    let page = lab.d.drain_frozen(second, "y", &[]).await;
    let queued = lab.queued_or_done(&page, reached.backend_pid).await;
    assert_eq!(
        queued,
        holds_j(point, false),
        "{context}: does the second page queue behind the frozen one?"
    );
    if let Late::Mid = late {
        lab.write(&fill(r.late, n, key)).await;
    }
    lab.d.release(&mut frozen, point).await;
    frozen.finish().await;
    page.finish().await;
    lab.check_applied(&context).await;
    match late {
        Late::Post => lab.write(&fill(r.late, n, key)).await,
        Late::Xip => xip
            .expect("the late transaction")
            .batch_execute("commit")
            .await
            .expect("commit the late transaction"),
        Late::Pre | Late::Mid => {}
    }
    // The late change's page: refused where the recompute's basis covers
    // it, applied as a delta where it doesn't.
    lab.check(&context).await;
}

/// J1 family 8: a `P` cell's recompute against a page of `b` on the same
/// key, and an `M` cell's (one of them crossing `J.matches` to 0) against a
/// page of `a`, either one frozen at each point before its commit, with a
/// late change of the recomputed cell committed before, during, after or
/// across the recompute. The recompute is an image-less change's (a
/// `Recompute` ring row), which repairs a change capture never saw.
/// [`a_release_takes_the_j_lock_against_the_other_sides_pages`] pins the
/// same lock order for a held key's release.
async fn recomputes_vs_the_other_side(key: Key, kind: Kind) {
    let mut lab = lab(key, &[kind]).await;
    let mut n = 0;
    for r in &RECOMPUTES {
        for late in [Late::Pre, Late::Mid, Late::Post, Late::Xip] {
            for recompute_frozen in [true, false] {
                for point in &POINTS[..6] {
                    n += 1;
                    recompute_race(&mut lab, n, r, late, recompute_frozen, *point).await;
                }
            }
        }
    }
}

#[tokio::test]
async fn family8_recomputes_vs_the_other_side_left() {
    recomputes_vs_the_other_side(Key::Single, L).await;
}

#[tokio::test]
async fn family8_recomputes_vs_the_other_side_inner_pair_key() {
    recomputes_vs_the_other_side(Key::Pair, I).await;
}

/// J1 family 8: a cell recomputed twice around a captured change of it,
/// with capture missing a change before each recompute. The change commits
/// after the first recompute and before the second; its page drains between
/// the two recomputes' pages (applied as a delta, then rewritten by the
/// second) or after both (refused by the second's basis), and the end is
/// the oracle either way.
#[tokio::test]
async fn family8_a_cell_recomputed_twice_around_a_change() {
    let mut lab = lab(Key::Single, &[L, I]).await;
    let mut n = 0;
    for r in &RECOMPUTES[..2] {
        for change_between in [true, false] {
            n += 1;
            let context = format!("{}, the change drained between: {change_between}", r.name);
            lab.step(&fill(SEED, n, lab.key)).await;
            uncaptured(&lab, r.table, &fill(r.drift, n, lab.key)).await;
            let first = r.sealed(&mut lab, n).await;
            lab.d.drain(first, "x").await;
            lab.check_applied(&context).await;
            let change = lab.sealed(&fill(r.late, n, lab.key)).await;
            uncaptured(&lab, r.table, &fill(r.drift, n, lab.key)).await;
            let second = r.sealed(&mut lab, n).await;
            if change_between {
                lab.d.drain(change, "x").await;
                lab.check_applied(&context).await;
                lab.d.drain(second, "x").await;
            } else {
                lab.d.drain(second, "x").await;
                lab.check_applied(&context).await;
                lab.d.drain(change, "x").await;
            }
            lab.check(&context).await;
        }
    }
}

// ------------------------------------------------------------ family 13

/// J1 family 13: a recompute takes cells of both sides to zero while the
/// captured deletes that emptied them are still undrained, and tombstone
/// collection runs before those pages drain. A zero cell's basis is what
/// refuses its stale pages, so no collection may remove a zero cell before
/// the drained prefix passes them: the cells and their basis outlive the
/// collection, and the stale pages are refused rather than applied a second
/// time. A collection that ignored the drained prefix (J1's `S13.gc-early`
/// control) fails both checks.
#[tokio::test]
async fn family13_zero_cells_outlive_collection_until_their_stale_pages_drain() {
    for key in [Key::Single, Key::Pair] {
        let mut lab = lab(key, &[L, I]).await;
        lab.step(&fill(SEED, 1, key)).await;
        let stale = lab
            .sealed(
                "delete from public.a where id in (101, 102); \
                 delete from public.b where id = 101",
            )
            .await;
        lab.d
            .stage_recomputes_with_prior(
                "public.a",
                &[
                    (
                        "101",
                        Some(r#"{"id": 101, "ga": 11, "t": 1, "k": 1, "x": 10}"#.to_string()),
                    ),
                    (
                        "102",
                        Some(r#"{"id": 102, "ga": 12, "t": 1, "k": 1, "x": null}"#.to_string()),
                    ),
                ],
            )
            .await;
        lab.d
            .stage_recomputes_with_prior(
                "public.b",
                &[(
                    "101",
                    Some(r#"{"id": 101, "t": 1, "k": 1, "gb": 11, "y": 100}"#.to_string()),
                )],
            )
            .await;
        let recompute = lab.d.seal().await;
        lab.d.drain(recompute, "r").await;
        lab.check_applied("after the recompute").await;
        lab.d.retire().await;
        lab.d.collect_tombstones().await;
        for kind in [L, I] {
            for (part, cells) in [("p", 2), ("m", 1)] {
                let hidden = lab.hidden(kind, part).await;
                assert_eq!(
                    lab.scalar(&format!(
                        "select count(*) from {hidden} \
                         where k = 1 and __n = 0 and __basis is not null"
                    ))
                    .await,
                    cells.to_string(),
                    "{key:?}/{}: {part}'s zero cells stay through the collection",
                    kind.name
                );
            }
        }
        lab.d.drain(stale, "stale").await;
        lab.check(&format!("{key:?}: the stale pages drained"))
            .await;
    }
}

/// A `JOIN` whose side is a partitioned root with no rows builds like any
/// other (there is nothing to bound); with rows it builds per leaf
/// ("Partitioned sides" below).
#[tokio::test]
async fn a_partitioned_side_with_no_rows_builds_whole() {
    trellis::dev::staging::set_group_basis(true);
    let ddl = "create table public.a (id integer not null, ga integer, t integer, \
                   k integer not null, x integer, primary key (k, id)) partition by list (k); \
               create table public.a0 partition of public.a for values in (1, 2); \
               create table public.a1 partition of public.a default; \
               create table public.b (id integer primary key, t integer, k integer, \
                   gb integer, y integer); create index on public.b (k)";
    let d = Driver::start_unbuilt(
        &[],
        ddl,
        &[
            ("id", int4()),
            ("ga", int4()),
            ("t", int4()),
            ("k", int4()),
            ("x", int4()),
        ],
        &[
            "TRANSFORM l FROM public.a LEFT JOIN public.b ON a.k = b.k GROUP BY a.ga \
          WITHOUT SOURCE INDEX SELECT a.ga AS ga, COUNT(*) AS n",
        ],
        &["public.a", "public.b"],
    )
    .await;
    let lab = Lab {
        d,
        key: Key::Single,
        kinds: vec![L],
    };
    lab.build().await;
    lab.assert_built("a partitioned side with no rows").await;
}

/// Each changed `T` group reaches the target-mutation seam with its prior
/// image, so a transform reading the `JOIN` target follows it: a group
/// written, changed, moved and deleted. (An `INNER` join, whose groups here
/// have no `NULL` part: a 1-1 reader can't hold one, #1141.)
#[tokio::test]
async fn a_join_target_feeds_its_readers_through_the_seam() {
    trellis::dev::staging::set_group_basis(true);
    let int8 = ValueType::Integer(IntWidth::Int8);
    let mut d = Driver::start(
        DDL,
        &[
            ("id", int4()),
            ("ga", int4()),
            ("t", int4()),
            ("k", int4()),
            ("x", int4()),
        ],
        &[&I.definition(Key::Single)],
        &["public.a", "public.b"],
    )
    .await;
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM l2 FROM public.i SELECT n AS n, sx AS sx",
        &[("n".to_string(), int8), ("sx".to_string(), int8)].into(),
        "public",
    )
    .await
    .expect("define the reader");
    trellis::intake::markers::settle_registrations(d.pool()).await;
    for sql in [
        "insert into public.a values (1, 1, 0, 1, 10), (2, 2, 0, 2, 5); \
         insert into public.b values (1, 0, 1, 7, 100), (2, 0, 2, 7, 1)",
        "update public.b set gb = 8 where id = 1; insert into public.a values (3, 1, 0, 1, 1)",
        "delete from public.b where id = 1",
        "delete from public.a where k = 1",
    ] {
        d.user().await.batch_execute(sql).await.expect(sql);
        d.settle().await;
        assert_eq!(
            d.rows("select ga, gb, n, sx from public.l2 order by ga, gb")
                .await,
            d.rows("select ga, gb, n, sx from public.i order by ga, gb")
                .await,
            "after {sql}"
        );
    }
}

/// A `TRUNCATE` of the joined side refills `T` with each fact's unmatched
/// term, its joined-side group columns `NULL` whatever their types: here
/// `timestamptz` and `numeric(10, 2)`, on a two-column `text`/`integer`
/// key, with a fact whose key has a `NULL` part.
#[tokio::test]
async fn a_truncate_of_the_joined_side_refills_typed_unmatched_groups() {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(
        "create table public.o (id integer primary key, cid text, rid integer, \
                                region text, amount numeric(10, 2)); \
         create table public.c (id integer primary key, cid text, rid integer, \
                                at timestamptz, price numeric(10, 2), w bigint); \
         create index on public.o (cid, rid); create index on public.c (cid, rid)",
        &[
            ("id", int4()),
            ("cid", ValueType::Text),
            ("rid", int4()),
            ("region", ValueType::Text),
            ("amount", ValueType::Numeric),
        ],
        &[
            "TRANSFORM r FROM public.o LEFT JOIN public.c ON o.cid = c.cid AND o.rid = c.rid \
             GROUP BY o.region, c.at, c.price \
            WITHOUT SOURCE INDEX SELECT o.region AS region, c.at AS at, c.price AS price, COUNT(*) AS n, \
                    SUM(o.amount) AS revenue, SUM(c.w) AS w",
        ],
        &["public.o", "public.c"],
    )
    .await;
    let actual = "select region, at::text, price::text, n, revenue::text, w::text \
                  from public.r order by 1, 2, 3";
    let expected = "select o.region, c.at::text, c.price::text, count(*), sum(o.amount)::text, \
                           sum(c.w)::text \
                    from public.o left join public.c on o.cid = c.cid and o.rid = c.rid \
                    group by o.region, c.at, c.price order by 1, 2, 3";
    for sql in [
        "insert into public.o values (1, 'x', 1, 'emea', 1.5), (2, 'x', 1, 'emea', 2.25), \
                                     (3, 'y', 2, 'apac', 4), (4, null, 2, 'apac', 3), \
                                     (5, 'z', 9, null, 1)",
        "insert into public.c values (1, 'x', 1, '2026-01-01 00:00+00', 9.99, 7), \
                                     (2, 'x', 1, '2026-02-01 00:00+00', 1.10, 8), \
                                     (3, 'y', 2, '2026-03-01 00:00+00', null, 9)",
        "truncate public.c",
        "insert into public.c values (4, 'y', 2, '2026-04-01 00:00+00', 5.00, 1)",
        "begin; truncate public.c; \
         insert into public.c values (5, 'x', 1, '2026-05-01 00:00+00', 2.00, 2); commit",
        "truncate public.o",
        "insert into public.o values (6, 'x', 1, 'emea', 1), (7, 'q', 1, 'emea', 2)",
    ] {
        d.user().await.batch_execute(sql).await.expect(sql);
        let batch = d.seal().await;
        d.drain(batch, "x").await;
        assert_eq!(d.rows(actual).await, d.rows(expected).await, "after {sql}");
    }
}

/// A page that both applies captured changes to a cell and recomputes it:
/// the cell gets two delta rows (the apply's and the recompute's), which
/// the product sums, so `P_old`/`M_old` and the `LEFT` flip terms see the
/// cell's whole change once. On each side, with a drift only the
/// recompute repairs, a captured change to the recomputed key itself, a
/// captured change of another row in the same cell, and on `b` a flip to
/// unmatched hidden behind a captured insert and delete in the same page.
#[tokio::test]
async fn a_page_that_applies_and_recomputes_one_cell_counts_it_once() {
    let mut lab = lab(Key::Single, &SIX).await;
    for n in 1..=3 {
        lab.step(&fill(SEED, n, lab.key)).await;
    }
    uncaptured(&lab, "public.a", "update public.a set x = 7 where id = 101").await;
    lab.write(
        "insert into public.a values (103, 11, 1, 1, 5); \
         update public.a set x = 9 where id = 101",
    )
    .await;
    lab.d
        .stage_recomputes_with_prior(
            "public.a",
            &[(
                "101",
                Some(r#"{"id": 101, "ga": 11, "t": 1, "k": 1, "x": 10}"#.to_string()),
            )],
        )
        .await;
    let batch = lab.d.seal().await;
    lab.d.drain(batch, "x").await;
    lab.check("a: apply and recompute of one P cell").await;
    uncaptured(
        &lab,
        "public.b",
        "delete from public.b where id in (101, 201, 301)",
    )
    .await;
    lab.write(
        "insert into public.b values (103, 1, 1, 11, 5), (203, 2, 2, 22, 5), (303, 3, 3, 31, 5); \
         delete from public.b where id = 303",
    )
    .await;
    lab.d
        .stage_recomputes_with_prior(
            "public.b",
            &[
                (
                    "101",
                    Some(r#"{"id": 101, "t": 1, "k": 1, "gb": 11, "y": 100}"#.to_string()),
                ),
                (
                    "201",
                    Some(r#"{"id": 201, "t": 2, "k": 2, "gb": 21, "y": 100}"#.to_string()),
                ),
                (
                    "301",
                    Some(r#"{"id": 301, "t": 3, "k": 3, "gb": 31, "y": 100}"#.to_string()),
                ),
            ],
        )
        .await;
    let batch = lab.d.seal().await;
    lab.d.drain(batch, "x").await;
    lab.check("b: apply and recompute of one M cell").await;
}

// ------------------------------------------------------------ the build

// A `JOIN`'s fresh build, rebuild in place and `RESUME` (#1109, #1097's
// decision): both structures in one transaction (`join::build`), run here by
// hand from the plan job (a fresh build or a `RESUME`) or the sweep job (a
// rebuild in place), frozen at the pause points of the build's transaction.
// Every scenario ends at the oracle: each target equals its `JOIN … GROUP
// BY`, `P` and `M` each side's `GROUP BY`, `J.matches = Σ M.n`, and
// `T = Σ_k P ⊗ M`.

/// Rows for the tables a build is defined over: duplicates in a cell, `NULL`
/// join keys on both sides (a fact's key is a cell of `P`; a dimension's
/// joins nothing), `NULL` group columns and values, a key only one side
/// has, and decoys on a second `t` for the pair key.
const BUILD_SEED: &str = "\
    insert into public.a values (1, 1, 0, 1, 10), (2, 1, 0, 1, 5), (3, 2, 0, 2, 7), \
        (4, 2, 0, 3, null), (5, 3, 0, null, 4), (6, null, 0, 2, 1), (7, 1, 1, 1, 100), \
        (8, 4, 0, 8, 2); \
    insert into public.b values (1, 0, 1, 10, 3), (2, 0, 1, 11, 4), (3, 0, 2, 10, null), \
        (4, 0, null, 12, 5), (5, 0, 9, 13, 6), (6, 1, 1, 14, 50), (7, 0, 1, 10, 1)";

/// A lab whose tables hold `seed` before `kinds` are defined over them, and
/// whose builds have started: each definition is `backfilling` with its plan
/// job unclaimed.
async fn seeded(key: Key, kinds: &[Kind], seed: &str) -> Lab {
    seeded_in(DDL, key, kinds, seed).await
}

/// [`seeded`] over tables created by `ddl`.
async fn seeded_in(ddl: &str, key: Key, kinds: &[Kind], seed: &str) -> Lab {
    trellis::dev::staging::set_group_basis(true);
    let definitions: Vec<String> = kinds.iter().map(|k| k.definition(key)).collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let d = Driver::start_unbuilt(
        &[],
        &format!("{ddl}; {seed}"),
        &[
            ("id", int4()),
            ("ga", int4()),
            ("t", int4()),
            ("k", int4()),
            ("x", int4()),
        ],
        &definitions,
        &["public.a", "public.b"],
    )
    .await;
    let mut lab = Lab {
        d,
        key,
        kinds: kinds.to_vec(),
    };
    if ddl.contains("partition by") {
        // The capture pass records a partitioned root's tree, which a
        // per-leaf build routes by.
        lab.pass().await;
    }
    lab
}

const BUILD_OPTIONS: trellis::staging::build::WorkerOptions =
    trellis::staging::build::WorkerOptions {
        chunk_rows: 1_000,
        drain_batch_cap: usize::MAX,
        heartbeat_interval: Duration::from_secs(5),
        reclaim_ttl: Duration::from_secs(60),
    };

impl Lab {
    /// Claims the one job of `kind` (`plan` for a fresh build or a `RESUME`,
    /// `sweep` for a rebuild in place) and runs it in its own task, frozen at
    /// each of `points` on `kind`'s target once reached.
    async fn build_frozen(
        &self,
        job: &'static str,
        points: &[PausePoint],
    ) -> drain_driver::Running<()> {
        let target = self.kinds[0].target();
        let points: Vec<(PausePoint, &str)> =
            points.iter().map(|p| (*p, target.as_str())).collect();
        self.d
            .run_frozen(&points, |pool| async move {
                let claimed = trellis::defs::chunk_queue::claim_chunks_of(
                    &**pool.get().await.expect("pool"),
                    "build",
                    1,
                    &[job],
                )
                .await
                .expect("claim the build");
                assert_eq!(claimed.len(), 1, "one {job} job to claim");
                trellis::staging::build::run_claimed(&pool, &claimed[0], "build", &BUILD_OPTIONS)
                    .await;
                Ok(())
            })
            .await
    }

    /// Runs every build to its end, as the staging worker does.
    async fn build(&self) {
        trellis::staging::build::settle_builds(self.d.pool()).await;
    }

    async fn statuses(&self) -> Vec<String> {
        self.d
            .rows("select status::text from transform_definitions order by id")
            .await
    }

    /// Every definition is `live` and no job is left.
    async fn assert_built(&self, context: &str) {
        let errors = self
            .d
            .rows("select last_error from backfill_chunks where last_error is not null")
            .await;
        assert!(errors.is_empty(), "{context}: a job failed: {errors:?}");
        assert_eq!(
            self.statuses().await,
            vec!["(live)"; self.kinds.len()],
            "{context}: every definition is live"
        );
        assert_eq!(
            self.d
                .rows("select kind, last_error from backfill_chunks")
                .await,
            Vec::<String>::new(),
            "{context}: no job is left"
        );
    }
}

/// Test 1: a fresh build over seeded tables, on every kind and both key
/// shapes, ends `live` with the oracle's rows and hidden structures.
async fn fresh_build(key: Key) {
    let mut lab = seeded(key, &SIX, BUILD_SEED).await;
    assert_eq!(
        lab.statuses().await,
        vec!["(backfilling)"; SIX.len()],
        "the builds started"
    );
    lab.build().await;
    lab.assert_built("fresh build").await;
    lab.check("fresh build").await;
}

#[tokio::test]
async fn build_fresh_over_seeded_tables() {
    fresh_build(Key::Single).await;
}

#[tokio::test]
async fn build_fresh_over_seeded_tables_pair_key() {
    fresh_build(Key::Pair).await;
}

/// What a page between a build's start and its commit changes, and from
/// which pause point of the build on it queues behind the build's locks (a
/// key it names is held from `AfterJoinLock`, a cell with a `NULL` key part
/// from `AfterEntryLock`; a key or cell the enumeration didn't see is not
/// held, but its `T` group may be, from `AfterJoinTargetLock`).
const AROUND: [(&str, &str, &str, Option<PausePoint>); 8] = [
    (
        "a: a row of an enumerated cell",
        "update public.a set x = x + 1 where id = 1",
        "update public.a set x = x + 1 where id = 2",
        Some(PausePoint::AfterJoinLock),
    ),
    (
        "a: a new cell on an enumerated key",
        "insert into public.a values (32, 77, 0, 1, 1)",
        "insert into public.a values (33, 78, 0, 1, 1)",
        Some(PausePoint::AfterJoinLock),
    ),
    (
        "a: a new key",
        "insert into public.a values (30, 1, 0, 40, 1)",
        "insert into public.a values (34, 1, 0, 41, 1)",
        None,
    ),
    (
        "a: a cell with a NULL key",
        "update public.a set x = 9 where id = 5",
        "update public.a set x = 10 where id = 5",
        Some(PausePoint::AfterEntryLock),
    ),
    (
        "a: a row moves between enumerated keys",
        "update public.a set k = 8 where id = 3",
        "update public.a set k = 3 where id = 3",
        Some(PausePoint::AfterJoinLock),
    ),
    (
        "b: a row of an enumerated cell",
        "update public.b set y = 7 where id = 1",
        "update public.b set y = 8 where id = 1",
        Some(PausePoint::AfterJoinLock),
    ),
    (
        "b: a new cell on an enumerated key",
        "insert into public.b values (31, 0, 1, 99, 1)",
        "insert into public.b values (35, 0, 1, 98, 1)",
        Some(PausePoint::AfterJoinLock),
    ),
    (
        "b: the last row of a key goes (a flip to unmatched)",
        "delete from public.b where id = 5",
        "insert into public.b values (36, 0, 9, 15, 2)",
        Some(PausePoint::AfterJoinLock),
    ),
];

const BUILD_POINTS: [PausePoint; 6] = [
    PausePoint::BeforeJoinLock,
    PausePoint::AfterJoinLock,
    PausePoint::AfterEntryLock,
    PausePoint::BetweenJoinSideWrites,
    PausePoint::AfterJoinTargetLock,
    PausePoint::BeforeRebuildCommit,
];

fn at_or_after(point: PausePoint, from: Option<PausePoint>) -> bool {
    from.is_some_and(|from| {
        BUILD_POINTS.iter().position(|p| *p == point)
            >= BUILD_POINTS.iter().position(|p| *p == from)
    })
}

/// Test 2 (F2, G2): changes committed before the build starts and drained
/// after it, and a page drained while the build is frozen at each step,
/// whether it queues behind the build's locks or runs through; the build's
/// commit leaves the oracle's rows either way.
async fn pages_around_a_build(key: Key, kind: Kind) {
    for point in BUILD_POINTS {
        for (name, sql, during_sql, held_from) in AROUND {
            let context = format!("{key:?} {} at {point:?}, {name}", kind.name);
            // A new key's cell is unmatched: its `T` group (`LEFT`'s) is one
            // the build locked after the product.
            let held_from = match (name, kind.left) {
                ("a: a new key", true) => Some(PausePoint::AfterJoinTargetLock),
                _ => held_from,
            };
            let mut lab = seeded(key, &[kind], BUILD_SEED).await;
            // Committed and sealed before the build starts, drained after it
            // commits: the build's snapshot sees it, and its basis refuses
            // the change.
            lab.write(sql).await;
            let before = lab.d.seal().await;
            let mut building = lab.build_frozen("plan", &[point]).await;
            let reached = building.reached(point).await;
            // A second change, on the same shape of cell, while it is frozen.
            lab.write(during_sql).await;
            let during = lab.d.seal().await;
            let page = lab.d.drain_frozen(during, "during", &[]).await;
            let queued = lab.queued_or_done(&page, reached.backend_pid).await;
            assert_eq!(
                queued,
                at_or_after(point, held_from),
                "{context}: the page queues behind the build exactly when it names a lock \
                 the build holds"
            );
            lab.d.release(&mut building, point).await;
            building.finish().await;
            page.finish().await;
            lab.d.drain(before, "before").await;
            lab.assert_built(&context).await;
            lab.check(&context).await;
        }
    }
}

#[tokio::test]
async fn build_pages_around_a_build_left() {
    pages_around_a_build(Key::Single, L).await;
}

#[tokio::test]
async fn build_pages_around_a_build_inner_pair_key() {
    pages_around_a_build(Key::Pair, I).await;
}

/// J1 family 9's writes on both sides of one build, on [`BUILD_SEED`]'s
/// keys (all enumerated): before it starts, while it is frozen (one
/// statement per side), and after its commit.
const BOTH_BEFORE: &str = "update public.a set ga = 2 where id = 2; \
                           insert into public.b values (40, 0, 3, 10, 9)";
const BOTH_DURING_A: &str = "update public.a set x = x + 1 where id = 3; \
                             insert into public.a values (41, 4, 0, 8, 6)";
const BOTH_DURING_B: &str = "update public.b set y = y + 1 where id = 2; \
                             delete from public.b where id = 5";
const BOTH_AFTER: &str = "delete from public.a where id = 1; \
                          update public.b set k = 2 where id = 1";

/// J1 family 9: a build with writes landing on both sides, before it
/// starts, while it is frozen at each step and after its commit. While it
/// is frozen, either a page of each side drains (on keys of their own, so
/// each queues on the build, not on the other) or one mixed page carrying
/// both; each queues behind the build once it holds its `J` rows, and runs
/// through before that. The build's commit and the pages leave the
/// oracle's rows. A fresh build, whose `J` inserts hold the keys, and a
/// rebuild in place, where every `J` row exists and its `J` lock holds them.
async fn writes_on_both_sides_around_a_build(key: Key, kind: Kind, mixed: bool, in_place: bool) {
    for point in BUILD_POINTS {
        let context = format!(
            "{key:?} {} at {point:?}, mixed: {mixed}, in place: {in_place}",
            kind.name
        );
        let held = at_or_after(point, Some(PausePoint::AfterJoinLock));
        let mut lab = seeded(key, &[kind], BUILD_SEED).await;
        let job = if in_place {
            lab.build().await;
            request_rebuild(&lab, "a").await;
            "sweep"
        } else {
            "plan"
        };
        // The ring holds two undrained batches here, and the frozen build
        // lets only one seal through (it is open across the predecessor
        // segment's fence). So with one mixed page during the build, both
        // sides' changes committed before it drain after its commit; with
        // a page per side, the `a` page commits before the build starts
        // and drains while it is frozen, and the `b` page commits during.
        let (before, a) = if mixed {
            (Some(lab.sealed(BOTH_BEFORE).await), None)
        } else {
            (None, Some(lab.sealed(BOTH_DURING_A).await))
        };
        let mut building = lab.build_frozen(job, &[point]).await;
        let reached = building.reached(point).await;
        let pages = if let Some(a) = a {
            let b = lab.sealed(BOTH_DURING_B).await;
            let pages = vec![
                lab.d.drain_frozen(a, "during a", &[]).await,
                lab.d.drain_frozen(b, "during b", &[]).await,
            ];
            lab.both_queued_or_done(&pages, reached.backend_pid, held, &context)
                .await;
            pages
        } else {
            let batch = lab
                .sealed(&format!("{BOTH_DURING_A}; {BOTH_DURING_B}"))
                .await;
            let page = lab.d.drain_frozen(batch, "during", &[]).await;
            assert_eq!(
                lab.queued_or_done(&page, reached.backend_pid).await,
                held,
                "{context}: does the mixed page queue behind the build?"
            );
            vec![page]
        };
        lab.d.release(&mut building, point).await;
        building.finish().await;
        for page in pages {
            page.finish().await;
        }
        let after = lab.sealed(BOTH_AFTER).await;
        lab.d.drain(after, "after").await;
        if let Some(before) = before {
            lab.d.drain(before, "before").await;
        }
        lab.assert_built(&context).await;
        lab.check(&context).await;
    }
}

impl Lab {
    /// Waits until every one of `pages` queued behind backend `pid` (`held`)
    /// or ran to its end (`!held`).
    async fn both_queued_or_done(
        &self,
        pages: &[RunningDrain],
        pid: i32,
        held: bool,
        context: &str,
    ) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let queued: i64 = self
                .d
                .ctl
                .query_one(
                    "select count(*) from pg_stat_activity \
                     where $1 = any(pg_blocking_pids(pid))",
                    &[&pid],
                )
                .await
                .expect("read pg_stat_activity")
                .get(0);
            let finished = pages.iter().filter(|p| p.is_finished()).count();
            if held {
                assert_eq!(finished, 0, "{context}: a page ran through the build");
                if queued == pages.len() as i64 {
                    return;
                }
            } else {
                assert_eq!(queued, 0, "{context}: a page queued behind the build");
                if finished == pages.len() {
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "{context}: the pages neither queued nor finished within 60 s"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

#[tokio::test]
async fn family9_writes_on_both_sides_around_a_build_left() {
    for (mixed, in_place) in [(false, false), (true, false), (false, true), (true, true)] {
        writes_on_both_sides_around_a_build(Key::Single, L, mixed, in_place).await;
    }
}

#[tokio::test]
async fn family9_writes_on_both_sides_around_a_build_inner_pair_key() {
    for (mixed, in_place) in [(false, false), (true, false), (false, true), (true, true)] {
        writes_on_both_sides_around_a_build(Key::Pair, I, mixed, in_place).await;
    }
}

/// J1 family 9's other role: a page of either side, on an enumerated key,
/// frozen at each point of its apply while a fresh build or a rebuild in
/// place starts. The build queues behind the page once the page holds the
/// key's `J` row (from its `J` insert where the build has yet to create the
/// row, from its `J` lock where the row exists) and builds through before
/// that, so the page then meets cells whose basis already counts its change.
/// The end is the oracle either way.
async fn a_build_against_a_frozen_page(key: Key, kind: Kind) {
    for in_place in [false, true] {
        for (side, sql) in [
            ("a", "update public.a set x = x + 1 where id = 1"),
            ("b", "update public.b set y = y + 1 where id = 1"),
        ] {
            for point in POINTS {
                let context = format!(
                    "{key:?} {}: the {side} page frozen at {point:?}, in place: {in_place}",
                    kind.name
                );
                let mut lab = seeded(key, &[kind], BUILD_SEED).await;
                let job = if in_place {
                    lab.build().await;
                    request_rebuild(&lab, "a").await;
                    "sweep"
                } else {
                    "plan"
                };
                let batch = lab.sealed(sql).await;
                let target = kind.target();
                let mut page = lab
                    .d
                    .drain_frozen(batch, "page", &[(point, target.as_str())])
                    .await;
                let reached = page.reached(point).await;
                let building = lab.build_frozen(job, &[]).await;
                assert_eq!(
                    lab.queued_or_done(&building, reached.backend_pid).await,
                    holds_j(point, !in_place),
                    "{context}: does the build queue behind the page?"
                );
                lab.d.release(&mut page, point).await;
                page.finish().await;
                building.finish().await;
                lab.assert_built(&context).await;
                lab.check(&context).await;
            }
        }
    }
}

#[tokio::test]
async fn family9_a_build_against_a_frozen_page_left() {
    a_build_against_a_frozen_page(Key::Single, L).await;
}

#[tokio::test]
async fn family9_a_build_against_a_frozen_page_inner_pair_key() {
    a_build_against_a_frozen_page(Key::Pair, I).await;
}

/// Runs `sql` on `lab`'s connection to `part` of `kind`'s hidden tables.
async fn hidden_rows(
    lab: &Lab,
    kind: Kind,
    part: &str,
    columns: &str,
    filter: &str,
) -> Vec<String> {
    let table = lab.hidden(kind, part).await;
    lab.d
        .rows(&format!("select {columns} from {table} {filter}"))
        .await
}

/// `Trellis::request_backfill` of `table`, the rebuild a user asks for.
async fn request_rebuild(lab: &Lab, table: &str) {
    let trellis = trellis::Trellis::connect(
        trellis::Config::from_dsn(lab.d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    trellis
        .request_backfill(table)
        .await
        .expect("request a rebuild");
}

/// Test 3 (rule 3): a page creates a `P` cell on an enumerated key after the
/// enumeration, before the build's `J` lock. The build's `M` changes that
/// key, so the product has a `T` group for the new cell that the
/// enumeration did not name: it is locked with the product's groups, and its
/// delta lands.
#[tokio::test]
async fn build_t_lock_set_covers_a_cell_created_after_the_enumeration() {
    let mut lab = seeded(Key::Single, &[L], BUILD_SEED).await;
    let mut building = lab
        .build_frozen("plan", &[PausePoint::BeforeJoinLock])
        .await;
    building.reached(PausePoint::BeforeJoinLock).await;
    lab.step("insert into public.a values (32, 77, 0, 1, 1)")
        .await;
    lab.d
        .release(&mut building, PausePoint::BeforeJoinLock)
        .await;
    building.finish().await;
    lab.assert_built("a cell created after the enumeration")
        .await;
    assert_eq!(
        hidden_rows(
            &lab,
            L,
            "t",
            "ga, gb, __n",
            "where ga = 77 and __n <> 0 order by gb"
        )
        .await,
        ["(77,10,2)", "(77,11,1)", "(77,14,1)"],
        "the new cell's T groups hold the page's and the build's deltas"
    );
    lab.check("a cell created after the enumeration").await;
}

/// Test 5 (rule 5): the build writes the cells it enumerated. A key first
/// seen after the enumeration is left to its page, though the build's
/// snapshot sees its row; the page applies it once.
#[tokio::test]
async fn build_writes_only_the_cells_it_enumerated() {
    let mut lab = seeded(Key::Single, &[L], BUILD_SEED).await;
    let mut building = lab
        .build_frozen("plan", &[PausePoint::BeforeJoinLock])
        .await;
    building.reached(PausePoint::BeforeJoinLock).await;
    lab.write("insert into public.a values (30, 1, 0, 40, 1)")
        .await;
    let late = lab.d.seal().await;
    lab.d
        .release(&mut building, PausePoint::BeforeJoinLock)
        .await;
    building.finish().await;
    assert_eq!(
        hidden_rows(&lab, L, "p", "ga, k, __n", "where k = 40").await,
        Vec::<String>::new(),
        "the cell the enumeration didn't see is not written"
    );
    lab.d.drain(late, "late").await;
    assert_eq!(
        hidden_rows(&lab, L, "p", "ga, k, __n", "where k = 40").await,
        ["(1,40,1)"],
        "its page applies it once"
    );
    lab.assert_built("a key first seen after the enumeration")
        .await;
    lab.check("a key first seen after the enumeration").await;
}

/// Test 6 (R8): a cell whose only row moved out, undrained, before the build
/// is a zero cell with a basis, so the later `-1` of the move is refused.
#[tokio::test]
async fn build_enumerates_the_cell_an_undrained_old_image_names() {
    let mut lab = seeded(Key::Single, &[L], BUILD_SEED).await;
    lab.write("update public.a set ga = 9 where id = 4").await;
    let moved = lab.d.seal().await;
    lab.build().await;
    assert_eq!(
        hidden_rows(
            &lab,
            L,
            "p",
            "ga, k, __n, __basis is not null",
            "where ga = 2 and k = 3"
        )
        .await,
        ["(2,3,0,t)"],
        "the old image's cell is zero, with the build's snapshot as its basis"
    );
    lab.d.drain(moved, "moved").await;
    lab.assert_built("an undrained move").await;
    lab.check("an undrained move").await;
}

/// Edits the sources behind capture's back, so `P`, `M` and `T` agree with
/// the old rows and the tables hold new ones: a fact edited, one that was
/// alone in its cell deleted (a cell only the hidden table names), a key's
/// dimension rows deleted (a flip to unmatched) and a dimension row added to
/// a key that had none (a flip to matched).
async fn drift(lab: &Lab) {
    uncaptured(
        lab,
        "public.a",
        "update public.a set x = x + 100 where id = 1; delete from public.a where id = 4",
    )
    .await;
    uncaptured(
        lab,
        "public.b",
        "delete from public.b where k = 1; insert into public.b values (50, 0, 8, 20, 9)",
    )
    .await;
}

/// Test 13: a `request_backfill` of the `FROM` table and of the joined table
/// each rebuild the `JOIN` in place (a sweep job alone), repairing what
/// drifted from the tables, and leave no catch-up marker failing.
async fn rebuild_in_place(key: Key, kind: Kind) {
    for table in ["a", "b"] {
        let context = format!("{key:?} {} rebuild of {table}", kind.name);
        let mut lab = seeded(key, &[kind], BUILD_SEED).await;
        lab.build().await;
        lab.assert_built(&context).await;
        drift(&lab).await;
        // A visible row that disagrees with its partials, and one with no
        // group at all.
        lab.write(&format!(
            "update public.{name} set n = n + 1000; \
             insert into public.{name} ({columns}, n) values ({values}, 1)",
            name = kind.name,
            columns = kind.groups().join(", "),
            values = kind
                .groups()
                .iter()
                .map(|_| "424242")
                .collect::<Vec<_>>()
                .join(", "),
        ))
        .await;
        request_rebuild(&lab, table).await;
        assert_eq!(lab.statuses().await, ["(backfilling)"], "{context}");
        assert_eq!(
            lab.d.rows("select kind from backfill_chunks").await,
            ["(sweep)"],
            "{context}: a JOIN rebuild is its sweep job alone"
        );
        lab.build().await;
        lab.assert_built(&context).await;
        lab.check(&context).await;
        assert_eq!(
            lab.d
                .rows("select table_name from pending_backfill where last_error is not null")
                .await,
            Vec::<String>::new(),
            "{context}: no catch-up marker is failing"
        );
        // The request's marker is discharged without a catch-up of the
        // JOIN, which would fail in the orphan sweep for good.
        trellis::intake::markers::settle_registrations(lab.d.pool()).await;
        assert_eq!(
            lab.d
                .rows("select table_name from pending_backfill where last_error is not null")
                .await,
            Vec::<String>::new(),
            "{context}: the discharge failed no marker"
        );
        lab.assert_built(&context).await;
    }
}

#[tokio::test]
async fn build_rebuild_in_place_left() {
    rebuild_in_place(Key::Single, L).await;
}

#[tokio::test]
async fn build_rebuild_in_place_inner_pair_key() {
    rebuild_in_place(Key::Pair, I).await;
}

/// Test 10 (rule 8, G6): a reader of the target sees the old rows, hand edit
/// and all, until the build's one commit, whichever step it is frozen at, and
/// the oracle's rows after.
#[tokio::test]
async fn build_readers_see_the_old_target_until_the_commit() {
    let mut lab = seeded(Key::Single, &[L], BUILD_SEED).await;
    lab.build().await;
    drift(&lab).await;
    lab.write("update public.l set n = n + 1000").await;
    let old = lab.d.rows(&L.actual()).await;
    request_rebuild(&lab, "a").await;
    let points = [
        PausePoint::AfterJoinLock,
        PausePoint::BetweenJoinSideWrites,
        PausePoint::AfterJoinTargetLock,
        PausePoint::BeforeRebuildCommit,
    ];
    let mut building = lab.build_frozen("sweep", &points).await;
    for point in points {
        building.reached(point).await;
        assert_eq!(
            lab.d.rows(&L.actual()).await,
            old,
            "a reader sees the old target at {point:?}"
        );
        lab.d.release(&mut building, point).await;
    }
    building.finish().await;
    lab.assert_built("a rebuild under a reader").await;
    lab.check("a rebuild under a reader").await;
    assert_ne!(lab.d.rows(&L.actual()).await, old);
}

/// Test 9 (rule 7): a `TRUNCATE` whose page drains after the build wipes what
/// the build wrote, and the barrier reapplies what follows it.
#[tokio::test]
async fn build_truncates_committed_before_the_build_drain_after_it() {
    for table in ["a", "b"] {
        let mut lab = seeded(Key::Single, &[L], BUILD_SEED).await;
        lab.write(&format!("truncate public.{table}")).await;
        let truncated = lab.d.seal().await;
        lab.write(&format!(
            "insert into public.{table} values (60, 1, 0, 1, 5)"
        ))
        .await;
        let after = lab.d.seal().await;
        lab.build().await;
        lab.assert_built("truncate before the build").await;
        lab.d.drain(truncated, "truncate").await;
        lab.d.drain(after, "after").await;
        lab.check(&format!("truncate of {table} before the build"))
            .await;
    }
}

/// Test 9 (rule 7): a `TRUNCATE` statement issued while the build holds its
/// tables queues behind the build's `ACCESS SHARE` and runs after its commit;
/// its page then clears what the build wrote.
#[tokio::test]
async fn build_a_truncate_statement_queues_behind_the_build() {
    let mut lab = seeded(Key::Single, &[L], BUILD_SEED).await;
    let mut building = lab
        .build_frozen("plan", &[PausePoint::AfterEntryLock])
        .await;
    let reached = building.reached(PausePoint::AfterEntryLock).await;
    let user = lab.d.user().await;
    let truncating = tokio::spawn(async move { user.batch_execute("truncate public.a").await });
    lab.d.wait_blocked_behind(reached.backend_pid).await;
    assert!(!truncating.is_finished(), "the TRUNCATE queues");
    lab.d
        .release(&mut building, PausePoint::AfterEntryLock)
        .await;
    building.finish().await;
    truncating
        .await
        .expect("task")
        .expect("the truncate runs after the build");
    lab.d.settle().await;
    lab.assert_built("a truncate during the build").await;
    lab.check("a truncate during the build").await;
    assert_eq!(
        lab.d.rows("select count(*) from public.l").await,
        ["(0)"],
        "the truncate's page cleared what the build wrote"
    );
}

/// Test 9 (rule 7): a page carrying a `TRUNCATE` holds the lock row `FOR
/// UPDATE`, so a build that starts meanwhile waits for it, then builds from
/// the emptied table.
#[tokio::test]
async fn build_waits_for_a_truncate_page_holding_the_lock_row() {
    let mut lab = seeded(Key::Single, &[L], BUILD_SEED).await;
    lab.write("truncate public.b").await;
    let truncated = lab.d.seal().await;
    let target = L.target();
    let mut page = lab
        .d
        .drain_frozen(
            truncated,
            "truncate",
            &[(PausePoint::BeforeJoinLock, target.as_str())],
        )
        .await;
    let reached = page.reached(PausePoint::BeforeJoinLock).await;
    let building = lab.build_frozen("plan", &[]).await;
    lab.d.wait_blocked_behind(reached.backend_pid).await;
    assert!(!building.is_finished(), "the build waits for the page");
    lab.d.release(&mut page, PausePoint::BeforeJoinLock).await;
    page.finish().await;
    building.finish().await;
    lab.assert_built("a build behind a truncate page").await;
    lab.check("a build behind a truncate page").await;
}

/// Pauses `kind`'s definition the way a capture disruption does, by hand.
async fn pause(lab: &Lab, kind: Kind) {
    lab.write(&format!(
        "update transform_definitions set status = 'paused' where target_table = 'public.{}'",
        kind.name
    ))
    .await;
}

/// Resumes `kind`'s definition and starts its build (`backfilling`, its plan
/// job unclaimed).
async fn resume(lab: &Lab, kind: Kind) {
    trellis::staging::quarantine::resume_transform(lab.d.pool(), kind.name)
        .await
        .expect("resume");
    trellis::intake::markers::discharge_registrations(lab.d.pool())
        .await
        .expect("start the build");
    assert_eq!(reader_status(lab, kind.name).await, ["(backfilling)"]);
}

/// Test 11: a `RESUME` keeps the visible target, recreates the hidden tables
/// and builds them. The definition was paused through writes on both sides (a
/// group emptied among them) and a hand edit of the target and of `T`; frozen
/// before its commit, a reader sees the old rows, and after it the oracle's:
/// the emptied group's visible row, which no `T` group names, is gone.
async fn resume_keeps_the_target(key: Key, kind: Kind) {
    let mut lab = seeded(key, &[kind], BUILD_SEED).await;
    lab.build().await;
    lab.assert_built("before the pause").await;
    pause(&lab, kind).await;
    for sql in [
        "insert into public.a values (70, 5, 0, 1, 1); insert into public.b values (70, 0, 3, 15, 1)",
        "update public.a set x = 99 where id = 2; update public.b set y = 33 where id = 2",
        "update public.a set k = 9 where id = 3; delete from public.b where id = 7",
        "delete from public.a where id = 5",
        "delete from public.b where k = 2",
    ] {
        lab.step(sql).await;
    }
    let t = lab.hidden(kind, "t").await;
    lab.write(&format!(
        "update public.{} set n = n + 5; update {t} set __n = __n + 5",
        kind.name
    ))
    .await;
    let old = lab.d.rows(&kind.actual()).await;
    resume(&lab, kind).await;
    let mut building = lab
        .build_frozen("plan", &[PausePoint::BeforeRebuildCommit])
        .await;
    building.reached(PausePoint::BeforeRebuildCommit).await;
    assert_eq!(
        lab.d.rows(&kind.actual()).await,
        old,
        "a reader sees the old rows until the commit"
    );
    lab.d
        .release(&mut building, PausePoint::BeforeRebuildCommit)
        .await;
    building.finish().await;
    lab.assert_built("the resume").await;
    lab.check("the resume").await;
}

#[tokio::test]
async fn build_resume_keeps_the_target_left() {
    resume_keeps_the_target(Key::Single, L).await;
}

#[tokio::test]
async fn build_resume_keeps_the_target_inner_pair_key() {
    resume_keeps_the_target(Key::Pair, I).await;
}

/// A `RESUME` over two empty tables removes the visible rows the old build
/// left (nothing else ever would): the target ends empty.
#[tokio::test]
async fn build_resume_over_two_empty_tables_removes_the_old_rows() {
    let mut lab = seeded(Key::Single, &[L], BUILD_SEED).await;
    lab.build().await;
    pause(&lab, L).await;
    lab.step("delete from public.a; delete from public.b").await;
    assert_ne!(
        lab.d.rows("select count(*) from public.l").await,
        ["(0)"],
        "the old rows are still there"
    );
    resume(&lab, L).await;
    lab.build().await;
    lab.assert_built("a resume over empty tables").await;
    assert_eq!(
        lab.d.rows("select * from public.l").await,
        Vec::<String>::new()
    );
    lab.check("a resume over empty tables").await;
}

/// Test 14: a job that dies before its commit leaves nothing and runs again
/// whole; a `PAUSE` and `DROP TRANSFORM` meanwhile wait for it and log no
/// deadlock.
#[tokio::test]
async fn build_job_lifecycle() {
    let mut lab = seeded(Key::Single, &[L], BUILD_SEED).await;
    let building = {
        let mut building = lab
            .build_frozen("plan", &[PausePoint::AfterEntryLock])
            .await;
        building.reached(PausePoint::AfterEntryLock).await;
        building
    };
    building.kill().await;
    lab.write("update backfill_chunks set claimed_by = null, claimed_at = null")
        .await;
    assert_eq!(
        hidden_rows(&lab, L, "p", "count(*)", "").await,
        ["(0)"],
        "a job that died before its commit wrote nothing"
    );
    lab.build().await;
    lab.assert_built("a job that died").await;
    lab.check("a job that died").await;

    // A second rebuild changes nothing.
    request_rebuild(&lab, "a").await;
    lab.build().await;
    lab.assert_built("a second build").await;
    lab.check("a second build").await;

    // A PAUSE waits for a build in flight.
    request_rebuild(&lab, "b").await;
    let mut building = lab
        .build_frozen("sweep", &[PausePoint::AfterEntryLock])
        .await;
    let reached = building.reached(PausePoint::AfterEntryLock).await;
    let trellis = trellis::Trellis::connect(
        trellis::Config::from_dsn(lab.d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect");
    let pausing = tokio::spawn(async move {
        trellis.apply("PAUSE TRANSFORM l").await?;
        trellis.apply("DROP TRANSFORM l").await
    });
    lab.d.wait_blocked_behind(reached.backend_pid).await;
    assert!(!pausing.is_finished(), "the PAUSE waits for the build");
    lab.d
        .release(&mut building, PausePoint::AfterEntryLock)
        .await;
    building.finish().await;
    pausing
        .await
        .expect("task")
        .expect("the PAUSE and DROP run after the build");
    assert_eq!(
        lab.d
            .rows("select count(*) from transform_definitions")
            .await,
        ["(0)"]
    );
    assert_eq!(lab.d.deadlocks_logged(), Vec::<String>::new());
}

/// Test 15: a page that holds a key past the build's `lock_timeout` makes its
/// `J` statement fail with `55P03`; the job is released for a retry, the
/// definition stays `backfilling`, and the retry converges.
#[tokio::test]
async fn build_a_lock_timeout_is_retried() {
    trellis::dev::staging::set_group_basis(true);
    let d = Driver::start_unbuilt(
        &[("lock_timeout", "300ms")],
        &format!("{DDL}; {BUILD_SEED}"),
        &[
            ("id", int4()),
            ("ga", int4()),
            ("t", int4()),
            ("k", int4()),
            ("x", int4()),
        ],
        &[&L.definition(Key::Single)],
        &["public.a", "public.b"],
    )
    .await;
    let mut lab = Lab {
        d,
        key: Key::Single,
        kinds: vec![L],
    };
    let j = lab.hidden(L, "j").await;
    let holder = lab.d.user().await;
    holder
        .batch_execute(&format!(
            "begin; insert into {j} (k) values (1) on conflict do nothing; \
             select 1 from {j} where k = 1 for update"
        ))
        .await
        .expect("hold a key");
    lab.build_frozen("plan", &[]).await.finish().await;
    assert_eq!(lab.statuses().await, ["(backfilling)"], "not paused");
    let failed = lab
        .d
        .rows("select attempts, last_error is not null, claimed_by is null from backfill_chunks")
        .await;
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert!(failed[0].ends_with(",t,t)"), "{failed:?}");
    holder.batch_execute("commit").await.expect("release");
    lab.write("update backfill_chunks set next_attempt_at = null")
        .await;
    lab.build().await;
    lab.assert_built("a lock timeout").await;
    lab.check("a lock timeout").await;
}

/// Test 17: what stays refused. A field build of a `JOIN` (nothing can ask
/// for one: `ALTER TRANSFORM` takes only a 1-1 target), here forged on the
/// job, fails it rather than walk a primary key.
#[tokio::test]
async fn build_refuses_a_field_build() {
    let lab = seeded(Key::Single, &[L], BUILD_SEED).await;
    lab.write("update backfill_chunks set fields = array['n'] where kind = 'plan'")
        .await;
    lab.build_frozen("plan", &[]).await.finish().await;
    let error = lab
        .d
        .rows("select kind, last_error from backfill_chunks")
        .await;
    assert!(
        error.len() == 1 && error[0].contains("a field build of a JOIN"),
        "the job fails, and plans no chunk: {error:?}"
    );
}

// ---------------------------------------------------- the readers (#1287)

const I2_EXPECTED: &str = "select ga, sum(n), sum(sx) from ( \
        select a.ga as ga, count(*) as n, sum(a.x) as sx from public.a \
        join public.b on a.k = b.k group by a.ga, b.gb) s group by ga order by ga";
const I3_EXPECTED: &str = "select n, count(*), count(sx) from ( \
        select ga, sum(n) as n, sum(sx) as sx from ( \
            select a.ga as ga, count(*) as n, sum(a.x) as sx from public.a \
            join public.b on a.k = b.k group by a.ga, b.gb) s group by ga) r \
        group by n order by n";

/// `I` live over the seeded tables with a group-basis reader `i2` of its
/// target, and with `deeper` a reader `i3` of that.
async fn chained(deeper: bool) -> Lab {
    let mut lab = seeded(Key::Single, &[I], BUILD_SEED).await;
    lab.build().await;
    lab.assert_built("the JOIN before its readers").await;
    let columns = |c: &[(&str, ValueType)]| -> std::collections::HashMap<String, ValueType> {
        c.iter().map(|(n, t)| (n.to_string(), *t)).collect()
    };
    let int8 = ValueType::Integer(IntWidth::Int8);
    trellis::defs::install_definition(
        lab.d.pool(),
        "TRANSFORM i2 FROM public.i GROUP BY ga WITHOUT SOURCE INDEX \
         SELECT ga AS ga, SUM(n) AS n, SUM(sx) AS sx",
        &columns(&[("ga", int4()), ("gb", int4()), ("n", int8), ("sx", int8)]),
        "public",
    )
    .await
    .expect("define the reader");
    trellis::intake::markers::settle_registrations(lab.d.pool()).await;
    lab.build().await;
    lab.d.settle().await;
    if deeper {
        trellis::defs::install_definition(
            lab.d.pool(),
            "TRANSFORM i3 FROM public.i2 GROUP BY n WITHOUT SOURCE INDEX \
             SELECT COUNT(*) AS groups, COUNT(sx) AS t",
            &columns(&[("ga", int4()), ("n", int8), ("sx", int8)]),
            "public",
        )
        .await
        .expect("define the second reader");
        trellis::intake::markers::settle_registrations(lab.d.pool()).await;
        lab.build().await;
        lab.d.settle().await;
    }
    lab
}

async fn reader_status(lab: &Lab, target: &str) -> Vec<String> {
    lab.d
        .rows(&format!(
            "select status::text from transform_definitions where target_table = 'public.{target}'"
        ))
        .await
}

/// The chain after the `JOIN`'s resumed build has committed: every reader the
/// build parked is rebuilt, then each level agrees with its oracle.
async fn chain_converges(lab: &mut Lab, deeper: bool) {
    trellis::staging::build::settle_builds(lab.d.pool()).await;
    lab.d.settle().await;
    trellis::staging::build::settle_builds(lab.d.pool()).await;
    lab.d.settle().await;
    assert_eq!(
        lab.d
            .rows("select status::text from transform_definitions order by id")
            .await,
        vec!["(live)"; 2 + usize::from(deeper)]
    );
    lab.check("the JOIN").await;
    assert_eq!(
        lab.d
            .rows("select ga, n, sx from public.i2 order by ga")
            .await,
        lab.d.rows(I2_EXPECTED).await,
        "i2"
    );
    if deeper {
        assert_eq!(
            lab.d
                .rows("select n, groups, t from public.i3 order by n")
                .await,
            lab.d.rows(I3_EXPECTED).await,
            "i3"
        );
    }
}

/// A page applied to a held group between a `JOIN`'s `RESUME` and the build's
/// lock statement computes its prior image from the empty partials, so the
/// seam row it stages moves the reader's groups by a delta against a value
/// the reader never held. The build's commit parks the target's readers
/// instead: a group-basis reader is rebuilt, and converges (#1287).
async fn a_chained_reader_is_rebuilt(deeper: bool) {
    let mut lab = chained(deeper).await;
    pause(&lab, I).await;
    resume(&lab, I).await;
    let mut building = lab
        .build_frozen("plan", &[PausePoint::BeforeJoinLock])
        .await;
    building.reached(PausePoint::BeforeJoinLock).await;
    // A held group (ga 1) on both sides, before the build's lock statement.
    lab.write(
        "update public.a set x = x + 100 where id = 1; \
         update public.b set y = y + 7 where id = 2",
    )
    .await;
    let batch = lab.d.seal().await;
    lab.d.drain(batch, "page").await;
    lab.d
        .release(&mut building, PausePoint::BeforeJoinLock)
        .await;
    building.finish().await;
    assert_eq!(
        reader_status(&lab, "i2").await,
        ["(backfilling)"],
        "the build's commit rebuilds the reader"
    );
    if deeper {
        assert_eq!(
            reader_status(&lab, "i3").await,
            ["(live)"],
            "the second reader is not rebuilt"
        );
    }
    chain_converges(&mut lab, deeper).await;
}

#[tokio::test]
async fn build_a_group_basis_reader_of_a_resumed_join_is_rebuilt_and_converges() {
    a_chained_reader_is_rebuilt(false).await;
}

#[tokio::test]
async fn build_the_second_reader_of_a_resumed_join_converges_without_being_rebuilt() {
    a_chained_reader_is_rebuilt(true).await;
}

/// Test 9 (rule 7): the build holds the lock row `FOR SHARE`, so a page that
/// carries a `TRUNCATE` (the row `FOR UPDATE`) waits for its commit, then
/// clears what the build wrote.
#[tokio::test]
async fn build_holds_the_lock_row_against_a_truncate_page() {
    let mut lab = seeded(Key::Single, &[L], BUILD_SEED).await;
    lab.write("truncate public.a").await;
    let truncated = lab.d.seal().await;
    let mut building = lab
        .build_frozen("plan", &[PausePoint::BeforeJoinLock])
        .await;
    let reached = building.reached(PausePoint::BeforeJoinLock).await;
    let page = lab.d.drain_frozen(truncated, "truncate", &[]).await;
    lab.d.wait_blocked_behind(reached.backend_pid).await;
    assert!(!page.is_finished(), "the truncate page waits for the build");
    lab.d
        .release(&mut building, PausePoint::BeforeJoinLock)
        .await;
    building.finish().await;
    page.finish().await;
    lab.assert_built("a truncate page behind the build").await;
    assert_eq!(lab.d.rows("select count(*) from public.l").await, ["(0)"]);
    lab.check("a truncate page behind the build").await;
}

/// Rule 1 on a rebuild in place, where every `J` row exists already: the
/// build's one sorted `FOR UPDATE` of the union of both sides' keys makes a
/// page that names an enumerated key, of either side, wait for the commit,
/// while a key the enumeration didn't see runs through.
#[tokio::test]
async fn build_in_place_holds_the_j_rows_of_both_sides_keys() {
    let mut lab = seeded(Key::Single, &[I], BUILD_SEED).await;
    lab.build().await;
    request_rebuild(&lab, "a").await;
    let mut building = lab
        .build_frozen("sweep", &[PausePoint::AfterJoinLock])
        .await;
    let reached = building.reached(PausePoint::AfterJoinLock).await;
    for (sql, queues) in [
        ("update public.a set x = x + 1 where id = 1", true),
        ("update public.b set y = y + 1 where id = 3", true),
        ("insert into public.a values (80, 1, 0, 45, 1)", false),
    ] {
        lab.write(sql).await;
        let batch = lab.d.seal().await;
        let page = lab.d.drain_frozen(batch, "page", &[]).await;
        assert_eq!(
            lab.queued_or_done(&page, reached.backend_pid).await,
            queues,
            "{sql}"
        );
        if !queues {
            page.finish().await;
        } else {
            lab.d
                .release(&mut building, PausePoint::AfterJoinLock)
                .await;
            building.finish().await;
            page.finish().await;
            building = lab
                .build_frozen("sweep", &[PausePoint::AfterJoinLock])
                .await;
            building.kill().await;
            break;
        }
    }
    lab.d.settle().await;
    lab.check("pages around a rebuild in place").await;
}

/// Rule 1 (`twoJ`): the build's `J` lock is one sorted statement over the
/// union of both sides' keys. A page that names a key only the joined side
/// has (9) and a `FROM` side key above it (20) queues behind the build on the
/// lower key, holding nothing the build waits on. Had the build locked one
/// side's keys and then the other's, the page would hold 9 while waiting on
/// 20, and the build's second statement would wait on 9: a deadlock.
#[tokio::test]
async fn build_locks_both_sides_keys_in_one_statement() {
    let mut lab = seeded(
        Key::Single,
        &[L],
        &format!("{BUILD_SEED}; insert into public.a values (9, 1, 0, 20, 1)"),
    )
    .await;
    lab.build().await;
    request_rebuild(&lab, "a").await;
    let mut building = lab
        .build_frozen("sweep", &[PausePoint::AfterJoinLock])
        .await;
    let reached = building.reached(PausePoint::AfterJoinLock).await;
    lab.write(
        "begin; update public.b set y = y + 1 where id = 5; \
         update public.a set x = x + 1 where id = 9; commit",
    )
    .await;
    let batch = lab.d.seal().await;
    let page = lab.d.drain_frozen(batch, "page", &[]).await;
    assert!(
        lab.queued_or_done(&page, reached.backend_pid).await,
        "the page queues behind the build"
    );
    lab.d
        .release(&mut building, PausePoint::AfterJoinLock)
        .await;
    building.finish().await;
    page.finish().await;
    assert_eq!(lab.d.deadlocks_logged(), Vec::<String>::new());
    lab.assert_built("a page on both sides' keys").await;
    lab.check("a page on both sides' keys").await;
}

/// Test 14: a `PAUSE` and `RESUME` while a resumed definition's build is
/// frozen after it took its job row, before it read a hidden table. The
/// `JOIN`'s resume drops and recreates the hidden tables the build is about
/// to read, and only then waits on the build's job row. The build holds the definition row from before its
/// job row, so the `PAUSE` waits for its commit and the resume runs after it:
/// no deadlock, and the build that follows the resume is exact.
#[tokio::test]
async fn build_a_pause_and_resume_wait_for_a_build_in_flight() {
    let mut lab = seeded(Key::Single, &[L], BUILD_SEED).await;
    lab.build().await;
    pause(&lab, L).await;
    lab.step("insert into public.a values (40, 1, 0, 2, 3)")
        .await;
    resume(&lab, L).await;
    let mut building = lab
        .build_frozen("plan", &[PausePoint::BeforeJoinBuild])
        .await;
    let reached = building.reached(PausePoint::BeforeJoinBuild).await;
    let trellis = trellis::Trellis::connect(
        trellis::Config::from_dsn(lab.d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect");
    let pool = lab.d.pool().clone();
    let operator = tokio::spawn(async move {
        trellis.apply("PAUSE TRANSFORM l").await.expect("pause");
        trellis::staging::quarantine::resume_transform(&pool, "l")
            .await
            .expect("resume");
    });
    lab.d.wait_blocked_behind(reached.backend_pid).await;
    lab.d
        .release(&mut building, PausePoint::BeforeJoinBuild)
        .await;
    building.finish().await;
    operator.await.expect("the operator's task");
    assert_eq!(lab.d.deadlocks_logged(), Vec::<String>::new());
    trellis::intake::markers::discharge_registrations(lab.d.pool())
        .await
        .expect("start the build");
    lab.build().await;
    lab.assert_built("a pause and resume during a build").await;
    lab.check("a pause and resume during a build").await;
}

// ------------------------------------------ partitioned sides (#1286, #1109)

/// `a` and `b` (the [`DDL`] columns), each partitioned by list on `t` into
/// three leaves (`t` 0, 1 and 2 or 3) when its flag is set. The join key
/// `(t, k)` holds the partition key, and `k` is nullable, so a cell can have
/// a `NULL` key part.
fn partitioned_ddl(a: bool, b: bool) -> String {
    let table = |name: &str, columns: &str, partitioned: bool| -> String {
        if !partitioned {
            return format!("create table public.{name} (id integer primary key, {columns}); ");
        }
        format!(
            "create table public.{name} (id integer not null, {columns}, primary key (t, id)) \
                 partition by list (t); \
             create table public.{name}0 partition of public.{name} for values in (0); \
             create table public.{name}1 partition of public.{name} for values in (1); \
             create table public.{name}2 partition of public.{name} for values in (2, 3); "
        )
    };
    // Column order as in `DDL`: a (id, ga, t, k, x), b (id, t, k, gb, y).
    format!(
        "{}{}create index on public.a (t, k); create index on public.a (k); \
         create index on public.b (t, k); create index on public.b (k)",
        table(
            "a",
            "ga integer, t integer not null, k integer, x integer",
            a
        ),
        table(
            "b",
            "t integer not null, k integer, gb integer, y integer",
            b
        ),
    )
}

/// [`BUILD_SEED`] plus rows in the third leaf (`t` 2 and 3) of each side.
const PARTITIONED_SEED: &str = "\
    insert into public.a values (1, 1, 0, 1, 10), (2, 1, 0, 1, 5), (3, 2, 0, 2, 7), \
        (4, 2, 0, 3, null), (5, 3, 0, null, 4), (6, null, 0, 2, 1), (7, 1, 1, 1, 100), \
        (8, 4, 0, 8, 2), (20, 5, 2, 1, 3), (21, 5, 2, 2, 4), (22, 6, 3, null, 8); \
    insert into public.b values (1, 0, 1, 10, 3), (2, 0, 1, 11, 4), (3, 0, 2, 10, null), \
        (4, 0, null, 12, 5), (5, 0, 9, 13, 6), (6, 1, 1, 14, 50), (7, 0, 1, 10, 1), \
        (20, 2, 1, 30, 2), (21, 3, 2, 31, 3)";

/// What a build of a partitioned `JOIN` starts from.
#[derive(Debug, Clone, Copy)]
enum Mode {
    /// A fresh build over seeded tables.
    Fresh,
    /// A `RESUME` over a kept target, after writes while paused and hand
    /// edits.
    Resume,
    /// A rebuild in place of a `live` definition over tables that drifted.
    InPlace,
}

/// Changes capture never saw, on both sides: a row of a leaf, a row with a
/// `NULL` key part, a row of the last leaf and of the joined side.
async fn drift_partitioned(lab: &Lab) {
    uncaptured(
        lab,
        "public.a",
        "update public.a set x = x + 100 where id = 1; delete from public.a where id = 4; \
         update public.a set x = x + 50 where id = 5; update public.a set x = x + 7 where id = 20",
    )
    .await;
    uncaptured(
        lab,
        "public.b",
        "delete from public.b where k = 1 and t = 0; \
         insert into public.b values (50, 0, 8, 20, 9); update public.b set y = 99 where id = 20",
    )
    .await;
}

/// Sets `lab` (one `kinds[0]` definition, `live`) up so its next build is
/// `mode`'s, and returns the job kind that build runs as.
async fn prepare(lab: &mut Lab, mode: Mode) -> &'static str {
    let kind = lab.kinds[0];
    match mode {
        Mode::Fresh => "plan",
        Mode::Resume => {
            lab.build().await;
            lab.assert_built("before the pause").await;
            pause(lab, kind).await;
            for sql in [
                "insert into public.a values (70, 5, 0, 1, 1), (71, 7, 2, 1, 2); \
                 insert into public.b values (70, 0, 3, 15, 1)",
                "update public.a set x = 99 where id = 2; update public.b set y = 33 where id = 2",
                "update public.a set k = 9 where id = 3; delete from public.b where id = 7",
                "delete from public.a where id in (5, 22)",
                "delete from public.b where k = 2",
            ] {
                lab.step(sql).await;
            }
            let t = lab.hidden(kind, "t").await;
            lab.write(&format!(
                "update public.{} set n = n + 5; update {t} set __n = __n + 5",
                kind.name
            ))
            .await;
            resume(lab, kind).await;
            "plan"
        }
        Mode::InPlace => {
            lab.build().await;
            lab.assert_built("before the drift").await;
            drift_partitioned(lab).await;
            lab.write(&format!(
                "update public.{name} set n = n + 1000; \
                 insert into public.{name} ({columns}, n) values ({values}, 1)",
                name = kind.name,
                columns = kind.groups().join(", "),
                values = kind
                    .groups()
                    .iter()
                    .map(|_| "424242")
                    .collect::<Vec<_>>()
                    .join(", "),
            ))
            .await;
            request_rebuild(lab, "a").await;
            assert_eq!(
                lab.d.rows("select kind from backfill_chunks").await,
                ["(sweep)"]
            );
            "sweep"
        }
    }
}

/// A build with a partitioned side, in each mode and for each choice of
/// partitioned sides, ends at the oracle with zero diff, and its steps run
/// per side and leaf: the cursor names the last one (`side:oid`).
async fn partitioned_case(a: bool, b: bool, mode: Mode, kind: Kind) {
    let context = format!(
        "a partitioned {a}, b partitioned {b}, {mode:?}, {}",
        kind.name
    );
    let mut lab = seeded_in(&partitioned_ddl(a, b), Key::Pair, &[kind], PARTITIONED_SEED).await;
    prepare(&mut lab, mode).await;
    lab.build().await;
    lab.assert_built(&context).await;
    lab.check(&context).await;
}

#[tokio::test]
async fn partitioned_from_side_with_an_unpartitioned_joined_side_builds() {
    for mode in [Mode::Fresh, Mode::Resume, Mode::InPlace] {
        partitioned_case(true, false, mode, L).await;
    }
    partitioned_case(true, false, Mode::Resume, I).await;
}

#[tokio::test]
async fn partitioned_joined_side_with_an_unpartitioned_from_side_builds() {
    for mode in [Mode::Fresh, Mode::Resume, Mode::InPlace] {
        partitioned_case(false, true, mode, L).await;
    }
    partitioned_case(false, true, Mode::InPlace, I).await;
}

#[tokio::test]
async fn both_sides_partitioned_build() {
    for mode in [Mode::Fresh, Mode::Resume, Mode::InPlace] {
        partitioned_case(true, true, mode, L).await;
    }
    partitioned_case(true, true, Mode::Resume, I).await;
    partitioned_case(true, true, Mode::InPlace, I).await;
}

impl Lab {
    /// Runs `job` frozen between two of its transactions, and returns once
    /// the transaction before the pause has committed: the job's next step is
    /// the one after.
    async fn next_step(&self, job: &'static str) -> drain_driver::Running<()> {
        let mut building = self
            .build_frozen(job, &[PausePoint::BetweenRebuildStructures])
            .await;
        building.reached(PausePoint::BetweenRebuildStructures).await;
        building
    }

    /// Kills `building` where it is, as a worker that dies, and frees its
    /// claim for the next run.
    async fn abandon(&self, building: drain_driver::Running<()>) {
        building.kill().await;
        self.write("update backfill_chunks set claimed_by = null, claimed_at = null")
            .await;
    }

    /// The job's cursor.
    async fn cursor(&self) -> String {
        self.scalar("select coalesce(lo, '') from backfill_chunks where not done")
            .await
    }

    /// Runs the job `steps` steps in, each run killed after its first step
    /// (the cursor is what carries the job on), leaving the cursor after the
    /// last of them.
    async fn run_steps(&self, job: &'static str, steps: usize) {
        for _ in 0..steps {
            let building = self.next_step(job).await;
            self.abandon(building).await;
        }
    }

    /// One pass of the capture reconcile, which records the trees.
    async fn pass(&mut self) {
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
    }

    /// The pass, a seal, the correction engine before any drain, and a drain.
    async fn correct(&mut self) {
        self.pass().await;
        let batch = self.d.seal().await;
        trellis::staging::partition_correction::run_pending(
            self.d.pool(),
            &trellis::staging::partition_correction::Options::default(),
        )
        .await
        .expect("run the pending corrections");
        self.d.drain(batch, "after").await;
        self.d.settle().await;
    }
}

/// A job killed after a step resumes at its cursor and does not write a
/// committed step again: the cursor names the last step (`side:oid`), and
/// the cells of the steps before it keep their bases.
#[tokio::test]
async fn a_killed_join_build_resumes_after_the_last_committed_step() {
    let mut lab = seeded_in(
        &partitioned_ddl(true, false),
        Key::Pair,
        &[L],
        PARTITIONED_SEED,
    )
    .await;
    let p = lab.hidden(L, "p").await;
    let m = lab.hidden(L, "m").await;
    // Steps: the unpartitioned joined side whole, then the `FROM` side's three
    // leaves and its remainder, then the final `T` step.
    lab.run_steps("plan", 1).await;
    assert_eq!(lab.cursor().await, "joined:whole");
    let m_bases = format!("select count(*), count(__basis) from {m}");
    assert_eq!(
        lab.d.rows(&m_bases).await,
        lab.d
            .rows(&format!("select count(*), count(*) from {m}"))
            .await,
        "the joined side's cells carry a basis"
    );
    assert_eq!(
        lab.d.rows(&format!("select count(*) from {p}")).await,
        ["(0)"],
        "no cell of the FROM side yet"
    );
    lab.run_steps("plan", 1).await;
    let cursor = lab.cursor().await;
    assert!(
        cursor.starts_with("from:") && cursor != "from:remainder",
        "{cursor}"
    );
    let leaf = format!("select ga, t, k, __n, __basis::text from {p} where t = 0 order by 1, 2, 3");
    let first_leaf = lab.d.rows(&leaf).await;
    assert!(!first_leaf.is_empty());
    let basis_of_m = format!("select t, k, gb, __basis::text from {m} order by 1, 2, 3");
    let m_before = lab.d.rows(&basis_of_m).await;
    // The check holds at every committed step.
    lab.check_applied("after a leaf").await;
    lab.run_steps("plan", 1).await;
    lab.check_applied("after a second leaf").await;
    lab.build().await;
    lab.assert_built("the resumed job").await;
    lab.check("the resumed job").await;
    assert_eq!(
        lab.d.rows(&leaf).await,
        first_leaf,
        "the first leaf was not written again"
    );
    assert_eq!(
        lab.d.rows(&basis_of_m).await,
        m_before,
        "the joined side was not written again"
    );
}

/// A page between two leaves' transactions of a rebuild in place: one that
/// committed before the rebuild (undrained) is refused by the rebuilt cell's
/// basis, counted once; one committed after applies as a delta; a leaf not
/// yet rebuilt counts the page that applied to its old cells. `T` equals
/// `Σ P ⊗ M` at every committed state, and the end is the oracle.
#[tokio::test]
async fn a_page_between_two_steps_of_a_join_build_is_exact() {
    for (a, b) in [(true, false), (false, true), (true, true)] {
        let context = format!("a {a}, b {b}");
        let mut lab = seeded_in(&partitioned_ddl(a, b), Key::Pair, &[L], PARTITIONED_SEED).await;
        lab.build().await;
        lab.assert_built(&context).await;
        // Committed before the rebuild, still undrained: a row of the first
        // leaf of each side, one of the second, and a joined key.
        let undrained = lab
            .sealed(
                "update public.a set x = x + 1 where id in (1, 7); \
                 update public.b set y = y + 1 where id in (1, 6)",
            )
            .await;
        request_rebuild(&lab, "a").await;
        // Two steps in: one side whole or a first leaf has committed.
        lab.run_steps("sweep", 1).await;
        let mut building = lab.next_step("sweep").await;
        lab.check_applied(&format!("{context}: after two steps"))
            .await;
        lab.d.drain(undrained, "undrained").await;
        lab.check_applied(&format!("{context}: after the undrained page"))
            .await;
        lab.step(
            "update public.a set x = x + 10 where id in (2, 7, 20); \
             update public.b set y = y + 10 where id in (2, 6, 20)",
        )
        .await;
        lab.check_applied(&format!("{context}: after a later page"))
            .await;
        lab.d
            .release(&mut building, PausePoint::BetweenRebuildStructures)
            .await;
        building.finish().await;
        lab.assert_built(&context).await;
        lab.check(&context).await;
    }
}

/// J1 family 9's recomputes during a chunked build: between two steps of a
/// fresh build or a rebuild in place with a partitioned side, rows of every
/// leaf of both sides change behind capture's back (a value, and a
/// dimension row moving to another `gb`, named by its prior image), and
/// their recomputes drain, while a captured change of the same rows waits
/// undrained. A recompute reads its cells from the whole table, so it
/// counts leaves no step has built yet, and the steps that build them later
/// write the same cells absolutely; the captured change's page, drained
/// last, is refused by the basis that counts it. The end is the oracle.
#[tokio::test]
async fn family9_a_recompute_between_two_steps_of_a_join_build_is_exact() {
    // A row's primary-key text: `(t, id)` on a partitioned side.
    fn row_key(partitioned: bool, t: i32, id: i32) -> String {
        if partitioned {
            format!("{t}\u{1f}{id}")
        } else {
            id.to_string()
        }
    }
    for in_place in [false, true] {
        for (a, b) in [(true, false), (false, true), (true, true)] {
            let context = format!("in place: {in_place}, a {a}, b {b}");
            let mut lab =
                seeded_in(&partitioned_ddl(a, b), Key::Pair, &[L], PARTITIONED_SEED).await;
            let job = if in_place {
                lab.build().await;
                request_rebuild(&lab, "a").await;
                "sweep"
            } else {
                "plan"
            };
            lab.run_steps(job, 1).await;
            let mut building = lab.next_step(job).await;
            // Captured and undrained until the build ends: the recompute's
            // basis counts it, so its page is refused then.
            let stale = lab
                .sealed("update public.a set x = x + 1 where id in (1, 7, 20)")
                .await;
            uncaptured(
                &lab,
                "public.a",
                "update public.a set x = x + 7 where id in (1, 7, 20)",
            )
            .await;
            uncaptured(
                &lab,
                "public.b",
                "update public.b set y = y + 7, gb = gb + 100 where id in (1, 6, 20)",
            )
            .await;
            let a_keys: Vec<String> = [(0, 1), (1, 7), (2, 20)]
                .iter()
                .map(|(t, id)| row_key(a, *t, *id))
                .collect();
            let a_keys: Vec<(&str, Option<String>)> =
                a_keys.iter().map(|k| (k.as_str(), None)).collect();
            let b_rows = [
                (0, 1, r#"{"id": 1, "t": 0, "k": 1, "gb": 10, "y": 3}"#),
                (1, 6, r#"{"id": 6, "t": 1, "k": 1, "gb": 14, "y": 50}"#),
                (2, 20, r#"{"id": 20, "t": 2, "k": 1, "gb": 30, "y": 2}"#),
            ];
            let b_keys: Vec<String> = b_rows
                .iter()
                .map(|(t, id, _)| row_key(b, *t, *id))
                .collect();
            let b_keys: Vec<(&str, Option<String>)> = b_keys
                .iter()
                .zip(b_rows)
                .map(|(k, (_, _, prior))| (k.as_str(), Some(prior.to_string())))
                .collect();
            lab.d.stage_recomputes_with_prior("public.a", &a_keys).await;
            lab.d.stage_recomputes_with_prior("public.b", &b_keys).await;
            let recomputed = lab.d.seal().await;
            lab.d.drain(recomputed, "recompute").await;
            lab.check_applied(&format!("{context}: after the recompute"))
                .await;
            lab.d
                .release(&mut building, PausePoint::BetweenRebuildStructures)
                .await;
            building.finish().await;
            lab.build().await;
            lab.d.drain(stale, "stale").await;
            lab.assert_built(&context).await;
            lab.check(&context).await;
        }
    }
}

/// `T`-level R8 is the final step's: a visible group no contribution backs
/// any more (a group of the earlier life that the source lost while paused)
/// stays through every side and leaf step and is removed by the final one.
#[tokio::test]
async fn only_the_final_step_removes_a_visible_group_without_contributions() {
    let mut lab = seeded_in(
        &partitioned_ddl(true, false),
        Key::Pair,
        &[L],
        PARTITIONED_SEED,
    )
    .await;
    lab.build().await;
    lab.assert_built("before the pause").await;
    pause(&lab, L).await;
    lab.step("delete from public.a where ga = 6").await;
    let held = "select count(*) from public.l where ga = 6";
    assert_eq!(lab.d.rows(held).await, ["(1)"]);
    resume(&lab, L).await;
    // The joined side whole, then three leaves and the remainder: five steps.
    lab.run_steps("plan", 4).await;
    let mut building = lab.next_step("plan").await;
    assert_eq!(
        lab.d.rows(held).await,
        ["(1)"],
        "the side and leaf steps leave the group's visible row"
    );
    lab.d
        .release(&mut building, PausePoint::BetweenRebuildStructures)
        .await;
    building.finish().await;
    assert_eq!(lab.d.rows(held).await, ["(0)"], "the final step removes it");
    lab.assert_built("the resume").await;
    lab.check("the resume").await;
}

/// A cell that routes to no recorded leaf (a leaf created after the tree was
/// recorded) is built by the remainder, read through the root; so is a cell
/// with a `NULL` key part, which has no `J` row.
#[tokio::test]
async fn a_cell_that_routes_to_no_recorded_leaf_is_built_by_the_remainder() {
    let mut lab = seeded_in(
        &partitioned_ddl(true, false),
        Key::Pair,
        &[L],
        PARTITIONED_SEED,
    )
    .await;
    lab.build().await;
    lab.assert_built("before").await;
    lab.write("create table public.a3 partition of public.a for values in (4)")
        .await;
    lab.step("insert into public.a values (30, 7, 4, 1, 5), (31, 8, 4, null, 6)")
        .await;
    uncaptured(
        &lab,
        "public.a",
        "update public.a set x = x + 40 where id in (30, 31); \
         update public.a set x = x + 50 where id = 5",
    )
    .await;
    request_rebuild(&lab, "a").await;
    lab.build().await;
    lab.assert_built("the remainder").await;
    lab.check("the remainder").await;
}

/// Partition DDL between two steps of the build: a detach with a row written
/// to the detached table, and an attach. The build and the corrections
/// converge, and neither deadlocks the other.
#[tokio::test]
async fn partition_ddl_between_two_join_build_steps_converges() {
    let mut lab = seeded_in(
        &partitioned_ddl(true, false),
        Key::Pair,
        &[L],
        PARTITIONED_SEED,
    )
    .await;
    lab.build().await;
    lab.assert_built("before").await;
    request_rebuild(&lab, "a").await;
    lab.run_steps("sweep", 1).await;
    let mut building = lab.next_step("sweep").await;
    lab.write(
        "alter table public.a detach partition public.a1; \
         insert into public.a1 values (9000, 8, 1, 1, 5); \
         alter table public.a attach partition public.a1 for values in (1)",
    )
    .await;
    lab.pass().await;
    lab.d
        .release(&mut building, PausePoint::BetweenRebuildStructures)
        .await;
    building.finish().await;
    lab.correct().await;
    lab.correct().await;
    lab.d.settle().await;
    assert_eq!(lab.d.deadlocks_logged(), Vec::<String>::new());
    lab.assert_built("after the DDL and the build").await;
    lab.check("after the DDL and the build").await;
}

/// A leaf with no usable constraint text can't have its cells routed: the
/// job fails rather than build the side in one statement over the root.
#[tokio::test]
async fn a_leaf_without_a_constraint_text_halts_a_join_build() {
    let lab = seeded_in(
        &partitioned_ddl(true, false),
        Key::Pair,
        &[L],
        PARTITIONED_SEED,
    )
    .await;
    lab.write(
        "update partition_sources set nodes = jsonb_set(nodes, \
             array['nodes', (select (i - 1)::text from jsonb_array_elements(nodes->'nodes') \
                             with ordinality as e(n, i) \
                             where n->>'name' = 'public.a1'), 'constraint'], \
             'null'::jsonb)",
    )
    .await;
    assert_eq!(
        lab.building().await,
        side_step(JoinSide::From, JoinSidePart::Unroutable),
        "status() names the side the build can't route"
    );
    lab.build().await;
    let error = lab.d.rows("select last_error from backfill_chunks").await;
    assert!(
        error.len() == 1 && error[0].contains("no usable constraint text"),
        "{error:?}"
    );
}

// ------------------------------------------- the side under build (#1112)

impl Lab {
    /// What `status()` reports of the definition's build
    /// (`JoinStatus::building`). Bounded: the read must not wait on a build
    /// that is frozen with a step's transaction open.
    async fn building(&self) -> Option<JoinBuildStep> {
        let trellis = trellis::Trellis::connect(
            trellis::Config::from_dsn(self.d.db.dsn().to_string()).expect("valid dsn"),
            trellis::TrellisOptions::default(),
        )
        .await
        .expect("connect a Trellis");
        tokio::time::timeout(Duration::from_secs(30), trellis.status(self.kinds[0].name))
            .await
            .expect("status() never waits on the build")
            .expect("status")
            .expect("the definition exists")
            .join
            .expect("a JOIN reports its join status")
            .building
    }
}

fn side_step(side: JoinSide, part: JoinSidePart) -> Option<JoinBuildStep> {
    Some(JoinBuildStep::Side { side, part })
}

fn leaf(name: &str) -> JoinSidePart {
    JoinSidePart::Leaf(format!("public.{name}"))
}

/// The steps a build with these partitioned sides runs, in order, as
/// `status()` names them: unpartitioned sides whole, each partitioned side's
/// leaves in oid order (`*0`, `*1`, `*2` were created in that order) and its
/// remainder, then the final `T` step.
fn expected_steps(a: bool, b: bool) -> Vec<Option<JoinBuildStep>> {
    use JoinSide::{From, Joined};
    let mut steps = Vec::new();
    if !a {
        steps.push(side_step(From, JoinSidePart::Whole));
    }
    if !b {
        steps.push(side_step(Joined, JoinSidePart::Whole));
    }
    for (partitioned, side, table) in [(a, From, "a"), (b, Joined, "b")] {
        if partitioned {
            for n in 0..3 {
                steps.push(side_step(side, leaf(&format!("{table}{n}"))));
            }
            steps.push(side_step(side, JoinSidePart::Remainder));
        }
    }
    steps.push(Some(JoinBuildStep::Final));
    steps
}

/// `status()` reports the step a partitioned `JOIN`'s build runs next, read
/// while the job is frozen between two steps: each side and leaf in turn,
/// then the final step, read again while that step's transaction is open;
/// `None` once the build is done. For a fresh build, a `RESUME` and a rebuild
/// in place.
async fn side_under_build_case(a: bool, b: bool, mode: Mode) {
    let context = format!("a partitioned {a}, b partitioned {b}, {mode:?}");
    let mut lab = seeded_in(&partitioned_ddl(a, b), Key::Pair, &[L], PARTITIONED_SEED).await;
    let job = prepare(&mut lab, mode).await;
    let steps = expected_steps(a, b);
    assert_eq!(lab.building().await, steps[0], "{context}: queued");
    for (done, next) in steps.iter().skip(1).enumerate() {
        let building = lab.next_step(job).await;
        assert_eq!(
            lab.building().await,
            *next,
            "{context}: after step {done}, cursor {}",
            lab.cursor().await
        );
        lab.abandon(building).await;
    }
    let mut building = lab
        .build_frozen(job, &[PausePoint::BeforeRebuildCommit])
        .await;
    building.reached(PausePoint::BeforeRebuildCommit).await;
    assert_eq!(
        lab.building().await,
        Some(JoinBuildStep::Final),
        "{context}: the final step's transaction is open"
    );
    lab.d
        .release(&mut building, PausePoint::BeforeRebuildCommit)
        .await;
    building.finish().await;
    lab.assert_built(&context).await;
    assert_eq!(lab.building().await, None, "{context}: built");
    lab.check(&context).await;
}

#[tokio::test]
async fn status_reports_the_side_under_build_of_a_partitioned_from_side() {
    for mode in [Mode::Fresh, Mode::Resume, Mode::InPlace] {
        side_under_build_case(true, false, mode).await;
    }
}

#[tokio::test]
async fn status_reports_the_side_under_build_of_a_partitioned_joined_side() {
    side_under_build_case(false, true, Mode::Fresh).await;
    side_under_build_case(false, true, Mode::InPlace).await;
}

#[tokio::test]
async fn status_reports_the_side_under_build_of_two_partitioned_sides() {
    side_under_build_case(true, true, Mode::Resume).await;
}

/// A `JOIN` with no partitioned side builds in one transaction: `status()`
/// reports `Whole` while the job is queued and while its transaction is
/// open, and `None` once it committed. A fresh build and a rebuild in place.
#[tokio::test]
async fn status_reports_an_unpartitioned_join_build_as_whole() {
    for job in ["plan", "sweep"] {
        let lab = seeded(Key::Single, &[L], BUILD_SEED).await;
        if job == "sweep" {
            lab.build().await;
            assert_eq!(lab.building().await, None, "built");
            request_rebuild(&lab, "a").await;
        }
        assert_eq!(lab.building().await, Some(JoinBuildStep::Whole), "{job}");
        let mut building = lab
            .build_frozen(job, &[PausePoint::BeforeRebuildCommit])
            .await;
        building.reached(PausePoint::BeforeRebuildCommit).await;
        assert_eq!(
            lab.building().await,
            Some(JoinBuildStep::Whole),
            "{job}: its transaction is open"
        );
        lab.d
            .release(&mut building, PausePoint::BeforeRebuildCommit)
            .await;
        building.finish().await;
        lab.assert_built(job).await;
        assert_eq!(lab.building().await, None, "{job}: built");
    }
}

/// A paused `JOIN` reports no build, though its build's job is still queued
/// with its cursor: no worker is handed the job while it is paused, and its
/// `RESUME` discards it and reports the new build from its first step.
#[tokio::test]
async fn status_reports_no_build_while_a_join_is_paused() {
    let mut lab = seeded_in(
        &partitioned_ddl(true, false),
        Key::Pair,
        &[L],
        PARTITIONED_SEED,
    )
    .await;
    let job = prepare(&mut lab, Mode::Fresh).await;
    lab.run_steps(job, 2).await;
    assert_eq!(lab.building().await, side_step(JoinSide::From, leaf("a1")));
    pause(&lab, L).await;
    assert!(
        lab.cursor().await.starts_with("from:"),
        "the paused job keeps its cursor"
    );
    assert_eq!(lab.building().await, None, "paused");
    resume(&lab, L).await;
    assert_eq!(
        lab.building().await,
        side_step(JoinSide::Joined, JoinSidePart::Whole),
        "the resume's build starts over"
    );
}

/// A chained reader of a partitioned `JOIN`'s resume is parked once, by the
/// final step, not by any leaf.
#[tokio::test]
async fn a_reader_of_a_join_resumed_per_leaf_is_rebuilt_once() {
    let mut lab = seeded_in(
        &partitioned_ddl(true, true),
        Key::Pair,
        &[I],
        PARTITIONED_SEED,
    )
    .await;
    lab.build().await;
    let int8 = ValueType::Integer(IntWidth::Int8);
    trellis::defs::install_definition(
        lab.d.pool(),
        "TRANSFORM i2 FROM public.i GROUP BY ga WITHOUT SOURCE INDEX \
         SELECT ga AS ga, SUM(n) AS n, SUM(sx) AS sx",
        &[("ga", int4()), ("gb", int4()), ("n", int8), ("sx", int8)]
            .into_iter()
            .map(|(n, t)| (n.to_string(), t))
            .collect(),
        "public",
    )
    .await
    .expect("define the reader");
    trellis::intake::markers::settle_registrations(lab.d.pool()).await;
    lab.build().await;
    lab.d.settle().await;
    pause(&lab, I).await;
    resume(&lab, I).await;
    // Both sides partitioned: three leaves and a remainder each, then final.
    lab.run_steps("plan", 3).await;
    assert_eq!(
        reader_status(&lab, "i2").await,
        ["(live)"],
        "a leaf's transaction parks nothing"
    );
    lab.run_steps("plan", 5).await;
    assert_eq!(
        reader_status(&lab, "i2").await,
        ["(live)"],
        "no side or leaf step parks anything"
    );
    lab.build_frozen("plan", &[]).await.finish().await;
    assert_eq!(
        reader_status(&lab, "i2").await,
        ["(backfilling)"],
        "the final step rebuilds the reader"
    );
    trellis::staging::build::settle_builds(lab.d.pool()).await;
    lab.d.settle().await;
    lab.check("the JOIN").await;
    assert_eq!(
        lab.d
            .rows("select ga, n, sx from public.i2 order by ga")
            .await,
        lab.d
            .rows(&I2_EXPECTED.replace("a.k = b.k", "a.t = b.t and a.k = b.k"))
            .await
    );
}

/// A `PAUSE` and a `DROP` of a `JOIN` while a page of its joined table that
/// planned it is in flight (#1291). The page holds only `b`'s fence, the
/// joined table's, and is frozen on `i` before it reaches `l`, so it holds no
/// lock on `l`'s tables. The drop takes the joined table's fence as well as
/// its source's, so it waits for the page, which commits, and nothing is
/// charged.
#[tokio::test]
async fn a_pause_and_drop_of_a_join_wait_for_a_page_of_its_joined_table() {
    let mut lab = lab(Key::Single, &[I, L]).await;
    lab.step(
        "insert into public.a (id, ga, k, x) values (1, 1, 1, 10); \
         insert into public.b (id, k, gb, y) values (1, 1, 1, 100)",
    )
    .await;
    let batch = lab.sealed("update public.b set y = 200 where id = 1").await;
    let mut page = lab
        .d
        .drain_frozen(batch, "page", &[(PausePoint::BeforeJoinLock, "public.i")])
        .await;
    page.reached(PausePoint::BeforeJoinLock).await;

    let t = trellis::Trellis::connect(
        trellis::config::Config::from_dsn(lab.d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    t.apply("PAUSE TRANSFORM l").await.expect("pause");
    let mut dropping = tokio::spawn(async move {
        t.apply("DROP TRANSFORM l").await.expect("drop");
    });
    // The drop queues on the page's fence, or, without one, completes.
    tokio::select! {
        () = lab.d.wait_blocked(2) => {}
        _ = &mut dropping => {}
    }
    assert!(
        !dropping.is_finished(),
        "the drop waits for the page in flight"
    );
    lab.d.release(&mut page, PausePoint::BeforeJoinLock).await;
    let outcome = page.finish_result().await.expect("the page applies");
    assert!(outcome.is_some(), "the page committed");
    dropping.await.expect("the drop's task");
    assert_eq!(
        lab.d
            .rows(
                "select (select count(*) from drain_holdups), (select count(*) from poison), \
                        (select count(*) from key_deaths), \
                        (select count(*) from capture_failures), \
                        to_regclass('public.l') is null"
            )
            .await,
        ["(0,0,0,0,t)"],
        "the page failed no one"
    );
    lab.kinds = vec![I];
    lab.check("after the drop").await;
}
