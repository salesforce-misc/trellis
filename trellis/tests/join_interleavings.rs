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

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::{Driver, RunningDrain};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::interleave::PausePoint;

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
             SELECT {}, COUNT(*) AS n, COUNT(a.x) AS cx, SUM(a.x) AS sx, \
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
    /// for the second drain to reach one of them.
    async fn queued_or_done(&self, other: &RunningDrain, pid: i32) -> bool {
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
           SELECT c.region AS region, COUNT(*) AS n, SUM(o.amount) AS revenue, \
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

/// A `JOIN` whose side is a partitioned root that has rows halts with the
/// reason (its per-leaf build waits on #1286); a partitioned side with no
/// rows builds like any other.
#[tokio::test]
async fn the_build_halts_on_a_partitioned_side_with_rows() {
    trellis::dev::staging::set_group_basis(true);
    trellis::defs::partition_tree::open_gate_for_tests();
    let ddl = "create table public.a (id integer not null, ga integer, t integer, \
                   k integer not null, x integer, primary key (k, id)) partition by list (k); \
               create table public.a0 partition of public.a for values in (1, 2); \
               create table public.a1 partition of public.a default; \
               create table public.b (id integer primary key, t integer, k integer, \
                   gb integer, y integer); create index on public.b (k)";
    for (rows, built) in [
        ("", true),
        ("insert into public.a values (1, 1, 0, 1, 1)", false),
    ] {
        let d = Driver::start_unbuilt(
            &[],
            &format!("{ddl}; {rows}"),
            &[
                ("id", int4()),
                ("ga", int4()),
                ("t", int4()),
                ("k", int4()),
                ("x", int4()),
            ],
            &[
                "TRANSFORM l FROM public.a LEFT JOIN public.b ON a.k = b.k GROUP BY a.ga \
               SELECT a.ga AS ga, COUNT(*) AS n",
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
        let error = lab.d.rows("select last_error from backfill_chunks").await;
        if built {
            lab.assert_built("a partitioned side with no rows").await;
        } else {
            assert_eq!(lab.statuses().await, ["(paused)"]);
            assert!(error.len() == 1 && error[0].contains("#1286"), "{error:?}");
        }
    }
}

/// A side that is another transform's target (whose feed is #1100's) fails
/// the build and pauses the definition.
#[tokio::test]
async fn the_knob_refuses_a_join_over_a_target() {
    trellis::dev::staging::set_group_basis(true);
    let d = Driver::start(
        DDL,
        &[
            ("id", int4()),
            ("t", int4()),
            ("k", int4()),
            ("gb", int4()),
            ("y", int4()),
        ],
        &["TRANSFORM c FROM public.b SELECT t AS t, k AS k, gb AS gb, y AS y"],
        &["public.a", "public.b"],
    )
    .await;
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM l FROM public.a LEFT JOIN public.c ON a.k = c.k GROUP BY c.gb WITHOUT SOURCE INDEX \
         SELECT c.gb AS gb, COUNT(*) AS n",
        &[("id".to_string(), int4()), ("k".to_string(), int4())].into(),
        "public",
    )
    .await
    .expect("define");
    trellis::intake::markers::settle_registrations(d.pool()).await;
    assert_eq!(
        d.rows("select status::text from transform_definitions where target_table = 'public.l'")
            .await,
        ["(paused)"],
        "the build fails on a side that is a target"
    );
    let error = d
        .rows("select string_agg(last_error, ' | ') from backfill_chunks")
        .await;
    assert!(
        error.len() == 1 && error[0].contains("another definition's target"),
        "{error:?}"
    );
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
             SELECT o.region AS region, c.at AS at, c.price AS price, COUNT(*) AS n, \
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
    trellis::dev::staging::set_group_basis(true);
    let definitions: Vec<String> = kinds.iter().map(|k| k.definition(key)).collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let d = Driver::start_unbuilt(
        &[],
        &format!("{DDL}; {seed}"),
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
    Lab {
        d,
        key,
        kinds: kinds.to_vec(),
    }
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
    assert_eq!(lab.statuses().await, ["(backfilling)"]);
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

/// A `RESUME` of a `JOIN` whose target has rows and a reader that isn't paused
/// is refused at the build (#1287): the resume keeps the target and recreates
/// the hidden tables, so a page that applies before the build (here one on
/// both sides of a key) sends the reader prior images from the empty
/// partials, and a group-basis reader would keep the error for good. The
/// remedy the reason names repairs it: with the reader paused the `JOIN`
/// builds, and the reader's own resume rebuilds it.
#[tokio::test]
async fn build_resume_with_a_live_reader_halts() {
    let mut lab = seeded(Key::Single, &[I], BUILD_SEED).await;
    lab.build().await;
    trellis::defs::install_definition(
        lab.d.pool(),
        "TRANSFORM i2 FROM public.i GROUP BY ga WITHOUT SOURCE INDEX \
         SELECT ga AS ga, SUM(n) AS n, SUM(sx) AS sx",
        &[
            ("ga".to_string(), int4()),
            ("gb".to_string(), int4()),
            ("n".to_string(), ValueType::Integer(IntWidth::Int8)),
            ("sx".to_string(), ValueType::Integer(IntWidth::Int8)),
        ]
        .into(),
        "public",
    )
    .await
    .expect("define the reader");
    trellis::intake::markers::settle_registrations(lab.d.pool()).await;
    lab.build().await;
    lab.d.settle().await;
    pause(&lab, I).await;
    trellis::staging::quarantine::resume_transform(lab.d.pool(), "i")
        .await
        .expect("resume");
    trellis::intake::markers::discharge_registrations(lab.d.pool())
        .await
        .expect("start the build");
    lab.step(
        "insert into public.b values (60, 0, 1, 10, 1); \
         update public.a set x = x + 1 where id = 1",
    )
    .await;
    lab.build().await;
    assert_eq!(
        lab.d
            .rows("select status::text from transform_definitions where target_table = 'public.i'")
            .await,
        ["(paused)"]
    );
    let error = lab.d.rows("select last_error from backfill_chunks").await;
    assert!(error.len() == 1 && error[0].contains("#1287"), "{error:?}");
    // The remedy the reason names: pause the reader, resume the JOIN, which
    // builds, then resume the reader, which rebuilds from the JOIN's target.
    lab.write("insert into public.a values (41, 1, 0, 1, 2)")
        .await;
    let trellis = trellis::Trellis::connect(
        trellis::Config::from_dsn(lab.d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis
        .apply("PAUSE TRANSFORM i2")
        .await
        .expect("pause the reader");
    trellis::staging::quarantine::resume_transform(lab.d.pool(), "i")
        .await
        .expect("resume the JOIN");
    trellis::intake::markers::discharge_registrations(lab.d.pool())
        .await
        .expect("start the build");
    lab.build().await;
    trellis
        .apply("RESUME TRANSFORM i2")
        .await
        .expect("resume the reader");
    trellis::intake::markers::settle_registrations(lab.d.pool()).await;
    lab.build().await;
    lab.d.settle().await;
    assert_eq!(
        lab.d
            .rows("select status::text from transform_definitions order by id")
            .await,
        ["(live)", "(live)"]
    );
    lab.check("the JOIN after the remedy").await;
    assert_eq!(
        lab.d
            .rows("select ga, n, sx from public.i2 order by ga")
            .await,
        lab.d
            .rows("select ga, sum(n), sum(sx) from public.i group by ga order by ga")
            .await,
        "the reader follows the rebuilt JOIN"
    );
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
