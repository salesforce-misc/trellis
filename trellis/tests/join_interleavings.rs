//! Deterministic interleavings of the drain on `JOIN` targets (#1108 J3,
//! ADR-0019), ported from #1091 J1's model scenarios and its review's RV
//! cases.
//!
//! Every test turns the group-basis dev knob on before its driver starts:
//! a `JOIN` applies only behind it, and goes `live` only over two empty
//! tables until its build (#1109) exists. Rows arrive through capture.
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
                       gb integer, y integer)";

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
         create table public.c (id integer primary key, region text, weight integer)",
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

/// Until #1110, a `TRUNCATE` of either side halts the definition.
#[tokio::test]
async fn a_truncate_of_either_side_halts_the_join() {
    for table in ["public.a", "public.b"] {
        let mut lab = lab(Key::Single, &[L]).await;
        lab.step(
            "insert into public.a values (1, 1, 0, 1, 10); \
             insert into public.b values (1, 0, 1, 1, 100)",
        )
        .await;
        lab.write(&format!("truncate {table}")).await;
        let batch = lab.d.seal().await;
        let _ = lab
            .d
            .drain_frozen(batch, "x", &[])
            .await
            .finish_result()
            .await;
        assert_eq!(
            lab.scalar("select status from transform_definitions").await,
            "paused",
            "a truncate of {table} halts the JOIN"
        );
    }
}

/// Until #1109 the knob builds a `JOIN` only over two empty captured
/// tables: a side with rows, or a side that is another transform's target
/// (whose feed is #1100's), fails the build and pauses the definition.
#[tokio::test]
async fn the_knob_builds_a_join_only_over_empty_tables() {
    trellis::dev::staging::set_group_basis(true);
    let d = Driver::start(
        &format!("{DDL}; insert into public.b values (1, 0, 1, 1, 1)"),
        &[("id", int4())],
        &[],
        &["public.a", "public.b"],
    )
    .await;
    trellis::defs::install_definition(
        d.pool(),
        &L.definition(Key::Single),
        &Default::default(),
        "public",
    )
    .await
    .expect("define");
    trellis::intake::markers::settle_registrations(d.pool()).await;
    assert_eq!(
        d.rows("select status::text from transform_definitions")
            .await,
        ["(paused)"],
        "the build fails on a side with rows"
    );
    let error = d
        .rows("select string_agg(last_error, ' | ') from backfill_chunks")
        .await;
    assert!(
        error.len() == 1 && error[0].contains("a source that has rows"),
        "{error:?}"
    );
}

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
        "TRANSFORM l FROM public.a LEFT JOIN public.c ON a.k = c.k GROUP BY c.gb \
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
