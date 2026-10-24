//! Deterministic interleavings of the drain on a group-basis aggregate
//! (#1100 G2a, ADR-0002 "Aggregates: a basis per group, kept by deltas").
//!
//! Each test drives one interleaving by hand through `support/drain_driver.rs`, and none
//! sleeps or polls for convergence (#297). Every source starts empty and is
//! seeded through capture.
//!
//! The scenarios are #1043 E1's, at group granularity: R1 (a page reads a
//! group's basis after locking it), R2 (each side is checked against its own
//! group's basis, per transaction), F2 (sides are never folded per key), R3
//! (nested writes stage exact transition images, #1099), R7 (pages lock
//! groups in one sorted statement and never deadlock), and decision 4's
//! transient states (a group is visible only while its row count is above
//! 0). An absolute write, which only the build and recompute (#1101, #1102)
//! make, is planted by [`recompute`]: the model's locked group recompute.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::interleave::PausePoint;

const SRC: &str = "public.src";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n";
const ACTUAL: &str = "select g, total, n from public.agg order by g";
const EXPECTED: &str = "select g, sum(v), count(*) from public.src group by g order by g";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

/// `public.src (id, g, v)`, empty, with `definition` live over it on the
/// group basis, captured by triggers. `app` runs after the source's DDL
/// (an application trigger, say).
async fn start_with(
    definition: &str,
    source_ddl: &str,
    columns: &[(&str, ValueType)],
    app: &str,
) -> Driver {
    let d = Driver::start(
        &format!("{source_ddl}; {app}"),
        columns,
        &[definition],
        &[SRC],
    )
    .await;
    let hidden = scalar(
        &d,
        &format!(
            "select count(*) from pg_tables where schemaname = '{DEFAULT_SCHEMA}' \
             and tablename like 'agg\\_%'"
        ),
    )
    .await;
    assert_eq!(hidden, "1", "the definition is on the group basis");
    d
}

/// `sql`'s one value, as text.
async fn scalar(d: &Driver, sql: &str) -> String {
    let rows = d.rows(sql).await;
    assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
    rows[0].trim_matches(|c| c == '(' || c == ')').to_string()
}

async fn start() -> Driver {
    start_with(
        AGG,
        "create table public.src (id integer primary key, g integer, v integer); \
         create index on public.src (g)",
        &[("id", int4()), ("g", int4()), ("v", int4())],
        "",
    )
    .await
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it as one sealed page.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

/// Settles the pipeline and asserts the target equals the oracle.
async fn assert_oracle(d: &mut Driver) {
    d.settle().await;
    assert_eq!(
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
        "the target (left) differs from the oracle (right)"
    );
}

/// The hidden table of the one definition, qualified.
async fn hidden(d: &Driver) -> String {
    let id = scalar(
        d,
        "select id from transform_definitions where target_table = 'public.agg'",
    )
    .await;
    format!("{DEFAULT_SCHEMA}.agg_{id}")
}

/// The hidden partials of group `g`: `(n, count, sum, has basis)`.
async fn partials(d: &Driver, g: i32) -> Vec<String> {
    d.rows(&format!(
        "select __n, __count_1, __sum_1, __basis is not null from {} where g = {g}",
        hidden(d).await
    ))
    .await
}

/// A locked group recompute of `groups`, the absolute write #1102 will
/// make: lock the groups in one sorted statement, then one statement takes
/// the snapshot, `GROUP BY`s the source and writes the partials, the basis,
/// the stamp (the newest segment there is, read with the snapshot: R5) and
/// the visible rows. Runs on its own connection and is left open; the
/// caller commits it with [`Recompute::commit`].
struct Recompute {
    client: tokio_postgres::Client,
    pid: i32,
}

impl Recompute {
    async fn commit(self) {
        self.client.batch_execute("commit").await.expect("commit");
    }
}

async fn recompute(d: &Driver, groups: &[i32]) -> Recompute {
    let hidden = hidden(d).await;
    let client = d.user().await;
    let pid: i32 = client
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    let groups = groups
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    client
        .batch_execute(&format!(
            "begin; \
             insert into {hidden} (g) select g from unnest(array[{groups}]) g order by g \
                 on conflict (g) do update set __n = excluded.__n where false; \
             with a as ( \
                 select x.g, count(s.id) as n, count(s.v) as c, coalesce(sum(s.v), 0) as s \
                 from unnest(array[{groups}]) x(g) \
                 left join public.src s on s.g = x.g group by x.g), \
             h as ( \
                 update {hidden} h set __n = a.n, __count_1 = a.c, __sum_1 = a.s, \
                     __basis = pg_current_snapshot(), \
                     __stamp = (select max(seg_seq) from {DEFAULT_SCHEMA}.segments) \
                 from a where h.g = a.g returning h.g) \
             delete from public.agg where g in (select g from h); \
             insert into public.agg (g, total, n) \
             select g, case when __count_1 = 0 then null else __sum_1 end, __n \
             from {hidden} where g in ({groups}) and __n > 0"
        ))
        .await
        .expect("the recompute");
    Recompute { client, pid }
}

// ------------------------------------------------------------------ apply

/// Inserts, updates, group moves, `NULL` arguments and deletes, each its own
/// page, against Postgres over the source.
#[tokio::test]
async fn pages_keep_count_and_sum_exact() {
    let mut d = start().await;
    for sql in [
        "insert into public.src values (1, 1, 10), (2, 1, 5), (3, 2, null), (4, null, 7)",
        "update public.src set v = 20 where id = 1",
        "update public.src set g = 2 where id = 2",
        "update public.src set v = null where id = 1; insert into public.src values (5, 3, 1)",
        "delete from public.src where id in (3, 4)",
        "update public.src set g = null where id = 5",
    ] {
        step(&mut d, sql).await;
        assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await, "after {sql}");
    }
    assert_oracle(&mut d).await;
}

/// A page whose sides change nothing visible leaves the visible row
/// unwritten: an update that keeps the group and the value nets to zero.
#[tokio::test]
async fn a_page_that_changes_nothing_writes_nothing() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    let before = d.rows("select xmin::text from public.agg").await;
    step(&mut d, "update public.src set v = v where id = 1").await;
    assert_eq!(d.rows("select xmin::text from public.agg").await, before);
    assert_oracle(&mut d).await;
}

/// #1096 decision 7: a page that changes a group's partials but no visible
/// value writes the hidden row and leaves the visible row alone. A `NULL`
/// `v` changes the row count, which no field shows.
#[tokio::test]
async fn a_page_that_changes_only_hidden_partials_leaves_the_visible_row() {
    let mut d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total",
        "create table public.src (id integer primary key, g integer, v integer); \
         create index on public.src (g)",
        &[("id", int4()), ("g", int4()), ("v", int4())],
        "",
    )
    .await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    let before = d.rows("select xmin::text from public.agg").await;
    step(&mut d, "insert into public.src values (2, 1, null)").await;
    assert_eq!(partials(&d, 1).await, ["(2,1,10,f)"]);
    assert_eq!(d.rows("select xmin::text from public.agg").await, before);
    assert_eq!(
        d.rows("select g, total from public.agg").await,
        d.rows("select g, sum(v) from public.src group by g").await
    );
}

/// #1096 decision 1: composed fields and expression arguments, without
/// `MIN`/`MAX` (the ledger's
/// `composed_fields_and_expression_arguments_equal_postgres`).
#[tokio::test]
async fn composed_fields_and_expression_arguments_equal_postgres() {
    let mut d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g \
        WITHOUT SOURCE INDEX SELECT SUM(a) + COUNT(b) AS mixed, SUM(a + b) AS ab, AVG(a) AS mean, \
                COALESCE(SUM(b), 0) AS sb, SUM(a) > COUNT(b) AS apart, COUNT(a + b) AS nab, \
                COUNT(*) AS n",
        "create table public.src (id integer primary key, g integer, a integer, b integer); \
         create index on public.src (g)",
        &[("id", int4()), ("g", int4()), ("a", int4()), ("b", int4())],
        "",
    )
    .await;
    let actual = "select g, mixed::text, ab::text, mean::text, sb::text, apart, nab, n \
                  from public.agg order by g";
    let expected = "select g, (sum(a) + count(b))::text, sum(a + b)::text, avg(a)::text, \
                           coalesce(sum(b), 0::numeric)::text, sum(a) > count(b), \
                           count(a + b), count(*) \
                    from public.src group by g order by g";
    for sql in [
        "insert into public.src values (1, 1, 1, 10), (2, 1, 5, null), (3, 2, null, null), \
                                       (4, 2, 2, 3)",
        "update public.src set a = 9 where id = 1; insert into public.src values (5, 2, 7, 1)",
        "update public.src set b = null where id in (1, 4); \
         update public.src set g = 1 where id = 5",
        "delete from public.src where id = 2; update public.src set a = null where id = 5",
        "update public.src set a = 4, b = 4 where id = 3",
        "delete from public.src where g = 1",
    ] {
        step(&mut d, sql).await;
        assert_eq!(d.rows(actual).await, d.rows(expected).await, "after {sql}");
    }
}

/// `numeric(p,s)` sums and averages carry Postgres's scale (#1096 decision
/// 2), through values leaving the group.
#[tokio::test]
async fn numeric_sums_and_averages_keep_the_declared_scale() {
    let mut d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g \
        WITHOUT SOURCE INDEX SELECT SUM(m) AS total, AVG(m) AS mean, COUNT(m) AS nm, COUNT(*) AS n",
        "create table public.src (id integer primary key, g integer, m numeric(10, 2)); \
         create index on public.src (g)",
        &[("id", int4()), ("g", int4()), ("m", ValueType::Numeric)],
        "",
    )
    .await;
    let actual = "select g, total::text, mean::text, nm, n from public.agg order by g";
    let expected = "select g, sum(m)::text, avg(m)::text, count(m), count(*) \
                    from public.src group by g order by g";
    for sql in [
        "insert into public.src values (1, 1, 1.5), (2, 1, 0.25), (3, 1, null)",
        "delete from public.src where id = 2",
        "update public.src set m = 3 where id = 1",
        "update public.src set g = 2 where id = 1",
    ] {
        step(&mut d, sql).await;
        assert_eq!(d.rows(actual).await, d.rows(expected).await, "after {sql}");
    }
}

/// `GROUP BY` columns may take any name outside the reserved `__` prefix,
/// including the names the apply's own statements could use for their
/// working columns (`n`, `txid`, `lsn`, `hc`, `basis`, `nn`, `c1`, `a1`),
/// and a field may read a group column beside its aggregates.
#[tokio::test]
async fn group_columns_named_like_the_applys_working_columns() {
    let names = ["n", "txid", "lsn", "hc", "basis", "nn", "c1", "a1"];
    let columns: Vec<(&str, ValueType)> = std::iter::once(("id", int4()))
        .chain(names.iter().map(|name| (*name, int4())))
        .chain(std::iter::once(("v", int4())))
        .collect();
    let group = names.join(", ");
    let mut d = start_with(
        &format!(
            "TRANSFORM agg FROM public.src GROUP BY {group} \
            WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(v) AS nv, n + COUNT(*) AS np"
        ),
        &format!(
            "create table public.src (id integer primary key, {}, v integer); \
             create index on public.src ({group})",
            names
                .iter()
                .map(|name| format!("{name} integer"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        &columns,
        "",
    )
    .await;
    let actual = format!("select {group}, total, nv, np from public.agg order by {group}");
    let expected = format!(
        "select {group}, sum(v), count(v), n + count(*) from public.src \
         group by {group} order by {group}"
    );
    for sql in [
        "insert into public.src values (1, 1, 2, 3, 4, 5, 6, 7, 8, 10), \
                                       (2, 1, 2, 3, 4, 5, 6, 7, 8, 5), \
                                       (3, null, 2, 3, 4, 5, 6, 7, 8, null)",
        "update public.src set n = 2, v = 1 where id = 1",
        "delete from public.src where id = 2",
    ] {
        step(&mut d, sql).await;
        assert_eq!(
            d.rows(&actual).await,
            d.rows(&expected).await,
            "after {sql}"
        );
    }
}

/// Two `GROUP BY` columns and every pattern of `NULL`s in them: each pattern
/// is its own index probe (the group lookup), and each finds its own group,
/// in the hidden table and in the target, as groups fill, move and empty.
#[tokio::test]
async fn every_pattern_of_null_group_columns_finds_its_group() {
    let mut d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g, h WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n",
        "create table public.src (id integer primary key, g integer, h integer, v integer); \
         create index on public.src (g, h)",
        &[("id", int4()), ("g", int4()), ("h", int4()), ("v", int4())],
        "",
    )
    .await;
    let actual = "select g, h, total, n from public.agg order by g, h";
    let expected = "select g, h, sum(v), count(*) from public.src group by g, h order by g, h";
    for sql in [
        "insert into public.src values (1, 1, 1, 1), (2, 1, null, 2), (3, null, 1, 3), \
                                       (4, null, null, 4), (5, 1, null, 5), (6, null, 1, 6)",
        "update public.src set v = v * 10",
        "update public.src set g = null where id = 1; update public.src set h = 1 where id = 2",
        "update public.src set h = null where id in (3, 6)",
        "delete from public.src where id in (4, 5)",
        "delete from public.src",
    ] {
        step(&mut d, sql).await;
        assert_eq!(d.rows(actual).await, d.rows(expected).await, "after {sql}");
    }
    assert_oracle(&mut d).await;
}

// ------------------------------------------------------------------- R1, R2

/// R1: a page reads each group's basis in a statement after its lock. A
/// recompute holding group 1's lock has counted the page's change C in its
/// snapshot; the page, already draining, queues on the lock, and once the
/// recompute commits it reads the new basis and refuses C. A page that read
/// the basis before its lock would count C twice.
#[tokio::test]
async fn r1_a_page_reads_the_basis_after_its_lock() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    write(&d, "insert into public.src values (2, 1, 5)").await;
    let batch = d.seal().await;
    let held = recompute(&d, &[1]).await;
    let page = d.drain_frozen(batch, "a", &[]).await;
    d.wait_blocked_behind(held.pid).await;
    held.commit().await;
    page.finish().await;
    assert_eq!(partials(&d, 1).await, ["(2,2,15,t)"]);
    assert_oracle(&mut d).await;
}

/// R1, the other order: the page locks group 1 first and is frozen there;
/// a recompute of group 1 queues behind it, and its snapshot, taken after
/// the page commits, sees the page's change, so it rewrites the group to
/// the same value.
#[tokio::test]
async fn r1_a_recompute_queued_behind_a_page_sees_its_change() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    write(&d, "insert into public.src values (2, 1, 5)").await;
    let batch = d.seal().await;
    let mut page = d
        .drain_frozen(batch, "a", &[(PausePoint::AfterEntryLock, "public.agg")])
        .await;
    let frozen = page.reached(PausePoint::AfterEntryLock).await;
    let d2 = &d;
    let recomputing = async move { recompute(d2, &[1]).await };
    let (held, ()) = tokio::join!(recomputing, async {
        d.wait_blocked_behind(frozen.backend_pid).await;
        d.release(&mut page, PausePoint::AfterEntryLock).await;
    });
    held.commit().await;
    page.finish().await;
    assert_eq!(partials(&d, 1).await, ["(2,2,15,t)"]);
    assert_oracle(&mut d).await;
}

/// R2: a group move's sides are each checked against their own group's
/// basis. The move of row 1 from group 1 to 2 is visible in group 2's basis
/// (a recompute after it), not group 1's: the page applies the old side to
/// group 1 and refuses the new side for group 2.
#[tokio::test]
async fn r2_a_move_is_checked_against_each_groups_basis() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 20)",
    )
    .await;
    write(&d, "update public.src set g = 2 where id = 1").await;
    let batch = d.seal().await;
    recompute(&d, &[2]).await.commit().await;
    d.drain(batch, "a").await;
    assert_eq!(partials(&d, 1).await, ["(0,0,0,f)"]);
    assert_eq!(partials(&d, 2).await, ["(2,2,30,t)"]);
    assert_oracle(&mut d).await;
}

/// R2 per transaction: two transactions' changes to one group on one page,
/// one before a recompute of the group and one after. The page applies only
/// the later one.
#[tokio::test]
async fn r2_one_page_applies_only_the_transactions_its_groups_basis_missed() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    write(&d, "insert into public.src values (2, 1, 5)").await;
    recompute(&d, &[1]).await.commit().await;
    write(&d, "insert into public.src values (3, 1, 1)").await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_eq!(partials(&d, 1).await, ["(3,3,16,t)"]);
    assert_oracle(&mut d).await;
}

/// R2 by visibility, not position (#558 exp 2's scenario 10): a writer's
/// change is in flight when a recompute of its group takes its snapshot, so
/// the basis doesn't see it, though the writer's transaction id is below the
/// snapshot's `xmax` (a later transaction completed first). The page applies
/// it. A filter that orders the change against the basis (planted bug
/// `gb_lsn_not_visibility`) refuses it, and the group loses it.
#[tokio::test]
async fn r2_a_change_in_flight_at_the_basis_applies() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    let writer = d.user().await;
    writer
        .batch_execute("begin; insert into public.src values (2, 1, 5)")
        .await
        .expect("the in-flight write");
    // A later transaction completes first, so the snapshot's `xmax` is past
    // the writer's and lists it as in flight.
    write(&d, "insert into public.src values (3, 2, 1)").await;
    recompute(&d, &[1]).await.commit().await;
    assert_eq!(partials(&d, 1).await, ["(1,1,10,t)"], "the basis missed it");
    writer.batch_execute("commit").await.expect("commit");
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_eq!(partials(&d, 1).await, ["(2,2,15,t)"]);
    assert_oracle(&mut d).await;
}

/// R1 between pages: a page on a group waits at its lock statement for a
/// page that has applied to the group and not committed, then applies
/// against the committed row. A page that didn't lock the group (planted bug
/// `gb_skip_group_lock`) reads the row before the first commits, and its
/// update by `ctid` then finds the row moved and adds nothing.
#[tokio::test]
async fn r1_a_page_waits_for_a_page_that_applied_to_its_group() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    write(&d, "insert into public.src values (2, 1, 5)").await;
    let first = d.seal().await;
    write(&d, "insert into public.src values (3, 1, 1)").await;
    let second = d.seal().await;
    let mut held = d
        .drain_frozen(first, "a", &[(PausePoint::AfterGroupUpsert, "public.agg")])
        .await;
    let frozen = held.reached(PausePoint::AfterGroupUpsert).await;
    let page = d.drain_frozen(second, "b", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut held, PausePoint::AfterGroupUpsert).await;
    held.finish().await;
    page.finish().await;
    assert_eq!(partials(&d, 1).await, ["(3,3,16,f)"]);
    assert_oracle(&mut d).await;
}

// ----------------------------------------------------------------------- F2

/// F2: a key's changes on one page are never folded. Row 1 moves from group
/// 1 to 2 (T1), group 2 is recomputed (its basis sees T1), and row 1 moves on
/// to group 3 (T2), all before the page. Per (group, transaction): group 1
/// takes T1's old side, group 2 refuses T1's new side and takes T2's old
/// side, group 3 takes T2's new side. A fold (T1's old image, T2's new
/// image) would never take row 1 out of group 2.
#[tokio::test]
async fn f2_a_basis_between_two_changes_of_a_key() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    write(&d, "update public.src set g = 2 where id = 1").await;
    recompute(&d, &[2]).await.commit().await;
    write(&d, "update public.src set g = 3 where id = 1").await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_eq!(partials(&d, 1).await, ["(0,0,0,f)"]);
    assert_eq!(partials(&d, 2).await, ["(0,0,0,t)"]);
    assert_eq!(partials(&d, 3).await, ["(1,1,10,f)"]);
    assert_oracle(&mut d).await;
}

/// F2: a key passing through three groups on one page names each of them.
#[tokio::test]
async fn f2_a_key_through_three_groups_on_one_page() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 1)",
    )
    .await;
    write(
        &d,
        "update public.src set g = 2 where id = 1; update public.src set g = 3 where id = 1",
    )
    .await;
    write(&d, "update public.src set g = 4, v = 11 where id = 1").await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_oracle(&mut d).await;
}

// ----------------------------------------------------------------------- R3

/// R3 (#1099): nested writes stage exact transition images, so each
/// statement's sides sum to its rows' net change: an `AFTER ROW` trigger
/// that rewrites the row it fires on, one that deletes an insert, and one
/// that updates it.
#[tokio::test]
async fn r3_nested_writes_sum_once() {
    let mut d = start_with(
        AGG,
        "create table public.src (id integer primary key, g integer, v integer); \
         create index on public.src (g)",
        &[("id", int4()), ("g", int4()), ("v", int4())],
        "create function public.app() returns trigger language plpgsql as $$ \
         begin \
             if pg_trigger_depth() = 1 then \
                 if tg_op = 'INSERT' and new.v < 0 then \
                     delete from public.src where id = new.id; \
                 elsif tg_op = 'INSERT' and new.v = 0 then \
                     update public.src set v = 100, g = 9 where id = new.id; \
                 elsif tg_op = 'UPDATE' and new.v % 2 = 1 then \
                     update public.src set v = new.v + 1 where id = new.id; \
                 end if; \
             end if; \
             return null; \
         end $$; \
         create trigger app after insert or update on public.src \
             for each row execute function public.app()",
    )
    .await;
    for sql in [
        "insert into public.src values (1, 1, 2), (2, 1, -1), (3, 2, 0), (4, 2, 4)",
        "update public.src set v = 3 where id = 1",
        "update public.src set v = v + 1",
        "insert into public.src values (5, 1, -7); update public.src set g = 2 where id = 1",
    ] {
        step(&mut d, sql).await;
        assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await, "after {sql}");
    }
}

// ------------------------------------------------------------- decision 4

/// Pages commit out of order, so a group's partials can describe no real
/// data: a delete's page applied before its insert's leaves group 1 at
/// `n = -1`. The group is not visible while `n <= 0`, and the converged
/// state is exact.
#[tokio::test]
async fn a_group_is_visible_only_while_its_count_is_above_zero() {
    let mut d = start().await;
    write(&d, "insert into public.src values (1, 1, 10)").await;
    let insert = d.seal().await;
    write(&d, "delete from public.src where id = 1").await;
    let delete = d.seal().await;
    d.drain(delete, "a").await;
    assert_eq!(partials(&d, 1).await, ["(-1,-1,-10,f)"]);
    assert!(d.rows(ACTUAL).await.is_empty());
    d.drain(insert, "b").await;
    assert_eq!(partials(&d, 1).await, ["(0,0,0,f)"]);
    assert_oracle(&mut d).await;
}

// ----------------------------------------------------------------------- R4

/// How many hidden rows the one definition has, zero groups included.
async fn hidden_rows(d: &Driver) -> i64 {
    scalar(d, &format!("select count(*) from {}", hidden(d).await))
        .await
        .parse()
        .expect("a count")
}

/// R4: groups that churn (each filled and emptied again, the shape that made
/// #1043's JOIN model 16x slower with the zero rows left in place) leave
/// zero groups with no basis, and the GC collects every one, so the hidden
/// table holds only the groups with rows however many have come and gone.
#[tokio::test]
async fn r4_the_gc_bounds_a_hidden_table_whose_groups_churn() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (0, 0, 1)").await;
    for round in 0..5 {
        let first = 1 + round * 1_000;
        step(
            &mut d,
            &format!(
                "insert into public.src select i, i, i from generate_series({first}, {}) i",
                first + 999
            ),
        )
        .await;
        step(&mut d, "delete from public.src where id > 0").await;
        assert_eq!(hidden_rows(&d).await, 1_001, "round {round}: before the GC");
        assert_eq!(d.collect_tombstones().await, 1_000, "round {round}");
        assert_eq!(hidden_rows(&d).await, 1, "round {round}: after the GC");
    }
    assert_eq!(partials(&d, 0).await, ["(1,1,1,f)"]);
    assert_oracle(&mut d).await;
}

/// R4, #1043's `RV.gc-n0`: pages applied out of order leave group 1 at
/// `n = 0` with its sum still owed (`-2`). A GC that tests `n = 0` alone
/// would delete it, and the late page would then leave `sum = 2` behind.
/// Only a group whose every partial is zero is collected.
#[tokio::test]
async fn r4_a_group_with_a_nonzero_partial_stays() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 1)").await;
    write(&d, "update public.src set v = 3 where id = 1").await;
    let update = d.seal().await;
    write(&d, "delete from public.src where id = 1").await;
    let delete = d.seal().await;
    d.drain(delete, "a").await;
    assert_eq!(partials(&d, 1).await, ["(0,0,-2,f)"]);
    assert_eq!(d.collect_tombstones().await, 0);
    d.drain(update, "b").await;
    assert_eq!(partials(&d, 1).await, ["(0,0,0,f)"]);
    assert_eq!(d.collect_tombstones().await, 1);
    assert_eq!(hidden_rows(&d).await, 0);
    assert_oracle(&mut d).await;
}

/// R4 with a basis (#1043's `R.recreate-gc-race`): a recompute empties
/// group 5 after the delete C1 is sealed but before it drains, so the zero
/// group's basis already counts C1. The GC keeps it while C1's segment is
/// undrained, though a later segment has drained, and C1's page is then
/// refused by the basis. A GC by the highest drained segment, or by
/// `n = 0` alone, would let C1 apply to a fresh group: `n = -1`. Once the
/// contiguous drained prefix passes the stamp, the group goes.
#[tokio::test]
async fn r4_a_zero_group_with_a_basis_stays_until_the_prefix_passes_its_stamp() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 5, 4), (2, 6, 1)").await;
    write(&d, "delete from public.src where id = 1").await;
    let c1 = d.seal().await;
    recompute(&d, &[5]).await.commit().await;
    assert_eq!(partials(&d, 5).await, ["(0,0,0,t)"]);
    write(&d, "update public.src set v = 2 where id = 2").await;
    let later = d.seal().await;
    d.drain(later, "a").await;
    assert_eq!(d.collect_tombstones().await, 0);
    assert_eq!(partials(&d, 5).await, ["(0,0,0,t)"]);
    d.drain(c1, "b").await;
    assert_eq!(partials(&d, 5).await, ["(0,0,0,t)"], "C1 refused");
    // The recompute stamped the segment that was active when it ran, which
    // drains only once sealed: the settle seals and drains it.
    assert_oracle(&mut d).await;
    assert_eq!(partials(&d, 5).await, Vec::<String>::new());
}

/// R4: a basis with no stamp protects its group for good, since nothing
/// says when the changes it saw have drained.
#[tokio::test]
async fn r4_a_basis_without_a_stamp_keeps_its_group() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 1)").await;
    recompute(&d, &[2]).await.commit().await;
    write(
        &d,
        &format!("update {} set __stamp = null where g = 2", hidden(&d).await),
    )
    .await;
    d.settle().await;
    assert_eq!(partials(&d, 2).await, ["(0,0,0,t)"]);
}

/// R4: the GC never waits on a page. A page frozen holding the lock of zero
/// group 1 keeps it from the GC, which skips it and returns; the page then
/// refills the group.
#[tokio::test]
async fn r4_the_gc_skips_a_group_a_page_holds() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    step(&mut d, "delete from public.src where id = 1").await;
    assert_eq!(partials(&d, 1).await, ["(0,0,0,f)"]);
    write(&d, "insert into public.src values (2, 1, 5)").await;
    let refill = d.seal().await;
    let mut page = d
        .drain_frozen(refill, "a", &[(PausePoint::AfterEntryLock, "public.agg")])
        .await;
    page.reached(PausePoint::AfterEntryLock).await;
    assert_eq!(d.collect_tombstones().await, 0);
    assert_eq!(partials(&d, 1).await, ["(0,0,0,f)"]);
    d.release(&mut page, PausePoint::AfterEntryLock).await;
    page.finish().await;
    assert_eq!(partials(&d, 1).await, ["(1,1,5,f)"]);
    assert_oracle(&mut d).await;
}

/// R4, the other order: the GC has claimed and deleted zero group 1 but not
/// committed when a page refilling it starts. The page's lock statement
/// queues on the GC's delete, and once the GC commits it inserts the group
/// afresh, with no basis, and applies its change to it.
#[tokio::test]
async fn r4_a_page_meeting_a_group_the_gc_is_deleting_inserts_it_afresh() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    step(&mut d, "delete from public.src where id = 1").await;
    assert_eq!(partials(&d, 1).await, ["(0,0,0,f)"]);
    let hidden = hidden(&d).await;
    let gc = d.user().await;
    let gc_pid: i32 = gc
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    gc.batch_execute("begin").await.expect("begin");
    let deleted = gc
        .execute(
            &format!(
                "delete from {hidden} where ctid = any(array( \
                     select ctid from {hidden} where __zero and __basis is null \
                     order by g for update skip locked))"
            ),
            &[],
        )
        .await
        .expect("the GC's delete");
    assert_eq!(deleted, 1);
    write(&d, "insert into public.src values (2, 1, 5)").await;
    let refill = d.seal().await;
    let page = d.drain_frozen(refill, "a", &[]).await;
    d.wait_blocked_behind(gc_pid).await;
    gc.batch_execute("commit").await.expect("commit");
    page.finish().await;
    assert_eq!(partials(&d, 1).await, ["(1,1,5,f)"]);
    assert_oracle(&mut d).await;
}

// ------------------------------------------------------------- plan control

/// The page's apply statement fetches the hidden rows it updates by TID and
/// probes both tables' group identity indexes, `NULL` groups included:
/// nothing in it scans either table whole while the page holds its groups'
/// locks (#1100 G2b). The hidden rows' TID scan is over the array of the
/// page's `ctid`s, not a probe per row from a nested loop: on this test's
/// tiny tables `enable_seqscan = off` alone makes the planner pick that
/// probe (a disabled nested loop), but on a large hidden table with real
/// statistics nothing stops it hashing the whole table instead.
#[tokio::test]
async fn the_apply_statement_reads_no_table_whole() {
    let d = start().await;
    let plan = trellis::dev::staging::explain_group_apply(d.pool(), "agg")
        .await
        .expect("explain");
    let lines: Vec<&str> = plan.lines().collect();
    assert!(
        lines
            .windows(2)
            .any(|w| w[0].contains("Tid Scan on agg_") && w[1].contains("TID Cond: (ctid = ANY")),
        "{plan}"
    );
    assert!(!plan.contains("Seq Scan on agg"), "{plan}");
}

/// A zero-group GC batch walks the partial `where __zero` index in key
/// order: its cost is the zero groups, not the groups.
#[tokio::test]
async fn the_zero_group_gc_walks_its_index() {
    let mut d = start().await;
    let hidden = hidden(&d).await;
    let (_, table) = hidden.split_once('.').expect("qualified");
    let plan = trellis::staging::retire::explain_collect_zero_groups(&mut d.ctl, &hidden, 0)
        .await
        .expect("explain");
    assert!(
        plan.contains(&format!("Index Scan using {table}_g_idx")),
        "{plan}"
    );
    assert!(!plan.contains("Seq Scan"), "{plan}");
}

// ----------------------------------------------------------------------- R7

/// R7: 8 workers × 20 batches of 320 rows over the same 20 groups, every
/// batch split across the 8 workers. Every page locks its groups in one
/// sorted statement, so no worker deadlocks and the groups are right.
#[tokio::test]
async fn r7_concurrent_pages_over_shared_groups_never_deadlock() {
    let mut d = start().await;
    let before = d.deadlocks_logged().len();
    let mut seed: u64 = 12345;
    for round in 0..20 {
        let mut writers = Vec::new();
        for writer in 0..8 {
            let mut rows = Vec::new();
            for i in 0..40 {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let id = writer * 100_000 + round * 100 + i;
                rows.push(format!(
                    "({id}, {}, {})",
                    (seed >> 33) % 20,
                    (seed >> 20) % 100
                ));
            }
            // The writers overlap on the rows they update, and an update
            // walks them in heap order: two writers could take row locks in
            // opposite orders and deadlock each other (#1258). Locking the
            // rows in id order first makes the writers deadlock-free, so a
            // logged deadlock below can only be between Trellis's pages.
            let sql = format!(
                "insert into public.src values {}; \
                 select id from public.src where id % 3 = {k} order by id for update; \
                 update public.src set g = (g + 7) % 20 where id % 3 = {k}",
                rows.join(", "),
                k = writer % 3
            );
            let user = d.user().await;
            writers.push(tokio::spawn(async move {
                user.batch_execute(&sql).await.expect("a writer's rows");
            }));
        }
        for writer in writers {
            writer.await.expect("writer task");
        }
        let batch = d.seal().await;
        let mut workers = Vec::new();
        for w in 0..8 {
            workers.push(d.drain_share(batch, &format!("w{w}"), 8).await);
        }
        for worker in workers {
            worker.finish().await;
        }
    }
    assert_eq!(d.deadlocks_logged().split_off(before), Vec::<String>::new());
    assert_oracle(&mut d).await;
}

// ------------------------------------------------------------ truncate (F5)

/// The ids of the segments a free worker could claim now, in order: the
/// drain barrier's view (`next_claimable_segments`).
async fn claimable(d: &Driver) -> Vec<i64> {
    trellis::staging::apply::next_claimable_segments(&d.ctl, 1_000_000)
        .await
        .expect("next claimable segments")
}

/// A source `TRUNCATE` empties the hidden table and the target, and raises
/// the floor; an insert later in the truncating transaction applies, as
/// does one after it.
#[tokio::test]
async fn a_truncate_clears_the_groups_and_raises_the_floor() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 20), (3, 3, 30)",
    )
    .await;
    step(&mut d, "delete from public.src where id = 3").await;
    step(
        &mut d,
        "begin; insert into public.src values (4, 1, 1); truncate public.src; \
         insert into public.src values (5, 2, 5); commit",
    )
    .await;
    assert_eq!(
        d.rows(&format!(
            "select g, __n, __basis is null from {} order by g",
            hidden(&d).await
        ))
        .await,
        ["(2,1,t)"],
        "the wipe took every group, the zero group 3 among them"
    );
    let floor = d
        .rows("select floor::text from ledger_truncate_floor where target_table = 'public.agg'")
        .await;
    assert_eq!(floor.len(), 1, "the floor is raised");
    step(&mut d, "insert into public.src values (6, 1, 6)").await;
    assert_oracle(&mut d).await;
}

/// T.floor: a change from before the truncate that reaches a page after the
/// truncate's page is refused by the floor alone. The driver drains the two
/// segments out of order, which the barrier never does.
#[tokio::test]
async fn a_change_from_before_a_truncate_is_refused_after_it() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    write(&d, "insert into public.src values (2, 1, 5), (3, 2, 7)").await;
    let before = d.seal().await;
    write(
        &d,
        "truncate public.src; insert into public.src values (4, 2, 1)",
    )
    .await;
    let truncate = d.seal().await;
    d.drain(truncate, "a").await;
    d.drain(before, "b").await;
    assert_eq!(partials(&d, 1).await, ["(0,0,0,f)"]);
    assert_oracle(&mut d).await;
}

/// T.concurrent-earlier, and the barrier's other side: a page of an earlier
/// segment still open (it has locked its groups and is frozen there) holds
/// the truncate's segment back, and a later segment waits for the
/// truncate's. A wipe that ran under the earlier page would leave the
/// group row that page creates.
#[tokio::test]
async fn the_truncate_page_waits_for_earlier_pages_and_later_ones_for_it() {
    let mut d = start().await;
    write(&d, "insert into public.src values (1, 9, 10)").await;
    let earlier = d.seal().await;
    write(&d, "truncate public.src").await;
    let truncate = d.seal().await;
    write(&d, "insert into public.src values (2, 9, 5)").await;

    let mut page = d
        .drain_frozen(earlier, "a", &[(PausePoint::AfterEntryLock, "public.agg")])
        .await;
    page.reached(PausePoint::AfterEntryLock).await;
    assert_eq!(
        claimable(&d).await,
        [earlier],
        "the truncate isn't claimable while an earlier page is open"
    );
    d.release(&mut page, PausePoint::AfterEntryLock).await;
    page.finish().await;
    let later = d.seal().await;

    assert_eq!(
        claimable(&d).await,
        [truncate],
        "the truncate's segment drains alone"
    );
    d.drain(truncate, "b").await;
    assert_eq!(claimable(&d).await, [later]);
    d.drain(later, "c").await;
    assert_eq!(partials(&d, 9).await, ["(1,1,5,f)"]);
    assert_oracle(&mut d).await;
}

/// R9: segments are planned in order around a truncate. A seal that stopped
/// between its phases leaves the truncate's segment unfenced, its
/// `has_truncate` undecided. The seal gate seals no later segment until its
/// fence is published, so none can be claimed past it: drained first, a
/// later segment's post-truncate insert would be wiped by the truncate.
#[tokio::test]
async fn no_segment_is_sealed_past_an_unfenced_one() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    write(&d, "truncate public.src").await;
    d.retire().await;
    let stuck = trellis::staging::seal::seal_phase1(&mut d.ctl)
        .await
        .expect("seal phase 1")
        .sealed_seg_seq;
    write(&d, "insert into public.src values (2, 1, 5)").await;
    assert_eq!(
        d.try_seal().await,
        None,
        "the seal gate holds the next seal"
    );
    assert!(claimable(&d).await.is_empty());
    trellis::staging::seal::seal_phase2(&d.ctl, stuck, drain_driver::WAKE)
        .await
        .expect("seal phase 2");
    let later = d.seal().await;
    assert_eq!(claimable(&d).await, [stuck]);
    d.drain(stuck, "a").await;
    assert_eq!(claimable(&d).await, [later]);
    d.drain(later, "b").await;
    assert_oracle(&mut d).await;
}

// --------------------------------------------------- image-less changes (R8)

/// An image-less change (a `Recompute`) has no sides: its page recomputes the
/// group of the key's current row, and of its prior-image hint, under the
/// page's own group locks. The recompute is absolute, so it also repairs a
/// group whose hidden partials had drifted.
#[tokio::test]
async fn an_image_less_change_recomputes_the_groups_it_names() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 1, 5), (3, 2, 7)",
    )
    .await;
    // Drift both groups behind the pipeline's back.
    d.ctl
        .batch_execute(&format!(
            "update {h} set __n = __n + 5, __sum_1 = __sum_1 + 100; \
             update public.agg set total = total + 100, n = n + 5",
            h = hidden(&d).await
        ))
        .await
        .expect("drift");
    d.stage_recomputes_with_prior(
        SRC,
        &[("1", Some(r#"{"id": 1, "g": 2, "v": 10}"#.to_string()))],
    )
    .await;
    write(&d, "insert into public.src values (4, 1, 1)").await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_eq!(partials(&d, 1).await, ["(3,3,16,t)"]);
    assert_eq!(partials(&d, 2).await, ["(1,1,7,t)"]);
    assert_oracle(&mut d).await;
}

// ------------------------------------------------- quarantine release (R8)

/// Holds `key` for the one definition, as an eviction would: from now on
/// every page parks its changes.
async fn hold(d: &Driver, key: &str) {
    d.ctl
        .execute(
            "insert into poison (transform_id, src_table, key, last_error) \
             select id, $1, $2, 'test' from transform_definitions \
             where target_table = 'public.agg'",
            &[&SRC, &key],
        )
        .await
        .expect("hold the key");
}

/// The `g` of each group the held row of `key` names, sorted.
async fn held_groups(d: &Driver, key: &str) -> Vec<String> {
    d.rows(&format!(
        "select coalesce(g::jsonb ->> 'g', 'null') from poison_held, unnest(agg_groups) g \
         where key = '{key}' order by 1"
    ))
    .await
}

async fn release(d: &Driver, key: &str) {
    trellis::staging::release_key(d.pool(), "agg", SRC, key)
        .await
        .expect("release the key");
}

/// A held key moves 1 → 2 → 3, each change parked. Meanwhile an absolute
/// write to group 2 (a recompute) counts the key there. The release
/// recomputes every group the parked changes named, so group 2 loses the
/// key again; recomputing only the current row's group would leave it.
#[tokio::test]
async fn a_release_recomputes_every_group_the_held_key_moved_through() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 20)",
    )
    .await;
    hold(&d, "1").await;
    step(&mut d, "update public.src set g = 2 where id = 1").await;
    recompute(&d, &[2]).await.commit().await;
    step(&mut d, "update public.src set g = 3, v = 11 where id = 1").await;
    assert_eq!(held_groups(&d, "1").await, ["(1)", "(2)", "(3)"]);
    assert_eq!(
        partials(&d, 1).await,
        ["(1,1,10,f)"],
        "group 1 drifts while held"
    );
    release(&d, "1").await;
    assert_eq!(partials(&d, 1).await, ["(0,0,0,t)"]);
    assert_eq!(partials(&d, 2).await, ["(1,1,20,t)"]);
    assert_eq!(partials(&d, 3).await, ["(1,1,11,t)"]);
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
    assert_oracle(&mut d).await;
}

/// Another key's release is the absolute write during the hold: keys 1 and
/// 2 are both held in group 1 and both move to group 2. Key 2's release
/// recomputes group 2 counting key 1 there; key 1's release still covers
/// groups 1 and 2, whatever key 2's did.
#[tokio::test]
async fn a_release_after_another_keys_release_to_the_same_group() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 1, 20), (3, 2, 1)",
    )
    .await;
    hold(&d, "1").await;
    hold(&d, "2").await;
    step(&mut d, "update public.src set g = 2 where id in (1, 2)").await;
    release(&d, "2").await;
    step(&mut d, "update public.src set v = 12 where id = 1").await;
    release(&d, "1").await;
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
    assert_oracle(&mut d).await;
}

/// A change to the key still in the ring at the release applies as a delta
/// afterwards, filtered by the recomputed groups' basis: its move into group
/// 3 is already counted there, and its old side leaves group 2, which the
/// release recomputed from the current row too.
#[tokio::test]
async fn a_change_undrained_at_the_release_is_filtered_by_the_basis() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    hold(&d, "1").await;
    step(&mut d, "update public.src set g = 2 where id = 1").await;
    write(&d, "update public.src set g = 3 where id = 1").await;
    let pending = d.seal().await;
    write(&d, "insert into public.src values (2, 4, 1)").await;
    release(&d, "1").await;
    assert_eq!(partials(&d, 3).await, ["(1,1,10,t)"]);
    d.drain(pending, "a").await;
    assert_eq!(partials(&d, 2).await, ["(0,0,0,t)"]);
    assert_eq!(partials(&d, 3).await, ["(1,1,10,t)"]);
    assert_oracle(&mut d).await;
}

/// The automatic retry of a held key releases it the same way.
#[tokio::test]
async fn the_automatic_retry_recomputes_the_held_keys_groups() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 20)",
    )
    .await;
    hold(&d, "1").await;
    step(&mut d, "update public.src set g = 2, v = 3 where id = 1").await;
    let poisoned_at: std::time::SystemTime = d
        .ctl
        .query_one("select poisoned_at from poison where key = '1'", &[])
        .await
        .expect("poisoned_at")
        .get(0);
    trellis::staging::quarantine::retry_held_key(d.pool(), "agg", SRC, "1", poisoned_at)
        .await
        .expect("retry the key");
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
    assert_oracle(&mut d).await;
}

/// The release's recompute is an absolute write between other keys'
/// changes to its group: one committed before it but still in the ring is
/// counted by the recompute and refused by its basis when its page runs
/// (before the page of the release's own `Recompute`, which would recompute
/// the group again), and one still open during it is not counted and
/// applies afterwards.
#[tokio::test]
async fn a_release_counts_each_concurrent_change_to_its_group_once() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 1, 20)",
    )
    .await;
    hold(&d, "1").await;
    step(&mut d, "update public.src set v = 11 where id = 1").await;
    write(&d, "insert into public.src values (3, 1, 5)").await;
    let pending = d.seal().await;
    let open = d.user().await;
    open.batch_execute("begin; update public.src set v = 25 where id = 2")
        .await
        .expect("an open change");
    release(&d, "1").await;
    assert_eq!(partials(&d, 1).await, ["(3,3,36,t)"]);
    d.drain(pending, "a").await;
    assert_eq!(
        partials(&d, 1).await,
        ["(3,3,36,t)"],
        "the recompute counted the insert; its delta is refused"
    );
    open.batch_execute("commit").await.expect("commit");
    assert_oracle(&mut d).await;
}

/// A held key leaves the `NULL` group: its held row names that group by a
/// `NULL` part, and the release recomputes it by `IS NOT DISTINCT FROM`,
/// keeping the group's other row.
#[tokio::test]
async fn a_release_recomputes_a_null_group() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, null, 10), (2, null, 20), (3, 2, 1)",
    )
    .await;
    hold(&d, "1").await;
    step(&mut d, "update public.src set g = 2 where id = 1").await;
    assert_eq!(held_groups(&d, "1").await, ["(2)", "(null)"]);
    release(&d, "1").await;
    assert_eq!(
        d.rows(&format!(
            "select __n, __sum_1 from {} where g is null",
            hidden(&d).await
        ))
        .await,
        ["(1,20)"]
    );
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
    assert_oracle(&mut d).await;
}

/// The evicting change is parked with its groups: a constraint on the
/// target refuses the change of key 1 that moves it into group 2, and the
/// isolation evicts the key at a threshold of one. Its held row names both
/// groups, and once the constraint is gone the release fixes both.
#[tokio::test]
async fn the_evicting_changes_groups_are_held_and_released() {
    use trellis::staging::quarantine::{IsolationOutcome, isolate_and_evict};
    use trellis::staging::{BucketFilter, claim, fold};

    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 20)",
    )
    .await;
    d.ctl
        .batch_execute("alter table public.agg add constraint small check (total < 25)")
        .await
        .expect("constrain the target");
    write(&d, "update public.src set g = 2 where id = 1").await;
    let seg = d.seal().await;
    let folded = {
        let mut conn = d.pool().get().await.expect("pool");
        let txn = conn.transaction().await.expect("txn");
        claim::claim(&txn, seg, "w", 1).await.expect("claim");
        let folded = fold::fold(&txn, seg, BucketFilter::all())
            .await
            .expect("fold");
        txn.commit().await.expect("commit the claim");
        folded
    };
    let outcome = isolate_and_evict(d.pool(), seg, "w", drain_driver::WAKE, &folded, 1, false)
        .await
        .expect("isolation");
    assert!(
        matches!(outcome, IsolationOutcome::Evicted { evicted: 1, .. }),
        "{outcome:?}"
    );
    assert_eq!(held_groups(&d, "1").await, ["(1)", "(2)"]);
    d.drain(seg, "w").await;
    d.ctl
        .batch_execute("alter table public.agg drop constraint small")
        .await
        .expect("lift the constraint");
    assert_eq!(partials(&d, 1).await, ["(1,1,10,f)"], "the move is skipped");
    release(&d, "1").await;
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
    assert_oracle(&mut d).await;
}

// ------------------------------------------------------------------ refusals

/// Define refuses `SUM` over a `numeric` with no declared scale on the group
/// basis (#1096 decision 2).
#[tokio::test]
async fn define_refuses_an_unconstrained_numeric_sum() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = drain_driver::connect(db.dsn()).await;
    client
        .batch_execute(
            "create table public.src (id integer primary key, g integer, m numeric); \
             create index on public.src (g)",
        )
        .await
        .expect("source");
    let columns = [
        ("id".to_string(), int4()),
        ("g".to_string(), int4()),
        ("m".to_string(), ValueType::Numeric),
    ]
    .into_iter()
    .collect();
    let err = trellis::dev::defs::install_definition(
        &db.pool,
        "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(m) AS total",
        &columns,
        "public",
    )
    .await
    .expect_err("refused");
    assert!(err.to_string().contains("no declared scale"), "{err}");
}
