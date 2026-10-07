//! Deterministic interleavings of a definition's registration and a
//! transform resume with drain pages, over a relationship's settled
//! projection (issues #768 and #770), on the real engine, driven by hand
//! through `support/drain_driver.rs`. No test sleeps or polls for
//! convergence (#297).
//!
//! A define and a resume bump their source's version fence, ensure or
//! refresh the projections of the to-one relationships they read, and lock
//! the `relationship_projections` rows of the ones they refresh `for
//! update`. A drain page holds the fences of its sources `for share`, then
//! the `relationship_projections` rows of the relationships it carries a
//! reverse record for `for share`, in `relationship_id` order, then the
//! projection rows. Each test sets up a page and a define or resume so that
//! a lock taken in another order closes a cycle, and checks that none did:
//! the server logged no deadlock and every transaction committed.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use drain_driver::{Driver, RunningDrain};
use trellis::defs::ValueType;
use trellis::staging::interleave::PausePoint;

const ONE: &str = "public.one";

fn src_columns() -> HashMap<String, ValueType> {
    [
        ("id", ValueType::Numeric),
        ("g", ValueType::Numeric),
        ("p", ValueType::Numeric),
        ("q", ValueType::Numeric),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect()
}

/// `public.par (id, w, x)` with parents 1 and 2, `public.src (id, g, p)`
/// with children 1 and 2 of parent 1 and 3 of parent 2, the relationship
/// `parent` from `src.p` to `par.id`, the 1-1 definitions `one` on `src`
/// reading `parent.w`, `sg` on `src` reading no relationship and `pt` on
/// `par`, both tables captured, drained to quiescence.
async fn start_parent() -> Driver {
    let columns: Vec<(&str, ValueType)> = vec![
        ("id", ValueType::Numeric),
        ("g", ValueType::Numeric),
        ("p", ValueType::Numeric),
        ("q", ValueType::Numeric),
        ("w", ValueType::Numeric),
    ];
    Driver::start_with_relationships(
        "create table public.par (id integer primary key, w numeric, x numeric); \
         create table public.src (id integer primary key, g integer, p integer, q integer); \
         insert into public.par values (1, 10, 100), (2, 20, 200); \
         insert into public.src values (1, 1, 1, null), (2, 1, 1, null), (3, 2, 2, null);",
        &columns,
        &["RELATIONSHIP parent FROM src.p TO par.id"],
        &[
            "TRANSFORM one FROM public.src SELECT g AS g, parent.w AS pw",
            "TRANSFORM sg FROM public.src SELECT g AS g",
            "TRANSFORM pt FROM public.par SELECT w AS w",
        ],
        &["public.par", "public.src"],
    )
    .await
}

async fn write(d: &Driver, sql: &str) {
    d.user()
        .await
        .batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Registers `text` on `public.src` in its own task.
fn define(d: &Driver, text: &'static str) -> tokio::task::JoinHandle<Result<(), String>> {
    let pool = d.pool().clone();
    tokio::spawn(async move {
        trellis::defs::install_definition(&pool, text, &src_columns(), "public")
            .await
            .map(drop)
            .map_err(|err| err.to_string())
    })
}

/// How many backends wait on a lock another holds.
async fn blocked(d: &Driver) -> i64 {
    d.ctl
        .query_one(
            "select count(*) from pg_stat_activity where cardinality(pg_blocking_pids(pid)) > 0",
            &[],
        )
        .await
        .expect("read pg_stat_activity")
        .get(0)
}

/// Waits until `drain` has finished, or `n` backends are queued on locks:
/// whichever the order the engine takes its locks in leads to.
async fn finished_or_blocked(d: &Driver, drain: &RunningDrain, n: i64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !drain.is_finished() && blocked(d).await < n {
        assert!(
            Instant::now() < deadline,
            "the drain neither finished nor queued within 60 s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn assert_no_deadlock(d: &Driver) {
    let deadlocks = d.deadlocks_logged();
    assert!(
        deadlocks.is_empty(),
        "the server logged a deadlock:\n{}",
        deadlocks.join("\n---\n"),
    );
}

/// Issue #770: a define widens the projections it reads (a new column is an
/// `alter table`, which locks the projection) before it bumps its source's
/// version fence. A page holding the fence, frozen after its entry lock,
/// carries a change of child 1, so it bumps parent 1's projection row
/// (`__trellis_gen`) after its target write. The define queues on the page
/// at the fence; released, the page queues on the define's lock on the
/// projection, and the cycle is closed. The define must bump the fence
/// before it touches a projection.
#[tokio::test]
async fn a_define_waiting_on_a_page_at_the_fence_holds_no_projection() {
    let mut d = start_parent().await;
    write(&d, "update public.src set g = 5 where id = 1").await;
    let batch = d.seal().await;
    let mut page = d
        .drain_frozen(batch, "page", &[(PausePoint::AfterEntryLock, ONE)])
        .await;
    let frozen = page.reached(PausePoint::AfterEntryLock).await;
    let defined = define(&d, "TRANSFORM two FROM public.src SELECT parent.x AS px");
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut page, PausePoint::AfterEntryLock).await;
    let drained = page.finish_result().await;
    let defined = defined.await.expect("the define's task");
    assert_no_deadlock(&d);
    drained.expect("the page commits");
    defined.expect("the define commits");
    settle_and_check(&mut d).await;
}

/// The define's refresh (issue #768) locks the relationship's
/// `relationship_projections` row `for update`. A page carrying a reverse
/// record for the relationship holds that row `for share` before it writes
/// the projection. Had the define widened the projection (a new column is
/// an `alter table`, which locks it) before it took that row, a page
/// between the two would queue on the projection while the define queued on
/// the row. `one`, the relationship's only reader, is paused, so the define
/// refreshes the projection. Two parents' changes have been computed: the
/// first page has written the projection and is frozen before it commits,
/// the second is frozen after its own target's entry lock, before it takes
/// the row. The define queues on the first page; the second then goes on.
#[tokio::test]
async fn a_define_refreshing_a_projection_takes_its_row_before_the_projection() {
    let mut d = start_parent().await;
    d.ctl
        .execute(
            "update transform_definitions set status = 'paused' where target_table = $1",
            &[&ONE],
        )
        .await
        .expect("pause one");
    // Both sealed first: a page frozen before it commits holds its
    // segment, which a seal waits for.
    write(&d, "update public.par set w = 11 where id = 1").await;
    let first_batch = d.seal().await;
    write(&d, "update public.par set w = 21 where id = 2").await;
    let second_batch = d.seal().await;
    let mut first = d
        .drain_frozen(
            first_batch,
            "first",
            &[(PausePoint::BeforeCommit, "public.pt")],
        )
        .await;
    let frozen = first.reached(PausePoint::BeforeCommit).await;
    let mut second = d
        .drain_frozen(
            second_batch,
            "second",
            &[(PausePoint::AfterEntryLock, "public.pt")],
        )
        .await;
    second.reached(PausePoint::AfterEntryLock).await;
    let defined = define(&d, "TRANSFORM two FROM public.src SELECT parent.x AS px");
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut second, PausePoint::AfterEntryLock).await;
    // Either the second page drains past the define, or it queues on it.
    finished_or_blocked(&d, &second, 2).await;
    d.release(&mut first, PausePoint::BeforeCommit).await;
    let first_drained = first.finish_result().await;
    let second_drained = second.finish_result().await;
    let defined = defined.await.expect("the define's task");
    assert_no_deadlock(&d);
    first_drained.expect("the first page commits");
    second_drained.expect("the second page commits");
    defined.expect("the define commits");
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_two(&d).await;
}

/// `public.za (id, w)` and `public.al (id, v)`, `public.src (id, g, p, q)`,
/// the relationships `ra` from `src.p` to `za.id` (declared first, so the
/// lower id) and `rb` from `src.q` to `al.id`, the 1-1 definition `one`
/// reading both, paused, every table captured, drained to quiescence.
async fn start_two_parents() -> Driver {
    let columns: Vec<(&str, ValueType)> = vec![
        ("id", ValueType::Numeric),
        ("g", ValueType::Numeric),
        ("p", ValueType::Numeric),
        ("q", ValueType::Numeric),
    ];
    let d = Driver::start_with_relationships(
        "create table public.za (id integer primary key, w numeric); \
         create table public.al (id integer primary key, v numeric); \
         create table public.src (id integer primary key, g integer, p integer, q integer); \
         insert into public.za values (1, 10), (2, 20); \
         insert into public.al values (1, 100), (2, 200); \
         insert into public.src values (1, 1, 1, 1), (2, 1, 2, 2);",
        &columns,
        &[
            "RELATIONSHIP ra FROM src.p TO za.id",
            "RELATIONSHIP rb FROM src.q TO al.id",
        ],
        &["TRANSFORM one FROM public.src SELECT g AS g, ra.w AS zw, rb.v AS av"],
        &["public.za", "public.al", "public.src"],
    )
    .await;
    d.ctl
        .execute(
            "update transform_definitions set status = 'paused' where target_table = $1",
            &[&ONE],
        )
        .await
        .expect("pause one");
    d
}

/// A resume refreshes the projections of every relationship its definition
/// reads. A page carrying a reverse record for each takes their
/// `relationship_projections` rows `for share` in `relationship_id` order,
/// and so must the resume `for update`: by to-side name (`al` before `za`)
/// it would take `rb`'s row while the page, holding `ra`'s, waits for it.
/// A third transaction holding `ra`'s row (as a catch-up's refresh of `za`
/// does) lines the page and the resume up behind it.
#[tokio::test]
async fn a_resume_locks_its_projections_in_relationship_order() {
    let mut d = start_two_parents().await;
    let ra: i64 = d
        .ctl
        .query_one(
            "select id from relationship_definitions where name = 'ra'",
            &[],
        )
        .await
        .expect("read ra's id")
        .get(0);
    let mut third = d.user().await;
    let third_pid: i32 = third
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("read the third's backend pid")
        .get(0);
    let third = third.transaction().await.expect("begin the third");
    third
        .execute(
            "select 1 from relationship_projections where relationship_id = $1 for update",
            &[&ra],
        )
        .await
        .expect("the third locks ra's row");
    write(
        &d,
        "update public.za set w = 11 where id = 1; update public.al set v = 101 where id = 1",
    )
    .await;
    let batch = d.seal().await;
    let page = d.drain_frozen(batch, "page", &[]).await;
    d.wait_blocked_behind(third_pid).await;
    let resumed = tokio::spawn({
        let pool = d.pool().clone();
        async move {
            trellis::staging::quarantine::resume_transform(&pool, "one")
                .await
                .map_err(|err| err.to_string())
        }
    });
    d.wait_blocked(2).await;
    third.commit().await.expect("commit the third");
    let drained = page.finish_result().await;
    let resumed = resumed.await.expect("the resume's task");
    assert_no_deadlock(&d);
    drained.expect("the page commits");
    resumed.expect("the resume commits");
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_eq!(
        d.rows("select id, g, zw, av from public.one order by id")
            .await,
        d.rows(
            "select s.id, s.g, z.w, a.v from public.src s \
             left join public.za z on z.id = s.p left join public.al a on a.id = s.q \
             order by s.id"
        )
        .await,
        "one (left) differs from the join (right)"
    );
}

async fn settle_and_check(d: &mut Driver) {
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_eq!(
        d.rows("select id, g, pw from public.one order by id").await,
        d.rows(
            "select s.id, s.g, p.w from public.src s left join public.par p on p.id = s.p \
             order by s.id"
        )
        .await,
        "one (left) differs from the join (right)"
    );
    assert_two(d).await;
}

async fn assert_two(d: &Driver) {
    assert_eq!(
        d.rows("select id, px from public.two order by id").await,
        d.rows(
            "select s.id, p.x from public.src s left join public.par p on p.id = s.p \
             order by s.id"
        )
        .await,
        "two (left) differs from the join (right)"
    );
}

/// The quoted, qualified projection of the relationship `name`, the name a
/// page arms [`PausePoint::AfterReverseGuards`] by.
async fn projection_of(d: &Driver, name: &str) -> String {
    let table: String = d
        .ctl
        .query_one(
            "select p.projection_table from relationship_projections p \
             join relationship_definitions r on r.id = p.relationship_id \
             where r.name = $1",
            &[&name],
        )
        .await
        .expect("the relationship's projection")
        .get(0);
    format!("\"{}\".\"{table}\"", trellis::config::DEFAULT_SCHEMA)
}

/// Issue #848: a page's relationship reverse records lock their projection
/// rows in key order, the order a page's generation bump and the reverse
/// release take them in (ADR-0002 I5). The page meets its records in page
/// order, by route hash, which puts parent 10 before parent 9. Locked one record
/// at a time, a reverse page frozen after its first record's guards holds
/// parent 10's row; a forward page bumping parents 9 and 10 then locks 9 and
/// queues on 10, and the reverse page, released, queues on 9: a cycle.
/// Locked in key order up front, the reverse page holds both rows when it
/// freezes, and the forward page queues on 9 holding neither.
#[tokio::test]
async fn a_page_locks_its_reverse_projection_rows_in_key_order() {
    let mut d = start_parent().await;
    write(
        &d,
        "insert into public.par values (9, 90, 900), (10, 100, 1000); \
         insert into public.src values (4, 9, 9, null), (5, 10, 10, null);",
    )
    .await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    // Both sealed first: a page frozen before it commits holds its
    // segment, which a seal waits for.
    write(&d, "update public.par set w = w + 1 where id in (9, 10)").await;
    let reverse_batch = d.seal().await;
    // The cycle below needs the reverse page to meet parent 10 first. A page
    // meets its records in page order, by the ring rows' `route` (a hash of
    // the table and key), which puts 10 first. Without the fix the `free`
    // assertion fails whichever comes first.
    assert_eq!(
        d.rows(
            "select key from (select src_table, key, route from seg_0 \
             union all select src_table, key, route from seg_1 \
             union all select src_table, key, route from seg_2 \
             union all select src_table, key, route from seg_3) r \
             where src_table = 'public.par' and key in ('9', '10') \
             group by key order by min(route)"
        )
        .await,
        ["(10)", "(9)"],
        "the page meets parent 10 before parent 9"
    );
    write(&d, "update public.src set g = g + 1 where id in (4, 5)").await;
    let forward_batch = d.seal().await;
    let projection = projection_of(&d, "parent").await;
    let mut reverse = d
        .drain_frozen(
            reverse_batch,
            "reverse",
            &[(PausePoint::AfterReverseGuards, &projection)],
        )
        .await;
    let frozen = reverse.reached(PausePoint::AfterReverseGuards).await;
    // Which of the two rows the frozen page left free, read without
    // waiting, and checked once the pages are done.
    let free = d
        .rows(&format!(
            "select id from {projection} where id in (9, 10) order by id for update skip locked"
        ))
        .await;
    let forward = d.drain_frozen(forward_batch, "forward", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut reverse, PausePoint::AfterReverseGuards)
        .await;
    let reversed = reverse.finish_result().await;
    let forwarded = forward.finish_result().await;
    assert_no_deadlock(&d);
    reversed.expect("the reverse page commits");
    forwarded.expect("the forward page commits");
    assert_eq!(
        free,
        Vec::<String>::new(),
        "the reverse page holds every projection row its records touch before its \
         first record's guards"
    );
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_eq!(
        d.rows("select id, g, pw from public.one order by id").await,
        d.rows(
            "select s.id, s.g, p.w from public.src s left join public.par p on p.id = s.p \
             order by s.id"
        )
        .await,
        "one (left) differs from the join (right)"
    );
}
