//! #623 D8a: capture's live re-read runs only when another write to the
//! same table ran in the capturing statement's span, so a `SERIALIZABLE`
//! write that nothing else touched takes no predicate lock on the source and
//! can't be cancelled with `40001` on Trellis's account.
//!
//! - **No predicate lock** on the source table or its primary key from a
//!   plain insert, also after an empty statement in the same transaction;
//!   two serializable writers inserting interleaved keys on one leaf page
//!   both commit (they didn't with an unconditional re-read).
//! - **The re-read still runs when it's needed**, and the last ring row of
//!   every key carries the live row: a function the statement calls that
//!   rewrites the row at the statement's own trigger depth, and sibling
//!   events of one statement (a writable CTE that inserts a key its main
//!   statement deletes, whose insert is captured first; `INSERT … ON
//!   CONFLICT DO UPDATE` re-keying a row and inserting its old key).
//!   Nested trigger writes are pinned by `capture_reread.rs`,
//!   `ledger_interleavings.rs` and `capture_join.rs`, and here a nested
//!   empty statement after a nested rewrite, which must not restart the
//!   outer span.
//! - **An FK action's update merged into one capture call re-reads** when a
//!   row was updated twice (review): it fires no statement trigger of its
//!   own, so only the repeated key in the old rows shows it.
//! - **The transition path pairs updates by key text**, so a key moved
//!   between equal `numeric` values still stages the old key's delete.
//! - **A missing begin trigger errs towards re-reading.**
//!
//! Capture is installed by hand and the ring is read directly; nothing waits
//! for convergence (#297).

use std::collections::BTreeMap;

use testkit::TestCluster;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, IsolationLevel, NoTls};
use trellis::capture::install::{self, Progress};
use trellis::capture::sql::{CaptureEvent, CaptureSpec, trigger_name};
use trellis::config::DEFAULT_SCHEMA;

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

/// `public.t (id bigint primary key, g int, a int)` holding `(1..=10, i, i)`,
/// captured imaging `g` and `a`.
async fn captured_table(dsn: &str) -> Client {
    let mut raw = connect(dsn).await;
    raw.batch_execute(
        "create table public.t (id bigint primary key, g int, a int); \
         insert into public.t select i, i, i from generate_series(1, 10) i",
    )
    .await
    .expect("create public.t");
    let spec = CaptureSpec::new(
        "public.t",
        vec!["id".to_string()],
        ["g".to_string(), "a".to_string()],
        Vec::new(),
    )
    .expect("valid spec");
    match install::install(&mut raw, DEFAULT_SCHEMA, &spec, None)
        .await
        .expect("install")
    {
        Progress::Done(_) => {}
        Progress::Waiting(wait) => panic!("an install without a deadline landed: {wait}"),
    }
    raw
}

/// The SIREAD locks this session's transaction holds on `public.t` or its
/// primary key, as `locktype:relation` strings.
async fn source_predicate_locks(client: &Client) -> Vec<String> {
    client
        .query(
            "select l.locktype || ':' || l.relation::regclass::text \
             from pg_locks l \
             where l.mode = 'SIReadLock' and l.pid = pg_backend_pid() \
               and l.relation in ('public.t'::regclass, 'public.t_pkey'::regclass) \
             order by 1",
            &[],
        )
        .await
        .expect("pg_locks")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

/// Every ring row for `public.t` as `(key, op, new_image)`, in `(lsn,
/// change_id)` order.
async fn ring(client: &Client) -> Vec<(String, String, Option<String>)> {
    table_ring(client, "public.t").await
}

/// Every ring row for `table` as `(key, op, new_image)`, in `(lsn,
/// change_id)` order.
async fn table_ring(client: &Client, table: &str) -> Vec<(String, String, Option<String>)> {
    let arms: Vec<String> = (0..4)
        .map(|n| format!("select src_table, key, op, new_image, lsn, change_id from seg_{n}"))
        .collect();
    client
        .query(
            &format!(
                "select key, op, new_image::text from ({}) r where src_table = $1 \
                 order by lsn, change_id",
                arms.join(" union all ")
            ),
            &[&table],
        )
        .await
        .expect("read the ring")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

/// The table each key's last ring row describes: its new image, or nothing
/// for a delete.
async fn ring_state(client: &Client) -> BTreeMap<String, String> {
    table_ring_state(client, "public.t").await
}

/// [`ring_state`] for `table`.
async fn table_ring_state(client: &Client, table: &str) -> BTreeMap<String, String> {
    let mut state = BTreeMap::new();
    for (key, op, image) in table_ring(client, table).await {
        match op.as_str() {
            "delete" => {
                state.remove(&key);
            }
            _ => {
                state.insert(key, image.expect("an insert or update carries its image"));
            }
        }
    }
    state
}

/// `public.t`'s rows the ring touched, imaged the way capture images them.
async fn live_state(client: &Client, keys: &[&str]) -> BTreeMap<String, String> {
    client
        .query(
            "select id::text, jsonb_build_object('a', a::text, 'g', g::text, 'id', id::text)::text \
             from public.t where id::text = any($1)",
            &[&keys],
        )
        .await
        .expect("read public.t")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// The ring's state for `keys` agrees with the table.
async fn assert_ring_is_live(client: &Client, keys: &[&str]) {
    let staged: BTreeMap<String, String> = ring_state(client)
        .await
        .into_iter()
        .filter(|(k, _)| keys.contains(&k.as_str()))
        .collect();
    assert_eq!(
        staged,
        live_state(client, keys).await,
        "ring:\n{:#?}",
        ring(client).await
    );
}

// ---------------------------------------------------------------- no lock

/// A plain insert, single-row or batched, takes no SIREAD lock on the
/// source, and neither does one after an empty statement: the empty
/// statement's capture still closes its span. Before the gate every insert
/// took a page lock on `t_pkey` for its re-read.
#[tokio::test]
async fn a_plain_serializable_insert_takes_no_predicate_lock_on_the_source() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = captured_table(db.dsn()).await;

    let txn = raw
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .start()
        .await
        .expect("begin");
    txn.batch_execute("insert into public.t values (11, 11, 11)")
        .await
        .expect("insert one row");
    assert_eq!(
        source_predicate_locks(txn.client()).await,
        Vec::<String>::new()
    );
    txn.batch_execute(
        "insert into public.t select i, i, i from generate_series(12, 20) i; \
         update public.t set a = 0 where false; \
         insert into public.t values (21, 21, 21); \
         insert into public.t values (22, 22, 22)",
    )
    .await
    .expect("a batch, an empty update and two more inserts");
    assert_eq!(
        source_predicate_locks(txn.client()).await,
        Vec::<String>::new()
    );
    txn.commit().await.expect("commit");
    assert_ring_is_live(&raw, &["11", "15", "20", "21", "22"]).await;
}

/// The reproduction from D8a's review: two serializable transactions each
/// insert two keys on the same leaf page, interleaved. With no capture both
/// commit, and with capture they must too.
#[tokio::test]
async fn interleaved_serializable_inserts_on_one_leaf_page_both_commit() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = captured_table(db.dsn()).await;

    let mut one = connect(db.dsn()).await;
    let mut two = connect(db.dsn()).await;
    let t1 = one
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .start()
        .await
        .expect("begin one");
    let t2 = two
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .start()
        .await
        .expect("begin two");
    t1.batch_execute("insert into public.t values (101, 1, 1)")
        .await
        .expect("one inserts 101");
    t2.batch_execute("insert into public.t values (102, 2, 2)")
        .await
        .expect("two inserts 102");
    t1.batch_execute("insert into public.t values (103, 3, 3)")
        .await
        .expect("one inserts 103");
    t2.batch_execute("insert into public.t values (104, 4, 4)")
        .await
        .expect("two inserts 104");
    t1.commit().await.expect("one commits");
    if let Err(e) = t2.commit().await {
        assert_ne!(
            e.code(),
            Some(&SqlState::T_R_SERIALIZATION_FAILURE),
            "capture made a serializable insert fail: {e}"
        );
        panic!("two's commit failed: {e}");
    }
    assert_ring_is_live(&raw, &["101", "102", "103", "104"]).await;
}

// ------------------------------------------------- the re-read still runs

/// A function the statement calls from `RETURNING` rewrites the row the
/// statement just updated. Its statement runs at the outer statement's own
/// trigger depth, so `pg_trigger_depth()` can't tell it from a later
/// statement; the span state can. The outer capture runs last and must
/// image the rewritten row.
#[tokio::test]
async fn a_rewrite_by_a_function_the_statement_calls_is_imaged() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = captured_table(db.dsn()).await;
    raw.batch_execute(
        "create function public.rewrite(k bigint) returns int language sql as $$ \
             update public.t set a = 99 where id = k returning a \
         $$",
    )
    .await
    .expect("create rewrite()");

    raw.batch_execute(
        "begin isolation level serializable; \
         update public.t set a = 2 where id = 1 returning public.rewrite(id); \
         commit",
    )
    .await
    .expect("update key 1, rewritten by rewrite()");
    let a: i32 = raw
        .query_one("select a from public.t where id = 1", &[])
        .await
        .expect("read key 1")
        .get(0);
    assert_eq!(a, 99, "the function's rewrite is the live row");
    assert_ring_is_live(&raw, &["1"]).await;
}

/// A writable CTE inserts key 5 after the main statement deleted it: the
/// scan deletes key 5, then pulls the CTE (through the hashed subplan) for
/// key 6, which inserts key 5 again. The CTE's node finishes first, so its
/// insert is captured first and the delete's capture comes last. Its transition row says key 5 is gone, but
/// the live table has the CTE's row: the delete's capture must re-read.
#[tokio::test]
async fn a_sibling_event_of_the_same_statement_leaves_the_live_row_last() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = captured_table(db.dsn()).await;

    raw.batch_execute(
        "begin isolation level serializable; \
         with i as (insert into public.t values (5, 50, 50) returning id) \
         delete from public.t where id >= 5 and (id = 5 or id in (select id from i)); \
         commit",
    )
    .await
    .expect("delete key 5 and insert it again in one statement");
    let a: i32 = raw
        .query_one("select a from public.t where id = 5", &[])
        .await
        .expect("key 5 is live")
        .get(0);
    assert_eq!(a, 50);
    let ops: Vec<String> = ring(&raw)
        .await
        .into_iter()
        .filter(|(k, _, _)| k == "5")
        .map(|(_, op, _)| op)
        .collect();
    assert_eq!(
        ops,
        ["insert", "update"],
        "the CTE's insert is captured first, then the delete's re-read finds the live row"
    );
    assert_ring_is_live(&raw, &["5"]).await;

    // An upsert that re-keys the row it conflicts with and then inserts the
    // freed key.
    raw.batch_execute(
        "begin isolation level serializable; \
         insert into public.t values (7, 70, 70), (7, 71, 71) \
             on conflict (id) do update set id = 17; \
         commit",
    )
    .await
    .expect("re-key 7 to 17 and insert 7 again");
    assert_ring_is_live(&raw, &["7", "17"]).await;
}

/// With the begin trigger disabled, no statement sets a span start, so every
/// capture that writes rows re-reads: slower and lock-taking, but never
/// imaging a stale row.
#[tokio::test]
async fn without_the_begin_trigger_every_capture_rereads() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = captured_table(db.dsn()).await;
    raw.batch_execute(&format!(
        "alter table public.t disable trigger {}",
        trigger_name(DEFAULT_SCHEMA, CaptureEvent::Begin)
    ))
    .await
    .expect("disable the begin trigger");
    raw.batch_execute(
        "create function public.rewrite(k bigint) returns int language sql as $$ \
             update public.t set a = 99 where id = k returning a \
         $$",
    )
    .await
    .expect("create rewrite()");

    let txn = raw
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .start()
        .await
        .expect("begin");
    txn.batch_execute("insert into public.t values (11, 11, 11)")
        .await
        .expect("insert one row");
    assert_eq!(
        source_predicate_locks(txn.client()).await,
        ["page:t_pkey"],
        "the insert's capture re-read key 11"
    );
    txn.batch_execute("update public.t set a = 2 where id = 1 returning public.rewrite(id)")
        .await
        .expect("update key 1, rewritten by rewrite()");
    txn.commit().await.expect("commit");
    assert_ring_is_live(&raw, &["1", "11"]).await;
}

/// An FK action's update runs in the trigger query level of the statement
/// that fired it, fires no `BEFORE` statement trigger there, and shares the
/// transition tables of the table's update already queued at that level: one
/// capture call and no new span. A row updated twice is then in the
/// transition tables twice, and a merge join pairs an old version with the
/// intermediate one last. The old rows' repeated key makes capture re-read.
/// Both shapes: two cascading keys on one row, and a self-referencing key
/// that cascades into rows its own statement also updated.
#[tokio::test]
async fn an_fk_actions_update_merged_into_one_capture_is_reread() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.p (id int primary key); \
         insert into public.p values (1), (2); \
         create table public.c (id int primary key, \
             a int references public.p (id) on update cascade, \
             b int references public.p (id) on update cascade, v int); \
         insert into public.c select i, 1, 2, i from generate_series(1, 20) i; \
         create table public.s (id int primary key, \
             parent int references public.s (id) on update cascade, v int); \
         insert into public.s values (1, null, 0); \
         insert into public.s select i, 1, 0 from generate_series(2, 20) i",
    )
    .await
    .expect("create p, c and s");
    for (table, columns) in [
        ("public.c", ["a", "b", "v"]),
        ("public.s", ["parent", "v", "id"]),
    ] {
        let spec = CaptureSpec::new(
            table,
            vec!["id".to_string()],
            columns.map(str::to_string),
            Vec::new(),
        )
        .expect("valid spec");
        match install::install(&mut raw, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("install")
        {
            Progress::Done(_) => {}
            Progress::Waiting(wait) => panic!("an install without a deadline landed: {wait}"),
        }
    }

    // The plan the application's settings choose orders the join's output;
    // this one emits a stale pairing last without the re-read.
    raw.batch_execute(
        "set enable_hashjoin = off; set enable_nestloop = off; \
         begin isolation level serializable; \
         update public.p set id = id + 10; \
         update public.s set id = case id when 1 then 100 else id end, v = v + 1; \
         commit; \
         reset enable_hashjoin; reset enable_nestloop",
    )
    .await
    .expect("cascade into c and s");

    for (table, image) in [
        (
            "public.c",
            "jsonb_build_object('a', a::text, 'b', b::text, 'id', id::text, 'v', v::text)",
        ),
        (
            "public.s",
            "jsonb_build_object('id', id::text, 'parent', parent::text, 'v', v::text)",
        ),
    ] {
        let live: BTreeMap<String, String> = raw
            .query(&format!("select id::text, {image}::text from {table}"), &[])
            .await
            .expect("read the table")
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(table_ring_state(&raw, table).await, live, "{table}");
    }
}

/// A nested statement that changes nothing, after a nested rewrite in the
/// same span, opens and closes a span of its own while the outer statement's
/// is still open. It must not move the outer span's start past the rewrite.
#[tokio::test]
async fn an_empty_nested_statement_after_a_rewrite_keeps_the_outer_span() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = captured_table(db.dsn()).await;
    raw.batch_execute(
        "create function public.rewrite_then_nothing() returns trigger language plpgsql as $$ \
         begin \
             update public.t set a = 99 where id = new.id; \
             update public.t set a = 0 where false; \
             return null; \
         end $$; \
         create trigger rewrite_then_nothing after update on public.t for each row \
             when (pg_trigger_depth() < 1) execute function public.rewrite_then_nothing()",
    )
    .await
    .expect("create the application trigger");

    raw.batch_execute("update public.t set a = 2 where id = 1")
        .await
        .expect("update key 1, rewritten by the trigger");
    assert_ring_is_live(&raw, &["1"]).await;
}

/// On the transition path a key moved between equal `numeric` values (`1.0`
/// to `1.00`) still stages the old key's delete: the update pairs old and
/// new rows by key text.
#[tokio::test]
async fn an_equal_valued_key_move_without_a_reread_deletes_the_old_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.n (id numeric primary key, a int); \
         insert into public.n values (1.0, 1)",
    )
    .await
    .expect("create public.n");
    let spec = CaptureSpec::new(
        "public.n",
        vec!["id".to_string()],
        ["a".to_string()],
        Vec::new(),
    )
    .expect("valid spec");
    match install::install(&mut raw, DEFAULT_SCHEMA, &spec, None)
        .await
        .expect("install")
    {
        Progress::Done(_) => {}
        Progress::Waiting(wait) => panic!("an install without a deadline landed: {wait}"),
    }

    raw.batch_execute("update public.n set id = 1.00")
        .await
        .expect("move the key");
    let mut staged = table_ring(&raw, "public.n").await;
    staged.sort();
    assert_eq!(
        staged,
        [
            ("1.0".to_string(), "delete".to_string(), None),
            (
                "1.00".to_string(),
                "insert".to_string(),
                Some(r#"{"a": "1", "id": "1.00"}"#.to_string())
            ),
        ]
    );
}
