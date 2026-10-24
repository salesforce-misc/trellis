//! #623 D8a: capture images the live row, re-read by primary key, and an
//! update that changed no imaged column stages nothing.
//!
//! - **No leak (the user's gate on D's Q8).** The re-read joins only rows the
//!   capturing statement wrote, whose row locks (or unique-index entries) its
//!   transaction holds, so no other transaction's commit can reach the image.
//!   Checked with a writer held between its statement and its capture while
//!   another session commits, at `READ COMMITTED`, `REPEATABLE READ` and
//!   `SERIALIZABLE`; for a primary-key move and a nested trigger that moves
//!   the key; for `INSERT … ON CONFLICT DO UPDATE` over another session's
//!   commit; and for an FK cascade driven by another table's statement.
//! - **Nested writes.** A nested write that deletes, re-keys or re-inserts the
//!   row its outer statement wrote leaves the outer statement's ring row (the
//!   later one) carrying the live state.
//! - **Skip-no-op.** An update of only unimaged columns, or one that sets
//!   imaged columns to the values they had (types with no equality operator
//!   included), stages nothing.
//!
//! The re-read runs only when another write to the table ran in the
//! statement's span (`capture_ssi.rs` checks that gate). The tests that check
//! the re-read's own SQL (no leak, its `age` and key-text filters, its plan)
//! disable the begin trigger with [`always_reread`], the gate's fallback, so
//! every capture re-reads whether or not a nested write happened. The
//! nested-write tests run with the gate on.
//!
//! Capture is installed by hand and the ring is read directly; nothing waits
//! for convergence (#297).

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, IsolationLevel, NoTls};
use trellis::capture::install::{self, Progress};
use trellis::capture::sql::{CaptureEvent, CaptureSpec, trigger_name};
use trellis::config::DEFAULT_SCHEMA;

/// The advisory lock the application trigger in the no-leak tests waits on.
const HOLD_LOCK: i64 = 623_008;

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

async fn install_spec(client: &mut Client, spec: &CaptureSpec) {
    match install::install(client, DEFAULT_SCHEMA, spec, None)
        .await
        .expect("install")
    {
        Progress::Done(_) => {}
        Progress::Waiting(wait) => panic!("an install without a deadline landed: {wait}"),
    }
}

/// Makes every capture of `table` re-read the live row: with its begin
/// trigger disabled no statement marks a span start, and the gate falls back
/// to the re-read (#623 D8a).
async fn always_reread(client: &Client, table: &str) {
    client
        .batch_execute(&format!(
            "alter table {table} disable trigger {}",
            trigger_name(DEFAULT_SCHEMA, CaptureEvent::Begin)
        ))
        .await
        .expect("disable the begin trigger");
}

fn spec(table: &str, columns: &[&str]) -> CaptureSpec {
    CaptureSpec::new(
        table,
        vec!["id".to_string()],
        columns.iter().map(|c| c.to_string()),
        Vec::new(),
    )
    .expect("valid spec")
}

/// One ring row: `(row_txid, key, op, old_image, new_image, group_key)`.
type Ring = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<Vec<String>>,
);

/// Every ring row for `table`, in `(lsn, change_id)` order.
async fn ring(client: &Client, table: &str) -> Vec<Ring> {
    let arms: Vec<String> = (0..4)
        .map(|n| {
            format!(
                "select src_table, row_txid, key, op, old_image, new_image, group_key, lsn, \
                 change_id from seg_{n}"
            )
        })
        .collect();
    client
        .query(
            &format!(
                "select row_txid::text, key, op, old_image::text, new_image::text, group_key \
                 from ({}) r where src_table = $1 order by lsn, change_id",
                arms.join(" union all ")
            ),
            &[&table],
        )
        .await
        .expect("read the ring")
        .into_iter()
        .map(|row| {
            (
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
                row.get(5),
            )
        })
        .collect()
}

/// The ring rows `txid` staged for `table`, without the txid.
async fn staged_by(
    client: &Client,
    table: &str,
    txid: &str,
) -> Vec<(String, String, Option<String>, Option<String>)> {
    ring(client, table)
        .await
        .into_iter()
        .filter(|r| r.0 == txid)
        .map(|r| (r.1, r.2, r.3, r.4))
        .collect()
}

/// `(key, op, old_image, new_image)` with owned strings, for comparisons.
fn row(
    key: &str,
    op: &str,
    old: Option<&str>,
    new: Option<&str>,
) -> (String, String, Option<String>, Option<String>) {
    (
        key.to_string(),
        op.to_string(),
        old.map(str::to_string),
        new.map(str::to_string),
    )
}

async fn txid(client: &Client) -> String {
    client
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("txid")
        .get(0)
}

/// `public.t (id int primary key, a int, b int)` holding `(1,1,1)`,
/// `(2,2,2)`, captured imaging `a`, with an application `AFTER` row trigger
/// on update and insert that waits on [`HOLD_LOCK`] whenever `b` is
/// negative, which holds a writer between its statement and its capture.
async fn held_table(dsn: &str) -> Client {
    let mut raw = connect(dsn).await;
    raw.batch_execute(&format!(
        "create table public.t (id int primary key, a int, b int); \
         insert into public.t values (1, 1, 1), (2, 2, 2); \
         create function public.hold() returns trigger language plpgsql as $$ \
         begin \
             if new.b < 0 then perform pg_advisory_xact_lock({HOLD_LOCK}); end if; \
             return null; \
         end $$; \
         create trigger hold after insert or update on public.t \
             for each row execute function public.hold()"
    ))
    .await
    .expect("create public.t");
    install_spec(&mut raw, &spec("public.t", &["a"])).await;
    raw
}

/// Starts `sql` in its own transaction at `level` on a new connection,
/// returning that transaction's xid and a handle that commits once the
/// statement finishes.
async fn spawn_held_writer(
    dsn: &str,
    level: IsolationLevel,
    sql: &'static str,
) -> (String, tokio::task::JoinHandle<()>) {
    let mut client = connect(dsn).await;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let txn = client
            .build_transaction()
            .isolation_level(level)
            .start()
            .await
            .expect("begin");
        // Take the snapshot (and the xid) before the other session commits.
        txn.query_one("select count(*) from public.t", &[])
            .await
            .expect("snapshot");
        tx.send(txid(txn.client()).await).expect("send txid");
        txn.batch_execute(sql).await.expect("the held write");
        txn.commit().await.expect("commit");
    });
    (rx.await.expect("txid"), handle)
}

/// Waits until a backend is blocked on [`HOLD_LOCK`].
async fn wait_for_holder(raw: &Client) {
    for _ in 0..500 {
        let waiting: bool = raw
            .query_one(
                "select exists (select 1 from pg_locks \
                 where locktype = 'advisory' and objid = $1::bigint::oid and not granted)",
                &[&HOLD_LOCK],
            )
            .await
            .expect("pg_locks")
            .get(0);
        if waiting {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the writer never reached its application trigger");
}

/// The no-leak check at one isolation level: while the writer is held
/// between its update of key 1 and its capture, another session commits a
/// change to key 2 and a new key 3, and can't touch key 1 at all. The
/// writer's capture stages key 1 alone, with its own value.
async fn no_leak_at(level: IsolationLevel) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = held_table(db.dsn()).await;
    always_reread(&raw, "public.t").await;

    let mut holder = connect(db.dsn()).await;
    let hold = holder.transaction().await.expect("begin holder");
    hold.query_one("select pg_advisory_xact_lock($1)", &[&HOLD_LOCK])
        .await
        .expect("take the hold");

    let (writer_txid, writer) = spawn_held_writer(
        db.dsn(),
        level,
        "update public.t set a = 10, b = -1 where id = 1",
    )
    .await;
    wait_for_holder(&raw).await;

    let other = connect(db.dsn()).await;
    other
        .batch_execute(
            "update public.t set a = 99 where id = 2; \
             insert into public.t values (3, 33, 3)",
        )
        .await
        .expect("another session commits other keys");
    // Key 1 is the writer's: another session can't commit a newer version.
    let blocked = other
        .batch_execute(
            "begin; set local lock_timeout = '200ms'; \
             update public.t set a = 77 where id = 1",
        )
        .await;
    assert!(blocked.is_err(), "key 1's row lock is the writer's");
    other.batch_execute("rollback").await.expect("rollback");

    hold.commit().await.expect("release the hold");
    writer.await.expect("the writer commits");

    assert_eq!(
        staged_by(&raw, "public.t", &writer_txid).await,
        vec![row(
            "1",
            "update",
            Some(r#"{"a": "1", "id": "1"}"#),
            Some(r#"{"a": "10", "id": "1"}"#)
        )],
        "the writer's capture stages its own key and value only"
    );
}

#[tokio::test]
async fn the_reread_sees_no_other_commit_at_read_committed() {
    no_leak_at(IsolationLevel::ReadCommitted).await;
}

#[tokio::test]
async fn the_reread_sees_no_other_commit_at_repeatable_read() {
    no_leak_at(IsolationLevel::RepeatableRead).await;
}

#[tokio::test]
async fn the_reread_sees_no_other_commit_at_serializable() {
    no_leak_at(IsolationLevel::Serializable).await;
}

/// A new key the held writer inserted is its own too: another session's
/// insert of the same key waits on the unique index rather than committing
/// a row the re-read could find.
#[tokio::test]
async fn a_held_insert_owns_its_new_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = held_table(db.dsn()).await;
    always_reread(&raw, "public.t").await;

    let mut holder = connect(db.dsn()).await;
    let hold = holder.transaction().await.expect("begin holder");
    hold.query_one("select pg_advisory_xact_lock($1)", &[&HOLD_LOCK])
        .await
        .expect("take the hold");
    let (writer_txid, writer) = spawn_held_writer(
        db.dsn(),
        IsolationLevel::ReadCommitted,
        "insert into public.t values (5, 50, -1)",
    )
    .await;
    wait_for_holder(&raw).await;

    let other = connect(db.dsn()).await;
    let blocked = other
        .batch_execute(
            "begin; set local lock_timeout = '200ms'; \
             insert into public.t values (5, 55, 5) on conflict (id) do update set a = 56",
        )
        .await;
    assert!(
        blocked.is_err(),
        "key 5's unique-index entry is the writer's"
    );
    other.batch_execute("rollback").await.expect("rollback");

    hold.commit().await.expect("release the hold");
    writer.await.expect("the writer commits");
    assert_eq!(
        staged_by(&raw, "public.t", &writer_txid).await,
        vec![row("5", "insert", None, Some(r#"{"a": "50", "id": "5"}"#))]
    );
}

/// `INSERT … ON CONFLICT DO UPDATE` over a row another session committed
/// while the writer waited: the update half images the writer's own result.
#[tokio::test]
async fn an_upsert_over_another_sessions_commit_images_its_own_result() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = held_table(db.dsn()).await;
    always_reread(&raw, "public.t").await;

    let mut other = connect(db.dsn()).await;
    let pending = other.transaction().await.expect("begin other");
    pending
        .batch_execute("insert into public.t values (8, 1, 8)")
        .await
        .expect("an uncommitted insert of key 8");

    let mut writer = connect(db.dsn()).await;
    let writer_task = tokio::spawn(async move {
        let txn = writer.transaction().await.expect("begin writer");
        let id = txid(txn.client()).await;
        txn.batch_execute(
            "insert into public.t values (8, 2, 8), (9, 9, 9) \
             on conflict (id) do update set a = public.t.a + excluded.a",
        )
        .await
        .expect("the upsert");
        txn.commit().await.expect("commit");
        id
    });
    // The upsert waits on key 8's unique-index entry until `other` commits.
    tokio::time::sleep(Duration::from_millis(200)).await;
    pending.commit().await.expect("other commits key 8");
    let writer_txid = writer_task.await.expect("the writer commits");

    assert_eq!(
        staged_by(&raw, "public.t", &writer_txid).await,
        vec![
            row(
                "8",
                "update",
                Some(r#"{"a": "1", "id": "8"}"#),
                Some(r#"{"a": "3", "id": "8"}"#)
            ),
            row("9", "insert", None, Some(r#"{"a": "9", "id": "9"}"#)),
        ],
        "the update trigger fires before the insert trigger"
    );
}

/// Under `REPEATABLE READ` and `SERIALIZABLE` the snapshot predates the
/// writer's statement, so a version of a key that another session deleted
/// or re-keyed after the snapshot stays visible to the writer beside the row
/// its own insert of that key writes. The re-read images the writer's row,
/// never the other session's deleted one, and when a nested write deletes
/// the writer's row, it stages a delete rather than imaging the stale
/// version (#623 D8a review).
#[tokio::test]
async fn a_version_another_session_removed_after_the_snapshot_is_never_imaged() {
    for level in [IsolationLevel::RepeatableRead, IsolationLevel::Serializable] {
        for removal in [
            "delete from public.t where id = 1",
            "update public.t set id = 3 where id = 1",
        ] {
            let cluster = TestCluster::start();
            let db = cluster.create_isolated_database().await;
            let raw = held_table(db.dsn()).await;
            always_reread(&raw, "public.t").await;
            raw.batch_execute(
                "create function public.undo_insert() returns trigger language plpgsql as $$ \
                 begin \
                     if new.a = 0 then delete from public.t where id = new.id; end if; \
                     return null; \
                 end $$; \
                 create trigger undo_insert after insert on public.t \
                     for each row execute function public.undo_insert()",
            )
            .await
            .expect("create the undo trigger");

            let mut writer = connect(db.dsn()).await;
            let txn = writer
                .build_transaction()
                .isolation_level(level)
                .start()
                .await
                .expect("begin");
            txn.query_one("select count(*) from public.t", &[])
                .await
                .expect("take the snapshot");
            raw.batch_execute(removal)
                .await
                .expect("another session removes key 1");
            txn.batch_execute(
                "insert into public.t values (1, 50, 1); \
                 delete from public.t where id = 2; \
                 insert into public.t values (2, 0, 2)",
            )
            .await
            .expect("re-create key 1, and key 2 deleted by the trigger");
            let id = txid(txn.client()).await;
            txn.commit().await.expect("commit");

            assert_eq!(
                staged_by(&raw, "public.t", &id).await,
                vec![
                    row("1", "insert", None, Some(r#"{"a": "50", "id": "1"}"#)),
                    row("2", "delete", Some(r#"{"a": "2", "id": "2"}"#), None),
                    row("2", "delete", Some(r#"{"a": "0", "id": "2"}"#), None),
                    row("2", "delete", Some(r#"{"a": "0", "id": "2"}"#), None),
                ],
                "{level:?}, {removal}"
            );
        }
    }
}

/// A key move between two values its type's `=` calls equal but that render
/// differently (`numeric` `1.0` to `1.00`) is a delete of the old key text
/// and an insert of the new one: the ring keys by the text, so the re-read
/// of the old key must not find the moved row (#623 D8a review).
#[tokio::test]
async fn a_key_move_its_types_equality_calls_a_no_op_deletes_the_old_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.n (id numeric primary key, a int); \
         insert into public.n values (1.0, 1)",
    )
    .await
    .expect("create public.n");
    install_spec(&mut raw, &spec("public.n", &["a"])).await;
    always_reread(&raw, "public.n").await;

    raw.batch_execute("begin; update public.n set id = 1.00")
        .await
        .expect("re-scale key 1.0");
    let id = txid(&raw).await;
    raw.batch_execute("commit").await.expect("commit");
    assert_eq!(
        staged_by(&raw, "public.n", &id).await,
        vec![
            row("1.0", "delete", Some(r#"{"a": "1", "id": "1.0"}"#), None),
            row("1.00", "insert", None, Some(r#"{"a": "1", "id": "1.00"}"#)),
        ]
    );
}

/// A nested write in a subtransaction (an `EXCEPTION` block) is this
/// transaction's too, so the outer capture images it, also when `age()`
/// first ran in the transaction before it had an xid.
#[tokio::test]
async fn a_nested_write_in_a_subtransaction_is_the_live_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = held_table(db.dsn()).await;
    raw.batch_execute(
        "create function public.bump() returns trigger language plpgsql as $$ \
         begin \
             if new.a = 200 then \
                 begin \
                     update public.t set a = 201 where id = new.id; \
                 exception when others then null; \
                 end; \
             end if; \
             return null; \
         end $$; \
         create trigger bump after update on public.t \
             for each row execute function public.bump()",
    )
    .await
    .expect("create the bump trigger");

    for level in ["read committed", "repeatable read"] {
        raw.batch_execute(&format!(
            "begin isolation level {level}; select age('3'::xid); \
             update public.t set a = 200 where id = 1"
        ))
        .await
        .expect("update key 1, bumped by the trigger");
        let id = txid(&raw).await;
        raw.batch_execute(
            "commit; update public.t set a = 1 where id = 1; \
             update public.t set a = 1 where id = 1",
        )
        .await
        .expect("commit and reset");
        let staged = staged_by(&raw, "public.t", &id).await;
        assert_eq!(
            staged.last(),
            Some(&row(
                "1",
                "update",
                Some(r#"{"a": "1", "id": "1"}"#),
                Some(r#"{"a": "201", "id": "1"}"#)
            )),
            "{level}: {staged:?}"
        );
    }
}

/// A primary-key move is a delete of the old key and an insert of the new
/// one, the insert imaging the live row.
#[tokio::test]
async fn a_key_move_stages_a_delete_and_an_insert() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = held_table(db.dsn()).await;

    raw.batch_execute("begin; update public.t set id = 10, a = 11 where id = 1")
        .await
        .expect("move key 1");
    let id = txid(&raw).await;
    raw.batch_execute("commit").await.expect("commit");
    assert_eq!(
        staged_by(&raw, "public.t", &id).await,
        vec![
            row("1", "delete", Some(r#"{"a": "1", "id": "1"}"#), None),
            row("10", "insert", None, Some(r#"{"a": "11", "id": "10"}"#)),
        ]
    );
}

/// A nested trigger that moves the key its outer statement updated: the
/// nested capture stages the move first, and the outer statement's row (the
/// later one) finds key 2 gone and stages its delete, never the transition
/// table's version of key 2.
#[tokio::test]
async fn a_nested_key_move_leaves_the_outer_capture_a_delete() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = held_table(db.dsn()).await;
    raw.batch_execute(
        "create function public.rekey() returns trigger language plpgsql as $$ \
         begin \
             if new.a = 100 and new.id < 100 then update public.t set id = new.id + 100 where id = new.id; end if; \
             return null; \
         end $$; \
         create trigger rekey after update on public.t \
             for each row execute function public.rekey()",
    )
    .await
    .expect("create the rekey trigger");

    raw.batch_execute("begin; update public.t set a = 100 where id = 2")
        .await
        .expect("update key 2, re-keyed to 102 by the trigger");
    let id = txid(&raw).await;
    raw.batch_execute("commit").await.expect("commit");
    assert_eq!(
        staged_by(&raw, "public.t", &id).await,
        vec![
            // The nested statement's capture.
            row("2", "delete", Some(r#"{"a": "100", "id": "2"}"#), None),
            row("102", "insert", None, Some(r#"{"a": "100", "id": "102"}"#)),
            // The outer statement's: key 2 is gone from the live table.
            row("2", "delete", Some(r#"{"a": "2", "id": "2"}"#), None),
        ]
    );
}

/// A nested trigger that deletes the row its outer insert wrote: the outer
/// capture stages a delete. One that re-inserts the key its outer delete
/// removed: the outer capture stages the live row.
#[tokio::test]
async fn a_nested_delete_or_reinsert_leaves_the_outer_capture_the_live_state() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = held_table(db.dsn()).await;
    raw.batch_execute(
        "create function public.undo_insert() returns trigger language plpgsql as $$ \
         begin \
             if new.a = 0 then delete from public.t where id = new.id; end if; \
             return null; \
         end $$; \
         create trigger undo_insert after insert on public.t \
             for each row execute function public.undo_insert(); \
         create function public.undo_delete() returns trigger language plpgsql as $$ \
         begin \
             if old.id = 1 then insert into public.t values (old.id, -5, 0); end if; \
             return null; \
         end $$; \
         create trigger undo_delete after delete on public.t \
             for each row execute function public.undo_delete()",
    )
    .await
    .expect("create the undo triggers");

    raw.batch_execute("begin; insert into public.t values (4, 0, 4)")
        .await
        .expect("insert key 4, deleted by the trigger");
    let id = txid(&raw).await;
    raw.batch_execute("commit").await.expect("commit");
    assert_eq!(
        staged_by(&raw, "public.t", &id).await,
        vec![
            row("4", "delete", Some(r#"{"a": "0", "id": "4"}"#), None),
            row("4", "delete", Some(r#"{"a": "0", "id": "4"}"#), None),
        ],
        "the nested delete, then the outer insert finding key 4 gone"
    );

    raw.batch_execute("begin; delete from public.t where id = 1")
        .await
        .expect("delete key 1, re-inserted by the trigger");
    let id = txid(&raw).await;
    raw.batch_execute("commit").await.expect("commit");
    assert_eq!(
        staged_by(&raw, "public.t", &id).await,
        vec![
            row("1", "insert", None, Some(r#"{"a": "-5", "id": "1"}"#)),
            row(
                "1",
                "update",
                Some(r#"{"a": "1", "id": "1"}"#),
                Some(r#"{"a": "-5", "id": "1"}"#)
            ),
        ],
        "the nested insert, then the outer delete finding key 1 back"
    );
}

/// An FK cascade driven by a statement on another table: the child's
/// capture stages the cascade's deletes and key updates, imaging the child's
/// live rows.
#[tokio::test]
async fn an_fk_cascade_from_another_tables_statement_images_the_child() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.p (id int primary key); \
         insert into public.p values (1), (2); \
         create table public.c (id int primary key, \
             pid int references public.p (id) on delete cascade on update cascade, v int); \
         insert into public.c values (10, 1, 100), (11, 1, 110), (20, 2, 200)",
    )
    .await
    .expect("create p and c");
    install_spec(&mut raw, &spec("public.c", &["pid", "v"])).await;
    always_reread(&raw, "public.c").await;

    raw.batch_execute(
        "begin; delete from public.p where id = 1; update public.p set id = 3 where id = 2",
    )
    .await
    .expect("cascade into c");
    let id = txid(&raw).await;
    raw.batch_execute("commit").await.expect("commit");
    assert_eq!(
        staged_by(&raw, "public.c", &id).await,
        vec![
            row(
                "10",
                "delete",
                Some(r#"{"v": "100", "id": "10", "pid": "1"}"#),
                None
            ),
            row(
                "11",
                "delete",
                Some(r#"{"v": "110", "id": "11", "pid": "1"}"#),
                None
            ),
            row(
                "20",
                "update",
                Some(r#"{"v": "200", "id": "20", "pid": "2"}"#),
                Some(r#"{"v": "200", "id": "20", "pid": "3"}"#)
            ),
        ]
    );
}

// ------------------------------------------------------------ skip-no-op

/// An update of only columns capture doesn't image stages nothing, and in a
/// statement that changes an imaged column on one row and not another, only
/// the changed row is staged.
#[tokio::test]
async fn an_update_of_only_unimaged_columns_stages_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = held_table(db.dsn()).await;
    let before = ring(&raw, "public.t").await.len();

    raw.batch_execute("update public.t set b = b + 1")
        .await
        .expect("update b");
    assert_eq!(ring(&raw, "public.t").await.len(), before);

    raw.batch_execute(
        "begin; update public.t set a = case when id = 1 then 5 else a end, b = b + 1",
    )
    .await
    .expect("update a on key 1 only");
    let id = txid(&raw).await;
    raw.batch_execute("commit").await.expect("commit");
    assert_eq!(
        staged_by(&raw, "public.t", &id).await,
        vec![row(
            "1",
            "update",
            Some(r#"{"a": "1", "id": "1"}"#),
            Some(r#"{"a": "5", "id": "1"}"#)
        )]
    );
}

/// Setting imaged columns to the values they hold stages nothing, for types
/// with no equality operator too (`json`, `point`, `xml`). A value that is
/// equal but renders differently (`numeric` `1.5` to `1.50`, `json` with new
/// whitespace) does stage: the comparison is never looser than the images.
#[tokio::test]
async fn an_update_to_the_same_values_stages_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.v (id int primary key, n numeric, j json, p point, x xml, \
             arr int[], s text); \
         insert into public.v values (1, 1.5, '{\"k\":1}', '(1,2)', '<a/>', '{1,2}', 's')",
    )
    .await
    .expect("create public.v");
    install_spec(
        &mut raw,
        &spec("public.v", &["n", "j", "p", "x", "arr", "s"]),
    )
    .await;

    raw.batch_execute(
        "update public.v set n = n, j = j, p = p, x = x, arr = arr, s = s; \
         update public.v set n = 1.5, s = 's', arr = '{1,2}'",
    )
    .await
    .expect("same-value updates");
    assert_eq!(ring(&raw, "public.v").await, Vec::<Ring>::new());

    raw.batch_execute("update public.v set n = 1.50")
        .await
        .expect("an equal numeric with another scale");
    raw.batch_execute("update public.v set j = '{\"k\": 1}'")
        .await
        .expect("json with new whitespace");
    let staged: Vec<(String, Option<String>)> = ring(&raw, "public.v")
        .await
        .into_iter()
        .map(|r| (r.2, r.4))
        .collect();
    assert_eq!(staged.len(), 2, "{staged:?}");
    assert!(staged[0].1.as_deref().unwrap().contains(r#""n": "1.50""#));
    assert!(
        staged[1]
            .1
            .as_deref()
            .unwrap()
            .contains(r#""j": "{\"k\": 1}""#)
    );
}

// ------------------------------------------------------------ plan shape

/// `(seq_scan, idx_scan)` of `table` once `client`'s own pending counts have
/// reached the shared counters (they otherwise wait for the session to sit
/// idle for a second).
async fn scan_counts(client: &Client, table: &str) -> (i64, i64) {
    client
        .batch_execute("select pg_stat_force_next_flush()")
        .await
        .expect("force the next stats flush");
    client.batch_execute("select 1").await.expect("flush stats");
    client
        .batch_execute("select pg_stat_clear_snapshot()")
        .await
        .expect("clear the stats snapshot");
    let row = client
        .query_one(
            "select seq_scan, idx_scan from pg_stat_user_tables where relid = $1::text::regclass",
            &[&table],
        )
        .await
        .expect("table stats");
    (row.get(0), row.get::<_, Option<i64>>(1).unwrap_or(0))
}

/// The live re-read stays a primary-key probe per captured row after the
/// table grows, although PL/pgSQL planned it once, on the session's first
/// write, against a table a `VACUUM` had just seen empty. Costed against
/// that, a sequential scan beats the index, and a cached sequential scan
/// would read the whole table for every captured row (a 1,000-row insert
/// ran at 3% of control before the probe was forced). Inserts don't scan
/// the table themselves, so any sequential scan of it after the setup is the
/// re-read's.
///
/// The counters are cumulative and flushed per backend, lazily: setup (the
/// primary key's index build, say) leaves a scan pending in the `raw`
/// session that reaches the shared counters only once that session has been
/// idle for a second, which is by timing, so under load it lands between any
/// two reads (#1233). The test therefore flushes every session, reads the
/// counters as a baseline before the writes, and asserts on the delta.
#[tokio::test]
async fn the_reread_never_scans_the_table_after_planning_against_it_empty() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute("create table public.g (id bigint primary key, a int)")
        .await
        .expect("create public.g");
    raw.batch_execute("vacuum public.g")
        .await
        .expect("vacuum public.g");
    install_spec(&mut raw, &spec("public.g", &["a"])).await;
    always_reread(&raw, "public.g").await;

    let (seq_before, idx_before) = scan_counts(&raw, "public.g").await;

    let writer = connect(db.dsn()).await;
    writer
        .batch_execute(
            "insert into public.g values (0, 0); \
             insert into public.g select i, i from generate_series(1, 2000) i; \
             insert into public.g select i, i from generate_series(2001, 4000) i; \
             select pg_stat_force_next_flush()",
        )
        .await
        .expect("writes planned against an empty table");
    writer.batch_execute("select 1").await.expect("flush stats");

    let (seq_after, idx_after) = scan_counts(&raw, "public.g").await;
    let (seq, idx) = (seq_after - seq_before, idx_after - idx_before);
    assert_eq!(seq, 0, "the re-read scanned public.g");
    assert!(idx >= 4001, "one probe per captured row, got {idx}");
    assert_eq!(ring(&raw, "public.g").await.len(), 4001);
}
