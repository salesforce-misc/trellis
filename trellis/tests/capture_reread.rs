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
//!   included), stages nothing. A move of a relationship join column alone
//!   still stages, on both sides of the relationship.
//!
//! Capture is installed by hand and the ring is read directly; nothing waits
//! for convergence (#297).

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, IsolationLevel, NoTls};
use trellis::capture::columns::{capture_spec, load_catalog};
use trellis::capture::install::{self, Progress};
use trellis::capture::sql::CaptureSpec;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{create_relationship, install_definition};

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

/// A relationship's join columns are imaged even when no definition reads
/// them as a field, so a move of one alone still stages: the from-side's
/// `from_col` (with the old and new parent in `group_key`) and the to-side's
/// `to_col` (a unique column, not its key). A to-side column nothing reads
/// stages nothing.
#[tokio::test]
async fn a_move_of_a_join_column_alone_still_stages() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.parent (id int primary key, code text unique, name text, \
             unread text); \
         insert into public.parent values (1, 'p1', 'one', 'u'), (2, 'p2', 'two', 'u'); \
         create table public.child (id int primary key, pcode text, amount int, note text); \
         insert into public.child values (10, 'p1', 5, 'n')",
    )
    .await
    .expect("create parent and child");
    create_relationship(
        &db.pool,
        "RELATIONSHIP child_parent FROM child.pcode TO parent.code",
    )
    .await
    .expect("relationship");
    let columns: HashMap<String, ValueType> = [
        ("id".to_string(), ValueType::Numeric),
        ("pcode".to_string(), ValueType::Text),
    ]
    .into();
    install_definition(
        &db.pool,
        "TRANSFORM named FROM public.child SELECT child_parent.name AS parent_name",
        &columns,
        "public",
    )
    .await
    .expect("install the definition");
    trellis::intake::markers::settle_registrations(&db.pool).await;

    let client = db.pool.get().await.expect("pool client");
    let catalog = load_catalog(&**client, DEFAULT_SCHEMA)
        .await
        .expect("load the capture catalog");
    let child = capture_spec(&**client, &catalog, "public.child")
        .await
        .expect("child spec");
    let parent = capture_spec(&**client, &catalog, "public.parent")
        .await
        .expect("parent spec");
    drop(client);
    assert_eq!(child.columns(), ["id", "pcode"]);
    assert_eq!(child.group_key(), ["pcode"]);
    assert_eq!(parent.columns(), ["code", "id", "name"]);
    install_spec(&mut raw, &child).await;
    install_spec(&mut raw, &parent).await;
    // The registration stages its own `recompute` rows; only what capture
    // stages for the writes below counts.
    let captured = |rows: Vec<Ring>| -> Vec<Ring> {
        rows.into_iter()
            .filter(|r| ["insert", "update", "delete"].contains(&r.2.as_str()))
            .collect()
    };

    raw.batch_execute(
        "update public.child set note = 'm', amount = 6; \
         update public.parent set unread = 'w'",
    )
    .await
    .expect("updates of unimaged columns");
    assert!(captured(ring(&raw, "public.child").await).is_empty());
    assert!(captured(ring(&raw, "public.parent").await).is_empty());

    raw.batch_execute(
        "update public.child set pcode = 'p2'; \
         update public.parent set code = 'p1b' where id = 1",
    )
    .await
    .expect("join-column moves");
    let child_rows: Vec<(String, Option<Vec<String>>)> = captured(ring(&raw, "public.child").await)
        .into_iter()
        .map(|r| (r.2, r.5))
        .collect();
    assert_eq!(
        child_rows,
        vec![(
            "update".to_string(),
            Some(vec!["p1".to_string(), "p2".to_string()])
        )]
    );
    let parent_rows: Vec<(String, String, Option<String>)> =
        captured(ring(&raw, "public.parent").await)
            .into_iter()
            .map(|r| (r.1, r.2, r.4))
            .collect();
    assert_eq!(
        parent_rows,
        vec![(
            "1".to_string(),
            "update".to_string(),
            Some(r#"{"id": "1", "code": "p1b", "name": "one"}"#.to_string())
        )]
    );
}

// ------------------------------------------------------------ plan shape

/// The live re-read stays a primary-key probe per captured row after the
/// table grows, although PL/pgSQL planned it once, on the session's first
/// write, against a table a `VACUUM` had just seen empty. Costed against
/// that, a sequential scan beats the index, and a cached sequential scan
/// would read the whole table for every captured row (a 1,000-row insert
/// ran at 3% of control before the probe was forced). Inserts don't scan
/// the table themselves, so any sequential scan of it is the re-read's.
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

    raw.batch_execute("select pg_stat_clear_snapshot()")
        .await
        .expect("clear the stats snapshot");
    let row = raw
        .query_one(
            "select seq_scan, idx_scan from pg_stat_user_tables where relid = 'public.g'::regclass",
            &[],
        )
        .await
        .expect("table stats");
    let (seq, idx): (i64, i64) = (row.get(0), row.get::<_, Option<i64>>(1).unwrap_or(0));
    assert_eq!(seq, 0, "the re-read scanned public.g");
    assert!(idx >= 4001, "one probe per captured row, got {idx}");
    assert_eq!(ring(&raw, "public.g").await.len(), 4001);
}
