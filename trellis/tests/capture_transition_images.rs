//! Every transition row counts once.
//!
//! A reader that sums each ring row's `f(new) − f(old)` (the group basis)
//! needs a transaction's ring rows to telescope: summed as signed multisets
//! of images, `+new` and `−old` per row must come to the rows the
//! transaction ended with minus the rows it started with. Each test here runs
//! one nested-write shape and checks exactly that, where a row's sides are
//! its `transition_images` when set (JSON `null` meaning none) and its
//! `old_image`/`new_image` otherwise (`capture::sql`, "Every transition row
//! counts once"). The six `H1.*` shapes each have a test named
//! after them; the rest cover the other ways a statement's rows reach one
//! capture call twice.
//!
//! Each test also checks what a 1-1 reader sees: every key's last ring row
//! carries the row the transaction ended with (the live re-read).
//! `capture_reread.rs` pins the re-read itself. Capture is installed by hand
//! and the ring is read directly; nothing waits for convergence.

use std::collections::BTreeMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::install::{self, Progress};
use trellis::capture::sql::CaptureSpec;
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

/// Installs capture on `table` imaging `columns` (plus the key, `id`).
async fn capture(client: &mut Client, table: &str, columns: &[&str]) {
    let spec = CaptureSpec::new(
        table,
        vec!["id".to_string()],
        columns.iter().map(|c| c.to_string()),
    )
    .expect("valid spec");
    match install::install(client, DEFAULT_SCHEMA, &spec, None)
        .await
        .expect("install")
    {
        Progress::Done(_) => {}
        Progress::Waiting(wait) => panic!("an install without a deadline landed: {wait}"),
    }
}

/// A signed multiset of images, by their `jsonb` text.
type Images = BTreeMap<String, i64>;

fn add(images: &mut Images, image: String, by: i64) {
    let n = images.get(&image).copied().unwrap_or(0) + by;
    if n == 0 {
        images.remove(&image);
    } else {
        images.insert(image, n);
    }
}

/// `table`'s rows as capture images them: `id` and `columns`, each as its
/// text, as a `jsonb` text.
async fn table_images(client: &Client, table: &str, columns: &[&str]) -> Images {
    let pairs: Vec<String> = std::iter::once("id")
        .chain(columns.iter().copied())
        .map(|c| format!("'{c}', {c}::text"))
        .collect();
    let mut images = Images::new();
    for row in client
        .query(
            &format!(
                "select jsonb_build_object({})::text from {table}",
                pairs.join(", ")
            ),
            &[],
        )
        .await
        .expect("read the table")
    {
        add(&mut images, row.get(0), 1);
    }
    images
}

/// `table`'s rows by key, imaged as [`table_images`] does.
async fn final_rows(client: &Client, table: &str, columns: &[&str]) -> BTreeMap<String, String> {
    let pairs: Vec<String> = std::iter::once("id")
        .chain(columns.iter().copied())
        .map(|c| format!("'{c}', {c}::text"))
        .collect();
    client
        .query(
            &format!(
                "select id::text, jsonb_build_object({})::text from {table}",
                pairs.join(", ")
            ),
            &[],
        )
        .await
        .expect("read the table")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// Every ring row of transaction `txid` for `table`, in `(lsn, change_id)`
/// order.
async fn ring(client: &Client, table: &str, txid: &str) -> Vec<RingRow> {
    let arms: Vec<String> = (0..4)
        .map(|n| {
            format!(
                "select src_table, row_txid, key, op, old_image, new_image, \
                 transition_images, lsn, change_id from seg_{n}"
            )
        })
        .collect();
    client
        .query(
            &format!(
                "select key, op, old_image::text, new_image::text, \
                        (transition_images -> 'old')::text, \
                        (transition_images -> 'new')::text, \
                        transition_images is not null \
                 from ({}) r where src_table = $1 and row_txid::text = $2 \
                   and op <> 'schema_changed' \
                 order by lsn, change_id",
                arms.join(" union all ")
            ),
            &[&table, &txid],
        )
        .await
        .expect("read the ring")
        .into_iter()
        .map(|row| {
            // A side's JSON `null` means the row has none.
            let side = |i: usize| -> Option<String> {
                row.get::<_, Option<String>>(i).filter(|v| v != "null")
            };
            RingRow {
                key: row.get(0),
                op: row.get(1),
                old_image: row.get(2),
                new_image: row.get(3),
                transition: row.get::<_, bool>(6).then(|| (side(4), side(5))),
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RingRow {
    key: String,
    op: String,
    old_image: Option<String>,
    new_image: Option<String>,
    /// `transition_images`' `(old, new)` when it is set.
    transition: Option<(Option<String>, Option<String>)>,
}

/// The ring rows' `Σ +new − old`, each row's sides its transition images.
fn chain_sum(rows: &[RingRow]) -> Images {
    let mut images = Images::new();
    for row in rows {
        let (old, new) = row
            .transition
            .clone()
            .unwrap_or_else(|| (row.old_image.clone(), row.new_image.clone()));
        if let Some(new) = new {
            add(&mut images, new, 1);
        }
        if let Some(old) = old {
            add(&mut images, old, -1);
        }
    }
    images
}

/// What a 1-1 reader ends with for each key the rows name: the last row's
/// `new_image`, or nothing after a delete.
fn last_images(rows: &[RingRow]) -> BTreeMap<String, Option<String>> {
    let mut last = BTreeMap::new();
    for row in rows {
        let image = (row.op != "delete")
            .then(|| row.new_image.clone())
            .flatten();
        last.insert(row.key.clone(), image);
    }
    last
}

/// Runs `sql` in one transaction on `client` and checks that the ring rows
/// it staged for `table` sum to the table's rows after minus its rows
/// before, imaging `columns`, and that each key's last row is its final
/// state. Returns the rows.
async fn assert_chains(client: &Client, table: &str, columns: &[&str], sql: &str) -> Vec<RingRow> {
    let before = table_images(client, table, columns).await;
    client
        .batch_execute(&format!("begin; {sql}"))
        .await
        .unwrap_or_else(|e| panic!("run {sql:?}: {e:?}"));
    let txid: String = client
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("txid")
        .get(0);
    client.batch_execute("commit").await.expect("commit");
    let after = table_images(client, table, columns).await;
    let rows = ring(client, table, &txid).await;

    let mut expected = after;
    for (image, n) in before {
        add(&mut expected, image, -n);
    }
    assert_eq!(
        chain_sum(&rows),
        expected,
        "{sql:?}: the ring rows' +new − old must be the rows after minus the rows before; \
         ring rows: {rows:#?}"
    );

    let by_key = final_rows(client, table, columns).await;
    for (key, image) in last_images(&rows) {
        assert_eq!(
            image.as_ref(),
            by_key.get(&key),
            "{sql:?}: key {key}'s last ring row must be its final state; ring rows: {rows:#?}"
        );
    }
    rows
}

/// `public.src (id int primary key, g int, v int)` holding `(1, 1, 2)`,
/// `(2, 1, 4)`, imaging `g` and `v`.
async fn src_table(dsn: &str) -> Client {
    let mut client = connect(dsn).await;
    client
        .batch_execute(
            "create table public.src (id int primary key, g int, v int); \
             insert into public.src values (1, 1, 2), (2, 1, 4)",
        )
        .await
        .expect("create public.src");
    capture(&mut client, "public.src", &["g", "v"]).await;
    client
}

const SRC: &[&str] = &["g", "v"];

/// An application `AFTER` row trigger on `public.src` for `events`, running
/// `body` (PL/pgSQL statements over `new`/`old`) at depth 1 only.
async fn app_trigger(client: &Client, events: &str, body: &str) {
    client
        .batch_execute(&format!(
            "create function public.app() returns trigger language plpgsql as $$ \
             begin \
                 if pg_trigger_depth() = 1 then {body} end if; \
                 return null; \
             end $$; \
             create trigger app after {events} on public.src \
                 for each row execute function public.app()"
        ))
        .await
        .expect("create the application trigger");
}

/// `H1.nested-update`: an `AFTER ROW` trigger
/// rewrites the row its own statement wrote (`a → b`, then `b → c`). The
/// nested capture stages `(b, c)` and the outer one `(a, c)` with the live
/// row, which alone sum to `2c − a − b`.
#[tokio::test]
async fn h1_nested_update() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = src_table(db.dsn()).await;
    app_trigger(
        &client,
        "update",
        "if new.v % 2 = 1 then update public.src set v = new.v + 1 where id = new.id; end if;",
    )
    .await;

    let rows = assert_chains(
        &client,
        "public.src",
        SRC,
        "update public.src set v = 3 where id = 1",
    )
    .await;
    assert_eq!(
        rows.last().expect("the outer row"),
        &RingRow {
            key: "1".to_string(),
            op: "update".to_string(),
            old_image: Some(r#"{"g": "1", "v": "2", "id": "1"}"#.to_string()),
            new_image: Some(r#"{"g": "1", "v": "4", "id": "1"}"#.to_string()),
            transition: Some((
                Some(r#"{"g": "1", "v": "2", "id": "1"}"#.to_string()),
                Some(r#"{"g": "1", "v": "3", "id": "1"}"#.to_string()),
            )),
        },
        "the outer row images the live row and carries its transition rows"
    );
    assert_chains(
        &client,
        "public.src",
        SRC,
        "update public.src set v = v + 1",
    )
    .await;
}

/// `H1.insert-nested-delete`: an insert whose row an `AFTER ROW` trigger
/// deletes. The outer capture finds the key gone and stages a delete
/// imaging its new row as `old_image`; its transition images say it
/// inserted that row.
#[tokio::test]
async fn h1_insert_nested_delete() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = src_table(db.dsn()).await;
    app_trigger(
        &client,
        "insert",
        "if new.v < 0 then delete from public.src where id = new.id; end if;",
    )
    .await;

    let rows = assert_chains(
        &client,
        "public.src",
        SRC,
        "insert into public.src values (9, 1, -1), (10, 1, 10)",
    )
    .await;
    let nine = r#"{"g": "1", "v": "-1", "id": "9"}"#.to_string();
    assert!(
        rows.contains(&RingRow {
            key: "9".to_string(),
            op: "delete".to_string(),
            old_image: Some(nine.clone()),
            new_image: None,
            transition: Some((None, Some(nine))),
        }),
        "{rows:#?}"
    );
}

/// `H1.insert-nested-update`: an insert whose row an `AFTER ROW` trigger
/// updates.
#[tokio::test]
async fn h1_insert_nested_update() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = src_table(db.dsn()).await;
    app_trigger(
        &client,
        "insert",
        "if new.v % 2 = 1 then update public.src set v = new.v + 1 where id = new.id; end if;",
    )
    .await;

    assert_chains(
        &client,
        "public.src",
        SRC,
        "insert into public.src values (9, 1, 5), (10, 1, 10)",
    )
    .await;
}

/// `H1.nested-delete-reinsert`, one of the two failures E1 observed: an
/// update whose row an `AFTER ROW` trigger deletes and re-inserts.
#[tokio::test]
async fn h1_nested_delete_reinsert() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = src_table(db.dsn()).await;
    app_trigger(
        &client,
        "update",
        "if new.v = 7 then \
             delete from public.src where id = new.id; \
             insert into public.src values (new.id, new.g, 8); \
         end if;",
    )
    .await;

    assert_chains(
        &client,
        "public.src",
        SRC,
        "update public.src set v = 7 where id = 1",
    )
    .await;
}

/// A self-referencing `ON UPDATE CASCADE` table, imaging `g` and `v`, and
/// also `o` and `trellis_pair`, which share names with the update re-read's
/// own aliases.
async fn fk_table(dsn: &str, rows: &str) -> Client {
    let mut client = connect(dsn).await;
    client
        .batch_execute(&format!(
            "create table public.fk (id int primary key, \
                 g int references public.fk (id) on update cascade, v int, \
                 o int, trellis_pair int); \
             insert into public.fk values {rows}"
        ))
        .await
        .expect("create public.fk");
    capture(&mut client, "public.fk", FK).await;
    client
}

const FK: &[&str] = &["g", "v", "o", "trellis_pair"];

/// `H1.fk-cascade-self`, the other engine
/// case: row 1 is its own parent, so moving its key cascades into the same
/// row. The cascade's update joins the outer update's transition tables:
/// the old keys are distinct (1, then 100) but the new key 100 occurs twice,
/// and pairing by key used the old row `(100, 1)` twice.
#[tokio::test]
async fn h1_fk_cascade_self() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = fk_table(
        db.dsn(),
        "(1, 1, 10, 0, 0), (2, 1, 20, 0, 0), (3, 3, 30, 0, 1)",
    )
    .await;

    assert_chains(
        &client,
        "public.fk",
        FK,
        "update public.fk set id = 100 where id = 1",
    )
    .await;
    assert_chains(
        &client,
        "public.fk",
        FK,
        "update public.fk set id = id + 1000",
    )
    .await;
}

/// `H1.fk-cascade-other`: the cascade touches another row only.
#[tokio::test]
async fn h1_fk_cascade_other() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = fk_table(
        db.dsn(),
        "(3, 3, 30, 0, 0), (1, 3, 10, 0, 0), (2, 1, 20, 0, 0)",
    )
    .await;

    assert_chains(
        &client,
        "public.fk",
        FK,
        "update public.fk set id = 100 where id = 1",
    )
    .await;
}

/// A nested trigger that re-keys the row its outer statement wrote, and one
/// that puts back the key its outer delete removed: the outer delete
/// stages the live row as an `update` whose transition images say it had no
/// new row.
#[tokio::test]
async fn a_nested_rekey_or_reinsert_sums_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = src_table(db.dsn()).await;
    client
        .batch_execute(
            "create function public.rekey() returns trigger language plpgsql as $$ \
             begin \
                 if new.v = 100 and new.id < 100 then \
                     update public.src set id = new.id + 100 where id = new.id; \
                 end if; \
                 return null; \
             end $$; \
             create trigger rekey after insert or update on public.src \
                 for each row execute function public.rekey(); \
             create function public.redo() returns trigger language plpgsql as $$ \
             begin \
                 if old.id = 1 then insert into public.src values (old.id, 1, -5); end if; \
                 return null; \
             end $$; \
             create trigger redo after delete on public.src \
                 for each row execute function public.redo()",
        )
        .await
        .expect("create the triggers");

    assert_chains(
        &client,
        "public.src",
        SRC,
        "update public.src set v = 100 where id = 2",
    )
    .await;
    assert_chains(
        &client,
        "public.src",
        SRC,
        "insert into public.src values (5, 1, 100)",
    )
    .await;
    let rows = assert_chains(
        &client,
        "public.src",
        SRC,
        "delete from public.src where id = 1",
    )
    .await;
    assert_eq!(
        rows.last().expect("the outer row"),
        &RingRow {
            key: "1".to_string(),
            op: "update".to_string(),
            old_image: Some(r#"{"g": "1", "v": "2", "id": "1"}"#.to_string()),
            new_image: Some(r#"{"g": "1", "v": "-5", "id": "1"}"#.to_string()),
            transition: Some((Some(r#"{"g": "1", "v": "2", "id": "1"}"#.to_string()), None)),
        }
    );
}

/// Two cascading foreign keys on one row: one statement on the parent table
/// updates the child row twice, and both versions land in one capture call,
/// so a key repeats on both sides.
#[tokio::test]
async fn two_cascades_into_one_row_sum_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client
        .batch_execute(
            "create table public.p (id int primary key); \
             insert into public.p values (1), (2); \
             create table public.c (id int primary key, \
                 x int references public.p (id) on update cascade, \
                 y int references public.p (id) on update cascade); \
             insert into public.c values (10, 1, 2), (11, 2, 2), (12, 1, 1)",
        )
        .await
        .expect("create p and c");
    capture(&mut client, "public.c", &["x", "y"]).await;

    assert_chains(
        &client,
        "public.c",
        &["x", "y"],
        "update public.p set id = id + 10",
    )
    .await;
}

/// Sibling events of one statement: an upsert that inserts some keys and
/// updates others, a writable CTE that deletes a key and inserts it again,
/// and a `MERGE`.
#[tokio::test]
async fn sibling_events_of_one_statement_sum_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = src_table(db.dsn()).await;

    assert_chains(
        &client,
        "public.src",
        SRC,
        "insert into public.src values (1, 1, 11), (3, 1, 3) \
         on conflict (id) do update set v = excluded.v",
    )
    .await;
    assert_chains(
        &client,
        "public.src",
        SRC,
        "with d as (delete from public.src where id = 2 returning id) \
         insert into public.src select id, 1, 22 from d",
    )
    .await;
    assert_chains(
        &client,
        "public.src",
        SRC,
        "merge into public.src t using (values (1, 0), (9, 9)) s(id, v) on t.id = s.id \
         when matched then update set v = s.v when not matched then insert values (s.id, 1, s.v)",
    )
    .await;
}

/// The schema-changed path always re-reads and builds its
/// statement at run time; it stages transition images too.
#[tokio::test]
async fn the_schema_changed_path_sums_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client
        .batch_execute(
            "create table public.src (id int primary key, g int, v int, b int); \
             insert into public.src values (1, 1, 1, 1), (2, 1, 2, 2)",
        )
        .await
        .expect("create public.src");
    capture(&mut client, "public.src", &["g", "v", "b"]).await;
    app_trigger(
        &client,
        "insert or update",
        "update public.src set v = new.v + 100 where id = new.id;",
    )
    .await;
    client
        .batch_execute("alter table public.src drop column b")
        .await
        .expect("drop an imaged column");

    assert_chains(
        &client,
        "public.src",
        SRC,
        "update public.src set v = v + 10",
    )
    .await;
    assert_chains(
        &client,
        "public.src",
        SRC,
        "insert into public.src values (3, 1, 3)",
    )
    .await;
    assert_chains(
        &client,
        "public.src",
        SRC,
        "update public.src set id = 7 where id = 3",
    )
    .await;
}

/// A statement no other write touched images its transition tables and
/// leaves the transition images unset: the common path writes nothing new.
#[tokio::test]
async fn a_statement_without_a_nested_write_sets_no_transition_images() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = src_table(db.dsn()).await;

    for sql in [
        "insert into public.src values (3, 1, 3)",
        "update public.src set v = v + 1",
        "update public.src set id = id + 10 where id = 1",
        "delete from public.src where id = 2",
    ] {
        let rows = assert_chains(&client, "public.src", SRC, sql).await;
        assert!(
            rows.iter().all(|r| r.transition.is_none()),
            "{sql:?} must not re-read: {rows:#?}"
        );
    }
}

/// A partitioned source: the root's statement trigger sees every leaf's
/// rows, and a nested write the application trigger makes through the root
/// (including onto a row an update moved to another partition) or straight
/// to a leaf counts once.
#[tokio::test]
async fn a_partitioned_source_sums_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client
        .batch_execute(
            "create table public.src (id int primary key, g int, v int) partition by range (id); \
             create table public.src_a partition of public.src for values from (0) to (100); \
             create table public.src_b partition of public.src for values from (100) to (1000); \
             insert into public.src values (1, 1, 2), (2, 1, 4), (150, 1, 6)",
        )
        .await
        .expect("create public.src");
    capture(&mut client, "public.src", SRC).await;
    app_trigger(
        &client,
        "insert or update",
        "if new.v % 2 = 1 then update public.src set v = new.v + 1 where id = new.id; end if;",
    )
    .await;

    for sql in [
        "update public.src set v = v + 1",
        "update public.src set id = id + 100, v = v + 1 where id < 100",
        "update public.src_b set v = v + 1",
        "insert into public.src values (5, 1, 1), (500, 1, 3)",
    ] {
        assert_chains(&client, "public.src", SRC, sql).await;
    }
}

/// An insert's new rows can hold one key twice: a `BEFORE ROW` trigger
/// deletes the row the statement inserted a moment earlier, and the
/// statement inserts the key again.
#[tokio::test]
async fn an_insert_that_writes_a_key_twice_sums_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = src_table(db.dsn()).await;
    client
        .batch_execute(
            "create function public.replace() returns trigger language plpgsql as $$ \
             begin delete from public.src where id = new.id; return new; end $$; \
             create trigger replace before insert on public.src \
                 for each row execute function public.replace()",
        )
        .await
        .expect("create the trigger");

    let rows = assert_chains(
        &client,
        "public.src",
        SRC,
        "insert into public.src values (9, 1, 1), (9, 1, 2), (1, 5, 5)",
    )
    .await;
    assert!(
        rows.iter().any(|r| r.transition.is_some()),
        "the first version of key 9 isn't the live row: {rows:#?}"
    );
}

/// A key swap under a deferrable primary key, with and without a nested
/// write: each key is on both sides once, and pairs with the row that now
/// holds it.
#[tokio::test]
async fn a_key_swap_sums_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client
        .batch_execute(
            "create table public.src (id int primary key deferrable initially immediate, \
                 g int, v int); \
             insert into public.src values (1, 1, 2), (2, 1, 4), (3, 1, 6)",
        )
        .await
        .expect("create public.src");
    capture(&mut client, "public.src", SRC).await;

    assert_chains(
        &client,
        "public.src",
        SRC,
        "update public.src set id = 3 - id where id in (1, 2)",
    )
    .await;
    app_trigger(
        &client,
        "update",
        "update public.src set v = new.v + 1000 where id = new.id;",
    )
    .await;
    assert_chains(
        &client,
        "public.src",
        SRC,
        "update public.src set id = case id when 1 then 2 when 2 then 3 else 1 end",
    )
    .await;
}

/// `H1.fk-cascade-self` with a composite key: the new-key check and the
/// one-to-one pairing work on the joined key.
#[tokio::test]
async fn a_composite_key_cascade_sums_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client
        .batch_execute(
            "create table public.ck (a int, id int, pa int, pid int, v int, \
                 primary key (a, id), \
                 foreign key (pa, pid) references public.ck (a, id) on update cascade); \
             insert into public.ck values (1, 1, 1, 1, 10), (1, 2, 1, 1, 20), (2, 1, 2, 1, 30)",
        )
        .await
        .expect("create public.ck");
    let spec = CaptureSpec::new(
        "public.ck",
        vec!["a".to_string(), "id".to_string()],
        ["pa", "pid", "v"].iter().map(|c| c.to_string()),
    )
    .expect("valid spec");
    match install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
        .await
        .expect("install")
    {
        Progress::Done(_) => {}
        Progress::Waiting(wait) => panic!("an install without a deadline landed: {wait}"),
    }

    // `table_images` adds `id`; `a` is the key's other column.
    let columns = &["a", "pa", "pid", "v"];
    let before = table_images(&client, "public.ck", columns).await;
    client
        .batch_execute("begin; update public.ck set id = id + 100 where a = 1 and id = 1")
        .await
        .expect("update");
    let txid: String = client
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("txid")
        .get(0);
    client.batch_execute("commit").await.expect("commit");
    let mut expected = table_images(&client, "public.ck", columns).await;
    for (image, n) in before {
        add(&mut expected, image, -n);
    }
    let rows = ring(&client, "public.ck", &txid).await;
    assert_eq!(chain_sum(&rows), expected, "ring rows: {rows:#?}");
}
