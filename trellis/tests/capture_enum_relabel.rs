//! An enum value relabelled (`ALTER TYPE ... RENAME VALUE`) pauses every
//! definition that reads a column of the type, and `RESUME` rebuilds it from
//! the source without casting any label it stored (#1241, ADR-0020 "Stored
//! value text is trusted only under the type's current value identity").
//!
//! Trellis keeps an enum value as its label text in ring images, held keys
//! and a 1-1 ledger's `__from_key`. A relabel keeps the value's `pg_enum`
//! OID and fires no trigger, so that text names a label that no longer
//! exists. Each definition records its enum values by OID; the capture pass
//! finds a recorded OID whose label changed and pauses the definition with
//! the reason. Its resume waits for the changes staged before the relabel
//! to drain (the drain skips a paused definition's share of them), and
//! rebuilds. A 1-1 keyed by an enum starts again from empty, since its
//! ledger keys are label text.
//!
//! Each shape: the relabel pauses it with the reason, and `RESUME` then
//! gives zero oracle difference, no held key and `self_check` `Converged`.
//! The ledger `GROUP BY` path, which needs the group-basis knob off, is
//! `capture_enum_relabel_ledger.rs`. Every step is explicit; nothing polls
//! (#297).

#[path = "support/relabel.rs"]
mod relabel;

use relabel::{H, RELABEL};
use trellis::defs::TransformStatus;

/// A 1-1 enum passthrough beside an aggregate on the same table that
/// doesn't name the enum, with changes staged across the relabel. The drain
/// before the pass fails their cast and holds their keys. Both read the
/// source's whole row as its recorded column types (`source_columns`), so
/// the pass pauses both, and their resumes discard the held keys and
/// rebuild.
#[tokio::test]
async fn a_passthrough_beside_an_aggregate_pauses_and_resumes_exactly() {
    trellis::dev::staging::set_group_basis(true);
    let mut h = H::new(
        "create table public.s (id int primary key, tone public.tone, g int, n int); \
         create index on public.s (g); \
         insert into public.s values (1, 'calm', 1, 1), (3, 'calm', 2, 3);",
        &[
            "TRANSFORM t1 FROM public.s SELECT tone AS tone, n AS n",
            "TRANSFORM agg FROM public.s GROUP BY g SELECT g AS g, COUNT(*) AS c, SUM(n) AS sn",
        ],
    )
    .await;
    h.write(
        "insert into public.s values (2, 'brisk', 1, 2); \
         update public.s set tone = 'brisk', n = 10 where id = 1",
    )
    .await;
    h.write(RELABEL).await;
    h.drain_past_failures().await;
    assert!(h.held().await > 0, "the failed casts hold their keys");

    h.capture_pass().await;
    h.assert_paused_by_the_relabel(&["t1", "agg"]).await;
    h.resume(&["t1", "agg"]).await;
    h.converge("after resume").await;
    h.assert_exact(
        "t1",
        "select id, tone::text, n from public.t1",
        "select id, tone::text, n from public.s",
    )
    .await;
    h.assert_exact(
        "agg",
        "select g, c, sn from public.agg",
        "select g, count(*), sum(n) from public.s group by g",
    )
    .await;

    // Writes after the resume apply under the new label.
    h.write("insert into public.s values (4, 'zippy', 2, 4); delete from public.s where id = 3")
        .await;
    h.converge("later writes").await;
    h.assert_exact(
        "t1",
        "select id, tone::text, n from public.t1",
        "select id, tone::text, n from public.s",
    )
    .await;
}

const ENUM_KEYED: &str = "create table public.k (tone public.tone primary key, n int); \
     insert into public.k values ('brisk', 1), ('calm', 2);";

const ENUM_KEYED_TARGET: &str = "select tone::text, n from public.kt";
const ENUM_KEYED_ORACLE: &str = "select tone::text, n from public.k";

/// A 1-1 keyed by an enum with nothing pending: correct after the relabel,
/// but its ledger's keys are the old label's text, which the next rebuild
/// would cast. The relabel pauses it, the resume rebuilds it from empty, and
/// a later `request_backfill` succeeds.
#[tokio::test]
async fn an_enum_keyed_one_to_one_with_nothing_pending_rebuilds_and_backfills_later() {
    trellis::dev::staging::set_group_basis(true);
    let mut h = H::new(ENUM_KEYED, &["TRANSFORM kt FROM public.k SELECT n AS n"]).await;
    h.assert_exact("kt", ENUM_KEYED_TARGET, ENUM_KEYED_ORACLE)
        .await;
    h.write(RELABEL).await;
    h.capture_pass().await;
    h.assert_paused_by_the_relabel(&["kt"]).await;
    // A delete while it is paused: the rebuild from empty never brings it
    // back.
    h.write("delete from public.k where tone = 'calm'").await;
    h.resume(&["kt"]).await;
    h.converge("after resume").await;
    h.assert_exact("kt", ENUM_KEYED_TARGET, ENUM_KEYED_ORACLE)
        .await;
    assert_eq!(
        h.rows("select \"__from_key\" from public.kt__ledger").await,
        ["(zippy)"],
        "the ledger holds the current label"
    );

    h.trellis
        .request_backfill("public.k")
        .await
        .expect("request_backfill");
    h.converge("after request_backfill").await;
    h.assert_exact("kt", ENUM_KEYED_TARGET, ENUM_KEYED_ORACLE)
        .await;
}

/// A 1-1 keyed by an enum with changes to the relabelled key staged across
/// the relabel, and the resume made before they drain: it waits for the
/// drain to skip them, then rebuilds. Without that wait, the rebuild
/// re-derives the old label's key and fails (the mutation check).
#[tokio::test]
async fn an_enum_keyed_one_to_one_with_changes_pending_resumes_exactly() {
    trellis::dev::staging::set_group_basis(true);
    let mut h = H::new(ENUM_KEYED, &["TRANSFORM kt FROM public.k SELECT n AS n"]).await;
    h.write("update public.k set n = 10 where tone = 'brisk'; delete from public.k where tone = 'calm'; insert into public.k values ('calm', 3)")
        .await;
    h.write(RELABEL).await;
    h.capture_pass().await;
    h.assert_paused_by_the_relabel(&["kt"]).await;
    h.resume(&["kt"]).await;
    assert_eq!(h.status("kt").await, TransformStatus::Paused);
    let waiting = h.capture_failure("kt").await.unwrap_or_default();
    assert!(
        waiting.starts_with("resuming: waiting for the changes staged before an enum value"),
        "{waiting}"
    );
    h.converge("after resume").await;
    h.assert_exact("kt", ENUM_KEYED_TARGET, ENUM_KEYED_ORACLE)
        .await;
}

/// A 1-1 keyed by an enum whose released key's `recompute` is still pending
/// across the relabel. The `recompute` carries no image, but its key is the
/// old label's text: the resume waits for it to drain too, or the rebuilt
/// definition re-derives the old label and holds the key.
#[tokio::test]
async fn an_enum_keyed_one_to_one_waits_for_a_pending_recompute_of_the_old_label() {
    trellis::dev::staging::set_group_basis(true);
    let mut h = H::new(ENUM_KEYED, &["TRANSFORM kt FROM public.k SELECT n AS n"]).await;
    // Hold 'brisk' with a constraint its write breaks, then release it: the
    // release stages a `recompute` keyed 'brisk'.
    h.write("alter table public.kt add constraint small check (n < 100)")
        .await;
    h.write("update public.k set n = 1000 where tone = 'brisk'")
        .await;
    h.drain_past_failures().await;
    assert!(h.held().await > 0, "the failing write holds its key");
    h.write("alter table public.kt drop constraint small").await;
    h.trellis
        .release_key("kt", "public.k", "brisk")
        .await
        .expect("release_key");
    h.write(RELABEL).await;
    h.capture_pass().await;
    h.assert_paused_by_the_relabel(&["kt"]).await;
    h.resume(&["kt"]).await;
    let waiting = h.capture_failure("kt").await.unwrap_or_default();
    assert!(
        waiting.starts_with("resuming: waiting for the changes staged before an enum value"),
        "{waiting}"
    );
    h.converge("after resume").await;
    h.assert_exact("kt", ENUM_KEYED_TARGET, ENUM_KEYED_ORACLE)
        .await;
}

/// `GROUP BY tone` on the group basis, with a change staged across the
/// relabel and the resume made before it drains.
#[tokio::test]
async fn a_group_basis_group_by_the_enum_with_a_change_pending_resumes_exactly() {
    trellis::dev::staging::set_group_basis(true);
    let mut h = H::new(
        "create table public.s (id int primary key, tone public.tone, n int); \
         create index on public.s (tone); \
         insert into public.s values (1, 'brisk', 1), (2, 'calm', 2), (3, 'brisk', 3);",
        &["TRANSFORM per_tone FROM public.s GROUP BY tone \
           SELECT tone AS tone, COUNT(*) AS c, SUM(n) AS sn"],
    )
    .await;
    h.write(
        "insert into public.s values (4, 'brisk', 4); \
         update public.s set n = 30 where id = 3; delete from public.s where id = 1",
    )
    .await;
    h.write(RELABEL).await;
    h.capture_pass().await;
    h.assert_paused_by_the_relabel(&["per_tone"]).await;
    h.resume(&["per_tone"]).await;
    h.converge("after resume").await;
    h.assert_exact(
        "per_tone",
        "select tone::text, c, sn from public.per_tone",
        "select tone::text, count(*), sum(n) from public.s group by tone",
    )
    .await;
}

/// A `JOIN` grouped by the enum, with a change staged across the relabel.
#[tokio::test]
async fn a_join_grouped_by_the_enum_with_a_change_pending_resumes_exactly() {
    trellis::dev::staging::set_group_basis(true);
    let mut h = H::new(
        "create table public.items (id int primary key, tone public.tone); \
         create table public.notes (id int primary key, item_id int); \
         create index on public.items (tone); create index on public.notes (item_id); \
         insert into public.items values (1, 'brisk'), (2, 'calm'); \
         insert into public.notes values (1, 1), (2, 2), (3, 1);",
        &[
            "TRANSFORM by_tone FROM public.items JOIN public.notes ON items.id = notes.item_id \
             GROUP BY items.tone SELECT items.tone AS tone, COUNT(*) AS c",
        ],
    )
    .await;
    h.write(
        "insert into public.items values (3, 'brisk'); insert into public.notes values (4, 3); \
         update public.items set tone = 'brisk' where id = 2",
    )
    .await;
    h.write(RELABEL).await;
    h.capture_pass().await;
    h.assert_paused_by_the_relabel(&["by_tone"]).await;
    h.resume(&["by_tone"]).await;
    h.converge("after resume").await;
    h.assert_exact(
        "by_tone",
        "select tone::text, c from public.by_tone",
        "select i.tone::text, count(*) from public.items i \
         join public.notes n on n.item_id = i.id group by i.tone",
    )
    .await;
}

/// `ADD VALUE` adds an OID and relabels nothing: no definition pauses, the
/// new value is recorded, and a write of it applies.
#[tokio::test]
async fn a_new_value_pauses_nothing_and_is_recorded() {
    trellis::dev::staging::set_group_basis(true);
    let mut h = H::new(
        "create table public.s (id int primary key, tone public.tone); \
         create index on public.s (tone); \
         create table public.k (tone public.tone primary key, n int);",
        &[
            "TRANSFORM t1 FROM public.s SELECT tone AS tone",
            "TRANSFORM per_tone FROM public.s GROUP BY tone SELECT tone AS tone, COUNT(*) AS c",
            "TRANSFORM kt FROM public.k SELECT n AS n",
        ],
    )
    .await;
    let recorded = "select count(*) from definition_enum_values";
    assert_eq!(h.count(recorded).await, 6, "two values for each reader");
    h.write("alter type public.tone add value 'zesty'").await;
    h.write(
        "insert into public.s values (1, 'zesty'), (2, 'brisk'); \
         insert into public.k values ('zesty', 1)",
    )
    .await;
    h.converge("after the new value").await;
    h.assert_paused_by_the_relabel(&[]).await;
    assert_eq!(h.count(recorded).await, 9, "the new value is recorded");
    h.assert_exact(
        "t1",
        "select id, tone::text from public.t1",
        "select id, tone::text from public.s",
    )
    .await;
    h.assert_exact(
        "kt",
        "select tone::text, n from public.kt",
        "select tone::text, n from public.k",
    )
    .await;
    h.assert_exact(
        "per_tone",
        "select tone::text, c from public.per_tone",
        "select tone::text, count(*) from public.s group by tone",
    )
    .await;
}

/// A relabel of an enum type a definition no longer reads (its source's
/// column was re-typed to another enum, and the old type's
/// `definition_types` row stays until a resume) pauses nothing, and a
/// relabel of the type it reads now does.
#[tokio::test]
async fn a_relabel_of_a_type_no_longer_read_pauses_nothing() {
    trellis::dev::staging::set_group_basis(true);
    let mut h = H::new(
        "create type public.mood as enum ('brisk', 'calm'); \
         create table public.s (id int primary key, tone public.tone, n int); \
         insert into public.s values (1, 'brisk', 1);",
        &["TRANSFORM t1 FROM public.s SELECT n AS n"],
    )
    .await;
    h.write(
        "alter table public.s alter column tone type public.mood using tone::text::public.mood",
    )
    .await;
    // However the re-type is taken up (a rebuild, or a pause and a resume).
    for _ in 0..4 {
        h.pass().await;
        if h.status("t1").await == TransformStatus::Paused {
            h.resume(&["t1"]).await;
        }
        h.build().await;
        h.drain().await;
    }
    h.converge("after the re-type").await;
    assert_eq!(
        h.rows("select type_name from definition_types").await,
        ["(public.tone)"],
        "the re-type paused nothing, so the old type's row stays"
    );
    let types = "select count(*) from definition_enum_values v \
                 where v.type_oid = 'public.tone'::regtype";
    assert_eq!(h.count(types).await, 0, "the old type's values are gone");

    h.write(RELABEL).await;
    h.capture_pass().await;
    h.assert_paused_by_the_relabel(&[]).await;

    h.write("alter type public.mood rename value 'calm' to 'cool'")
        .await;
    h.capture_pass().await;
    let reason = h.capture_failure("t1").await.unwrap_or_default();
    assert!(
        reason.starts_with("enum value 'calm' of type public.mood was renamed to 'cool'"),
        "{reason}"
    );
    h.resume(&["t1"]).await;
    h.converge("after resume").await;
    h.assert_exact(
        "t1",
        "select id, n from public.t1",
        "select id, n from public.s",
    )
    .await;
}

/// A definition paused by the operator when the relabel comes takes the
/// relabel's record too, so its resume is the relabel's: it waits for the
/// staged changes and rebuilds from empty.
#[tokio::test]
async fn an_operator_pause_takes_the_relabel_record() {
    trellis::dev::staging::set_group_basis(true);
    let mut h = H::new(ENUM_KEYED, &["TRANSFORM kt FROM public.k SELECT n AS n"]).await;
    h.trellis.apply("PAUSE TRANSFORM kt").await.expect("pause");
    h.write("update public.k set n = 10 where tone = 'brisk'")
        .await;
    h.write(RELABEL).await;
    h.capture_pass().await;
    assert_eq!(
        h.count("select count(*) from capture_failures where relabel_lsn is not null")
            .await,
        1
    );
    h.resume(&["kt"]).await;
    h.converge("after resume").await;
    h.assert_exact("kt", ENUM_KEYED_TARGET, ENUM_KEYED_ORACLE)
        .await;
}
