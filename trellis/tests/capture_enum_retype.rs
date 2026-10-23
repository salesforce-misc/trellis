//! A column re-typed from one enum to another (`ALTER COLUMN ... TYPE
//! other_enum USING ...`) pauses every definition that reads it, and
//! `RESUME` rebuilds it from the source without casting any label it stored
//! (#1327, ADR-0020 "Stored value text is trusted only under the type's
//! current value identity").
//!
//! No value of one enum is a value of another, so the labels Trellis keeps
//! (ring images, held keys, a 1-1 ledger's `__from_key`, the columns it
//! created of the old type) name nothing under the new type. The capture
//! pass finds the column's recorded enum differs from its live one, pauses
//! each reader with the reason and a gate, as a relabel does, and records
//! the new type, so a later pass finds nothing new. The resume waits for
//! the changes staged before the re-type to drain, re-types each copy
//! without a cast (a key's rows are deleted first) and rebuilds. The type
//! matrix (`type_matrix.rs`) holds the same change in every place; this
//! holds the pass's record, and changes staged across the re-type. Every
//! step is explicit; nothing polls (#297).

#[path = "support/relabel.rs"]
mod relabel;

use relabel::H;
use trellis::defs::TransformStatus;

/// `mood` shares no label with `tone`; the re-type maps each value.
const SOURCE: &str = "create type public.mood as enum ('fast', 'slow'); \
     create table public.s (id int primary key, tone public.tone, n int); \
     create index on public.s (tone); create index on public.s (n); \
     create table public.k (tone public.tone primary key, n int); \
     insert into public.s values (1, 'brisk', 1), (2, 'calm', 2), (3, 'brisk', 3); \
     insert into public.k values ('brisk', 1), ('calm', 2);";

const MAP: &str = "(case tone::text when 'brisk' then 'fast' else 'slow' end)::public.mood";

fn retype() -> String {
    format!(
        "alter table public.s alter column tone type public.mood using {MAP}; \
         alter table public.k alter column tone type public.mood using {MAP}"
    )
}

/// The reason the re-type of `table`'s `tone` pauses a reader with.
fn reason(table: &str) -> String {
    format!(
        "column \"tone\" of {table}, which this definition reads, changed type from enum \
         public.tone to enum public.mood; Trellis's staged changes and keys still name the old \
         label; resume to rebuild the target from the source"
    )
}

/// Each definition's gate and reason.
async fn records(h: &H) -> Vec<String> {
    h.rows(
        "select d.target_table, f.relabel_lsn::text, f.error \
         from capture_failures f join transform_definitions d on d.id = f.transform_id",
    )
    .await
}

/// A passthrough, a group-basis `GROUP BY` the enum, a `MIN` of it (an
/// aggregate field of the enum's type) and a 1-1 keyed by it, with changes
/// staged across the re-type. The pass pauses each once with
/// the reason and records the new type; a later pass changes neither its
/// gate nor its reason. `RESUME` waits for the staged changes and rebuilds
/// each exactly, and later writes apply under the new type.
#[tokio::test]
async fn a_re_type_to_another_enum_pauses_each_reader_once_and_resumes_exactly() {
    trellis::dev::staging::set_group_basis(true);
    let mut h = H::new(
        SOURCE,
        &[
            "TRANSFORM t1 FROM public.s SELECT tone AS tone, n AS n",
            "TRANSFORM per_tone FROM public.s GROUP BY tone \
             SELECT tone AS tone, COUNT(*) AS c, SUM(n) AS sn",
            "TRANSFORM lo FROM public.s GROUP BY n SELECT n AS n, MIN(tone) AS lo",
            "TRANSFORM kt FROM public.k SELECT n AS n",
        ],
    )
    .await;
    h.write(
        "insert into public.s values (4, 'calm', 4); \
         update public.s set n = 30 where id = 3; delete from public.s where id = 1; \
         update public.k set n = 10 where tone = 'brisk'",
    )
    .await;
    h.write(&retype()).await;
    h.capture_pass().await;
    for (target, table) in [
        ("t1", "public.s"),
        ("per_tone", "public.s"),
        ("lo", "public.s"),
        ("kt", "public.k"),
    ] {
        assert_eq!(h.status(target).await, TransformStatus::Paused, "{target}");
        let failure = h.capture_failure(target).await.unwrap_or_default();
        assert!(failure.starts_with(&reason(table)), "{target}: {failure}");
    }
    let gated = records(&h).await;
    assert_eq!(
        h.count("select count(*) from capture_failures where relabel_lsn is not null")
            .await,
        4,
        "each pause has a gate: {gated:?}"
    );
    // The new type is recorded: the token each reads it under, and the key's.
    assert_eq!(
        h.rows("select source_columns->>'tone' from transform_definitions")
            .await,
        ["(enum:public.mood)"; 4]
    );
    assert_eq!(
        h.rows("select distinct type_name from definition_key_types where column_name = 'tone'")
            .await,
        ["(public.mood)"]
    );
    // A later pass finds nothing new: neither the gate nor the reason moves.
    h.write("insert into public.s values (5, 'fast', 5)").await;
    h.capture_pass().await;
    assert_eq!(records(&h).await, gated, "a later pass moved a gate");

    h.resume(&["t1", "per_tone", "lo", "kt"]).await;
    h.converge("after resume").await;
    h.assert_exact(
        "t1",
        "select id, tone::text, n from public.t1",
        "select id, tone::text, n from public.s",
    )
    .await;
    h.assert_exact(
        "per_tone",
        "select tone::text, c, sn from public.per_tone",
        "select tone::text, count(*), sum(n) from public.s group by tone",
    )
    .await;
    // `MIN` is recompute-only, off the group basis, which `self_check`
    // doesn't audit.
    h.assert_exact_unaudited(
        "lo",
        "select n, lo::text from public.lo",
        "select n, min(tone)::text from public.s group by n",
    )
    .await;
    h.assert_exact(
        "kt",
        "select tone::text, n from public.kt",
        "select tone::text, n from public.k",
    )
    .await;

    // Writes after the resume apply under the new type.
    h.write(
        "insert into public.s values (6, 'slow', 6); delete from public.s where id = 2; \
         update public.k set n = 20 where tone = 'slow'",
    )
    .await;
    h.converge("later writes").await;
    h.assert_exact(
        "t1",
        "select id, tone::text, n from public.t1",
        "select id, tone::text, n from public.s",
    )
    .await;
    h.assert_exact(
        "per_tone",
        "select tone::text, c, sn from public.per_tone",
        "select tone::text, count(*), sum(n) from public.s group by tone",
    )
    .await;
    // `MIN` is recompute-only, off the group basis, which `self_check`
    // doesn't audit.
    h.assert_exact_unaudited(
        "lo",
        "select n, lo::text from public.lo",
        "select n, min(tone)::text from public.s group by n",
    )
    .await;
    h.assert_exact(
        "kt",
        "select tone::text, n from public.kt",
        "select tone::text, n from public.k",
    )
    .await;
}

/// A re-type to `text` keeps every label as a value of the new type: it is
/// no change of value identity, so this check pauses nothing for it.
#[tokio::test]
async fn a_re_type_to_text_is_no_value_identity_change() {
    trellis::dev::staging::set_group_basis(true);
    let mut h = H::new(
        SOURCE,
        &["TRANSFORM t1 FROM public.s SELECT tone AS tone, n AS n"],
    )
    .await;
    h.write("alter table public.s alter column tone type text")
        .await;
    h.capture_pass().await;
    assert_eq!(
        h.count("select count(*) from capture_failures where relabel_lsn is not null")
            .await,
        0
    );
}
