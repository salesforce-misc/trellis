//! Integration tests for the claim-time fold (issue #10, stage 04), run
//! against a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/04-claiming-and-the-fold.md for the design
//! these tests hold the implementation to. Every test builds its own
//! from-scratch Rust oracle (the expected per-key folded record, computed
//! directly from the raw rows inserted) rather than re-deriving the SQL —
//! and is meant to fail on a naive implementation: a blanket `COALESCE`, a
//! filter on `op`, an own-slot-only window, or ignoring `change_id`.

use std::time::SystemTime;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::{BucketFilter, FoldedChange, TRUNCATE_SENTINEL_KEY, fold, seal};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `sealing.rs`/`staging_ring.rs`'s convention.
async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

/// One raw row to insert into a ring table, with every fold-relevant column
/// exposed (unlike `StagedChange`, which enforces the image-bearing/
/// image-less split at the type level — these tests need to construct rows
/// the type wouldn't let a producer build, e.g. an image-less `update`, to
/// exercise the fold's own discriminator rather than the type's).
#[derive(Clone)]
struct RawRow<'a> {
    key: &'a str,
    op: &'a str,
    lsn: Option<u64>,
    old_image: Option<&'a str>,
    new_image: Option<&'a str>,
    origin_lsn: Option<u64>,
    src_changed: bool,
    hop_gen: i32,
    group_key: Option<Vec<&'a str>>,
}

impl<'a> RawRow<'a> {
    fn recompute(key: &'a str, hop_gen: i32) -> Self {
        Self {
            key,
            op: "recompute",
            lsn: None,
            old_image: None,
            new_image: None,
            origin_lsn: None,
            src_changed: false,
            hop_gen,
            group_key: None,
        }
    }
}

async fn insert_row(client: &Client, table: &str, row: &RawRow<'_>) {
    let lsn = row.lsn.map(PgLsn::from);
    let origin_lsn = row.origin_lsn.map(PgLsn::from);
    let src_changed = row.src_changed.then(SystemTime::now);
    client
        .execute(
            &format!(
                "insert into {table} \
                 (src_table, key, op, lsn, old_image, new_image, origin_lsn, src_changed, \
                  hop_gen, group_key) \
                 values ('orders', $1, $2, $3, $4::text::jsonb, $5::text::jsonb, $6, $7, $8, $9)"
            ),
            &[
                &row.key,
                &row.op,
                &lsn,
                &row.old_image,
                &row.new_image,
                &origin_lsn,
                &src_changed,
                &row.hop_gen,
                &row.group_key,
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("insert row {:?} into {table} failed: {e}", row.key));
}

fn find<'a>(folded: &'a [FoldedChange], key: &str) -> &'a FoldedChange {
    folded
        .iter()
        .find(|f| f.key == key)
        .unwrap_or_else(|| panic!("key {key:?} missing from fold output: {folded:?}"))
}

/// Seals the currently-active segment and returns its `seg_seq` — the tests'
/// standard "close the batch" step before folding it.
async fn seal_active_segment(client: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

#[tokio::test]
async fn fold_matches_a_from_scratch_oracle_across_mixed_ops() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // "inserted": a plain insert, one row, one image.
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "inserted",
            op: "insert",
            lsn: Some(100),
            old_image: None,
            new_image: Some(r#"{"v":1}"#),
            origin_lsn: Some(100),
            src_changed: true,
            hop_gen: 0,
            group_key: Some(vec!["g1"]),
        },
    )
    .await;

    // "updated-twice": two updates at increasing lsn — new_image should be
    // the later one, old_image the earlier one's pre-image.
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "updated-twice",
            op: "update",
            lsn: Some(10),
            old_image: Some(r#"{"v":"a"}"#),
            new_image: Some(r#"{"v":"b"}"#),
            origin_lsn: Some(10),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "updated-twice",
            op: "update",
            lsn: Some(20),
            old_image: Some(r#"{"v":"b"}"#),
            new_image: Some(r#"{"v":"c"}"#),
            origin_lsn: Some(20),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;

    // "deleted": a delete carries only old_image.
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "deleted",
            op: "delete",
            lsn: Some(30),
            old_image: Some(r#"{"v":"gone"}"#),
            new_image: None,
            origin_lsn: Some(30),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;

    // "recomputed": a bare recompute trigger from reverse propagation.
    insert_row(&client, "seg_0", &RawRow::recompute("recomputed", 1)).await;

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold");

    let inserted = find(&folded, "inserted");
    assert_eq!(inserted.new_image, Some(r#"{"v": 1}"#.to_string()));
    assert_eq!(inserted.old_image, None);
    assert_eq!(inserted.lsn, Some(PgLsn::from(100)));
    assert_eq!(inserted.origin_lsn, Some(PgLsn::from(100)));
    assert!(inserted.src_changed.is_some());
    assert_eq!(inserted.hop_gen, 0);
    assert_eq!(inserted.group_key, Some(vec!["g1".to_string()]));

    let updated = find(&folded, "updated-twice");
    assert_eq!(updated.new_image, Some(r#"{"v": "c"}"#.to_string()));
    assert_eq!(updated.old_image, Some(r#"{"v": "a"}"#.to_string()));
    assert_eq!(updated.lsn, Some(PgLsn::from(20)));
    assert_eq!(updated.origin_lsn, Some(PgLsn::from(10)));
    // Issue #409: two staged rows fold to one record that still counts both.
    assert_eq!(updated.row_count, 2);
    assert_eq!(inserted.row_count, 1);

    let deleted = find(&folded, "deleted");
    assert_eq!(deleted.new_image, None);
    assert_eq!(deleted.old_image, Some(r#"{"v": "gone"}"#.to_string()));

    let recomputed = find(&folded, "recomputed");
    assert_eq!(recomputed.new_image, None);
    assert_eq!(recomputed.old_image, None);
    assert_eq!(recomputed.lsn, None);
    assert!(recomputed.src_changed.is_none());
    // Not a source change, so hop_gen carries per the non-source rule
    // (pinned to MAX — see fold.rs's doc comment on FoldedChange::hop_gen).
    assert_eq!(recomputed.hop_gen, 1);

    assert_eq!(folded.len(), 4, "exactly one record per key: {folded:?}");
}

/// Issue #133: `group_key`'s real merge rule is a per-key **union** of every
/// raw row's own touched-join-key array — not "pick one row's value" the
/// way `new_image`/`old_image`'s arg-extremes work. Three raw rows touching
/// three distinct join values (with some overlap between consecutive rows,
/// and a fourth, group_key-less row thrown in) must fold to the full
/// deduplicated union of all of them, not just one arbitrary row's array —
/// and, in particular, the union must still carry "3", even though it's
/// long gone from the folded `new_image` by the time this key's history
/// ends at lsn 40 (the exact "fold erases the join key" shape this issue
/// fixes: `staging::apply::RelationshipGenBump` reads this field, not
/// `new_image`, for precisely this reason).
#[tokio::test]
async fn group_key_folds_to_the_real_union_of_every_raw_rows_touched_values() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "repointed",
            op: "insert",
            lsn: Some(10),
            old_image: None,
            new_image: Some(r#"{"post":"3"}"#),
            origin_lsn: Some(10),
            src_changed: true,
            hop_gen: 0,
            group_key: Some(vec!["3"]),
        },
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "repointed",
            op: "update",
            lsn: Some(20),
            old_image: Some(r#"{"post":"3"}"#),
            new_image: Some(r#"{"post":"2"}"#),
            origin_lsn: Some(20),
            src_changed: true,
            hop_gen: 0,
            group_key: Some(vec!["3", "2"]),
        },
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "repointed",
            op: "update",
            lsn: Some(30),
            old_image: Some(r#"{"post":"2"}"#),
            new_image: Some(r#"{"post":"5"}"#),
            origin_lsn: Some(30),
            src_changed: true,
            hop_gen: 0,
            group_key: Some(vec!["2", "5"]),
        },
    )
    .await;
    // A fourth row for the same key with no group_key at all (e.g. a
    // no-op-shaped change on some other column) — must not poison the
    // union with a NULL element or otherwise disturb it.
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "repointed",
            op: "update",
            lsn: Some(40),
            old_image: Some(r#"{"post":"5"}"#),
            new_image: Some(r#"{"post":"5"}"#),
            origin_lsn: Some(40),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold");

    let record = find(&folded, "repointed");
    let mut group_key = record
        .group_key
        .clone()
        .expect("group_key must be populated when any raw row carried one");
    group_key.sort();
    assert_eq!(
        group_key,
        vec!["2".to_string(), "3".to_string(), "5".to_string()],
        "must be the full deduplicated union of every raw row's touched values, \
         not just one row's (arbitrary) value: {folded:?}"
    );
    // The folded image only ever names the last endpoint (5) — exactly the
    // erasure this issue fixes: "3" (and, transiently, "2") are invisible
    // in new_image/old_image but must still survive via group_key.
    assert_eq!(record.new_image, Some(r#"{"post": "5"}"#.to_string()));
    // Issue #409: joining `group_keys` back in must not multiply the group's
    // rows — one per staged row, however many join values each carried.
    assert_eq!(record.row_count, 4);
}

/// A key born inside the batch: an insert immediately followed by an update
/// of the *same key in the same source transaction*, so both rows share one
/// `lsn` and are ordered only by `change_id` — the tie-break #31 exists for.
#[tokio::test]
async fn a_key_born_in_the_batch_folds_old_image_to_null_via_the_change_id_tie_break() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // Same lsn on both rows: one source commit that inserted then updated
    // the same key. Insert order fixes `change_id` order (a shared
    // sequence, assigned at insert time) since nothing else distinguishes
    // them.
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "born-in-batch",
            op: "insert",
            lsn: Some(50),
            old_image: None,
            new_image: Some(r#"{"v":"first"}"#),
            origin_lsn: Some(50),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "born-in-batch",
            op: "update",
            lsn: Some(50),
            old_image: Some(r#"{"v":"first"}"#),
            new_image: Some(r#"{"v":"second"}"#),
            origin_lsn: Some(50),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold");

    let record = find(&folded, "born-in-batch");
    // A naive fold that ordered only by `lsn` (both rows tie) would pick an
    // arbitrary row for "first"/"last" and could easily get old_image wrong
    // (e.g. `{"v":"first"}` instead of NULL). change_id must break the tie:
    // the insert (lower change_id) is FIRST, so old_image is honestly NULL.
    assert_eq!(record.old_image, None);
    assert_eq!(record.new_image, Some(r#"{"v": "second"}"#.to_string()));
}

/// An image-less row must never win either arg-extreme, even when it sorts
/// highest/lowest by lsn — and a key whose rows are *all* image-less still
/// folds to a present record with both images NULL.
#[tokio::test]
async fn image_less_rows_never_win_an_arg_extreme_but_still_produce_a_record() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // "clobber-attempt": one genuine image-bearing insert at lsn 10, plus an
    // image-less row (both images NULL, allowed by the schema for any op,
    // not just 'recompute') at lsn 999 — deliberately the highest lsn in the
    // group, to see whether a naive `ORDER BY lsn DESC` fold picks *it* as
    // "last" and hands back NULL instead of the real image.
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "clobber-attempt",
            op: "insert",
            lsn: Some(10),
            old_image: None,
            new_image: Some(r#"{"v":"real"}"#),
            origin_lsn: Some(10),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "clobber-attempt",
            op: "update",
            lsn: Some(999),
            old_image: None,
            new_image: None,
            origin_lsn: None,
            src_changed: false,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;

    // "all-image-less": every row for this key is image-less.
    insert_row(&client, "seg_0", &RawRow::recompute("all-image-less", 0)).await;
    insert_row(&client, "seg_0", &RawRow::recompute("all-image-less", 0)).await;

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold");

    let clobber = find(&folded, "clobber-attempt");
    assert_eq!(
        clobber.new_image,
        Some(r#"{"v": "real"}"#.to_string()),
        "the image-less row at the higher lsn must not clobber the real image"
    );
    // The image-less row's lsn (999) still advances the watermark: lsn is
    // GREATEST over every row, image-less included.
    assert_eq!(clobber.lsn, Some(PgLsn::from(999)));

    let all_image_less = find(&folded, "all-image-less");
    assert_eq!(all_image_less.new_image, None);
    assert_eq!(all_image_less.old_image, None);
}

/// A real `truncate` sentinel is one instance of this more general
/// property: "image-less but must survive the fold, unfiltered by `op`"
/// holds for *any* op, not just `'recompute'`/`'truncate'` — this test
/// exercises it with a plain image-less `update`, independent of either
/// special op. The truncate-specific behavior (the void filter, `is_truncate`)
/// is exercised directly below.
#[tokio::test]
async fn an_image_less_load_bearing_row_survives_the_fold_unfiltered_by_op() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // This row's op is 'update', not 'recompute' — so a fold that applied
    // the image-bearing discriminator as a row-level WHERE (rather than
    // scoping it inside the arg-extreme aggregates only) would drop this
    // row outright, making the key vanish from the fold's output entirely
    // rather than surviving as a present record with both images NULL.
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "sentinel-shape",
            op: "update",
            lsn: Some(5),
            old_image: None,
            new_image: None,
            origin_lsn: None,
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold");

    let record = find(&folded, "sentinel-shape");
    assert_eq!(record.new_image, None);
    assert_eq!(record.old_image, None);
    assert_eq!(record.lsn, Some(PgLsn::from(5)));
    assert!(record.src_changed.is_some());
}

/// `origin_lsn` LEAST-merges across a restage while `lsn` GREATEST-advances:
/// a key restaged with an *older* origin_lsn keeps that older origin_lsn
/// even as its `lsn` moves to the newest row's.
#[tokio::test]
async fn origin_lsn_least_merges_while_lsn_greatest_advances() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "restaged",
            op: "update",
            lsn: Some(100),
            old_image: Some(r#"{"v":1}"#),
            new_image: Some(r#"{"v":2}"#),
            origin_lsn: Some(50),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;
    // The restage: an older origin_lsn (this key first originated further
    // back than the batch's other row for it knew), but a newer commit lsn.
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "restaged",
            op: "update",
            lsn: Some(200),
            old_image: Some(r#"{"v":2}"#),
            new_image: Some(r#"{"v":3}"#),
            origin_lsn: Some(10),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold");

    let record = find(&folded, "restaged");
    assert_eq!(
        record.origin_lsn,
        Some(PgLsn::from(10)),
        "origin_lsn must LEAST-merge to the older value"
    );
    assert_eq!(
        record.lsn,
        Some(PgLsn::from(200)),
        "lsn must GREATEST-advance to the newest value"
    );
}

/// Issue #321: `min_image_lsn` is the LEAST `lsn` over a key's image-bearing
/// rows only. A recompute row (NULL `lsn`) and an image-less row with a real
/// `lsn` must not pull it down, and a key with no image-bearing row at all
/// folds it to `None`, while `lsn` still GREATEST-advances over every row.
#[tokio::test]
async fn min_image_lsn_is_the_least_lsn_over_image_bearing_rows_only() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let update = |lsn: u64, old: &'static str, new: &'static str| RawRow {
        key: "k",
        op: "update",
        lsn: Some(lsn),
        old_image: Some(old),
        new_image: Some(new),
        origin_lsn: None,
        src_changed: true,
        hop_gen: 0,
        group_key: None,
    };
    insert_row(&client, "seg_0", &update(300, r#"{"v":2}"#, r#"{"v":3}"#)).await;
    insert_row(&client, "seg_0", &update(100, r#"{"v":1}"#, r#"{"v":2}"#)).await;
    insert_row(&client, "seg_0", &RawRow::recompute("k", 0)).await;
    // Image-less but LSN-bearing: below every image-bearing row, so it would
    // win a plain `min(lsn)`.
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            lsn: Some(50),
            old_image: None,
            new_image: None,
            ..update(0, "", "")
        },
    )
    .await;
    insert_row(&client, "seg_0", &RawRow::recompute("bare", 0)).await;

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold");

    let record = find(&folded, "k");
    assert_eq!(record.min_image_lsn, Some(PgLsn::from(100)));
    assert_eq!(record.lsn, Some(PgLsn::from(300)));
    assert_eq!(find(&folded, "bare").min_image_lsn, None);
}

/// A straddler — a row landing in the predecessor slot's half of the fence —
/// folds in correctly, ties this to the both-slots window (not just the
/// fold's own slot). Mirrors `sealing.rs`'s
/// `the_straddler_is_claimed_exactly_once`, but checks the *folded content*,
/// not just presence.
#[tokio::test]
async fn a_straddler_folds_in_correctly_via_the_both_slots_fence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // The straddler: begins before the seal, targets the segment being
    // sealed's own slot (seg_0), and commits after the seal's fence is
    // captured — landing, for good, in a slot its own batch's fence never
    // scans.
    let mut writer = connect_raw(db.dsn()).await;
    let writer_txn = writer.transaction().await.expect("begin writer");
    let straddler_lsn = PgLsn::from(77);
    writer_txn
        .execute(
            "insert into seg_0 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
             values ('orders', 'straddler', 'insert', $1, null, $2::text::jsonb, 0)",
            &[&straddler_lsn, &r#"{"v":"straddled"}"#],
        )
        .await
        .expect("insert straddler");

    let seg1 = seal_active_segment(&mut sealer).await;
    writer_txn.commit().await.expect("commit straddler");
    let seg2 = seal_active_segment(&mut sealer).await;

    let txn1 = sealer.transaction().await.expect("begin fold txn 1");
    let folded1 = fold::fold(&txn1, seg1, BucketFilter::all())
        .await
        .expect("fold batch 1");
    txn1.commit().await.expect("commit fold txn 1");
    let txn2 = sealer.transaction().await.expect("begin fold txn 2");
    let folded2 = fold::fold(&txn2, seg2, BucketFilter::all())
        .await
        .expect("fold batch 2");

    assert!(
        !folded1.iter().any(|f| f.key == "straddler"),
        "batch 1's fence was captured before the straddler committed"
    );
    let record = find(&folded2, "straddler");
    assert_eq!(record.new_image, Some(r#"{"v": "straddled"}"#.to_string()));
    assert_eq!(record.lsn, Some(PgLsn::from(77)));
}

/// Sanity check on the bucket filter itself: every row lands in exactly one
/// bucket of a partition, and `BucketFilter::all()` recovers the whole
/// batch — the "disjoint and complete" property the routing key promises,
/// checked here at the fold layer the same way `sealing.rs` checks it at the
/// fence layer.
#[tokio::test]
async fn bucket_filter_partitions_the_batch_disjointly_and_completely() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    for i in 0..20 {
        insert_row(&client, "seg_0", &RawRow::recompute(&format!("key-{i}"), 0)).await;
    }

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");

    const BUCKET_COUNT: i64 = 4;
    let mut seen = Vec::new();
    for b in 0..BUCKET_COUNT {
        let folded = fold::fold(&txn, seg_seq, BucketFilter::buckets(BUCKET_COUNT, vec![b]))
            .await
            .expect("fold one bucket");
        seen.extend(folded.into_iter().map(|f| f.key));
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 20, "every key must land in exactly one bucket");

    let whole = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold whole batch");
    assert_eq!(whole.len(), 20);
}

/// The truncate-void filter (issue #60): an image-bearing row at or below a
/// truncate sentinel for its own `src_table`, in the same fenced window,
/// must be voided out of the fold entirely — not merely stripped of its
/// images, but absent from the output altogether, since the truncate erased
/// whatever it recorded. A row strictly after the truncate must survive
/// untouched, as must a recompute trigger regardless of where it falls
/// (its `lsn` is NULL, so the void filter's row-comparison against a NULL
/// component is never satisfied — see `fold.rs`'s doc comment on why this is
/// correct: a recompute re-reads live state, so its position relative to the
/// truncate doesn't matter). The sentinel's own group must come back with
/// `is_truncate = true`, exclusively — no other key's group may carry it.
#[tokio::test]
async fn a_truncate_voids_stale_rows_but_not_post_truncate_writes_or_recomputes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // "stale": an insert landing before the truncate — must be voided out of
    // the fold entirely, not just image-stripped.
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "stale",
            op: "insert",
            lsn: Some(10),
            old_image: None,
            new_image: Some(r#"{"v":"before"}"#),
            origin_lsn: Some(10),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;

    // The truncate sentinel, at a higher lsn (and, since rows are inserted
    // in this order, a higher change_id too).
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: TRUNCATE_SENTINEL_KEY,
            op: "truncate",
            lsn: Some(20),
            old_image: None,
            new_image: None,
            origin_lsn: None,
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;

    // "fresh": an insert landing strictly after the truncate — must survive.
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "fresh",
            op: "insert",
            lsn: Some(30),
            old_image: None,
            new_image: Some(r#"{"v":"after"}"#),
            origin_lsn: Some(30),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;

    // "reinserted": written on both sides of the truncate. Only the
    // post-truncate row survives into its group, so only it counts toward
    // `row_count` (issue #409).
    for (lsn, image) in [(12, r#"{"v":"gone"}"#), (32, r#"{"v":"back"}"#)] {
        insert_row(
            &client,
            "seg_0",
            &RawRow {
                key: "reinserted",
                op: "insert",
                lsn: Some(lsn),
                old_image: None,
                new_image: Some(image),
                origin_lsn: Some(lsn),
                src_changed: true,
                hop_gen: 0,
                group_key: None,
            },
        )
        .await;
    }

    // A recompute trigger present in the same window — survives
    // unconditionally, regardless of the truncate.
    insert_row(&client, "seg_0", &RawRow::recompute("recomputed", 1)).await;

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold");

    assert!(
        !folded.iter().any(|f| f.key == "stale"),
        "a row voided by a truncate must be absent from the fold output entirely: {folded:?}"
    );

    let fresh = find(&folded, "fresh");
    assert_eq!(fresh.new_image, Some(r#"{"v": "after"}"#.to_string()));
    assert!(!fresh.is_truncate);

    let recomputed = find(&folded, "recomputed");
    assert_eq!(recomputed.new_image, None);
    assert!(!recomputed.is_truncate);

    let sentinel = find(&folded, TRUNCATE_SENTINEL_KEY);
    assert!(
        sentinel.is_truncate,
        "the sentinel's own group must surface is_truncate"
    );

    let reinserted = find(&folded, "reinserted");
    assert_eq!(reinserted.new_image, Some(r#"{"v": "back"}"#.to_string()));
    assert_eq!(reinserted.old_image, None);

    // Issue #409: a truncate sentinel counts as one staged row, and a row
    // the truncate voided was never applied, so it isn't counted anywhere:
    // not in a group of its own ("stale" has none), and not in a group that
    // survives through a post-truncate write ("reinserted" counts only that
    // write).
    assert_eq!(sentinel.row_count, 1);
    assert_eq!(fresh.row_count, 1);
    assert_eq!(recomputed.row_count, 1);
    assert_eq!(reinserted.row_count, 1);

    assert_eq!(
        folded.len(),
        4,
        "exactly fresh, reinserted, recomputed, and the sentinel itself: {folded:?}"
    );
}

/// The ordering hazard within a single source transaction: an insert and a
/// truncate sharing one commit `lsn` are only distinguishable by
/// `change_id` (intake's append order = execution order). A pre-truncate
/// insert in the same transaction (lower `change_id`) must be voided; a
/// post-truncate insert in the same transaction (higher `change_id`) must
/// survive — `lsn` alone cannot tell these apart.
#[tokio::test]
async fn a_same_transaction_truncate_is_ordered_by_change_id_not_lsn() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // All three rows share lsn 50 — one source transaction. Insertion order
    // fixes change_id order: "pre" first, then the truncate, then "post".
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "pre",
            op: "insert",
            lsn: Some(50),
            old_image: None,
            new_image: Some(r#"{"v":"pre"}"#),
            origin_lsn: Some(50),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: TRUNCATE_SENTINEL_KEY,
            op: "truncate",
            lsn: Some(50),
            old_image: None,
            new_image: None,
            origin_lsn: None,
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &RawRow {
            key: "post",
            op: "insert",
            lsn: Some(50),
            old_image: None,
            new_image: Some(r#"{"v":"post"}"#),
            origin_lsn: Some(50),
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        },
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold");

    assert!(
        !folded.iter().any(|f| f.key == "pre"),
        "a naive lsn-only ordering would tie \"pre\" with the truncate and could \
         let it survive; change_id must break the tie so it doesn't: {folded:?}"
    );
    let post = find(&folded, "post");
    assert_eq!(post.new_image, Some(r#"{"v": "post"}"#.to_string()));
}

/// Issue #392: a `recompute` folded with the key's CDC change leaves the
/// record carrying the change's images, as before, and `has_recompute` keeps
/// the recompute's intent. A key with no `recompute` row doesn't get it.
#[tokio::test]
async fn has_recompute_survives_a_fold_with_the_keys_cdc_change() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let update = |key: &'static str| RawRow {
        key,
        op: "update",
        lsn: Some(100),
        old_image: Some(r#"{"v":5}"#),
        new_image: Some(r#"{"v":6}"#),
        origin_lsn: None,
        src_changed: true,
        hop_gen: 0,
        group_key: None,
    };
    insert_row(&client, "seg_0", &RawRow::recompute("mixed", 0)).await;
    insert_row(&client, "seg_0", &update("mixed")).await;
    insert_row(&client, "seg_0", &update("plain")).await;
    insert_row(&client, "seg_0", &RawRow::recompute("bare", 0)).await;

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold");

    let mixed = find(&folded, "mixed");
    assert_eq!(mixed.old_image, Some(r#"{"v": 5}"#.to_string()));
    assert_eq!(mixed.new_image, Some(r#"{"v": 6}"#.to_string()));
    assert!(mixed.has_recompute);
    assert!(!find(&folded, "plain").has_recompute);
    assert!(find(&folded, "bare").has_recompute);
}

/// Issue #486: a key inserted and deleted in one batch folds to neither
/// image, and `vanished_images` keeps the insert's post-image and the
/// delete's pre-image so the record still names its groups. A key with an
/// image on either side, or with no image-bearing row at all, carries none.
#[tokio::test]
async fn a_key_born_and_died_in_the_batch_keeps_the_images_that_name_its_groups() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let row =
        |key: &'static str, op: &'static str, lsn: u64, old: Option<&'static str>, new| RawRow {
            key,
            op,
            lsn: Some(lsn),
            old_image: old,
            new_image: new,
            origin_lsn: None,
            src_changed: true,
            hop_gen: 0,
            group_key: None,
        };
    // Born into group a, moved to b, died there.
    insert_row(
        &client,
        "seg_0",
        &row("moved", "insert", 10, None, Some(r#"{"g":"a"}"#)),
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &row(
            "moved",
            "update",
            20,
            Some(r#"{"g":"a"}"#),
            Some(r#"{"g":"b"}"#),
        ),
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &row("moved", "delete", 30, Some(r#"{"g":"b"}"#), None),
    )
    .await;
    // Born and died in the same group: one image, not two copies of it.
    insert_row(
        &client,
        "seg_0",
        &row("same", "insert", 10, None, Some(r#"{"g":"z"}"#)),
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &row("same", "delete", 20, Some(r#"{"g":"z"}"#), None),
    )
    .await;
    // A recompute's prior-image hint is not a death image.
    insert_row(
        &client,
        "seg_0",
        &row("same", "recompute", 0, Some(r#"{"g":"hint"}"#), None),
    )
    .await;
    // Inserted then updated: it still has a post-image.
    insert_row(
        &client,
        "seg_0",
        &row("born", "insert", 10, None, Some(r#"{"g":"a"}"#)),
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &row(
            "born",
            "update",
            20,
            Some(r#"{"g":"a"}"#),
            Some(r#"{"g":"c"}"#),
        ),
    )
    .await;
    insert_row(
        &client,
        "seg_0",
        &row("died", "delete", 20, Some(r#"{"g":"d"}"#), None),
    )
    .await;
    insert_row(&client, "seg_0", &RawRow::recompute("bare", 0)).await;

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, BucketFilter::all())
        .await
        .expect("fold");

    let moved = find(&folded, "moved");
    assert_eq!((&moved.old_image, &moved.new_image), (&None, &None));
    let mut vanished = moved.vanished_images.clone();
    vanished.sort();
    assert_eq!(
        vanished,
        vec![r#"{"g": "a"}"#.to_string(), r#"{"g": "b"}"#.to_string()]
    );
    assert_eq!(moved.min_image_lsn, Some(PgLsn::from(10)));

    let same = find(&folded, "same");
    assert_eq!(same.vanished_images, vec![r#"{"g": "z"}"#.to_string()]);
    assert!(same.has_recompute);

    for key in ["born", "died", "bare"] {
        assert!(
            find(&folded, key).vanished_images.is_empty(),
            "{key} must carry no vanished images"
        );
    }
}
