//! CI harness for issue #102/#94's settled-state validation model: see
//! `trellis/tests/spikes/issue-102-settled-state-v2.sql` for the model
//! itself, and `trellis/tests/spikes/README.md` for the design catalog it
//! implements. Ported from `spike/issue-102-validation-v2` per issue #138
//! part 1.
//!
//! Epic #127's Phase 1 (the settled-parent projection plus its four
//! correctness guards, shipped in PR #162) rests its correctness argument
//! entirely on this differential/mutation-test model: a self-contained
//! PL/pgSQL simulation of the staging ring (segments, buckets, out-of-order
//! drain), a confirmed-LSN watermark, parent INSERT/DELETE, from-side FK
//! re-point, and NULL groups, fuzzed against several candidate designs and
//! diffed against a plain SQL oracle. Before this file, that argument only
//! existed as a hand-run `psql -f` + `issue-102-campaign.sh` artifact on a
//! spike branch -- this wraps it in `cargo test` so it can't silently bit-rot.
//!
//! Two lanes, mirroring the fast/deep split `generative/README.md` documents
//! for the same cost problem (a full campaign is too expensive to pay on
//! every PR) but had never actually been wired into any CI workflow:
//!
//! * [`campaign_fast_lane_confirms_the_shipped_design_and_negative_controls`]
//!   runs under the default `cargo test` (so on every PR): a cheap run count
//!   just confirming the model still detects corruption at all (D0/D1/D3, the
//!   known-unsound negative controls) and that the shipped design (D5) still
//!   shows none.
//! * [`campaign_deep_lane_confirms_every_guard_is_load_bearing`] is
//!   `#[ignore]`d by default and runs the full 8-design, 3000-run-per-design
//!   campaign -- adding the four guard-ablation variants (D5-a..D5-d) to the
//!   fast lane's four. 3000 runs is not an arbitrary "more is better" choice:
//!   per the model's own header and `trellis/tests/spikes/README.md`, guard
//!   (d)'s necessity (the per-parent reverse-ordering check) only becomes
//!   visible at 3000 runs -- it showed zero extra corruption at 200-1500 runs
//!   in the original campaign. Asserting D5-d's ablation at a cheaper case
//!   count would not "fail safe": it would silently stop testing what it
//!   claims to test. Wired into the nightly deep lane in
//!   `.github/workflows/nightly.yml`.
//!
//! Both lanes also assert the model's own harness self-check --
//! `HARNESS_VIOLATION_unstaged_below_watermark` must be exactly 0 for every
//! design, every time. A nonzero count there means the *model's* watermark
//! bookkeeping is broken, not the design under test (this exact bug once
//! produced a spurious D5 failure, fixed by the clamp in the model's
//! `intake()`); it fails with a distinctly worded assertion so it can never
//! be mistaken for an actual corrupted-run failure.
//!
//! The run count for both lanes is overridable via `SPIKE_102_CAMPAIGN_RUNS`
//! (mirroring the generative crate's own `PROPTEST_CASES` convention), so a
//! developer can crank either lane up or down while iterating without
//! editing this file.

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};

/// The self-contained model: creates its own `m` schema, so it never
/// conflicts with `trellis`'s own migrated schema in the same isolated test
/// database.
const MODEL_SQL: &str = include_str!("spikes/issue-102-settled-state-v2.sql");

/// Ops per run, and the campaign's base seed -- both fixed to match the
/// hand-run campaign's own values (`issue-102-campaign.sh`), so results stay
/// comparable across the ported harness and the original artifact.
const OPS_PER_RUN: i32 = 50;
const BASE_SEED: i32 = 900_000;

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn load_model(client: &Client) {
    client
        .batch_execute(MODEL_SQL)
        .await
        .expect("load issue-102-settled-state-v2.sql");
}

fn campaign_runs(default: i32) -> i32 {
    std::env::var("SPIKE_102_CAMPAIGN_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct FuzzResult {
    corrupted: i32,
    worst: String,
}

/// `reset_world` (called once per fuzz run) deletes every row from every
/// model table and repopulates it. Measured empirically: calling
/// `fuzz(design, runs, ...)` in one shot makes per-run cost grow
/// *superlinearly* with `runs` (800 runs measured ~7x the cost of 300, not
/// ~2.7x) -- the dead tuples that `DELETE` leaves behind accumulate across a
/// campaign, and the short-lived test process never lives long enough for
/// autovacuum's default naptime to reclaim any of it. Left unchecked, this
/// alone would make the deep lane's 3000-run campaign take hours rather than
/// minutes -- the exact cost problem issue #138 asks this harness to solve,
/// not reintroduce.
///
/// The fix is chunking: call `fuzz()` repeatedly at [`CHUNK_SIZE`] runs per
/// call, `VACUUM`ing the model's tables between chunks so table size (and
/// per-run cost) stays flat instead of climbing across the whole campaign.
/// This is purely a performance change, not a behavioral one: `fuzz`'s
/// internal seed for run `r` is `seed0 + r`, so calling it in back-to-back
/// chunks with `seed0` advanced by each chunk's size reproduces exactly the
/// same sequence of per-run seeds a single `fuzz(design, runs, ...)` call
/// would have used.
const CHUNK_SIZE: i32 = 100;

async fn run_campaign(client: &Client, design: &str, runs: i32) -> FuzzResult {
    client
        .batch_execute("set search_path to m, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'; delete from stats;")
        .await
        .expect("reset stats before campaign");

    let mut done = 0;
    let mut corrupted = 0;
    let mut worst: Option<String> = None;
    while done < runs {
        let chunk = CHUNK_SIZE.min(runs - done);
        let row = client
            .query_one(
                "select corrupted, worst from fuzz($1, $2, $3, $4)",
                &[&design, &chunk, &OPS_PER_RUN, &(BASE_SEED + done)],
            )
            .await
            .unwrap_or_else(|e| panic!("fuzz('{design}', {chunk}, ...) failed: {e}"));
        corrupted += row.get::<_, i32>(0);
        let chunk_worst: String = row.get(1);
        if worst.is_none() && chunk_worst != "-" {
            worst = Some(chunk_worst);
        }
        done += chunk;

        // VACUUM can't run inside a transaction block, so it must be its own
        // single-statement message rather than share a `batch_execute` with
        // anything else (a multi-statement simple-query message is
        // implicitly one transaction).
        client
            .batch_execute(
                "vacuum chg, rev, rev_hold, rev_hold_rows, drained, segs, proj, attr, tgt, src_posts, src_post_tags",
            )
            .await
            .expect("vacuum model tables between chunks");
    }
    FuzzResult {
        corrupted,
        worst: worst.unwrap_or_else(|| "-".to_string()),
    }
}

async fn harness_violations(client: &Client) -> i64 {
    client
        .query_opt(
            "select v from stats where name = 'HARNESS_VIOLATION_unstaged_below_watermark'",
            &[],
        )
        .await
        .expect("read HARNESS_VIOLATION_unstaged_below_watermark")
        .map(|r| r.get(0))
        .unwrap_or(0)
}

/// Runs one design's campaign and asserts both the harness self-check and
/// the expected corruption outcome.
///
/// `expect_corruption = true` means "known-unsound" (a negative control or a
/// guard ablation) -- corrupted must be > 0, or the model has stopped being
/// able to detect the very unsoundness it exists to catch. `false` means the
/// shipped design -- corrupted must be exactly 0.
async fn assert_design(client: &Client, design: &str, runs: i32, expect_corruption: bool) {
    let result = run_campaign(client, design, runs).await;

    let violations = harness_violations(client).await;
    assert_eq!(
        violations, 0,
        "HARNESS_VIOLATION_unstaged_below_watermark = {violations} while campaigning design \
         '{design}' at {runs} runs -- this means issue-102-settled-state-v2.sql's own watermark \
         bookkeeping is broken (the confirmed-LSN clamp in its intake()), NOT that design \
         '{design}' is unsound. Fix the model before trusting any corruption count from this run."
    );

    if expect_corruption {
        assert!(
            result.corrupted > 0,
            "design '{design}' showed ZERO corrupted runs out of {runs} -- this design is a \
             known-unsound negative control or guard ablation. If it now shows zero, the \
             validation model itself has regressed (stopped detecting corruption), not that \
             '{design}' became sound. (worst diff seen: {})",
            result.worst
        );
    } else {
        assert_eq!(
            result.corrupted, 0,
            "design '{design}' (the shipped settled-parent-projection design, epic #127 Phase \
             1 / PR #162) showed {} corrupted run(s) out of {runs}: {}",
            result.corrupted, result.worst
        );
    }
}

/// Fast lane: runs on every `cargo test`. Cheap enough to pay on every PR
/// (a few hundred runs per design, four designs), while still giving real
/// signal: the negative controls (D0/D1/D3) must still show corruption --
/// proof the model can detect anything at all -- and the shipped design (D5)
/// must still show none.
///
/// Deliberately does NOT include the guard-ablation designs (D5-a..D5-d):
/// see the module docs and
/// [`campaign_deep_lane_confirms_every_guard_is_load_bearing`] for why those
/// need the full 3000-run count to mean anything.
#[tokio::test]
async fn campaign_fast_lane_confirms_the_shipped_design_and_negative_controls() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect(db.dsn()).await;
    load_model(&client).await;

    let runs = campaign_runs(200);

    for design in ["D0", "D1", "D3"] {
        assert_design(&client, design, runs, true).await;
    }
    assert_design(&client, "D5", runs, false).await;
}

/// Deep lane: the full 8-design, 3000-run-per-design campaign
/// (`issue-102-campaign.sh`'s own numbers). `#[ignore]`d so it never runs
/// under a plain `cargo test`; run explicitly with `--ignored`, or via the
/// scheduled `.github/workflows/nightly.yml` job.
///
/// This is the only lane that exercises the guard ablations (D5-a..D5-d):
/// per the epic's own measurements, guard (d)'s necessity is invisible below
/// 3000 runs, so asserting it at the fast lane's case count would silently
/// test nothing.
#[tokio::test]
#[ignore = "3000-run x 8-design campaign; run via nightly.yml or explicitly with --ignored"]
async fn campaign_deep_lane_confirms_every_guard_is_load_bearing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect(db.dsn()).await;
    load_model(&client).await;

    let runs = campaign_runs(3000);

    for design in ["D0", "D1", "D3"] {
        assert_design(&client, design, runs, true).await;
    }
    assert_design(&client, "D5", runs, false).await;
    for design in ["D5-a", "D5-b", "D5-c", "D5-d"] {
        assert_design(&client, design, runs, true).await;
    }
}
