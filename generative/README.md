# `generative`

The generative correctness test suite (design doc:
`docs/generative-test-suite.md`, epic #2). See that doc for architecture,
oracle design, and the property list; this file is the crate-local
operational notes.

## A real full run is minutes, not seconds

If `cargo test -p generative` reports green in about a second, it executed
nothing — check `PROPTEST_CASES` and that the properties actually ran, not
just the meta-tests. Each proptest case round-trips through a real Postgres
cluster (`testkit`), so a healthy run at the design doc §9 case-count band
(12–24 per property) takes real wall-clock time. A suite that reports green
too fast to have done that work is a bug in the harness, not a fast pass —
see `docs/generative-test-suite.md` §6 and `generative/tests/meta.rs`. As of
`local_docs/generative-suite-improvement-plan.md`'s workstream A, this
wall-clock heuristic is backed by `run::Coverage`'s floor assertions
(`tests/coverage.rs`) for the fast, DB-free half of "did it actually run" —
this note stays as a smell to notice, not the only check.

## Current cost profile, and the CI split that exists today

At the design doc's default case count (16), a full `cargo test -p generative
--test convergence` run takes **~130–290s**, and that variance is understood,
not mysterious: per-op timing instrumentation (`GENERATIVE_QUIESCE_TIMING=1`,
`ManualBackend::quiesce`) across 10 runs (894 samples) found a clean bimodal
distribution — ~77% of quiesce calls resolve under 2s, and a distinct ~22%
cluster lands at 8.2–10.2s, matching the ~10s pipeline stall independently
suspected in `local_docs/transit-comparison.md` §3.3
(`SealConfig::age_gate`). A second pass (`GENERATIVE_COST_TIMING=1`,
covering cluster startup, per-case database provisioning, install, apply,
snapshot, and the oracle's recompute) found those phases collectively
account for **under 3% of wall-clock** — so the suite's cost is, almost
entirely, that stall, not test-harness overhead. This is a real engine bug,
not a suite problem, and is out of scope for this crate to fix; see the git
history for `generative/src/backend/manual.rs` (`ManualBackend::quiesce`) for
the full measurement writeup.

Because paying that cost 9 times over on every push/PR would be expensive,
the crate's 9 proptest properties are all named with a `property_` prefix
(enforced by a self-check in `tests/meta.rs` that scans every `proptest! {
... }` block in `tests/*.rs` and fails the build if any `#[test] fn` inside
one lacks the prefix — a future property added without it breaks CI rather
than silently skipping the split below). That prefix is the actual fast/deep
split, wired into two workflows:

- **`.github/workflows/ci.yml`'s "Test generative (fast lane)" step (every
  push/PR):** `cargo test -p generative -- --skip property_` — skips all 9
  properties at once, by prefix, and runs everything else in the crate (the
  lib, `coverage`, `meta`, `oracle`, `backend_seam`, `backfill`, and every
  hand-built pin/regression test) at each target's own default case count.
  This is the ~22s-and-under path described above; the DB-backed pins can
  still individually hit the quiesce stall, just far less often than a
  16-case property's ~80 ops.
- **`.github/workflows/nightly.yml` (scheduled, 06:00 UTC daily, plus
  `workflow_dispatch`):** deep-runs exactly one property,
  `property_convergence_holds_for_trivial_programs`, at `PROPTEST_CASES=200`
  — `cargo test -p generative --test convergence
  property_convergence_holds_for_trivial_programs`. This is the crate's most
  exercised property (see the design doc's property list) and the only one
  given an in-repo deep run today; the other 8 are not deep-run anywhere in
  this repo's CI config. The property's
  `FileFailurePersistence::SourceParallel` config (see
  `tests/convergence.rs`'s `proptest_config`) already writes any failing case
  to `generative/tests/convergence.proptest-regressions` and replays it first
  on the next run — commit that file if a deep run ever produces one, so the
  failure becomes a real, replayable regression the fast lane picks up too.

**Case-count calibration across all 9 properties is not this repo's job.** A
separate, out-of-repo nightly (run by the maintainer, not part of any
workflow file here) covers that; don't go looking for it in
`.github/workflows/` — it isn't there.

The "minutes, not seconds" heuristic above describes the *deep* lane (and a
manual `cargo test -p generative` with no `--skip`), not the PR-path fast
lane, which is designed to be fast.
