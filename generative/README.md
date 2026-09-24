# `generative`

The generative correctness test suite (design doc:
`docs/generative-test-suite.md`, epic #2). See that doc for architecture,
oracle design, and the property list; this file is the crate-local
operational notes.

## A real full run is minutes, not seconds

Every proptest property in this crate is `#[ignore]`d, so a plain `cargo
test -p generative` (or `cargo test --workspace`) runs only the fast,
non-property tests — see the CI split below. To actually deep-run the
properties, opt in with `-- --ignored` (properties only) or
`-- --include-ignored` (everything). If such an opted-in run reports green in
about a second, it executed nothing — check `PROPTEST_CASES` and that the
properties actually ran, not just the meta-tests. Each proptest case round-trips through a real Postgres
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
cluster lands at 8.2–10.2s. A second pass (`GENERATIVE_COST_TIMING=1`,
covering cluster startup, per-case database provisioning, install, apply,
snapshot, and the oracle's recompute) found those phases collectively
account for **under 3% of wall-clock** — so the suite's cost was, almost
entirely, that stall, not test-harness overhead; see the git history for
`generative/src/backend/manual.rs` (`ManualBackend::quiesce`) for the full
measurement writeup.

That stall was an engine bug, issue #452, not the seal age gate
(`SealConfig::age_gate`) that `local_docs/transit-comparison.md` §3.3
suspected: a convergence wait whose token landed past the last decoded
change waited for intake's keepalive-driven persist, throttled to once per
10s. The waiter now asks intake to confirm through its token with a logical
message, and the fast-lane binaries' serial time dropped from 385s to 174s
(fifteen binaries, measured 2026-09-24). The figures above predate that fix.
What's left of a quiesce's floor is mostly the backfill discharge's
`reconcile_interval` (5s), which a new registration waits out before its
backfill starts.

Because paying that cost for every property on every push/PR would be
expensive — and because a contributor's (or agent's) first reflex is a plain
`cargo test --workspace` — the properties are **opt-in**. Every `fn` inside a
`proptest! { ... }` block carries
`` #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"] ``
and a `property_` name prefix, both enforced by a self-check in
`tests/meta.rs` that scans every `proptest!` block in `tests/*.rs` and fails
the build if any `fn` inside one lacks either (so a new property can't
silently start running on every push). That attribute is the fast/deep split,
wired into two workflows:

- **`.github/workflows/ci.yml`'s "Test" step (every push/PR):** a plain
  `cargo test --workspace`. For this crate that skips every property (they're
  ignored) and runs everything else (the lib, `coverage`, `meta`, `oracle`,
  `backend_seam`, `backfill`, and every hand-built pin/regression test) at
  each target's own default case count. This is the ~22s-and-under path
  described above; the DB-backed pins can still individually hit the quiesce
  stall, just far less often than a 16-case property's ~80 ops.
- **`.github/workflows/nightly.yml` (scheduled, 06:00 UTC daily, plus
  `workflow_dispatch`):** deep-runs exactly one property,
  `property_convergence_holds_for_trivial_programs`, at `PROPTEST_CASES=200`
  — `cargo test -p generative --test convergence
  property_convergence_holds_for_trivial_programs -- --ignored`. This is the
  crate's most exercised property (see the design doc's property list) and
  the only one given an in-repo deep run today; the others are not deep-run
  anywhere in this repo's CI config. The property's
  `FileFailurePersistence::SourceParallel` config (see
  `tests/convergence.rs`'s `proptest_config`) already writes any failing case
  to `generative/tests/convergence.proptest-regressions` and replays it first
  on the next run — commit that file if a deep run ever produces one, so the
  failure becomes a real, replayable regression. Note that the fast lane does
  *not* replay it (the property is ignored there); a regression worth pinning
  on every push should also get a hand-minimized pin test.

Useful local invocations:

```sh
cargo test --workspace                                   # fast: no properties
cargo test -p generative -- --ignored                    # every property, default 16 cases
PROPTEST_CASES=4 cargo test -p generative --test noise -- --ignored   # one file, quick smoke
cargo test -p generative --test convergence -- --include-ignored     # a file's properties + its other tests
```

**Case-count calibration across all the properties is not this repo's job.** A
separate, out-of-repo nightly (run by the maintainer, not part of any
workflow file here) covers that; don't go looking for it in
`.github/workflows/` — it isn't there.

The "minutes, not seconds" heuristic above describes the *deep* lane (any
run with `--ignored`/`--include-ignored`), not the default fast lane, which is
designed to be fast.
