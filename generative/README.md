# `generative`

The generative correctness test suite (design doc:
`docs/generative-test-suite.md`). See that doc for architecture,
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
see `docs/generative-test-suite.md` §6 and `generative/tests/meta.rs`. This
wall-clock heuristic is backed by `run::Coverage`'s floor assertions
(`tests/coverage.rs`) for the fast, DB-free half of "did it actually run" —
this note stays as a smell to notice, not the only check.

## Cost profile and the CI split

The suite's cost is dominated by waiting for the engine to converge, not by
harness overhead: cluster startup, per-case database provisioning, install,
apply, snapshot and the oracle's recompute together are a small fraction of
wall-clock. A quiesce's floor is mostly the staging worker's
`reconcile_interval` (5s), which a new registration waits out before its
build starts. Set `GENERATIVE_QUIESCE_TIMING=1` for per-call quiesce timing
(`ManualBackend::quiesce`: one `QUIESCE_TIMING <ms> ring_ms=<ms>
progress_restarts=<n> ok=<bool>` line on stderr per quiesce) and
`GENERATIVE_COST_TIMING=1` for the cost split by phase.

A quiesce gives up once the ring has made no progress for 30 s, and in any
case after 180 s. Progress is any change to the ring's drain state: a drain
page's cursor or bucket bit, a seal or retire, a 1-1 `JOIN` fan-out step, a
key entering or leaving quarantine. So a slow drain is waited out (the 1-1
`JOIN` tier's racing cases stall each drain page for seconds while it holds
the `segments` row lock, and those stalls run one after another), and a
stuck one still fails 30 s after it stopped. Every quiesce timeout first
prints the database's state to stderr, one `QUIESCE_DUMP` line per row: the
ring, segments, claims, cursors, holdups, drainer heartbeats, definition
status, quarantine, pending fan-outs, and every backend's wait event and
locks.

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
  anywhere in this repo's CI config.

**Failing seeds stay local; failures become pins.** Every property's
`FileFailurePersistence::SourceParallel` config writes a failing case's seed
to `generative/tests/<file>.proptest-regressions` and replays it first on the
next local run. Those files are git-ignored. A seed is only an RNG state, so
it names the same program only while the strategy keeps its shape: a
change to the generator makes every checked-in seed replay an unrelated
program that guards nothing, so none are checked in. A failure worth keeping
is transcribed from its printed `Program` into a hand-built pin, which the
fast lane runs on every push and no generator change can invalidate (see
`tests/concurrent_convergence.rs`'s shrink-trust convention).

**A failure shrinks for at most five minutes.** A case that fails by timing
out pays that timeout again on every shrink step, so each config sets
`max_shrink_time` from `generative::shrink` and reports the smallest case found
by then, with its seed. `PROPTEST_MAX_SHRINK_TIME` (milliseconds, `0` for no
limit) overrides it.

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
