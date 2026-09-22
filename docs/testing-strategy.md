# Testing Strategy

Trellis makes one hard promise (`docs/data-flow.md#correctness`): *once caught up
to a given LSN, each incrementally-maintained target is exactly equal to a full
recompute of its definition against the source data at that LSN, for any
interleaving of source changes.* Everything below exists to make that
promise — and the liveness, safety, and resource behavior around it — falsifiable.

Each tier is defined by its **trigger** (what makes it run a case), its **judge**
(what decides pass/fail), and its **reproducibility contract** (can a failure be
replayed deterministically). Tiers are ordered cheapest-first. The governing rule:
**push every check to the lowest tier that can hold it** — a race found by chaos
must be driven down into a deterministic pin; a tier never re-litigates what a
lower one already guarantees.

| Tier | Trigger | Judge | Reproducible? | Cadence |
|---|---|---|---|---|
| 1. Compiler & type system | every build | `rustc` accepts/rejects | fully | every edit / CI |
| 2. Linters & format | every build | `clippy`/`rustfmt` | fully | every edit / CI |
| 3. Unit tests | scripted | assertion | fully | every PR / CI |
| 4. Integration tests | scripted, real Postgres | assertion | fully | every PR / CI |
| 5. Generative tests | random valid program, seeded | independent oracle at quiescence | at program level (shrinking) | TBD |
| 6. Chaos tests | wall-clock random workload + faults | invariants over recorded history | **no** | TBD |

The tier 5↔6 line is the one that most needs stating, so it gets its own section (§6).

---

## 1. Compiler & type system

**Make illegal states unrepresentable, so whole bug classes never reach a test.**
`rustc` + `edition = 2024` across the workspace enforce memory safety, exhaustive
`match`, ownership/lifetime discipline, and `Result` propagation for free. We spend
the type system on:

- **Newtypes over primitives** for values that must not be confused — LSNs, primary
  keys, column identifiers, exact-decimal `numeric`. Distinct types are a
  zero-runtime-cost correctness check.
- **Enums for closed vocabularies** (op kinds, outcome classes, AST node kinds) so a
  new variant forces every `match` to be revisited.
- **`#[non_exhaustive]` / sealed traits** where a public shape must stay evolvable.

It cannot check runtime values, Postgres semantics, or timing. Everything below
assumes the code compiles.

## 2. Linters & format

**One non-negotiable house style plus curated correctness lints, enforced in CI so
review never spends attention on them.** A merge is blocked on:

- `cargo fmt --all -- --check` — formatting settled by the tool, never by review.
- `cargo clippy --all-targets --all-features -- -D warnings` — **every warning is a
  hard error.** Catches mechanical correctness smells (needless clones, bad
  comparison chains, fallible-conversion foot-guns, `.unwrap()` where it should
  propagate).

CI installs current stable Rust with `rustfmt`/`clippy` via the vendored
`.github/actions/rust-toolchain` composite action (org policy allows only repo-local
or allowlisted actions). Style and lint only; no claim about behavior.

## 3. Unit tests

**Pure logic in isolation, no Postgres** — fast, deterministic, run on every
`cargo test`. These own:

- The AST and parser — accept/reject, precedence, error messages.
- The `numeric` exact-decimal type — arithmetic, scale, comparison-by-value.
- The AST→`SELECT` printer (#36) as pure string production.
- Dependency-order resolution, batch/fold bookkeeping, outcome classification —
  anything expressible as *value in, expected value out*.

The cheapest place to pin a specific computed answer, and the home of any
generative/chaos failure that root-causes to a pure function.

## 4. Integration tests

**Real behavior against a real Postgres, scripted end-to-end.** These live in
`trellis/tests/*` and lean on `testkit`, which owns a disposable cluster
(`initdb`/`postgres`/`pg_ctl` on a private socket, `wal_level=logical`, torn down on
`Drop`); CI provisions the server binaries. They own:

- The logical-replication ingestion path end-to-end: source DML → pgoutput decode →
  staging → tuple-marker and LSN-confirmation validation.
- **Scripted interleavings** — hand-authored concurrency/crash scenarios that must
  always hold: `testkit::crash::CrashGuard` (SIGKILL a child mid-drain),
  `crash::OpenTransaction` (a straddling transaction across a batch boundary).
- Any behavior that needs a database but is a *fixed* case, not a generated one.

**This tier is the terminal home of every race bug found above it** — a
generative/chaos finding is not fixed until it exists here (or in tier 3) as a
deterministic pin that fails before the fix and passes after. Generative and
integration deliberately **share one action vocabulary**, so a scripted scenario is
just a generated program with its draws pinned.

## 5. Generative tests

**The combinatorial correctness claim: byte-identical convergence to an independent
oracle over random valid programs, with failures shrunk to a minimal reproducing
program.** This is the `generative` crate; full architecture lives in
`docs/generative-test-suite.md`, build plan in the tracking epic
(salesforce-misc/trellis#2). In brief:

- **Trigger:** a randomly generated *valid* program — schema, transform definitions,
  and a sequence of source mutations — driven through the real engine against a
  `testkit` cluster.
- **Judge — Postgres itself is the oracle.** Because the calculation grammar is
  committed (ADR-0004) to an immutable subset of PostgreSQL operators/functions, the
  oracle renders each definition back to a `SELECT` (per-row projection for 1-1,
  `GROUP BY` for aggregates), runs it in the same cluster, and asserts the persisted
  target equals it. It shares no evaluation code with the engine, so it catches even
  a bug in the engine's own evaluator. The evaluator-driven `recompute` is retained
  as a secondary parity check.
- **Reproducibility:** at the *program* level via shrinking; seeds replay the
  program, and findings terminate as hand-minimized pins in tiers 3–4.
- **Properties it lights up as engine stages land:** per-op convergence,
  operation-error handling, idempotency (at-least-once treated as exactly-once),
  order-insensitivity over commuting ops, read-your-own-writes via `await`, and — the
  hardest — non-idempotent aggregate delta convergence.

Its properties are opt-in: each is `#[ignore]`d, so PR-time CI (plain
`cargo test --workspace`) runs only the crate's fast non-property tests, and deep
runs opt in with `-- --ignored` plus a `PROPTEST_CASES` override (a healthy deep
run is minutes, not seconds — a "pass" in one second executed nothing). It
assumes the system *reaches* quiescence and judges *what the answer is* there; it
structurally cannot judge timing, liveness, or long-horizon resource behavior — tier 6.

## 6. Chaos tests

**Everything that survives *without* quiescence and *without* reproducibility.**
Chaos is the black-box, wall-clock tier: continuous randomized workload plus
continuous operational faults against a running system, judged not by an oracle
snapshot but by **invariants over recorded history.** Non-deterministic by design,
it can never block a merge.

### The dividing line: quiescence + reproducibility

**If a bug can be caught by "program + deterministic fault placement → quiesce →
compare to the oracle," and replayed from a seed, it belongs to tier 5 (or a pin in
tier 4). Chaos owns only what that mold cannot hold.**

**What chaos must NOT cover — generative already owns it:**

- **Converged-state correctness / oracle equality** — generative's entire purpose.
  Chaos cannot quiesce on demand or shrink, so it cannot reliably assert
  byte-identical equality and should not try.
- **Per-op convergence, idempotency, order-insensitivity, read-your-own-writes** —
  all seeded, reproducible, oracle-judged; re-rolling them under wall-clock
  randomness adds flake, not coverage.
- **Fine-grained race exploration at *known* sites** — tier 5 places named failpoints
  deterministically at those sites and judges at quiescence. Chaos re-rolling them at
  random adds noise, not signal.

Chaos does not re-verify *what the answer is*; it assumes the lower tiers pin that,
and borrows the oracle once (below) to keep them honest.

**What chaos MUST cover — generative cannot:**

- **Emergent real-time races** — the interleavings you didn't know to place a
  failpoint at; only genuine wall-clock concurrency surfaces them.
- **Liveness and progress** — convergence is still *reached*, and within a bound,
  under sustained disruption: no deadlock, livelock, permanent stall, or unbounded
  lag runaway.
- **Long-horizon degradation and resource safety** — leaks and slow bleed visible
  only over hours: memory growth, connection/file-handle leaks, slot lag, WAL
  retention, disk consumption.
- **Operational faults not modeled as oracle identities** — Postgres restart/failover
  under live load, network partition/latency, disk-full, OOM killer, clock skew,
  pool exhaustion. (SIGKILL is *shared* — `CrashGuard` gives tiers 4–5 a
  deterministic version — but *sustained, compound, randomly-timed* failure is chaos
  alone.)
- **Compound fault overlap too large to enumerate or seed** — several faults in the
  same window, which no finite seed set covers.

### Architectural requirements

Alongside the generative harness, the chaos tier needs:

- **A continuous workload generator** — a randomized stream of source mutations under
  real load, *reusing the generative `model`/`generate` vocabulary* so findings
  translate into seeded programs.
- **A wall-clock fault scheduler** — coarse operational faults on random real-time
  timing, at operational rather than code-site granularity.
- **A history recorder** — since you can't quiesce-and-compare, record observable
  history (LSN/watermark progression, periodic target snapshots, worker lifecycle,
  lag/resource metrics) so invariants are checked *post hoc* over the timeline.
- **Invariant checkers over history:**
  - *Safety* (every moment): no LSN/watermark regression; no target value that never
    corresponded to any real source state (no torn/phantom reads); no observable
    double-apply; monotonic progress.
  - *Liveness* (eventually): once faults quiesce, convergence within a bounded time;
    steady-state lag bounded under load; no permanent stall.
  - *Resource* (bounded over the run): slot lag, WAL, memory, connections don't grow
    without bound.
- **A quiet-window convergence gate** — periodically stop the workload, let it settle,
  run the generative **oracle once** as ground truth. The single place chaos reaches
  back to tier 5, bridging "invariants held throughout" to "the answer is correct."
- **Non-reproducibility as a first-class output contract** — a chaos run *discovers* a
  bug, it doesn't *terminate* one. Every finding must be mined into a deterministic
  tier-4 pin or seeded tier-5 program before it counts as fixed; a seed that
  "reproduces" a race isn't trusted, because it doesn't survive timing changes.
  Chaos is lead generation for the reproducible tiers.
- **Isolation and cadence** — its own environment, bounded blast radius, nightly /
  pre-release schedule, never a per-commit gate.

---

## How the tiers compose

The tiers form a ratchet; when a bug is found, work flows one direction:

1. Chaos (6) discovers an emergent failure over real time.
2. It is reproduced as a seeded generative program (5), or — if not combinatorial —
   a scripted scenario (4).
3. The minimal case is pinned as an integration test (4) or, if it reduces to pure
   logic, a unit test (3).
4. Where possible, the shape is made unrepresentable (1) or caught by a lint (2).

The higher a fix lands, the cheaper and more permanent it is. Every upper tier's
design intent is to *shrink its own surface* by feeding durable pins downward — so
that over time the expensive, non-deterministic tiers exercise genuinely new ground,
not what the fast tiers already guard.
