# The Generative Test Suite

How Trellis proves the claim in its README — *"once caught up to a given LSN,
each incrementally-maintained target is exactly equal to a full recompute of its
definition against the source data at that LSN, for any interleaving of source
changes"* (`docs/data-flow.md#correctness`).

Hand-written examples cannot cover a combinatorial space (definition shapes ×
value domains × op sequences × drain interleavings × concurrency), and each
encodes the same assumptions the implementation does. The suite instead
**generates random valid programs** — a schema, transform definitions, and a
sequence of source mutations — drives each through the real engine against a real
Postgres (`testkit`), compares the settled derived state against an **independent
recompute oracle**, and shrinks any failure to a minimal reproducing program.

---

## 1. Architecture

The suite is the `generative` crate — a workspace member, so `cargo test -p
generative` (and CI's `clippy --all-targets --all-features`) compile it and a
signature change can't rot it silently. It depends on `trellis` and `testkit`
(the latter a normal dependency, not just a dev-dependency, since
`SubprocessBackend` needs `CrashGuard` from the library itself).

Three of its features stay off by default and are switched on only by the
crate's own self dev-dependency, so none can reach a production
`cargo build --workspace`:

- `engine-access` (ADR-0012) enables `trellis/test-util`, which is what makes
  `trellis::dev` — the curated, uncommitted door onto the engine — exist at
  all. Every module in `src/lib.rs` is gated on it, because they pervasively
  name engine types; a plain `cargo build --workspace` therefore compiles this
  crate's lib to nothing. That is deliberate: a non-dev dependency edge
  carrying `trellis/test-util` would unify into `cli`'s production binary.
- `proptest` — the generator strategies.
- `subprocess-backend` — the `engine_subprocess` bin and the crash backend,
  which also needs `trellis/test-util`'s Phase 3 pre-commit pause hook.

`testkit` already provides:

- **A disposable cluster** — `TestCluster` runs its own `postgres` on a private
  socket, started from a copy of a migrated template data directory and torn
  down on `Drop`. Unlocks the whole
  database-administration action space (restart, checkpoint).
- **Process lifecycle** — `crash::CrashGuard` supervises a child it can `SIGKILL`,
  so "kill a worker mid-drain" is a real code path.
- **A straddler primitive** — `crash::OpenTransaction` holds a transaction open
  across a concurrent operation.

The suite's modules are strictly separated:

```
model/      plain-data description of a generated program. Names no engine internals.
generate/   generators producing only valid programs from a Config. Makes no engine calls.
oracle/     independent recompute. Uses only the shared components named in §2.
backend/    the ONLY module that drives the engine's pipeline and reads back.
run/        drives a program through the backend, asserts the properties.
tests/      property declarations, hand-built pins, meta-tests.
```

The seam is a structural guarantee: the oracle *cannot* cheat by calling
maintenance code if that code is only reachable through `backend/`, and a second
backend drops in without touching generators or oracle. Two exist:
`ManualBackend` (an in-process `trellis::Client`) and `SubprocessBackend`
(a real OS child process `CrashGuard` can genuinely `SIGKILL`, for
crash tests that an in-process drop-and-replace cannot honestly stand in for).
They share all DDL/DML/read-back rendering via `backend/sql.rs`; the only thing
that actually differs between them is how the engine is started and stopped.

### The model

A `Program` is plain, `Debug`-legible data — what the shrinker minimizes and what
prints on failure:

```
Program {
  tables:      [ { name, pk_col, columns: [{name, type}] } ]
  defs:        [ TransformDef ]         // reuses trellis::defs::ast, see §2
  ops:         [ Insert{table,row} | Update{table,pk,changes} | Delete{table,pk} ]
}
```

Use small fixed name pools (`t0`, `d0`, `c0`): a shrunk counterexample you can
read at a glance beats one that is technically smaller. Give every table its PK
column **unconditionally**, so no shrink step can strand a definition referencing
it.

`defs` reuses `trellis::defs::ast::TransformDef` directly — 1-1 definitions
(including numeric `+`), `GROUP BY` aggregates, and two-table `JOIN … GROUP BY`
definitions (the JOIN tier). Non-DML actions (§7) slot in beside the DML ops.

### The backend seam

One module drives the engine, and its job is narrow:

- create source tables; install transform definitions and their target tables;
- apply each op as **raw source DML** — never an application-level notification
  API, because the production path is trigger capture of source writes
  (`docs/data-flow.md#capture-by-triggers`);
- `quiesce()` — block until the engine catches up, defined exactly as a *client's*
  read-your-own-writes check: watermark the LSN after the last commit, `await`
  until derived state converges past it. Not a `sleep`. Over a
  partitioned source it first waits for the reconcile pass to record the tree
  and for each correction it queued to finish, since partition maintenance
  reaches a target only through a correction.
- `snapshot()` — read back merged source+derived state as a deterministic,
  **ordered** map (table → pk → column), so diffs and equality are stable.

## 2. The oracle is Postgres itself

Per ADR-0004, the calculation grammar is an *immutable subset of PostgreSQL's
operators and functions* — every accepted expression has identical semantics to
the same expression in Postgres. So the oracle *is Postgres*: render a
definition's formulas (and predicate, and grain) back to a `SELECT` against the
source, run it in the same cluster, and assert the persisted target equals it.

- **1-1:** `SELECT pk, (price + tax) AS total FROM orders` → compare row-by-row.
- **Aggregate:** `SELECT grain, sum(measure) FROM src GROUP BY grain` → compare.
- **Join aggregate:** the definition's own `SELECT keys, aggregates FROM a [LEFT]
  JOIN b ON … GROUP BY keys` over the two tables → compare byte for byte.

Postgres evaluates every field of the engine's targets (ADR-0002), so a
divergence is a pipeline / fold / exactly-once-apply / ordering / convergence
bug, or a rendering bug in the engine's compiler. This oracle shares **no
rendering code** with the engine — only the parser AST and a small AST→`SELECT`
printer — so a rendering bug in the engine cannot agree with it. It catches
every class of bug and is fast at volume. It can't outgrow the oracle: the grammar
is *committed* to a Postgres subset, so a construct with no Postgres equivalent is
out of scope by definition.

### Refuse to guess on unmodeled shapes

Meeting a definition shape the generator isn't supposed to produce, the oracle
**panics with a message naming the missing work** — never a best-effort value. A
silent fallback means the day the generator widens, someone gets a flood of false
differentials that look like an engine bug.

## 3. Generate valid programs, not garbage

Filtering random garbage down to valid programs is slow, biases the distribution
invisibly, and breaks shrinking. We generate within the rules:

- **Seed before mutate:** inserts with fixed small PKs first, so updates and
  deletes have rows to hit.
- **Definitions reference only columns that exist**, built from the declared schema.
- **Bounded value domains, with the numeric-path pairing written down beside the
  domain as a load-bearing invariant.** A sum over small integers matches the
  engine's exact path; widen the domain and the oracle overflows into a different
  numeric path while the column type is unchanged. Both sides settle on Trellis's
  `numeric.rs` exact-decimal type; domains keep them on the same path.
- **Deliberately generate the awkward values:** nulls in every nullable column,
  empty strings (distinct from NULL), the literal text `"NULL"`, strings
  containing any key-encoding delimiter.

Two rules protect the suite from lying to itself:

- **An install rejection is a hard failure, never a skip.** If generators emit
  only valid programs, a definition-time rejection is a generator or engine bug —
  a `_ => skip` arm silently stops testing whole shapes.
- **Generator coverage meta-tests** (no database): every scalar type appears both
  as a column and via a derivation; every operator appears over every supported
  argument type. Coverage that silently drops out is otherwise invisible.

### Generated expressions

A derived field is not a fixed shape. `generate::DerivedShape` is a random
well-typed expression drawn from `generative::expr_gen`, so the end-to-end
tiers (capture, apply, target equals the source query) run the same
expressions the conformance properties check against Postgres.

The generator grows trees from rules read off the registry
(`trellis::dev::defs::registry`), not from a list of its own: for every
operator, each operand-type pair `operator_result_type` admits; for every
function, its declared signature. Adding an entry to `OPERATORS` or
`FUNCTIONS` therefore adds it to every property and tier that draws
expressions, with no change to the generator. What the registry cannot say is
written by hand, each with a test that fails when it falls behind:

- `COALESCE`, which is variadic and so outside the registry.
- The literal pools. Every `TYPED_LITERALS` keyword needs canonical samples in
  `typed_literal_samples`. `TRUE`, `FALSE` and `CAST(NULL AS <type>)` are
  drawn as leaves of their types, and the conformance properties also draw
  a bare `NULL` as either operand of an operator that takes one type on both
  sides (the end-to-end tiers don't: their oracle types an operator from its
  operands' own types).
- A function argument that must be a literal (`LITERAL_ONLY_ARGS`, today
  `REGEXP_COUNT`'s pattern).
- The unadmitted constructs a near miss names (`UNADMITTED_OPERATORS`,
  `UNADMITTED_FUNCTIONS`). A line the registry later admits stops being a near
  miss on its own.
- The float against a bare number literal, which define admits although the
  registry refuses the float/exact mix.

A `Profile` sets the literal magnitude and nesting depth. The conformance
properties draw boundary literals (`NaN`, `±Infinity`, range edges, numbers
wider than any type) to depth 3. The end-to-end tiers draw small literals to
depth 2, so a program the engine accepts also runs: Postgres computes every
field, and one that raised for some row would quarantine it and keep the
program from converging.

#### Expression conformance

`tests/expr_conformance.rs` checks the generated expressions against a real
Postgres, over a throwaway table of random rows (`NULL`, extremes, `NaN`,
`±Infinity`, infinite dates), with four properties:

1. **Type.** The column define creates has the `format_type` of `CREATE TABLE
   AS SELECT <the user's text>`.
2. **Value.** `SELECT <the engine's rendered SQL>` and `SELECT <the user's
   text>` agree on every row: equal text, or the same SQLSTATE class. Both
   renderers are checked: the build's (`render_expr_sql`) and the one a 1-1
   apply and a `JOIN` write embed (`render_qualified_expr_sql`).
3. **Round trip.** Parsing the text gives the tree it was written from (in the
   minimal-parentheses and the fully parenthesized spelling), and
   `parse(render(parse(text))) == parse(text)`.
4. **Refusal.** Every near miss is refused at define, and the error names the
   construct. The near misses are the wrong-typed operand of each operator and
   function (a call no overload of the name accepts), prefix `-` over a
   non-numeric operand, a `CAST` the registry does not admit and a target it has
   no spelling for, a constant zero divisor, a chained comparison, a simple
   `CASE`, an `IS TRUE`-style test, each word Postgres reserves used as a
   column (the live server also refuses each as an unquoted column name), a
   `NULL` define can't type (both operands of an arithmetic operator, the
   operand of prefix `-` or `AT TIME ZONE`, an operand beside a type no
   operator takes on both sides, any function argument), a `CASE` whose condition or results have
   the wrong type, a `NULLIF` whose first argument Postgres casts, a
   literal-only or integer-literal-only argument given a column, relative or
   non-canonical typed literals, and the operators and functions the registry
   does not admit (including the `STABLE` and `VOLATILE` ones). The date
   constructs add their own: a field or unit the type lacks, the `STABLE`
   overloads Postgres picks silently (`DATE_TRUNC` of a `date`, `EXTRACT` of a
   `timestamptz`, `CAST(tz AS timestamp)`), a zone that is not `'UTC'`, an
   interval-producing `-`, and a constant date past `date`'s range.

Postgres folds constants while it plans, so an expression it rejects for every
row (`2147483647 + 1`, `1.5 / 0`) is refused at define by design. A constant
zero divisor is refused whatever the dividend, so for a column dividend (which
Postgres plans without error) the checker accepts the refusal when no row of
the table computes a value, or when a `COALESCE`, `CASE`, `AND` or `OR` may
keep the division from running. Inside those, define follows the planner: it
refuses a constant in a `CASE` result whatever the condition, and nothing
after a constant that ends the `CASE`, `AND`, `OR` or `COALESCE` (the
constants-only property reaches those folds, including `NULL` ones from
`NULLIF`). The Value property
treats "refused at define, and Postgres raises class 22 on a `WHERE false`
probe" as agreement, and flags both a refusal where Postgres plans the
expression without error (a false refusal) and an acceptance where Postgres
raises while planning it (a missed refusal).

A gap that is open (define admits what Postgres rejects) is tolerated in one
named place in `generative::expr_conformance`, reported as a known gap, and
pinned by a test that fails the day define refuses it, which is when the
tolerance goes. The tolerance covers the gap and nothing beside it: the
checker rewrites the gap away (a pattern Postgres rejects becomes `'a'`, a
function call over constants, innermost first, becomes the literal Postgres
computes for it) and the rewritten expression must pass every property.

Deterministic tests in the same file pin each precedence tier (every pair of
registry operators, generically) and each refusal by name, and check every
rule one level deep over every leaf of its types (the table's column and
each literal in the pool), so a renderer's cast for one literal or one
refusal's boundary is reached on every run, not by luck. `TRUE`, `FALSE` and
a typed `NULL` go into every operand position of every rule one at a time,
and a bare `NULL` into every context that types it. They run in the
normal suite, with a small-case-count run of each property from a fixed seed.
The randomized properties are `#[ignore]`d and draw fresh cases each run:
`PROPTEST_CASES=2000 cargo test -p generative --test expr_conformance --
--ignored`. A failure prints the expression's text; add it to
`hard_cases_agree_with_postgres` to replay it. A property over constants only
(`property_constant_expressions_conform_to_postgres`) is where the plan-time
range refusals are reached.

#### Adding an operator or function

1. Add the entry to `OPERATORS` or `FUNCTIONS` with its precedence and types
   (a function with several signatures is several entries of one name; an
   argument that must be an integer literal is listed in
   `INTEGER_LITERAL_ARGS`; one that must be a string literal is in
   `expr_gen::LITERAL_ONLY_ARGS`, and an operator whose right side is a given
   literal, as `AT TIME ZONE`'s `'UTC'`, in `LITERAL_RHS_OPERATORS`). Every
   operand-type pair `operator_result_type`
   admits is drawn from then on, as are prefix `-` (`negate_result_type`) and
   each `CAST` (`cast_result_type`, `CAST_TARGETS`).
   The forms the registry cannot express (`NOT`, `IS [NOT] NULL`, `CASE`,
   `NULLIF`, `EXTRACT(<field> FROM x)`) are `Rule` variants in `expr_gen` with a spelling in `write_text`
   that keeps the tree through a re-parse (an operand that starts with `-`
   after `NOT` is parenthesized).
2. Run the conformance properties with a large `PROPTEST_CASES`. A failure
   names the property, the text and the row or type that differs.
3. If define refuses a combination Postgres accepts on purpose (the float/exact
   comparison is one), teach the generator the admitted spelling next to
   `Rule::FloatVsLiteral` and the near-miss list the refused one.
4. If the operator makes a new constant fold fail at plan time, make define
   refuse it and let the Value property confirm Postgres raises on the probe.
5. Teach the end-to-end oracle (`oracle::render_expr`, `field_value_type`) the
   operator's rendering and typing, and add a literal pool for any new
   typed-literal keyword.

## 4. The properties

Each property is a test under `generative/tests/`. The properties are
`#[ignore]`d deep-lane tests named `property_*` (§9); the hand-built pins
beside them run on every `cargo test`.

- **Convergence, per op** (`tests/convergence.rs`). After each op: `quiesce`,
  assert materialized state equals the oracle. Per-op checking localizes
  shrinking to the *first* diverging op. Cost is a full round trip per op, so
  case counts stay low.
- **Operation errors are checked, not swallowed.** A rejected op (update of a
  never-inserted row) is applied to the oracle too, and state must still match — an
  op that errors changed nothing.
- **Repeated application** (`tests/repeated_apply.rs`). The same statement run
  twice is real, distinct source traffic (the engine's apply is exactly-once by
  construction, so a redelivered change can't be simulated through raw DML);
  both runs converge to the oracle. The exactly-once claim itself is pinned by
  deterministic interleaving tests under `trellis/tests/`.
- **Order-insensitivity over commuting ops** (`tests/order_insensitivity.rs`).
  Reordering ops on *distinct* `(table, pk)` targets must converge identically.
- **Read-your-own-writes** (`tests/read_your_own_writes.rs`). Trellis ships
  `await(LSN, timeout)` as its freshness promise: every convergence check takes a
  token after the op, awaits it, and asserts the visible derived state includes
  every effect of ops 1..k. A negative control shows a read without the await can
  see stale state, so `quiesce` is doing real work.
- **Bulk operations** (`tests/bulk_operations.rs`). `TRUNCATE` and bulk inserts
  with a shrinkable row count.
- **Mid-stream definition install** (`tests/mid_stream_definition_install.rs`).
  A definition installed after some ops have run builds its target from the rows
  the source already holds and then follows the stream.
- **Backfill** (`tests/backfill.rs`). A definition installed over a populated
  source builds exactly those rows; no change data capture stands in for the
  build.
- **Engine lifecycle** (`tests/client_lifecycle.rs`). A client torn down and
  replaced mid-stream, and an extra worker-only client started alongside the
  first, both converge.
- **Crash during Phase 3** (`tests/subprocess_crash.rs`). A real `SIGKILL` of the
  engine process inside a drain batch's open transaction; the re-drain redoes the
  batch and the target converges.
- **Database administration and backup/restore** (`tests/db_admin.rs`,
  `tests/backup_restore.rs`; §7).
- **Untracked-table noise and checkpoints** (`tests/noise.rs`). Writes and DDL on
  tables Trellis doesn't track, and `CHECKPOINT`s, interleaved with the program
  never change the converged state.
- **Out-of-band tampering** (`tests/self_check_cross_check.rs`). Direct writes to
  derived tables are found by `self_check`, whose SQL renderer is independently
  authored from the oracle's; the suite asserts the two agree on every generated
  definition.
- **Two instances** (`tests/two_instance_noise.rs`). Two Trellis instances in one
  database (or in two), each converging against its own oracle, including one
  instance reading the other's target.
- **Concurrent convergence** (`tests/concurrent_convergence.rs`). The same
  programs and oracle against the real worker pool and multi-worker claiming, in
  tiers: small programs; hot keys and groups taking bursts from several
  connections; mid-burst re-reads, pauses, resumes and installs while change
  capture for the same keys is in flight; cooling keys whose last write lands
  mid-burst; and sweep-only steady-load and cooling-backfill tiers that stall
  pages and build chunks at their entry-lock step (the stall is process-wide, so
  they run only in the planted-bug sweep, §6).
- **Non-idempotent delta convergence** (the aggregate slice of the above).
  Byte-identical convergence to a from-scratch `GROUP BY` oracle under aggregate
  delta maintenance. Grain domains stay *tiny* (`0`, `1`, `2`, plus occasional
  `NULL` — see `generative::generate::strategy::grain_value`'s doc comment) so many
  rows share a group and deletes really kill groups. A `NULL` grouping value is
  drawn unconditionally. The suite doesn't generate a *chained* definition (one
  `TRANSFORM` reading another's target) except in the two-instance property;
  front-door coverage of chains lives in
  `trellis/tests/defs_aggregate_chained_single_column_group_key.rs`.
- **Join convergence** (`tests/join_convergence.rs`). Two
  tables, one to three `LEFT` or `INNER` join aggregates over them (joined on one
  column or on two, grouped by the joined table's column, by a column of each
  table, or by the `FROM` table's alone, with `COUNT`/`SUM`/`AVG` on either side,
  an expression argument and composed fields), and inserts, updates and deletes
  on both tables with `NULL` keys, groups and values now and then. The join keys
  spread over a dozen values, land mostly on one hot key, fan out over three
  keys that both tables keep writing, or spread so wide (64 keys) that two
  workers' pages of one batch change a key from opposite sides without sharing
  a partial cell, so only the per-key join lock orders them. The ops are issued
  as the concurrent tier issues them (bursts split over lanes, several drain
  workers, a short seal cadence), so both sides' writes race into the same
  batches and onto the same keys; every burst is checked once it settles, and
  every definition must still be `live` at the end. A definition installs up
  front or part way through the stream, so most `JOIN`s build over tables that
  hold rows or are still being written; later bursts pause and resume a
  definition (the resume rebuilds its target in place) and re-read either
  table (`request_backfill`) while the lanes keep writing. The per-burst check
  compares each target with the definition's own query, so it covers the build's
  outcome. In a quarter of the cases the run first settles the up-front
  definitions over the empty tables. The coverage report counts the builds that
  fire over rows left by earlier bursts, so a run shows that they happen, and
  reads the engine's build-rows counter around each case to show that its
  builds wrote rows. About
  one op in a hundred is a `TRUNCATE` of either table. The tables are
  unpartitioned and no definition reads another's target. Run it with `cargo
  test -p generative --test join_convergence -- --ignored`. Its planted-bug
  sweep, `join_planted_bugs_are_caught`, gates the `JOIN` plants and the
  group-basis writer's plants that a `JOIN`'s `P` and `M` cells share (§6).
  Half its cases are the `racing` shape (40 keys, 500 to 900 ops in bursts of
  600 to 900 paced from the first op, definitions live before the first write,
  short seals, every page stalled before its commit for up to 2 to 3.5 s), where
  pages of one key's opposite sides are open at once with no shared cell, so
  only the `J` and `T` locks order them. A `racing` case takes about 15 s, so a
  sweep process of 48 cases takes about 12 minutes. `join_no_j_lock` and
  `join_t_skip_lock` are known misses here: the `racing` shape catches each in
  1 or 2 of 48 cases, and whether a case lines the race up depends on timing,
  so a sweep would miss either one too often to gate it. `gb_early_zero_gc` is
  a known miss too: it needs a zero group's rebuild stamp past the drained
  prefix when the GC pass runs, and the GC runs every `housekeeping_interval`
  (5 s by default, which the harness doesn't set). Their deterministic pins
  gate all three. `gb_lsn_not_visibility` is not drawn here, as its writers are
  single statements.
- **Partitioned-source convergence** *(ADR-0017)*. The first table of a
  generated program is a partitioned source: `HASH` over one to four leaves,
  `LIST` with a leaf per grain value or fewer and a `DEFAULT`, or `RANGE` over
  one to three bounds, partitioned by the grain column, which every
  definition reading it groups by. Its primary key is `(pk, grain)`, so its
  grain is a `bigint` and a `NULL` grain is written as `0`. The other tables
  and definitions draw as usual. It runs serially
  (`property_convergence_holds_over_a_partitioned_source` in
  `tests/convergence.rs`) and under the concurrent backend
  (`property_a_partitioned_source_converges_under_the_concurrent_backend`).
  The coverage report's `partition_strategies` line counts the strategies
  drawn. On top of its seeds and mutates, the partitioned table draws
  partition maintenance (a leaf detached, also `CONCURRENTLY`, attached
  again with its rows, dropped or truncated, and a new leaf created empty,
  written directly, or built outside the tree and attached with rows, in
  place of a dropped leaf when every bound is held),
  partition-key updates that move a row to another partition (or are
  refused, to a value no leaf holds), and writes named on a leaf, or on a
  detached table, instead of the root. Each op's expected outcome comes from
  a model of which leaf holds each `(pk, grain)` row, so a revival of a
  moved row, a write into a detached bound, or an attach the `DEFAULT`'s
  rows forbid is expected to succeed or fail as Postgres decides. Every
  check waits for the reconcile pass to record the tree and for every
  correction it queued to finish, so the oracle compares only settled
  states, after each op serially and after each burst concurrently. A burst
  may change the tree several times, so one pass sees them all, except that
  over a source that may have a `DEFAULT` it never both brings a leaf in
  (created or attached) and removes it: that is the short-lived-leaf gap in
  [known correctness gaps](known-correctness-gaps.md#a-short-lived-leaf-beside-a-default-partition).
  The `partition_ops` line counts each kind drawn. The hot-key tier has no
  partitioned variant
  ([#1331](https://github.com/salesforce-misc/trellis/issues/1331)).

### Two runtimes, one oracle

The same programs run in two modes against the same oracle:

- **Manual:** harness-driven, one worker, lockstep apply → quiesce → compare.
  Deterministic; source of clean shrinks and reproducible seeds.
- **Concurrent:** the real worker pool and multi-worker claiming, quiescence
  as the client `await` check. Exercises wakeup, parallel claiming, and await
  semantics.

Identical shape means a divergence in only one mode localizes immediately to
concurrency.

## 5. Comparison semantics: exactness is a per-type decision

- **Exact equality** for integers, text, dates, timestamps, UUIDs, booleans.
- **Exact decimals compared by value, ignoring scale** (`2.50 ≡ 2.5`). Byte-level
  rendering is pinned in a separate test.
- **Floats within a combined absolute+relative tolerance** (~1e-9), NaN ≡ NaN. (Not
  reachable until the value language gains floats.)
- **A column present on one side and absent on the other is a divergence,** not a
  missing-key skip — this is how an unwritten derived cell surfaces.

On failure, emit one line per differing cell
(`table[pk].column: expected=… got=…`), present/missing lines for rows and tables,
**and the whole `Program`**. A diff without the program is unactionable.

## 6. What keeps a green run meaningful

**A green run must prove it ran.** `testkit` provisions its own cluster, so the
classic "misconfigured cluster skips, every skip counts as pass" trap is mostly
designed out — but we still enforce the shape:

- Split the outcome into `Ran` / `Unavailable` (nothing to provision — genuinely
  inconclusive, never green) / `BackendUnusable` (provisioned but couldn't stand
  up — never a pass, carries the error text). One `outcome.as_pass()` decision
  point every test asserts through.
- A **meta-test** that fails fast if the harness can't stand up a cluster it should
  have been able to.
- **A README rule of thumb:** a real full run is minutes, not seconds. A suite that
  "passed" in one second executed nothing.
- **Print the resolved connection target**; refuse to run against a database the
  run did not name.

**Findings terminate as deterministic pins; generative runs are lead generation.**
A seed reproduces the *program*, not reliably a *race*, and does not survive
generator refactors. Three artifact kinds:

- **Saved seeds**, replayed first on the next local run — cheap exact-regression
  catching while the generator keeps its shape. Kept local, never checked in (§9).
- **Hand-built minimized programs** in the model's types — durable pins that double
  as documentation, immune to generator refactors.
- **A control test beside each finding** — a case that must *converge* — proving the
  harness can tell green from red.

### The baseline bar: no unknown failures

The planted-bug sweep (`generative::plant_sweep`; `planted_bugs_are_caught` in
`generative/tests/concurrent_convergence.rs` for the concurrent tier,
`join_planted_bugs_are_caught` in `generative/tests/join_convergence.rs` for the
`JOIN` tier) runs a tier's seeded cases once unplanted, as the baseline, then
once per planted bug, and judges each plant only on cases the baseline passed.
**No unknown failure is accepted in a generative case**: the baseline passes
only with **zero failures outside the quarantine list**,
`generative/baseline-quarantine.txt`. The rule is the same for every tier
(`cooling_key`, `hot_key`, `mid_burst`, `steady_load`, `cooling_backfill`,
`join`).

- **Each list entry** is one line, `<tier> <seed>:<case> #<issue> <what fails>`.
  The case numbering is the sweep's, as `GENERATIVE_PLANT_ONLY=<seed>:<case>` takes
  it, and the issue is an **open** issue that pins that case's failure. An
  entry covers the whole case: while it's listed, any failure in that case is
  left out, not only the one its issue pins.
- **A failure that isn't listed fails the baseline.** Triage it before the
  baseline counts: either file it as a bug with its pinned case (check first
  whether it also fails without your change, and whether an issue already
  covers it) and list it, or fix the harness defect behind it. A failure is
  never waved away as a flake. A listed case is left out of every plant's
  count, whether it failed this run or not.
- **The list can't go stale.** The sweep looks up every listed issue before it
  runs anything, and fails if one is closed: the fix has landed, so the entry
  goes (or the issue was closed early and the case still fails, so it reopens).
  The lookup goes through `gh api` and is tried again after a network blip; a
  sweep that still can't make it doesn't run, and neither does one whose entry
  names a pull request or an issue that doesn't exist. After the baseline, the
  sweep reports a listed case that passed every time it ran. A race can pass
  now and then, so that is a prompt to look, not a failure.
- **A tier's strategy change redraws its cases**, so the same `<seed>:<case>`
  names a different program. The list isn't a regression pin (those are
  hand-built, above), so re-sweep the tier and redo its entries.

The bar is a zero with a list, not a failure share: a share lets a real bug fail
a fixed fraction of cases through routine sweeps, while a strict zero with no
list would let a known, filed failure block every unrelated change's sweep.

To run the baseline alone, against the bar:

```text
TMPDIR=/dev/shm GENERATIVE_PLANTS=none GENERATIVE_PLANT_TIER=steady_load \
GENERATIVE_PLANT_SEEDS=8 cargo test -p generative --test concurrent_convergence \
    planted_bugs_are_caught -- --ignored --nocapture
```

`GENERATIVE_PLANTS=all` runs the baseline and then the tier's plant gate. The
tiers are calibrated for a tmpfs cluster, so run sweeps there. A process run
directly with `GENERATIVE_PLANT_CHILD=1` (as a repro loop does) is one of the
sweep's workers, not a gate: it prints a `plant-sweep` row per case and exits
0 even when a case fails, with no list or bar applied, so read its rows.

## 7. The action space, and how it grows

Every candidate action falls in one of three buckets by its effect on the oracle:

- **Bucket 1 — oracle identities:** restart Postgres, kill/scale workers,
  checkpoint, `VACUUM`, DML/DDL on *untracked* tables, session-setting
  changes. None change the correct converged state, so the oracle needs **no
  changes** — the action drops into the op stream, then quiesce and compare.
- **Bucket 2 — cheap for a recompute oracle to model:** define/redefine/remove a
  transform mid-stream, change a grain. One line on the definition list. Unlocks
  define-then-load ≡ load-then-define.
- **Bucket 3 — source data/shape changes:** source DDL, `TRUNCATE`, restore. The
  source effect is easy; the real work is modeling the engine's *policy* when a
  definition's dependencies change under it — Trellis's quarantine and pause
  policy. The oracle models only the settled cases.

The actions, cheapest-first (every one below is built):

1. **Untracked-object noise** (bucket 1) — nearly free; converged state must be
   unchanged by DML/DDL on tables Trellis doesn't track.
2. **Definition lifecycle** (bucket 2) — unlocks define-then-load ≡ load-then-define.
3. **Engine lifecycle** (bucket 1) — kill/restart/scale workers; where resume and
   idempotent re-application get tested.
4. **Transaction shapes** (bucket 3) — a transaction held open across a batch
   boundary (via `OpenTransaction`).
5. **Database administration** (bucket 1) — restart PG, checkpoint:
   `model::DbAdminAction`, driven by `run::run_convergence_with_db_admin`,
   swept by `tests/db_admin.rs`. A restart or checkpoint fires with the anchor
   op's change still in flight and the engine must reconnect by itself.
   Backup/restore is a cold file-level copy (`model::RestorePlan`, driven by
   `run::run_convergence_with_restore`, swept by `tests/backup_restore.rs`):
   back up right after op *k* with its change in flight, run to the end,
   restore into a fresh cluster, replay *k+1..n*, converge with no pause (§8
   has the decision behind that). The `pg_basebackup` variant is not
   generated ([#1422](https://github.com/salesforce-misc/trellis/issues/1422)).
6. **Out-of-band tampering** (bucket 1, detection) — direct writes to derived
   tables, found by `self_check` (`tests/self_check_cross_check.rs`).

Two structural notes for when faults enter the stream:

- **Bulk operations are their own code paths.** `COPY`, `INSERT…SELECT`, one huge
  transaction, mass grain migration, and especially **`TRUNCATE` (its own
  capture trigger, not a bulk delete)** each reach batching paths row-at-a-time
  DML never does. Model bulk as a generated dimension with a
  **shrinkable row-count**.
- **Keep shrinking alive under faults.** Fault *placement* is deterministic (named
  failpoints where races live); the *schedule* is not. Seeds replay the placement;
  the oracle judges converged state at quiescence, which is schedule-independent.

## 8. Four layers, one action vocabulary

| Layer | Trigger | Judged by | Reproducible? | Cadence |
|---|---|---|---|---|
| 1. Deterministic (unit, integration, scripted interleaving pins) | scripted | assertion | fully | every run |
| 2. Generative, healthy path | random program, seeded | recompute oracle at quiescence | at program level (shrinking) | every run |
| 3. Generative + fault actions | random program incl. faults, seeded | same oracle at quiescence | at program level (shrinking) | minutes–hours |
| 4. Black-box chaos | wall-clock random | invariants over recorded history | no | nightly / pre-release |

- **Layer 1** is `trellis/tests/*` and the `testkit` crash/straddler
  pins. Every race bug found by layers 2–4 terminates as a layer-1 pin here.
- **Layer 2** is the healthy-path properties of §4.
- **Layer 3** is reachable only because a fault action is an oracle identity (§7)
  and because the generative and scripted harnesses **share one action vocabulary**:
  a scripted scenario is a generated program with its draws pinned.
- **A restore needs no restore-specific handling.** Trellis keeps its catalog, staging ring, capture triggers and
  targets in the database it reads, so a restore rolls them all back to one
  point and what comes back is consistent with itself. Layer 3's
  backup/restore action checks that assumption: the cold-copy case
  (restore, replay, converge, no pause) is generated; the `pg_basebackup`
  case is not ([#1422](https://github.com/salesforce-misc/trellis/issues/1422)). One thing a
  server stop does carry over is a claim a worker took just
  before it: the claim's worker can't run the job or give it back, so only
  the stale-claim sweep frees it, a reclaim TTL later. The restore and
  restart runs give the engine a 10 s TTL so that recovery fits well inside
  the 30 s quiesce.

## 9. Operational shape

- Reuse one cluster across cases, with an isolated database per case. Serialize shared-cluster access through a process-wide lock.
- Dial case counts down (12–24 per property, env override for deep runs); give each
  property its own count by cost; give shrinking a generous iteration cap.
- Persist failing seeds locally, replayed first on the next run, but never check
  them in: a seed only names the same program while the strategy keeps its shape,
  so a checked-in seed goes stale on the next generator change. A failure
  worth keeping becomes a hand-built pin. (The baseline quarantine list in §6
  does name cases by seed, but only to exclude known, filed failures from a
  sweep's baseline. The sweep fails on an entry whose issue is closed and
  reports one whose case passed, but it can't tell that a generator change
  has redrawn a listed case, so that change redoes the tier's entries.)
- **Planted-bug sweeps and their baseline.** The sweeps (`generative::plant_sweep`:
  the concurrent tier's `planted_bugs_are_caught`, the `JOIN` tier's
  `join_planted_bugs_are_caught`) re-run generated cases with a deliberate bug
  planted (`trellis/src/plant.rs`) and check the tier catches it, against an
  unplanted baseline that must have zero failures outside the quarantine list
  (§6). A plant in the tier's `known_misses` is run and reported but doesn't fail
  the sweep when uncaught, and one in `not_gated` (with its reason) is left out
  of `GENERATIVE_PLANTS=all`. Each plant breaks one apply path (the group basis,
  a `JOIN`, a 1-1 target, a 1-1 `JOIN`), and a tier leaves out the plants of
  paths its programs never take: the concurrent tier draws no `JOIN`, the
  `JOIN` tier nothing else, and no tier draws a 1-1 `JOIN`. `early_tombstone_gc` and `chunk_without_entry_lock` are left to
  their deterministic pins too: on a 1-1 target no tier's schedule makes either
  diverge (each tier's `not_gated` says what was measured). Each group-basis,
  1-1, `JOIN` and 1-1 `JOIN` plant is pinned by a deterministic interleaving
  test that fails with it armed, named in its doc comment.
- Document how to run one property alone on a clean database; every property
  bootstraps what it reads, so it never depends on run order.

## 10. Settled decisions

- **Property-testing framework.** `proptest` supplies the typed generator
  strategies and the shrinking.
- **Concurrent-runtime shrink trust.** A concurrent-mode shrink is advisory:
  when a failure reproduces under the manual runtime, re-shrink it there.
- **The oracle and erroring rows.** The Postgres-SQL oracle must model
  quarantine once erroring operations exist (a row that makes a formula error is
  kept out of the target, whereas one `SELECT` over the source aborts on it): it
  computes per row and excludes the erroring rows, or renders error to `NULL`.
