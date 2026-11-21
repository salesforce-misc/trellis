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
  database-administration action space (restart, checkpoint, freeze, xid
  aging). `TestCluster::age_xids_to_epoch_edge` ages its transaction ids in
  about a second: it freezes every database and moves the next id with
  `pg_resetwal -x`, in as many jumps as wraparound protection allows.
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
  tables:      [ { name, pk_col, columns: [{name, type}], source_key } ]
  defs:        [ TransformDef ]         // reuses trellis::defs::ast, see §2
  ops:         [ Insert{table,row} | Update{table,pk,changes} | Delete{table,pk} ]
}
```

Use small fixed name pools (`t0`, `d0`, `c0`): a shrunk counterexample you can
read at a glance beats one that is technically smaller. Give every table its PK
column **unconditionally**, so no shrink step can strand a definition referencing
it. A table read through a `KEY (...)` (`source_key`, ADR-0026) keeps `pk_col` as
the column ops address rows by, as its primary key or a plain `NOT NULL UNIQUE`
column.

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
  reaches a target only through a correction. It gives up once the ring has
  made no progress for 30 s (180 s at most), printing the database's state
  first.
- `snapshot()` — read back merged source+derived state as a deterministic,
  **ordered** map (table → pk → column), so diffs and equality are stable.

### Roles

A case runs as a deployment does
([recommendations — Roles and permissions](recommendations.md#roles-and-permissions)),
so an assumption only a superuser satisfies fails the run.
`backend::CaseDatabase` provisions the database and the backends'
`connect_as` takes its `backend::Roles`:

- `trellis_runner`, not superuser, no `BYPASSRLS`, holding `CREATE` on the
  database and membership in `app_owner`, runs the migrations and every
  Trellis connection, the oracle's pool and the harness's catalog reads
  included;
- `app_owner` creates the sources in `public`, so it owns them, and makes
  every write;
- the bootstrap superuser runs only `CHECKPOINT` and `VACUUM FREEZE`
  (`execute_admin`), the cluster-wide xid aging, and the privilege tier's
  grants and owner changes (§7).

Targets go in `public` or in `trellis_targets`, which `trellis_runner` owns,
drawn from the program's digest at the first install.

Every convergence property runs this way, the self-check tampering
properties (their tampering writes as the Trellis role, which owns the
targets) and the subprocess crash pins too; `tests/backend_seam.rs` pins the
owners and each session's role. `connect` runs everything as the superuser.
Hand-built pins outside a property's runner still use it.

`two_instance_noise.rs` runs both instances as these roles in one database.
Each instance's sources are `app_owner`'s, in a schema of its own (`app_a`,
`app_b`) that its definitions name, so same-named sources stay apart; the
shared-source and chain shapes keep theirs in `public`.

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
- **Float and `interval` sums** are the one exception. Postgres's `sum()` over
  them depends on scan order, and the engine reads the exact sum instead
  ([ADR-0002](decisions/0002-async-data-flow.md#float-and-interval-sums-are-exact)).
  So the `SELECT` gathers each group's argument values, and the oracle
  (`oracle::exact_sums`) sums them exactly in Rust: a float rounded once, an
  `interval` field by field, its `AVG` through `interval_div`'s steps. A group
  whose sum is out of range is a raise, which no target value equals. Only a
  top-level `SUM`/`AVG` is modelled; a nested one over these types panics.

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
  containing any key-encoding delimiter, and the numeric `NaN` (a
  `numeric(p,s)` column admits it, where `±Infinity` is refused, so it is the
  only special numeric value drawn for the columns `SUM`/`AVG` read).

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
depth 2 and divide only by `NULLIF(divisor, 0)`, so a drawn expression never
raises for a row. Erroring rows are drawn on purpose instead, by the held-key
tier (§10, "The oracle and erroring rows").

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
   no spelling for, a literal zero divisor, a chained comparison, a simple
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
   `timestamptz`, `CAST(tz AS timestamp)`), a zone that is not `'UTC'`, and an
   interval-producing `-`.

Postgres folds constants while it plans, and define evaluates none of them
([ADR-0022](decisions/0022-grammar-mirrors-postgres.md)): an expression
Postgres rejects for every row while planning (`2147483647 + 1`) defines, and
its rows fail at run time like any per-row error. The Value property treats
"admitted, and Postgres raises class 22 on a `WHERE false` probe" as agreement
(failing while planning), and flags a refusal where Postgres plans the
expression without error (a false refusal) and an acceptance where Postgres
fails to plan it for another reason, such as an operator it can't resolve (a
missed refusal). The one constant rule define has is textual: a `/` or `%`
whose divisor is written as a literal zero is refused wherever it sits, and
the checker restates that rule on its own and flags define on either side of
it, whatever Postgres would do with the division.

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
(`property_constant_expressions_conform_to_postgres`) is where Postgres's
plan-time folding is reached, so most of its failing draws agree as failing
while planning.

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
4. Teach the end-to-end oracle (`oracle::render_expr`, `field_value_type`) the
   operator's rendering and typing, and add a literal pool for any new
   typed-literal keyword.

### Generated predicates

`generate::where_clause` gives about half the definitions whose key space
applies a `WHERE` one (`where_clause::filters`: every key space but the plain
aggregate with `MIN`/`MAX`). Each predicate is total, reads the
columns the op streams update, and is derived from a digest of the drawn
program rather than drawn, so every strategy draws what it drew before: a case
without a predicate is the same program. A plain aggregate's and a plain 1-1's
read `c1`, `c2`, the flag and the grain; a 1-1 `JOIN`'s read `ja` and one or
two joined tables, cross-table comparisons and the anti-join included. A few
are `FALSE`, `NULL` or `(TRUE)`. A join aggregate's are one or two `AND`ed
conjuncts, each over one table's integer columns, none over `jb` under `LEFT`
(ADR-0019). The chained-reader tier draws none: it rewrites definition texts.

The meta-test `every_where_crossing_bucket_is_drawn_for_each_filtering_key_space`
(`tests/coverage.rs`) replays the serial, hot-key, cooling-key, `JOIN`, 1-1
`JOIN` and star draws with no database (`run::coverage::where_crossings`, over
the evaluator `where_clause::eval`) and fails when a key space has a crossing bucket
(`in_in`, `in_out`, `out_in`, `out_out`) no change lands in. The coverage
report prints the same tally as `where_crossings`. A join aggregate's
crossing is a row of one side changing that side's conjuncts.

The `WHERE` plants are gated where their key space is drawn: the aggregate
ones in every concurrent tier but `min_max`; the plain 1-1 ones in every tier
with a 1-1 definition, `o2o_where_build_unfiltered` only in the two that
rebuild under load (mid-burst, steady-load); `join_where_m_unfiltered` in the `JOIN`
tier; the 1-1 `JOIN` ones in the two-table tier (the star tier gates
`join1_where_build_unfiltered`). Deterministic pins alone gate the rest:
`gb_where_filtered_naming` needs a held key's release or a partition
correction, the `*_column_not_imaged` plants a capture widen, and
`o2o_where_no_tombstone` only grows the ledger, which no check reads.

### Generated transactions and statement forms

`generate::txn_shapes` widens how every concurrent tier issues its ops
(the hot-key family, `min_max`, `JOIN`, 1-1 `JOIN`, star and chained). Like
the predicates, it is derived from the program's digest, so each strategy
draws the program it drew before.

- **Statement forms.** Some single-row writes on a plain table become `INSERT
  … ON CONFLICT (key) DO NOTHING` or `DO UPDATE`, a one-row `MERGE`, or a
  writable CTE (`Op::Upsert`, `Op::Merge`, `Op::Cte`). Each has its plain
  write's source effect (`Op::write`): an insert of an existing row becomes a
  skip expected to affect no rows, and an update of a missing row is never
  upserted.
- **Transactions.** A lane wraps runs of up to five of its ops in one
  transaction (`model::TxnWrap`, run by `OpApplier::apply_txn`) at `READ
  COMMITTED`, `REPEATABLE READ` or `SERIALIZABLE`. Most read their first row
  before writing, some wait between statements, some rehearse (run, roll
  back, run again) and some hold a savepoint, released or rolled back to and
  re-run. An op that may fail runs in its own savepoint. A serialization
  failure or deadlock reruns the transaction from the start; the coverage
  report counts these as `txn_retries`.
- **Prepared transactions.** Some transactions end in `PREPARE TRANSACTION`
  and then `COMMIT PREPARED` or `ROLLBACK PREPARED` (`model::PrepareWrap`); a
  rolled-back one runs again and commits. One that ends its lane may stay
  prepared until the burst's actions (a pause, a resume, a build) have fired,
  so they land while it is in doubt. A prepared transaction still left when
  the oracle is about to read the source fails the case
  (`RunError::LeftPrepared`). Every testkit cluster allows prepared
  transactions. No concurrent plan restarts the engine, so a restart across
  one is `tests/prepared_transactions.rs`'s by-hand case.
- **Application wiring.** Some tables get triggers and a foreign key, as an
  application installs them (`ConcurrentPlan::wiring`, `generate::wiring`).
  A shadow row `k + SHADOW_OFFSET` follows each row `k`, kept by a row or
  statement trigger, `BEFORE` or `AFTER`, in the same table or another. An
  `AFTER` trigger links it by a foreign key that cascades or sets `NULL`.
  A statement trigger also flips a per-connection tally row. A rewrite
  trigger flips a column of each even-keyed row it writes, `BEFORE` in the
  row, `AFTER` by a nested update. Every write lands on the firing lane's
  own row, its shadow or its connection's tally; no op names the last two.

The oracle is unchanged: a transaction's keys are its lane's, a rolled-back
run is repeated before the commit, and a trigger or cascade never changes
which rows an op finds, so every op's expected outcome holds. The meta-tests
`every_transaction_shape_is_drawn_against_every_key_space` and
`every_wiring_shape_is_drawn_against_every_key_space` (`tests/coverage.rs`)
fail when a shape isn't drawn on a table each key space reads; the coverage
report prints the tally as `txn_shapes`. The plant `capture_slot_from_snapshot`
(capture reads its ring slot at the writer's snapshot) is the transaction
shape's: only a snapshot taken before a seal and a write after it reach it.
The cooling-key, cooling-backfill and mid-burst tiers gate it; the hot-key and
steady-load tiers seal too slowly to catch it reliably. The plant
`capture_skip_nested_reread` (capture never re-reads after a nested
statement's capture) is the wiring's: only the `AFTER` rewrite reaches it.
It reaches a target only through a 1-1 definition: a `GROUP BY` sums the
nested change and the outer one, right in either order. The cooling-key,
hot-key, mid-burst and steady-load tiers gate it; the cooling-backfill tier
has no 1-1 definition.

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
- **Held keys** (`tests/held_keys.rs`). Programs whose rows make a plain 1-1
  field raise, and the fuse that quarantines a definition (§10, "The oracle
  and erroring rows").
- **Isolation** (`tests/held_keys.rs`,
  `property_a_definition_failing_in_combination_stalls_no_other`). A
  held-key program plus a table `iso` read by `iso_d` and a sibling `iso_s`
  (`generate::isolation`). One two-row `INSERT` fails `iso_d`'s apply only
  in combination: two `1e308` rows in one group of a `float8` `SUM`, the
  `interval` infinities on Postgres 17+, or a unique index or trigger on its
  target that refuses the second row. `iso_d` must pause with
  [T0247](errors.md#t0247-definitionfailsalone), naming the SQLSTATE and
  both keys, its target frozen at its value before the pair. Every other
  definition, `iso_s` over the same rows included, keeps converging and
  none pauses. Once the run settles no holdup or undrained segment is left,
  so the ring retires. The held-key sweep gates
  `isolation_pauses_every_reader` on these cases.
- **Out-of-band tampering** (`tests/self_check_cross_check.rs`). Direct writes to
  derived tables are found by `self_check`, whose SQL renderer is independently
  authored from the oracle's; the suite asserts the two agree on every generated
  definition. `property_self_check_catches_tampered_extremes` does the same for
  the `MIN`/`MAX` shape (`generate::min_max`, run serially to its end): it sets
  one hidden extreme of a group wrong, alone or with its visible field, deletes a
  live group's hidden row, or deletes a zero group's (which must report nothing),
  and `self_check` must report exactly that, each extreme's recompute equal by
  value to the oracle's. Its sweep, `extremes_tamper_planted_bugs_are_caught`,
  gates `sc_extremes_uncompared`.
- **Epoch chains** (`tests/epoch_chain.rs`, `generate::epoch`). Every other
  property starts each case from a fresh database. This one runs many programs
  back to back on one instance and one database: each epoch starts at a quiet
  point with drops, redefinitions, definitions dropped in an earlier epoch
  brought back, second definitions and restarts, then writes rows carrying on
  from what the earlier epochs left. Every epoch ends with a quiesce, the
  oracle and a `Strict` `self_check`, so a failure names its epoch. A failing
  chain shrinks whole epochs first, then within the epochs left.
  `GENERATIVE_MAX_EPOCHS` (default 12) sets the longest chain; a deep run sets
  50 or more. The residue property (`residue::property_*`) laps the ring and
  checks, at each quiet point after the engine settles: no dead tuples and
  bounded pages in any Trellis table; an empty ring; each ledger and group
  table holding the rows the oracle expects; and catalog, registry, capture
  triggers and functions naming only what is defined (`run::hidden`).
- **Two instances** (`tests/two_instance_noise.rs`). Two Trellis instances in one
  database (or in two), each converging against its own oracle, including one
  instance reading the other's target.
- **Concurrent convergence** (`tests/concurrent_convergence.rs`). The same
  programs and oracle against the real worker pool and multi-worker claiming, in
  tiers: small programs; hot keys and groups taking bursts from several
  connections, also over a partitioned source; mid-burst re-reads, pauses, resumes, installs and
  definition changes while change capture for the same keys is in flight;
  cooling keys whose last write lands
  mid-burst; and sweep-only steady-load and cooling-backfill tiers that stall
  pages and build chunks at their entry-lock step, and a fresh build inside its
  first statement so pages make stragglers (the stall is process-wide, so
  they run only in the planted-bug sweep, §6).
- **Non-idempotent delta convergence** (the aggregate slice of the above).
  Byte-identical convergence to a from-scratch `GROUP BY` oracle under aggregate
  delta maintenance. Grain domains stay *tiny* (`0`, `1`, `2`, plus occasional
  `NULL` — see `generative::generate::strategy::grain_value`'s doc comment) so many
  rows share a group and deletes really kill groups. A `NULL` grouping value is
  drawn unconditionally. The numeric `NaN` is drawn for `c1`/`c2` (a few values
  in a hundred, inserted and updated), which the group basis counts apart from a
  group's running sum. The suite generates a *chained* definition (one
  `TRANSFORM` reading another's target) only in the two-instance property and
  the chained-reader tier, whose readers are `JOIN`s; front-door coverage of
  an aggregate chained off a target lives in
  `trellis/tests/defs/defs_aggregate_chained_single_column_group_key.rs`.
- **`MIN`/`MAX` convergence** (#1117; the concurrent tier's `min_max` shape,
  `generate::min_max`). One table with nullable group columns `g1` and
  `g2` and an argument of each type the engine keeps an extreme of
  (`bigint`, `numeric`, `float8`, `timestamptz`, `date`, `interval`), some
  values equal but spelled apart (`1.0` and `1.00`, `0` and `-0`,
  `26:00:00` and `1 day 02:00:00`, and `float8`'s infinities and `NaN`).
  One to three definitions group by `g1`, `(g1, g2)` or `g2` with
  `COUNT(*)` and one to four `MIN`/`MAX` fields, and the harness creates the
  index of the group columns then the argument for each. The op stream is
  retention-shaped: a group's current extreme and its ties deleted, the oldest rows deleted (`ts` grows with each insert), whole
  groups deleted, rows moved between groups, `NULL` arguments set and
  cleared, rows inserted tied with a group's extreme in another spelling,
  new extremes inserted and deleted again (half of those alone in
  a group of their own, the delete followed by another row of the group),
  and in a quarter of the cases a `TRUNCATE`. Bursts are paced from their
  start, so a row's insertion and deletion seal into different batches, and
  later bursts re-read the table and pause and resume a definition; one
  definition may install part way through. The oracle is each definition's
  own `GROUP BY`, and every burst is audited by a `Strict` `self_check`.
  Its property,
  `property_min_max_extremes_converge_under_concurrent_drains`, runs it
  without stalls; its sweep tier (`GENERATIVE_PLANT_TIER=min_max`) also
  stalls pages before their group lock, so a later batch's page of a group
  drains before an older one's, and gates the extremes' plants: of 48 cases
  it caught `gbx_shared_basis` in 48, `gbx_null_key_by_equality` in 45,
  `gbx_probe_before_lock` in 43, `gbx_absolute_write_keeps_extremes` in 27
  and `gbx_probe_no_xbasis` in 13. `gbx_null_extreme_no_probe` (2 of 48) and
  `gbx_gc_ignores_xstamp` (1 of 48) are known misses, and the
  `self_check` plants are left to the tampering tier and their pins (§9).
- **Float and `interval` sums** (#1119, `generate::exact_sums`). Half the
  `min_max` cases add a `real`, a `double precision` and an `interval` column
  and one to three `SUM`/`AVG` fields over them per definition; three `JOIN`
  cases in ten make `ja.x` and `jb.y` one of those types and keep only the
  top-level `SUM`/`AVG`/`COUNT` fields. The arm is derived from the case's
  existing draws (the `min_max` seed, the first op's `pick`), so the draws
  of every case are what they were before it. The values take the shapes
  where the order-dependent sum and the exact one differ: magnitudes that
  cancel (`1e20`, `1`, `-1e20`), inexact decimals, subnormals, `-0`, rare
  `NaN` and infinities, `interval`s with mixed signs, equal values in
  different fields, microseconds past `2^53`, fields near their limits, and
  on Postgres 17+ the infinities (one sign per case: both in a group raise).
  No group may leave its type's range even transiently, since the engine
  would hold its key and stall the convergence wait: the `min_max` arm keeps
  a budget of every magnitude written, and the `JOIN` arm's magnitudes stay
  in range over any product of its row counts. The raise itself is pinned
  by a build over an out-of-range group (`tests/exact_sums.rs`).
- **Join convergence** (`tests/join_convergence.rs`). Two
  tables, one to three `LEFT` or `INNER` join aggregates over them (joined on one
  column or on two, grouped by the joined table's column, by a column of each
  table, or by the `FROM` table's alone, with `COUNT`/`SUM`/`AVG` on either side,
  an expression argument and composed fields), and inserts, updates and deletes
  on both tables with `NULL` keys, groups and values now and then (in a quarter
  of the cases the argument columns `ja.x` and `jb.y` are `numeric(14,2)`, with
  the numeric `NaN` among the values, and no `SUM(ja.x + 1)`: `ja.x + 1` is an
  unconstrained `numeric`, whose `SUM` define refuses). The join keys
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
  short seals, every page stalled before its commit for up to 250 to 437 ms),
  where pages of one key's opposite sides are open at once with no shared
  cell, so only the `J` and `T` locks order them. Two things line that race
  up. The runner caps a `racing` case's drains at 8 records
  (`drain_batch_cap`): uncapped, the segments sealed while every worker
  stalls coalesce into one page whose cells every other page shares, and the
  cell locks order everything. And the ops come in pairs across tables: six
  ops after each op of a pair's first half, an op on the other table names
  the same key, so the two land in nearby segments whose pages run at once.
  A `racing` case takes about 10 s, so a sweep process of 48 cases takes
  about 8 minutes. Of 48 cases the tier caught `gb_fold_sides_per_key` in
  44, `join_drop_flip` in 34, `join_other_side_before_j` in 18,
  `join_no_j_lock` in 17 and `join_t_skip_lock` in 16, the last two in about
  half the `racing` cases. Every case runs housekeeping, where the zero-group
  GC runs, every 10 ms. `gb_early_zero_gc` is a known miss even so (caught in
  4 of 48): it needs a rebuild that leaves a group at zero while a change its
  basis saw is still undrained, then a GC pass before that change's page, and
  `racing` cases build nothing. Its deterministic pins gate it.
  `gb_lsn_not_visibility` is not drawn here, as its writers are single
  statements.
- **1-1 JOIN convergence** (`tests/join_one_to_one_convergence.rs`, ADR-0021).
  Two tables (`ja`, `jb`), one to three `LEFT` or `INNER` 1-1 `JOIN`
  definitions over them with no `GROUP BY` (one target row per `ja` row,
  keyed by its primary key, with fields that read either table), and inserts,
  updates and deletes on both tables. `jb`'s `ON` columns hold a unique key,
  and each definition joins on one of seven `ON` shapes: `a` on its primary key
  or on a plain column, `b` on its primary key or a unique column, a second
  plain column beside the key, a composite unique key `(c1, c2)`, and a text
  key. A key is `NULL` now and then, or only the second column of the pair, and
  a `jb` row never shares its key with another live `jb` row. All of a burst's
  `jb` ops are issued from one lane (two lanes could race two rows for a key
  one is about to free) and the `ja` ops are spread over the others. Key
  shapes, builds, pauses and resumes, re-reads and commit stalls are the
  aggregate `JOIN` tier's, and so is the `racing` shape, with `racing` two
  cases in seven, except that its pages stall for up to 2 to 3.5 s, its ops
  aren't paired across tables and its drains aren't capped. Every other shape
  but `wide` writes in short bursts (30 to 150 ops), each checked once it
  settles, with two to six pauses (each with its
  `RESUME`, a rebuild in place) and re-reads after the first burst, and in
  three cases in four a `TRUNCATE` of either table now and then, with two to
  four more in the last few ops of a burst: the planted bugs of a truncate, a
  rebuild's orphan pass and its `M` pass leave a target wrong only until later
  writes repair it, so the check has to come soon after. The
  oracle is the definition's own query without the `GROUP BY`
  (`render_join_one_to_one_select`), compared byte for byte; every burst is
  also audited by a `Strict` `self_check`, which reads the hidden ledgers.
  While a burst's lanes run, and once more after it settles, the tier checks
  an in-run hidden-state invariant (`generative::invariant`): in rounds 10 ms
  apart it reads each definition's `R`, `M`, `J`, target, pending fan-out
  markers and both tables as `R` and `M` have applied them under one
  `REPEATABLE READ` snapshot (`trellis::dev::staging::one_to_one_join_snapshot`),
  after taking `ACCESS SHARE` on the ring, both tables, the hidden tables and
  the target, so that no `TRUNCATE`, ring retirement or `RESUME` commits
  inside the read. It checks that `R` and `M` are their tables as applied
  (the live rows minus the ring's unapplied changes, as `self_check`
  subtracts them), that each target row is its `R` entry's fields over the
  newest live `M` entry on its key (with `INNER` dropping a row with none, and a
  `WHERE` that isn't `TRUE` on the row dropping it at either kind),
  and that every join key a live entry holds has its `J` row. The rows of a
  key with a pending fan-out marker that come after its cursor are left out
  of the target check, and a definition that isn't `live` or has a build
  chunk not done is skipped. A failure says whether a mid-burst sample or
  the one after the burst settled found it. Its reach is in hidden state the
  target doesn't show: with it off, 4 × 12 sweeps catch none of
  `join1_brd_no_key`, `join1_rd_no_key` (a join key with no `J` row),
  `join1_lsn_only` and `join1_snapshot_after_read` (an `R` or `M` entry off
  its table), and with it on, mid-burst samples catch each in one to three
  cases of 48. For the lost updates that later writes repair before the
  burst settles, the sweeps measure no gain: with it off, then in two sweeps
  with it on, `join1_no_j_lock` is caught in 4, then 5 and 5 cases of 48,
  `join1_vis_only` in 3, then 3 and 3, and `join1_m_before_j` in 8, then 11
  and 10. That is within run-to-run noise, though sweeps this size would
  show only a gain of about two and a half times the rate or more, and a
  sample that catches such a case first may be catching one the end-state
  check catches too. No known miss
  reaches the rate gating takes (the per-plant numbers are in the tier's
  `ONE_TO_ONE_KNOWN_MISSES`). The tier's keys never hold the 10k rows a pending
  fan-out needs, so the marker exemption is never exercised here; the
  checker's unit tests cover it, and
  `trellis/tests/join_one_to_one/join_one_to_one_fan_out.rs` reads the snapshot under a
  pending fan-out, chunk by chunk, to show that its rows are exact up to the
  cursor and stale past it.
  `GENERATIVE_JOIN1_INVARIANT=off` turns the invariant off; a number sets the
  pause between rounds in milliseconds.
  A side that is another transform's target is drawn by the chained-reader
  tier below, not here, nor is a value that makes a field fail (a held row:
  `join_one_to_one_quarantine`). Run it with `cargo test -p generative --test
  join_one_to_one_convergence -- --ignored`. Its planted-bug sweep,
  `one_to_one_join_planted_bugs_are_caught` (run with
  `GENERATIVE_PLANT_CASES=6`: 24 cases catch every gated plant, and the
  default twelve take hours), gates the plants it catches in about one
  case in five or more: `join1_no_placeholder`, `join1_old_key_unlocked`,
  `join1_no_orphan_pass`, `join1_truncate_keeps_r` and `join1_m_before_j`.
  `join1_old_key_unlocked` is gated on liveness only: the held-key recheck
  turns the missing lock into a `JoinKeyMoved` retry loop, so the sweep
  catches it as a quiesce timeout; its deterministic pins gate the lock.
  The other `Join1*` plants are narrow interleavings, or leave a target that
  the rest of the build or the next writes repair before the burst settles:
  the tier runs fourteen of them as known misses, with their measured rates
  in its `ONE_TO_ONE_KNOWN_MISSES`, and leaves four out, each with why (a held
  row, a write past capture, a page the barrier never lets drain late). Their
  deterministic pins gate all of them.
- **Star 1-1 JOIN convergence** (the star tier: `tests/join_one_to_one_convergence.rs`'s
  `property_star_join_targets_converge_under_concurrent_drains`, drawn by
  `generate::join_one_to_one::star_case_strategy`). The same generator over a
  root `ja` and one to four joined tables, one per position (`jb`, then `jc`,
  `jd`, `je`; at most 5 tables), each `LEFT` or `INNER` on its own drawn `ON`
  shape. The two-table JOIN is its one-position case: an op names its table
  by position, every position's table is written, truncates included, and
  each joined table's ops are issued from one lane per burst. A position past
  the first reads `ja`'s shared columns (`id`, `(c1, c2)`, `s`, `t`) or a
  bigint key of its own (`k2` to `k4`), and selects fields over its table.
  The tier draws what reaches races across positions: a narrow key space
  (80 keys, 3 of them taking two draws in five) in which a root row's own
  keys equal its `k` half the time, so it is hot at two or more positions at
  once; the stream cut into shuffled pages of 1 to 15 draws, half of them
  mixed (one root key written at the root and at every position, the
  drawn "late" position's write last); root rows that move at one
  position alone; updates concentrated on a few rows; and bursts that end in
  a race (a root row and a row of each joined table put on a hot key, then
  value changes of the joined rows at every position, with nothing after
  them to repair what they leave). Its cases drain with 8 workers, drains
  capped at a drawn 1 to 15 rows, a commit stall of up to 20 ms, and the
  engine's test-only knobs (`trellis::dev::interleave`,
  `trellis::dev::staging`, `trellis::dev::defs`), each process-wide for the
  case: 30% of a page's lock steps stalled up to a drawn 1 to 12 ms; 12% of
  pages re-derived, each holding its unlocked read open before its key
  lock; in three cases in four a split threshold of 2 to 16 rows, so one
  batch's pages drain on several workers side by side, and lanes paced at
  2 to 20 ms an op, so rows move while pages drain; and in half the cases a
  fan-out chunk of 1 to 4 rows, below a hot key's rows, so fan-outs go
  through pending markers and chunks. None of the knobs exists in a normal
  build, and each changes when things happen or how a page reads, never
  what a correct engine converges to. One
  case in four runs its metamorphic twin too: the same program with every
  definition's `JOIN` clauses in a drawn order, which renumbers the
  positions and so changes the engine's `(position, key)` lock order but no
  row. The oracle is the star's multi-join in the definition's order
  (`render_join_one_to_one_select`). The invariant's star check
  (`invariant::check_star`) runs every clause at each position against that
  position's `M_p`, `J` rows and table, with a target row only where every
  `INNER` position matched, and the in-run sampler runs it on every star's
  target, as it does the two-table check, over the engine's
  position-indexed snapshot read.
  The star tier's property, sweep and metamorphic check all run behind
  `join_one_to_one::star_tier_enabled`, which is on; the two-table
  tiers draw and check exactly what they did without a star
  (`tests/join_star_tier.rs` holds their draws' digests). The same file
  checks, with no engine apply running, that the strategy reaches every
  table count, kind, position, hot-key overlap, mixed page and knob; that the
  oracle is the multi-join over a real Postgres at 3, 4 and 5 tables, against
  the file's own model of a star, under any `JOIN` order; and that define
  admits every drawn star definition. Its planted-bug
  sweep (tier `join_star`) runs every `Join1*` and `star_*` plant but those
  the runner can't reach (a held row, a write past capture, a page the
  barrier never lets drain late, a target at a later position, and the
  plants whose only failure is a deadlock the engine retries). It gates the
  plants it catches in about one case in five or more:
  `join1_no_placeholder`, `join1_old_key_unlocked`, `join1_no_orphan_pass`,
  `join1_truncate_keeps_r`, `join1_m_before_j`, `star_root_skip_j_last`,
  `star_root_skip_j_first`, `star_root_skip_j_middle`,
  `star_chunk_skip_j_last` and `star_chunk_skip_j_first`. The rest run as
  known misses, with their measured rates in the tier's
  `STAR_KNOWN_MISSES`. Every `star_*` plant it runs fires, but the R-entry
  rule's ablations, the late-key and re-check plants and the fan-out
  chunk's are caught in a few cases of 48 at most: each needs two pages,
  or a page and a source write, to meet inside a window of a few
  milliseconds, which a deterministic pin holds open with a pause point and
  the tier's timing only hits now and then. Their pins gate all of them.
  The two-table tier leaves every plant that fires only on a star to this
  one.
- **Chained-reader convergence** (`tests/join_chained_convergence.rs`,
  ADR-0002 "Multi-hop"). A `JOIN`, aggregate or 1-1, one of whose sides is
  another definition's target in the same instance, so the target-mutation
  seam feeds it (its image feed and write token) rather than capture. A case
  is a case of the `JOIN` tier or of the 1-1 `JOIN` tier, half each, over
  their tables and op stream, with one side moved a hop down: an upstream,
  a plain 1-1 definition over `ja` or `jb`, passes every column through but
  one, which it computes as `<column> + n` or `COALESCE(<column>, n)`, the
  join key or the value a field reads. Every `JOIN` reads the upstream's
  target in place of the table. A 1-1 `JOIN`'s chained `b` is joined on
  columns that include its primary key, as define requires; a chained `a`,
  and either side of an aggregate `JOIN`, on any of its tier's `ON` shapes.
  On top of its tier's writes (key moves, `NULL` keys, `TRUNCATE`s), every
  fourth to tenth update of the upstream's source moves its row to another
  primary key, which the upstream sees as a delete and an insert; a burst
  that does this issues all of that table's ops from one lane. The bursts
  after the first pause and resume the upstream (the `RESUME` rebuilds it
  outside the seam, and so rebuilds the `JOIN`s reading it) and redefine its
  computed column (`ALTER TRANSFORM … ALTER`, a field build that rewrites
  every row through the seam), besides the tier's own installs, pauses,
  resumes and re-reads, which may pick the upstream too. An upstream pause
  never spans a `JOIN`'s install, which define would refuse, and a `JOIN`
  installed up front waits for its upstream to go `live`. Bursts are short
  (30 to 150 ops; 260 to 600 for `wide`); `racing` isn't drawn. Each `JOIN`'s
  oracle is its query written over the upstream's query
  (`oracle::chained_relation`), once both have converged, so an upstream that
  went wrong can't hide the `JOIN`'s own divergence; the upstream is checked
  against its own query, and every target by a `Strict` `self_check`. Not
  drawn: a chain of more than one hop, a side that is an aggregate or `JOIN`
  target, a paused upstream column and a held row
  (`join_over_targets`, `group_basis_chained` and
  `join_one_to_one_over_targets` pin those). Run it with `cargo test -p
  generative --test join_chained_convergence -- --ignored`. Its planted-bug
  sweep, `chained_join_planted_bugs_are_caught`, runs the seam's plants and
  no other, 48 cases each (about 45 minutes in all). It gates
  `seam_imageless_row`, which it catches in about two cases in five (an
  aggregate `JOIN` over a row that moves keys; a 1-1 `JOIN` takes the
  image-less row as a Re-derive request and stays exact). `seam_token_early`
  is a known miss: it fires in most cases but needs two upstream pages of one
  key to read their tokens in one order and lock the key in the other, and
  the tier caught it in none of 48. Its deterministic pin gates it.
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
  place of a dropped leaf when every bound is held), bound changes (a
  detached table attached under another free bound once its rows are moved
  into it, where a `CHECK` constraint a concurrent detach left on it stays
  and refuses the move, or the new bound's rows; a `LIST` leaf's value list
  widened by a detach and an attach; a `HASH` leaf re-split into leaves at
  a multiple of its modulus, so one tree mixes moduli), middles (a leaf
  partitioned again by `LIST` of the grain, with a leaf for up to two of
  its grains and a `DEFAULT` or not, under any free bound but the root's
  `DEFAULT`, created in the tree or attached with rows, then
  detached, also `CONCURRENTLY`, attached again or dropped like any leaf,
  with writes and truncates named on its own leaves),
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
  The `partition_ops` line counts each kind drawn. The hot-key tier has a
  partitioned variant
  (`property_partitioned_hot_keys_converge_under_concurrent_drains` in
  `tests/concurrent_convergence.rs`): its hot table is partitioned by the
  grain, by `HASH` over one to four leaves, `LIST` with a leaf for each of
  the first one to seven grains and a `DEFAULT`, or `RANGE` over two to
  seven bounds, and the model routes every grain its sliding group window
  reaches. So a hot group's rows move between partitions while the lanes
  race and several workers drain them. Every definition over the hot table
  groups by the grain, and the variant draws no partition maintenance,
  which would put the hot table in one lane.
- **Source-key convergence** *(ADR-0026, `generate::source_key`)*. One
  program in four drawn by the convergence property and the concurrent
  backend's property reads its first definition's source through a
  `KEY (...)`: one or two `text` columns, each `NOT NULL` or nullable under a
  `NULLS NOT DISTINCT` index, beside the primary key or without one. Every
  1-1 and `GROUP BY` definition over the table names the key. Keys take
  `NULL`, `''`, `chr(1)`, a doubled `chr(1)` and `chr(31)`, and updates move
  rows between keys. A model of the live keys expects a write onto a taken
  key to fail. The oracle keeps a `NULL` key part apart from `''`. The
  concurrent tier keeps a keyed table's writes in one lane, since which of
  two racing writes to one key fails depends on commit order. The
  `source_keys` coverage line counts each variant. One 1-1 JOIN tier case
  in four reads its joined table `jb` through such a key
  (`join_one_to_one::key_joined`): every definition joins
  `ON ja.k0 = jb.k0 [AND ja.k1 = jb.k1]` with `jb KEY (...)`, and `ja`'s
  `text` `ON` columns take the same values, `NULL` included, so `ja` rows
  point at both `jb`'s `''` row and its `NULL` row. The sweep tier
  `GENERATIVE_PLANT_TIER=source_key` and the 1-1 JOIN tier's
  `joined_key_planted_bugs_are_caught` (a nullable key in every case) gate
  `null_key_as_empty`, #2126's capture rendering restored, on each side.

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
- **Exact decimals compared by value, ignoring scale** (`2.50 ≡ 2.5`), and
  `NaN ≡ NaN` as Postgres's `=` has it for `numeric`. Byte-level rendering is
  pinned in a separate test.
- **Floats by value**, exactly, with Postgres's equality (`NaN ≡ NaN`,
  `-0 ≡ 0`); a `real` read as one. Every float is Postgres's own value or an
  exact sum rounded once, so the bits agree.
- **An aggregate's `interval` by value**, as Postgres's `=` compares it (a month
  is 30 days, a day 24 hours): `26:00:00 ≡ 1 day 02:00:00`. An aggregate holds an
  equal value (ADR-0020): `MIN`/`MAX` keep one of a tie's spellings, and a
  `SUM`/`AVG` moving to an equal value (`30 days` to `1 mon`) keeps its old
  one. A 1-1 field's `interval` stays exact text.
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

**A pause is a failure unless the tier provokes it.** Quiescing treats a `paused`
definition as settled, so the run ends promptly rather than waiting on it. The
check that follows reads every definition's catalog state, and a definition
`paused` there fails the run as `unexpected pause: <definition>: <reason>`
(`RunError::UnexpectedPause`), ahead of the oracle comparison, which a pause
would also fail without naming the cause. The reason is the definition's
`capture_failures` row or its poisoned keys' error.

The allowlist is `run::pause::allowance`: a tier on it may find a definition
paused, and a tier not on it treats any pause as a failure. On it: any program
with partition maintenance (`Op::Partition`). The source-DDL tier (§7) is
judged by "never silently wrong" instead, where a pause is one of the
endings.

Every divergence report, in every backend, prints each definition's status and
pause reason under `definitions:` beside the cell diffs.

**Status tells the truth.** Every settled check reads each definition's status
as `Trellis::definitions` reports it (`catching_up` included) beside the stored
one. A definition reporting `live` with nothing held and no failure recorded
claims an exact target, so one that diverges is marked `status lies` in the
report (`run::status_lies`). Most tiers hold every target to the oracle anyway.
The check bites where a run excuses a target, as the isolation draw does for
its failing definition (`RunError::StatusLies`). It runs after a quiesce, so a
window in which the harness itself took Postgres or the engine away is over by
then. No other window is exempt.

**Bounded retries.** Every quiesce reads `drain_holdups.attempts`: the failed
drains of a segment's pages charged to no key or definition, counted until every
failing page commits. At 20 the quiesce fails with `RetriesUnbounded`,
naming each segment, instead of waiting out its window. Below that, a ring that
doesn't move is the quiesce's own timeout. The engine pauses nothing for it;
the check only reports.

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
`JOIN` tier, `one_to_one_join_planted_bugs_are_caught` in
`generative/tests/join_one_to_one_convergence.rs` for the 1-1 `JOIN` tier,
`star_join_planted_bugs_are_caught` and `joined_key_planted_bugs_are_caught`
in the same file for its star and keyed-joined-table tiers,
`chained_join_planted_bugs_are_caught` in
`generative/tests/join_chained_convergence.rs` for the chained-reader tier,
`extremes_tamper_planted_bugs_are_caught` in
`generative/tests/self_check_cross_check.rs` for the `MIN`/`MAX` tampering tier,
`held_key_planted_bugs_are_caught` in `generative/tests/held_keys.rs` for the
held-key tier) runs a tier's seeded cases once unplanted, as the baseline, then
once per planted bug, and judges each plant only on cases the baseline passed.
**No unknown failure is accepted in a generative case**: the baseline passes
only with **zero failures outside the quarantine list**,
`generative/baseline-quarantine.txt`. The rule is the same for every tier
(`cooling_key`, `hot_key`, `mid_burst`, `steady_load`, `cooling_backfill`,
`min_max`, `source_key`, `join`, `join_one_to_one`, `join_one_to_one_key`,
`join_star`, `join_chained`,
`extremes_tamper`, `held_keys`).

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

### The reachability floor

Coverage floors prove the generators draw a shape; the reachability floor
proves the shape reaches the engine path. Each plant's hook counts a *reach*
whenever its precondition holds, armed or not. A hook whose precondition
costs a query the correct path doesn't run counts each pass instead
(`plant::visit`), and one whose precondition shows only as the correct
path's error counts where that error is classified. A *reach marker*
(`trellis::dev::plant::Marker`) counts a path that has no plant, such as
capture's nullable-key encoding. The report marks pass counts, markers
included, `(passes)`: they prove only that the path runs. Both compile only under `test-util`. The
subprocess backend asks each engine child for its counts before killing it
and folds them into the sweep's own.

Every sweep prints a reachability report after its baseline: per site, the
baseline cases that reached it and its reaches in all. Then the floor holds:
**every site is reached at least once in the tier's baseline**, unless the
tier's `not_gated` names it, or `generative::reach_floor::EXEMPT` lists it
for that tier (or for every tier, `*`) with why. A site the tier doesn't
gate but reaches in a quarter or more of its baseline cases is held too,
listed in `reach_floor::REACHED_UNGATED`: losing it is lost exploration
that no catch rate shows. When a run's ungated reaches differ from that
list, the report prints the entry the run would give. A site no tier reaches is
listed for `*` and names the deterministic test that reaches it. A site a
tier reaches only now and then is exempt for that tier, with its measured
rate.

The floor is judged on the tier's default baseline (seeds 1–4, 12 cases
each) or a bigger run, which draws the same cases first. A smaller run, or
`GENERATIVE_PLANT_ONLY`, prints the report and skips the floor. It costs no
extra cases, since every sweep already runs the baseline. The meta-test
`the_reachability_floors_exemptions_are_well_formed` checks that both lists
name real sites and tiers, and that each exemption gives a reason.

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
   The mid-burst and steady-load tiers change definitions while the lanes
   write (`generate::add_burst_actions`): a 1-1 field `ADD`, `ALTER` and
   `DROP` (an `ADD` of a column no definition reads widens the source's
   capture), a `DROP TRANSFORM` defined again under the same or a new name,
   unchanged or with a field changed, and a second definition over a loaded
   one's source, dropped a burst later. The `JOIN`, 1-1 `JOIN` and star
   tiers drop and redefine a definition, unchanged, and add and drop a
   second one (`generate::join::add_join_actions`). The chained-reader tier
   redefines a field too (`model::BurstAction::Alter`). The epoch tier drops
   and defines definitions between epochs, including ones dropped many epochs
   before (§4). Draws resolve in
   burst order, so a later burst acts on the definitions as the earlier
   changes left them, and the oracle checks each definition as the last
   statement left it; the backend checks that a drop took its target.
   `tests/coverage.rs` floors each change; the sweeps of both tiers gate
   `field_release_before_widen`, a field released before its capture widen
   lands.
3. **Engine lifecycle** (bucket 1) — kill/restart/scale workers; where resume and
   idempotent re-application get tested.
4. **Transaction shapes** (bucket 1) — multi-statement transactions at each
   isolation level, savepoints, rollbacks and serialization retries,
   `ON CONFLICT`, `MERGE` and writable-CTE writes, and application triggers
   and foreign-key cascades (§3, "Generated transactions and statement
   forms"); (bucket 3) a transaction held open across a batch boundary (via
   `OpenTransaction`).
5. **Database administration** (bucket 1) — restart PG, checkpoint, freeze,
   xid aging: `model::DbAdminAction`, driven by
   `run::run_convergence_with_db_admin`, swept by `tests/db_admin.rs`. Each
   fires with the anchor op's change still in flight, and the engine must
   reconnect by itself after a restart. `VacuumFreeze` freezes the case
   database; a freeze keeps each row's raw `xmin`, so capture's drift checks
   must read no change. `WrapXids` ages the cluster to one commit-log page
   short of the next 32-bit epoch boundary and burns all but a drawn tail of
   the ids left, so the drains and the next ops cross the boundary with ring
   rows, fences and bases from the old epoch live, and the program's earlier
   rows have an `age(xmin)` wrapped negative. The `fence_xid32` plant (fences
   compared as 32-bit ids) needs that crossing, so no planted-bug sweep tier
   runs it; `db_admin.rs`'s wrap pin and `staging`'s sealing pin catch it.
   `DbAdminPlan::counters_near` starts the engine's `bigint` counters (ring
   change ids and segment numbers, definition, build and catalog ids) just
   under `2^31` or a million short of `2^63` before the install, so the
   program crosses 32 bits or runs on 19-digit ids. It is plain SQL as the
   operator (`ManualBackend::advance_counters`): the counters are Postgres
   sequences and columns, so there is no engine hook. The renumbered ring
   (one active segment, none below it) is the state retiring every segment
   leaves. `ring_slot_mirror` is a four-value `smallint` and has no headroom.
   `DbAdminAction::StepClock` steps the server's wall clock (ten minutes or
   three hours forward, five minutes back; steps add up) with libfaketime
   preloaded into Postgres. `now()` and every stored timestamp move, so
   heartbeats, claims and gauges compared against `now()` see the jump,
   and no definition may pause for it. The engine reads no wall clock of its
   own, and tokio's monotonic clock does not move. A steppable engine clock
   is a later step. See [Wall-clock steps](#wall-clock-steps-libfaketime) for
   the prerequisite.
   Backup/restore is a cold file-level copy (`model::RestorePlan`, driven by
   `run::run_convergence_with_restore`, swept by `tests/backup_restore.rs`):
   back up right after op *k* with its change in flight, run to the end,
   restore into a fresh cluster, replay *k+1..n*, converge with no pause (§8
   has the decision behind that). There is no `pg_basebackup` variant: a base
   backup restores a transactionally consistent snapshot of the same ordinary
   logged tables, so it would test Postgres's recovery rather than Trellis (the
   upgrade battery's `basebackup` scenario restores one taken while writers ran).
6. **Out-of-band tampering** (bucket 1, detection) — direct writes to derived
   tables, found by `self_check` (`tests/self_check_cross_check.rs`).
7. **Source DDL** (bucket 3) — schema and maintenance changes to a tracked
   source with changes in flight: `model::SourceDdl`, driven by
   `run::run_convergence_with_source_ddl`, swept by `tests/source_ddl.rs`.
   The engine runs with a reconcile interval longer than a case and the run
   steps the passes, so each event lands inside one interval or across a
   pass as drawn. The oracle doesn't model the pause policy: after each op,
   `run::judge` holds every definition to **never silently wrong**. It is
   exact; or paused with a reason, and exact after its `RESUME` (or the
   `RESUME` refuses with a code); or wrong only as an entry of
   [known correctness gaps](known-correctness-gaps.md) the drawn shape names
   (`gaps::Gap`, by the entry's stable anchor), which `self_check` reports
   and the entry's repair fixes. Anything else fails, so the register is a
   checked allowlist: a missing entry fails, and so does one whose detection
   or repair doesn't hold.

8. **Privilege and ownership changes** (bucket 3) — `REVOKE`/`GRANT` of
   Trellis's `SELECT` or its schema's `USAGE`, the owner role revoked from
   Trellis, an owner Trellis does or doesn't inherit, row-level security on a source with and without a policy
   for its role, the application role changed, and the drain's writes to a
   target refused, directly or by the application's trigger on it: `SourceDdl::privilege`, drawn by
   `generate::source_ddl::privilege_plan_for`, swept by `tests/privileges.rs`,
   on the source-DDL runner and judge. A change the application's writes
   can't make as the owner moves them to `app_writer`, a login holding
   grants on the sources and nothing Trellis inherits. Every run fails on an
   application statement that fails inside a capture function
   (`RunError::CaptureFailedWriter`, ADR-0002 I6).

Two structural notes for when faults enter the stream:

- **Bulk operations are their own code paths.** `COPY`, `INSERT…SELECT`, one huge
  transaction, mass grain migration, and especially **`TRUNCATE` (its own
  capture trigger, not a bulk delete)** each reach batching paths row-at-a-time
  DML never does. Model bulk as a generated dimension with a
  **shrinkable row-count**.
- **Keep shrinking alive under faults.** Fault *placement* is deterministic (named
  failpoints where races live); the *schedule* is not. Seeds replay the placement;
  the oracle judges converged state at quiescence, which is schedule-independent.

## 8. Three layers, one action vocabulary

The suite's place among the [testing tiers](testing-strategy.md), split by what
judges a run:

| Layer | Tiers | Trigger | Judged by | Reproducible? | Cadence |
|---|---|---|---|---|---|
| 1. Deterministic (unit, integration, scripted interleaving pins) | 3–4 | scripted | assertion | fully | every run |
| 2. Generative, healthy path | 5 | random program, seeded | recompute oracle at quiescence | at program level (shrinking) | nightly, and on demand with `-- --ignored` |
| 3. Generative + fault actions | 5 | random program incl. faults, seeded | same oracle at quiescence | at program level (shrinking) | minutes–hours |

- **Layer 1** is `trellis/tests/*`, its pause-point interleaving pins, and the
  hand-built pins in `generative/tests/pins`. Every race bug found by layers 2–3
  terminates as a layer-1 pin.
- **Layer 2** is the healthy-path properties of §4.
- **Layer 3** is reachable only because a fault action is an oracle identity (§7)
  and because the generative and scripted harnesses **share one action vocabulary**:
  a scripted scenario is a generated program with its draws pinned.
- **A restore needs no restore-specific handling.** Trellis keeps its catalog, staging ring, capture triggers and
  targets in the database it reads, so a restore rolls them all back to one
  point and what comes back is consistent with itself. Layer 3's
  backup/restore action checks that assumption: the cold-copy case
  (restore, replay, converge, no pause) is generated; a `pg_basebackup` restore
  is the same snapshot of the same tables and is covered by the upgrade battery
  instead. One thing a
  server stop does carry over is a claim a worker took just
  before it: the claim's worker can't run the job or give it back, so only
  the stale-claim sweep frees it, a reclaim TTL later. The restore and
  restart runs give the engine a 10 s TTL so that recovery fits well inside
  the quiesce's 30 s no-progress window.

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
  `join_planted_bugs_are_caught`, the 1-1 `JOIN` tier's
  `one_to_one_join_planted_bugs_are_caught` and its star tier's
  `star_join_planted_bugs_are_caught`, the chained-reader tier's
  `chained_join_planted_bugs_are_caught`, the `MIN`/`MAX` tampering tier's
  `extremes_tamper_planted_bugs_are_caught`) re-run generated cases with a deliberate bug
  planted (`trellis/src/plant.rs`) and check the tier catches it, against an
  unplanted baseline that must have zero failures outside the quarantine list
  (§6). A quiesce timeout counts as a catch once the plant fired; the report's
  "timed out" column counts them, so a plant caught only by hanging shows.
  A plant in the tier's `known_misses` is run and reported but doesn't fail
  the sweep when uncaught, and one in `not_gated` (with its reason) is left out
  of `GENERATIVE_PLANTS=all`. Each plant breaks one apply path (the group basis,
  a group-basis aggregate's `MIN`/`MAX` extremes or `self_check`'s comparison
  of them, a `JOIN`, a 1-1 target, a 1-1 `JOIN`, a target's seam feed to its
  readers), and a tier leaves out the plants of paths its programs never
  take: the concurrent tier draws no `JOIN`, the `JOIN` tier nothing else,
  only the concurrent tier's `min_max` shape and the tampering tier draw
  `MIN`/`MAX`, and only the
  1-1 `JOIN` tier gates the 1-1 `JOIN`'s plants. The
  chained-reader tier gates only the seam's plants: its programs take the
  other paths too, but the tiers above gate those. `early_tombstone_gc` and `chunk_without_entry_lock` are left to
  their deterministic pins too: on a 1-1 target no tier's schedule makes either
  diverge (each tier's `not_gated` says what was measured). The `self_check`
  extremes plants (`sc_…`) are the tampering tier's: `sc_extremes_uncompared`
  makes `self_check` miss a tampered extreme, which that tier asks it to find.
  The other two are left to their pins: `sc_extremes_under_basis` reports a
  correct extreme only while a change is in flight, and
  `sc_extreme_groups_uncompared` misses a group only while a probe is ahead of
  the partials' drain, and every tier audits a target once it has settled.
  Each group-basis, extremes, `self_check`, 1-1, `JOIN`, 1-1 `JOIN` and seam
  plant is pinned by a deterministic interleaving test that fails with it
  armed, named in its doc comment.
- Document how to run one property alone on a clean database; every property
  bootstraps what it reads, so it never depends on run order.

### Wall-clock steps (libfaketime)

The clock-step tests (`generative/tests/db_admin.rs`, `*clock*`) preload
libfaketime into the test Postgres server and move its clock by rewriting an
offset file (`FAKETIME_TIMESTAMP_FILE` with `FAKETIME_NO_CACHE`), so a step
needs no restart. Linux only (the preload is `LD_PRELOAD`); on macOS set
`TRELLIS_SKIP_FAKETIME=1`.

- **Install.** Fedora: `sudo dnf install libfaketime`. Debian and Ubuntu:
  `sudo apt-get install faketime` (it pulls in the library).
- **Lookup.** `TRELLIS_LIBFAKETIME` (a path to `libfaketime.so.1`), then
  `/usr/lib64/`, `/usr/lib/{x86_64,aarch64}-linux-gnu/faketime/` and
  `/usr/lib/faketime/`. A `TRELLIS_LIBFAKETIME` naming a missing file fails.
- **Missing library.** The clock tests skip, printing `!!!! SKIPPED` to
  stderr (run with `--nocapture` to see it on a passing run).
- **Skip on purpose.** `TRELLIS_SKIP_FAKETIME=1` skips them with the same
  message.
- **CI.** `TRELLIS_REQUIRE_FAKETIME=1` turns a missing library into a failure;
  `ci.yml` and `nightly.yml` install `faketime` and set it. Setting it
  together with `TRELLIS_SKIP_FAKETIME` fails.

## 10. Settled decisions

- **Property-testing framework.** `proptest` supplies the typed generator
  strategies and the shrinking.
- **Concurrent-runtime shrink trust.** A concurrent-mode shrink is advisory:
  when a failure reproduces under the manual runtime, re-shrink it there.
- **The oracle and erroring rows.** The held-key tier
  (`generate::held_keys`, `run::run_convergence_with_held_keys`) draws
  erroring rows on purpose: a plain 1-1 field divides by `c2 - K`, with no
  `NULLIF`, and a drawn share of the table's `c2` writes is set to `K`. Its
  oracle computes per row (one `SELECT` aborts on the first row that raises)
  and models the engine's contract ([ADR-0003](decisions/0003-quarantine-storage-and-api.md),
  [`held_keys`](status-reference.md#held_keys)): a raising row's key is held,
  its target row stays as the last check expected it (absent if it had none)
  through every later change to the key, a `TRUNCATE` clears it while the key
  stays held, and five held keys quarantine the definition and freeze its
  target. After every op it checks each target against that, and that
  `status()`'s `held_keys.count` and `sample_quarantined` name exactly the
  held keys. A held key blocks every engine convergence wait, so the tier
  waits with a test-only check that skips held keys' parked changes
  (`trellis::dev::staging::await_converged_except_held`): a case settles as
  fast with a held key as without, and shrinking pays no timeout per step.
  Its fuse family gives one definition two raising fields, four rows at most
  each, so the whole-transform fuse trips before either column fuse. Only
  the plain 1-1 key space is modelled for a row that fails alone; every
  other tier keeps its draws raise-free. Rows that fail only in combination
  are the isolation family's, which needs no model of them: the definition
  pauses (T0247) and its target holds still.
