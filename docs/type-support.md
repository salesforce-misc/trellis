# Data-type support matrix

Which PostgreSQL data types Trellis supports, in **which role**. A living
reference, updated as each type/role lands. Driven by
[Epic #123](https://github.com/salesforce-misc/trellis/issues/123); the
candidacy constraint is
[ADR-0004](decisions/0004-transform-definition-grammar.md).

A type is never simply "supported." It earns its way into six roles, ordered
**floor → ceiling** — the matrix doubles as a maturity ladder, and most types
climb it left-to-right:

1. **Ingest / passthrough** — decode from logical replication and copy into a
   derived table unchanged. The floor under every other role, and nearly
   everything clears it. Since #108 a column's role is decided by its raw
   `pg_attribute.atttypid`, through the OID registry in
   `trellis/src/defs/pg_type.rs`, rather than by text-matching
   `format_type`'s rendering: every family the registry knows becomes a
   `ValueType::Other(PgType)` that keeps its concrete PG type end-to-end
   (`ddl::pg_type_name`), instead of the old `_ => Text` lie. An OID the
   registry *can't* place — an enum (dynamically assigned OID), an array,
   range, composite, domain or extension type — is `PgType::Unrecognized`
   and stays out of the validator's view entirely (`Trellis::source_columns`),
   so referencing it is a clean unresolved-column error; promoting those
   families is #117/#122's job.

   Since #111 the *exact integer* types are no longer part of that
   passthrough story at all: `smallint`/`integer`/`bigint` have their own
   `ValueType::Integer(IntWidth)` (see `trellis/src/integer.rs`), so a
   derived integer column is declared `integer` rather than `numeric`. #112
   finished the split: `real`/`double precision` have their own
   `ValueType::Float(FloatWidth)` (`trellis/src/float.rs`), so
   `ValueType::Numeric` now means `numeric`/`decimal` and nothing else.
2. **Filter (predicate)** — appear in a `WHERE` predicate. Needs immutable
   comparison operators and the predicate grammar (stubbed to `TRUE` today,
   `ast.rs:185`).
3. **Join / relationship / `GROUP BY` key** — be matched by equality. Today via
   raw `::text` rendering (`TEXT_STABLE_JOIN_KEY_TYPES`,
   `trellis/src/defs/catalog.rs`; `to_rows_by_key`, `eval.rs`). Relationship
   join keys and 1-1 primary keys gate on that allowlist by pg type name; a
   `GROUP BY` key gates on its `ValueType` instead
   (`validate::UnsupportedGroupByKeyType`), rejecting every
   `ValueType::Other` family **except `oid`** — `::text` matching disagrees
   with those types' own `=` (`'1 day'::interval = '24 hours'`,
   `timestamptz`/`bytea` under session GUCs), and `json` has no `=` at all,
   whereas `oid_out` is canonical unsigned decimal (#111). #112 tightened
   the same gate for `real`/`double precision`, which used to slip through
   as `ValueType::Numeric`: `-0` and `0` are `=` in Postgres but render as
   `'-0'` and `'0'`, so a `::text`-matched float key splits one Postgres
   group into two target rows.
4. **Primary key** — identify a target row. A stricter join key: it must come
   from the source's replica identity and present in the old-image for
   updates/deletes. Gated to the join-key-safe allowlist
   (`is_text_stable_join_key_type`) — an unsafe single-column PK
   (`numeric`/`timestamptz`/`interval`/`bytea`, which `::text`-matching
   would silently mismatch) is rejected at define time with
   `DdlError::UnsupportedPrimaryKeyType` (#107). The typed key index
   (below) later unlocks all of those, `interval` included: it compares
   *decoded values*, so a rendering that is ambiguous (`'1 day'` vs
   `'24 hours'`) or merely inconsistent between two renderers stops
   mattering. What the index does not help with is a value whose *ordering*
   is undefined, and no type here has that problem.
5. **Computed 1-1 target** — a scalar calculated field *produces* this type
   (distinct from passthrough). Needs an immutable evaluator arm **and** grammar
   to spell a literal/cast of the type.
6. **Aggregate target** — an aggregate calculated field folds to this type.
   Gated by fold economics: `SUM`/`COUNT`/`AVG` invertible (cheap deltas);
   `MIN`/`MAX` orderable-but-not-invertible; `array_agg`/`string_agg`/`jsonb_agg`
   order-sensitive.

## Legend

| Mark | Meaning |
|---|---|
| ✅ | works today |
| 🎯 | in scope for the epic |
| ⚠️ | conditional / needs design decision |
| ❌ | excluded (immutability or no equality) |
| — | not applicable |

`COUNT(*)` is type-agnostic row-counting (`registry.rs:160`, #75) — not a
per-type capability, so it's omitted from the aggregate cells.

## Matrix

| Postgres type(s) | Ingest / passthrough | Filter | Join key | Primary key | Computed 1-1 | Aggregate | Notes |
|---|---|---|---|---|---|---|---|
| `smallint` `integer` `bigint` | ✅ | 🎯 | ✅ | ✅ | ✅ | ✅ SUM/AVG/MIN/MAX | typed integer with Postgres overflow semantics (#111) |
| `oid` | ✅ | 🎯 | ✅ | ✅ | ✅ | — | key + literal roles (#111); no arithmetic — Postgres has no `oid + oid` |
| `numeric` `decimal` | ✅ | 🎯 | 🎯 typed index | 🎯 typed index | ✅ | ✅ | rejected as PK today; `1.0`≠`1.00` under text match |
| `real` `double precision` | ✅ | 🎯 | 🎯 typed index | 🎯 typed index | ✅ | ✅ SUM/AVG/MIN/MAX (recompute-only) | typed binary float, Postgres's non-IEEE order (#112); `-0`≠`'-0'` under text match keeps it off the key roles |
| `boolean` | ✅ | 🎯 | 🎯 | ⚠️ (rare) | ✅ | 🎯 `bool_and`/`bool_or` | value type exists |
| `uuid` | ✅ | 🎯 | ✅ | ✅ | ✅ | ⚠️ MIN/MAX | landed in #79 |
| `text` `varchar` | ✅ | 🎯 | ✅ | ✅ | ✅ | 🎯 MIN/MAX/`string_agg` | requires deterministic collation |
| `char(n)` `citext` | ✅ | ⚠️ | ❌ padding/case | ❌ | ⚠️ passthrough | — | hazard is padding/case, not volatility |
| `bytea` | ✅ (hex text) | 🎯 | 🎯 typed index | 🎯 typed index | ✅ literal (#109) | ⚠️ MIN/MAX | `bytea_output` GUC affects text render; literal must be canonical lowercase hex |
| `date` | ✅ | 🎯 | ✅ | ✅ | ✅ literal (#109) | ✅ MIN/MAX | key roles landed in #113 — no typed index needed. `DATE 'YYYY-MM-DD'` only; `'today'` is why `date_in` is STABLE |
| `time` `timetz` | ✅ | 🎯 | ✅ | ✅ | ✅ literal (#113) | ✅ MIN/MAX | `time_out`/`timetz_out` are the block's only IMMUTABLE output functions; `timetz`'s `=` is identity on `(time, zone)` |
| `timestamp` | ✅ | 🎯 | ✅ | ✅ | ✅ literal (#109) | ✅ MIN/MAX | key roles landed in #113/#248 — bijective under `::text`, and `to_jsonb` (the live-row reads) now renders it the same way (#248 replaced `to_jsonb(t.*)` with an explicit per-column `jsonb_build_object`) |
| `timestamptz` | ✅ | 🎯 | 🎯 typed index | 🎯 typed index | 🎯 | 🎯 MIN/MAX | #248 fixed the `to_jsonb` split, but a *second*, independent defect remains: a `TimeZone`-dependent render on a walsender Trellis cannot pin (#113, #246, still open) |
| `interval` | ✅ | 🎯 | 🎯 typed index | 🎯 typed index | 🎯 | ✅ `SUM` (recompute-only); ❌ MIN/MAX | `'24 hours' = '1 day'` but they render differently, so no *text* match works; #110 comparing decoded values would. `max` is scan-order dependent even on the server (#113) |
| `jsonb` | ✅ | 🎯 | ⚠️ typed index | ⚠️ | 🎯 needs a canonicalizer | ⚠️ `jsonb_agg` STABLE in PG | `json` excluded (no `=`); `jsonb_out` re-sorts keys, so a literal needs #115's value model |
| `inet` `cidr` `macaddr` `macaddr8` | ✅ | 🎯 | 🎯 | 🎯 | 🎯 | ⚠️ MIN/MAX | |
| enum types | ✅ | 🎯 | 🎯 | 🎯 | ⚠️ | 🎯 MIN/MAX | order fixed at type creation |
| `bit` `bit varying` | ✅ | 🎯 | 🎯 | 🎯 | 🎯 | 🎯 `bit_and`/`bit_or` | |
| array types | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ⚠️ `array_agg` (order-sensitive) | deferred; large design |
| range types | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | deferred |
| composite / row types | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | — | deferred |
| geometric (`point`…) | ✅ | ⚠️ | ❌ | ❌ | ⚠️ | ❌ | niche; deep-defer |
| `money` | ⚠️ locale text | ❌ | ❌ | ❌ | ❌ | ❌ | I/O is `STABLE` (`lc_monetary`); use `numeric` |
| `json` | ✅ | ❌ | ❌ | ❌ | ⚠️ passthrough | ⚠️ | no `=` operator; prefer `jsonb` |
| `xml` `tsvector` `tsquery` | ✅ | ❌ | ❌ | ❌ | ⚠️ passthrough | ❌ | no useful immutable `=`/ordering |

## The immutability gate

ADR-0004 admits only operators/functions whose output depends solely on their
inputs. `pg_proc.provolatile` is the ground truth — several intuitions are wrong:

* **`money`** — comparison (`cash_eq`/`cash_cmp`) *is* immutable, but text I/O
  (`cash_out`) is `STABLE` (`lc_monetary`), so the CDC-decoded text is
  locale-dependent. Excluded; use `numeric`.
* **`json`** — has *no* `=` operator; can never be a key. `jsonb` can.
* **`text`/`varchar`/`char`** — Postgres marks these comparisons IMMUTABLE
  despite collation-sensitivity. Our own bar is a **deterministic collation** (or
  a normalized stored form); `char(n)`'s hazard is blank-padding, not volatility.
* **`timestamptz`/`bytea`** — value comparison immutable, but *text rendering*
  is GUC-dependent (`TimeZone`, `bytea_output`). `bytea_output` is pinned;
  `TimeZone` deliberately is not (#113, below), so for `timestamptz` a
  **typed key index** — comparing decoded values, not text — is the real
  unlock.
* **`interval`** — `interval_cmp` is immutable and perfectly well-defined,
  but it compares *total spans* (30 days to a month, 24 hours to a day)
  while `interval_out` prints the three stored fields, so one value has
  many renderings. Not a volatility problem and not a GUC problem: a
  structural one, and the same class as float `-0`/`0`.
* **`timestamp`** — nothing about the *type* was ever at issue:
  `timestamp_out` is a bijection under the pinned `DateStyle`. What used to
  block it was that Trellis had a *second* internal renderer (`to_jsonb`, in
  the live-row reads) that spelled it differently; immutability was never
  the gate, renderer agreement was (#113, fixed by #248).
* **`jsonb_agg`** — marked `STABLE` in Postgres; whether Trellis's own
  deterministic reimplementation may treat it as immutable is an open question.
* **`xml`/`tsvector`/`tsquery`** — no useful immutable equality/ordering; niche.

## Cross-cutting concerns

* **Casts / coercion lattice** — computing a new type needs literal and `CAST`
  grammar. **Landed for literals (#109):** `DATE '2024-01-01'` and the
  equivalent `CAST('2024-01-01' AS date)` both produce a real typed constant,
  over an allowlist (`date`, `timestamp`, `bytea`, and `oid` since #111) —
  independent of #248's key-role fix, since a typed literal is always
  rendered via `::text`, never via `to_jsonb` — whose literal text must be
  in Postgres's canonical output spelling — see
  [ADR-0004](decisions/0004-transform-definition-grammar.md#typed-literals-issue-109).
  Still open: a **general** coercion lattice (`CAST(<expr> AS <type>)` over a
  non-literal), which each type family's own child decides, since most pairs
  are not immutable.
* **Typed key index** — replacing the raw-`::text` match unlocks
  `timestamptz`/`bytea`/`numeric`/`real`/`double precision`/… as safe keys;
  the single
  highest-leverage child for the key roles. Note what it is *not* needed
  for: the exact integer types and `oid` render canonically (`-`, then
  digits — no `+`, no leading zeros, no padding, no session GUC), so
  `a::text = b::text` already agrees with their native `=` for every value,
  which is why they are on `TEXT_STABLE_JOIN_KEY_TYPES` rather than waiting
  for the index. #111's contribution to the key roles was therefore *type
  honesty*, not a new encoding: integer keys used to work by sharing
  `numeric`'s decimal rendering, which is also why the `GROUP BY` key gate
  still lets a genuinely unsafe `numeric` key through (see the
  `numeric`/`decimal` row) — tightening that is #110's call. #112 tightened
  its own half: floats no longer reach that gate as `ValueType::Numeric`,
  and are refused. #113 removed `date`/`time`/`timetz` from the index's
  to-do list for the same reason as `oid`; `timestamp` stayed on it a while
  longer, for a reason the index does not actually address (two *internal*
  renderers disagreeing), until #248 fixed the renderer disagreement
  directly and removed it too — see below.
* **Exact integer semantics (#111)** — `+` and the aggregates follow
  Postgres's own operator family, including its overflow behaviour: `int4 +
  int4` is `integer` and raises `22003 numeric_value_out_of_range` past the
  range, `int4 + numeric` promotes to unbounded `numeric` and cannot;
  `sum(int2|int4) -> bigint`, `sum(int8) -> numeric`, `avg(<int>) ->
  numeric`, `min`/`max` keep their argument's width. `COUNT(*)` is
  unchanged (`numeric`); Postgres types it `bigint`, which is #120's to
  align since it has nothing to do with the integer split. Cross-checked
  against a live server in `trellis/tests/defs_exact_integers.rs` via
  `pg_typeof`, never against Trellis's own renderer (ADR-0013).
* **Binary float semantics (#112)** — Postgres, not IEEE 754, is the oracle
  here, and it differs in exactly two places, both deliberate so that floats
  can be ordered and grouped at all: **`NaN = NaN` is true** and **`NaN`
  sorts above every value including `Infinity`**; `-0 = 0` is true (IEEE
  agrees). `trellis::float::compare` implements that order and is the only
  float comparison in the crate. Operators and aggregates follow Postgres's
  own family: `real + real` is `real`, but a float mixed with *anything*
  else (`integer`, `numeric`, `double precision`) resolves to `float8pl` and
  is `double precision`; a finite pair that overflows raises `22003 value
  out of range: overflow` while `'Infinity' + 1e38` does not;
  `sum(real) -> real` (no widening, unlike the integers), `avg(real) ->
  double precision`, `min`/`max` keep their argument's width. Note
  `COALESCE` unifies the same pair *differently* — `coalesce(real, numeric)`
  is `real` — because Postgres resolves it through `select_common_type`
  rather than operator overloading; both are modelled separately.
  Cross-checked against a live server in `trellis/tests/defs_floats.rs`.

  Two consequences worth stating plainly:

  * **Not a key, for now.** Postgres would happily index a float primary key
    (its btree opclass *is* the total order above), so the refusal is about
    Trellis's current raw-`::text` key matching, which cannot represent
    `-0 = 0`. #110's typed key index is the unlock; the matrix's join/PK
    cells say `🎯 typed index` for exactly that reason, not `❌`.
  * **`SUM`/`AVG` are recompute-only.** Float addition is neither
    associative nor order-independent, and `NaN`/`Infinity` are absorbing,
    so there is no exact inverse to delta with — `defs::invertibility`
    classifies them alongside `MIN`/`MAX`. This is the one *numeric*
    argument type where that is true.

  Float text also depends on a GUC: `extra_float_digits` must be `>= 1` for
  the shortest-round-trip rendering `trellis::float::render` reproduces, so
  it joins `DateStyle`/`bytea_output` in `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`.
* **Temporal semantics (#113, #248)** — the temporal block's six families
  were all marked `🎯 typed index` on the assumption that #110 was the
  prerequisite for any key role. Asked of a real server, per family, that
  assumption turned out to hold for only some of them — and the reason the
  others were refused was *not* the one the marking assumed.

  * **`date`, `time`, `timetz` and (since #248) `timestamp` are keys,
    `GROUP BY` keys and primary keys** — the `oid` reasoning from #111
    ("text-stability is a property of the *rendering*, not of the operator
    set"). `date_out` under the already-pinned `DateStyle = 'ISO, YMD'` is a
    bijection: the year field widens (`5874897-12-31`), the era is an
    explicit ` BC` suffix, and `infinity`/`-infinity` have their own
    spellings. `time_out` and `timetz_out` read **no GUC at all** — they are
    the only two `IMMUTABLE` output functions in the block (`date_out`,
    `timestamp_out`, `timestamptz_out` and `interval_out` are all `STABLE`).
    `timetz` is the counter-intuitive one and it is safe for the opposite of
    the obvious reason: `select '12:00:00+00'::timetz =
    '17:30:00+05:30'::timetz` is **false**, because `timetz_cmp` sorts by
    GMT-equivalent time *and then by zone*, so `=` is identity on the stored
    `(time, zone)` pair — exactly what `timetz_out` prints.
  * **Text-stability needs two checks, not one, and `timestamp` used to fail
    the second — issue #248 fixed it.** Trellis used to turn a column into
    text in **two** different ways: per column as `<col>::text`, and — in
    the live-row reads (`staging::apply`'s `read_live_rows_batch`,
    `fetch_to_side_rows`, `fetch_relationship_projection_rows` and its
    reverse-trigger projection read; `staging::quarantine`'s sweep; and the
    `RETURNING to_jsonb(t.*)::text` old-image captures issue #196 relies on)
    — a whole row at once via `to_jsonb(t.*)` + `jsonb_each_text`. `to_jsonb`
    does not call the type's output function for datetimes; it uses
    `jsonb`'s own ISO-8601 writer. One row, one session, both renderings, as
    read off a live server *before* #248:

    | column | `::text` | `to_jsonb` | |
    |---|---|---|---|
    | `date` | `2024-06-15` | `2024-06-15` | same |
    | `time` | `12:34:56` | `12:34:56` | same |
    | `timetz` | `12:34:56+00` | `12:34:56+00` | same |
    | `interval` | `1 day 02:00:00` | `1 day 02:00:00` | same |
    | `timestamp` | `2024-06-15 12:34:56` | `2024-06-15T12:34:56` | **differs** |
    | `timestamptz` | `2024-06-15 12:34:56+00` | `2024-06-15T12:34:56+00:00` | **differs** |

    (`numeric`, the integer widths, `uuid`, `bytea` and the floats were
    swept too and all agree — this is a datetime quirk of `to_jsonb`, not a
    general property of it.)

    The `T` was not cosmetic: a `timestamp` `GROUP BY` key seeded once
    through a backfill and once through CDC-then-live-read became **two
    target rows for one Postgres group** — reproduced during #113's review
    (`total = 20` where the correct answer was `15`). It also reached
    `MIN`/`MAX`, because those return an input *verbatim* — a fold over
    `to_jsonb`-read rows could return the `T`-spelled string where a
    server-side `min()` returns the space-spelled one, which ADR-0013's
    byte-exact cross-check would report as a divergence. **Issue #248**
    closed this by replacing every `to_jsonb(t.*)` call site above with an
    explicit per-column `jsonb_build_object('<col>', <col>::text, ...)`
    (`staging::apply::row_as_text_jsonb_sql`, over each table's live
    `pg_catalog` column list, or the already-known `pk`/field columns for
    `apply_target`'s own old-image capture), so every renderer the engine
    itself uses now agrees on `timestamp`'s text — bare Postgres
    `to_jsonb(t.*)` still spells it with a `T`, but nothing in Trellis calls
    it that way any more. `trellis::temporal::is_render_consistent` is the
    gate, now admitting `timestamp`, and
    `defs_temporal.rs`'s `to_jsonb_and_text_agree_for_every_admitted_key_type`
    sweeps the **whole** key allowlist so a *future* family can't repeat the
    same gap unnoticed.

    Note the shape: one value, two renderers, no arbiter — the same defect
    *shape* as issue #246, one layer in (there it is the pool versus the
    walsender; here it was `::text` versus `to_jsonb` inside one process).
    Closing #248 does not touch #246 — different renderer pairs — which is
    why `timestamptz` needs #246 too, independently (below).
  * **`timestamptz` has a second, independent blocker that #248 does not
    touch, and pinning `TimeZone` is not the fix.** Pinning
    `TimeZone = 'UTC'` in `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` *would* make
    its `::text` rendering a bijection on the instant, and Trellis owning
    all its own connections means the blast radius on application sessions
    is nil. It still fails, because logical-decoding output is produced by
    the output function running in the **walsender**, under the walsender's
    GUCs, and `pgwire_replication`'s `ReplicationConfig` (v0.4) exposes no
    way to send startup runtime parameters. Today the two agree by falling
    back to the same server default; pinning the pool alone would trade that
    accidental symmetry for a guaranteed asymmetry on every non-UTC server.
    Tracked as **issue #246** (still open as of #248 landing), which also
    covers the pre-existing exposure for the `DateStyle`/`bytea_output`
    pins. (Postgres itself honours startup `options` on a replication
    connection — `PGOPTIONS='-c timezone=UTC'` works with `pg_recvlogical`
    — so this is a library gap, not a wall.)

    This gives the rule for adding a fifth GUC to that constant: each one
    currently pinned is **output-identical to a stock server's default**, so
    the unpinned walsender agrees unless an operator deliberately
    reconfigured the server. `TimeZone` has no such stock value;
    `IntervalStyle` does (`postgres`), which is why #113 added that one and
    not this one.
  * **`interval` can never be a text-matched key.**
    `'24 hours'::interval = '1 day'::interval` is **true** while their
    `::text` differs — one value, many renderings, structurally the float
    `-0`/`0` defect with a dense equivalence class instead of a single
    pathological pair. Unlike `timestamp`'s old problem, no renderer
    reconciliation helps here; #110's typed key index would, by comparing
    decoded values.
  * **`MIN`/`MAX` land for `date`/`time`/`timetz`/`timestamp`.** `interval`
    is refused on a *third*, separate ground: `max(v)` over `{'1 day', '24 hours',
    '2 hours'}` returns `24:00:00` scanning one way and `1 day` the other,
    on a live server, because `interval_larger` is a left fold over a tie
    that is not byte-identical. Postgres's own answer is not a function of
    its input multiset, so ADR-0013's byte-exact recompute cross-check can
    never settle it, whatever Trellis computes.
  * **`SUM(interval)` lands, and is recompute-only** — same destination as
    #112's float `SUM`/`AVG`, arrived at from the opposite direction. On
    finite, in-range values interval addition *is* exact, commutative and
    associative with an exact inverse (three independent integer fields,
    added fieldwise by `interval_pl`, no justification — `'1 mon' + '30
    days'` is `1 mon 30 days`, not `2 mons`). But the monoid is **partial**,
    and a delta cannot represent the gaps:

    * `'infinity'::interval` is a legitimate value (PG 17+). `infinity + 1
      day` is `infinity`; `infinity + -infinity` **raises**. A group holding
      `{infinity, 1 day}` whose infinity row is deleted has a well-defined
      true sum of `1 day`, but the delta computes `infinity + (-infinity)`
      and raises — and replays identically on every retry, so it is a drain
      that never progresses, not a quarantine.
    * Overflow is scan-order dependent *on the server itself*: `sum(v)` over
      `{'2147483647 days', '1 days', '-1 days'}` raises ascending and
      returns `2147483647 days` descending.

    Recompute-only removes both: `probe_recompute_fields_bulk` renders
    `(sum(<col>))::text` and lets Postgres fold the group in one pass, so
    Trellis raises exactly when a server-side `sum()` over the same rows
    would. Where #112's floats are *total but inexact*, interval is *exact
    but partial* — either way there is no inverse to delta with.
  * **Literals:** `TIME`/`TIMETZ` join the allowlist; `TIMESTAMPTZ` and
    `INTERVAL` do not. `INTERVAL`'s blocker is on the *input* side and is
    its own: `interval_in` reads `IntervalStyle`, so `'-1 2:03:04'` is
    `-1 days +02:03:04` under `postgres` and a different value under
    `sql_standard` — a definition installed against one server would not
    stay correct read on another. (A `timetz` offset literal is canonical
    when its trailing all-zero tail is dropped and only then:
    `+05:00:30` is what `timetz_out` prints, while `+05:30:00` is not.)
  * `crate::temporal` owns the comparison order, the `interval` value model
    and `interval_out`'s rendering; unlike #111/#112 it mints **no new
    `ValueType` variant**, because every role added here is a function of
    the family alone, which `PgType` already carries. Cross-checked against
    a live server in `trellis/tests/defs_temporal.rs`.
* **Aggregate maintenance** — order-sensitive aggregates (`array_agg`,
  `string_agg`, `jsonb_agg`) need an incremental-delta design or a
  group-recompute fallback, and an `ORDER BY`-inside-aggregate grammar decision.
</content>
</invoke>
