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
   `ValueType::Other` family **except `oid` and `bytea`** — `::text` matching
   disagrees with those types' own `=` (`'1 day'::interval = '24 hours'`,
   `timestamptz` under the session's `TimeZone`), and `json` has no `=` at
   all, whereas `oid_out` is canonical unsigned decimal (#111) and
   `byteaout` under the pinned `bytea_output = 'hex'` is a bijection with no
   session-GUC dependence at all (#114). #112 tightened
   the same gate for `real`/`double precision`, which used to slip through
   as `ValueType::Numeric`: `-0` and `0` are `=` in Postgres but render as
   `'-0'` and `'0'`, so a `::text`-matched float key splits one Postgres
   group into two target rows.
4. **Primary key** — identify a target row. A stricter join key: it must come
   from the source's replica identity and present in the old-image for
   updates/deletes. Gated to the join-key-safe allowlist
   (`is_text_stable_join_key_type`) — an unsafe single-column PK
   (`numeric`/`timestamptz`/`interval`, which `::text`-matching
   would silently mismatch) is rejected at define time with
   `DdlError::UnsupportedPrimaryKeyType` (#107). The typed key index
   (below) later unlocks those: it compares
   *decoded values*, so a rendering that is ambiguous (`'1 day'` vs
   `'24 hours'`) or merely inconsistent between two renderers stops
   mattering. What the index does not help with is a value whose *ordering*
   is undefined, and no type here has that problem. `bytea` (#114) needed
   neither the allowlist wait nor the index: its rendering was already a
   bijection once `bytea_output` was pinned, so it went straight onto the
   allowlist alongside `oid`.
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
| `boolean` | ✅ | 🎯 | ✅ `GROUP BY` only; ⚠️ relationship/PK | ⚠️ (rare) | ✅ | ✅ `bool_and`/`bool_or` (recompute-only) | `ValueType::Boolean` predates #119; has a second `::text` renderer no other admitted type does (#119) |
| `uuid` | ✅ | 🎯 | ✅ | ✅ | ✅ | ⚠️ MIN/MAX | landed in #79 |
| `text` `varchar` | ✅ | 🎯 | ✅ | ✅ | ✅ | 🎯 MIN/MAX/`string_agg` | requires deterministic collation |
| `char(n)` `citext` | ✅ | ⚠️ | ❌ padding/case | ❌ | ⚠️ passthrough | — | hazard is padding/case, not volatility |
| `bytea` | ✅ (hex text) | 🎯 | ✅ | ✅ | ✅ literal (#109) | ❌ no such Postgres aggregate | `bytea_output` pinned to `hex`, whose rendering is a bijection (#114); literal must be canonical lowercase hex; Postgres has no `min(bytea)`/`max(bytea)` despite `bytea` having a full btree opclass |
| `date` | ✅ | 🎯 | ✅ | ✅ | ✅ literal (#109) | ✅ MIN/MAX | key roles landed in #113 — no typed index needed. `DATE 'YYYY-MM-DD'` only; `'today'` is why `date_in` is STABLE |
| `time` `timetz` | ✅ | 🎯 | ✅ | ✅ | ✅ literal (#113) | ✅ MIN/MAX | `time_out`/`timetz_out` are the block's only IMMUTABLE output functions; `timetz`'s `=` is identity on `(time, zone)` |
| `timestamp` | ✅ | 🎯 | ✅ | ✅ | ✅ literal (#109) | ✅ MIN/MAX | key roles landed in #113/#248 — bijective under `::text`, and `to_jsonb` (the live-row reads) now renders it the same way (#248 replaced `to_jsonb(t.*)` with an explicit per-column `jsonb_build_object`) |
| `timestamptz` | ✅ | 🎯 | 🎯 typed index | 🎯 typed index | 🎯 | 🎯 MIN/MAX | #248 fixed the `to_jsonb` split, but a *second*, independent defect remains: a `TimeZone`-dependent render on a walsender Trellis cannot pin (#113, #246, still open) |
| `interval` | ✅ | 🎯 | 🎯 typed index | 🎯 typed index | 🎯 | ✅ `SUM` (recompute-only); ❌ MIN/MAX | `'24 hours' = '1 day'` but they render differently, so no *text* match works; #110 comparing decoded values would. `max` is scan-order dependent even on the server (#113) |
| `jsonb` | ✅ | 🎯 | ⚠️ typed index | ⚠️ | 🎯 needs a canonicalizer | ⚠️ `jsonb_agg` STABLE in PG | `json` excluded (no `=`); `jsonb_out` re-sorts keys, so a literal needs #115's value model |
| `inet` | ✅ | 🎯 | ✅ `GROUP BY` only; ❌ relationship/PK | ❌ | ✅ | ✅ MIN/MAX (own type) | second `::text` renderer (`network_show`) disagrees with `inet_out` on bare host addresses — the `boolean` shape (#116) |
| `cidr` | ✅ | 🎯 | ✅ | ✅ | ✅ | ❌ MIN/MAX | `cidr_out`/`::text` never diverge, unlike `inet`; `min`/`max(cidr)` only reachable via Postgres's own implicit upcast to `inet`, which would change the result's type (#116) |
| `macaddr` `macaddr8` | ✅ | 🎯 | ✅ | ✅ | ✅ | ❌ no such Postgres aggregate | no `pg_cast` `::text` override, unlike `inet`/`cidr`; `bytea`'s "opclass but no aggregate" finding repeats exactly (#116) |
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
* **`timestamptz`** — value comparison immutable, but *text rendering*
  is GUC-dependent (`TimeZone`). `TimeZone` deliberately is not pinned
  (#113, below), so for `timestamptz` a
  **typed key index** — comparing decoded values, not text — is the real
  unlock.
* **`bytea`** — looked like it belonged in the bullet above (its text
  rendering is also GUC-dependent, on `bytea_output`), and #114 found that
  assumption wrong on closer inspection: `bytea_output` *is* pinned (to
  `hex`), and unlike `TimeZone` that pin is enough on its own — `byteaout`
  under `hex` is a bijection with no second axis of variation the way
  `timestamptz_out` has (`TimeZone` *and* the walsender/pool split, #246).
  No typed key index needed; see "Bytea semantics" below.
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
  `timestamptz`/`numeric`/`real`/`double precision`/… as safe keys;
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
* **Bytea semantics (#114)** — the epic's own scope note for this issue
  ("text/wire rendering depends on the `bytea_output` GUC -> key role needs
  decoded comparison") predicted the same `timestamptz`/`interval` shape
  #110's typed key index exists for. Asked of a live server, per #111's
  playbook rather than assumed from the note, that prediction was wrong:

  * **`byteaout` under the pinned `bytea_output = 'hex'` is a bijection, with
    no second renderer and no second GUC to go wrong.** Every distinct byte
    string has exactly one canonical spelling — `\x` followed by an even
    number of lowercase hex digits — and every such spelling names exactly
    one byte string. There is no `bytea` analogue of float's `-0`/`0` or
    interval's `'1 day'`/`'24 hours'`: a positional, fixed-width-per-byte
    encoding structurally cannot produce two spellings of one value the way
    a variable-width or field-based one can. Verified live across a grid
    spanning the empty value (`\x`, distinct from `NULL`), embedded `NUL`
    bytes, and the full `0x00..=0xff` byte range: `count(distinct v)` and
    `count(distinct v::text)` agree exactly, and `ORDER BY v` and
    `ORDER BY v::text` produce the identical row order — ASCII orders hex
    digits `'0'..'9' < 'a'..'f'` in exactly the order their nibble values
    need, so lexicographic *text* comparison already reproduces `bytea`'s
    own `bytea_cmp` (`trellis/tests/defs_bytea.rs`).
  * **The render-consistency half — `to_jsonb` versus `::text` — already
    agreed for `bytea`, both before and after #248.** `to_jsonb`'s special
    ISO-8601 datetime writer is what broke `timestamp`/`timestamptz`;
    `bytea` was never routed through it — `to_jsonb` calls a value's own
    output function for every non-numeric, non-datetime scalar type, so
    `to_jsonb(bytea_col)` and `bytea_col::text` were always the same string.
    `staging::apply::row_as_text_jsonb_sql` (#248's fix) now builds every
    row read as an explicit per-column `<col>::text`, which was already
    `bytea`'s only renderer — so #114 needed no renderer-reconciliation work
    at all, unlike #113/#248.
  * **No typed key index needed.** `bytea` went straight onto
    `catalog::TEXT_STABLE_JOIN_KEY_TYPES` and the `GROUP BY` key admit list
    alongside `oid`, the same way four of the six temporal families did in
    #113 — #110's typed key index was never the prerequisite the epic's
    framing assumed.
  * **`MIN`/`MAX(bytea)` is `❌`, not `⚠️`, and not because of any rendering
    hazard.** `bytea` has a full btree opclass — `<`/`>`/`=`/`ORDER BY` all
    work and are `IMMUTABLE` — but Postgres never wired a
    `min(bytea)`/`max(bytea)` aggregate to it: `select min(v) from (values
    ('\x00'::bytea)) t(v)` is `ERROR: function min(bytea) does not exist` on
    a live Postgres 17, and no `pg_proc` row named `min`/`max` takes a lone
    `bytea` argument. ADR-0004 admits a subset of Postgres's own grammar;
    `MIN`/`MAX(bytea)` has no server-side construct to be a subset of, so
    `registry::aggregate_result_type` returns `None` for it and the
    validator reports the ordinary `FunctionArgTypeMismatch` any
    aggregate/type mismatch gets — no new error variant needed. This is
    independent of the join/PK/`GROUP BY` key roles, which only need
    equality and hold it regardless.
  * `bytea` mints **no new `ValueType` variant**, for the same reason the
    temporal families didn't (#113's module doc): every role it gained here
    is a function of the family alone, which `PgType::Bytea` already
    carries, and it needs no value model beyond the hex text it already
    passes through end to end. Cross-checked against a live server in
    `trellis/tests/defs_bytea.rs`.
* **Boolean semantics (#119)** — the epic's scope note read `boolean` as the
  simplest case left: `boolout` renders exactly two values (`'t'`/`'f'`), an
  obvious bijection, so on the "text-stability is a property of the
  rendering" reasoning #111/#113/#114 each confirmed for their own families,
  it looked like a straight allowlist addition. Asked of a live server, that
  reasoning turned out to have an unstated assumption none of the previous
  four type-family issues had reason to test: that a type's `::text` cast
  *is* its output function. For every type this epic had touched so far,
  that assumption holds. `boolean` is the one exception:

  * **`boolean` has two independent, disagreeing renderers**, verified
    against `pg_cast` rather than assumed: `select castfunc::regproc from
    pg_cast where castsource = 'boolean'::regtype and casttarget =
    'text'::regtype` names `pg_catalog.text(boolean)`, a *second*, dedicated
    cast function Postgres ships only for `boolean` (`'true'`/`'false'`),
    distinct from `boolout` (`'t'`/`'f'`, what CDC/`pgoutput` decodes and
    what `intake::extract_key` stores verbatim). Every other type swept —
    `smallint`/`integer`/`bigint`/`oid`/`uuid`/`text`/the temporal
    families/`bytea` — has no `pg_cast` row for `text` at all; their `::text`
    *is* their output function. This is a structurally new defect shape for
    the epic: not a GUC dependence (`timestamptz`, `bytea`'s ruled-out
    prediction), not a rendering-vs-equality mismatch (`interval`, float
    `-0`/`0`), but a second, wholly independent cast function that silently
    disagrees with the first.
  * **The join/primary-key role stays refused, and this is why `boolean` is
    *not* added to `catalog::TEXT_STABLE_JOIN_KEY_TYPES`.** `intake::
    extract_key` builds a CDC-derived key's text from the wire tuple
    verbatim (`boolout`'s `'t'`/`'f'`), and several of `staging::apply`'s
    scalar single-key lookups (`check_reverse_guards` and its siblings)
    still compare that against a live column's `{col}::text` rendering
    (`pg_catalog.text(boolean)`'s `'true'`/`'false'`) — two different
    strings for one value, never matching. Issue #125's bulk `key_array_filter`
    already sidesteps this for its own lookups (it casts the *bound array*
    to the column's native type, so `boolin` — permissive enough to accept
    both spellings — reconciles them before comparing), but the remaining
    scalar sites do not. Admitting `boolean` today would repeat #248's "one
    value, two renderers, no arbiter" defect shape, from a renderer pair no
    earlier lane had reason to find. Extending `key_array_filter`'s pattern
    to every scalar lookup (or #110's typed key index, which would subsume
    it) is the prerequisite; until then the join/PK-key cells stay
    unchecked. `trellis/tests/defs_boolean.rs`'s
    `boolean_is_refused_as_a_relationship_join_key_and_primary_key` pins the
    refusal as deliberate, not an oversight.
  * **The `GROUP BY` key role's *final SQL* comparison was already safe**
    (`validate::reject_unsupported_group_by_key_type` admitted
    `ValueType::Boolean` before this issue existed), because
    `staging::apply_aggregate`'s keyset match always binds the group key as
    a native-typed array (`$1::text[]::boolean[]`), the same `boolin`
    reconciliation `key_array_filter` uses. That was necessary but not
    sufficient: review for this issue found a **second, genuinely live**
    instance of the same defect shape one layer earlier, entirely in
    memory. `staging::apply_aggregate::accumulate_changes` buckets one
    drain batch's touched rows into `GroupPlan`s keyed by
    `derive_group_key`'s own `text` — a bare Rust `HashMap` key compared
    byte-for-byte, with no database (and so no `boolin`) anywhere in that
    comparison. A row that arrived with CDC's `'t'` spelling and a row that
    arrived via a bare recompute's live-read `'true'` spelling used to land
    in *two* separate `GroupPlan`s that both then independently wrote to
    the one physical row the SQL layer correctly resolved them to —
    corrupting its value (a delta add stacked on an unrelated forced
    recompute, observed live as `total = 25` where the correct answer was
    `15`) rather than visibly splitting it into two rows the way #248's
    pre-fix `timestamp` did. That is a worse failure mode than #248's,
    precisely because it is silent. `apply_aggregate::
    canonicalize_group_key_part` — a no-op for every `ValueType` but
    `Boolean` — now normalizes each `GROUP BY` part's text before it folds
    into that dedup key, closing the gap at its source rather than only at
    the SQL boundary. `trellis/tests/defs_boolean.rs`'s
    `a_boolean_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two`
    reproduces the corruption end-to-end and pins the fix; a matching
    no-DB unit test
    (`derive_group_key_normalizes_every_boolean_spelling_to_the_same_dedup_key`)
    pins the root cause directly.
  * **`bool_and`/`bool_or` are recompute-only**, on `MIN`/`MAX`'s reasoning,
    not `SUM`'s. Both are total, commutative, associative folds over
    `{true, false}` with no overflow/rounding/partial-monoid hazard — the
    kind of shape that looks delta-able the way `SUM` is. The trap is
    deletion: the only state an invertible model here could maintain is the
    aggregate's own current one-bit value, and that is not enough to invert
    a delete. Concretely, two different single-row deletions from the same
    `{false, true, true}` group (`bool_and = false`) land at two different
    true answers — delete the `false` row and the group becomes `true`;
    delete either `true` row instead and it stays `false` — so "the old
    aggregate was `false`" alone cannot tell a delta model which case it is
    in. This is `Invertibility::RecomputeOnly`'s own "a deleted row might
    have held the current min/max, and there's no way to recover the
    next-best value from the aggregate's current state alone," verbatim,
    with `true`/`false` standing in for a min/max candidate. A design
    tracking hidden true-count/false-count partials *would* be genuinely
    invertible (`bool_and` is exactly "false-count `== 0`", decrementable on
    delete) but needs a new composite-aggregate shape `defs::invertibility::
    PartialField` was never built for, plus matching `staging::
    apply_aggregate` carrier/probe plumbing — considered and deferred as a
    speculative expansion beyond this issue's scope, not attempted.
    Recompute-only costs nothing extra to wire up: `registry::
    aggregate_result_type` routes `bool_and`/`bool_or` straight into
    `staging::apply_aggregate`'s ordinary `AggFieldKind::RecomputeOnly`
    fallback, the same free ride float `SUM`/`AVG` and `SUM(interval)` get.
    Live-verified in `trellis/tests/defs_boolean.rs`, including the
    concrete two-deletions demonstration
    (`bool_and_or_deletion_cannot_be_inverted_from_the_aggregate_alone`).
  * `boolean` mints **no new `ValueType` variant** — `ValueType::Boolean`
    already existed as a computed-target type before this issue (the
    epic's own framing), and every role gained here is a function of the
    type alone. Cross-checked against a live server in
    `trellis/tests/defs_boolean.rs`.
* **Network-address semantics (#116)** — the epic's scope note grouped
  `inet`/`cidr`/`macaddr`/`macaddr8` as one family and marked `MIN`/`MAX`
  `⚠️` across the board. Asked of a live server per family, per #111–#119's
  playbook, they land three different ways — the epic's most differentiated
  outcome yet, and the first issue since #119 to find a *second* type with
  boolean's exact hazard shape:

  * **`pg_cast`, checked for all four, not assumed.** `select
    castfunc::regproc from pg_cast where castsource in ('inet', 'cidr',
    'macaddr', 'macaddr8')::regtype[] and casttarget = 'text'::regtype`
    names `pg_catalog.text(inet)` (`prosrc = network_show`) for **both**
    `inet` and `cidr` — Postgres reuses one cast function because `cidr`'s
    on-disk representation *is* an `inet` with host bits forced to zero —
    and no row at all for `macaddr`/`macaddr8`, whose `::text` is therefore
    `macaddr_out`/`macaddr8_out` directly, already bijective (every accepted
    input spelling — colon/hyphen/dot-grouped/bare hex — normalizes to one
    canonical lowercase colon-separated output). They join
    `catalog::TEXT_STABLE_JOIN_KEY_TYPES` the way `bytea`/`oid` did.
  * **Sharing a `pg_cast` row does not mean sharing its divergence — that
    turned out to be a per-type fact, checked live rather than inferred from
    the row.** `inet_out` (what CDC/`pgoutput` decodes, what
    `intake::extract_key` stores verbatim) omits the `/prefixlen` suffix
    exactly when the stored netmask covers the whole address
    (`'192.168.1.5'::inet::text` via `inet_out` is `192.168.1.5`), while
    `network_show` — the shared cast, what `<col>::text` and hence
    `staging::apply::row_as_text_jsonb_sql`'s live reads call — always
    prints it explicitly (`192.168.1.5/32`). `cidr_out` never omits the
    netmask in the first place (a `cidr` value's entire point is that the
    network prefix is significant), so `cidr_out(v)::text = v::text` holds
    unconditionally — verified live across a v4/v6 grid including the
    host-bits-zero-only values `cidr_in` alone accepts. `inet` repeats
    `boolean`'s exact defect shape (#119) — one value, two renderers, no
    arbiter, `staging::apply::check_reverse_guards` and its scalar siblings
    still doing raw `{col}::text = $1` matching — and is refused as a
    relationship/primary key for the identical reason. `cidr` is not, and
    joins the allowlist cleanly.
  * **`inet`'s `GROUP BY` key role is nonetheless admitted — the same split
    `boolean` got, for the same mechanical reason.**
    `staging::apply_aggregate`'s keyset match never does raw-text
    comparison; it casts the *bound array* to the column's native type
    (`$1::text[]::inet[]`), and `inet_in` is permissive enough to parse both
    spellings back to the identical stored value
    (`'192.168.1.5'::inet = '192.168.1.5/32'::inet` is `true`, verified
    live). That reconciles the SQL half automatically but not
    `staging::apply_aggregate::accumulate_changes`'s in-memory `GroupPlan`
    bucketing, which compares `derive_group_key`'s text byte-for-byte with
    no database in the loop — `boolean`'s exact live bug shape. This issue
    adds an `inet` arm to `apply_aggregate::canonicalize_group_key_part`
    (backed by `crate::netaddr::canonicalize_group_key_text`) alongside
    `boolean`'s, closing the gap the same way #119 did, and
    `trellis/tests/defs_netaddr.rs`'s
    `an_inet_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two`
    reproduces the shape end-to-end and pins the fix, the same way
    `defs_boolean.rs`'s equivalent test does for `boolean`.
  * **`MIN`/`MAX` diverge three ways, none of them the epic's original
    `⚠️` guess.** `inet` has a real `min(inet)`/`max(inet)` that keeps its
    own type (`pg_typeof(min(v))` is `inet`, verified live) — the epic's
    `min`/`max`-keeps-argument-type rule holds, and `crate::netaddr::compare`
    reproduces `network_cmp`'s three-level tie-break (cross-family by
    `family`, same-family by the shared prefix up to the smaller netmask,
    then by netmask length, then by the full address) closely enough that
    the Rust-side fold matches a server-side `min`/`max` byte-for-byte
    across a grid exercising every tier
    (`trellis/tests/defs_netaddr.rs`'s
    `inet_min_max_keeps_its_type_and_the_fold_matches_a_server_side_aggregate`).
    `cidr` has **no** `min(cidr)`/`max(cidr)` of its own — only Postgres's
    own implicit upcast to `inet` reaches one (`castcontext = 'i'` in
    `pg_cast`), and `pg_typeof(min(cidr_col))` is demonstrably `inet`, not
    `cidr` — a genuinely new shape none of the epic's other `MIN`/`MAX`
    refusals have: the aggregate exists, but only for a *different* type
    than the column's own. Admitting it would mean
    `registry::aggregate_result_type` silently changing a `cidr` computed
    field's declared type to `inet`, which this issue declines rather than
    invent speculatively — `❌`, not `⚠️`, by the same "no construct to be
    a subset of" reasoning #114 used for `bytea`, applied one type up.
    `macaddr`/`macaddr8` repeat `bytea`'s finding outright: a full,
    `IMMUTABLE` btree opclass each (`macaddr_ops`/`macaddr8_ops`), but no
    `min`/`max` aggregate wired to either — `select min(v) from (values
    ('08:00:2b:01:02:03'::macaddr)) t(v)` is `ERROR: function min(macaddr)
    does not exist` on a live Postgres 17, and no `pg_proc` row names
    `min`/`max` over a lone `macaddr`/`macaddr8` argument.
    `registry::aggregate_result_type` returns `None` for both.
  * **`to_jsonb` agrees with `::text` for `cidr`/`macaddr`/`macaddr8`, and
    disagrees for `inet`.** `to_jsonb` calls a value's own output function
    for every non-numeric, non-datetime scalar type — the same fact #114
    established for `bytea` — so for `inet` it renders through `inet_out`,
    not through `pg_cast`'s `network_show` override, and inherits `inet_out`'s
    host-address elision the same way `inet_out` itself does. This is
    `to_jsonb`-versus-`inet_out` agreement, not a *third* renderer: the real
    conflict remains `inet_out` (⟵ CDC) versus `<col>::text` (⟵ everything
    the engine itself renders, since issue #248's
    `staging::apply::row_as_text_jsonb_sql` replaced every bare
    `to_jsonb(t.*)` row-body read with an explicit `<col>::text`, so nothing
    in the engine actually calls bare `to_jsonb` for a row body any more).
  * **Typed literals land for all four**, unlike the epic's earlier
    "each waits for its own child" placeholder in `defs::typed_literal`.
    `inet_in`/`cidr_in`/`macaddr_in`/`macaddr8_in` and their `_out`
    counterparts are all `IMMUTABLE` (`pg_proc.provolatile`), clearing the
    same bar `date`/`oid`/`bytea`/the floats did. The canonical-form checker
    for each accepts exactly the spelling that type's actual live renderer
    emits — `network_show`'s always-explicit-netmask form for `INET`/`CIDR`
    (not `inet_out`'s elided one), and `macaddr_out`/`macaddr8_out`'s one
    lowercase colon-grouped spelling for the other two. `INET`/`CIDR`
    additionally round-trip through Rust's own `std::net::IpAddr` parser for
    the address part, which agrees with Postgres's rendering on every value
    tried live except the deprecated IPv4-compatible form (`::192.168.1.1`,
    distinct from the IPv4-*mapped* `::ffff:192.168.1.1`, which does agree)
    — the checker rejects that one form outright rather than risk silently
    mis-canonicalizing it, the same "reject rather than guess" posture every
    other canonical checker in `defs::typed_literal` takes.
  * `inet`/`cidr`/`macaddr`/`macaddr8` mint **no new `ValueType` variant** —
    every role gained here is a function of the family alone, which
    `PgType` already carries, the same reasoning `crate::temporal`'s and
    `bytea`'s module docs give for their own families. `crate::netaddr` owns
    the ordering (`inet`'s `MIN`/`MAX`), the `GROUP BY` canonicalization, and
    the four typed-literal checkers. Cross-checked against a live server in
    `trellis/tests/defs_netaddr.rs`.
* **Aggregate maintenance** — order-sensitive aggregates (`array_agg`,
  `string_agg`, `jsonb_agg`) need an incremental-delta design or a
  group-recompute fallback, and an `ORDER BY`-inside-aggregate grammar decision.
</content>
</invoke>
