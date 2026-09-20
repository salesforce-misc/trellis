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
   (`numeric`/`timestamptz`/`bytea`, which `::text`-matching would silently
   mismatch) is rejected at define time with `DdlError::UnsupportedPrimaryKeyType`
   (#107). The typed key index (below) later unlocks those types as safe keys.
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
| `date` | ✅ | 🎯 | 🎯 typed index | 🎯 typed index | ✅ literal (#109) | 🎯 MIN/MAX | `DATE 'YYYY-MM-DD'` only; `'today'` is why `date_in` is STABLE |
| `timestamp` `timestamptz` | ✅ | 🎯 | 🎯 typed index | 🎯 typed index | ✅ literal (#109), `timestamptz` 🎯 | 🎯 MIN/MAX | tz text render is TZ-dependent, so only plain `timestamp` can be spelled |
| `time` `timetz` `interval` | ✅ | 🎯 | 🎯 | ⚠️ | 🎯 | 🎯 MIN/MAX; `SUM(interval)` | interval eq well-defined; fidelity is the risk |
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
  is GUC-dependent (`TimeZone`, `bytea_output`). This is why a **typed key
  index** — comparing decoded values, not text — is the real unlock.
* **`jsonb_agg`** — marked `STABLE` in Postgres; whether Trellis's own
  deterministic reimplementation may treat it as immutable is an open question.
* **`xml`/`tsvector`/`tsquery`** — no useful immutable equality/ordering; niche.

## Cross-cutting concerns

* **Casts / coercion lattice** — computing a new type needs literal and `CAST`
  grammar. **Landed for literals (#109):** `DATE '2024-01-01'` and the
  equivalent `CAST('2024-01-01' AS date)` both produce a real typed constant,
  over an allowlist (`date`, `timestamp`, `bytea`, and `oid` since #111)
  whose literal text must be
  in Postgres's canonical output spelling — see
  [ADR-0004](decisions/0004-transform-definition-grammar.md#typed-literals-issue-109).
  Still open: a **general** coercion lattice (`CAST(<expr> AS <type>)` over a
  non-literal), which each type family's own child decides, since most pairs
  are not immutable.
* **Typed key index** — replacing the raw-`::text` match unlocks
  `timestamp`/`bytea`/`date`/`numeric`/… as safe keys; the single
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
  and are refused.
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
* **Aggregate maintenance** — order-sensitive aggregates (`array_agg`,
  `string_agg`, `jsonb_agg`) need an incremental-delta design or a
  group-recompute fallback, and an `ORDER BY`-inside-aggregate grammar decision.
</content>
</invoke>
