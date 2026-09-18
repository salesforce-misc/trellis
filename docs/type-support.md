# Data-type support matrix

Which PostgreSQL data types Trellis supports, in **which role**. This is a
living reference — updated as each type/role lands. The epic that drives it is
[Epic: Data-type support (#123)](https://github.com/salesforce-misc/trellis/issues/123);
the constraint that decides which
types can even be candidates is
[ADR-0004](decisions/0004-transform-definition-grammar.md).

A type is never simply "supported." It earns its way into six capabilities,
ordered here **floor → ceiling** — each row of the matrix doubles as a maturity
ladder, and most types climb it left-to-right:

1. **Ingest / passthrough** — faithfully decode the value from logical
   replication and copy it into a derived table unchanged. The floor under every
   other role. Today nearly everything clears it: unmapped types fall through to
   `Text` and the target keeps the source's concrete PG type
   (`trellis/src/defs/ddl.rs:293-324`).
2. **Filter (predicate)** — appear in a `WHERE` / partial-data predicate
   (`created_at > '2020-01-01'`). Needs immutable comparison operators and the
   predicate grammar (stubbed to `TRUE` today, `ast.rs:185`).
3. **Join / relationship / `GROUP BY` key** — be matched across rows by
   equality. Today matched by raw `::text` rendering
   (`TEXT_STABLE_JOIN_KEY_TYPES`, `trellis/src/defs/catalog.rs:2156`;
   `to_rows_by_key`, `eval.rs:65-106`).
4. **Primary key** — identify a target row. A stricter join key: it must come
   from the source's replica identity and be present in the old-image for
   updates/deletes. Gated to the same text-stable allowlist as join keys
   (`is_text_stable_join_key_type`) — an unsafe single-column PK type is
   rejected at define time with `DdlError::UnsupportedPrimaryKeyType` (#107).
5. **Computed 1-1 target** — a scalar calculated field *produces* a value of
   this type (distinct from passthrough). Needs an immutable evaluator arm **and**
   grammar to spell a literal/cast of the type.
6. **Aggregate target** — an aggregate calculated field folds to this type.
   Gated by fold economics: `SUM`/`COUNT`/`AVG` invertible (cheap deltas);
   `MIN`/`MAX` orderable-but-not-invertible; `array_agg`/`string_agg`/`jsonb_agg`
   order-sensitive.

> **Primary-key types are gated to the join-key-safe set** (#107).
> `source_primary_key` reuses `is_text_stable_join_key_type` (the shared source
> of truth with the join-key allowlist), so a single-column `numeric`/
> `timestamptz`/`bytea` PK — which `::text`-matching would silently mismatch — is
> rejected at define time (`DdlError::UnsupportedPrimaryKeyType`) rather than
> accepted. The typed key index (below) is what later *unlocks* those types as
> safe keys by comparing decoded values instead of text.

## Legend

| Mark | Meaning |
|---|---|
| ✅ | works today |
| 🎯 | in scope for the epic |
| ⚠️ | conditional / needs design decision |
| ❌ | excluded (immutability or no equality) |
| — | not applicable |

`COUNT(*)` is type-agnostic row-counting (`registry.rs:160`, #75) — it works for
any group and is *not* a per-type capability, so it's omitted from the aggregate
cells below.

## Matrix

| Postgres type(s) | Ingest / passthrough | Filter | Join key | Primary key | Computed 1-1 | Aggregate | Notes |
|---|---|---|---|---|---|---|---|
| `smallint` `integer` `bigint` | ✅ | 🎯 | ✅ | ✅ | ✅ | ✅ SUM/AVG/MIN/MAX | 🎯 split into a typed integer (exact key round-trip) |
| `oid` | ✅ | 🎯 | 🎯 | 🎯 | 🎯 | — | behaves like `int`; low priority |
| `numeric` `decimal` | ✅ | 🎯 | 🎯 typed index | 🎯 typed index | ✅ | ✅ | rejected as PK today; `1.0`≠`1.00` under text match |
| `real` `double precision` | ✅ | 🎯 | 🎯 typed index | ⚠️ NaN/±0 | 🎯 (distinguish from `numeric`) | 🎯 | IEEE edge cases need a decision |
| `boolean` | ✅ | 🎯 | 🎯 | ⚠️ (rare) | ✅ | 🎯 `bool_and`/`bool_or` | value type exists |
| `uuid` | ✅ | 🎯 | ✅ | ✅ | ✅ | ⚠️ MIN/MAX | landed in #79 |
| `text` `varchar` | ✅ | 🎯 | ✅ | ✅ | ✅ | 🎯 MIN/MAX/`string_agg` | requires deterministic collation |
| `char(n)` `citext` | ✅ | ⚠️ | ❌ padding/case | ❌ | ⚠️ passthrough | — | hazard is padding/case, not volatility |
| `bytea` | ✅ (hex text) | 🎯 | 🎯 typed index | 🎯 typed index | 🎯 | ⚠️ MIN/MAX | `bytea_output` GUC affects text render |
| `date` | ✅ | 🎯 | 🎯 typed index | 🎯 typed index | 🎯 | 🎯 MIN/MAX | |
| `timestamp` `timestamptz` | ✅ | 🎯 | 🎯 typed index | 🎯 typed index | 🎯 | 🎯 MIN/MAX | tz text render is TZ-dependent |
| `time` `timetz` `interval` | ✅ | 🎯 | 🎯 | ⚠️ | 🎯 | 🎯 MIN/MAX; `SUM(interval)` | interval eq well-defined; fidelity is the risk |
| `jsonb` | ✅ | 🎯 | ⚠️ typed index | ⚠️ | 🎯 | ⚠️ `jsonb_agg` STABLE in PG | `json` excluded (no `=`) |
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

* **`money`** — comparison (`cash_eq`/`cash_cmp`) *is* immutable; the **text I/O**
  (`cash_out`) is `STABLE` (`lc_monetary`). Since CDC decodes values as text, the
  decoded representation is locale-dependent. Excluded; use `numeric`.
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

* **Casts / coercion lattice** — computing a value of a new type needs literal
  and `CAST` grammar (numeric + quoted-text literals only today); tracked as its
  own epic child.
* **Typed key index** — replacing the raw-`::text` match unlocks
  `timestamp`/`bytea`/`date`/`numeric`/… as safe keys; the single highest-leverage
  child for the key roles.
* **Aggregate maintenance** — order-sensitive aggregates (`array_agg`,
  `string_agg`, `jsonb_agg`) need an incremental-delta design or a
  group-recompute fallback, and an `ORDER BY`-inside-aggregate grammar decision.
