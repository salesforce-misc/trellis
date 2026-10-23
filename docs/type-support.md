# Data-type support matrix

Which PostgreSQL data types Trellis supports, in which role, and why the
roles that are refused are refused. The candidacy constraint is
[ADR-0004](decisions/0004-transform-definition-grammar.md): an operator,
function or type role is admitted only with semantics identical to stable
PostgreSQL, immutable, and with a re-implemented evaluator.

A type is never simply "supported". It has a verdict in each of five roles,
ordered floor to ceiling:

1. **Passthrough** — captured as text and copied into a derived table
   unchanged. Nearly every type clears it (see
   [Unreferenceable types](#unreferenceable-types) for the exceptions).
2. **Join / `GROUP BY` key** — matched by equality. A relationship join column
   must be on the join-key allowlist (`TEXT_STABLE_JOIN_KEY_TYPES` in
   `trellis/src/defs/catalog.rs`, by Postgres type name, plus any enum). A
   `GROUP BY` key is gated on its `ValueType` instead
   (`validate::reject_unsupported_group_by_key_type`).
3. **Primary key** — identifies a row. A source's key must be on the same
   allowlist as join columns (`DdlError::UnsupportedPrimaryKeyType`). The role
   covers every table whose rows Trellis keys: both endpoints of a
   relationship too, including an aggregate target whose `GROUP BY` key is
   off the allowlist.
4. **Computed 1-1 target** — a scalar calculated field produces this type.
   Needs an immutable evaluator arm and, for a constant, a typed literal.
5. **Aggregate target** — an aggregate calculated field folds to this type.

A `WHERE` predicate other than `TRUE` is not supported
([transforms](transforms.md#supported-sources-and-targets)), so no type has a
filter role.

## Matrix

| Mark | Meaning |
|---|---|
| yes | supported |
| no | refused at define time |
| n/a | no such construct in Postgres, or not meaningful |

| Postgres type(s) | Passthrough | Join key | `GROUP BY` key | Primary key | Computed 1-1 | Aggregates |
|---|---|---|---|---|---|---|
| `smallint` `integer` `bigint` | yes | yes | yes | yes | yes | `SUM` `AVG` `MIN` `MAX` |
| `oid` | yes | yes | yes | yes | typed literal only | no (no arithmetic in Postgres) |
| `numeric` `decimal` | yes | no | yes | no | yes | `SUM` `AVG` `MIN` `MAX` |
| `real` `double precision` | yes | no | no | no | yes | `SUM` `AVG` `MIN` `MAX` (recompute) |
| `boolean` | yes | no | yes | no | yes | `bool_and` `bool_or` (recompute) |
| `uuid` | yes | yes | yes | yes | yes | no `MIN`/`MAX` |
| `text` `varchar` `char(n)` | yes | `text` `varchar` only | yes | `text` `varchar` only | yes | `MIN` `MAX` `array_agg` `string_agg` (recompute) |
| `bytea` | yes | yes | yes | yes | typed literal | no `MIN`/`MAX` |
| `date` `time` `timetz` `timestamp` `timestamptz` | yes | yes | yes | yes | yes; typed literal for all but `timestamptz` | `MIN` `MAX` |
| `interval` | yes | no | no | no | yes | `SUM` (recompute); no `MIN`/`MAX` |
| `jsonb` | yes | no | no | no | typed literal | `jsonb_agg` of a `jsonb` argument (recompute) |
| `inet` | yes | no | yes | no | typed literal | `MIN` `MAX` (recompute) |
| `cidr` | yes | yes | yes | yes | typed literal | no `MIN`/`MAX` |
| `macaddr` `macaddr8` | yes | yes | yes | yes | typed literal | no `MIN`/`MAX` |
| `bit` | yes | yes | no | yes | no | `bit_and` `bit_or` (recompute, result `bit varying`) |
| `bit varying` | yes | yes | yes | yes | typed literal | `bit_and` `bit_or` (recompute) |
| enum types | yes | yes | yes | yes | no | `MIN` `MAX` (recompute) |
| `json` `money` `xml` `tsvector` `tsquery` | yes | no | no | no | passthrough only | no |
| array, range, composite, domain, geometric, extension types (`citext`) | no | no | no | no | no | no |

`COUNT(*)` and `COUNT(<expr>)` are type-agnostic and typed `bigint`, as in
Postgres; they are omitted from the aggregate column. `MIN`/`MAX` over `text`
and enum are refused when wrapped around a to-many relationship path in a 1-1
target (every other aggregate, including the boolean, bit and network-address
reducers, folds there;
`TextAggregateOverToManyRelationshipUnsupported`,
`EnumAggregateOverToManyRelationshipUnsupported`): the DB-less evaluator that
folds those has no collation or enum order to consult. "Recompute" means the
aggregate has no exact inverse, so a group is recomputed by Postgres itself
(a server-side `min()`, `sum()` and so on) rather than updated by a delta
(`defs::invertibility`).

### The types of the columns Trellis creates

A passthrough and a 1-1 target's key columns take their source column's type
at define, with no type modifier (a passthrough an `ALTER TRANSFORM … ADD` adds
takes it the same way). A `numeric(10,2)`, a `timestamp(3)`, a `char(5)`, a
`bit(8)`, a `varchar(40)` and an `interval(3)` column are each copied as
`numeric`, `timestamp`, `bpchar`, `"bit"`, `character varying` and `interval`.
A 1-1 target's key columns keep the source column's collation; a passthrough
takes the database default. A modifier constrains what the source can
hold. It plays no part in a comparison, a join or an index, and Postgres rounds
silently when it assigns a value into a `numeric(p,s)`, `timestamp(p)`,
`timestamptz(p)`, `time(p)`, `timetz(p)` or `interval` column, so a copy that
kept the modifier would lose values after a catalog-only widening that fires no
trigger. The type is the source column's type OID, rendered schema-qualified
whatever the session's `search_path`, by one function (`defs::copy_type`) that
every column Trellis creates and every cast to one goes through
([ADR-0020](decisions/0020-type-identity.md)). So `\d` on a target shows no
modifiers. A 1-1 copy holds the source's exact rendering: a `numeric` rewritten from
`1.25` to `1.2500`, an `interval` from `'1 day'` to `'24 hours'` and a float
from `-0` to `0` reach the copy, though each pair is equal as a value
([ADR-0020](decisions/0020-type-identity.md)). A `GROUP BY` key's column (in the target, its ledger and its
group-delta table) takes its value family's type: `integer` or `bigint` by width, `text` for any
string, unconstrained `numeric`, a temporal type at full precision. A to-one
relationship's projection keys by the to-side column's exact type, and holds
each to-side column read through it in that column's type. A `JOIN`
holds its `GROUP BY` and `ON` columns in each column's type and collation, in
its target and its hidden tables, with no modifier.

Two `JOIN` `ON` columns pair when they have the same type (`text` and
`varchar` count as one type) and the same deterministic collation. A type
modifier never counts: a `varchar(10)` column pairs with a `varchar(50)`, a
`text` or a Trellis target's unbounded copy of one, and a `timestamp(0)` with a
`timestamp(3)`, because the modifier plays no part in a comparison, and the
key lookups cast to the unbounded type, so no key is truncated. So widening
one side's modifier, which is routine, pauses nothing; a narrowing that
re-renders the stored keys pauses as any key column's does. `integer` against
`bigint` doesn't pair, because Trellis never casts a join key
([ADR-0020](decisions/0020-type-identity.md)).

A calculated field's column, an aggregate's or a `JOIN`'s field column and
an aggregate ledger's contribution column (typed as the aggregate's
argument) take the type
define's inference gives their expression: `SUM` over an `integer` is
`bigint`, `MIN` over one is `integer`.

None of them changes type when the source column does, with one exception:
an integer or float *width* (`integer` to `bigint`, `real` to `double
precision`, `bigint` to `numeric`), which has no unmodified form to copy as. A
change of any modifier (a wider or narrower `numeric` precision or scale, a
temporal or `interval` precision, a `char(n)` or `bit(n)` length, a `varchar`
bound or its move to `text`) changes no copy, and a widening of one needs no
pause and no rebuild. A change that re-renders the stored values (a `char(n)`
length, a `bit(n)` rewrite, a `numeric` scale, a narrower precision) rebuilds the
column's readers, or pauses one that keys by it, except that a wider `numeric`
scale pauses no `GROUP BY` key: its group rows keep the text they were written
with, which is still an equal value
([Numeric and floating point](#numeric-and-floating-point)). A width change pauses the
transforms that own the column, and `RESUME` re-types every column to the type
define would give it from the live schema before it rebuilds
([transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)).
The re-type honours the operator's `statement_timeout`: a rewrite it cancels three times
ends the resume, and the transform stays paused with the remedies in its
`capture_failure` ([recommendations](recommendations.md#statement_timeout)).

## Text rendering

Trellis moves every value as text and matches keys by exact text, so a type is
usable as a key only when its text is a bijection with its `=`: one rendering
per value, and every value with one rendering. Two things keep renderings
deterministic.

**Pinned output settings.** Every connection Trellis opens, and every capture
function (as its own `SET` clauses), pins `DateStyle = 'ISO, YMD'`,
`bytea_output = 'hex'`, `extra_float_digits = 1`, `IntervalStyle = 'postgres'`,
`TimeZone = 'UTC'`, `lc_monetary = 'C'`, `standard_conforming_strings = on` and
`xmloption = 'content'` (`pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`). A capture
image therefore does not depend on the writing session's settings, and a value
captured and the same value read back are rendered alike
(`trellis/tests/capture_parity.rs` pins one column of each family against a
golden fixture).

**One renderer.** A capture image renders a column with `format('%s', col)`,
which calls the type's output function. Every other text read of a row the
engine makes is an explicit per-column `col::text` (`jsonb_build_object`, never
a bare `to_jsonb(t.*)`, which spells `timestamp` and `timestamptz` differently).
For nearly every type `::text` is the output function. The exceptions are
types with a `pg_cast` row to `text`:

- `boolean`: `boolout` gives `t`/`f`, the cast gives `true`/`false`.
- `inet` (and `cidr`, which shares the cast function): `inet_out` omits the
  `/32` of a bare host address, `network_show` prints it. `cidr_out` never
  omits the prefix, so `cidr` has one spelling; `inet` has two.

Those two types have two spellings of one value, so they are refused as join
and primary keys. They are allowed as `GROUP BY` keys because the ledger never
matches a group by its text: it casts the key text into a typed column through
the type's input function, which accepts both spellings
(`'192.168.1.5'::inet = '192.168.1.5/32'::inet`).

Each of the following is a per-type consequence of those two rules.

## Collation

String comparison follows the column's collation, which Trellis does not track
per value. So:

- **Key columns need a deterministic collation.** Trellis matches keys by exact
  text, but a nondeterministic collation's `=` (an ICU collation with
  `deterministic = false`) treats strings that differ as equal. The rule covers
  a source's primary key, each `GROUP BY` key, each relationship join column
  and each endpoint's primary key, and is refused at define time with an error
  naming the column and collation. A deterministic non-default collation such
  as `"C"` is fine. A 1-1 target's key columns take the source key's
  collation; an aggregate target's take the database default, which is always
  deterministic.
- **`STRPOS` and `REGEXP_COUNT` need a deterministic collation** on the column
  they read, directly or through `COALESCE`, another field or a relationship
  path, because Postgres refuses them under a nondeterministic one while
  Trellis would compute them from the exact text. `CHAR_LENGTH` and
  `OCTET_LENGTH` accept any column.
- **`MIN`/`MAX` of text** is pushed down to Postgres (which orders by the
  column's collation), so it is supported wherever a group is recomputed
  server-side. It is refused around a to-many relationship path in a 1-1
  target, where only a Rust evaluator is available: that evaluator answers a
  single-distinct-value group and raises rather than assume byte order for a
  real tie.
- **`char(n)`** is treated as `text` but is not on the key allowlist: its
  blank-padding is a hazard, not a volatility one.

What happens when a collation changes after define is in
[capture by triggers](staging-and-claiming/01-capture-by-triggers.md#a-re-typed-key-column-or-an-outgrown-copy),
and what that misses in
[known correctness gaps](known-correctness-gaps.md#a-columns-values-rewritten-in-place-by-alter-column-type).

## Exact integers

`smallint`, `integer` and `bigint` are `ValueType::Integer` with Postgres's
own overflow behaviour (`trellis/src/integer.rs`). `int4 + int4` is `integer`
and raises `22003` past range; `int4 + numeric` promotes to unbounded
`numeric`. `sum(int2|int4)` is `bigint`, `sum(int8)` is `numeric`, `avg(<int>)`
is `numeric`, and `min`/`max` keep their argument's width. `oid` is an
unsigned 32-bit integer with no arithmetic; its `oid_out` is canonical
decimal, so it is text-stable and a full key. Cross-checked against a live
server in `trellis/tests/defs_exact_integers.rs`.

## Numeric and floating point

`numeric`/`decimal` is `ValueType::Numeric`. `1.0` and `1.00` are equal in
Postgres but render differently, so `numeric` is refused as a join key and
primary key. It is admitted as a `GROUP BY` key, where a group is one equal
value, as Postgres's own `GROUP BY` makes it: a group row holds one of the
equal values' texts. A group written as `1.25` stays `1.25` when a rewrite
re-renders the source's values as `1.2500`, and that is correct; a rebuild
doesn't rewrite a key whose rendering alone changed. An aggregate result is
likewise one equal value: a `SUM` over `numeric` may render `1.5` or `1.50`.
`self_check` matches an aggregate's and a `JOIN`'s groups, and compares their
results, by the type's `=`
([ADR-0013](decisions/0013-self-check-production-recompute-audit.md)).

`real` and `double precision` are `ValueType::Float` (`trellis/src/float.rs`).
Postgres, not IEEE 754, is the oracle: `NaN = NaN` is true, `NaN` sorts above
`Infinity`, and `-0 = 0`. Operators follow Postgres's families: `real + real`
is `real`, a float mixed with anything else is `double precision`, a finite
overflow raises `22003`, `sum(real)` is `real`, `avg(real)` is
`double precision`. `COALESCE` resolves a `real`/`numeric` pair to `real`,
unlike the operators, and is modelled separately. Floats are refused as every
key role: `-0` and `0` are equal but render as `'-0'` and `'0'`, which would
split one group into two target rows. `SUM` and `AVG` are recompute-only:
float addition is not associative, and `NaN`/`Infinity` are absorbing, so
there is no exact inverse. `extra_float_digits >= 1` is pinned for the
shortest-round-trip rendering `trellis::float::render` reproduces. Checked in
`trellis/tests/defs_floats.rs`.

## Temporal types

`date`, `time`, `timetz`, `timestamp` and `timestamptz` are keys in every
role, `GROUP BY` and primary key included (`temporal::is_text_stable`).

- `date_out` under the pinned `DateStyle` is a bijection (wide years, an
  explicit ` BC` suffix, `infinity` spellings). `time_out` and `timetz_out`
  read no setting. `timetz`'s `=` is identity on the stored `(time, zone)`
  pair, which is exactly what it prints.
- `timestamptz` is a bijection on the instant because `TimeZone` is pinned to
  `UTC` on both the pool and the capture function.
- `timestamp` and `timestamptz` are the two types where bare `to_jsonb`
  disagrees with `::text` (a `T` separator), which is why no engine read uses
  it. `defs_temporal.rs` sweeps the whole key allowlist for render agreement.
- `MIN`/`MAX` work for the five families above.
- **`interval`** is refused as a key: `'24 hours' = '1 day'` is true but they
  render differently, and no renderer fix helps. It has no `MIN`/`MAX`:
  `max` over `{'1 day', '24 hours', '2 hours'}` returns different answers
  depending on scan order even inside Postgres, so a byte-exact recompute
  could never settle it. `SUM(interval)` is supported and recompute-only:
  `interval` addition is exact but partial (`infinity + -infinity` raises, and
  overflow depends on scan order), which a delta cannot represent.
- Typed literals exist for `DATE`, `TIMESTAMP`, `TIME` and `TIMETZ`, not
  `TIMESTAMPTZ` (no canonical-form checker) or `INTERVAL` (`interval_in` reads
  `IntervalStyle`). The text must be in Postgres's canonical output spelling.

`crate::temporal` owns the comparison order, the `interval` value model and
`interval_out`'s rendering. Checked in `trellis/tests/defs_temporal.rs`.

## `bytea`

`byteaout` under the pinned `bytea_output = 'hex'` is a bijection (`\x` plus
lowercase hex), with no second renderer, so `bytea` is a key in every role.
There is no `min(bytea)`/`max(bytea)` in Postgres despite the btree opclass,
so `MIN`/`MAX` is refused as an ordinary argument-type mismatch. The typed
literal must be canonical lowercase hex. Checked in
`trellis/tests/defs_bytea.rs`.

## `boolean`

`ValueType::Boolean`. Two spellings (see [Text rendering](#text-rendering)):
refused as join and primary key, admitted as a `GROUP BY` key.
`bool_and`/`bool_or` are supported and recompute-only: the aggregate's current
one-bit value cannot tell which of two different deletions happened (deleting
the `false` of `{false, true, true}` makes `bool_and` true; deleting a `true`
leaves it false). Checked in `trellis/tests/defs_boolean.rs`.

## Network addresses

- `cidr`, `macaddr` and `macaddr8` have one `::text` spelling (the latter two
  normalize every input form to lowercase colon-grouped) and are full keys.
- `inet` has two spellings: refused as join/primary key, admitted as a
  `GROUP BY` key.
- `MIN`/`MAX` exist for `inet` only, keep its type, and follow `network_cmp`'s
  ordering (`crate::netaddr::compare`). `min(cidr)` only exists through an
  implicit upcast to `inet`, which would change the result's type, so it is
  refused; `macaddr`/`macaddr8` have no `min`/`max`.
- Typed literals exist for `INET`, `CIDR`, `MACADDR` and `MACADDR8` in
  `network_show`'s always-explicit-prefix spelling. The IPv4-compatible IPv6
  form (`::192.168.1.1`) is rejected rather than risk mis-canonicalizing it.

Checked in `trellis/tests/defs_netaddr.rs`.

## Bit strings

Neither `bit` nor `bit varying` has a second renderer, and both are join and
primary keys. Neither has `MIN`/`MAX` in Postgres. They differ on DDL: a bare
`bit` declares `bit(1)`, which truncates, while a bare `bit varying` is
unconstrained. So a role that must declare a new column or cast from the bare
type is `bit varying` only: `GROUP BY` key and typed literal (`VARBIT`). Join
and primary keys reuse the existing column's own concrete type, so both work.
`bit_and`/`bit_or` accept either type but always return `bit varying`, for
the same reason, and are recompute-only (deleting different rows from the same
group yields different results from one starting value). Postgres raises on
`bit_and` over differently sized `bit varying` values (`cannot AND bit
strings of different sizes`); the live pipeline gets that native error from
the server-side recompute. Checked in `trellis/tests/defs_bit.rs`.

## `jsonb`

`jsonb_out` canonicalizes object key order, but not embedded number scale:
`'{"a": 1}'::jsonb = '{"a": 1.0}'::jsonb` is true while the two render
differently. So `jsonb` is refused as every key role. The typed literal
`JSONB` must be in canonical spelling (sorted keys, no exponent notation,
canonical string escapes); it is rejected rather than normalized.

`JSONB_AGG` accepts a `jsonb` argument only. Postgres marks it `STABLE`
because for some argument types (`timestamptz`, `money`) its row-to-`jsonb`
conversion reads a session setting; converting an already-`jsonb` value reads
none. It is recompute-only, folds a `NULL` row in as a JSON `null` element,
and, with no `ORDER BY` in the grammar, does not guarantee element order
across recomputes (the set of elements is always correct). `json` has no `=`
and is never a key. `MIN`/`MAX(jsonb)` is not supported. Checked in
`trellis/tests/defs_jsonb.rs`.

## Enums

An enum type is identified by its OID ([ADR-0020](decisions/0020-type-identity.md)).
Each definition records the OID of every enum it reads beside the
schema-qualified name it casts by (`enum:<schema>.<type>`). Two distinct enum
types are distinct `ValueType`s, and two columns of different enum types are
never joinable.

- `enumout` is a bijection with no second renderer. Enums are join, `GROUP BY`
  and primary keys; the join/primary-key check is a live `to_regtype` probe
  (`catalog::is_enum_type_name`) rather than a static list.
- `MIN`/`MAX` keep the argument's enum type and follow creation order
  (`pg_enum.enumsortorder`), not alphabetical order. They are recompute-only
  and pushed to Postgres, which reads the type's current shape, so
  `ALTER TYPE … ADD VALUE` needs no special handling.
- There is no typed-literal syntax: a literal's keyword would be a per-schema
  identifier.
- The evaluator has no connection, so it cannot order a multi-value tie; a
  1-1 target's `MIN`/`MAX` over a to-many relationship path is refused.

Postgres refuses a `DROP TYPE` of an enum while a column Trellis created
uses it, and the retype or drop of the source column that has to come first
pauses the definitions that read it, so the drop can follow their `RESUME`
or `DROP TRANSFORM`.

Renaming the type (`ALTER TYPE … RENAME TO`) or moving it to another schema
(`… SET SCHEMA`) is followed in place, whenever it happens: before the
staging worker's first pass over a new definition, or while the definition,
or the one whose target it reads, is paused. The type keeps its OID and every
value, so the worker's capture pass, on finding an OID a definition recorded
under another qualified name now, records the new name where Trellis casts by
it and releases the keys a drain held for the old name. Nothing pauses or
rebuilds. A logical dump and restore gives every type a new OID: the restore
pauses every definition, the pass compares none of their recorded OIDs, and
`RESUME` records the new ones. `pg_upgrade` keeps them.

Renaming a value (`ALTER TYPE … RENAME VALUE`) pauses every definition that
reads a column of the type, whatever the place: passthrough, key, `GROUP BY`,
`JOIN` column or an input to a calculated field. The columns Trellis created
follow the new label, but Trellis also keeps values as label text (staged
changes, held keys, a 1-1 ledger's keys), and that text names a label that
no longer exists. Each definition records its enum values by their
`pg_enum` OIDs, and the capture pass finds the one whose label changed;
`capture_failure` names the old and new label. `RESUME` waits for the changes
staged before the relabel to drain and rebuilds the target from the source,
from empty for a target keyed by an enum source key
([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#an-enum-value-relabelled)).
Adding a value (`ADD VALUE`) changes no label and pauses nothing.

Re-typing a column from one enum to another (`ALTER COLUMN … TYPE other_enum
USING …`) pauses every definition that reads it the same way, whatever the
place: no value of the old enum is a value of the new one, so no label
Trellis kept for the column casts to it. `RESUME` re-types each column
Trellis created of the old enum without casting a value (a key column's rows
are deleted first, since its old keys can't be expressed in the new type),
and rebuilds from the source, so the target reads `backfilling` until it
converges. A re-type from an enum to `text` keeps each label as a value and
is handled as any other re-type.

## `array_agg` and `string_agg`

`ARRAY_AGG(<text>)` and `STRING_AGG(<text>, '<separator>')` take a `text`,
`varchar` or `char(n)` argument (a `char(n)` value loses its trailing spaces,
as in a cast to `text`) and are recompute-only: a delta would need the
group's whole input, not a summary of it.
`ARRAY_AGG` produces `text[]` and keeps a `NULL` row as a `NULL` element.
`STRING_AGG` skips `NULL` rows and is `NULL` when none remain. The separator
must be a string literal, and neither aggregate is accepted over a to-many
relationship path. The grammar has no `ORDER BY` inside the call, so, as in
Postgres without one, element order isn't guaranteed across recomputes: the
set of elements is always right, so compare the elements as a set, or sort
them when you read them.
A later transform that reads a `text[]` target column sees an unrecognized
type. Checked in `trellis/tests/defs_list_aggregates.rs`.

## Aggregates that are not supported

`jsonb_agg` over a non-`jsonb` argument, `array_agg` and `string_agg` over a
non-text argument, and `MIN`/`MAX` of `jsonb`, `uuid`, `bytea`, `cidr`,
`macaddr` and `bit` are absent (the last five because Postgres has no such
aggregate).

## Typed literals and casts

A calculated field can spell a constant of a type that has no literal syntax
of its own: `DATE '2024-01-01'`, or the equivalent `CAST('2024-01-01' AS
date)`. The accepted keywords are `DATE`, `TIMESTAMP`, `TIME`, `TIMETZ`,
`BYTEA`, `OID`, `REAL`, `DOUBLE PRECISION`, `INET`, `CIDR`, `MACADDR`,
`MACADDR8`, `VARBIT` and `JSONB` (`defs::typed_literal::TYPED_LITERALS`). The
literal's text must be in the type's canonical Postgres output spelling, which
also makes the Rust evaluator's text agree with Postgres's byte for byte. A
general `CAST(<expr> AS <type>)` over a non-literal is refused by name. See
[ADR-0004](decisions/0004-transform-definition-grammar.md#typed-literals-spell-constants-in-canonical-postgres-form).

## Immutability

`pg_proc.provolatile` is the ground truth, and several intuitions are wrong:

- `money` comparison is immutable but `cash_out` is `STABLE` (`lc_monetary`).
  The pin to `C` makes a passthrough column render identically everywhere;
  it is still refused as a key or computed value. Use `numeric`.
- `date_out`, `timestamp_out`, `timestamptz_out` and `interval_out` are
  `STABLE` because they read a setting; the pins above make them
  deterministic in Trellis. `time_out` and `timetz_out` read no setting.
- `date_in`/`timestamp_in` are `STABLE` because they accept relative spellings
  (`'today'`); only canonical ISO literals are admitted.
- `text`/`varchar` comparisons are `IMMUTABLE` in Postgres despite collation
  sensitivity; Trellis's own bar is a deterministic collation (see
  [Collation](#collation)).
- `jsonb_in`/`jsonb_out` are immutable; `array_agg` and `string_agg` are
  immutable; only `jsonb_agg` is `STABLE`, for the reason above.
- `xml`, `tsvector` and `tsquery` have no useful immutable equality or
  ordering.

## Unreferenceable types

A column whose type the registry can place in none of the families above (an
array, range, composite, domain, geometric or extension type such as `citext`)
is `PgType::Unrecognized`. It is captured as text, but it is kept out of the
validator's view (`Trellis::source_columns`), so a definition that names it
fails with an ordinary unresolved-column error. This applies to a source's
primary key too.
