---
status: accepted
date: 2026-10-17
deciders: Michael Ries
---

# Transform-Definition Grammar

[transforms](../transforms.md) describes the logical model but not how a user
writes a transform down. This ADR settles the surface syntax.

## Definitions use a minimal, purpose-built grammar

Transform definitions use a **minimal, purpose-built grammar**, parsed at
definition time into an AST the validator and execution layer consume. It
borrows SQL's spelling — same operators, functions, and shape for a `GROUP BY`
list or join condition — but the accepted language is exactly what we specify,
starting from stable operations common to **PostgreSQL 15+**.

The *statement* shape (`TRANSFORM / FROM / SELECT / WHERE`, later `GROUP BY` /
`JOIN`) is ours; the *expression semantics* are an **immutable subset of
Postgres's**. That subset lets Postgres serve as the correctness oracle (see the
[generative test suite](../generative-test-suite.md)), because Postgres
evaluates every expression: the engine renders a definition into the
statements that write the target and computes no value itself.
Every expression rule mirrors Postgres's lexer and parser, and the grammar may
refuse what Postgres accepts but never accept what it refuses
([ADR-0022](0022-grammar-mirrors-postgres.md)).

## The grammar is split by concern

The grammar is **split by concern**, mirroring the pieces
[transforms](../transforms.md) already chooses separately, so granularity rules
are enforced by *which* grammar applies rather than one validator branching on
context:

* **Key-space** — the granularity definition, which decides the target's
  primary key: a `GROUP BY` list for aggregates, a join clause plus a
  `GROUP BY` list for JOIN transforms, nothing for 1-1. Tiny and closed.
  Enforces that aggregate columns are wrapped in an aggregate function and
  that each of a JOIN transform's source columns is qualified by its side.
* **Calculated-field expressions** — the scalar-expression language, reused for
  every calculated field.
* **Partial-data predicate** — the same expression grammar with a `-> bool`
  requirement, restricting which source rows participate.

## Why our own grammar, not the Postgres parser

The obvious alternative is Postgres's real grammar (`libpg_query`
through its Rust bindings, or the pure-Rust `sqlparser-rs`). We reject both
because the engine must know exactly what a definition means: it infers each
field's type to declare the target column, decides which aggregates can be
maintained by small **atomic delta adjustments** (the basis of cheap
incremental maintenance, see [data-flow](../data-flow.md)), and checks
constants and literals at define time.

So the grammar's function set and the renderer's function set are the **same
list**: we accept only what we render, and every addition is a paired
grammar-plus-renderer change. Adopting a full SQL parser would invert this —
we'd inherit its entire surface and forbid it piece by piece — and we parse
fragments, not whole statements. We want the accepted language to *be* the spec.

## A 1-1 definition is `TRANSFORM`, `FROM`, `SELECT` and an optional `WHERE`

The 1-1 statement is:

```text
TRANSFORM <target>
FROM <source>
SELECT <expr> AS <field> [, <expr> AS <field> ...]
[WHERE <predicate>]
```

`<expr>` is a column reference, a numeric or single-quoted string literal,
`TRUE`, `FALSE`, `NULL`,
`<expr> + <expr>`, `<expr> - <expr>`, `<expr> * <expr>`, `<expr> / <expr>`,
`<expr> % <expr>`, `-<expr>`, a comparison (`=`, `<>`, `!=`, `<`, `<=`, `>`,
`>=`, `IS [NOT] DISTINCT FROM`), `<expr> AND <expr>`, `<expr> OR <expr>`,
`NOT <expr>`, `<expr> IS [NOT] NULL`, `<expr> AT TIME ZONE 'UTC'`, a searched
`CASE WHEN <expr> THEN <expr> [WHEN ...] [ELSE <expr>] END`, `CAST(<expr> AS
<integer, numeric or timestamp type>)` (the target also spelled `int`, `int4`,
`int8` or `decimal`), `CAST(NULL AS <type>)` to any type an expression has a
value of, `EXTRACT(<field> FROM <expr>)`, or a function call
(`strpos`, `octet_length`, `char_length`, `regexp_count`, `abs`, `ceil`,
`floor`, `round`, `trunc`, `date_trunc`, `coalesce`, `nullif`; general
`func(args)` syntax). Operators lex and bind as Postgres's do
([ADR-0022](0022-grammar-mirrors-postgres.md)). `AND`, `OR`, `IS` and `AT` are
operators only after a complete operand, `END` only closes a `CASE`, and
`EXTRACT` is a keyword only before a `(`; elsewhere that word is a
column name. `NOT` and `CASE` are keywords wherever an operand follows them (a
name other than `AS`, `IS`, `AND`, `OR`, `WHEN`, `THEN`, `ELSE` or `END`, a
number, a string, a `(` or a `-`), so `NOT -x > 0` is `NOT ((-x) > 0)`. A bare
identifier is never a word Postgres reserves (`not`, `case`, `end`, `order`,
`user`...): the grammar has no quoted identifiers, so such a source
column is read qualified with its table (`orders.order`), as Postgres allows,
and the refusal says so ([ADR-0022](0022-grammar-mirrors-postgres.md)).
`orders.order AS order` passes that column through, as `order AS order`
would. `AT TIME ZONE`
binds tighter than `*` and looser than prefix `-`. Values
carry a Postgres type (the ones [type support](../type-support.md) lists), and
every operator/function's argument and return types are type-checked at
definition time. `<predicate>` accepts only the
literal `TRUE`; any other `WHERE` is refused at definition time.

### Typed literals spell constants in canonical Postgres form

A calculated field can also *spell* a constant of a type that has no literal
syntax of its own, which is what lets it **produce** — rather than merely pass
through — a value of that type (the "computed 1-1 target" role in
[type support](../type-support.md)). Two spellings, one AST node:

```text
DATE '2024-01-01'
CAST('2024-01-01' AS date)
```

Both are valid, unambiguous Postgres, and Postgres folds them to the *same*
constant (`EXPLAIN (VERBOSE)` prints `'2024-01-01'::date` for each), so the
renderer can write our node back as text that means exactly what we parsed. Accepted types are an allowlist of type families (`date`,
`timestamp`, `bytea` and `jsonb` among them), each admitted by the rules below.

Deliberately out:

* **`<expr>::<type>`** — Postgres-only sugar, and a postfix operator in a
  parser whose precedence table this ADR already flags as delicate. It buys
  nothing over `CAST`. Rejected by name, pointing at the two spellings above.
* **General `CAST(<expr> AS <type>)`** — a coercion lattice, not a literal.
  Every (source, target) pair needs its own volatility verdict and rendering
  rule, and most interesting pairs aren't immutable (`timestamptz` → `date`
  reads `TimeZone`). Each type family decides its own pairs when it is
  admitted. Admitted today: an integer to a strictly wider integer (`integer`,
  `bigint`), any integer to `numeric` and a `date` to `timestamp`, the casts
  that are exact and read no setting, with no modifier; every other pair is
  rejected by name.

The literal's text must be in that family's **canonical Postgres output
spelling** — `DATE '2024-1-5'` is refused even though Postgres parses it. This
is a safe subset in the same style as `COALESCE`'s gaps below, and both halves
of the ADR's bar drive it:

* **Immutability.** `pg_proc` marks `date_in` and `timestamp_in` `STABLE`, not
  `IMMUTABLE` — an intuition worth checking, since `jsonb_in`/`byteain` beside
  them *are* immutable. The reason is the relative spellings they accept
  (`DATE 'today'`, `TIMESTAMP 'now'`). Admitting only absolute ISO-8601
  removes exactly that surface.
* **Round-trip identity.** A value travels as text, and the generative suite
  compares a target's text against Postgres's rendering byte-for-byte.
  Requiring canonical form makes the literal's spelling and Postgres's
  rendering agree by construction instead of needing a per-family
  normalizer.

`jsonb` is the family this second rule costs most: `jsonb_out` re-sorts object
keys by length then bytes, collapses duplicates, and renormalizes numbers. The
literal is checked, not normalized: one with exponent notation, an
out-of-order or duplicate object key, or a non-canonical string escape is
refused, so no `jsonb` value model is needed.

`COALESCE(<expr>, ...)` is accepted — first non-`NULL` argument, or
`NULL` if all are. It's immutable, but *variadic*, so it isn't a registry
function: the parser special-cases it (at least one argument, as Postgres does)
and the renderer emits Postgres's `COALESCE`. It is a **safe subset** of
Postgres's `COALESCE`; the accepted-language-is-the-spec goal makes the gaps
worth naming:

* Numeric arguments resolve to a common type as Postgres's `COALESCE` does
  (`integer` and `numeric` give `numeric`; a float wins over every exact type).
  Any other mix of types is refused, where Postgres resolves some mixes (`date`
  and `timestamp`, say) to a common type.
* No `unknown`-typed literal: a quoted literal is always `Text`, so
  `COALESCE(<numeric>, '0')` is a type mismatch here. Write `COALESCE(<numeric>, 0)`.
* A `NULL` argument takes the other arguments' type, as in Postgres
  (`COALESCE(x, NULL)` has `x`'s); one that is all `NULL`s is `text`.

Each gap is pinned by a divergence test.

`FROM <source>` is where a key-space clause slots in without changing the outer
shape:

* `GROUP BY <col1>[, <col2>...]` (at most 32 columns, Postgres's cap on an
  index key) produces a `KeySpace::Aggregate` whose
  calculated fields may reference a grouping column directly or wrap any other
  column in `SUM` or `AVG` (over an integer or `numeric(p,s)`; see
  `docs/type-support.md`), resolved against a separate aggregate-function
  registry. The grammar also parses `MIN`, `MAX`, `bool_*`, `bit_*`,
  `array_agg`, `string_agg` and `jsonb_agg`, only so define can refuse them by
  family ([ADR-0002](0002-async-data-flow.md)). `COUNT(*)` and
  `COUNT(<expr>)` (counts non-null occurrences of `<expr>`, a
  different Postgres semantic from `COUNT(*)`) are both supported.
* `FROM <a> <join> <b> ON <cond>`, with a required `GROUP BY`, makes a JOIN
  transform ([0019-join-transforms](0019-join-transforms.md)):

  ```text
  TRANSFORM <target>
  FROM <a> { JOIN | INNER JOIN | LEFT JOIN | LEFT OUTER JOIN } <b>
    ON <a>.<col> = <b>.<col> [AND <a>.<col> = <b>.<col> ...]
  GROUP BY <side>.<col>[, <side>.<col>...]
  SELECT <side>.<col> AS <field>, <aggregate>(<expr over one side>) AS <field> [, ...]
  ```

  The join kind is SQL's prefix: `JOIN` and `INNER JOIN` are stored and
  rendered as `INNER JOIN`, `LEFT JOIN` and `LEFT OUTER JOIN` as `LEFT JOIN`.
  A side is named by its bare table name, with no aliases, and every source
  column is written `<side>.<col>` in `ON`, `GROUP BY` and fields alike; a bare
  name means a field, and a dotted name whose head isn't a side is an unknown
  table qualifier. `<cond>` is one
  or more equalities joined by `AND`, each between one column of each side
  (either order), with no column named by two terms. Each `GROUP BY` key becomes a
  target column named after its bare column name, so two keys sharing a bare
  name are refused. The `GROUP BY` columns, the `ON` columns of a side, and a
  side's `ON` and `GROUP BY` columns together each number at most 32, Postgres's
  cap on an index key. An aggregate argument reads one side; one mixing both
  sides is refused, and the admitted aggregates are ADR-0019's. `WHERE` stays
  `TRUE`-only; a future `WHERE` applies after the join, as in SQL, so adding it
  changes nothing a JOIN transform already means. Recognized and refused by
  name: two sides sharing a bare table name (which covers self-joins);
  `RIGHT`, `FULL`, `CROSS`, a comma join and a second `JOIN`; an expression
  key, a non-equality condition, `OR`, a one-side predicate, an equality
  between two columns of one side, `USING` and `NATURAL`; and a `JOIN` with no
  `GROUP BY`.

## Growth policy

We ship the stable core first and add operators and functions incrementally,
driven by community feedback. The bar for admitting one: (a) **semantics
identical to a stable PostgreSQL 15+ operator/function**; (b) **immutable** per
[transforms](../transforms.md#calculated-fields); (c) the renderer emits it as the Postgres function of that name.
Anything volatile, session- or collation-dependent stays out.

Binary operators parse by precedence climbing over a per-operator precedence table
in the operator registry. As in Postgres, the arithmetic operators, `AND` and
`OR` are left-associative, and the comparisons (`IS [NOT] DISTINCT FROM`
among them) are non-associative, so `a < b < c` is refused. A new operator
declares its tier there.

## Consequences

* Every operator or function a user can write is one the renderer emits: each
  addition is a paired grammar-plus-renderer change, and a Postgres function
  outside the list is refused until then.
* The accepted language is narrower than Postgres's in places a SQL user will
  notice: a typed literal must use its type's canonical spelling, `::` casts and
  a `CAST` other than the integer widenings, `date` to `timestamp` and a
  `NULL`'s are refused, `COALESCE` refuses mixed non-numeric argument types,
  and a `NULL` is refused where Postgres would type it by its
  function-candidate rules (a function argument) or find it ambiguous.
* Precedence lives in our own operator table, so each new operator has to
  declare its tier there correctly.
