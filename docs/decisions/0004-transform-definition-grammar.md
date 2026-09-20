---
status: accepted
date: 2026-08-20
deciders: Michael Ries
consulted: 
informed:
---

# Transform-Definition Grammar

[transforms](../transforms.md) describes the logical model but not how a user
writes a transform down. This ADR settles the surface syntax.

## Decision

Transform definitions use a **minimal, purpose-built grammar**, parsed at
definition time into an AST the validator and execution layer consume. It
borrows SQL's spelling — same operators, functions, and shape for a `GROUP BY`
list or join condition — but the accepted language is exactly what we specify,
starting from stable operations common to **PostgreSQL 15+**.

The *statement* shape (`TRANSFORM / FROM / SELECT / WHERE`, later `GROUP BY` /
`JOIN`) is ours; the *expression semantics* are an **immutable subset of
Postgres's**. That subset lets Postgres serve as the primary correctness oracle,
with our re-implemented evaluator as a secondary cross-check (see
`docs/generative-test-suite.md`).

The grammar is **split by concern**, mirroring the pieces
[transforms](../transforms.md) already chooses separately, so granularity rules
are enforced by *which* grammar applies rather than one validator branching on
context:

* **Key-space** — the granularity definition, which decides the target's
  primary key: a `GROUP BY` list for aggregates, a join condition and type for
  cross-joins, nothing for 1-1. Tiny and closed. Enforces that aggregate columns
  are wrapped in an aggregate function and cross-join columns are side-qualified.
* **Calculated-field expressions** — the scalar-expression language, reused for
  every calculated field.
* **Partial-data predicate** — the same expression grammar with a `-> bool`
  requirement, restricting which source rows participate.

## Why our own grammar, not the Postgres parser

The obvious alternative is Postgres's real grammar (`libpg_query` via
`pg_query.rs`, or the pure-Rust `sqlparser-rs`). We reject both because we
execute formulas ourselves rather than issuing SQL to Postgres per batch: we
re-implement evaluation in our own layer so we can do things Postgres-as-executor
can't — notably small **atomic delta adjustments** to aggregated `numeric`
values, the basis of cheap incremental maintenance (see [data-flow](../data-flow.md)).

So the grammar's function set and the evaluator's function set are the **same
list**: we accept only what we've re-implemented, and every addition is a paired
grammar-plus-evaluator change. Adopting a full SQL parser would invert this —
we'd inherit its entire surface and forbid it piece by piece — and we parse
fragments, not whole statements. We want the accepted language to *be* the spec.

## Concrete syntax (1-1 slice, issues #22, #62)

The 1-1 slice (`trellis/src/defs`) uses:

```text
TRANSFORM <target>
FROM <source>
SELECT <expr> AS <field> [, <expr> AS <field> ...]
[WHERE <predicate>]
```

`<expr>` is a column reference, a numeric or single-quoted string literal,
`<expr> + <expr>`, `<expr> > <expr>`, or a function call (`strpos`,
`octet_length`, `char_length`, `regexp_count`; general `func(args)` syntax landed
with these four). Values carry one of four types — `Numeric`, `Text`, `Boolean`,
`Uuid` (the last added in issue #79) — and every operator/function's argument and
return types are type-checked at definition time. `<predicate>` accepts only the
literal `TRUE` for now; the partial-data predicate is otherwise deferred.

### Typed literals (issue #109)

A calculated field can also *spell* a constant of a type that has no literal
syntax of its own, which is what lets it **produce** — rather than merely pass
through — a value of that type (`docs/type-support.md`'s "computed 1-1 target"
role). Two spellings, one AST node:

```text
DATE '2024-01-01'
CAST('2024-01-01' AS date)
```

Both are valid, unambiguous Postgres, and Postgres folds them to the *same*
constant (`EXPLAIN (VERBOSE)` prints `'2024-01-01'::date` for each), so the
SQL-rendering oracle can render our node back to text that means exactly what
we evaluated. Accepted types are an allowlist — today `date`, `timestamp`,
`bytea` — in `trellis/src/defs/typed_literal.rs`, which is also where each
family's reasoning lives.

Deliberately out:

* **`<expr>::<type>`** — Postgres-only sugar, and a postfix operator in a
  parser whose precedence table this ADR already flags as delicate. It buys
  nothing over `CAST`. Rejected by name, pointing at the two spellings above.
* **General `CAST(<expr> AS <type>)`** — a coercion lattice, not a literal.
  Every (source, target) pair needs its own volatility verdict and evaluator
  arm, and most interesting pairs aren't immutable (`timestamptz` → `date`
  reads `TimeZone`). Each type family's own issue decides its own pairs;
  rejected here by name.

The literal's text must be in that family's **canonical Postgres output
spelling** — `DATE '2024-1-5'` is refused even though Postgres parses it. This
is a safe subset in the same style as `COALESCE`'s gaps below, and both halves
of the ADR's bar drive it:

* **Immutability.** `pg_proc` marks `date_in` and `timestamp_in` `STABLE`, not
  `IMMUTABLE` — an intuition worth checking, since `jsonb_in`/`byteain` beside
  them *are* immutable. The reason is the relative spellings they accept
  (`DATE 'today'`, `TIMESTAMP 'now'`). Admitting only absolute ISO-8601
  removes exactly that surface.
* **Cross-check agreement.** A computed value travels as text, and the
  generative suite compares the Rust evaluator's text against Postgres's
  rendering byte-for-byte. Requiring canonical form makes the two renderers
  agree by construction instead of needing a per-family normalizer.

`jsonb` is the family this second rule holds back: `jsonb_out` re-sorts object
keys by length then bytes, collapses duplicates, and renormalizes numbers, so
checking a literal is canonical means implementing a real `jsonb` value model
— #115's job, which needs one anyway for `jsonb_agg`.

`COALESCE(<expr>, ...)` is accepted (issue #64) — first non-`NULL` argument, or
`NULL` if all are. It's immutable, but *variadic*, so it isn't a registry
function: the parser special-cases it (at least one argument, as Postgres does)
and the evaluator short-circuits. It is a **safe subset** of Postgres's
`COALESCE`; the accepted-language-is-the-spec goal makes the gaps worth naming:

* All arguments must resolve to the *same* type (exact match), where Postgres
  resolves to a common type. Because this type lattice is coarser (one `Numeric`,
  no distinct `int`/`bigint`), no *expressible* mismatch behaves differently.
* No `unknown`-typed literal: a quoted literal is always `Text`, so
  `COALESCE(<numeric>, '0')` is a type mismatch here. Write `COALESCE(<numeric>, 0)`.
* No `NULL` literal at all, so `COALESCE(x, NULL)` isn't expressible; bare `NULL`
  parses as a column reference and is rejected as unresolved.

Each gap is pinned by a divergence test in `trellis/src/defs` and tracked toward
full compatibility.

`FROM <source>` is where a key-space clause slots in without changing the outer
shape:

* `GROUP BY <col1>[, <col2>...]` produces a `KeySpace::Aggregate` whose
  calculated fields may reference a grouping column directly or wrap any other
  numeric column in exactly one of `SUM`, `MIN`, `MAX`, `AVG`, resolved against a
  separate aggregate-function registry. `COUNT(*)` (issue #75) is supported;
  `COUNT(<column>)` is recognized by name only, to give a "not yet implemented"
  error.
* `JOIN <other> ON <cond> [INNER|LEFT|RIGHT]` for cross-join is recognized and
  rejected by name; cross-join remains out of scope. Relationship reference
  syntax is defined with the relationship feature — see
  [0006-relationships](0006-relationships.md).

## Growth policy

We ship the stable core first and add operators and functions incrementally,
driven by community feedback. The bar for admitting one: (a) **semantics
identical to a stable PostgreSQL 15+ operator/function**; (b) **immutable** per
[transforms](../transforms.md#calculated-fields); (c) a re-implemented evaluator.
Anything volatile, session- or collation-dependent stays out.

Undecided:

* Cross-join side-qualification syntax — `JOIN` is recognized and rejected by
  name, not yet parsed.
* Whether the grammar and its stored schema are versioned independently of the
  transform-redefinition scheme (see [open-questions](../open-questions.md)).
* Operator precedence: the parser is flat left-associative with no precedence
  table. Safe today only because every Numeric-returning operator (`+`) outranks
  the sole Boolean-returning one (`>`) in the type lattice, so any regrouping
  that would change the result also fails type-checking. Re-verify before adding
  a second Boolean- or Numeric-returning operator at a different precedence tier.
