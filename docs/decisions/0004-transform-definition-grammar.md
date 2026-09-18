---
status: accepted
date: 2026-08-20
deciders: Michael Ries
consulted: 
informed:
---

# Transform-Definition Grammar

[transforms](../transforms.md) describes the logical model — granularity,
calculated fields, relationships, partial-data predicates — but not how a user
writes one down, and [open-questions](../open-questions.md) left the surface
syntax undecided. This ADR settles it: transform definitions use a **minimal,
purpose-built grammar** that borrows SQL's spelling for familiarity but is our
own language, not a Postgres dialect.

The calculation grammar is an **immutable subset of PostgreSQL's operator and
function semantics**. The *statement* shape (`TRANSFORM / FROM / SELECT / WHERE`,
and later `GROUP BY` / `JOIN`) is ours; the *expression semantics* are Postgres's.
This constraint allows us to use Postgres as the primary correctness oracle, while
the internal evaluator serves as a secondary cross-check (see `docs/generative-test-suite.md`).

## Decision

Trellis defines a small grammar of its own, parsed at definition time into an
AST the validator and execution layer consume. It looks like SQL — same operator and
function spelling, same shape for a `GROUP BY` list or join condition — so it's
familiar on sight, but the accepted language is exactly what we specify. We
start from the stable operations common to **PostgreSQL 15+**.

The grammar is **split by concern**, mirroring the pieces
[transforms](../transforms.md) already chooses separately. Splitting them lets
granularity-specific rules be enforced by *which* grammar applies, not by one
validator branching on context:

* **Key-space** — the granularity definition, which decides the target's
  primary key: a `GROUP BY` list for aggregates, a join condition and type for
  cross-joins, nothing for 1-1. Deliberately tiny and closed. Enforces that
  aggregate columns are wrapped in an aggregate function and cross-join columns
  are side-qualified.
* **Calculated-field expressions** — the scalar-expression language, reused for
  every calculated field.
* **Partial-data predicate** — the same expression grammar with a `-> bool`
  requirement, restricting which source rows participate.

## Why our own grammar, not the Postgres parser

The obvious alternative is Postgres's real grammar — `libpg_query` (via
`pg_query.rs`) exposes the server parser; `sqlparser-rs` offers a pure-Rust
approximation. We reject both as the *definition* surface because we execute
formulas ourselves.

Trellis does **not** evaluate formulas by issuing SQL to Postgres per batch. We
re-implement evaluation in our own execution layer, modeled on Postgres's
source, so we can do things Postgres-as-executor can't — notably small **atomic
delta adjustments** to aggregated `numeric` values, the basis of cheap
incremental maintenance (see [data-flow](../data-flow.md)).

That makes the grammar's function set and the execution layer's function set the
**same list**: we can only accept an operator or function we have actually
re-implemented. Every addition is a paired grammar-plus-evaluator change, not a
parser flipping on syntax we can't yet compute. Adopting a full SQL parser would
invert this — we'd inherit its entire grammar surface and forbid it piece by
piece, and we're parsing fragments (an expression, a predicate, a grouping
list), not whole statements. We want the accepted language to *be* the spec.

Re-implementing evaluation also sharpens the correctness bar. Because the
accepted grammar is an immutable subset of Postgres semantics, the
[correctness oracle](../data-flow.md#correctness) *is Postgres*: the generative
suite renders a definition back to a `SELECT` and asserts the persisted target
equals what Postgres computes for the same source data. The re-implemented
evaluator is retained as a secondary cross-check against that Postgres oracle.

## Concrete syntax (1-1 slice, issues #22, #62)

The 1-1 slice of the grammar (`trellis/src/defs`) uses:

```text
TRANSFORM <target>
FROM <source>
SELECT <expr> AS <field> [, <expr> AS <field> ...]
[WHERE <predicate>]
```

`<expr>` is a column reference, a numeric literal, a single-quoted string
literal, `<expr> + <expr>`, `<expr> > <expr>`, or a function call
(`strpos`, `octet_length`, `char_length`, `regexp_count` — general
`func(args)` call syntax landed with these four; adding another function
means a registry entry plus a re-implemented evaluator arm, same bar as an
operator). Values carry one of four types — `Numeric`, `Text`, `Boolean`, `Uuid`
(the last added in issue #79) — and every operator/function's argument and
return types are type-checked at definition time (see [transforms](../transforms.md)). `<predicate>` accepts
only the literal `TRUE` for now (the partial-data predicate is otherwise
deferred).

`COALESCE(<expr>, ...)` is also accepted (issue #64) — the first
non-`NULL` argument, or `NULL` if all are. It's immutable, so it clears the
growth-policy bar, but it's *variadic* rather than fixed-arity, so it isn't a
registry function: the parser special-cases it (requiring at least one
argument, as Postgres does), and the evaluator short-circuits on the first
non-`NULL`. It is a **safe subset** of Postgres's `COALESCE`, not yet full
parity — the accepted-language-is-the-spec goal makes the gaps worth naming:

* All arguments must resolve to the *same* type (exact match), where Postgres
  runs type resolution to a common type. Because this grammar's type lattice
  is coarser than Postgres's (a single `Numeric`, no distinct `int`/`bigint`),
  no *expressible* mismatch behaves differently — cross-category mixes like
  `numeric`/`text` are rejected by both.
* There is no `unknown`-typed literal: a quoted literal is always `Text`, so
  `COALESCE(<numeric>, '0')` — which Postgres coerces to `numeric` — is a type
  mismatch here. Write a numeric literal (`COALESCE(<numeric>, 0)`) instead.
* There is no `NULL` literal in the grammar at all, so Postgres's idiomatic
  `COALESCE(x, NULL)` isn't expressible; a bare `NULL` parses as a column
  reference and is rejected as unresolved.

These are tracked toward full compatibility; each is pinned by a divergence
test in `trellis/src/defs`.

`FROM <source>` is deliberately where a key-space clause slots in — `GROUP BY
<cols>` for the aggregate case, `JOIN <other> ON <cond> [INNER|LEFT|RIGHT]` for
cross-join — without changing the statement's outer shape. `JOIN` is recognized
and rejected by name; cross-join remains out of scope. Relationship reference
syntax is defined with the relationship feature — see
[0006-relationships](0006-relationships.md).

`GROUP BY <col1>[, <col2>...]` is parsed in this same reserved slot, producing
a `KeySpace::Aggregate` whose calculated fields may reference a grouping column
directly or wrap any other numeric column in exactly one of `SUM`, `MIN`,
`MAX`, or `AVG` — the same paired grammar-plus-evaluator bar every other
addition here meets. Aggregate function calls use the same `func(args)` syntax
general function calls use, resolved against a separate aggregate-function
registry reachable only inside a `GROUP BY` definition. `COUNT(*)` (arity-0 row
counting, issue #75) is supported; `COUNT(<column>)` inside a `GROUP BY` is
recognized by name only, to give a specific "not yet implemented" error — the
same reservation-by-name pattern `JOIN` uses.

## Growth policy

We ship the stable core first and add functions and operators incrementally,
driven by community feedback. The bar for admitting one: (a) **semantics
identical to a stable PostgreSQL 15+ operator/function**; (b)
**immutable** per [transforms](../transforms.md#calculated-fields); and (c) a
re-implemented evaluator. Anything volatile, session- or collation-dependent
stays out.

Undecided:

* Cross-join side-qualification syntax — `JOIN` is recognized and rejected by
  name, not yet parsed.
* Whether the grammar and its stored schema are versioned independently of the
  transform-redefinition scheme (see [open-questions](../open-questions.md)).
* Operator precedence: the parser is currently flat left-associative with no
  precedence table. Safe today only because every Numeric-returning operator
  (`+`) outranks the sole Boolean-returning one (`>`) in the type lattice, so
  any regrouping that would produce a different result also fails
  type-checking rather than silently computing the wrong answer. This
  invariant must be re-verified before a second Boolean- or Numeric-returning
  operator at a different precedence tier is added.
