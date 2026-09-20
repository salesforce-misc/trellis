---
status: accepted
date: 2026-09-19
deciders: Michael Ries
---

# Curate the Public API Surface

Trellis presents a deep interface: a small, deliberate public surface backed by a
large internal engine. This document fixes what is public, what is not, and how the
sibling crates (`benchmark`, `generative`) and embedders exercise the engine's
behaviour without the engine's internals becoming API.

## The three tiers

**Tier 1 — the facade.** `Trellis`, `Client`, and `BlockingTrellis` are the only
front doors. `Trellis` covers the whole lifecycle; `Client` starts and stops the
in-process engine; `BlockingTrellis` is the synchronous mirror. Everything an
application does, it does through these — and every definition-changing operation
enters through a single grammar-driven entrypoint on them (see below).

**Tier 2 — composable primitives.** An embedder may reach these directly: `Config`,
`Pool`, `migrate`, `Identity`, `metrics`, `Error`/`ErrorCode`, `Numeric`, and `otel`
(behind the `otlp` feature). They are public and stable, re-exported at the crate
root, and documented as deliberate building blocks rather than incidental leaks.

**Tier 3 — the engine.** `defs`, `staging`, and `intake` are `pub(crate)`. No
external crate names `trellis::defs::*`, `trellis::staging::*`, or
`trellis::intake::*`. The engine is machinery the facade composes; it is not API.

## Decisions

### The engine is `pub(crate)`

`defs`, `staging`, and `intake` — and every module beneath them — are crate-private.
The curated re-exports at the crate root are the boundary, not a suggestion layered
over reachable internals.

### Public error types are nameable

The error types the public errors wrap — `CatalogError`, `DdlError`, `ParseError`,
`StagingError`, `IntakeError`, `ApplyError` — are re-exported at the crate root even
though they originate in `pub(crate)` modules. An embedder handed an `Error` must be
able to name and match its payload (`fn handle(e: &CatalogError)`, `impl From<...>`);
an unnameable public error type is not an acceptable surface.

### One grammar-driven entrypoint for every definition change

The facade does not grow a typed method per operation — `define`,
`define_relationship`, `pause`, `resume`, `amend`, `drop`, and whatever comes next.
Every definition-changing operation enters through a **single `apply` entrypoint**
(and its blocking mirror) that accepts a statement in Trellis's grammar — the same
grammar the CLI client speaks. The statement itself discriminates the operation:
define a transform, define a relationship, pause, resume, amend, drop. Parsing the
statement is where the operation is decided; the facade signature does not change as
operations are added.

This is the deliberate front door, not a convenience layered over typed methods. The
CLI, the embedded language bindings, and framework migration files all present the
same text, so one grammar and one entrypoint mean each operation's semantics live in
exactly one place instead of being re-spelled as a method on every binding. Text is
also the simplest thing to carry across an FFI boundary — a single string in, plain
data out — so a new operation is a grammar addition, not new surface each host must
mirror and keep in sync.

Read paths stay typed. Status, the recompute audit, quarantine sampling, and
convergence-await return structured data and take structured arguments, not grammar;
the unified entrypoint governs the definition/mutation surface, not queries.

### Read-your-writes is a facade capability

The facade exposes a convergence-await method: take a watermark token, then block
until the engine has caught up through it. Embedders and tests that need
read-your-writes call it; the underlying convergence machinery in `staging` stays
crate-private. No consumer reaches convergence primitives by naming the staging
module.

### Sibling crates verify through the public API and through Postgres, not the engine

`benchmark` and `generative` are consumers of the public API, held to the same
boundary as any embedder — with one narrow, sanctioned exception (below).

- **Drive the system through the facade.** Registering definitions and relationships,
  requesting backfills, reading status, and awaiting convergence all go through
  `Trellis`/`Client`. A test or benchmark does not call engine functions to reach a
  state; it reaches states the way a user does. This keeps the suites honest — they
  exercise the real parse → register → backfill → apply path, and they cannot drive
  the system into a state no user can express.
- **Express transforms as text.** The generative suite generates definition *text*
  and installs it through the public path, exercising the parser, rather than
  constructing engine ASTs directly.
- **Postgres is the oracle.** Correctness is verified by comparing a persisted target
  against an equivalent, independently-authored SQL query — for an aggregate target,
  the corresponding `GROUP BY` — executed by Postgres, not by re-running the engine's
  own evaluator over the same data. The comparison is byte-identical for almost every
  value, and up to the type's own `=` for the narrow class of non-injective aggregate
  folds (`MIN`/`MAX` over `float`/`interval`) where byte-identity would be stricter
  than correctness. `Trellis::self_check` (see
  [ADR-0013](0013-self-check-production-recompute-audit.md), *Comparison semantics*)
  is the shipped form of this check and the public API the suites use for it.

### One sanctioned exception: the fuzz suite's independent cross-check leg

The generative suite keeps a second, evaluator-driven recompute leg whose sole
purpose is to catch divergence between the engine's Rust evaluator and Postgres
semantics. By design it must stay independent of both the shipped `self_check` and
the suite's own SQL oracle, so it cannot become a public method. It — and the small
set of type and registry lookups its correctness assertions need — is reachable only
through a curated, dev-only re-export at the crate root, gated behind a cargo feature
enabled only from test and benchmark targets and never on a production dependency
edge. The gate carries no public-API or semver commitment: anything reached through
it may change without notice.

## Consequences

- The crate root's public surface is exactly the tier-1 facade, the tier-2 primitives,
  and the wrapped error types. Everything else is crate-private.
- The dev-only gated surface is small and specific — the fuzz suite's independent
  verification legs — rather than a re-export of the engine. Adding to it is a
  deliberate, visible act.
- Because the sibling crates drive the facade and verify against Postgres, they test
  the same paths users take, and a regression in a user-reachable path cannot hide
  behind an internal shortcut a test used to reach a state.
