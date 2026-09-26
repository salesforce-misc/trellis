---
status: accepted
date: 2026-09-16
deciders: Michael Ries
consulted:
informed:
---

# Embeddable Clients for Ruby and Elixir

A common way to run Trellis is in-process inside a host app in another
language — a Rails or Phoenix app that serializes schema change through ordered
migration files and wants the `TRANSFORM` derived from a table to live in the
same file as its `CREATE TABLE`. The host loads Trellis over an FFI boundary
rather than deploying a separate service.

This ADR records four decisions about that boundary. Roadmap, phasing, and
packaging live in GitHub issues.

It builds on three pieces of [ADR-0008](0008-public-api-design.md) (issue #82)
made for this boundary:

* [`BlockingTrellis`](../../trellis/src/blocking.rs) — synchronous facade over
  async `Trellis`, owning a dedicated thread with its own Tokio runtime.
* [`ErrorCode`](../../trellis/src/error_code.rs) — a `#[non_exhaustive]`,
  small taxonomy with a stable `as_str()`, reported by every error that
  can reach a caller.
* The transform status lifecycle (#55): `define()` returns once the definition
  is registered and its backfill queued; completion is polled via `status()`.

## Decision 1: each binding wraps the `trellis` crate directly

Each binding is a thin Rust crate wrapping `BlockingTrellis`: an Elixir
[Rustler](https://github.com/rusterlium/rustler) NIF, and a Ruby
[Magnus](https://github.com/matsadler/magnus) extension on
[`rb-sys`](https://github.com/oxidize-rb/rb-sys).

**Each ships through its own language's registry** — Hex for Elixir,
RubyGems for Ruby — because that is where its users already look and what lets
a host declare Trellis in `mix.exs` or a `Gemfile`. Each package carries a
compiled native extension, so an installing app needs no Rust toolchain.
Neither binding crate goes to `crates.io`; a Rust program would depend on
`trellis` directly.

**There is no shared FFI library** — no `trellis-ffi` cdylib exporting a C ABI.
Rustler and Magnus are Rust-native: they encode host values from Rust types
directly, so a `Vec<QuarantineEntry>` becomes Elixir structs or Ruby objects in
one step. A C ABI would mean flattening every rich type into C structs and
re-inflating it in each host — two hand-written marshalling layers per type
instead of one, plus manual allocation contracts where a mistake segfaults
rather than raises. Its only payoff is a third host language later, cheaper to
add as another Rustler/Magnus crate than to pay for up front.

A shared *Rust* helper crate for decision 4's conversions is compatible with
this and may be introduced if the bindings duplicate enough conversion code;
it holds no Rustler or Magnus types and is not the rejected C-ABI shim.

## Decision 2: bindings never reimplement the client over SQL

A binding may not touch Trellis's catalog, staging, or target tables with its
own SQL. Every operation goes through the `trellis` crate.

A pure-Ruby/pure-Elixir client issuing SQL needs no native extension, which is
attractive for packaging — and still wrong. What a client must get right is not
"run this query" but the instance-identity check, pinned `search_path`,
grammar, backfill markers and their fences, backfill enumeration, and
quarantine addressing. Every one is a moving contract; three independent
implementations means every engine change lands three times and stays correct
in at most one. Divergence is silent and yields wrong derived data, not an error.

Corollary: the bindings are deliberately thin. Where a host-language
convenience needs something the crate doesn't expose, widen the crate's API,
don't reach around it.

## Decision 3: the binding owns one handle; Rust owns its threads

**One handle per OS process, created at boot, held as an opaque resource.**
This resolves ADR-0008's first open question. `Trellis::connect` runs the
instance-identity check, builds the pool, and optionally starts background
workers — none of it per-call.

* **Elixir:** a `ResourceArc` owned by a process in the supervision tree, so
  shutdown is a supervisor concern and the destructor is only a backstop.
* **Ruby:** a `TypedData` behind a module-level singleton, with explicit
  shutdown and `at_exit` as backstop.

**Every call is dirty/GVL-released**, because each method blocks on a database
round trip — far past what either host VM tolerates on a scheduler thread.

* **Elixir:** every NIF runs on a dirty IO scheduler, no exceptions for
  calls that look cheap (`status/2` is still a query). The lone exception is
  reading the metrics registry, an in-process read with no round trip.
* **Ruby:** every call releases the GVL and supplies an unblocking function so
  `Thread#kill` and Ctrl-C are not swallowed — which requires an interruptible
  reply wait rather than a bare blocking receive.

**Rust's threads stay Rust's, and are bounded.** The host VM does not schedule,
see, or join the Tokio runtime thread, the pool connections, or the drain
workers; `shutdown` is the only way to stop them. Because they are invisible to
the host's sizing, the binding must bound them: `BlockingTrellis` currently
defaults to one worker per core, which inside a BEAM node already sized to core
count doubles the thread population. The work is IO, not compute, so the
bindings pass a small explicit worker count — a knob the crate does not have yet.

**A handle does not survive `fork`.** Puma, Unicorn, Passenger, and Resque all
fork; Rust threads do not cross `fork`, so a child inherits the handle and its
file descriptors but no threads to service them, and every call hangs forever.
The Ruby binding therefore connects *after* fork and records the owning pid,
raising on any call from a different pid rather than deadlocking. The BEAM does
not fork, but the pid guard is cheap enough to carry in both.

## Decision 4: only plain data crosses, and errors cross as `(code, message)`

**Errors** carry exactly `ErrorCode::as_str()` plus the `Display` message — no
error chain, no `source()` walking, no internal variant names.

* **Elixir:** `{:error, %Trellis.Error{code: :validation, message: "..."}}`
  from non-bang functions; bang variants raise the same struct.
* **Ruby:** an exception hierarchy under `Trellis::Error`, one subclass per
  code, each carrying `#code` and `#message`. Raising is the default.

`ErrorCode` is `#[non_exhaustive]`, so each binding needs a fallback arm for an
unrecognized code and a test asserting every *current* variant has an explicit
mapping — so adding a code is a deliberate binding change, not a silent downgrade.

**Values.** Only plain data crosses; engine types carrying structure are
flattened on the Rust side, once:

* **`Definition` does not cross** — it carries the `TransformDef` AST and a
  `HashMap<String, ValueType>`. `define` returns the summary fields (`id`,
  `target_table`, `source_table`, `source_version`, `status`, `created_at`)
  plus source columns as a name → type-name map. An embedder wanting the AST
  wants the Rust crate.
* **`SystemTime`** crosses as epoch microseconds, rehydrated to `DateTime` /
  `Time` in the host layer.
* **`TransformStatus` / `QuarantineState`** cross as `as_str()` and become
  atoms (Elixir) or symbols (Ruby) from a closed set allocated at load time —
  never `String.to_atom` on a database value.
* **`QuarantineTarget`** crosses as its `transform` / `transform.column`
  address string (the idiom ADR-0008 decision 5 settled), not a two-variant
  struct.
* **`sample_quarantined`'s cursor** crosses as an opaque value the caller
  round-trips from the previous page.

The rule behind each: a shape may cross only if both hosts can represent it
without inventing semantics for it. When it can't, it's flattened in Rust,
where there is one implementation to get right — decision 2 applied to types.

## Consequences

* The `trellis` crate stays the single implementation of client behavior, and
  gains API surface when a binding needs it rather than the bindings growing SQL.
* Two binding crates must track the crate's public API; keeping them in this
  repo, built and tested by the same CI run, is the cheapest way to hold that line.
* Decision 3 needs a runtime worker-thread knob the crate lacks today.
* Decision 4's flattening is mechanical and testable in Rust, ahead of either host.

## Open questions

* Published package and module names on Hex and RubyGems — expensive to change
  after release, and `trellis` may be taken on either.
* A multi-tenant host (one app, several databases) implies several handles, in
  tension with decision 3's per-process singleton. Deferred until a concrete need.

## Related issues

#82 / [ADR-0008](0008-public-api-design.md) (the public API these bindings
wrap), #55 (the status lifecycle behind the poll-to-`live` contract), #49 /
[ADR-0009](0009-observability-decisions.md) (the metrics and logs an embedder
surfaces), #144 (the worker-registry heartbeat and `Trellis::has_live_drain_workers`
health check this decision's third point implies a fleet needs — see
[docs/embedding.md](../embedding.md)).
