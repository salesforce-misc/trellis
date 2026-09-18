---
status: accepted
date: 2026-09-16
deciders: Michael Ries
consulted:
informed:
---

# Embeddable Clients for Ruby and Elixir

A common way to run Trellis is inside an application written in another
language — a Rails or Phoenix app that already serializes schema change
through ordered, repeatable migration files, and wants the `TRANSFORM` that
derives from a table to live in the same file as the `CREATE TABLE`. That means
the host process loads Trellis **in-process**, over an FFI boundary, rather
than deploying and sequencing a separate service.

This ADR records four decisions about that boundary: what binds to what, what
each binding is allowed to reimplement, who owns which threads, and what
shapes are permitted to cross. Roadmap, phasing, packaging, and host-framework
ergonomics are tracked in GitHub issues, not here.

This was blocked on issue #82, which [ADR-0008](0008-public-api-design.md)
settled *and implemented*. Three pieces of that work exist specifically for
this boundary and the decisions below build directly on them:

* [`BlockingTrellis`](../../trellis/src/blocking.rs) — a synchronous facade
  over the async `Trellis`, owning a dedicated thread with its own Tokio
  runtime (ADR-0008 decision 1).
* [`ErrorCode`](../../trellis/src/error_code.rs) — a `#[non_exhaustive]`,
  six-variant taxonomy with a stable `as_str()`, reported by every error type
  that can reach a caller (ADR-0008 decision 3).
* The transform status lifecycle (#55): `define()` returns once the definition
  is registered and its backfill is queued; completion is polled via
  `status()`, not awaited.

## Decision 1: each binding wraps the `trellis` crate directly

Each binding is a thin Rust crate depending on `trellis` and wrapping
`BlockingTrellis`:

* **Elixir** — a [Rustler](https://github.com/rusterlium/rustler) NIF.
* **Ruby** — [Magnus](https://github.com/matsadler/magnus) on the
  [`rb-sys`](https://github.com/oxidize-rb/rb-sys) toolchain.

Those crates are a build input, not the deliverable. **Each binding is
published through its own language's registry** — a Hex package on `hex.pm`
for Elixir, a gem on `rubygems.org` for Ruby — because that is where the
people who need it already look, and it is what lets a host app declare Trellis
in `mix.exs` or its `Gemfile` alongside every other dependency. Neither binding
crate goes to `crates.io`: a Rust program has no use for one, and would depend
on `trellis` directly. Each published package carries a compiled native
extension, so an installing application needs no Rust toolchain.

**There is no shared FFI library between them.** Specifically, no
`trellis-ffi` cdylib exporting a C ABI for `ruby-ffi` and Erlang's raw NIF API
to consume.

Rustler and Magnus are both Rust-native: they encode and decode host values
from Rust types directly, so a `Vec<QuarantineEntry>` becomes a list of Elixir
structs or an array of Ruby objects in one step. Routing through a C ABI would
mean flattening every rich type (`Option<TransformStatus>`, a paging cursor,
a nested error) into C structs *and then* re-inflating each one in each host
language — two hand-written marshalling layers per type instead of one, plus
manual allocation and free contracts across the boundary, in the one place
where a mistake is a segfault rather than an exception. The only thing a C ABI
buys is a third host language later, and that is cheaper to add as a third
Rustler/Magnus-style crate than to pay for up front.

A shared *Rust* helper crate for the plain-data conversions in decision 4 is
compatible with this decision and may be introduced if the two bindings
duplicate enough conversion code to justify it. It would hold no Rustler or
Magnus types, and it is not the C-ABI shim rejected above.

## Decision 2: bindings never reimplement the client over SQL

A binding may not reach Trellis's catalog, staging, or target tables with its
own SQL. Every operation goes through the `trellis` crate.

The alternative — a pure-Ruby gem and a pure-Elixir library issuing SQL
directly — needs no native extension at all, which is genuinely attractive for
packaging. It is still the wrong call. The behavior a client has to get right
is not "run this query": it is the instance-identity check, the pinned
`search_path`, the grammar, the coverage fence captured at definition time,
the backfill enumeration, and the quarantine addressing rules. Every one of
those is a moving contract, and putting it in three independent
implementations means every engine change lands three times and is correct in
at most one of them until someone notices. Divergence here is silent and
produces wrong derived data, not an error.

The corollary: the bindings are deliberately thin. Where a host-language
convenience would require knowing something about Trellis's schema that the
crate does not expose, the answer is to widen the crate's API, not to reach
around it.

## Decision 3: the binding owns one handle; Rust owns its threads

**One handle per OS process, created at boot, held as an opaque resource.**
This resolves ADR-0008's first open question ("one `Trellis` handle per app
boot, or a fresh connection per call?"). `Trellis::connect` runs the
instance-identity check, builds a connection pool, and optionally starts
background workers — none of which is per-call work.

* **Elixir:** a `ResourceArc` holding the `BlockingTrellis`, owned by a process
  in the application's supervision tree, so shutdown is a supervisor concern
  and the resource destructor is only the backstop.
* **Ruby:** a `TypedData` object behind a module-level singleton, with an
  explicit shutdown and `at_exit` as the backstop.

**Every call is dirty/GVL-released.** Each `BlockingTrellis` method blocks on
a database round trip — far past what either host VM tolerates on a scheduler
thread.

* **Elixir:** every NIF runs on a dirty IO scheduler, with no exceptions for
  calls that look cheap (`status/2` is still a query). The one true exception
  is reading the metrics registry, which is a process-wide in-process read with
  no round trip.
* **Ruby:** every call releases the GVL for its duration, so other Ruby threads
  keep running, and supplies an unblocking function so `Thread#kill` and
  Ctrl-C are not swallowed. That requires the reply wait to be interruptible
  rather than a bare blocking receive.

**Rust's threads stay Rust's, and are bounded.** The host VM does not schedule,
see, or join the Tokio runtime thread, the pool's connections, or the drain
workers; `shutdown` is the only supported way to stop them. Because those
threads are invisible to the host's own sizing, the binding must bound them
explicitly: `BlockingTrellis` currently builds a multi-thread runtime defaulting
to one worker per core, which inside a BEAM node that already sized its
scheduler pool to core count doubles the thread population. The runtime's work
is IO, not compute, so the bindings pass a small explicit worker count. This
needs a knob the crate does not have yet.

**A handle does not survive `fork`.** Puma, Unicorn, Passenger, and Resque all
fork; Rust threads do not cross `fork`, so a child inherits the handle struct
and its file descriptors but no threads to service them, and every call hangs
on a reply that can never arrive. The Ruby binding therefore connects *after*
fork and records the owning pid in the handle, raising on any call from a
different pid rather than deadlocking. The BEAM does not fork, so this is a
Ruby-side rule, but the pid guard is cheap enough to carry in both.

## Decision 4: only plain data crosses, and errors cross as `(code, message)`

**Errors.** The boundary carries exactly the stable `ErrorCode::as_str()` plus
the `Display` message. No error chain, no `source()` walking, no internal
variant names — ADR-0008 decision 3 built `ErrorCode` for this and the
bindings do not go around it.

* **Elixir:** `{:error, %Trellis.Error{code: :validation, message: "..."}}` from
  non-bang functions; bang variants raise the same struct.
* **Ruby:** an exception hierarchy under `Trellis::Error`, one subclass per
  code, each carrying `#code` and `#message`. Raising is the idiomatic default;
  no tuple returns.

`ErrorCode` is `#[non_exhaustive]`, so each binding needs a fallback arm
mapping an unrecognized code onto its base error type, and a test asserting
every *current* variant has an explicit mapping — so adding a code is a
deliberate binding change rather than a silent downgrade.

**Values.** Only plain data crosses. Engine types that carry structure are
flattened on the Rust side, once:

* **`Definition` does not cross.** It carries the parsed `TransformDef` AST and
  a `HashMap<String, ValueType>`. `define` returns the summary fields (`id`,
  `target_table`, `source_table`, `source_version`, `status`, `created_at`)
  plus source columns as a name → type-name map. An embedder who wants the AST
  wants the Rust crate.
* **`SystemTime`** crosses as epoch microseconds and is rehydrated to
  `DateTime` / `Time` in the host layer.
* **`TransformStatus` and `QuarantineState`** cross as their `as_str()` forms
  and become atoms (Elixir) or symbols (Ruby) drawn from a closed set
  allocated at load time — never `String.to_atom` on a value from the database.
* **`QuarantineTarget`** crosses as its `transform` / `transform.column`
  address string, the addressing idiom ADR-0008 decision 5 already settled, not
  as a two-variant struct.
* **`sample_quarantined`'s cursor** crosses as an opaque value the caller
  round-trips from the previous page, rather than a tuple callers construct.

The rule behind each of these: a shape is allowed to cross only if both host
languages can represent it without either one inventing semantics for it. When
a type fails that test, it is flattened in Rust, where there is one
implementation to get right — which is decision 2 applied to types.

## Consequences

* The `trellis` crate stays the single implementation of client behavior, and
  gains API surface when a binding needs something, rather than the bindings
  growing their own SQL.
* Two binding crates must be kept in step with the crate's public API. Keeping
  them in this repository, built and tested by the same CI run, is the cheapest
  way to hold that line.
* Decision 3 requires a runtime worker-thread knob the crate does not have
  today.
* Decision 4's flattening is mechanical and testable in Rust, ahead of either
  host language, and should be.

## Open questions

* Published package and module names on Hex and RubyGems — expensive to change
  after a release, and `trellis` may be taken on either.
* A multi-tenant host (one application, several databases) implies several
  handles, which is in tension with decision 3's per-process singleton shape.
  Deferred until a concrete need appears.

## Related issues

#82 / [ADR-0008](0008-public-api-design.md) (the public API these bindings
wrap), #55 (the status lifecycle behind the poll-to-`live` contract), #49 /
[ADR-0009](0009-observability-decisions.md) (the metrics and logs an embedder
surfaces).
