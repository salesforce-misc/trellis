# Binding parity fixture

`cases.json` is the one description of what the Elixir and Ruby bindings
return, shared by both (issue #155). Each host's test suite runs it against a
Trellis it connects itself, and checks every result against the shape written
here:

- Ruby: `clients/ruby/test/parity_test.rb`, with `test/support/parity.rb`.
- Elixir: `clients/elixir/test/trellis/parity_test.exs`, with
  `test/support/parity.ex`.

Neither suite owns the file. Each binding's own tests check its conversions
in its own idiom. This file checks that both read the same `trellis-embed`
flattening the same way: field names, which values can be `nil`, words
against strings, times, empty results, paging, and errors. If one binding
drifts, its suite fails here even when its own tests still pass.

The suites run as part of each binding's ordinary test run (`bundle exec rake
test`, `mix test`), so they run in the `ruby` and `elixir` CI jobs.

## Layout

- `records`: every value type a binding returns, and the shape of each field.
  A record's fields must match exactly: no missing fields, no extra ones. Each
  suite also checks that this list is exactly its own value types, field for
  field.
- `live`: a script of steps, run top to bottom on a private testkit cluster
  (it runs the staging worker, and it ends by poisoning a key, #588). A step is
  `{"op", "args", "opts", "expect" | "until" + "waiting_for", "save",
  "covers", "about"}`.
  - `op` is one of the binding's calls (`connect`, `apply`, `status`, ...), or
    a harness step: `sql` (run each statement over the suite's own
    connection) or `now` (save the host's current time).
  - `args` are positional and `opts` are the call's options. Each host
    passes them however its API takes them. Ruby uses keywords. Elixir uses
    a keyword list, except `await_converged`, where `timeout_ms` is
    positional.
  - `until` polls the call until its result matches, for at most 30 seconds,
    then fails naming `waiting_for` and the last result (#297).
  - `save` keeps the host's result for a later `{"$ref": "name.field"}`.
  - `covers.statement_kind` marks the step that applies each
    `trellis::StatementKind`.
- `conversions`: native maps (as the extension or NIF hands them over) for
  values a live run can't produce cheaply, such as a failing backfill, a
  pre-epoch time, an `Applied` or an error code newer than the binding.

## Arguments

JSON values are taken as they are. The exceptions are
`{"$ref": "path"}` (a saved result, then down its fields and list indexes),
`{"$word": "strict"}` (a Ruby symbol or Elixir atom),
`{"$time_micros": N}` (a host time) and `{"$dsn": {"user": ...}}` (the
cluster's connection string with those keys replaced).

## Canonical form

Before comparing, each host turns its result into the same canonical form:

| Host value | Canonical |
|---|---|
| `nil`, booleans, integers, UTF-8 strings, lists | themselves |
| a symbol (Ruby) or atom (Elixir) | `{"$word": name}` |
| a UTC `Time` or `DateTime` at microsecond precision | `{"$time": epoch micros}` |
| a `Trellis::` Data value or `Trellis.` struct | `{"$record": name, "fields": {...}}` |
| a Hash or map with string keys | `{"$map": {...}}` |
| a raised `Trellis::Error` subclass, or `{:error, %Trellis.Error{}}` | `{"$error": code}` |

Some differences between the hosts are deliberate, and the canonical form
absorbs them:

- Elixir's `{:ok, value}` is `value`, and its `:ok` is `nil`, which is what
  Ruby returns from `migrate`, `request_backfill`, `await_converged`,
  `connect` and `shutdown`.
- Elixir's `apply/2` returns a tagged tuple (`{:resumed, columns}`,
  `:paused`, ...). It becomes the `Applied` record Ruby returns, with the
  kind's fields set and the rest `nil`.
- A Ruby error must be the class its code names (`not_found` is
  `Trellis::NotFoundError`). An Elixir error must be a `Trellis.Error`, and
  its bang variant must raise the same code.

A time that isn't UTC or has sub-microsecond digits, a map key that isn't a
string, or a value of any other type fails the step. It doesn't reach the
comparison.

## Shapes

| Shape | Matches |
|---|---|
| `null` | `nil` |
| `"string"`, `"integer"`, `"boolean"`, `"time"` | any value of that type |
| `{"eq": v}` | exactly `v` |
| `{"word": w}` | the word `w` |
| `{"word_in": set}` | a word from a closed set the extension reports: `transform_status`, `quarantine_state`, `cardinality`, `applied_kind`, `self_check_outcome`, `divergence_kind` |
| `{"time_micros": n}` | the time `n` microseconds from the epoch |
| `{"error": code}` | an error with that code |
| `{"ref": path}` | the same canonical value as a saved result |
| `{"nullable": s}` | `nil`, or `s` |
| `{"list": [s, ...]}` / `{"list_of": s}` | a list, item by item or every item |
| `{"map": {k: s}}` / `{"map_of": s}` | a map, key by key or every value |
| `{"record": name, "fields": {f: s}}` | that record, with `fields` narrowing the schema's shape for the fields named |

The word sets come from `trellis-embed` through each binding's native
module, and are never written out in either suite. So a status the engine
adds or drops (`catching_up`, #625) changes what `word_in` accepts, and the
fixture is left alone. Only a step that names a specific word has to change.

## Coverage

Each suite also fails when:

- an error code in `trellis-embed`'s `ERROR_CODES` has no live step that
  returns it;
- a `trellis::StatementKind` has no step that `covers` it;
- a record in `records` is never checked, or the binding has a value type
  (or field) the fixture doesn't describe;
- a public call of the binding isn't a fixture operation, or an operation is
  never run. Ruby's `connected?` is the one exception: it has no Elixir
  counterpart, because the Elixir handle is a value the caller holds.

## Adding a case

Add a step where the script already has the state it needs. Phase one runs
no background workers, so its results are deterministic. Prefer it, and put
a step in phase two (after the connect with the staging worker and drain
threads) only if it needs a live definition. Then run both suites: the
fixture only shows parity when both of them pass.
