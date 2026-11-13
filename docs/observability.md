# Observability

How an operator sees what the pipeline is doing: how fast changes propagate,
where they're stuck, and what work is pending or blocked. The counterpart to
[data-flow](data-flow.md) — that describes the flow; this describes how we
*measure* it. Rationale and alternatives live in
[ADR-0009](decisions/0009-observability-decisions.md).

## Goals and non-goals

**Goals**

* **Latency visibility** — median and p99 per transform, plus median and p99
  end-to-end (a source commit propagating through the whole chain to its final
  apply).
* **Pull-based export** — metrics in Prometheus text format, scraped into
  whatever the operator already runs.
* **Structured logs** — through a facade that can export OpenTelemetry.
* **Transform status** — every transform carries an observable lifecycle status
  (`waiting_to_backfill` → `backfilling` → `live`, plus `quarantined` and
  `paused`, which a halting failure also uses, and `catching_up`, which is
  reported while an upstream isn't `live`), so an
  operator can tell a new transform is still populating rather than live — the
  right-sized answer to the silent-stall problem.
* **Fleet-level worker liveness** — `Trellis::has_live_drain_workers`
 and `Trellis::has_live_staging_worker` answer the
  coarser, fleet-wide questions a per-transform status can't: is *any* drain
  worker running to build and maintain targets, and is the staging worker
  running to capture changes and move a new transform out of
  `waiting_to_backfill`. A healthy fleet needs both. See
  [docs/embedding.md](embedding.md#the-silent-stall-hazard) for the
  embedded-deployment misconfiguration this exists to catch.

**Non-goals**

* Being a metrics *backend*. Trellis exposes an in-process registry for
  scraping; it retains no history and does not replace Prometheus/Grafana.
* Tracing the *application's* write path. We instrument Trellis's own pipeline.

## The two subsystems

Metrics and logs are deliberately **separate subsystems**, each idiomatic on its
own and evolving independently:

```
metrics ──► in-process registry ──► render_prometheus()  (operator scrapes)

logs/spans ──► `tracing` facade ──► optional OTLP export layer
```

## Metrics

### What "latency" means

The clock is already flowing in: every staged source change carries
`src_changed`, the time its capture trigger ran (`trellis/src/capture/sql.rs`).
That is the change's time, not its commit's. Two families of measurement
follow:

* **Per-transform latency** — from a change arriving at a transform's input to
  its output being applied. One histogram per transform.
* **End-to-end latency** — from the *source* change to the *final* transform's
  apply, keyed by the **terminal transform** (not source→sink pair) to keep
  cardinality low.

The two histograms are:

* `trellis_transform_latency_seconds{transform}` — per-transform latency, one
  observation per folded change.
* `trellis_end_to_end_latency_seconds{transform}` — end-to-end latency, recorded
  only for a terminal transform (one whose target nothing else reads), summed
  across every source feeding it, and never for an intermediate hop.

Both use one exponential bucket set from 10 ms to 60 s.

Supporting counters/gauges keep the histograms interpretable. Every series
below, and the histograms above, also carries the `trellis_instance` label
([below](#several-instances-in-one-process)). Each name below takes the
`trellis_` prefix in the exposition:

* `changes_applied_total{transform}` — staged changes each transform folded
  and applied. One change is one row in the staging ring: a source row change
  that a capture trigger staged, or a row an upstream transform's
  write staged for the next hop. The count doesn't depend on how rows were
  batched, so you can compare it with the source's own write rate. The one
  exception is a `TRUNCATE`: it counts as one change, and rows staged before it
  that hadn't been applied yet are dropped uncounted, because they never reach
  the target. It adds up
  across hops. If 200 source rows update 10 keys of an aggregate that feeds a
  second transform, the aggregate reports 200 and the second transform reports
  10. It isn't the latency histograms' denominator. The claim-time fold
  collapses a key's rows into one folded change, and the histograms observe
  once per folded change, so `transform_latency_seconds_count` is the
  folded-change rate. Folded changes with no origin timestamp (bare recompute
  triggers) aren't observed there.
* `column_pause_lock_timeouts_total{op}` — waits for the column-pause lock that ran
  out the transaction's `lock_timeout` (30 s, or one second for the capture pass's
  cascade pairs), by the operation that waited: `pause`, `resume`, `fuse`, `cascade`,
  `define`, `drop`, `alter` or `capture`. Pause operations of an instance serialize on its one lock
  ([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)), so a nonzero rate means a
  holder stayed in it too long, or column pauses became a hot path. Each timeout failed
  one call with a retryable error; nothing is retried in a loop.
* `capture_auto_rebuilds_total{source_table, cause}` — rebuilds of a source
  table's readers that the staging worker's capture pass started on its own,
  pausing nobody, because they missed changes no capture trigger saw while
  their definitions stayed valid
  ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#a-column-changed-or-rewritten-without-a-trigger)).
  `cause` is `value_rewrite` (a read column's values rewritten in place by an
  `ALTER COLUMN … TYPE` to the same type with a `USING`), `column_changed`
  (a read column dropped and re-added, or retyped other than by a pure
  widening) or `generated_expression` (a read stored generated column's
  values rewritten under a new expression by `ALTER COLUMN … SET
  EXPRESSION`). Each count is one re-read of the table, the same as a
  `request_backfill`, and a `WARN` log names the table, the cause and the
  readers. Watch it to see how often these are detected: a steady rate means
  routine DDL keeps meeting a detector, which costs a rebuild each time.
* `held_key_retries_total` and `held_key_retries_succeeded_total` — held keys the
  staging worker released on its own ([the automatic retry](decisions/0003-quarantine-storage-and-api.md#retry-policy),
  off unless `TrellisOptions::held_key_retry` is set), and the retries whose key then applied
  cleanly and left quarantine. The difference is the retries that were poisoned again, or
  have not drained yet. The first counts a pass's releases; the second counts on the page that
  clears the key, in whichever process drains it.
* `seal_refused_total` — seals refused because the staging ring had no free slot,
  after the retirement the seal answers that with. A steady rate means capture
  outruns the drain and every slot is in use; see
  [recommendations](recommendations.md) for the ring's disk growth.
* `build_statement_seconds{class}` — worker time a Re-derive build spends in each
  class of statement: `chunk_setup` (the chunk's catalog reads, pooled
  connection, begin and claim fence), `chunk_keys` (the read of the keys in its
  range), `chunk_lock` (the sorted entry lock), `chunk_write` (the inserts and
  rewrites) and `chunk_commit` (the commit and marking the chunk done), and
  `plan` (the plan job's boundary walk and chunk inserts). The `_sum` of a
  class is worker-seconds; divide by `build_rows_total` for the cost per row
  built.
* `build_chunk_seconds` — the duration of each committed Re-derive build chunk's
  transaction, from its begin to its commit, the span its entry locks and
  snapshot are held. Its buckets are finer than the latency histograms': about
  25% apart from 1 ms to 125 s. `build_chunk_seconds_max` is a gauge of the
  longest such chunk the instance has committed.
* `page_transaction_seconds` — the duration of each committed drain page's apply
  transaction, from its begin to its commit: the span the page holds the entry and
  target-row locks of its keys. It has `build_chunk_seconds`' buckets.
* `build_rows_total` — source keys that Re-derive build chunks have re-derived.
* `build_chunk_lock_timeouts_total` — build chunks that gave up on their entry
  lock's short timeout and were released for a retry. A nonzero rate means a
  page or writer holds the keys a build wants.
* `join_lock_wait_seconds{transform}` — the time a `JOIN` page spends locking its
  `J` rows (the lock wait plus the read). A rising tail shows contention on hot
  join keys.
* `staging_segments{state}` — a cheap system-level gauge counting segments by
  state (ties to [the staging ring](staging-and-claiming/02-the-staging-ring.md)),
  chosen over a per-transform depth gauge for lower cost. The staging worker
  counts them every 5 s (`housekeeping_interval`).
* `ring_rows`, `ring_bytes`, `ring_oldest_undrained_age_seconds` and
  `ring_undrained_bytes{source_table}` — the staging ring's backlog, read by the
  same 5 s housekeeping statement that counts the segments, so they add no
  statement or transaction to the staging worker's idle load. The ring is the
  application's database's own disk: these are the numbers to size and alert on
  when capture outruns the drain
  ([recommendations — Ring sizing](recommendations.md#ring-sizing)).
  * `ring_bytes` is `pg_total_relation_size` over the four ring slots,
    heap and index, drained segments awaiting retirement included. `ring_rows`
    is their rows. Each slot is read whole while it is small; a slot of more
    than about 22,000 captured rows (512 pages) is estimated from a sample of
    its pages, so its rows are approximate.
  * `ring_oldest_undrained_age_seconds` is the age of the oldest row (by its
    `appended_at`, its writer's transaction start) in a segment that has not
    fully drained, as of the read, and 0 while none is waiting. It is read from
    the first rows of each such segment, so it also counts a bucket of the
    segment that has drained already, and is exact only to the skew
    of concurrent writers. Between two reads the gauge holds its last value.
  * `ring_undrained_bytes{source_table}` is the bytes of the segments that have
    not fully drained, split by the table each row belongs to in proportion to
    the sizes of the sampled rows. A segment counts whole until its last bucket
    drains. A table whose rows have all drained reads 0 for one read, then its
    series expires. The split shows which tables hold the backlog.

* `definitions{status}`, `paused_definitions{cause}`, `held_keys`,
  `quarantined_columns`, `drain_failures` and `capture_holdups{kind}` — what a
  definition's [status](status-reference.md) reports, in a form Prometheus can
  alert on. The staging worker reads them in the same 5 s housekeeping pass,
  from the catalog in one snapshot: the statuses `definitions()` reports, why
  each paused definition paused, and one statement of counts. Nothing is
  counted on the hot path, and no label names a definition, a table or a key.
  * `definitions{status}` counts definitions by each of the six status words,
    `catching_up` derived as `status()` derives it. Every word is always set,
    0 where no definition has it.
  * `paused_definitions{cause}` splits the `paused` count by cause, so an
    operator's `PAUSE` is told apart from a stop: `operator` (a pause with no
    record of why), `capture`, `disruption` and `halt` (the kind of the
    definition's [`capture_failure`](status-reference.md#capture_failure)),
    `backfill_failure` (its build kept failing) and `unknown`. They add up to
    `definitions{status="paused"}`. A definition an operator paused that then
    meets a disruption counts as `capture`, as its `capture_failure` says.
  * `held_keys` is the source keys held in quarantine, every definition's
    together, whatever its status. `quarantined_columns` is the columns of 1-1
    targets whose own fuse tripped; a column an operator paused
    (`PAUSE TRANSFORM t.col`) and the readers a pause cascades to aren't
    counted.
  * `drain_failures` is the open [drain failures](status-reference.md#drain_failure),
    one per failing segment. `capture_holdups{kind}` is the tables whose
    capture install is held up: `wait` for a lock another session holds
    ([`capture_wait`](status-reference.md#capture_wait)), `failure` when the
    install or widen fails for another reason. Neither changes a status word.
  * `staging_leader` is 1 in the process that holds the staging-worker lock and
    0 in one that stands by for it. `drain_workers` is the live drain workers in
    the worker registry, the count `has_live_drain_workers` tests.

  Only the process that leads staging sets all of these. A standby sets
  `staging_leader` 0 and nothing else, and a process with no staging worker (a
  web handle) sets none. A process that stops leading stops setting them, so
  they expire like any gauge ([below](#several-instances-in-one-process)).

Backfill progress, how far through its build a definition is, is deliberately
*not* a metric: it's the transform's [lifecycle status](status-reference.md).

### Alerting

Aggregate with `max by (trellis_instance)`: a leader change leaves the old and
the new leader's series side by side for up to 60 s.

| Alert | Expression |
|---|---|
| A definition stopped | `max by (trellis_instance) (trellis_paused_definitions{cause!="operator"}) > 0 or max by (trellis_instance) (trellis_definitions{status="quarantined"}) > 0` |
| Keys are held | `max by (trellis_instance) (trellis_held_keys) > 0` |
| A column's fuse tripped | `max by (trellis_instance) (trellis_quarantined_columns) > 0` |
| The pipeline is behind | `max by (trellis_instance) (trellis_ring_oldest_undrained_age_seconds) > 60` |
| No staging worker | `sum by (trellis_instance) (trellis_staging_leader) < 1 or absent(trellis_staging_leader)` |
| No drain worker | `max by (trellis_instance) (trellis_drain_workers) < 1` |

A stopped definition is invisible to the ring gauges: its writes are still
captured and drained, or not read at all, so `ring_*` and `changes_applied_total`
look like a quiet system.

"No worker anywhere" is the absence of the series, or a scrape target that is
down: only a process running a staging worker sets them, so `absent(...)` and
`up == 0` on the worker's job are the alerts for it. A fleet whose staging
worker runs in one process has no `drain_workers` series while that process is
down; the no-staging-worker alert is the one that fires.

### Several instances in one process

One process can hold several handles, for different catalog schemas or
different databases. They share one registry, and `render_prometheus()` returns
every handle's series in a single body, once. Every series carries
`trellis_instance="<database>/<schema>"` (`mydb/trellis`), always, even with a
single instance. It is the value the `trellis_instance` field of the instance's
[log events](#logs-and-traces) carries, so a series and a log line of one
instance match. The label is `trellis_instance` and not `instance` because
Prometheus attaches its own `instance` label to every scraped series (the scrape
target), and renames a clash to `exported_instance`. Two handles of the same
instance in one process write the same series. Handles in different processes
stay apart by that scrape-target `instance` label.

An instance whose handle stopped (shut down, or dropped) leaves its series in the
registry:

* **Counters and histograms** stay at their last value. Their rate is 0, which
  is true.
* **Gauges** (`staging_segments`, the `ring_*` gauges, the status and liveness gauges, `build_chunk_seconds_max`) disappear once
  nothing has set them for 60 seconds. A handle's background client (`staging`
  or `drain_threads`) sets each of its instance's gauges again every 300 ms
  (`maintenance_interval`), to its last known value, so a gauge expires only
  once no client of its instance runs in the process. A running instance whose
  staging worker cannot read the ring keeps reporting the last segment counts,
  ring gauges and status gauges it read. A process that stops leading staging
  stops setting them, so a standby never reports what the leader reports.
* **`trellis_instance_up{trellis_instance}`** is 1 while at least one handle of
  the instance runs in this process, and 0 once the last one has shut down or been
  dropped. It never expires, so it keeps reporting 0. Alert on
  `trellis_instance_up == 0`, not on the absence of a series.

A process restart empties the registry, so `trellis_instance_up` has no series
for an instance until its first handle in the new process starts.

### Quantiles via histograms, not summaries

Median and p99 are computed at query time from exported bucket counts
(`histogram_quantile` in PromQL), not client-computed summary quantiles.
Histograms **aggregate across instances**; summaries do not — and a deployment
may run more than one engine process against the same cluster. The cost is
choosing bucket boundaries up front: a single global exponential set spanning
~10ms–60s, not per-transform.

### Exposition: a mountable handler, not a bound port

The library stays HTTP-agnostic. It exposes a render function over its registry;
the operator serves the result from their own HTTP stack — no port binding, no
framework, no bind-address config.

[`Trellis::metrics`](../trellis/src/app.rs) — and
[`BlockingTrellis::metrics`](../trellis/src/blocking.rs) for callers without a
`tokio` runtime — returns a [`Metrics`](../trellis/src/metrics.rs) handle whose
[`render_prometheus`](../trellis/src/metrics.rs) returns the Prometheus text as a
`String`:

```rust
let body = trellis.metrics().render_prometheus();
// operator serves `body` from their own axum/actix/hyper /metrics route
```

The registry is process-wide, so mount `render_prometheus()` from inside the
process running the engine — a separate scrape-only process renders an empty
registry. Mount it once per process: any handle's `metrics()` renders every
handle's series (see [above](#several-instances-in-one-process)).
The bindings expose the same render as
`Trellis.Metrics.render_prometheus/0` (Elixir) and
`Trellis::Metrics.render_prometheus` (Ruby), each for a route the host app
already serves; Ruby also ships `Trellis::Metrics::RackApp`, a mountable Rack
route. For a process with no HTTP stack, `trellis run --prometheus-bind
<ADDR>` and the Ruby binding's `metrics_bind:` serve the registry from one
small TCP listener in `trellis-embed`
([`metrics_http.rs`](../clients/embed/src/metrics_http.rs)), alongside the
live pipeline.

### Retention: left to Prometheus, not Trellis

Trellis retains no metric history. Retention, rollup, and cross-instance
aggregation are what Prometheus/VictoriaMetrics/Thanos already do well;
duplicating that inside Trellis would add write load for no new capability.
Operators who want history point their scraper's retention at the
`render_prometheus()` endpoint (or `trellis run --prometheus-bind`) like any
other target.

## Logs and traces

Logs go through the **`tracing`** facade with an optional **OTLP export layer**:
operators running an OpenTelemetry collector get compliant output, and those who
don't still get structured local logs.

A change flowing source → hop → hop → apply *is* a trace. We adopt **spans** as a
first-class signal, modeling propagation as a `tracing` span tree. This makes the
pipeline's shape observable and carries per-hop latency for free — the
per-transform latency histogram is *derived from* span durations captured during
apply, not instrumented independently.

Every event the engine emits carries a `trellis_instance` field naming the
instance that logged it: its database and catalog schema, as `mydb/trellis`.
Several instances can share a process and a database, and the field is what
separates their lines in one stream. It is an event field rather than a span,
so it reaches every subscriber, including ones that record events only
(Elixir's `Logger`), and it adds no span to an OpenTelemetry trace.

The crate never installs a subscriber; the embedder does. The bindings do it
on the host's behalf. Elixir's `Trellis.LogBridge` installs one at application
start that forwards events (not spans) to `Logger`. Ruby's
`Trellis::LogBridge` does the same once the app sets `Trellis.logger`, which
the Rails integration sets to `Rails.logger`
(`clients/ruby/README.md`). A host can opt out, but composing its own
subscriber means building its own native library around the crate: the global
subscriber belongs to the copy of `tracing` linked into that library, so one
installed from another library never sees the engine's events
(`clients/elixir/README.md`).

## Stops and waits

Rather than instrument the backfill with bespoke metrics, every transform
carries an observable status, the same lever quarantine and pause use, so
backfill, quarantine and pause are arcs of one lifecycle. The
[status reference](status-reference.md) defines each status word and every
field of `DefinitionStatus`, and
[operations](operations.md#when-a-definition-needs-you) lists every reason a
transform stops and what to do. This section explains the mechanics behind
some of those fields.

### A build's fence on a seam-fed source

A Re-derive build starts with no transaction fence, and each of its chunks
reads under its own short snapshot after locking the ledger entries it
rewrites, so a long transaction elsewhere doesn't hold it, and it holds no long
snapshot of its own. The one exception is a build that reads another
transform's target (its source, or a `JOIN`'s joined table), which takes one
fence, after it starts, before its first chunk or its one-transaction build: a
transaction that held a transaction id then (one that had
written or locked a row) holds it, and `status().build_wait` names the fence
(`BuildWait::Fence { xid }`, a `backend_xid` at or below it in
`pg_stat_activity`; the Elixir and Ruby bindings report the `xid` as
`build_wait_xid`) until every such transaction has ended
([data-flow — The Re-derive build](data-flow.md#the-re-derive-build)). Because
the wait is on transactions older than the fence anywhere in the cluster, an
unrelated long-running transaction holds such a build in `backfilling` until
it ends.

This wait is **safe, not a fault**: the apply loop and every `live` transform are
unaffected, and the new definition applies from its start. So we deliberately
emit no stall metric, warning log, or timeout; `build_wait` is the whole
signal. The remedy is documentation, not a signal: an operator clears the wait
by ending the pinning transaction (idle-in-transaction connections, long
analytics queries, `pg_dump`, or workload on another database sharing the
cluster).

### Another version is live

While a Trellis process of another version is live on the instance (a deploy
that crosses a release), `status().version_handover` is set: it names this
binary's version and each live process of the other version by worker id and
version (`None` for a release before 0.2.0, which records none), and whether a
staging worker that registers no version holds the staging lock. Meanwhile
this binary's background processes stage, drain and build nothing, its
state-changing calls refuse with the `version_handover` error code, and each
background process logs a warning once when it starts standing by and an info
line when it takes over ([version handover](decisions/0023-version-handover.md)).
A process killed without a clean shutdown keeps counting until its registry row
ages out, up to the reclaim TTL of 30 seconds. The Elixir and Ruby bindings
report it as `version_handover`.

While this binary's migrations haven't run on the instance,
`status().migrations_pending` is set: it names them, and `cannot_run` says why
this handle's background process couldn't run them, when it tried and was
refused (its role can't alter Trellis's tables, say), with the remedy. A
background process runs them itself once no process of another version is
live, then logs an info line naming the migrations that ran as it takes over.
Where the catalog can't be read until they run, `status()` and
`definitions()` refuse with the `version_handover` code, naming them, and
`trellis status` prints them on a `Migrations pending:` line. The Elixir and
Ruby bindings report the refusal; they don't carry `migrations_pending` yet.

Every definition's status also carries its `floor`, the oldest Trellis release
that can run it ([defining transforms — Floors](transforms.md#floors)), which
`trellis status` prints as `floor=` and the bindings report as `floor`.

### Below the catalog floor

While this binary's version is below the instance's catalog floor, the oldest
Trellis version that can read its catalog (a later release migrated it),
`status().below_catalog_floor` is set: it names the floor and this binary's
version. Meanwhile this binary's background processes stage, drain and build
nothing and register nowhere, `migrate` and its state-changing calls refuse
with the `version_floor` error code, and each background process logs a
warning once when it starts standing by and an info line when it takes over
([the catalog floor](upgrading.md#the-catalog-floor)). It checks again every
standby poll (5 seconds). The Elixir and Ruby bindings report it as
`below_catalog_floor`.

### A build that keeps failing

A plain 1-1 transform's build runs as
chunks of its source's primary-key range on the drain workers, and each failing chunk is
logged as a warning with the definition, the chunk, the attempt and what happens next.
A transient failure (a lost connection, a lock conflict) is retried with backoff and
counts toward nothing. A failure on a row's data (SQLSTATE class `22` or `23`) is split
down to the row's key, which is quarantined as the drain quarantines a key
([ADR-0003](decisions/0003-quarantine-storage-and-api.md)); the build finishes without
it, `held_keys` counts it, and `Trellis::sample_quarantined` lists it. The key is held for this transform only:
every other transform reading the table keeps applying it, and the whole-transform fuse
counts it against this transform alone. Any other failure is retried, and its
fifth charged attempt pauses the transform (an aggregate's
build, which can't be narrowed to a key, is paused the same way). Fix the
cause and resume the transform, which rebuilds it. Meanwhile
`DefinitionStatus::backfill_failure` carries the failing chunk's error, attempt count
and next attempt time; a transform paused this way keeps the error there until it
is resumed.

### A halting failure

Some failures are no row's fault, so every key of the page reproduces them:
a source key the drain can't use (the table lost its primary key, or the key
changed to a type Trellis can't key by), a propagation wave past the hop bound,
an aggregate the group basis can't apply, or Postgres refusing the drain's role a read
or write (row-level security, which Trellis's `row_security = off` turns into an
error, or a missing privilege). Retrying the page would fail it forever,
and quarantining a key would blame one for nobody's fault. Instead the drain
**halts** the definitions the failure reaches and drains the page
without them:

* **What it pauses.** For a key error, every definition that reads the table.
  For a hop bound outside a
  cycle, the readers of the table the wave ran away through, not the
  definition that wrote it; inside a cycle, every member. For an aggregate the
  group basis can't apply, the definition that writes that target. For a refused read or
  write, the readers of each table the drain's role can't read and the writer
  of each target it can't write, as the catalog shows them for that role. In
  every case, also everything downstream of those, so no hop target goes
  quietly stale. Every
  other definition keeps converging, and the staging ring keeps retiring.
* **How it shows.** A halted definition is `paused`, like an operator pause,
  with no new status word. What tells it apart is
  `DefinitionStatus::capture_failure`: its `kind` is `halt` (rather than
  `capture`, a capture trigger that failed), with the table the failure named,
  the error and the time it halted. `Trellis::definitions` carries the same
  value as `DefinitionSummary::halt`, so one call finds every halted
  definition ([embedding — health checks](status-reference.md#capture_failure)), and
  the CLI's `trellis status` prints it on an indented line under each one.
* **One episode, one signal.** A halt logs one error line naming every
  definition it paused, and increments the single-row `halting_stops` table:
  `stop_count`, plus `last_reason` and `last_stopped_at` for the latest. A
  peer worker meeting the same failure, or a retry meeting it again, finds the
  closure already paused and records nothing, so the count is of episodes,
  not attempts. A halt that pauses nothing because a peer already paused the
  closure retries the page once. One that still pauses nothing (a refused
  read or write the catalog can't pin on a table,
  [row-level security](transforms.md#row-level-security-on-a-role-trellis-runs-as))
  surfaces the error and records the page as a drain holdup, which `status`
  reports as `drain_failure` on the definitions reading its tables. The drain
  worker re-claims the page at its poll interval under a collapsed warning.
* **A failing build start.** A Re-derive build's start that fails in a way
  that would repeat on every pass (its ledger table was dropped, say) halts the
  definition writing that target, and everything downstream of the target, the
  same way, with `kind` `halt`, the target as the table and the error.
  It never stops the staging worker's pass: the other definitions' builds
  start. A start that fails transiently, or
  by losing its connection, is retried on the next pass instead.
* **A schema the role can't use.** Each staging pass first halts the
  definitions that read or write a table in a schema its role lacks `USAGE`
  on, the same way, and leaves that schema's tables out of every step, their
  capture as it is. The rest of the pass runs for every other table, a halted
  `JOIN`'s tables in other schemas included.
* **Resuming.** Fix the cause, then `RESUME TRANSFORM` each halted definition,
  in any order; each resume rebuilds that definition as for any pause, and
  clears its halt. A resume re-validates the definition as define would, so
  while its source key is still of a type define
  refuses, the resume is refused and the halt stands. A cause define doesn't
  check (a propagation wave past the hop bound) halts it again after the
  resume, as a new episode with its own count and error line.

## Dependencies

Pinned in `trellis/Cargo.toml`; rationale in
[ADR-0009](decisions/0009-observability-decisions.md#1-dependencies-metrics-facade-not-prometheus-directly):

* `metrics` + `metrics-exporter-prometheus` for the registry and text rendering.
  The exporter's Hyper-listener feature is **not** enabled — this stays a pure
  registry + text-encoder.
* `tracing` (unconditional) for spans/events; `tracing-opentelemetry` +
  `opentelemetry-otlp` for OTLP export, behind the `otlp` Cargo feature (off by
  default, genuinely absent from the dependency tree when unused).

## Related

* [data-flow](data-flow.md) — the flow these metrics measure.
