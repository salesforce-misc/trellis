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
  right-sized answer to the silent-stall problem (#14).
* **Fleet-level worker liveness** — `Trellis::has_live_drain_workers`
  (issue #144) and `Trellis::has_live_staging_worker` (issue #428) answer the
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

Supporting counters/gauges keep the histograms interpretable. Every series
below, and the histograms above, also carries the `trellis_instance` label
([below](#several-instances-in-one-process)):

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
  `ALTER COLUMN … TYPE` to the same type with a `USING`) or `column_changed`
  (a read column dropped and re-added, or retyped other than by a pure
  widening). Each count is one re-read of the table, the same as a
  `request_backfill`, and a `WARN` log names the table, the cause and the
  readers. Watch it to see how often these are detected: a steady rate means
  routine DDL keeps meeting a detector, which costs a rebuild each time.
* `held_key_retries_total` and `held_key_retries_succeeded_total` — held keys the
  staging worker released on its own ([the automatic retry](decisions/0003-quarantine-storage-and-api.md#retry-policy),
  off unless `TrellisOptions::held_key_retry` is set), and the retries whose key then applied
  cleanly and left quarantine. The difference is the retries that were poisoned again, or
  have not drained yet. The first counts a pass's releases; the second counts on the page that
  clears the key, in whichever process drains it.
* `staging_segments{state}` — a cheap system-level gauge counting segments by
  state (ties to [the staging ring](staging-and-claiming/02-the-staging-ring.md)),
  chosen over a per-transform depth gauge for lower cost. The staging worker
  counts them every 5 s (`housekeeping_interval`).

Backfill progress is deliberately *not* a metric — it's the transform's
[lifecycle status](#transform-status-lifecycle), a small enumerable state.

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
* **Gauges** (`staging_segments`, `build_chunk_seconds_max`) disappear once
  nothing has set them for 60 seconds. A handle's background client (`staging`
  or `drain_threads`) sets each of its instance's gauges again every 300 ms
  (`maintenance_interval`), to its last known value, so a gauge expires only
  once no client of its instance runs in the process. A running instance whose
  staging worker cannot read the ring keeps reporting the last segment counts
  it read.
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
~10ms–60s, not per-transform in this pass.

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
`cli/src/commands/run.rs`'s `--prometheus-bind <ADDR>` flag is a minimal
template: a small TCP listener alongside the live pipeline answering each
request with the rendered registry.

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

The crate never installs a subscriber; the embedder does. The Elixir binding
does it on the host's behalf: `Trellis.LogBridge` installs one at application
start that forwards events (not spans) to `Logger`. A host can opt out, but
composing its own subscriber means building its own NIF around the crate:
the global subscriber belongs to the copy of `tracing` linked into the NIF
library, so one installed from another library never sees the engine's
events (`clients/elixir/README.md`, issue #149).

## Transform status lifecycle

Rather than instrument the backfill with bespoke metrics, every transform carries
an observable **status**. This is the same lever quarantine uses
([ADR-0003](decisions/0003-quarantine-storage-and-api.md) marks a fused transform
`quarantined`, and [ADR-0014](decisions/0014-pause-and-drop-a-transform.md) freezes one
as `paused`; both resume by re-running the *same* backfill), so backfill, quarantine and
pause are arcs of one lifecycle:

```
(new transform)──► waiting_to_backfill ──► backfilling ◄──► live
                          ▲                                    │
                          │ RESUME (re-runs the backfill)      │ PAUSE, auto-pause,
                          └──── paused / quarantined ◄─────────┘ or the fuse trips
```

* **`waiting_to_backfill`** — defined, but its source's existing rows haven't
  been read yet. Every new transform starts here, and registration returns
  with it here. It stays until the staging worker's reconcile pass finds its
  source (and a `JOIN`'s joined table) captured with the columns it reads, and
  no change staged before the capture install or widen is still pending
  ([stage 01](staging-and-claiming/01-capture-by-triggers.md#which-definitions-may-start-a-build)),
  then starts its build
  ([data-flow — Capturing a table's existing rows](data-flow.md#capturing-a-tables-existing-rows)).
* **`backfilling`** —* **`backfilling`** — the staging worker has started the transform's build,
  which is running on drain threads. The target is partial. A plain 1-1
  transform is built by the Re-derive build
  ([data-flow — The Re-derive build](data-flow.md#the-re-derive-build)), and an
  aggregate or a `JOIN` by one absolute write per structure
  ([data-flow — An aggregate's or a `JOIN`'s build](data-flow.md#an-aggregates-or-a-joins-build)).
  It is maintained from the start, so it goes from `backfilling` straight to
  `live` once its last chunk (or, for an aggregate or a `JOIN`, its write)
  has committed. Its resume rebuilds it
  the same way, a 1-1 target's over the ledger the freeze left (an aggregate's
  or a `JOIN`'s partials are written afresh), and so does a re-read of its source
  (`Trellis::request_backfill`) and the rebuild of an upstream whose target
  it reads. A `live` transform comes back here for a rebuild after:
  * an `ALTER TRANSFORM` that added columns or a resumed column (only those
    columns are rebuilt);
  * a re-read of a table it reads (`Trellis::request_backfill`, the table's
    capture triggers reinstalled after a restore brought them back disabled,
    or an automatic rebuild counted in `capture_auto_rebuilds_total`), in the
    call's own transaction (or the capture pass that starts the automatic
    rebuild), returning to `live` when the rebuild is done. Its target keeps
    its rows meanwhile: the rebuild rewrites them in place;
  * a rebuild of the transform whose target it reads, written outside the
    target-mutation seam.

  A `live` aggregate over a partitioned source, or a `live` `JOIN` with a
  partitioned table on either side, comes back here while it is corrected
  for a partition of that table that was attached, detached, dropped or
  truncated, or for rows it still counts that route to no current partition
  ([ADR-0017](decisions/0017-partitioned-sources.md)). The staging
  worker pass that detects the change queues the correction and moves the
  transform `backfilling` in the same transaction; until that pass, at most one
  pass after the DDL, it still reads `live`. It goes back to `live` once the
  correction's keys are re-derived and folded into its target, as at the end of
  a Re-derive build. Until a group that routes to the partition is corrected,
  rows an `ATTACH` added aren't counted in it yet, and rows a `DROP`, `DETACH`
  or `TRUNCATE` removed still are; groups of other partitions are exact. The
  correction's progress is persisted, so a
  restart resumes it and the transform still reads `backfilling`.
  `DefinitionStatus::correcting` lists one entry per partition it is being
  corrected for, oldest first: `node`, the schema-qualified partition (the
  source's root, for rows that route to no partition); `since`, when the pass
  queued the correction; and `keys_remaining`, the keys still to re-derive from
  the correction's persisted progress (`None` until its first page has counted
  them, and an estimate while it reads the partition's own rows). An entry
  leaves once its keys are re-derived. A paused or quarantined transform lists
  none; a correction that keeps failing is reported in `backfill_failure`. The
  CLI's `trellis status` prints one `correcting` line per entry under the
  transform.
* **`catching_up`** — reported, never stored: a `live` transform that reads an
  upstream which isn't `live` (paused, quarantined, rebuilding, or itself
  reporting `catching_up`, down the whole chain) keeps applying, but its
  target doesn't reflect the source yet, so a watermark token wouldn't wait
  for what is missing. `live` holds along a chain
  ([ADR-0002](decisions/0002-async-data-flow.md#what-live-promises)).

  So does a `live` transform that reads a paused column of an
  upstream 1-1 target (its fuse tripped, or `PAUSE TRANSFORM t.col`): an
  aggregate or `JOIN` keeps applying over the column's frozen values, exact
  against the target and stale against the source. `paused_upstream_columns`
  on `DefinitionStatus` names the column or columns, each `target.column`
  (the paused columns it reads, and those behind the transforms it reads,
  `[]` otherwise), and the CLI's `trellis status` prints a line under the
  transform. Nothing is paused or rebuilt: resuming the column writes
  through to the reader, the list empties and the status returns to `live`.
  A 1-1 reader's own field is paused with the column
  ([ADR-0003](decisions/0003-quarantine-storage-and-api.md)),
  and an aggregate that reads only other columns stays `live`.
* **`live`** — the steady state: a watermark token taken after a commit and
  awaited with `Trellis::await_converged` guarantees the target reflects that
  commit. The rows a 1-1 build staged into the ring for keys with a `NULL`
  part, which no chunk's range reaches, may still be draining when it flips,
  but they gate every token, so the await covers them
  ([ADR-0002](decisions/0002-async-data-flow.md#what-live-promises)).
* **`quarantined`** — the whole-transform fuse tripped
  ([ADR-0003](decisions/0003-quarantine-storage-and-api.md)). Resuming drops the
  transform back to `waiting_to_backfill`, deletes the keys it holds in
  quarantine (its own only; another transform's held keys stay held) and
  re-derives every key from the source. The fuse starts again from zero, so the
  transform gets a fresh eviction budget rather than re-tripping on the next one,
  and a key whose cause is still there is quarantined again.
* **`paused`** — frozen, holding its last value, and not maintained
  ([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)). Either an operator ran
  `PAUSE TRANSFORM`, or Trellis paused it because it can't keep it correct:
  * capture of a table it reads broke (a column it reads was renamed or dropped, a
    primary key was redefined, row-level security or a logical-replication
    subscription came to apply to a table it reads) or its target came under
    row-level security: `DefinitionStatus::capture_failure` names the table, the
    columns and the cause;
  * a table it reads was changed outside Trellis, or is gone from its name
    ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#capture-changed-outside-trellis)):
    `capture_failure` reads "the table `<table>` no longer exists at that name
    (it was dropped, renamed away, moved to another schema, or replaced by
    something that isn't a table)…", or "Trellis's
    capture of `<table>` was changed outside Trellis: the capture trigger `<name>`
    is gone, was dropped and created again, is disabled, is enabled in ORIGIN
    or REPLICA mode, or was altered; the capture function `<name>` was
    replaced…" (on a partitioned source's node, "(on `<node>`)" follows each finding), and goes on to say that changes made meanwhile may be missing,
    so Trellis can't guarantee the definition is correct, and that a resume
    reinstalls capture and rebuilds it. The `capture_failure`'s `kind` is
    `disruption`;
  * an enum value of a type it reads was relabelled (`ALTER TYPE … RENAME
    VALUE`), so the label text Trellis staged and keyed by names a value that
    no longer exists
    ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#an-enum-value-relabelled)):
    `capture_failure` reads "enum value '`<old>`' of type `<schema>.<type>` was
    renamed to '`<new>`'; Trellis's staged changes and keys still name the old
    label; resume to rebuild the target from the source", kind `capture`. Every
    definition reading a column of the type pauses, and one already paused
    gets the reason before its own. `RESUME` waits, its `capture_failure`
    reading "resuming: waiting for the changes staged before an enum value …
    to drain", until none is pending, then rebuilds;
  * the database was restored from a logical dump (`pg_dump`, `pg_restore`):
    every definition pauses, whatever its kind and source, because the
    ordering state the dump brought belongs to the original cluster
    ([ADR-0002](decisions/0002-async-data-flow.md), [ADR-0017](decisions/0017-partitioned-sources.md)).
    `capture_failure` reads "the database was restored from a logical dump, so
    the ordering state Trellis kept for this definition belongs to the
    original database; resume the definition to reset it and rebuild the
    target from the restored source, or drop the definition", with kind
    `disruption` (a definition that was already paused keeps its reason, and
    its kind `capture`);
  * its build kept failing in a way no retry gets past: the error stays on
    `backfill_failure`;
  * the drain hit a failure that every key reproduces (a source with no usable
    primary key, a propagation cycle past the hop bound, a read or write refused
    to the drain's role): the definitions it reaches
    and everything downstream of them are paused, with the cause on `capture_failure`.

  `RESUME TRANSFORM` first re-runs define's validation against the live schema. While
  define would refuse the transform (a column it reads is gone or retyped into
  something it can't use), the
  resume fails with `ApplyError::ResumeRefused`, whose message is define's own
  error naming the column and what to change, and the transform stays paused,
  untouched. The resume also refuses in three cases define doesn't, where define
  would accept the transform and the message says to drop it and define it again:
  its target table is missing, its 1-1 source key was redefined, or its ledger
  table is missing
  ([operations — `PAUSE` and `RESUME`](operations.md#pause-and-resume)). A field resume
  (`RESUME TRANSFORM t.col`) runs the same validation, but not the ledger
  check, since it rebuilds nothing. Otherwise the resume returns it to
  `waiting_to_backfill`. If columns
  Trellis created for it no longer have the types define would give them now (after
  `integer` to `bigint`, say), the resume returns at once but the transform stays `paused`, its
  `capture_failure` reading "resuming: …", until the staging worker has re-typed them
  (an `ALTER … TYPE` that waits for the table's lock) and moved it to
  `waiting_to_backfill`. A re-type that fails puts its error on `capture_failure`, and
  the transform stays paused with its target's rows kept. When the values can't be
  converted, the repair is `DROP TRANSFORM` and define it again, which builds the
  target from empty. A re-type of a target pauses the transforms chained off it whose
  own columns were typed from the old type: their `capture_failure` names the upstream
  resume, and the staging worker resumes them once the upstream is `live` again. A
  `RESUME` of a transform that reads another transform's target (its source or a
  `JOIN`'s joined table) while that transform isn't `live`
  returns at once, but the transform stays `paused`, its `capture_failure` reading
  "resuming: waiting for transform `<upstream>` … to be live", until the upstream is
  `live` (an upstream `catching_up` only for a paused column it reads doesn't
  hold it); then the staging worker resumes it as above. A `PAUSE` of a transform waiting
  either way withdraws the resume, and its `capture_failure` says so. The
  resume reconciles the target with current source
  data rather than replaying what was skipped while paused (the change stream is
  drained for the transform's siblings meanwhile), so the cost of a resume scales with
  the data, not with the length of the pause.

Two further fields on `DefinitionStatus` explain a transform that is waiting on
capture rather than on a build. `capture_wait` says the staging worker couldn't yet
take the brief lock it needs to install or widen the capture triggers on a table the
transform reads, and names the table, the operation, the lock mode and the sessions
holding it (an autovacuum worker, say). The worker retries every reconcile pass, and it
clears once the lock holder lets go. `capture_failure` is set while capture is broken
and only fixing the cause gets the transform going again: a pause (above), or an
install that failed for a reason other than a lock, which is retried every pass. The
worker records both in the catalog, so every process's `status()` reports them, wherever
the worker runs.

`held_keys` on `DefinitionStatus` is set while the transform holds source keys in
quarantine, whatever its status: how many, and when the one held longest was poisoned.
A held key's target row stays as it was, and a key with changes parked holds back every
watermark token taken since, so `await_converged` and `self_check` don't converge past it
until it is released. A finished `self_check` job's report carries the same `held_keys`, so a
`live` transform can't hide one. With the automatic retry on, `held_keys` also carries
`max_retry_attempts`, the most retries any held key has had, and `next_retry_at`, the earliest
time one is due (reported by a connection with `held_key_retry` set; `self_check` reports
none), and each `sample_quarantined` row carries its `retry_attempts` and `next_retry_at`.
`Trellis::sample_quarantined` lists the keys and their errors; fix the cause and release each with `Trellis::release_key`, or resume the
transform ([ADR-0003](decisions/0003-quarantine-storage-and-api.md#releasing-held-keys)).

`drain_failure` on `DefinitionStatus` is set while the drain keeps failing on a page
holding changes to a table the transform reads, and the failure is charged to no key and pauses nothing (#817): a
refused read or write the catalog can't pin on a table
([A halting failure](#a-halting-failure)), records that fail only together, a record
that fails only in two separate combinations of transforms
([operations](operations.md#a-page-that-fails-only-for-several-definitions-together)),
isolation stopping at its probe limit, or a failure that kept reproducing until the
page's retries ran out. The drain records such a page as a *drain holdup*, and every
pass retries it, so the transform's target stops short of it and every watermark token
taken since waits on it. The field carries the segment, the source tables on the page,
the latest error and its SQLSTATE, when the drain first and last failed on it, and how
many passes have. Every transform that isn't paused or quarantined and reads one of
those tables reports it, the oldest holdup first. It clears in the transaction that
commits the page; fix the cause the error names and the next pass does. A transient
failure (a lock timeout, a serialization failure, a dropped connection) is never recorded,
since retrying is how it resolves. `self_check` reports every open holdup with every
audit, as `drain_failures`, whichever transform it audits: it is an instance-level
finding, and the CLI's `trellis status` prints it under each transform it holds back.
The holdup is separate from `capture_failure`: it pauses and charges nothing
([ADR-0003](decisions/0003-quarantine-storage-and-api.md#retry-policy)).

`source_index_gaps` on `DefinitionStatus` lists the source indexes a `GROUP BY` or `JOIN`
transform needs and lacks (#1167): one entry per table and leading columns, with
a `role` (`GROUP BY` columns, a `JOIN`'s `ON` columns on its from side, or on its
joined side: `IndexRole`, the word `group_by`, `join_from` or `join_joined` in the
bindings) that names the part of the definition that needs the index, with
`opted_out` and a `create index` statement as the fix (`SourceIndexGap::fix`), which
Trellis doesn't run. Define and a `RESUME` refuse a transform without them, naming the same role, so an
entry is an index dropped or made invalid since, or one a transform defined `WITHOUT
SOURCE INDEX` runs without. It is a warning: nothing is paused and the target is
correct, but `self_check` refuses to walk a transform whose entry is not opted out
(an opted-out one walks, scanning the source once per page). It is empty for every
other target and when the indexes exist, is read from the catalog on each call, and
is one field for every target kind. The CLI's `trellis status` prints one warning line per
entry. Which indexes a target needs is in
[Supported sources and targets](transforms.md#supported-sources-and-targets).

`join` on `DefinitionStatus` is set for a `JOIN` transform and `None` for any
other. It reports three things, none of which changes the transform's status:

- `hidden_tables`: the estimated rows (`pg_class.reltuples`, 0 before the first
  analyze) and total on-disk size in bytes of `P`, `M`, `J` and `T`, the
  transform's hidden tables ([ADR-0019](decisions/0019-join-transforms.md)).
  Estimates keep the call cheap on large tables; they are as fresh as the table's
  last `ANALYZE` or autovacuum.
- `partials_warning`: set once `P`'s estimated rows reach half of the `FROM` table's
  (and the `FROM` table has at least 1000 estimated rows). `P` is the `FROM` table
  grouped by its `GROUP BY` columns and join key, so a `P` near the table's size
  means the grouping barely compresses it and the hidden table costs about as much
  as the table it summarises. A `FROM` table joined on a unique key (its primary key,
  say) has one `P` cell per row, so such a `JOIN` warns once the table passes 1000
  rows.
- `building`: the step the transform's build (a fresh build, a `RESUME` or a
  `request_backfill` rebuild) runs now, or runs next while no worker holds its job;
  `None` when no build is under way or the transform is paused or quarantined. A
  `JOIN` whose sides are both unpartitioned, or partitioned with no rows, builds in
  one transaction (`Whole`). One with a partitioned side that has rows builds in
  steps: each unpartitioned side whole, then each partitioned side (`FROM` first) one
  leaf at a time in oid order (`Leaf`, named `schema.table`) and then its `Remainder`
  (the cells that route to no recorded leaf), then a `Final` step that writes `T` and
  the target and completes the build. A side whose partition tree isn't recorded, or
  has a leaf without a usable constraint text, is `Unroutable`: the build fails there
  and `backfill_failure` says why. The report comes from the build job's cursor, which
  each step writes in its own transaction, through a plain read that takes no row
  lock, so a status call neither waits on a running step nor holds one up; it names
  the step after the last one that committed.

The `J` lock wait is a metric rather than a status field: the time a `JOIN` page
spends in its sorted `select … for update` of its `J` rows is the histogram
`trellis_join_lock_wait_seconds`, labelled by `transform`. It is mostly the wait for
another page or writer holding one of its join keys, so a rising tail shows
contention on hot keys.
`trellis status` prints a `join tables` line, a `join build` line while a build is
under way, and one warning line per finding.

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
`pg_stat_activity`) until every such transaction has ended
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
error, or a missing privilege, #766). Retrying the page would fail it forever,
and quarantining a key would blame one for nobody's fault. Instead the drain
**halts** the definitions the failure reaches (#663) and drains the page
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
  definition ([embedding — health checks](embedding.md#halted-definitions)), and
  the CLI's `trellis status` prints it on an indented line under each one.
* **One episode, one signal.** A halt logs one error line naming every
  definition it paused, and increments the single-row `halting_stops` table:
  `stop_count`, plus `last_reason` and `last_stopped_at` for the latest. A
  peer worker meeting the same failure, or a retry meeting it again, finds the
  closure already paused and records nothing, so the count is of episodes,
  not attempts. A halt that pauses nothing because a peer already paused the
  closure retries the page once. One that still pauses nothing (a refused
  read or write the catalog can't pin on a table,
  [row-level security](transforms.md#supported-sources-and-targets))
  surfaces the error and records the page as a drain holdup, which `status`
  reports as `drain_failure` on the definitions reading its tables. The drain
  worker re-claims the page at its poll interval under a collapsed warning
  (#660).
* **A failing build start.** A Re-derive build's start that fails in a way
  that would repeat on every pass (its ledger table was dropped, say) halts the
  definition writing that target, and everything downstream of the target, the
  same way (#986), with `kind` `halt`, the target as the table and the error.
  It never stops the staging worker's pass: the other definitions' builds
  start. A start that fails transiently, or
  by losing its connection, is retried on the next pass instead.
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
* [#14](https://github.com/salesforce-misc/trellis/issues/14) — the motivating stall.
</content>
</invoke>
