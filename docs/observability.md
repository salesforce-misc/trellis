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
  (`waiting_to_backfill` → `backfilling` → `catching_up` → `live`, plus
  `quarantined` and `paused`, which a halting failure also uses), so an
  operator can tell a new transform is still populating rather than live — the
  right-sized answer to the silent-stall problem (#14).
* **Fleet-level worker liveness** — `Trellis::has_live_drain_workers`
  (issue #144) and `Trellis::has_live_staging_worker` (issue #428) answer the
  coarser, fleet-wide questions a per-transform status can't: is *any* drain
  worker running to build and maintain targets, and is the staging worker
  running to capture changes and move a new transform out of
  `waiting_to_backfill`. A healthy fleet needs both. See
  [docs/embedding.md](embedding.md#the-silent-stall-hazard-issue-144) for the
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

Supporting counters/gauges keep the histograms interpretable:

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
* `staging_segments{state}` — a cheap system-level gauge counting segments by
  state (ties to [the staging ring](staging-and-claiming/02-the-staging-ring.md)),
  chosen over a per-transform depth gauge for lower cost.

Backfill progress is deliberately *not* a metric — it's the transform's
[lifecycle status](#transform-status-lifecycle), a small enumerable state.

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
registry. `cli/src/commands/run.rs`'s `--prometheus-bind <ADDR>` flag is a
minimal template: a small TCP listener alongside the live pipeline answering each
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
(new transform)──► waiting_to_backfill ──► backfilling ──► catching_up ◄──► live
                          ▲                                                   │
                          │ RESUME (re-runs the backfill)                     │ PAUSE, auto-pause,
                          └───────────── paused / quarantined ◄───────────────┘ or the fuse trips
```

* **`waiting_to_backfill`** — defined, but its source's existing rows haven't
  been read yet. Every new transform starts here, and registration returns
  with it here. It stays until the staging worker has joined its source
  (installed its capture triggers if needed and parked a `pending_backfill`
  marker) and the marker's transaction fence has settled (see caveat below)
  ([data-flow — Capturing a table's existing rows](data-flow.md#capturing-a-tables-existing-rows)).
  A direct build that failed comes back here too, retried after a backoff;
  `Trellis::status` reports its error meanwhile (`backfill_failure`).
* **`backfilling`** — the backfill discharge has captured the source and
  enqueued the transform's build, which is running: chunks, or one direct
  set-based build job, on drain threads. The target is partial.
  A plain aggregate (grouped by plain source columns, on a captured source) and
  a plain 1-1 transform (no relationship, on a captured source) are built by the
  Re-derive build instead
  ([data-flow — The Re-derive build](data-flow.md#the-re-derive-build)).
  The staging worker starts it with no marker, and it is maintained from the start, so it goes
  from `backfilling` straight to `live` once its last chunk (and, for an aggregate,
  its last group merge) has committed, with no `catching_up`. Its resume rebuilds it
  the same way, over the ledger the freeze left. A ring enumeration (the fallback
  for a shape neither build can render) never shows this status: it goes from
  `waiting_to_backfill` straight to `live` in the discharge's own transaction (or
  to `catching_up`, when its source is another transform's target).
* **`catching_up`** — the build has finished and apply maintains the
  transform exactly as a `live` one, but a go-live catch-up (a re-read of a
  table it reads, parked as a `pending_backfill` marker) hasn't been
  discharged yet, so the target may be missing changes that drained while it
  was building. The staging worker discharges a fresh marker on its next
  maintenance tick, and that discharge flips the transform `live`.
  A catch-up that keeps failing keeps the transform here, with the error on
  `Trellis::status` when the failing marker is on its source.
  A `live` transform comes back here for its own catch-up after:
  * an `ALTER TRANSFORM` that added columns;
  * a resumed column;
  * a rebuild of a transform whose target it reads;
  * a re-read of a table it reads (`Trellis::request_backfill`, or the table's
    capture triggers put back after someone dropped them).

  A `live` transform that reads an upstream which isn't `live` reports
  `catching_up` too.
* **`live`** — the steady state: a watermark token taken after a commit and
  awaited with `Trellis::await_converged` guarantees the target reflects that
  commit. A ring enumeration's rows may still be draining when it flips, but
  they gate every token, so the await covers them
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
  * its build kept failing in a way no retry gets past: the error stays on
    `backfill_failure`;
  * the drain hit a failure that every key reproduces (a source with no usable
    primary key, a propagation cycle past the hop bound, a read or write refused
    to the drain's role), or a backfill discharge or go-live catch-up was refused
    a read: the definitions it reaches
    and everything downstream of them are paused, with the cause on `capture_failure`.

  `RESUME TRANSFORM` first re-runs define's validation against the live schema. While
  define would refuse the transform (a column it reads is gone or retyped into
  something it can't use, a relationship's join columns no longer match, its 1-1
  source key was redefined), the resume fails with `ApplyError::ResumeRefused`,
  whose message is define's own error naming the column and what to change, and the
  transform stays paused, untouched. A field resume (`RESUME TRANSFORM t.col`) runs the
  same check. Otherwise the resume returns it to `waiting_to_backfill`. If Trellis's
  typed copies of its columns no longer have their sources' types (after `integer` to
  `bigint`, say), the resume returns at once but the transform stays `paused`, its
  `capture_failure` reading "resuming: …", until the staging worker has re-typed them
  (an `ALTER … TYPE` that waits for the table's lock) and moved it to
  `waiting_to_backfill`. A re-type that fails puts its error on `capture_failure`, and
  the transform stays paused. The resume reconciles the target with current source
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
until it is released. `self_check` reports the same `held_keys` with every audit, so a
`live` transform can't hide one. `Trellis::sample_quarantined` lists the keys and their
errors; fix the cause and release each with `Trellis::release_key`, or resume the
transform ([ADR-0003](decisions/0003-quarantine-storage-and-api.md#releasing-held-keys)).

### Backfill status and the `xmin` caveat

Every backfill (a new transform's, a resumed one's, or a catch-up) reads its
source only once a conservative transaction fence settles (the backfill discharge
checks that the snapshot's `xmin` is past the fence), except the Re-derive build of a
plain aggregate or a plain 1-1 transform: it starts with no fence, and each
of its chunks reads under its own short snapshot after locking the ledger
entries it rewrites, so a long transaction elsewhere doesn't hold it, and it holds no
long snapshot of its own
([data-flow — The Re-derive build](data-flow.md#the-re-derive-build)). Because `xmin` is
**cluster-global**, any unrelated long-running transaction *anywhere in the
cluster* pins it and holds every waiting backfill in `waiting_to_backfill`
until that transaction ends. Since every new transform goes through this wait
([ADR-0002](decisions/0002-async-data-flow.md#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)), a long
transaction delays every registration from going live, not only the ones on a
newly captured table.

This wait is **safe, not a fault**: the apply loop and every `live` transform are
unaffected, and even the new table's *new* changes are captured — only its
*historical* rows are withheld until the fence settles. So we deliberately emit no
stall metric, warning log, or timeout — the `waiting_to_backfill` status is the
whole signal.

The remedy is documentation, not a signal: an operator clears the wait by ending
the pinning transaction — idle-in-transaction connections, long analytics
queries, `pg_dump`, or workload on another database sharing the cluster.

### A backfill that keeps failing

A backfill can also fail outright, for example when a plain 1-1 transform's
source has lost its primary key. That is a fault, so unlike the fence wait it
is surfaced:

* **It doesn't hold up other tables.** The staging worker logs the failure as a
  warning and moves on to the next table's backfill in the same pass.
* **It is retried with backoff.** The next attempt waits 10 seconds, doubling
  after each further failure up to 5 minutes. It keeps retrying at that pace
  forever; nothing is quarantined automatically. Once you fix the cause (or drop
  the transform), the next attempt goes through. A new backfill request for the
  same table (`Trellis::request_backfill`, a resume, or any other catch-up)
  resets the backoff and runs at once.
* **`Trellis::status` reports it.** The transform stays `waiting_to_backfill`,
  and `DefinitionStatus::backfill_failure` carries the source table, the
  attempt count, the last error and the next attempt time. The source table's
  backfill also runs the catch-up of its `live` readers, so every transform
  reading that table reports the failure, whatever its status. It clears once
  an attempt goes through. `Trellis::definitions` reports the same value on
  every `DefinitionSummary`, and the CLI's `trellis status` prints it on an
  indented line under each affected definition, so one listing shows every
  stuck backfill.
* **A refusal pauses instead.** When Postgres refuses the staging worker's
  role a read the backfill needs (row-level security or a missing privilege,
  `42501`), retrying can't get past it, so the transforms the refusal reaches
  are paused as a halt ([A halting failure](#a-halting-failure)) and the
  backfill goes on without them. Only a refusal the catalog can't pin on a
  table is retried with backoff as above.

A build that has started fails differently. A plain 1-1 transform's build runs as
chunks of its source's primary-key range on the drain workers, and each failing chunk is
logged as a warning with the definition, the chunk, the attempt and what happens next.
A transient failure (a lost connection, a lock conflict) is retried with backoff and
counts toward nothing. A failure on a row's data (SQLSTATE class `22` or `23`) is split
down to the row's key, which is quarantined as the drain quarantines a key
([ADR-0003](decisions/0003-quarantine-storage-and-api.md)); the build finishes without
it, `held_keys` counts it, and `Trellis::sample_quarantined` lists it. The key is held for this transform only:
every other transform reading the table keeps applying it, and the whole-transform fuse
counts it against this transform alone. Any other failure is retried, and its
fifth charged attempt pauses the transform (an aggregate's or relationship-enriched
transform's build, which can't be narrowed to a key, is paused the same way). Fix the
cause and resume the transform, which rebuilds it. Meanwhile
`DefinitionStatus::backfill_failure` carries the failing chunk's error, attempt count
and next attempt time, ahead of any failure of the source table's marker; a transform
paused this way keeps the error there until it is resumed.

### A halting failure

Some failures are no row's fault, so every key of the page reproduces them:
a source key the drain can't use (the table lost its primary key, or the key
changed to a type Trellis can't key by), a propagation wave past the hop bound,
an aggregate target off the ledger, or Postgres refusing the drain's role a read
or write (row-level security, which Trellis's `row_security = off` turns into an
error, or a missing privilege, #766). Retrying the page would fail it forever,
and quarantining a key would blame one for nobody's fault. Instead the drain
**halts** the definitions the failure reaches (#663) and drains the page
without them:

* **What it pauses.** For a key error, every definition that reads the table,
  as its source or as a relationship's to-side. For a hop bound outside a
  cycle, the readers of the table the wave ran away through, not the
  definition that wrote it; inside a cycle, every member. For an aggregate off
  the ledger, the definition that writes that target. For a refused read or
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
  [gap 11](known-correctness-gaps.md#11-row-level-security-on-a-role-trellis-runs-as))
  surfaces the error, and the drain worker re-claims the page at its poll
  interval under a collapsed warning (#660).
* **A refused backfill.** A backfill discharge or go-live catch-up that
  Postgres refuses (`42501`, while it plans a build or re-reads the source)
  halts the same way, as the staging worker's role (#813): it pauses what the
  refusal reaches with `kind` `halt`, and the marker discharges without them,
  so the definitions show as halted rather than stuck in
  `waiting_to_backfill` or `catching_up`. A refusal the catalog can't pin on a
  table pauses nothing: the marker backs off and retries like any failed
  backfill, with the error on `backfill_failure`.
* **Resuming.** Fix the cause, then `RESUME TRANSFORM` each halted definition,
  in any order; each resume rebuilds that definition as for any pause, and
  clears its halt. A resume re-validates the definition as define would, so
  while its source key, or a relationship endpoint's, is still of a type define
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
