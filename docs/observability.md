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
  `quarantined` and `paused`), so an
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

**Non-goals (this pass)**

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

The clock is already flowing in: the **source commit timestamp** rides on the
replication `Commit` event (`commit_time_micros`,
`trellis/src/intake/mod.rs`). Two families of measurement follow:

* **Per-transform latency** — from a change arriving at a transform's input to
  its output being applied. One histogram per transform.
* **End-to-end latency** — from the *source* commit to the *final* transform's
  apply, keyed by the **terminal transform** (not source→sink pair) to keep
  cardinality low.

Supporting counters/gauges keep the histograms interpretable:

* `changes_applied_total{transform}` — staged changes each transform folded
  and applied. One change is one row in the staging ring: a source row change
  that intake staged from logical replication, or a row an upstream transform's
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
* `intake_restarts_total{outcome}` — times CDC intake stopped (`error`, or
  `stream_ended`) and the client restarted it with capped exponential backoff.
  Every stop is also logged at `error!` with the cause. A sustained
  non-zero rate means source changes aren't being staged. The third outcome,
  `producer_lock_held`, means another producer session holds the staging
  producer lock: this client is standing by and retrying so it can take over,
  and it logs that at `info!` once, then `debug!`, instead of as an error.
* `intake_consecutive_failures{slot}` — intake failures in a row since intake
  last stayed up for 60s. It reads `0` while intake is healthy and climbs while
  it's stuck restarting. Alert on this (say, `>= 3`) rather than on the
  lifetime counter, which can't tell an occasional blip from a stuck loop.
  `producer_lock_held` restarts count toward it, because the lock's holder
  can be this client's own previous session that Postgres hasn't yet noticed
  is dead (after a network partition that can last until the server's TCP
  keepalive gives up, which is hours with OS defaults), and then nothing is
  staging. If you deliberately run a second `staging_worker` client as a
  standby, its value climbs while the active one reads `0`, so alert on
  `min by (slot)` across processes.

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

**Implemented (issue #53).** [`Trellis::metrics`](../trellis/src/app.rs) — and
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
apply, not instrumented independently. Gates issue #56's design.

## Transform status lifecycle

Rather than instrument the backfill with bespoke metrics, every transform carries
an observable **status**. This is the same lever quarantine uses
([ADR-0003](decisions/0003-quarantine-storage-and-api.md) marks a fused transform
`quarantined` and resumes it by re-running the *same* backfill), so backfill and
quarantine are two arcs of one lifecycle:

```
(new transform)──► waiting_to_backfill ──► backfilling ──► catching_up ◄──► live
                          ▲                                                    │
                          │ (resume re-runs backfill)                          │ (fuse trips)
                          └───────────────────── quarantined ◄─────────────────┘
```

* **`waiting_to_backfill`** — defined, but its source's existing rows haven't
  been read yet. Every new transform starts here, and registration returns
  with it here. It stays until the staging worker has joined its source
  (published it if needed and parked a `pending_backfill` marker), the
  marker's transaction fence has settled (see caveat below), and intake has
  caught up to the read's snapshot
  ([data-flow — Capturing a table's existing rows](data-flow.md#capturing-a-tables-existing-rows)).
  A direct build that failed comes back here too, retried after a backoff;
  `Trellis::status` reports its error meanwhile (`backfill_failure`).
* **`backfilling`** — the backfill discharge has captured the source and
  enqueued the transform's build, which is running: chunks, or one direct
  set-based build job, on drain threads. The target is partial. A ring enumeration
  (the fallback for a shape neither build can render) never shows this: it
  goes from `waiting_to_backfill` straight to `live` in the discharge's own
  transaction (or to `catching_up`, when its source is another transform's
  target).
* **`catching_up`** — the build has finished and apply maintains the
  transform exactly as a `live` one, but a go-live catch-up (a re-read of a
  table it reads, parked as a `pending_backfill` marker) hasn't been
  discharged yet, so the target may be missing changes that drained while it
  was building. The staging worker discharges a fresh marker on its next
  maintenance tick, and that discharge flips the transform `live`. A `live`
  transform comes back here for its own catch-up: an `ALTER TRANSFORM` that
  added columns, a resumed column, or a stale backfill chunk given up after
  its rebuild. A catch-up that keeps failing keeps the transform here, with
  the error on `Trellis::status` when the failing marker is on its source.
* **`live`** — the steady state: a watermark token taken after a commit and
  awaited with `Trellis::await_converged` guarantees the target reflects that
  commit. A ring enumeration's rows may still be draining when it flips, but
  they gate every token, so the await covers them
  ([ADR-0016](decisions/0016-single-background-capture-path.md#what-live-promises)).
* **`quarantined`** — the fuse tripped
  ([ADR-0003](decisions/0003-quarantine-storage-and-api.md)). Resuming drops the
  transform back to `waiting_to_backfill`, re-runs the backfill, and **re-arms**
  the fuse: already-evicted keys stay evicted (and releasable one at a time, with
  their parked changes intact) but no longer count against the resumed transform,
  so it gets a fresh eviction budget rather than re-tripping on the next one.

### Backfill status and the `xmin` caveat

Every backfill (a new transform's, a resumed one's, or a catch-up) reads its
source only once a conservative transaction fence settles (`now.xmin >
fence`, `trellis/src/intake/publication.rs`). Because `xmin` is
**cluster-global**, any unrelated long-running transaction *anywhere in the
cluster* pins it and holds every waiting backfill in `waiting_to_backfill`
until that transaction ends. Since every new transform goes through this wait
([ADR-0016](decisions/0016-single-background-capture-path.md)), a long
transaction delays every registration from going live, not only the ones on a
newly published table.

This wait is **safe, not a fault**: the apply loop and every `live` transform are
unaffected, and even the new table's *new* changes stream through — only its
*historical* rows are withheld until the fence settles. So we deliberately emit no
stall metric, warning log, or timeout — the `waiting_to_backfill` status is the
whole signal.

The remedy is documentation, not a signal: an operator clears the wait by ending
the pinning transaction — idle-in-transaction connections, long analytics
queries, `pg_dump`, or workload on another database sharing the cluster.

### A backfill that keeps failing

A backfill can also fail outright, for example when a plain 1-1 transform's
source has lost its primary key. That is a fault, so unlike the fence wait it
is surfaced (issue #407,
[ADR-0016](decisions/0016-single-background-capture-path.md#consequences)):

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
  an attempt goes through.

## Dependencies

Approved and pinned in `trellis/Cargo.toml` (issues #51/#56); rationale in
[ADR-0009](decisions/0009-observability-decisions.md#1-dependencies-metrics-facade-not-prometheus-directly):

* `metrics` + `metrics-exporter-prometheus` for the registry and text rendering.
  The exporter's Hyper-listener feature is **not** enabled — this stays a pure
  registry + text-encoder.
* `tracing` (unconditional) for spans/events; `tracing-opentelemetry` +
  `opentelemetry-otlp` for OTLP export, behind the `otlp` Cargo feature (off by
  default, genuinely absent from the dependency tree when unused).

## Related

* [data-flow](data-flow.md) — the flow these metrics measure.
* [open-questions](open-questions.md#backfill-status-and-observability) — the
  backfill-status/lag-telemetry question this doc subsumes.
* [#14](https://github.com/salesforce-misc/trellis/issues/14) — the motivating stall.
</content>
</invoke>
