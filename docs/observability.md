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
  (`waiting_to_backfill` → `backfilling` → `live`, plus `quarantined`), so an
  operator can tell a new transform is still populating rather than live — the
  right-sized answer to the silent-stall problem (#14).
* **Fleet-level drain-worker liveness** — `Trellis::has_live_drain_workers`
  (issue #144) answers the coarser, fleet-wide question a per-transform status
  can't: is *anything* running that would ever move a transform out of
  `waiting_to_backfill` in the first place. See
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

* `changes_applied_total{transform}` — throughput denominator.
* `staging_segments{state}` — a cheap system-level gauge counting segments by
  state (ties to [the staging ring](staging-and-claiming/02-the-staging-ring.md)),
  chosen over a per-transform depth gauge for lower cost.
* `intake_restarts_total{outcome}` — times CDC intake stopped (`error`, or
  `stream_ended`) and the client restarted it with capped exponential backoff.
  Every stop is also logged (`error!`/`warn!`) with the cause. A sustained
  non-zero rate means source changes aren't being staged, so alert on it.

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
(new transform)──► waiting_to_backfill ──► backfilling ──► live
                          ▲                                  │
                          │ (resume re-runs backfill)        │ (fuse trips)
                          └──────────── quarantined ◄─────────┘
```

* **`waiting_to_backfill`** — defined and its backfill marker is durable, but
  pre-existing rows aren't enumerated yet. Where a transform sits while its
  transaction fence is unsettled (see caveat below).
* **`backfilling`** — pre-existing source rows are being enumerated and staged.
* **`live`** — backfill complete; tracking live changes only. The steady state.
* **`quarantined`** — the fuse tripped
  ([ADR-0003](decisions/0003-quarantine-storage-and-api.md)). Resuming drops the
  transform back to `waiting_to_backfill`, re-runs the backfill, and **re-arms**
  the fuse: already-evicted keys stay evicted (and releasable one at a time, with
  their parked changes intact) but no longer count against the resumed transform,
  so it gets a fresh eviction budget rather than re-tripping on the next one.

### Backfill status and the `xmin` caveat

Adding a source table triggers a backfill of its pre-existing rows, gated on a
conservative transaction-fence settlement (`now.xmin > fence.xmax`,
`trellis/src/intake/publication.rs`). Because `xmin` is **cluster-global**, any
unrelated long-running transaction *anywhere in the cluster* pins it and holds
every waiting backfill in `waiting_to_backfill` until that transaction ends.

This wait is **safe, not a fault**: the apply loop and every `live` transform are
unaffected, and even the new table's *new* changes stream through — only its
*historical* rows are withheld until the fence settles. So we deliberately emit no
stall metric, warning log, or timeout — the `waiting_to_backfill` status is the
whole signal.

The remedy is documentation, not a signal: an operator clears the wait by ending
the pinning transaction — idle-in-transaction connections, long analytics
queries, `pg_dump`, or workload on another database sharing the cluster.

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
