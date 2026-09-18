# Observability

How an operator running Trellis sees what the pipeline is doing: how fast
changes are propagating, where they're stuck, and what work is pending or
blocked. This is the counterpart to [data-flow](data-flow.md) — that document
describes the flow; this one describes how we *measure* it.

The decisions behind this design, and the alternatives weighed, live in
[ADR-0009](decisions/0009-observability-decisions.md).

## Goals and non-goals

**Goals**

* **Latency visibility** — median and p99 for each individual transform, plus
  median and p99 for the end-to-end path (a source commit propagating through
  the whole chain of transforms to its final apply).
* **Pull-based export** — those metrics exposed in Prometheus text format so
  operators can scrape them into whatever they already run for dashboards and
  alerting.
* **Structured logs** — emitted through a facade that can export in
  OpenTelemetry form.
* **Transform status** — every transform carries an observable lifecycle status
  (`waiting_to_backfill` → `backfilling` → `live`, plus `quarantined`), so an
  operator can see a newly-defined transform is still populating rather than
  live — the right-sized answer to the silent-stall problem (#14; see the
  [`xmin` caveat below](#backfill-status-and-the-xmin-caveat)).

**Non-goals (for this pass)**

* Being a metrics *backend*. Trellis only exposes an in-process registry for
  scraping; it does not retain history of its own or replace Prometheus/
  Grafana/etc. (see [Retention](#retention-left-to-prometheus-not-trellis)).
* Distributed tracing across the *application's* code. We instrument Trellis's
  own pipeline, not the caller's write path.

## The two subsystems

Metrics and logs are deliberately **separate subsystems** rather than one
unified telemetry pipeline. Each is idiomatic on its own and they evolve
independently:

```
metrics ──► in-process registry ──► render_prometheus()  (operator scrapes)

logs/spans ──► `tracing` facade ──► optional OTLP export layer
```

## Metrics

### What "latency" means

Trellis has a natural clock already flowing in: the **source commit timestamp**
rides in on the replication `Commit` event (`commit_time_micros`, see
`trellis/src/intake/mod.rs`). Every propagated change can be timestamped at each
stage relative to that origin. Two families of measurement follow:

* **Per-transform latency** — time from a change becoming available at a
  transform's input to its output being applied. One histogram per transform.
* **End-to-end latency** — time from the *source* commit to the *final*
  transform's apply. For a DAG this is the whole-chain rollup, keyed by the
  **terminal transform** — settled in
  [ADR-0009](decisions/0009-observability-decisions.md#2-end-to-end-latency-keying-terminal-transform-only),
  not by source→sink pair, to keep cardinality low.

Recommended supporting counters/gauges so the histograms are interpretable:

* `changes_applied_total{transform}` — throughput denominator.
* `staging_segments{state}` — a cheap, system-level gauge counting segments
  by state (ties to [the staging ring](staging-and-claiming/02-the-staging-ring.md)),
  chosen over a per-transform depth gauge for its lower cost
  ([ADR-0009 decision 5](decisions/0009-observability-decisions.md#5-staging-ring-metrics-drop-the-per-transform-depth-gauge-add-a-cheap-segment-state-gauge)).

Backfill progress is deliberately *not* a metric — it's the transform's
[lifecycle status](#backfill-status-and-the-xmin-caveat), a small enumerable
state rather than a counter.

### Quantiles via histograms, not summaries

**Recommendation: histograms.** Median and p99 are computed at query time from
exported bucket counts (`histogram_quantile` in PromQL), rather than
client-computed summary quantiles. Histograms **aggregate across instances**;
summaries do not. Since a Trellis deployment may run more than one engine
process against the same cluster, aggregatability matters. The cost is choosing
bucket boundaries up front — settled in
[ADR-0009](decisions/0009-observability-decisions.md#6-histogram-bucket-boundaries)
as a single global exponential set spanning ~10ms-60s (the expected
sub-second to tens-of-seconds propagation range), not configurable per
transform in this pass.

### Exposition: a mountable handler, not a bound port

The library stays HTTP-agnostic. It exposes a render function over its registry;
the operator serves the result from their own HTTP stack:

```rust
let body = trellis.metrics().render_prometheus(); // Prometheus text format
// operator serves `body` from their own axum/actix/hyper /metrics route
```

No port binding, no HTTP framework, no bind-address config; wiring costs the
operator a few lines.

**Implemented (issue #53).** [`Trellis::metrics`](../trellis/src/app.rs) (and
[`BlockingTrellis::metrics`](../trellis/src/blocking.rs), for callers without
a `tokio` runtime of their own) returns a
[`trellis::metrics::Metrics`](../trellis/src/metrics.rs) handle whose
[`render_prometheus`](../trellis/src/metrics.rs) method is exactly the
`String`-returning call sketched above — no HTTP framework or bound socket
inside the `trellis` crate itself, per ADR-0009 decision 1. A minimal
end-to-end example, an axum-style `/metrics` handler mounted alongside a
running engine:

```rust,no_run
# async fn example(trellis: std::sync::Arc<trellis::Trellis>) -> String {
// A route handler in the operator's own HTTP stack, closing over the
// running `Trellis` (or just calling `trellis::metrics::Metrics::new()`
// directly — the registry is process-wide, not scoped to one `Trellis`
// connection, so any in-process handle reaches the same data).
trellis.metrics().render_prometheus()
# }
```

`cli/src/commands/run.rs`'s `--prometheus-bind <ADDR>` flag is a complete
(if deliberately minimal — no HTTP-parsing crate) example: when given,
`trellis run` hand-rolls a small TCP listener alongside the live pipeline and
answers every request with `Metrics::new().render_prometheus()` as a
`200 OK`, `Content-Type: text/plain; version=0.0.4; charset=utf-8` response —
worth reading as a template for wiring this into a real HTTP stack, since it
renders the same process's registry that's actually running the engine
(unlike a separate scrape-only process, whose registry would always be
empty). An embedder building their own binary mounts `render_prometheus()`
from inside whatever process is actually running
[`Trellis`]/[`Client`] (`staging`/`drain_threads` set), the same way.

### Retention: left to Prometheus, not Trellis

Trellis does **not** retain metric history of its own. Retention, rollup, and
cross-instance aggregation are exactly what a real Prometheus/VictoriaMetrics/
Thanos deployment already does well; duplicating that inside Trellis would add
write load and a schema object for no capability an operator's existing scrape/
TSDB stack doesn't already provide
([ADR-0009 decision 7](decisions/0009-observability-decisions.md#7-rollup-interval-and-retention-issue-54)).
Operators who want history configure their scraper's own retention against the
`render_prometheus()` endpoint (or `trellis run --prometheus-bind`) like any
other Prometheus target.

## Logs and traces

Logs go through the **`tracing`** facade with an optional **OTLP export layer**,
so operators who run an OpenTelemetry collector get compliant output and those
who don't still get structured local logs.

A change flowing source → hop → hop → apply *is* a trace. Settled in
[ADR-0009](decisions/0009-observability-decisions.md#3-traces-vs-flat-logs-spans-are-first-class):
we adopt **spans** as a first-class signal, modeling propagation as a
`tracing` span tree. This makes the pipeline's shape observable and carries
the per-hop latency data for free — the per-transform latency histogram is
*derived from* span durations captured during apply (downstream of fold,
where changes are already grouped by the transform(s) that consume them),
rather than instrumented independently. This gates issue #56's design
(span-based instrumentation of the propagation path).

## Transform status lifecycle

Rather than instrument the backfill with bespoke metrics and log events, every
transform carries an observable **status**. This is the same lever quarantine
already uses — [ADR-0003](decisions/0003-quarantine-storage-and-api.md) marks a
fused transform `quarantined` and resumes it by re-running the *same* backfill —
so backfill and quarantine are two arcs of one lifecycle:

```
(new transform)──► waiting_to_backfill ──► backfilling ──► live
                          ▲                                  │
                          │ (resume re-runs backfill)        │ (fuse trips)
                          └──────────── quarantined ◄─────────┘
```

* **`waiting_to_backfill`** — the transform is defined and its backfill marker
  is durable, but the pre-existing rows haven't been enumerated yet. This is
  where a transform sits while its transaction fence is unsettled (see the
  caveat below).
* **`backfilling`** — the pre-existing source rows are being enumerated and
  staged.
* **`live`** — backfill is complete; the transform is tracking live changes
  only. This is the steady state.
* **`quarantined`** — the fuse has tripped
  ([ADR-0003](decisions/0003-quarantine-storage-and-api.md)); resuming drops the
  transform back to `waiting_to_backfill` and re-runs the backfill. Resuming
  also **re-arms** the fuse: the already-evicted keys stay evicted (and stay
  releasable, one at a time, with their parked changes intact), but they no
  longer count against the resumed transform, so it gets a full fresh budget
  of new evictions before the fuse can trip again rather than re-tripping on
  the very next one.

### Backfill status and the `xmin` caveat

Adding a source table triggers a backfill of its pre-existing rows, gated on a
conservative transaction-fence settlement (`now.xmin > fence.xmax`,
`trellis/src/intake/publication.rs`). Because `xmin` is **cluster-global**, any
unrelated long-running transaction *anywhere in the cluster* pins it and holds
every waiting backfill in `waiting_to_backfill` until that transaction commits or
aborts.

This wait is **safe, not a fault**: the streaming apply loop and every already-
`live` transform are unaffected, and even the new table's *new* changes stream
through — only its *historical* rows are withheld until the fence settles. So we
deliberately do **not** emit a stall metric, a periodic warning log, or a
fail-loud timeout for it. The `waiting_to_backfill` status is the whole signal.

The remedy is documentation, not a signal: a transform stays in
`waiting_to_backfill` as long as a long-lived transaction pins the cluster's
`xmin`, and an operator clears it by clearing that transaction —
idle-in-transaction connections, long analytics queries, `pg_dump`, or workload
on another database sharing the cluster.

## Dependencies

Approved and pinned in `trellis/Cargo.toml` (issues #51/#56) — see
[ADR-0009](decisions/0009-observability-decisions.md#1-dependencies-metrics-facade-not-prometheus-directly)
for the full rationale, including why the `metrics` facade was chosen over
depending on the `prometheus` crate directly:

* `metrics` + `metrics-exporter-prometheus` for the in-process metrics
  registry and Prometheus text rendering. `metrics-exporter-prometheus`'s
  optional Hyper-listener feature is **not** enabled — this stays a pure
  registry + text-encoder, matching "Exposition: a mountable handler, not a
  bound port" above.
* `tracing` (unconditional) for spans/events; `tracing-opentelemetry` +
  `opentelemetry-otlp` for OTLP export, both behind the `otlp` Cargo feature
  (off by default, and genuinely absent from the dependency tree when
  unused — not merely unused at runtime).

## Related

* [data-flow](data-flow.md) — the flow these metrics measure.
* [open-questions](open-questions.md#backfill-status-and-observability) — the
  pre-existing backfill-status/lag-telemetry question this doc subsumes.
* [#14](https://github.com/salesforce-misc/trellis/issues/14) — the motivating stall.
