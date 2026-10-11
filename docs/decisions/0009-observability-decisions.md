---
status: accepted
date: 2026-09-15
deciders: Michael Ries
---

# Observability Decisions

[docs/observability.md](../observability.md) lays out the design for metrics,
logs/traces, and transform-status visibility. This ADR records the
specific choices behind it: dependencies, latency keying, where status lives,
the staging-ring metrics, bucket boundaries and retention.

## Decisions

### 1. Dependencies: `metrics` facade, not `prometheus` directly

**Decision:** `metrics` + `metrics-exporter-prometheus` for the metrics
registry and Prometheus text encoding; `tracing` + `tracing-opentelemetry` +
`opentelemetry-otlp` for logs/traces.

Both are pure in-process registry/encoder libraries — neither adds an HTTP
server to the core `trellis` crate. `metrics-exporter-prometheus`'s optional
Hyper-listener feature is **not** enabled, preserving `docs/observability.md`'s
"mountable handler, not a bound port" design (`render_prometheus()` stays a
plain function the operator serves from their own HTTP stack, as the CLI's
`--prometheus-bind` flag does).

The facade decouples recording an observation (via the `Recorder` trait,
implemented once by `metrics-exporter-prometheus`) from reading the registry
back out. That keeps any consumer — the `render_prometheus()` exposition path
or another reader — off a second recording path and avoids hardwiring
Prometheus's concrete `Registry`/`HistogramVec` types into this crate's call
sites. The one alternative weighed was depending on the `prometheus` crate
directly (the option `docs/observability.md` listed); rejected for exactly that
hardwiring, at the cost of one extra crate in the graph.

### 2. End-to-end latency keying: terminal transform only

**Decision:** one histogram per **terminal (sink) transform**, summed across
every source feeding it — not per source→sink pair.

Keying by pair multiplies cardinality by fan-in for no benefit: "how stale is
this transform's output" is the question operators ask, answered by the
terminal transform alone. Per-source breakdowns, if ever needed, are better
served by traces (decision 3) than a permanent high-cardinality series.

### 3. Traces vs. flat logs: spans are first-class

**Decision:** adopt `tracing` spans as a first-class signal for propagation. A
change moving source → hop → hop → apply is a span tree, and
per-hop/per-transform latency is *derived* from span durations rather than
timed a second time.

Two downstream effects:

* Instrumentation of the propagation path (capture, staging, fold, apply) is
  span-based, not flat-log-plus-metrics.
* The per-transform latency histogram is populated from
  span/timing data captured during fold, not a second independent measurement.
  Fold already holds the origin timestamp (decision 5); a span from that origin
  to the applied output gives the histogram its `.observe()` value directly, so
  traces and the latency metric stay consistent by construction.

### 4. Transform status storage: no new field

**Decision:** transform lifecycle status stays in the one status field of the
transform's definition — the `TransformStatus` enum
(`WaitingToBackfill | Backfilling | CatchingUp | Live | Quarantined | Paused`),
persisted as text and check-constrained.

Quarantine and the backfill lifecycle are two arcs of one state machine on that one
field: `Quarantined` is the value [ADR-0003](0003-quarantine-storage-and-api.md)'s
whole-transform fuse writes, and `Paused` is the operator-or-engine freeze of
[ADR-0014](0014-pause-and-drop-a-transform.md). ADR-0003's *column*-level tier
(`column_status`/`column_failures`) is a separate, finer-grained mechanism — a `live`
transform can carry paused columns without its `status` moving — so the two tiers don't
conflict.

The transitions are written where they happen: the `xmin`-fence wait and the backfill
discharge move a transform through `waiting_to_backfill`/`backfilling`/`live`, the fuse
trip writes `quarantined`, and a resume returns a frozen transform to
`waiting_to_backfill`. Per-key (`poison`) and per-column tiers have writers
too.

### 5. Staging-ring metrics: drop the per-transform depth gauge, add a cheap segment-state gauge

**Decision:** no per-transform `staging_ring_depth{transform}` gauge. Two
pieces cover the need:

* **Per-transform latency histogram**, computed at apply completion. The
  origin timestamp already rides on every staged change and truncate, stamped
  by the capture trigger, and apply already groups folded changes by consuming
  transform. One `.observe()` per transform-group there is effectively free: no
  new I/O, no new join.
* **`staging_segments{state}` gauge** (new, system-level): a count of segments
  by state (`Active`/`Sealed`/`Draining`/`Drained`), read on demand from
  segment metadata, not incremented on the hot path.

The rejected gauge needed hot-path bookkeeping (increment on stage, decrement
on fold) — a throughput risk for a "supporting" series. It's also the wrong
shape: the ring partitions by source-table/key-hash bucket, not by transform, so a live
per-transform depth would need a join the layout doesn't offer.
`staging_segments{state}` gives the same "is the ring backing up" signal at
near-zero cost.

**Throughput counter unit:** `trellis_changes_applied_total{transform}`
counts staged ring rows a transform folded and applied, not folded changes. The
fold collapses a fenced window's rows to one change per `(src_table, key)`, so
counting folded changes measured neither transactions nor rows. It measured a
batching artifact: the same 30 inserts could report 30, 3 or 1 depending on
when a segment sealed. A staged row matches the operator's model (rows written
to the source, comparable to the application's write rate and to
`pg_stat_user_tables`). It also composes across hops with no extra plumbing,
because a propagated change is itself a ring row: each target writer reports
every physically changed key through the target-mutation seam, which stages one
row per key for the next hop. The fold carries each group's row count with its
folded change, and a truncate sentinel counts as one row. The count
is taken after the truncate-void filter, so a row a later truncate in the same
window voided isn't counted: it was never applied, and the only alternative
would be to charge it to the sentinel, which counts as one row. Whether such a
row counts therefore depends on whether it drained before the truncate's
segment, the one place batching still shows through. The latency histograms
observe once per folded change, because a folded change carries one origin
timestamp and nobody asks for a row-weighted
quantile. The counter is therefore not the histograms' throughput
denominator. The histograms' own `_count` series is, minus bare recompute
triggers, which carry no origin timestamp and aren't observed.

### 6. Histogram bucket boundaries

**Decision:** exponential buckets, ~10ms–60s (matching the stated
"sub-second to tens-of-seconds propagation range"), one global default for
every histogram:

```
[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0]  // seconds
```

Not per-transform configurable: a single default keeps every histogram comparable.

### 7. Metric retention: left to Prometheus, not Trellis

**Decision:** Trellis retains no metric history. Retention, rollup, and
cross-instance aggregation are what an operator's Prometheus/VictoriaMetrics/
Thanos stack already does; duplicating a pruned history table inside Trellis
would add write load, a schema object, and a prune job for no gap Trellis needs
to fill. Operators configure their scraper's retention against
`render_prometheus()` like any other target (see
[Retention](../observability.md#retention-left-to-prometheus-not-trellis)).

## Consequences

* The `metrics` facade adds one crate to the graph, and the operator serves
  `render_prometheus()` from their own HTTP stack: the core crate binds no port.
* End-to-end latency is per terminal transform. A per-source breakdown comes
  from traces, not from a metric series.
* There is no per-transform ring depth: a backlog shows only system-wide, as
  segments by state.
* The throughput counter counts staged rows, so a row a later truncate in the
  same window voided counts or not depending on when its segment drained, and
  the counter is not the latency histograms' denominator.
* Every histogram shares one fixed set of buckets, from 10 ms to 60 s.
* Trellis keeps no metric history: retention and rollup need the operator's
  scraper and time-series stack.
