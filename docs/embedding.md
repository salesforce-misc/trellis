# Embedding Trellis

How to run Trellis in-process inside a host app — a Rails or Phoenix
application that wants its `TRANSFORM`/`RELATIONSHIP` declarations to live
alongside its own migrations, rather than deploying Trellis as a separate
service. The design commitments behind this shape are recorded in
[ADR-0010](decisions/0010-embeddable-clients.md); this doc is the operator/
embedder-facing walkthrough of what they mean in practice.

## The recommended shape

A Trellis-embedding fleet typically runs two kinds of process against the
same database:

* **Web and migration processes** — define transforms, read `status()`, run
  migrations. These connect at `drain_threads: 0`: no background CDC intake,
  no drain workers. Cheap to run in every web dyno/pod without each one
  competing to drain the same work.
* **One dedicated worker process** — runs the live pipeline: CDC intake +
  ring maintenance (`staging: true`) and one or more drain workers
  (`drain_threads: N`) that actually apply staged changes into target
  tables.

```rust
// A web process: define transforms, never drains anything.
let trellis = Trellis::connect(Config::resolve(None)?, TrellisOptions::default()).await?;
trellis.apply("TRANSFORM widget_totals FROM widgets SELECT price + tax AS total").await?;
```

Defining a transform is cheap wherever it runs. `apply` validates the
definition, creates the target table, records the transform as
`waiting_to_backfill`, and returns. It doesn't read the source table's rows and
doesn't touch the replication publication or slot, so it takes the same time
against an empty table as against a billion-row one. The dedicated worker does
the rest in the background: it publishes the source, reads its existing rows,
builds the target, and flips the transform to `live`
([data-flow — Capturing a table's existing rows](data-flow.md#capturing-a-tables-existing-rows)).
So a web process needs no publication ownership or replication privileges; only
the worker does. Code that needs the target populated polls `status()` until
the transform is `live`, then calls `await_converged` with a fresh
`watermark_token()` to let the backfill's staged rows drain. That pair is the
contract: `live` means the transform is in its steady state, so a token awaited
after it covers every commit at or before the token
([ADR-0016 — What `live` promises](decisions/0016-single-background-capture-path.md#what-live-promises)).
Until #476 lands, a chunked or direct build's go-live catch-up can still be
pending after `live`
([data-flow — What it asks of a deployment](data-flow.md#what-it-asks-of-a-deployment)).

*Planned (#427):* `DROP` still changes the publication from the process that
applies it, so a process that drops transforms still needs publication
privileges; the decided design moves that shrink to the staging worker's
reconcile pass, driven from the catalog.

```rust
// The one dedicated worker process for this fleet.
let worker = Trellis::connect(
    Config::resolve(None)?,
    TrellisOptions { staging: true, drain_threads: 4, ..Default::default() },
)
.await?;
```

## The silent-stall hazard (issue #144)

This shape has one sharp edge: **if the dedicated worker process is never
deployed, or gets scaled to zero, nothing errors.** Every `apply()` call
still succeeds, every transform still gets registered — it just sits in
`TransformStatus::WaitingToBackfill` forever, because nothing in the fleet
runs the staging worker that captures its source's rows, or the drain
workers (`drain_threads > 0`) that build and maintain its target. Read paths against
the target table quietly return nothing (or stale data, for a transform that
was already live before the worker process disappeared), with no exception,
timeout, or log line pointing at the actual cause.

This is the single most likely misconfiguration for an embedded deployment —
easy to hit (a deploy config typo, an autoscaler with a bad minimum, a worker
dyno nobody remembered to add), and hard to notice until someone asks why a
derived table looks empty or frozen.

### Detecting it: `has_live_drain_workers` and `has_live_staging_worker`

`Trellis`/`BlockingTrellis` expose two cheap, single-query health checks
built for exactly this, one per kind of worker:

```rust
if !trellis.has_live_drain_workers().await? || !trellis.has_live_staging_worker().await? {
    // Every transform in this fleet is at risk of sitting in
    // `WaitingToBackfill` forever — page someone, don't just log it.
}
```

It answers "is there at least one live drain worker anywhere in this fleet
right now" — not "is *this* connection running one." Call it from any
connection, including one that itself runs at `drain_threads: 0` (a web
process is exactly where you want this check to live, since that's the
process an uptime monitor or load balancer actually polls).

Under the hood, each `Client` started with `drain_threads > 0` registers
itself in a worker registry at startup, heartbeats it on every maintenance
tick, and removes it on clean shutdown; `has_live_drain_workers` is one
`exists(...)` query with no joins against that table, comparing each
worker's last heartbeat against the same reclaim-TTL notion of staleness the
engine already uses to decide a claim is dead (30s by default) — so a worker
that crashed without a clean shutdown stops counting as live within that
same window, no separate cleanup pass required. See
`trellis::staging::worker_registry`'s doc comment for the full mechanism.

`has_live_drain_workers` counts drain workers only, so a fleet whose drain
workers run but whose staging worker doesn't passes it while no change is
captured and every new transform stays in `WaitingToBackfill`.
`has_live_staging_worker` (issue #428) is the check for that half: it asks
whether some connection holds this instance's producer singleton, the
session-scoped advisory lock the staging worker's intake holds for as long as
it streams. It needs no heartbeat, since Postgres frees the lock the moment a
crashed worker's connection closes. It also reads `false` while a failed
intake waits to restart, when nothing is captured either. That wait starts at
a second and doubles to a minute while intake keeps failing, so a single
dropped connection reads `false` for about a second: page on it staying
`false` across a few checks, not on one reading.

**These are liveness checks, not backlog checks.** `true` means the worker is
alive; it says nothing about whether it is keeping up. Use
`Trellis::status`/`Trellis::watermark_token` + `await_converged` to reason
about an individual transform's own progress.

### Wiring it into a host health check

Neither the Ruby nor the Elixir binding exists yet (epic #140 — the
`Client::start`/`Trellis` shape above is the whole surface today; a binding
is a thin Rustler/Magnus wrapper over it, per ADR-0010 decision 1). Until
then, the pattern below is written against the Rust API directly, sized for
what a binding's eventual `Trellis.has_live_drain_workers?` /
`Trellis.has_live_staging_worker?` (Elixir and Ruby alike) calls are
expected to wrap one-to-one
— see ADR-0010 decision 4 for why a plain boolean needs no flattening to
cross that boundary.

**Phoenix**, wired as a `Plug` health-check endpoint polled by the
platform's liveness probe:

```elixir
defmodule MyAppWeb.HealthController do
  use MyAppWeb, :controller

  def workers(conn, _params) do
    # `MyApp.Trellis` is the supervised `ResourceArc` handle ADR-0010
    # decision 3 describes — one per node, held in the supervision tree.
    cond do
      not MyApp.Trellis.has_live_drain_workers?() ->
        send_resp(conn, 503, "no live drain workers in this fleet")

      not MyApp.Trellis.has_live_staging_worker?() ->
        send_resp(conn, 503, "no live staging worker in this fleet")

      true ->
        send_resp(conn, 200, "ok")
    end
  end
end
```

**Rails**, as a scheduled check (e.g. a recurring Sidekiq job or a
`rails-healthcheck`-style route) rather than on every request — this check
is a fleet-wide question, not something that needs to be re-answered on
every web request:

```ruby
class TrellisWorkerHealthCheck
  def self.perform
    unless Trellis.instance.has_live_drain_workers?
      Rails.logger.error("no live Trellis drain workers — every transform is stalled")
      # ... page, raise, whatever this app's alerting expects ...
    end
    unless Trellis.instance.has_live_staging_worker?
      Rails.logger.error("no live Trellis staging worker — no change is captured")
      # ... same ...
    end
  end
end
```

Either way, the shape is the same: poll on a timer (not once at boot — a
worker process can be scaled to zero well after a healthy start), and treat
`false` as "every transform in this fleet may be silently stuck," not as a
transient blip to retry past.
