---
status: accepted
date: 2026-08-15
deciders: Michael Ries
consulted: 
informed:
---

# Asynchronous Data Flow

This project could apply data transformations synchronously or asynchronously,
and the two patterns have very different effects on the user's experience.

## Decision

Trellis applies transformations **asynchronously**, using Postgres logical
replication to detect source-data changes and applying derivations in batches
outside the application's write path.

The tradeoffs that drive this choice follow.

## Synchronous

This architecture would update derived tables (including transitive derivations) at the time of comitting changes to source tables.
This could be accomplished with tools like triggers, plPgSQL, SQL body functions, etc.

### Pros

* Data consistency by construction
  * Transformations can be backfilled at the time of definition (with the ability to check on status), and all future transformations are applied in the same transaction that modifies their source data.
  * All reads are strongly consistent with the state of the source data.
* Extremely fast/efficient for shallow/sister data transformations
  * Populating additional columns directly on a source row table can be done in the same write operation, meaning we have very low-overhead and can support 500k+ changes/sec on even modest hardware.
* Invalid data fails at write-time, so applications know exactly which operation caused the problem, rather than having to check for failed calculations after-the-fact.
* Tool familiarity: All of the triggers, functions, etc are visible within the postgres catalog, and they are common patterns known to DBAs. Virtually all resource utilization happens in the database so you have a single pool of resources to manage.
  * no replication slot, everything survives a backup and restore with no special handling, does not require any clients to maintain connection in order to function

### Cons

* formulas that reference across relationships are unsound by default. Two transactions can run and end up leaving invalid data because they don't see each other's changes until after they've landed.
* data changes that disable triggers will leave stale data behind. This is true of things like logical replication being applied (ie AWS DMS), so the user would have to replicate ALL source tables AND target tables in order to have data consistency, easy to mess up
* No batch re-computes, all triggers fire on individual row changes. If 500 rows all get updated and they all fold into a single aggregate row, we have to execute 500 functions/scripts/snippets
  * This makes synchronous pattern much slower for maintaining aggregate tables where the asynchronous path can compose batches of individual source updates into a single write per affected group on the target table
* No clear method for chained updates (ie source row => aggregate table => second aggregate)
* One failed derivation blocks the entire write, no efficientw way of pausing or quarantining certain derivations while continuing with the rest.
  * this is the other side of the data consistency coin, we have to complete all derivations within the commit, so it's not possible to update 99 dependents and leave one broken, we have to fail the whole write.
* updates on highly connected rows become very slow for the regular OLTP workload
  * updating an account with thousands of orders and millions of line-items could require the equivalent of a very large bulk transaction, makes it harder to reason about the cost of ordinary SQL queries
  * this get worse when handling concurrent updates to related source rows. Imagine 20 individual updates that all aggregate into a single regional total, the 20th update will have to wait for the first 19 to clear their write locks on the aggregate row before it can process. Lock contention on writes becomes nested and quickly dominates performance

## Asynchronous

An asynchronous pattern would use the logical replication capabilities built into Postgres to watch source tables for changes.
When changes are detected (or new definitions need to be backfilled) clients will pull batches of changes rows and start to update the dependent derived tables/columns.

### Pros

* Batched operations can be used for both backfill and incremental updates.
  * A set of stale rows can be loaded and collapsed into the minimal number of writes needed for just the next layer of derivations.
  * Transitive dependencies are fulfilled by adding the rows that were modified by a previous step into the staging area.
  * Batched operates are especially valuable for aggregate operations where we can collapse N rows of source data change into a minimal M affected rows in the aggregated table
    * 180k+ source rows updates per second can be sustained indefinitely
* Invalid data is already committed to source tables, but we can quarantine or pause just the dependents that failed, allowing the rest of of the derivations to continue flowing
* Moves resource consumption and additional write-latency out of the hot update path of the application
  * Could also extend this to allow for "pausing" derivation work during busy times and resuming later
* Backpressure means increased latency in derived data, without affecting performance on source tables
* No concurrency tax for writers
  * Lots of concurrent updates across many source tables won't become slower due to contention on the target tables

### Cons

* Eventual consistency requires us to design around potential stale data
  * Could have an `await(LSN, timeout)` function that waits for all transactions up to LSN making it through all derivations
  * Would have latency metrics available to be monitored at all times
* Requires us to put calculated columns on a neighbor table, so we don't get notified about our own writes
* Replication slot usage
  * Danger of falling behind can block additional writes (we can mitigate this by writing data into a staging area)
    * Important to keep at least 1 Trellis client connected to PG whenever writes might be happening
  * Replication slots consume up to 1 full CPU in the database
  * Requires us to design for database failover scenarios (easier on Postgres 17+)
* Error API required
  * Client applications will need to check for errors since we won't detect them at the point of source data changing
* Total throughput is limited by the logical replication speed
  * PostgreSQL only allows up to 1 CPU to be used for decoding the WAL files and streaming logical replication changes over the API
  * This is a bottleneck in cases of high write-throughput
