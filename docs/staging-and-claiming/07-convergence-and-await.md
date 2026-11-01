# Cross-cutting — Convergence, and the read-your-writes predicate

← [Cleanup and reclaim](06-cleanup-and-reclaim.md) · next → [Invariants and the tests that pin them](08-invariants-and-pins.md)

**What this owns:** answering "has my write been reflected yet?" without lying.

**The guarantee:** *every un-reflected effect of a commit at position L is
represented by at least one pending position at or below L.* The predicate may
over-report ("not yet" when actually done); it must never say "done" when it is not.

Every naïve version produces **false `converged`** by narrowing the question to
something cheaper. If you build one thing from this series carefully, build this one.

## Watermark tokens

A caller reads its own writes without waiting for global idleness:

1. after its mutation commits, it reads `pg_current_wal_insert_lsn()` **on its own
   connection** — the **token**;
2. it polls until derived state reflects every commit at or below the token.

Taken *after* the write commits, the token bounds that write's position from above.
It is the WAL *insert* position, not `pg_current_wal_lsn()`'s *write* position: a
writer with `synchronous_commit = off` gets its commit back before its WAL is written,
so the write position can still sit below the `origin_lsn` its trigger stamped, and
the wait would return before the change is applied.
The engine's workers do the work; the caller only waits — no client-driven drain loop.
Capture triggers write a change's ring rows in the writer's own transaction
([01](01-capture-by-triggers.md)), so every commit at or below the token is already
in the ring when the token is read, and the waiter writes nothing.

## The predicate

`converged_through(token)` is **one statement**, so every condition is evaluated
against one committed snapshot. True iff all three hold (numbered 2–4, as in
`staging::converge`):

2. **the active batch's table holds no row with `origin_lsn <= token`** — the
   active tail. The pointer resolves in the same statement, so a concurrent seal
   cannot slip between resolve and scan;
3. **no non-active slot holds a still-pending row with `origin_lsn <= token`**,
   under one shared definition of "pending";
4. **no parked quarantine row has `origin_lsn <= token`** — the poison band
   ([06](06-cleanup-and-reclaim.md)). Held rows are per transform; a
   wait scoped to one transform (`await_converged_for`, the self-check
   auditor's) counts only that transform's.

`origin_lsn` is the **oldest un-reflected trigger position** carried by a pending
change. It LEAST-merges in the fold ([04](04-claiming-and-the-fold.md)) — a re-stage
can only make it older — while the ordinary position GREATEST-advances. Downstream
propagation carries the *minimum* trigger origin to the dependent it stages; `0`
means "unknown, conservatively old", and so does a `NULL` origin, which wins the
LEAST-merge rather than losing it. A capture trigger stamps each row with the
WAL insert position when it runs, below its commit's position; only rows with
genuinely unknown origin (a build's `NULL`-key recomputes, a write
made outside a drain) stay `NULL`.

> **Invariant (await soundness):** every un-reflected effect of a commit at
> position L is represented by ≥1 pending change with `origin_lsn <= L`. Any staging
> path that sets `origin_lsn` higher than the oldest un-reflected trigger — i.e.
> forgets the LEAST-merge — silently breaks read-your-writes.

## The trap: no per-batch summary band

The ring keeps no per-batch summary of origin positions (a `min_origin_lsn`,
`max_lsn` pair). A predicate built on one is the obvious optimization — one indexed
comparison per batch instead of a scan — and it is **wrong**. A band aggregated in
the seal's phase-1 flip would run *inside* the flip transaction, so it would describe
the rows *that transaction* could see, not the slot. Two writers land outside it:

- a **straddler** — invisible in `S_k`, claimed by batch *k+1*
  ([03](03-sealing-and-the-fence.md));
- a **phase-gap writer** — commits between the flip's commit and the later snapshot
  capture, so it *is* visible in `S_k` and batch *k* claims it.

Each would surface as a false `converged`. Condition 3 **asks the slot**, so it needs
no band and cannot disagree with the other pending readers. A band finalized at
snapshot capture would not be honest either: a snapshot-time aggregate still can't see
a straddler, so it would sit *beside* a straddler term — the shape that leaks.

**"Narrowing condition 3" is a recurring, unsound temptation** — drained-only,
straddler-only, band-only all admit a false `converged`. Buy the honest question back
with an index, not a weaker question. It is deliberately *not* narrowed to "buckets
not yet drained" either: a part-drained batch reports its whole slot as pending. That
over-reports — the safe direction; the batch settles moments later.

## Making the honest question cheap

**Index it.** Conditions 2 and 3 filter on `origin_lsn`, which the ordering index
(position and key) cannot serve. Without a dedicated index the probe is a sequential
scan, costliest exactly when the minimum origin sits late in a big sealed slot — the
case downstream propagation produces routinely. With the index it is a cheap seek at any size.

**The index is necessary but not sufficient**, and this is the half people miss. Ring
tables carry `autovacuum_enabled = off` ([02](02-the-staging-ring.md)), which also
disables **autoANALYZE**. With no statistics the planner's default inequality
selectivity picks a sequential scan at *every* size — not a threshold a big table
crosses. The two conditions get there differently, both load-bearing:

- **Condition 3** is served by statistics the engine writes itself: a single-column
  `ANALYZE (origin_lsn)` on a slot **as it is sealed** — the moment it stops growing
  and the moment condition 3 starts asking about it. Single-column so no other plan
  changes; best-effort, since a failed refresh costs plan quality and nothing else.
- **Condition 2 needs no statistics**, because it is written `min(origin_lsn) <=
  token`, not `EXISTS (… origin_lsn <= token)`. Equivalent when the column is `NOT
  NULL`, but `min()` plans as a `Limit 1` over the index regardless of planner belief.
  Required, not tidy: condition 2's slot is the **active** one, which grows
  continuously, so no `ANALYZE` cadence keeps it current and a stale histogram from a
  previous lap actively points the wrong way.

## Ask for the sign, not the number

| Question | Shape | Cost | Use |
|---|---|---|---|
| "has my token converged?" | the four conditions, one statement | cheap — index-served | the await poll |
| "is anything pending at all?" | `EXISTS`, short-circuits on first pending row | cheapest | a polled gate (`has_pending`, test and harness builds) |
| "how much is pending?" | `count(*)` over every slot plus quarantine | expensive — full scan | observability only, **never polled** |

`pending_count` is linear in the ring and no index helps — there is no predicate to
index away. It stays **off the polling path**: the engine's await polls the token
predicate, and a polled consumer wants the sign, not the number, so the sign is its
own question. One asymmetry in the `EXISTS` form: `true` short-circuits, `false` does
not — it is a gate ("has my work drained?"), not a work-arrival poll.

## What the await waits for

`await_converged(client, token, timeout)` polls the predicate on the caller's
connection, with a backoff that starts at 5 ms and doubles to a 250 ms ceiling. It
drains nothing and claims nothing: the engine's workers do that, and the poll only
reads. Each poll runs under a server-side `statement_timeout` of the remaining budget
(never less than a floor, so a zero timeout still makes one real check), so a poll
that blocks on a lock cannot overrun the deadline by more than the floor. The poll
returns when the predicate reads `true`; anything else, including a result it cannot
read, means "not yet".

A poll that does not converge in time ends in `ConvergenceTimeout`, carrying the token
and how long it waited, and it is a timeout (retryable), not a verdict about the
schema. It does not say *why* the token has not cleared. The predicate does not
distinguish an idle ring from a refused or contended one: while a row at or below the
token is un-applied it reads "not yet", whether the batch is waiting for a seal, held
by a worker that is applying it, or held by a worker that died and awaits the reclaim
TTL ([06](06-cleanup-and-reclaim.md)). It is the caller's budget that bounds the wait.

**Convergence is cluster-wide**, deliberately: the predicate reads the pointer and
every slot and does not care which worker holds a claim. A peer that *stalls* makes the
wait longer; a peer that *dies* is bounded by the reclaim TTL.

**Parked quarantine work is counted.** Condition 4 holds the wait for a parked row at
or below the token, because that work is un-applied: a quarantined key keeps blocking
a caller waiting on its band until an operator release or the transform's resume
clears it. `await_converged_for` narrows condition 4 to one transform's held keys,
since a key another transform holds is applied by this one as usual. The ring's pending
rows (conditions 2 and 3) count in full either way.

## No loose view feeds the predicate

A read of "every pending ring row" that is not fenced can include a straggler the fold
will attribute to a later batch. Over-exempting and over-reporting are both
conservative for a consumer that subtracts or reports unapplied rows, so
`self_check` reads the source's unapplied ring rows with its own predicate to subtract
them from a comparison (ADR-0013, "Quiescence"). That read is never wired into
`converged_through` or any other fenced read: the strict predicate above and a loose
view stay separate.
