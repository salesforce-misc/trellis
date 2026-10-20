# Defining Transforms

A transform describes how one derived (target) table is produced from one or
more source tables. Its definition is three separately-chosen pieces: the
**granularity** of the target's primary-key space, the **calculated fields**
that populate its non-key columns, and an optional **partial-data** predicate
restricting which source rows participate.

Trellis maintains these tables **incrementally** as source data changes, trading
spikey read load for a steady write load that keeps the table cheap to read (see
the README for fuller motivation).

## Supported sources and targets

Trellis reads tables it does not own ([0005](decisions/0005-source-schema-is-user-owned.md)),
so it validates them when a definition is applied and refuses what it can't
maintain correctly, with an error naming the table or column and the fix. These
refusals are the supported-subset contract, not bugs. Everything in the table
is checked for a transform's source, for each relationship endpoint and
to-side it reads through, and (where noted) for its target. Trellis never adds
a key, index or policy itself.

| Condition | Refused at define | If it appears after define | Remedy |
|---|---|---|---|
| A source with no primary key (or unique index standing in for one), a partition (a leaf or a middle) named as a source, a partitioned root that is itself a partition, or a table in an inheritance hierarchy. A partitioned root is checked by the rows below. | Yes. A statement trigger fires only for the table a statement names, so capture would miss writes made through the rest of the hierarchy. | A table that joins a hierarchy after define (`ATTACH PARTITION`, `INHERIT`, a child created `INHERITS` it, or the table dropped and recreated as a partitioned table, a partition or an inheritance child) pauses every transform that reads it on the staging worker's next reconcile pass, with the reason in `status()`'s `capture_failure`, and gets no capture installed. `self_check` reports a `capture` divergence. Writes made through the hierarchy before that pass are missed, or, on an inheritance parent, a child's rows are staged as the parent's own; the resume's rebuild re-reads the table and repairs both. A dropped key pauses the transforms that read it, with the reason in `status()`, and so does one retyped (see the key rows below). | Use a plain table with a primary key. After a later change, undo it (`DETACH PARTITION`, `NO INHERIT`, or recreate the table as a plain table), then `RESUME`, which refuses while the table is still in a hierarchy. |
| A source table, a `JOIN`'s joined table or a relationship's to-side that is dropped, renamed away, moved to another schema, swapped for another table or replaced by a view after define, or whose capture triggers or functions are disabled, dropped, recreated or replaced outside Trellis | n/a (a definition can't name a table that isn't there) | Pauses every transform that reads the table on the staging worker's next reconcile pass, with a `capture_failure` naming what it found and saying that changes made meanwhile may be missing, so correctness can't be guaranteed. Nothing is reinstalled or rebuilt until a `RESUME`. A partitioned source's tree is not checked for its triggers. | Put the table back (or finish the swap) and `RESUME`, which refuses while the table is missing, re-validates the definition against the table now at that name, reinstalls capture and rebuilds. Plan a scripted `RESUME` after a swap or a bulk load with triggers disabled. |
| A partitioned root ([0017](decisions/0017-partitioned-sources.md); `RANGE`, `LIST` or `HASH`, any depth, with or without a `DEFAULT`) as an aggregate's `FROM` whose `GROUP BY` omits a partition-key column at any level, as either table of a `JOIN` whose `GROUP BY` columns and `ON` columns from that table omit one, or as a to-many relationship's to-side whose join column is not every partition-key column at every level | Yes, naming the column. With every partition-key column in the group key, each group lives in exactly one partition, so a change to the tree touches only the groups routing to one node. | n/a | Add the named column to the `GROUP BY`, or join on it. |
| A partitioned root whose key level is an expression (no column to route by), whose partition-key column has a type other than `smallint`, `integer`, `bigint`, `numeric`, `text`, `varchar`, `uuid`, `date`, `timestamp`, `timestamptz` or `boolean`, a non-default operator class or a nondeterministic collation | Yes, naming the table and the column (for an expression, its key position). Trellis routes a group to its partition by evaluating the recorded bound over the group key, and it checks that only for these keys. | n/a | Partition by a plain column of a listed type with the type's default operator class and a deterministic collation. |
| A partitioned root with more than 1,000 leaves, or on PostgreSQL before 15 | Yes, stating the limit. No build statement prunes, so each one plans, locks and opens every leaf, and the default lock table runs out for one statement somewhat above 1,000. | n/a | Merge partitions to 1,000 or fewer leaves, or upgrade the server. |
| A partitioned root as a 1-1 transform's `FROM`, or as the to-side of a to-one relationship | Yes. A 1-1 target over a partitioned source would need its own partitioning, and partitioned targets are not supported. | n/a | Aggregate over the partitioned table instead, or use a plain table. |
| A partitioned root of a servable shape (every row above passes) | Yes, as `PartitionedSourceNotCaptured`. Capture covers every node of the tree. The reconcile pass queues a correction for each partition attached, detached, dropped or truncated, and for groups in a target that route to no partition, and an aggregate re-derives each correction's keys while it reports `backfilling`. A `JOIN` with the partitioned table on either side recomputes that side's partials that route to the partition, and carries the change into its target, while it reports `backfilling`. A count through a to-many relationship whose to-side is the partitioned table is corrected by recomputing the routed parents, and the definition reports `backfilling` until those recomputes are applied. | After a dump and restore of the database, the next reconcile pass pauses every transform that reads a partitioned source, with a `capture_failure` naming the restore, and runs no correction queued before it. A `pg_upgrade`, or the root dropped and recreated or swapped in by renames, is no restore. | Use a plain table. After a restore, `RESUME`, which rebuilds. |
| An aggregate target, or a 1-1 target over one, used as a source (it has no primary key: its identity is a `UNIQUE NULLS NOT DISTINCT` constraint) | Yes, for another Trellis instance. Inside the owning instance it can be chained off, because writes to a target reach its readers in the writing transaction, never through capture triggers. | n/a | Group over a 1-1 transform, or chain within the instance ([instance-identity](instance-identity.md)). |
| Row-level security that applies to a role Trellis runs as (#745, #765, #766) on a source, on a relationship's to-side, or on a target. Policies apply to a role when RLS is enabled and the role neither owns the table (directly or through membership) nor has `BYPASSRLS`, or owns it and the table is `FORCE ROW LEVEL SECURITY`. A superuser is exempt. Every Trellis connection runs with `row_security = off`, so a read or write the policies would filter fails rather than skipping rows. | Yes, checked for the role that owns the ring, and for the defining role on a to-one to-side and on a table that is another transform's target. Define checks before it widens the relationship's projection, its first read of the to-side, so this refusal comes ahead of the cycle, key, collation and unkeyed-source checks. Declaring a to-one relationship is refused when the to-side's policies apply to the session's role, since declaring fills the relationship's projection from it. | For the ring's owner, the staging worker's reconcile pass pauses every transform that reads the table (or, for a target, the transform that writes it), with the reason in `status()`'s `capture_failure`, and `self_check` reports a `capture` divergence. For a role a drain logs in as, its refused read or write pauses the same transforms, and everything downstream of them, with `capture_failure` of kind `halt`. So does a backfill discharge or go-live catch-up whose read the staging worker's role is refused, and a Re-derive build's group-delta merge whose write a drain's role is refused. A refused build chunk pauses its transform after its charged retries, with the error on `backfill_failure`. The window before the pass, and the refusals that only log or retry: [gap 11](known-correctness-gaps.md#11-row-level-security-on-a-role-trellis-runs-as). | Give the role `BYPASSRLS` (not inherited), or make it the table's owner without `FORCE ROW LEVEL SECURITY`; then `RESUME`. |
| A table a logical-replication subscription in the same database replicates into (#751) | Yes, in any sync state, including a disabled subscription. The subscription's apply worker fires only row-level triggers, so its changes never reach capture. Publishing a table to another database is fine. | Paused and reported as for RLS on the ring's owner. | Remove the table from the publication and `ALTER SUBSCRIPTION … REFRESH PUBLICATION`, or drop the subscription; then `RESUME`. |
| A nondeterministic collation (an ICU collation with `deterministic = false`) on a key column: a source primary key, a `GROUP BY` key (including a relationship path's to-side column), a relationship join column or an endpoint's primary key (#638). Also on a column passed to `STRPOS` or `REGEXP_COUNT`, directly or through `COALESCE`, another field or a relationship path. | Yes, for the definition, the relationship, and an `ALTER TRANSFORM` that adds or alters such a field. Trellis matches keys by exact text; such a collation's `=` would fold keys it keeps apart. `CHAR_LENGTH` and `OCTET_LENGTH` accept any column. | On a key column, the staging worker pauses every transform that keys by it, with the reason in `status()`'s `capture_failure`, and `RESUME` refuses until it's deterministic again. On a column only a text function reads: not detected. | Keep the column's collation deterministic ([type-support](type-support.md#collation)). |
| Relationship join columns that differ in type, type modifier or collation, or whose type is off the join-key allowlist (#590) | Yes, when the relationship is declared, and again for each transform that reads through it, at its define and at each `RESUME`. Trellis never casts a join key. `integer` against `bigint`, `text` against `varchar`, and `varchar(50)` against `varchar(255)` are all refused. | A type off the allowlist, or a type or modifier that no longer matches (one side widened, the other not yet), pauses every transform that reads through the relationship, with both columns and both types in `capture_failure`; `RESUME` refuses with the same error until they match. A change between deterministic collations pauses nothing (byte equality is unchanged), but `RESUME` refuses the pair until the collations match too. | Alter one column to match the other, then `RESUME`. |
| A relationship endpoint that is not keyable, has a key type off the primary-key allowlist, or (for one of this instance's targets) is not `live` (#429) | Yes. Every requirement is checked at declaration ([relationship-propagation](relationship-propagation.md#endpoint-requirements)), and again for each transform that reads through it, at its define and at each `RESUME`. | A key dropped later, or retyped off the allowlist, pauses its readers. | Fix the table, then declare the relationship. |
| A key column (a source key, a `GROUP BY` key, a relationship's join column or to-side key) retyped so that the values Trellis stored render differently: `timestamp` ↔ `timestamptz`, `date` → `timestamp`, `text` → `uuid` or `integer`, `varchar` → `character(n)`, or a narrower `numeric` scale, temporal precision or `varchar(n)` (the rewrite rounds, or strips trailing spaces) (#760) | n/a: define sees one type. | An `ALTER COLUMN … TYPE` rewrites the table without firing a trigger. The staging worker pauses every transform keyed by the column, with the old and new type in `capture_failure`. | `RESUME`: it re-types Trellis's copies of the column and rebuilds from the source. Or drop and define again. |
| A column Trellis creates a column from with its type, widened. Those columns are a 1-1 target's key, passthrough and calculated columns, an aggregate's `GROUP BY` key (copied into its target, ledger and group-delta table), its field columns and its ledger's contribution columns, a `JOIN`'s `GROUP BY` keys and `ON` columns (copied into its target and its hidden tables, each with its exact type and collation) and its field columns, and a relationship projection's key and its column for each to-side column read through it. Widenings are `integer` → `bigint` (and `smallint` up), an integer → `numeric` (unless every value of the `numeric` fits the integer), `real` → `double precision`, `varchar(n)` → longer or `text`, and a wider `numeric`, temporal, `interval` or `bit varying` precision (#767, #824) | n/a: each column takes its type from the source at define, or when an `ALTER TRANSFORM … ADD` adds it. | A widening that changes only the catalog (`varchar(n)` → longer, `text` or `varchar`; `numeric(p,s)` → more precision at the same scale) is applied to Trellis's columns in place by the staging worker: no pause, no rebuild, and the keys a too-long value held in between are released. A `character(n)` change or `text` → `varchar(n)` isn't one. Otherwise the column keeps its type, so a value it can't hold would fail its write. The staging worker pauses every transform that owns a column the type define would give it now outgrew (`SUM(qty)`'s ledger contribution after `qty` goes `integer` → `bigint`), naming the source column and each of those columns. A narrowing pauses nothing here, except a `varchar(n)` key's (the row above). A narrowing, a field's move to another type family (`integer` → `double precision`), or any other change that isn't a pure widening, rebuilds the table's readers on its own, and pauses a transform define would now refuse, one that evaluates the column as another type, or one whose `numeric` column must be re-typed to the new scale ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#a-column-changed-or-rewritten-without-a-trigger)). | `RESUME`: the staging worker `ALTER`s each column whose type differs from the one define would give it now, under `ACCESS EXCLUSIVE` (a table rewrite for `integer` → `bigint`; reads of the target wait), then rebuilds. A `JOIN`'s hidden tables are created again at the new types instead. A table with any widening that needs a rewrite takes this path for all its columns, its catalog-only widenings included. Or drop and define again. |
| Two `GROUP BY` keys that share a target column name (`buyer.name` and `seller.name`, or `name` beside `buyer.name`); keys can't be aliased | Yes. | n/a | Group over a 1-1 transform that selects them under distinct names (`buyer.name AS buyer_name`). |
| A field named after a 1-1 source key column (`id AS id`), or any field or `GROUP BY` column whose name starts with `__` (#566) | Yes. The target already carries the key columns, and `__` names are Trellis's hidden columns (such as an `AVG`'s running sum). `id AS order_id` is an ordinary column. | n/a | Rename the field. |
| An aggregate whose argument names another aggregate field when no source column has that name (`SUM(val) AS total, MAX(total)`), directly or through a field that reads one | Yes (`AggregateOfAggregateField`). The argument would nest one aggregate in another, which no build can run. | n/a | Aggregate the source column instead (`MAX(val)`). |
| `ARRAY_AGG` or `STRING_AGG` over an argument that isn't `text`, `varchar` or `char(n)`, `STRING_AGG` with a separator that isn't a string literal, or either over a to-many relationship path (#293) | Yes (`FunctionArgTypeMismatch`, `StringAggSeparatorNotLiteral`, `ListAggregateOverToManyRelationshipUnsupported`). `text[]` is the only array result type Trellis models, and the to-many relationship fold has no form of either aggregate. | n/a | Write the separator as a literal. For another argument type, or a to-many path, none: the grammar has no cast to `text`. |
| A bare name that could mean a source column or a field: the name of a source column and of a field whose expression is not just that column, read by another field (`(grp + 1) AS val, SUM(val) AS total`). An aggregate field's name inside another aggregate's argument is not ambiguous: it is the source column ([Calculated Fields](#calculated-fields)). | Yes (`AmbiguousColumnReference`), naming both meanings. `val AS val` passes the column through, so reading `val` elsewhere is not ambiguous. | A source column added later under the name of a field: nothing is detected, and the transform keeps reading the field. An `ALTER TRANSFORM` that adds a field of that name is refused, as define would. `RESUME` re-validates and refuses what define would: in an aggregate target, a field another field reads by that name, and in a 1-1 target, any field of that name. | At define, write `src.val` or `public.src.val` to read the source column, or rename the field and read it by its new name. After a later column, rename or drop that column and `RESUME`, or drop the transform and define it again. |
| A relationship named after its from-table (`RELATIONSHIP posts FROM posts.author_id TO users.id`) | Yes, when it's declared. A transform over `posts` reads `posts.<column>` as its own source column, so the relationship's paths could never be read. | n/a | Give the relationship another name. |
| A `JOIN` transform (`FROM a [LEFT\|INNER] JOIN b ON … GROUP BY … SELECT …`) that passes the checks in the rows below | No. Define registers it and creates its target, and it reports `waiting_to_backfill`: nothing builds or maintains a `JOIN` target yet, so the target stays empty (epic [#1091](https://github.com/salesforce-misc/trellis/issues/1091)). Each table is held to the rows above, as a `FROM` is. | A column the transform reads from either table dropped or renamed, or a key column of either (its primary key, an `ON` or a `GROUP BY` column) retyped as the key rows above describe, pauses the whole transform on the staging worker's next pass, naming that table and column in `status()`'s `capture_failure`, and so does a column of either table retyped to a type the transform evaluates differently. A widening of a column it copies is handled as the widening row above describes. Either table dropped or renamed leaves it `waiting_to_backfill`. `RESUME` checks both tables and refuses, naming the table, while define would refuse the transform. | Until it's built, use a [relationship](#relationships) with an aggregate over its to-many path. After a later change, undo it, then `RESUME`. |
| A `JOIN` that is `RIGHT`, `FULL`, `CROSS`, `NATURAL`, `USING` or a comma join; joins a table to itself, or two tables with the same bare name (`shop.orders JOIN archive.orders`); joins a third table; has no `GROUP BY`; has an `ON` that is anything but equalities between a column of each table joined by `AND` (`OR`, `<`, a literal, an expression, two columns of one table, a column pair repeated); reads a column without its table (`SUM(qty)`, `GROUP BY region`) or through a table that is neither side (a relationship path); has an aggregate whose argument reads both tables (`SUM(orders.x + items.y)`); or has a field that is neither a `GROUP BY` key selected on its own nor a composition of aggregates (a column outside an aggregate, a nested aggregate, no aggregate at all) | Yes (parse error, or `JoinSelf`, `JoinOnSameSide`, `JoinOnRepeatedPair`, `JoinUnqualifiedColumn`, `JoinUnknownTable`, `JoinUnresolvedColumn`, `JoinMixedSideArgument`, `JoinFieldShape`). Only `INNER` and `LEFT` equi-joins of two different tables are admitted, with every column written `<table>.<column>`. A field is a `GROUP BY` key from either table, or composes aggregates with literals, operators and functions (`COALESCE(SUM(items.qty), 0)`); each aggregate is `COUNT(*)`, or `COUNT`/`SUM`/`AVG` of a column or an expression over one table's columns (`SUM(items.qty + 1)`). Each argument is kept as a running total on its own table, so one reading both tables has no such total. | n/a | Rewrite the definition in the admitted shape. For `RIGHT JOIN`, swap the two tables and use `LEFT JOIN`. |
| A `JOIN` whose `ON` columns differ in type, type modifier or collation, are off the join-key allowlist, or have a nondeterministic collation; or a `GROUP BY` key of a type that can't be one | Yes (`JoinKeyTypeMismatch`, `JoinUnsupportedKeyType`, `JoinNondeterministicCollation`, `UnsupportedGroupByKeyType`). The same rules as a relationship's join columns and a `GROUP BY` key. | An `ON` column retyped so its pair no longer matches, or onto a refused type or a nondeterministic collation, pauses the whole transform on the staging worker's next pass, naming that table and column in `status()`'s `capture_failure`. A change between deterministic collations pauses nothing (byte equality is unchanged), but `RESUME` refuses the pair until the collations match too. | Alter one column to match the other, then `RESUME`. |
| `MIN`/`MAX`, `bool_and`/`bool_or`/`bit_and`/`bit_or`, float or `interval` `SUM`/`AVG`, or `array_agg`/`string_agg`/`jsonb_agg` in a `JOIN` transform. Plain aggregates keep these until [#1105](https://github.com/salesforce-misc/trellis/issues/1105) removes the per-row ledger. | Yes (`DeferredAggregate`), named by family. Only the aggregates kept exact by signed deltas are admitted. | n/a | Not supported yet: [#1117](https://github.com/salesforce-misc/trellis/issues/1117) (`MIN`/`MAX`), [#1118](https://github.com/salesforce-misc/trellis/issues/1118) (`bool_*`/`bit_*`), [#1119](https://github.com/salesforce-misc/trellis/issues/1119) (float and `interval`), [#1040](https://github.com/salesforce-misc/trellis/issues/1040) (`array_agg`/`string_agg`/`jsonb_agg`). |
| `SUM` or `AVG` over an unconstrained `numeric` column or an expression whose result is `numeric`, or over any type but `smallint`, `integer`, `bigint` and `numeric(p,s)`, in a `JOIN` transform | Yes (`UnconstrainedNumericAggregate`, `UnsupportedAggregateArgument`). Postgres shows a sum with as many decimal places as the most precise value in the group, which a running sum can't track; a declared scale removes the mismatch, and an expression's `numeric` result has none. | n/a | Sum the `numeric(p,s)` column itself, declare a scale (`ALTER TABLE … ALTER COLUMN … TYPE numeric(p,s)`), or wait for [#1120](https://github.com/salesforce-misc/trellis/issues/1120). |
| A `WHERE` other than `TRUE` | Yes (parse error). Partial data is not supported ([#804](https://github.com/salesforce-misc/trellis/issues/804)). | n/a | None. |
| Chaining off a transform that is not `live` (`TransformNotLive`) | Yes. | n/a | Wait for the upstream to go `live`, then define. |
| A target whose name is taken: a relation of any kind already exists in the target schema under the target's name or the name of a table Trellis keeps beside it (`<target>__ledger`, `<target>__deltas`). That includes another definition's target, in this instance or in another instance sharing the target schema. | Yes (`TargetTableExists`, a `conflict` error). Trellis writes only to target tables it creates, and `DROP TRANSFORM` drops the target, so it never takes over an existing relation. | n/a | Choose another target name, give the instance its own target schema ([instance-identity](instance-identity.md)), or drop or rename the existing relation. |
| A target in a Trellis catalog schema: `TRANSFORM <schema>.<target>` naming this instance's catalog schema, or a schema holding another instance's `trellis_instance` table (matched by schema name exactly as written, and by a table of that name, even one the role can't read). The target, its ledger and its group-delta table would live there, and the other instance's uninstall (`drop schema … cascade`) would drop them. | Yes (`TargetInCatalogSchema`, a `validation` error), before anything is created. An unqualified target takes `Config::target_schema`, which an attach already refuses to be either ([instance-identity](instance-identity.md#a-catalog-schema-that-belongs-to-another-instance)). | A target schema that gains a `trellis_instance` table after define (another instance attached to it) pauses nothing, and the transform keeps running. `RESUME` re-validates and refuses with the same error, leaving the transform paused. | Put the target in an ordinary schema (`TRANSFORM reporting.t`). After define, `DROP TRANSFORM` it (paused) and define it again in an ordinary schema: removing the other instance's catalog from the schema would drop the target with it. |

A refusal after define is a pause, not a loss: `RESUME` rebuilds the target from
source once the condition is fixed ([Status](#status)). `status()`'s
`capture_failure` carries the reason. Every `RESUME`, of a transform or of one
field, first runs define's validation against the live schema, and refuses,
leaving the transform paused, while define would refuse it; the error names the
column and what to change. A resume also refuses while the target's own tables
are no longer what define would create (a 1-1 target whose source key was
redefined, a ledger table that is missing, an aggregate whose group-delta table
is missing or has different columns; [The repair tools](known-correctness-gaps.md#the-repair-tools)), and
the error says to drop the transform and define it again; a field resume makes
only the source-key check. When a resume has copies to re-type it returns
at once, the transform still `paused` with a `capture_failure` that says so,
and the staging worker re-types them and starts the rebuild. The re-type honours
the `statement_timeout` of Trellis's role or database: one cancelled three times ends
the request, the transform still `paused`, and the `capture_failure` names the two
remedies, raising the timeout, restarting Trellis and resuming again, or `DROP
TRANSFORM` and defining again ([recommendations](recommendations.md#statement_timeout)). Other ways a
source can drift after define are in
[known correctness gaps](known-correctness-gaps.md).

Other writers are not affected by the subscription rule: a session with
`session_replication_role = replica` fires the capture triggers, which are
`ENABLE ALWAYS` ([stage 1](staging-and-claiming/01-capture-by-triggers.md)).
Changing a source key's collation to another deterministic one is safe, though
the rest of any build then scans the source and `self_check` reads the target
by scan rather than by index.

## Granularity

Granularity determines the target's primary-key space — what a single target row
represents relative to its source row(s). The grammar has three: 1-1, aggregate and join aggregate. A join aggregate
isn't built yet (see [Supported sources and targets](#supported-sources-and-targets)).

### 1-1

Exactly one target row per source row, inheriting the source's identity: its
primary key, or, when the source's key is nullable (an aggregate or `JOIN`
target's grouping columns), a `UNIQUE NULLS NOT DISTINCT` constraint over the
same columns, so a group whose key has a `NULL` part has its row here too.
Insertion/deletion maps 1-1. Always a single source table: deriving a target
from a join of two tables is a [join aggregate](#join-aggregate), not 1-1, even
when the join is one-to-one in practice.

The target always carries the source's key columns, so a field can't
reuse a key column's name (see [Supported sources and targets](#supported-sources-and-targets)).

### Aggregate (`GROUP BY`)

Each distinct combination of grouping values produces one target row, matching
the equivalent `GROUP BY` query against the source. The primary key is the tuple
of grouping columns; many source rows can map to one target row, and adding,
removing, or changing a source row can insert, delete, or update a target row.

Grouping-key columns may be referenced directly; any other source column must be
wrapped in exactly one aggregate: `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`,
`BOOL_AND`, `BOOL_OR`, `BIT_AND`, `BIT_OR`, `JSONB_AGG`, `ARRAY_AGG` or
`STRING_AGG`. `COUNT(*)` counts
rows in the group, and `COUNT(<expr>)` counts the rows where `<expr>`
isn't null. Which column types each aggregate accepts is in the
[type-support matrix](type-support.md).

A grouping key can also be a to-one relationship path (`GROUP BY post.author`).
Each key becomes a target column named after its bare column, and keys can't be
aliased. An aggregate target also has hidden columns of Trellis's own, such as
the running sum behind an `AVG`. Name limits are in
[Supported sources and targets](#supported-sources-and-targets).

```
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount
```

**Throughput depends on how many groups a batch touches, not on how few there
are.** Fewer groups do not buy more headroom, but they don't cost any either.
Issue #277 measured a single `SUM`/`COUNT(*)` aggregate offered 400k rows/sec
from 8 drain workers. It folded the same ~85k rows/sec at 10, 100 and 400
groups. At that end the cost is per source row (the claim-time fold and
decoding each staged row image are the largest sampled costs), and hot target
rows aren't what limits it. The shape that hurts
is many distinct groups receiving writes at once: 56–69k rows/sec at 4,000
groups, and ~2k rows/sec at 40,000. At 40,000 groups, one drain worker folded
more than eight did. Every group a batch touches costs an existence
probe against the source table, run while that group's target row is locked.
The probe can't use an index on the `GROUP BY` column, so drain workers whose
batches share groups queue behind each other for those rows. If you're planning
an aggregate with tens of thousands of groups written concurrently, benchmark
it first (`benchmark group-contention --groups <n>`).

### Join aggregate

An aggregate over the join of two tables (`FROM a [LEFT|INNER] JOIN b ON …
GROUP BY …`). Its target holds one row per group: a column per `GROUP BY` key
and per field, and nothing else. The state that keeps it exact lives in tables
Trellis keeps in its own catalog schema, named by the transform's internal id,
which readers get no grant on. Define registers it and creates its target, and
it reads `waiting_to_backfill`: nothing builds or maintains it yet.
[Supported sources and targets](#supported-sources-and-targets) lists the
checks; epic [#1091](https://github.com/salesforce-misc/trellis/issues/1091)
builds the rest.

Both tables are read the way a `FROM` is: each is captured, and a column the
transform reads that either table loses, or a key column either retypes,
pauses the whole transform on the staging worker's next pass, naming that
table. `RESUME` checks both tables again.

## Relationships

A join aggregate changes a table's granularity. Often you instead want to keep a
table's own granularity and merely *enrich* it with columns looked up from a
related table — order lines decorated with `product.category_name`.

A relationship is a named, directed link from one table to another, defined by a
join key (`order_line_items.product_id -> products.id`) and declared as its own
reusable statement. Either endpoint may be a source table or a transform
target, 1-1 or aggregate. A source-table endpoint needs a primary key, like any
table Trellis captures. A target endpoint doesn't: Trellis's own writes to it reach
the relationship directly, so it must be `live` when the
relationship is declared: its initial build writes it outside that path. Both
endpoints are written as bare table names and resolved once, through the
declaring connection's `search_path`; the relationship stays pinned to the two
tables found then, even if a same-named table later appears earlier on some
connection's `search_path`. `trellis status` shows each endpoint's schema. The
referencing table keeps its own primary
key and granularity — a 1-1 table stays 1-1 — and calculated fields on it
reference the related table's columns
through paths whose head is the relationship name. How depends on **cardinality**:

* **To-one** (the to-side join key is a primary key or `UNIQUE`): at most one
  related row. Referenced as a bare path (`product.category_name`); null if no
  related row matches.
* **To-many** (the to-side join key is not unique): many related rows may match.
  Referenced only when wrapped in exactly one aggregate (`sum(comments.word_count)`),
  computed over the related rows like a `GROUP BY` aggregate.

Each join column's type must be on the join-key allowlist in
[type-support.md](type-support.md) (a domain never is), and the two join
columns must have the same type, type modifier and collation, because Trellis
never casts a join key. The error names both columns and both types. Both
sides also need deterministic collations
([Supported sources and targets](#supported-sources-and-targets)).

These rules hold after define too. A join column later altered onto a
refused type or collation, or a pair whose types or modifiers no longer
match (one side widened to `bigint`, the other not yet), pauses every
transform that reads through the relationship, and `RESUME` refuses until
both sides match again ([Supported sources and targets](#supported-sources-and-targets)).

See [0006-relationships](decisions/0006-relationships.md) for the full design and
[0005-source-schema-is-user-owned](decisions/0005-source-schema-is-user-owned.md)
for how the cardinality/uniqueness prerequisites are validated (never imposed) on
the user's source schema.

## Calculated Fields

Every target table has a set of **calculated fields** — non-key columns populated
by a formula rather than copied from a source column. A formula may reference:

* Source columns feeding the calculated row, following the shape its granularity
  requires (see [Granularity](#granularity)).
* Related-table columns through a declared relationship — a bare path for to-one,
  wrapped in an aggregate for to-many (see [Relationships](#relationships)).
* Other calculated columns on the same target table, by name.

A bare name means a field when no source column has that name, and the source
column when no field does. A name that is both a source column and a field whose
expression is something else is refused at define as ambiguous, with both
meanings in the error ([Supported sources and targets](#supported-sources-and-targets)).
A 1-1 target refuses such a field outright, whether or not anything reads it.
Two reads of such a name are not ambiguous:

* A field reading its own name reads the source column (`SUM(val) AS val`), and
  a field that passes its column through (`val AS val`) is that column.
* In an aggregate target, an aggregate's argument never reads a column that is
  itself an aggregate: a name there means the source column, as in SQL.
  `SUM(val) AS val, MIN(val) AS lo` takes the minimum of the source column
  `val`, not of the sum. A column that isn't an aggregate (`(id + 1) AS adj,
  SUM(adj)`) is still read by name, as long as no source column is named `adj`.

A **qualified name** always reads the source column: `src.val`, or
`public.src.val`, where `src` is the `FROM` table. So `(grp + 1) AS val,
SUM(src.val) AS total` sums the source column beside the field that shadows it.
A two-part name whose head is not the `FROM` table is a relationship path, which
is why a relationship can't be named after its from-table. A three-part name
must name the `FROM` table and its schema. A field has no qualified form: to
read a field a source column shadows, rename the field.

Formulas may only use **immutable** functions and operators — those whose output
depends solely on their inputs. Anything depending on database state outside the
row (current time, timezone, collation, session settings, random values) is
disallowed: incremental maintenance requires that re-evaluating a formula on
unchanged inputs always yields the same result.

### Chaining and cycle detection

Calculated columns may reference each other, letting users build derivations out
of small named steps (a `margin` column defined in terms of `revenue` and `cost`).
Transforms also chain: a target table may itself be a source for another —
aggregated, joined, or referenced through a relationship. Trellis builds a
dependency graph spanning columns within a table and tables across chained
transforms, and evaluates in dependency order.

A transform can only chain off a target once that target's own transform is
`live`. Defining it while the upstream is still building (for example, a plain
1-1 target whose chunked backfill hasn't finished) is refused with
`TransformNotLive`. Each write to a target reaches the transforms reading it inside the
same transaction as the write; Trellis never installs capture triggers on a
target, not even one that is a relationship endpoint. That in-transaction hand-off
is why an aggregate target, which has no primary key, can be chained off at all.
It does not cross into another instance.

**Cycles are rejected at definition time across the whole graph.** A definition
that would introduce a cycle, directly or transitively, is invalid and rejected
before it runs, keeping evaluation order well-defined.

## Partial data

**Not supported:** the grammar accepts only `WHERE TRUE`, and any other
predicate is a parse error. This section describes the intended design.

Any target table, regardless of granularity, may be defined over a *subset* of
its source rows via a row-level predicate. Like a partial index, materializing
only a narrow, high-value slice keeps the write load small; excluded rows never
enter the target or incur maintenance cost.

## Target tables are Trellis-owned

A target is materialized as a plain table in the target schema, and reading it
is the whole point: query it, join it, index it, grant it to whoever needs it.
Its contents and shape, though, belong to Trellis
([0014-pause-and-drop-a-transform](decisions/0014-pause-and-drop-a-transform.md)).
Trellis assumes it is the table's only writer. A change made behind its back is
not corrected, and whether it reaches anything chained off the table is
undefined ([known correctness gaps](known-correctness-gaps.md#8-operator-changes-inside-trellis-owned-artifacts)). Treat a target as read-only, and specifically:

* **Do not insert, update or delete rows.** An edited row stays wrong until the
  next event that recomputes it, and a row added by hand is accounted for by
  nothing. `self_check`
  ([0013](decisions/0013-self-check-production-recompute-audit.md)) reports the
  divergence; it does not repair it. `PAUSE` then `RESUME` rebuilds the table
  from source.
* **Do not add, drop, rename or retype columns.** Trellis addresses its columns
  by name and type. Redefining the transform is how a column changes
  ([Changing a definition](#changing-a-definition)).
* **Do not change the key constraints.** A 1-1 target's key (its primary key,
  or its unique constraint over a nullable-keyed source) and an aggregate
  target's unique grouping constraint are how Trellis addresses a row when it
  upserts or deletes it.
* **Do not `TRUNCATE` or `DROP` the table yourself.** `DROP TRANSFORM` removes
  the table and its definition together, and refuses while another definition
  still chains off it. A hand `TRUNCATE` leaves the table empty until the
  transform is paused and resumed.
* **Don't let row-level security apply to the role that writes it.**
  Trellis writes a target as the role each worker connects as, with
  `row_security = off`, so a write that policies applying to that role would
  filter is refused rather than skipping rows.
  Trellis creates the target as the role that defines it, so that role owns
  it and is exempt unless the table is `FORCE ROW LEVEL SECURITY`. Enabling
  row-level security on a target for your application's readers is fine as
  long as the workers run as the table's owner (or a member of it) or have
  `BYPASSRLS`. Those readers see their policies only on their own connections:
  `Trellis::pool()`'s connections also run with `row_security = off`, so a
  read through them as a role the policies apply to fails instead. See
  [Supported sources and targets](#supported-sources-and-targets) for what
  happens when that stops holding. A definition is also refused if
  DDL around its target's creation, such as an event trigger that forces
  row-level security on every new table, makes the policies apply to the
  defining role.
* **Expect a partial table while `backfilling`, and after `RESUME`.** The
  [status](#status) says when the table is complete; a reader that needs
  completeness checks it.

Indexes, and grants to other roles, are yours to add. They live and die with the
table, so `DROP TRANSFORM` takes them with it. A unique index, constraint or
trigger you add that refuses a row Trellis writes, whether it checks the
statement or is deferred to the commit, fails that row's key like any value its
target refuses: the key ends up held in quarantine, and rows that fail only
together hold their page back as a `drain_failure` (see [Status](#status)).
Another Trellis instance may read
a 1-1 target keyed by a primary key as a source, exactly as it would any table
with a primary key. An aggregate target, and a 1-1 target over one, may not be
read that way (see [Supported sources and targets](#supported-sources-and-targets)).

## Status

Every defined transform carries an observable **status**:

* **`waiting_to_backfill`** — defined, but pre-existing source rows not yet read.
  Every new transform starts here: defining one returns before any source row
  is read ([data-flow — Capturing a table's existing rows](data-flow.md#capturing-a-tables-existing-rows)).
* **`backfilling`** — those rows have been read and the target is being built.
  A plain aggregate (grouped by plain columns of a table, not of another
  transform's target, with no relationship and no `MIN`/`MAX` of text), and a
  plain 1-1 transform (no relationship, over a table rather than another
  transform's target), is built while its live changes are applied, and goes
  from here straight to `live`
  ([data-flow — The Re-derive build](data-flow.md#the-re-derive-build)).
  A `live` 1-1 transform comes back here while an `ALTER TRANSFORM` that adds
  or changes a field, or a resumed column, is rebuilt in the background
  ([Changing a definition](#changing-a-definition)). A `live` transform this
  build serves also comes back here, from the call's return, while
  `request_backfill` on its source (or a re-install of its source's capture
  triggers) rebuilds it. A `live` aggregate over a partitioned source, or a
  `live` `JOIN` with a partitioned table on either side, comes back here when
  a partition of that table is attached, detached, dropped or truncated, or
  when it still counts rows that route to no partition: from
  the staging worker pass that detects it (until then, for at most one pass,
  it still reads `live`) until the keys the change touched are re-derived and
  folded into its target, then it is `live` again. A restart in the middle
  resumes the correction, and the transform still reads `backfilling`. Its
  status lists each partition it is being corrected for in `correcting`, with
  when the pass queued the correction and how many keys are left
  ([observability — Transform status lifecycle](observability.md#transform-status-lifecycle)).
* **`catching_up`** — the build is done and live changes are applied, but the
  target may still be missing changes made while it was building. A catch-up
  re-reads the source and takes it `live`.
* **`live`** — the steady state: once a transform reports `live`, awaiting a
  watermark token taken after a commit guarantees its target reflects that
  commit ([embedding — Reading your own writes](embedding.md#reading-your-own-writes)).
* **`quarantined`** — broken and no longer maintained (the quarantine fuse tripped);
  resuming re-runs the backfill, returning it to `waiting_to_backfill`.
* **`paused`** — frozen deliberately, by an operator's `PAUSE` rather than by the
  fuse. The same frozen state `quarantined` is, reached by the other trigger: the
  target stops being written to and holds its current, now-stale value. Resuming
  likewise re-runs the backfill from `waiting_to_backfill`. Trellis also pauses
  a transform itself when a source column it reads is renamed or dropped, or a
  column it keys by, or that a column Trellis created takes its type from,
  changes type, with the reason on
  its status (`capture_failure`)
  ([stage 01](staging-and-claiming/01-capture-by-triggers.md#a-renamed-or-dropped-column),
  [Supported sources and targets](#supported-sources-and-targets)). Every
  resume first re-validates the transform as define would, and refuses while
  define would refuse it, or while its target's tables are no longer what define
  would create ([The repair tools](known-correctness-gaps.md#the-repair-tools)).

A transform can also hold single source keys in quarantine whatever its status,
`live` included: a key whose change kept failing in this transform's apply (a
value its target refuses, say) is left out of its apply, and its target row
stays as it was. Every other transform reading the key keeps applying it. The
status reports them as `held_keys`: how many keys the transform holds, and when
the one held longest was poisoned. `self_check` reports the same with every
audit. `sample_quarantined` lists the keys with their errors, and once a key's
cause is fixed, `release_key(transform, source_table, key)` releases it (the CLI's
`trellis release`, and `release_key` in each binding): the key is re-derived from
its current row, and a key whose cause is still there is quarantined again.
Resuming the transform releases every key it holds
([ADR-0003 — Releasing held keys](decisions/0003-quarantine-storage-and-api.md#releasing-held-keys)).
`trellis status` prints each held key, its table and its transform quoted for the
shell where they need it, so they paste into `trellis release` as printed: text
goes inside `'...'`, with a quote written `'\''`, and a control character, such as
the separator between a composite key's columns, is written `$'\ooo'` in octal,
which bash and zsh read. A value that is exactly `-h`, `--help`, `-d` or
`--database-url` goes after a `--`: `trellis release -- <transform> <table> <key>`.

A release first waits for the drain pages in flight on the key's table to commit,
so it can't miss a change one of them is parking for the key. If that wait runs
past the lock timeout every Trellis connection has (30 seconds at most), the
release fails with a `timeout` error that says so, and changes nothing: the key is
still held, and running the release again releases it once the pages commit.

A failure the drain can't charge to a key and that pauses nothing (a read
Postgres refuses that the catalog can't pin on a table, records that fail only
together) holds a whole page back instead. Every drain pass retries it, and the
status of each transform reading a table on the page, directly or through a
relationship, reports it as `drain_failure`, with the error, the tables, and
since when, while the status word stays what it was. `self_check` reports every
such page. It clears when the page commits, so fix the cause the error names
([observability — Transform status lifecycle](observability.md#transform-status-lifecycle)).

An application can list defined transforms and read each one's status — enough to
tell a newly-defined transform is still populating, without a metrics pipeline
([embedding — Poll to `live`, don't wait](embedding.md#poll-to-live-dont-wait)
has the polling pattern).

A transform can sit in `waiting_to_backfill` while a long-lived cluster
transaction holds the backfill's fence open — a safe wait, explained with its
remedy under
[observability — Transform status lifecycle](observability.md#transform-status-lifecycle).

## Changing a definition

Every definition-changing operation is one statement of the same grammar a
definition itself is written in, run through one entrypoint —
`Trellis::apply(text)`, or `trellis apply '<statement>'` from a shell:

```text
TRANSFORM <target> FROM <source> [GROUP BY <keys>] SELECT <fields> [WHERE <predicate>]
RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>

PAUSE  TRANSFORM <target>[.<column>]
RESUME TRANSFORM <target>[.<column>]
DROP   TRANSFORM <target>
DROP   RELATIONSHIP [<schema>.]<from_table>.<relationship_name>
```

**Addressing.** A transform is named by its target table, bare (`widget_prices`)
or schema-qualified as `define` returns it (`public.widget_prices`); `ALTER
TRANSFORM` and `DROP TRANSFORM` take either form, and so do the `Trellis` methods
that name a transform (`status`, `quarantine_status`, `sample_quarantined`,
`release_key`, `self_check`). A qualified name must match exactly, so a wrong
schema names nothing. `PAUSE` and `RESUME` also take `<transform>.<column>`,
pausing or resuming a single calculated field rather than the whole definition.
The column form always takes the bare transform name, and it wins when an
address reads both ways: `a.b` is column `b` of transform `a` when `a` declares
a field `b`, and otherwise the transform registered as `a.b` (name that
transform bare when it collides). `DROP TRANSFORM a.b` refuses an address
whose `a` is a transform and that names no transform itself, since dropping a
field is `ALTER TRANSFORM a DROP b`. A relationship, which only `DROP` names,
is always scoped to its from-table (`posts.author`, or `blog.posts.author`),
because a relationship name is unique only there. Two same-named tables in
different schemas are different from-tables, so `blog.posts` and `shop.posts`
can each declare an `author`; when both do, the bare `posts.author` is refused
as ambiguous and the address must name the schema.

**Semantics** are covered by
[0014-pause-and-drop-a-transform](decisions/0014-pause-and-drop-a-transform.md).
In short: `PAUSE` and `DROP` are idempotent; `RESUME` re-validates the
transform against the live schema, refusing while define would or while the
target's own tables aren't what define would create, and rebuilds
by a fresh backfill rather than catching up on changes that happened during
the pause. A `RESUME` of a transform whose upstream (a transform whose target
it reads) isn't `live` is accepted and waits for it, and is re-validated once
it is, so a whole stack resumed at once, in any order, comes up in dependency
order; a `PAUSE` of a waiting
transform withdraws its resume;
`DROP` removes the target table's data along with the definition, and is refused
— naming the blockers — while another registered definition still chains off the
subject, so a chain is retired from the leaves inward. A relationship with the
target at either end counts as one.

**Pause and resume apply to transforms only.** That list of statements is
complete — there is no `PAUSE RELATIONSHIP`/`RESUME RELATIONSHIP`. A
relationship is a reusable *component* of a transform, not something that does
work of its own, so it has nothing to suspend; pausing the transform that uses
it is what stops that work. Retiring a relationship is meaningful, though — it
is a definition — so `DROP RELATIONSHIP` exists.

`DROP TRANSFORM <target>.<column>` is likewise not a statement: removing one
calculated field is an `ALTER TRANSFORM` operation, not a drop.

An `ALTER TRANSFORM` validates the definition it would leave against the source's
live columns, the schema as of its own commit and not as of define, with define's
rules: it refuses what define would refuse, naming the column and what to change.
A column the host added since define is readable by an added or changed field,
and a field named after it is refused. A column the host dropped, or retyped so
that a field reading it no longer validates, refuses an edit that leaves that
field, so drop or change the field in the same statement. An added or changed
field can't read a column whose type the host changed since the transform was
defined or last resumed, even one it would validate over: `PAUSE` and `RESUME` the
transform first, which re-types it over the source as it is now, then edit it.

An `ALTER` of a field compares the column's current type, modifier included, with the
type `DEFINE` would give the altered field: a bare passthrough takes its source column's
exact type, any other expression its value family's. A widening that changes only the
catalog (`varchar(8)` to `varchar(20)` or `text`, `numeric(10,2)` to `numeric`) re-types the
column in place within the `ALTER`'s transaction, under a short `ACCESS EXCLUSIVE` lock on
the target, before the field's values are re-derived. An `ALTER` that would rewrite the
target (a narrowing such as `numeric` to `numeric(10,2)`, or another type family) is
refused, naming the remedy: `DROP` the field, then `ADD` it again.

**`apply` only registers.** Every statement returns once the change is
recorded; none reads the transform's source rows. A new transform is built in the
background from `waiting_to_backfill`. An `ALTER TRANSFORM` that adds or
changes fields, and a `RESUME TRANSFORM <target>.<column>`, start a
*field build*: the transform goes `backfilling`, the changed fields are
computed for every new change from that moment on, and background chunks
rewrite them across the existing rows, reading the source once however many
fields changed. The transform reads `live` again once they are all done, so
poll its status before relying on the new fields
([embedding — Poll to `live`, don't wait](embedding.md#poll-to-live-dont-wait)).
Until then a row's new field can still be empty (an added field) or hold the
old formula's value (a changed or resumed one). A field whose formula reads a
source column the source's capture doesn't record yet waits, paused, until
capture records it before its build starts.

The one read inside the call is of a to-one relationship's to-side table, not of
a transform's source: declaring the relationship creates its projection and
seeds it from the to-side rows, and defining a transform that reads a column
through it widens that projection, in the same transaction
([embedding](embedding.md#what-the-staging-worker-needs-from-the-database)).

## Grammar versions

Every stored definition records the grammar version it was written in. A
breaking release that changes what accepted text means bumps that version and
keeps reading the older one under its old rules, so an upgrade never
re-interprets a stored definition. Each major version reads every grammar
version the previous two majors wrote. A transform written under an older
version than that is paused until you drop it and define it again, and a version
newer than the binary knows, from a downgrade, is refused. A stored definition
can be rewritten to the current version without a rebuild. The rules are in
[0018-grammar-versioning](decisions/0018-grammar-versioning.md).

## Scope

This document describes the logical model only, not the physical storage or
timing of derived data; see [data-flow](data-flow.md) for how changes propagate
and [0002-async-data-flow](decisions/0002-async-data-flow.md) for why that flow
is asynchronous.
