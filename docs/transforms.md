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
is checked for a transform's source, for each table a `JOIN` reads, and (where
noted) for its target. Trellis never adds
a key, index or policy itself.

| Condition | Refused at define | If it appears after define | Remedy |
|---|---|---|---|
| A source with no primary key (or unique index standing in for one), a partition (a leaf or a middle) named as a source, a partitioned root that is itself a partition, or a table in an inheritance hierarchy. A partitioned root is checked by the rows below. | Yes. A statement trigger fires only for the table a statement names, so capture would miss writes made through the rest of the hierarchy. | A table that joins a hierarchy after define (`ATTACH PARTITION`, `INHERIT`, a child created `INHERITS` it, or the table dropped and recreated as a partition or an inheritance child) pauses every transform that reads it on the staging worker's next reconcile pass, with the reason in `status()`'s `capture_failure`, and gets no capture installed. `self_check` reports a `capture` divergence. Writes made through the hierarchy before that pass are missed, or, on an inheritance parent, a child's rows are staged as the parent's own; the resume's rebuild re-reads the table and repairs both. A dropped key pauses the transforms that read it, with the reason in `status()`, and so does one retyped (see the key rows below). | Use a plain table with a primary key. After a later change, undo it (`DETACH PARTITION`, `NO INHERIT`, or recreate the table as a plain table), then `RESUME`, which refuses while the table is still in a hierarchy. |
| A source table or a `JOIN`'s joined table that is dropped, renamed away, moved to another schema, swapped for another table or replaced by a view after define, or whose capture triggers or functions are disabled, dropped, recreated or replaced outside Trellis | n/a (a definition can't name a table that isn't there) | Pauses every transform that reads the table on the staging worker's next reconcile pass, with a `capture_failure` of kind `disruption` naming what it found and saying that changes made meanwhile may be missing, so correctness can't be guaranteed. Nothing is reinstalled or rebuilt until a `RESUME`. A partitioned source's root and every node of its tree are checked, and the finding names the node; a root dropped and recreated (empty or with leaves), swapped by renames or renamed away is a disruption too, while partition maintenance (an attach, detach, new leaf, dropped leaf, `DEFAULT` or `HASH` re-split) is none. A plain table dropped and created again as a partitioned one, or the reverse, is a disruption as well. | Put the table back (or finish the swap) and `RESUME`, which refuses while the table is missing, re-validates the definition against the table now at that name, reinstalls capture and rebuilds from that table. A table created again with another primary key (a plain table converted to a partitioned one widens it by the partition key) is rebuilt the same way: what Trellis stored or staged under the old key is discarded, not read against the new one. A 1-1 target is keyed by its source's key, so its resume refuses then: `DROP TRANSFORM` and define it again. Plan a scripted `RESUME` after a swap or a bulk load with triggers disabled (the Ruby and Elixir clients have a test helper for fixture loads: [embedding](embedding.md)). |
| A partitioned root ([0017](decisions/0017-partitioned-sources.md); `RANGE`, `LIST` or `HASH`, any depth, with or without a `DEFAULT`) as an aggregate's `FROM` whose `GROUP BY` omits a partition-key column at any level, or as either table of a `JOIN` whose `GROUP BY` columns and `ON` columns from that table omit one | Yes, naming the column. With every partition-key column in the group key, each group lives in exactly one partition, so a change to the tree touches only the groups routing to one node. | A sub-level added later whose partition key the group key (a `JOIN` table's `GROUP BY` and `ON` columns) doesn't hold pauses every transform that reads the source on the staging worker's next reconcile pass, with the column in `status()`'s `capture_failure`. `RESUME` refuses until the tree complies, then rebuilds. | Add the named column to the `GROUP BY`, or join on it. After a later change, detach the sub-level, then `RESUME`. |
| A partitioned root whose key level is an expression (no column to route by), whose partition-key column has a type other than `smallint`, `integer`, `bigint`, `numeric`, `text`, `varchar`, `uuid`, `date`, `timestamp`, `timestamptz` or `boolean`, a non-default operator class or a nondeterministic collation | Yes, naming the table and the column (for an expression, its key position). Trellis routes a group to its partition by evaluating the recorded bound over the group key, and it checks that only for these keys. | A sub-level added later with such a key pauses every transform that reads the source, as the row above describes. | Partition by a plain column of a listed type with the type's default operator class and a deterministic collation. After a later change, detach the sub-level, then `RESUME`. |
| A partitioned root with more than 1,000 leaves, or on PostgreSQL before 15 | Yes, stating the limit. No build statement prunes, so each one plans, locks and opens every leaf, and the default lock table runs out for one statement at about 5,500 leaves with the default `max_locks_per_transaction`. | A tree that grows past 1,000 leaves pauses every transform that reads it on the staging worker's next reconcile pass, with a `capture_failure` naming the limit. `RESUME` refuses until the tree has 1,000 or fewer. | Merge partitions to 1,000 or fewer leaves, or upgrade the server. |
| An expression nested more than 100 levels deep: a chain of more than 100 operands joined by operators, or nested parentheses or calls to the same depth. A flat list of many columns, as `COALESCE` arguments or terms of one sum, is not deep. | Yes (`ExpressionTooDeep`), for `CREATE`, `ALTER TRANSFORM` and a `JOIN`'s fields alike. Every pass that walks an expression recurses once per level, so the bound keeps a long expression from overflowing the stack. | n/a (the definition is stored only once it passes) | Split the expression into calculated fields that read each other. Fields that read each other are bounded by the next row. |
| Calculated fields that read each other so many times, or so deeply, that inlining them into one expression yields more than a build can render (a chain of fields that each read the previous one more than once), or an expression that is more than 100 levels high once the fields it reads are inlined (each read of another field costs at least one level) | Yes (`FieldExpansionTooLarge`). Every build reads each field as one self-contained expression. | n/a (the fields are fixed at define; an `ALTER TRANSFORM` is checked the same way) | Shorten the chain, or read the source columns in the later fields. |
| A partitioned root as a 1-1 transform's `FROM` | Yes. A 1-1 target over a partitioned source would need its own partitioning, and partitioned targets are not supported. | A 1-1 transform's source dropped and created again as a partitioned table pauses the readers the same way. | Aggregate over the partitioned table instead, or use a plain table. |
| A partitioned root of a servable shape (every row above passes) | No. Capture covers every node of the tree. The reconcile pass queues a correction for each partition attached, detached, dropped or truncated, and for groups in a target that route to no partition, and an aggregate recomputes the groups that route to the partition while it reports `backfilling`. A `JOIN` with the partitioned table on either side recomputes that side's partials that route to the partition, and carries the change into its target, while it reports `backfilling`. | After a dump and restore of the database, the next reconcile pass pauses every definition, those that read a partitioned source among them, with a `capture_failure` naming the restore, and runs no correction queued before it. A `pg_upgrade`, or the root dropped and recreated or swapped in by renames, is no restore. | After a restore, `RESUME`, which rebuilds. |
| An aggregate target, or a 1-1 target over one, used as a source (it has no primary key: its identity is a `UNIQUE NULLS NOT DISTINCT` constraint) | Yes, for another Trellis instance. Inside the owning instance it can be chained off, because writes to a target reach its readers in the writing transaction, never through capture triggers. | n/a | Group over a 1-1 transform, or chain within the instance ([instance-identity](instance-identity.md)). |
| Row-level security that applies to a role Trellis runs as on a source or on a target. Policies apply to a role when RLS is enabled and the role neither owns the table (directly or through membership) nor has `BYPASSRLS`, or owns it and the table is `FORCE ROW LEVEL SECURITY`. A superuser is exempt. Every Trellis connection runs with `row_security = off`, so a read or write the policies would filter fails rather than skipping rows. | Yes, checked for the role that owns the ring, and for the defining role on a table that is another transform's target. | For the ring's owner, the staging worker's reconcile pass pauses every transform that reads the table (or, for a target, the transform that writes it), with the reason in `status()`'s `capture_failure`, and `self_check` reports a `capture` divergence. For a role a drain logs in as, its refused read or write pauses the same transforms, and everything downstream of them, with `capture_failure` of kind `halt`. A refused build chunk pauses its transform after its charged retries, with the error on `backfill_failure`. On a partitioned source, define and the pass check the root only: a policy on a partition below it reaches none of the reads that follow writes, which all go through the root, but the correction of that partition after a `TRUNCATE` of it, an attach of it with rows, or direct writes into it as a new leaf reads the partition itself, is refused, and pauses the transform after its charged retries the same way. The refusals that only log or retry: [observability — A halting failure](observability.md#a-halting-failure). | Give the role `BYPASSRLS` (not inherited), or make it the table's owner without `FORCE ROW LEVEL SECURITY`; then `RESUME`. |
| A table a logical-replication subscription in the same database replicates into | Yes, in any sync state, including a disabled subscription. The subscription's apply worker fires only row-level triggers, so its changes never reach capture. Publishing a table to another database is fine. | Paused and reported as for RLS on the ring's owner. | Remove the table from the publication and `ALTER SUBSCRIPTION … REFRESH PUBLICATION`, or drop the subscription; then `RESUME`. |
| A nondeterministic collation (an ICU collation with `deterministic = false`) on a key column: a source primary key, a `GROUP BY` key or a `JOIN`'s `ON` column. Also on a column passed to `STRPOS` or `REGEXP_COUNT`, or compared as text (`=`, `<>`, `!=`, `IS [NOT] DISTINCT FROM`, `NULLIF`), directly or through `COALESCE`, a `CASE` result or another field. | Yes, for the definition (a `JOIN` checks the column of whichever table the read names), a `RESUME` and an `ALTER TRANSFORM` that adds or alters such a field. Trellis matches keys by exact text; such a collation's `=` would fold keys it keeps apart. `CHAR_LENGTH`, `OCTET_LENGTH` and `IS [NOT] NULL` accept any column. Such a collation's `=` would call two different texts equal, which Trellis compares exactly. | On a key column, the staging worker pauses every transform that keys by it, with the reason in `status()`'s `capture_failure`, and `RESUME` refuses until it's deterministic again. On a column only a text function reads, Postgres refuses the function, so each changed row's write fails: its key is held for the transform, and the transform is quarantined once its held keys reach the fuse threshold. | Keep the column's collation deterministic ([type-support](type-support.md#collation)). |
| A `REGEXP_COUNT` pattern that isn't a string literal, or that Postgres rejects (`(?P<n>a)`, `a++`, `\pL`) | Yes (`NonLiteralRegexPattern`; `InvalidRegexPattern` for a pattern Postgres rejects, carrying the server's message). Postgres alone judges the pattern: define, `ALTER TRANSFORM` and `RESUME` probe each literal with `regexp_count('', pattern)` and refuse on `invalid_regular_expression`. Back-references, lookahead and `\m`/`\M` are admitted. | Only a Postgres upgrade that changes the verdict: each changed row's write then fails with `invalid_regular_expression` and is quarantined like any row Postgres can't compute, and a `RESUME` probes again and refuses with the server's message. | Write a pattern Postgres's `regexp_count` accepts. |
| A comparison (`=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`, `IS [NOT] DISTINCT FROM`, and `NULLIF`'s `=`) with a float (`real`, `double precision`) on one side and an exact numeric (`smallint`, `integer`, `bigint` or `numeric`) column, calculated field or expression on the other, including a calculated field or `COALESCE` that is a float. A bare number literal against a float is admitted (below), so `f > 0.5` and `f = 1` define | Yes (`MixedFloatExactComparison`), naming the field and both operand types, for the definition and an `ALTER TRANSFORM`. Postgres casts the exact side to `double precision`, so the comparison inherits float rounding (`0.1::numeric + 0.2 = 0.3::float8` is false). A float against a float and an exact numeric against an exact numeric are admitted; `+` admits the mix. A bare literal outside `double precision`'s range (`22003` in Postgres) is refused too (`FloatComparisonLiteralOutOfRange`). | A column retyped into the other family (`integer` → `double precision`) pauses the transform on the staging worker's next capture pass, with the reason in `status()`'s `capture_failure`, and `RESUME` refuses while the mix remains (the row on a widened column below). | Compare values of one family: rewrite so both sides are floats or both are exact. A bare number literal is cast to `double precision` and a `real` operand widens to it, as in Postgres, so `r > 0.1` over `r = 0.1::real` is true; write `f > REAL '0.1'` to compare as `real`. |
| A constant Postgres folds at plan time that fails for every row: a `CASE` result, `AND`/`OR` operand or `NULLIF` argument that is one of the constants below, wherever the rest of the expression reads columns (`CASE WHEN x > 0 THEN 1.5 / 0 END` fails every row, even those where `x > 0` is false); a `COALESCE` whose result is a float (`real` or `double precision`) with a constant argument outside that float's range, such as `COALESCE(real_col, 1000000000000000000000000000000000000000)`; a `+`, `-` or `*` of two integer constants whose result does not fit `integer` or `bigint`, such as `2147483647 + 1` or `2147483647 * 2`; prefix `-` or `ABS` of the smallest integer (`-(-2147483647 - 1)`, `ABS(-2147483648)`); a `+`, `-`, `*` or `/` of two float constants that overflows the float, or that underflows to zero from nonzero operands (`DOUBLE PRECISION '1e-200' * DOUBLE PRECISION '1e-200'`); or an exact constant beside a float that is outside `double precision`; a `date` constant plus or minus an integer constant that leaves `date`'s range (4714-11-24 BC to 5874897-12-31), such as `DATE '2024-01-01' + 2147483647`; or a constant `date` past 294276-12-31 cast to `timestamp`. A constant is a number literal, a `REAL`, `DOUBLE PRECISION` or `DATE` typed literal, or a `+`, `-`, `*`, `/`, `%`, prefix `-`, `ABS`, widening `CAST` or calculated field made only of constants, or a `COALESCE` led by a constant (whatever follows it), wherever it appears (an aggregate's argument, a `JOIN` field, a `COALESCE` argument). A constant beside only exact `COALESCE` arguments unifies upward and is never out of range, and `numeric` arithmetic does not overflow. Postgres never evaluates a `COALESCE` argument after a constant one, a `CASE` result after a condition that is constant true or in a branch whose condition is constant false or `NULL`, or an `AND` operand after a constant false (an `OR` operand after a constant true), so `COALESCE(5, 2147483647 + 1)`, `CASE WHEN 1 = 1 THEN 1 ELSE 1.5 / 0 END` and `1 = 2 AND 1.5 / 0 = 1` define. Define folds `TRUE`, `FALSE`, `NULL` and comparisons of number, float, text, `date`, `timestamp`, `time` and `timetz` constants, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, `CASE`, `NULLIF` and `COALESCE` over them, and a comparison (other than `IS [NOT] DISTINCT FROM`), arithmetic operator or function call with a `NULL` constant operand is `NULL` whatever its other operands read, as the planner folds it (`CASE WHEN x > NULLIF(1, 1) THEN 2147483647 + 1 END` defines); where the planner may fold a `CASE` condition, `AND`/`OR` operand or `COALESCE` argument to a value define cannot tell, it refuses nothing after it (`COALESCE(NULLIF(STRPOS('', ''), NULL), 2147483647 + 1)` and `CASE WHEN STRPOS('a', 'a') = 2 AND x > 0 THEN 2147483647 + 1 END` define). An expression that reads a column is not a constant: `int_col + 2147483647` defines and fails only for the rows it overflows, which are quarantined. A function call over constants other than `ABS` (`CHAR_LENGTH('abc')`, `CEIL(DOUBLE PRECISION '1e+308') * 10`, `DATE_TRUNC('millennium', CAST(DATE '0001-01-01' - 1721426 AS timestamp))`) is folded by Postgres too, but define has no value for it, so the failing expression around it, or a `CASE` result, `AND`/`OR` operand or `COALESCE` argument it may select, defines and every page fails ([#1446](https://github.com/salesforce-misc/trellis/issues/1446)) | Yes (`CoalesceLiteralOutOfRange` for the `COALESCE`, `CaseLiteralOutOfRange` for a `CASE` of a float type, `ConstantOutOfRange` for the arithmetic or cast), naming the field, the expression as written and the type, for the definition and an `ALTER TRANSFORM`. | A column retyped so a constant falls out of range (`double precision` → `real` under a `COALESCE`, `numeric` → `double precision` beside a huge constant) pauses the transform on the staging worker's next capture pass, with the reason in `status()`'s `capture_failure`, and `RESUME` refuses while define would refuse the definition. | Use a constant within the type's range, or a column. |
| A constant zero divisor of `/` or `%`: a literal (`n / 0`, `n / 0.0`), a float zero (`f / REAL '0'`, `-0` included) or a constant that folds to zero (`n / (1 - 1)`, `i % (2 - 2)`), whatever the dividend. A `NaN` float constant divided by zero is `NaN` in Postgres and defines. Postgres raises `22012` for every row that has a dividend, or at plan time when the dividend is a constant too, so no row could be written. A divisor that reads a column defines, and `x / col` fails only for the rows where `col` is zero, which are quarantined. Postgres evaluates a `COALESCE` argument only for the rows the earlier ones leave `NULL`, a `CASE` result only where its condition holds and an `AND`/`OR` operand only where the earlier ones allow, but a zero divisor under one is refused too, unless a constant before it ends the `COALESCE`, `CASE`, `AND` or `OR` | Yes (`ConstantDivisorZero`), naming the field and the division as written, for the definition and an `ALTER TRANSFORM`. | n/a: a constant does not change. | Use a nonzero divisor, or a column. |
| A comparison between two different temporal types (`date` and `timestamp`, `timestamp` and `timestamptz`, `time` and `timetz`). Same-type comparisons of `date`, `timestamp`, `timestamptz`, `time` and `timetz` are admitted, as are `boolean` and `uuid` against their own type and exact numerics against each other | Yes (`MixedTemporalComparison`), naming both types, for the definition and an `ALTER TRANSFORM`. Postgres converts one side first, and a conversion to or from `timestamptz` depends on the session's time zone. | A column retyped to another temporal type pauses the transform, as in the rows above. | Cast one side so both are one type, or compare two columns of one type. |
| `<`, `<=`, `>`, `>=` over `text`. `=`, `<>`, `!=` and `IS [NOT] DISTINCT FROM` over text are admitted (under a deterministic collation, row above) | Yes (`TextOrderingComparison`). The order of text depends on the collation, which Trellis does not evaluate. | A column retyped to `text` pauses the transform, as in the rows above. | Order by a non-text value, or compare `CHAR_LENGTH` of the text. |
| A comparison or `NULLIF` between types no family covers: `text` with a number, `boolean` with a number, `uuid` with `text`, and every type outside the families (`bytea`, `jsonb`, `interval`, enums, `inet` and the rest) | Yes (`ComparisonTypeMismatch`), naming the operator and both types. | A column retyped into a type outside the family pauses the transform, as in the rows above. | Compare two values of one family; `IS NULL` takes any type. |
| Comparisons that chain: `a < b < c`, `a = b = c`, `a < b = c`, and `IS DISTINCT FROM` followed by another `IS` form. Postgres reads these as a syntax error | Yes (`NonAssociativeComparison`, a parse error). | n/a | Join them with `AND` (`a < b AND b < c`) or add parentheses. |
| `IS TRUE`, `IS NOT TRUE`, `IS FALSE`, `IS NOT FALSE`, `IS UNKNOWN`, `IS NOT UNKNOWN`; the simple `CASE <operand> WHEN <value> THEN ...` | Yes (`UnsupportedIsForm`, `SimpleCaseUnsupported`, parse errors). Only `IS [NOT] NULL` and `IS [NOT] DISTINCT FROM` are admitted, and only the searched `CASE`. | n/a | `x IS NOT DISTINCT FROM TRUE` is `x IS TRUE` (likewise `FALSE`, and `IS DISTINCT FROM` for the `NOT` forms), and `x IS NULL` is `x IS UNKNOWN`; write `CASE WHEN <operand> = <value> THEN ...`. |
| A `NULL` literal its context gives no type: both operands of an arithmetic operator (`NULL + NULL`), the operand of prefix `-` or of `AT TIME ZONE`, an operand beside a type that no operator define admits takes on both sides (`date_col + NULL`, which Postgres finds ambiguous; `ts_col - NULL`, which it reads as the `interval`-producing `timestamp - timestamp`), or an argument of a function or aggregate (`abs(NULL)`, `SUM(NULL)`, `COUNT(NULL)`), directly or through a field that is only `NULL`. Postgres finds some of these ambiguous and types the rest by its function-candidate rules (`abs(NULL)` is `double precision`), which define does not model. | Yes (`UntypedNull`), naming the field and the construct, for the definition and an `ALTER TRANSFORM`. | n/a: a literal does not change. | Write `CAST(NULL AS <type>)` with the type you mean (`abs(CAST(NULL AS integer))`). |
| A `CASE` condition that is not a `boolean`, or `CASE` results that Postgres cannot unify the way `COALESCE` does (`CASE WHEN c THEN 1 ELSE 'x' END`). Results unify to a common type: two integers the wider, an exact numeric with a float the float, otherwise `numeric`, so `CASE WHEN c THEN 1 ELSE 2.5 END` is `numeric` and `real` with `numeric` is `real` | Yes (`CaseConditionNotBoolean`, `CaseResultTypeMismatch`), for the definition and an `ALTER TRANSFORM`. | A column retyped so the results no longer unify pauses the transform, as in the rows above. | Make the results one type, with `CAST` where an integer meets a float. |
| `NULLIF(a, b)` where Postgres casts `a` to compare it, so the result is not `a`'s type: an integer `a` with a `numeric` `b` (the result is `numeric`), and an exact `a` with a float `b` (the result is `double precision`). `NULLIF(a, b)` otherwise has `a`'s type, for any pair `=` admits, and `x / NULLIF(y, 0)` guards a division | Yes (`NullIfFirstArgumentCast`), naming both types and the type Postgres gives. | A column retyped into such a pair pauses the transform, as in the rows above. | `NULLIF(CAST(a AS numeric), b)`, or write the float first. |
| An expression whose calculated fields inline to more than 100,000 nodes in total, a very wide `CASE` included (each `WHEN x = 1 THEN 1` branch is four nodes, so about 25,000 of them) | Yes (`FieldExpansionTooLarge`). | n/a | Shorten the expression or split it into calculated fields. |
| `/` between two integers (`smallint`, `integer`, `bigint`), such as `qty / 2` | Yes (`IntegerDivision`), naming both operand types. Postgres truncates an integer quotient (`7 / 2` is `3`), which reads as a rounding rule a `numeric` quotient does not have. `numeric / integer`, `integer / numeric`, `numeric / numeric` and every float quotient are admitted. | A column retyped from `numeric` to an integer type pauses the transform on the staging worker's next capture pass, with the reason in `status()`'s `capture_failure`, and `RESUME` refuses while define would refuse the definition. | Divide as `numeric`: `CAST(qty AS numeric) / 2`. |
| `%` with a float operand (`real`, `double precision`). `%` takes integers and `numeric`: two integers give the wider width, anything with a `numeric` gives `numeric`, and the sign follows the dividend (`-7 % 2` is `-1`) | Yes (`FloatModulo`). Postgres has no float modulo. | A column retyped into a float pauses the transform, as in the row above. | Use `numeric` operands. |
| An argument to `ABS`, `CEIL`, `FLOOR`, `ROUND` or `TRUNC` that the function does not admit. `ABS` takes an integer, `numeric` or float and returns its type. `CEIL`, `FLOOR` and `TRUNC` take `numeric` or `double precision`. `ROUND(a)`, `ROUND(a, n)` and `TRUNC(a, n)` take a `numeric` `a` and an `n` that is an integer literal (negative allowed, `ROUND(a, -2)`), and return `numeric`. Refused: an integer argument (it is already whole, and Postgres would cast it to `double precision` and change the result's type), a `real` argument to `CEIL`, `FLOOR` or `TRUNC` (promoted to `double precision`), a float argument to `ROUND` (a float rounds half to even, `numeric` half away from zero, so one name would carry two rules), a scale that is a column or an expression | Yes (`NumericFunctionArgument` with the reason, `ScaleNotIntegerLiteral`, or `FunctionArgTypeMismatch` for a non-numeric argument), naming the field and the function, for the definition and an `ALTER TRANSFORM`. | A column retyped into a refused type pauses the transform, as in the rows above. | Use the integer itself; use a `numeric` value, or `CAST(qty AS numeric)` for an integer one; write the scale as a literal. |
| A `CAST(<expr> AS <type>)` that is not an exact conversion that reads no setting: an integer to a strictly wider integer (`CAST(smallint_col AS integer)`, `AS bigint`), any integer to `numeric` and a `date` to `timestamp` are admitted. Refused: a narrowing (`bigint` to `integer`), a cast to the operand's own type, a cast from `numeric`, a float or a non-numeric type, a cast to or from a float, a `timestamptz` to `timestamp` (it converts through the session `TimeZone`), any other target spelling (`smallint`, `text`, `real`, `int2`, `dec`, `timestamptz`, `date`), and a modifier (`numeric(10, 2)`, `decimal(10, 2)`: it rounds and can overflow; `timestamp(3)`). The accepted target spellings are `integer` (or `int`, `int4`), `bigint` (or `int8`), `numeric` (or `decimal`) and `timestamp`, in any case, in the target position only: a column named `int` is still a column. A `date` past 294276-12-31 is a value `timestamp` cannot hold: its row raises `22008` and is quarantined. An alias is typed and checked as its canonical target (`CAST(bigint_col AS int)` narrows, so it is refused), and `ALTER TRANSFORM` shows the canonical spelling | Yes (`UnsupportedCast` naming both types for an operand that does not convert; `TemporalOperand` for a `timestamptz`, with `AT TIME ZONE 'UTC'` as the remedy; `UnsupportedCastTarget` for a target or a modifier the grammar has no spelling for), for the definition and an `ALTER TRANSFORM`. The cast of a quoted literal (`CAST('1.5' AS real)`) is a typed literal, with its own rules. | A column retyped so its cast no longer converts (`integer` → `numeric`) pauses the transform, as in the rows above. | Widen the column instead, or compute in `numeric`; for a `timestamptz`, `tz AT TIME ZONE 'UTC'`. |
| `+` or `-` over a date or time operand other than `date ± integer`, `integer + date` and `date - date`: a `date` with a `bigint`, `numeric` or float (`d + n`), `date + date`, a number minus a date, a difference of two timestamps or of a date and a timestamp (`ts2 - ts1`, `tz2 - tz1`), any operand of type `interval`, and a number added to a `timestamp`, `timestamptz`, `time` or `timetz`. `date ± integer` takes a `smallint` or an `integer` count of days | Yes (`TemporalOperand`), naming the operator, the operand type and the reason, for the definition and an `ALTER TRANSFORM`. Postgres has no `date + bigint` or `date + numeric`. A difference of timestamps is an `interval`, which has no literal and whose month part makes a result depend on the date it is added to (`'2024-01-31' + 1 month - 1 month` is not `'2024-01-31'`), so nothing here produces or consumes one. | A column retyped into an unadmitted pair (`integer` → `bigint` beside a `date`) pauses the transform, as in the rows above. | Use an `integer` or `smallint` day count. For a duration, `EXTRACT(EPOCH FROM b) - EXTRACT(EPOCH FROM a)` is exact `numeric` seconds. |
| `DATE_TRUNC` of a `date` or a `timestamptz`, or `EXTRACT` of a `timestamptz`, `time`, `interval` or any type but `date` and `timestamp`; `date_part` | Yes (`TemporalOperand` for the first two; `date_part` is an unsupported function). `DATE_TRUNC('month', d)` resolves to the `timestamptz` overload, which is `STABLE` and returns `timestamptz`; both functions on a `timestamptz` read the session `TimeZone`; `date_part` returns `double precision`. `EXTRACT` returns an unconstrained `numeric`, so `SUM` or `AVG` of one is refused for its undeclared scale; `COUNT` of one, and grouping by a bucket through a chain (a 1-1 transform computes `DATE_TRUNC('month', ts) AS bucket`, an aggregate over that target uses `GROUP BY bucket`), define. | A column retyped into a refused type pauses the transform, as in the rows above. | `DATE_TRUNC('month', CAST(d AS timestamp))`; `DATE_TRUNC('day', tz AT TIME ZONE 'UTC')`; `EXTRACT(hour FROM tz AT TIME ZONE 'UTC')`; `EXTRACT` for `date_part`. |
| A `DATE_TRUNC` unit or an `EXTRACT` field that is not admitted, or that is not a literal. Units: `microseconds`, `milliseconds`, `second`, `minute`, `hour`, `day`, `week` (truncates to Monday), `month`, `quarter`, `year`, `decade`, `century`, `millennium`, in any case. Fields of a `date`: `year`, `quarter`, `month`, `week`, `day`, `dow`, `isodow`, `doy`, `isoyear`, `decade`, `century`, `millennium`, `epoch`; a `timestamp` also has `hour`, `minute`, `second`, `milliseconds`, `microseconds`. Refused: `EXTRACT(hour FROM <date>)`, `julian`, the `timezone` fields, abbreviations and plurals (`y`, `days`, `mon`) and a unit given as a column | Yes (`TemporalLiteral`), naming the construct, the text and what is admitted. A field Postgres lacks for the type raises in it; `julian` counts from a different origin for each type; the others have one spelling here. | A column retyped does not change a literal. | Use a listed spelling. |
| `AT TIME ZONE` with a zone other than the string literal `'UTC'` (`'America/New_York'`, `'utc'`, `'+05:00'`, a column), or over anything but a `timestamptz`: a `timestamp` gives a `timestamptz`, and a `date` converts through the session `TimeZone` | Yes (`TemporalLiteral` for the zone, `TemporalOperand` for the operand). Another zone's offsets come from the time zone database, which a release changes under past results, and `AT TIME ZONE` on a `timestamp` produces a `timestamptz`, which nothing here needs. `'UTC'` is parsed under `timezone_abbreviations`, which Trellis's sessions pin to `Default`. | A column retyped to another type pauses the transform, as in the rows above. | Convert in the source, or `CAST(d AS timestamp)` for a date. |
| A key column (a source key, a `GROUP BY` key or a `JOIN`'s `ON` column) retyped so that the values Trellis stored render differently: `timestamp` ↔ `timestamptz`, `date` → `timestamp`, `text` → `uuid` or `integer`, `varchar` → `character(n)`, or a narrower `numeric` scale, temporal precision or `varchar(n)` (the rewrite rounds, or strips trailing spaces) | n/a: define sees one type. | An `ALTER COLUMN … TYPE` rewrites the table without firing a trigger. The staging worker pauses every transform keyed by the column, with the old and new type in `capture_failure`. | `RESUME`: it re-types Trellis's copies of the column and rebuilds from the source. Or drop and define again. |
| A column Trellis creates a column from with its type, widened. Those columns are a 1-1 target's key, passthrough and calculated columns, an aggregate's `GROUP BY` key (copied into its target and its hidden table) and its field columns, a `JOIN`'s `GROUP BY` keys and `ON` columns (copied into its target and its hidden tables, each with its type, no modifier, and collation) and its field columns, and a 1-1 `JOIN`'s key, field columns and the columns of its hidden `R`, `M` and `J`. Widenings are `integer` → `bigint` (and `smallint` up), an integer → `numeric` (unless every value of the `numeric` fits the integer), `real` → `double precision`. A wider modifier is no widening: a copy has none ([ADR-0020](decisions/0020-type-identity.md)). | n/a: each column takes its type from the source at define, or when an `ALTER TRANSFORM … ADD` adds it. A column Trellis creates has the source column's type with no modifier (a 1-1 key column and a `JOIN`'s `GROUP BY` and `ON` columns also keep its collation; any other takes the database default), so a change of a modifier (a string bound, a `numeric` precision, a temporal precision, a move to `text`) changes nothing there. | A widening of a modifier that changes only the catalog needs no re-type, no pause and no rebuild. Otherwise the column keeps its type, so a value it can't hold would fail its write. The staging worker pauses every transform that owns a column the type define would give it now outgrew (a 1-1 target's copy of `qty` after `qty` goes `integer` → `bigint`), naming the source column and each of those columns. A narrowing pauses nothing here, except a `varchar(n)` key's (the row above). A narrowing, a field's move to another type family (`integer` → `double precision`), or any other change that isn't a pure widening, rebuilds the table's readers on its own, and pauses a transform define would now refuse, one that evaluates the column as another type, or one a `numeric` losing its scale invalidates (a `SUM`) ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#a-column-changed-or-rewritten-without-a-trigger)). | `RESUME`: the staging worker `ALTER`s each column whose type differs from the one define would give it now, under `ACCESS EXCLUSIVE` (a table rewrite for `integer` → `bigint`; reads of the target wait), then rebuilds. A `JOIN`'s or a 1-1 `JOIN`'s hidden tables are created again at the new types instead. Or drop and define again. |
| Two `GROUP BY` keys that share a target column name (a key listed twice, or a `JOIN`'s `orders.region` and `customers.region`); keys can't be aliased | Yes (`GroupByKeysShareTargetColumn`). Each key becomes a target column named after its bare column name. | n/a | List a key once. For a `JOIN`, join a 1-1 transform's target that selects one side's column under another name (`region AS customer_region`). |
| A key of more than 32 columns: a `GROUP BY` of more than 32 columns, or a `JOIN` side whose `ON` columns and `GROUP BY` columns not among them together number more than 32 (a 1-1 `JOIN` has no `GROUP BY`, so its `ON` alone). Postgres caps an index at 32 columns (`INDEX_MAX_KEYS`) and Trellis keys the target and a `JOIN`'s hidden tables on these columns. | Yes (`IndexKeyTooWide`, naming the clause and the limit). A source key is at most 32 columns too, so a 1-1 target's key never exceeds it. | n/a | Group by or join on fewer columns. |
| A table of more than 1,600 columns, Postgres's cap on one table (`MaxHeapAttributeNumber`). Trellis creates a target with a column per key and field, and hidden tables whose width follows the fields too: an aggregate's hidden table has its `GROUP BY` columns, a column counting each distinct argument and a column summing each one a `SUM` or `AVG` reads; a `JOIN`'s hidden partials tables do the same, per side and for the groups; a 1-1 `JOIN`'s ledger for a side has a column for every column that side's fields read. So about 800 distinct summed aggregate arguments overflow a hidden table that their target fits in. | Yes (`TableTooWide`, naming the table's role and the limit), for the definition, `RESUME` and an `ALTER TRANSFORM … ADD`. An `ALTER TRANSFORM` counts the columns earlier edits dropped from the target, and the column a `DROP z, ADD … AS z` replaces, since Postgres keeps counting a dropped column until the table is created again. An aggregate meets smaller limits first, which define does not refuse: from about 270 distinct summed arguments the statements that build and apply it exceed Postgres's 1,664-entry target list, so its build pauses or its changed groups are quarantined, with Postgres's error ([#1518](https://github.com/salesforce-misc/trellis/issues/1518)), and from about 500 a hidden table's row can outgrow a heap page ([#1516](https://github.com/salesforce-misc/trellis/issues/1516)). | n/a: the tables are created with the definition, and only an `ALTER TRANSFORM … ADD` of a 1-1 transform adds a column. | Define fewer fields, or fewer distinct aggregate arguments. If dropped fields filled the limit, drop the transform and define it again. |
| A field named after a 1-1 source key column (`id AS id`), or any field or `GROUP BY` column whose name starts with `__` | Yes. The target already carries the key columns, and `__` names are reserved for Trellis's hidden columns. `id AS order_id` is an ordinary column. | n/a | Rename the field. |
| An aggregate whose argument names another aggregate field when no source column has that name (`SUM(val) AS total, SUM(total)`), directly or through a field that reads one | Yes (`AggregateOfAggregateField`). The argument would nest one aggregate in another, which no build can run. | n/a | Aggregate the source column instead (`SUM(val)`). |
| A bare name that could mean a source column or a field: the name of a source column and of a field whose expression is not just that column, read by another field (`(grp + 1) AS val, SUM(val) AS total`). An aggregate field's name inside another aggregate's argument is not ambiguous: it is the source column ([Calculated Fields](#calculated-fields)). | Yes (`AmbiguousColumnReference`), naming both meanings. `val AS val` (or `src.val AS val`) passes the column through, so reading `val` elsewhere is not ambiguous. | A source column added later under the name of a field: nothing is detected, and the transform keeps reading the field. An `ALTER TRANSFORM` that adds a field of that name is refused, as define would. `RESUME` re-validates and refuses what define would: in an aggregate target, a field another field reads by that name, and in a 1-1 target, any field of that name. | At define, write `src.val` or `public.src.val` to read the source column, or rename the field and read it by its new name. After a later column, rename or drop that column and `RESUME`, or drop the transform and define it again. |
| A bare name in an expression that Postgres reserves: any word `pg_get_keywords()` marks reserved (`R`) or reserved as a type or function name (`T`), such as `order`, `user`, `end`, `not`, `from`, `left`, `case`, `and`, `is`. Postgres needs such a column quoted, and this grammar has no quoted identifiers. A name after a `.` (`t.order`), the label after `AS` (`val AS limit`) and a call (`left(x, 2)`) are not bare names and are fine; a non-reserved keyword (`extract`, `position`, `values`, `between`, `coalesce`, `nullif`, `char`) is still a column. A field label that is a reserved word can't be read by another field, for the same reason. | Yes (`ReservedWord`), for the definition and an `ALTER TRANSFORM`, naming the word and saying to qualify or rename it. A source table with a column of that name defines; an expression reads it qualified. | Adding or renaming a column to a reserved word needs quotes in Postgres; it is a rename like any other. A transform that reads the old name pauses, and one that doesn't read the column is unaffected. | Qualify the source column with its table (`orders.order`, which Postgres accepts unquoted too; `orders.order AS order` copies it under its own name), or rename the column or field. |
| A qualified name that doesn't name the `FROM` table: a two-part name whose head is another table (`product.cost`), or a three-part name whose schema isn't the `FROM` table's. A `JOIN`'s are in the `JOIN` rows below. | Yes (`QualifiedColumnNotFromSource`; a `GROUP BY` key is a parse error, `GroupByKeyNotFromSource`). `ALTER TRANSFORM` refuses it in an added or altered field. | n/a | Name the `FROM` table, or read the second table with a `JOIN`. |
| A `GROUP BY` aggregate or a `JOIN` whose source lacks the index its `self_check` walk pages by: for an aggregate, a valid, non-partial btree index on the plain source columns whose leading columns are the `GROUP BY` columns, in `GROUP BY` order; for a `JOIN`, on each side, one whose leading columns are that side's `ON` columns in `ON` order. A primary key or unique constraint that leads with them counts; an expression or partial index, one under a non-default operator class such as `text_pattern_ops`, or one whose leading columns are not all ascending with `NULLS LAST` (the default) or all `DESC` with `NULLS FIRST` (the `DESC` default, read backward), such as `(a, b DESC)`, doesn't. A 1-1 target needs nothing (its walk pages by the primary key), and a `JOIN`'s `T` part, with group columns from both tables, is served by no one table's index and needs none | Yes (`SourceIndexMissing`), naming the table, the columns, the part of the definition that needs the index (the `GROUP BY` columns, or a `JOIN`'s `ON` columns on its from or joined side) and a ready-to-run `create index`. `WITHOUT SOURCE INDEX` after the `GROUP BY` or `JOIN` clause (`TRANSFORM t FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT …`) opts out for that definition. Builds need no index. | Dropping or invalidating the index later pauses nothing, since it affects speed, never correctness. `status()` reports it in `source_index_gaps` (an opted-out definition's gap is listed too), `self_check` refuses to walk a definition that did not opt out, and a whole-transform `RESUME` refuses with the define message. An opted-out definition's `self_check` still runs, scanning the source once per page. | Create the index the message names; or define it `WITHOUT SOURCE INDEX`. |
| A `JOIN` transform (`FROM a [LEFT\|INNER] JOIN b ON … GROUP BY … SELECT …`) that passes the checks in the rows below | No. Define registers it and creates its target, and Trellis builds it from both tables. Each table is held to the rows above, as a `FROM` is. Either table may be another transform's target, read through that target's change feed; the build first waits out the transactions that began before it ([data flow](data-flow.md#the-re-derive-build), [ADR-0019](decisions/0019-join-transforms.md#a-side-may-be-another-transforms-target-through-the-target-change-feed)). | A column the transform reads from either table dropped or renamed, or a key column of either (its primary key, an `ON` or a `GROUP BY` column) retyped as the key rows above describe, pauses the whole transform on the staging worker's next pass, naming that table and column in `status()`'s `capture_failure`, and so does a column of either table retyped to a type the transform evaluates differently. A widening of a column it copies is handled as the widening row above describes. Either table dropped or renamed pauses it, as the row for a dropped source describes. `RESUME` checks both tables and refuses, naming the table, while define would refuse the transform. | After a later change, undo it, then `RESUME`. |
| A `JOIN` that is `RIGHT`, `FULL`, `CROSS`, `NATURAL`, `USING` or a comma join; joins a table to itself, or two tables with the same bare name (`shop.orders JOIN archive.orders`); joins a third table; has no `GROUP BY` (that is a 1-1 `JOIN`, in the next row); has an `ON` that is anything but equalities between a column of each table joined by `AND` (`OR`, `<`, a literal, an expression, two columns of one table, a column pair repeated, a column named by two terms); reads a column without its table (`SUM(qty)`, `GROUP BY region`) or through a table that is neither side; has an aggregate whose argument reads both tables (`SUM(orders.x + items.y)`); or has a field that is neither a `GROUP BY` key selected on its own nor a composition of aggregates (a column outside an aggregate, a nested aggregate, no aggregate at all) | Yes (parse error, or `OneToOneJoinDisabled` for no `GROUP BY`, `JoinSelf`, `JoinOnSameSide`, `JoinOnRepeatedPair`, `JoinOnRepeatedColumn`, `JoinUnqualifiedColumn`, `JoinUnknownTable`, `JoinUnresolvedColumn`, `JoinMixedSideArgument`, `JoinFieldShape`). Only `INNER` and `LEFT` equi-joins of two different tables are admitted, with every column written `<table>.<column>`. A field is a `GROUP BY` key from either table, or composes aggregates with literals, operators and functions (`COALESCE(SUM(items.qty), 0)`); each aggregate is `COUNT(*)`, or `COUNT`/`SUM`/`AVG` of a column or an expression over one table's columns (`SUM(items.qty + 1)`). Each argument is kept as a running total on its own table, so one reading both tables has no such total. | n/a | Rewrite the definition in the admitted shape. For `RIGHT JOIN`, swap the two tables and use `LEFT JOIN`. |
| A 1-1 `JOIN` (`FROM a [LEFT\|INNER] JOIN b ON … SELECT …`, no `GROUP BY`, [ADR-0021](decisions/0021-one-to-one-join-transforms.md)): any definition while the development knob `TRELLIS_DEV_ONE_TO_ONE_JOIN` is off; and, with it on, one whose `b` `ON` columns are not unique, that has an aggregate field, that reads a column that starts with `__`, whose side is partitioned, whose `ON` names one column in two terms, or whose `b` is another transform's 1-1 target joined on columns that leave out part of that target's primary key; or any transform that reads a 1-1 `JOIN`'s target | Yes (`OneToOneJoinDisabled`, `OneToOneJoinKeyNotUnique`, `OneToOneJoinAggregate`, `OneToOneJoinUnsupported`, `OneToOneJoinTargetRead`, `PartitionedSource`, `JoinOnRepeatedColumn`; a `WITHOUT SOURCE INDEX` is a parse error). With the knob on, `b`'s `ON` columns must contain a unique key of `b`: its primary key, a unique constraint or a non-partial unique index on plain columns (deferrable, or with `INCLUDE` columns, is fine), or the group key of an aggregate or `JOIN` target. When `b` is another transform's 1-1 target, its `ON` columns must also include every column of its primary key, so a unique index added to the target never admits a join on another column: Trellis orders changes per primary-key value only, so only the primary key is unique at every instant while the upstream drains. An `a` that is a 1-1 target may be joined on any columns. A field is any expression over `<table>.<column>` columns of either table. The target is keyed by `a`'s primary key. The joined table needs no source index. With the knob on, Trellis builds the target in chunks of `a`'s primary key, keeps it readable across a `RESUME`, and applies changes, Re-derives and `TRUNCATE`s of either table. A `TRUNCATE` of `a` empties the target; one of `b` starts a rebuild of it, so the transform reads `backfilling`, and a row keeps the `b` columns it held (an `INNER` row stays) until the rebuild reaches it. A change of `b` whose recompute of the `a` rows on its key would pass `TRELLIS_ONE_TO_ONE_JOIN_MAX_FANOUT` rows (5,000,000 by default), or fails on more of them than its bisection can name, pauses the transform, and everything downstream of it, with a `capture_failure` of kind `halt`. A row of `a` that recompute fails on is otherwise held in quarantine, as a row whose own change fails is, and the change applies to every other row. | As for a `JOIN` (the row above): a column the transform reads from either table dropped or renamed, a key column of either (`a`'s primary key, an `ON` column) retyped as the key rows above describe, or a column retyped to a type the transform evaluates differently, pauses the whole transform, naming that table and column in `status()`'s `capture_failure`. A widening of a column it copies (`a`'s key, a field, `R`, `M` and `J`) is handled as the widening row above describes; a field over both tables is reported by the pass of whichever table finds it, naming every column it is typed from. Either table dropped, renamed or joining a hierarchy pauses it, as for a source. `RESUME` refuses a `b` whose `ON` columns are no longer unique, and a changed key of `a`, naming the table; nothing checks `b`'s unique key while the transform runs ([known correctness gaps](known-correctness-gaps.md#bs-unique-key-dropped-under-a-1-1-join)). It also refuses any transform that would then read a 1-1 `JOIN`'s target (a source dropped while it was paused, whose name a 1-1 `JOIN`'s target has since taken). | Join `b` on a unique key (a 1-1 target's, on columns that include its primary key, or join the table that transform reads), give an aggregate a `GROUP BY`, and read the 1-1 `JOIN`'s sources rather than its target. After a later change, undo it, then `RESUME`; a widening needs only the `RESUME`. |
| A `JOIN` whose `ON` columns differ in type or collation, are off the join-key allowlist, or have a nondeterministic collation; or a `GROUP BY` key of a type that can't be one | Yes (`JoinKeyTypeMismatch`, `JoinUnsupportedKeyType`, `JoinNondeterministicCollation`, `UnsupportedGroupByKeyType`). Each `ON` column's type must be on the join-key allowlist in [type-support.md](type-support.md) (a domain never is), with a deterministic collation. The two columns of each pair need the same type and the same collation, because Trellis never casts a join key. A type modifier never counts, and `text` and `varchar` are one type: `varchar(10)` pairs with `varchar(50)`, `text` or a Trellis target's copy, and `timestamp(0)` with `timestamp(3)`, while `integer` against `bigint` is refused. A `GROUP BY` key follows the `GROUP BY` key rules. | An `ON` column retyped to another type (`integer` to `bigint`) so its pair no longer matches, or onto a refused type or a nondeterministic collation, pauses the whole transform on the staging worker's next pass, naming that table and column in `status()`'s `capture_failure`. A change of one column's modifier leaves the pair matching, so a widening pauses nothing; a change that re-renders the keys Trellis stored (a narrower bound or precision) pauses the transform, and `RESUME` rebuilds it. A change between deterministic collations pauses nothing (byte equality is unchanged), but `RESUME` refuses the pair until the collations match too. | Alter one column to match the other, then `RESUME`. |
| `MIN` or `MAX` in an aggregate or a `JOIN` transform | Yes (`DeferredAggregate`, family `MIN/MAX`). Trellis keeps only the aggregates it can keep exact from each change alone; a group's minimum or maximum needs the source read again when its current one is deleted. | n/a | Not supported yet: [#1117](https://github.com/salesforce-misc/trellis/issues/1117). |
| `bool_and`, `bool_or`, `bit_and` or `bit_or` in an aggregate or a `JOIN` transform | Yes (`DeferredAggregate`, family `bool_and/bool_or/bit_and/bit_or`). | n/a | Not supported yet: [#1118](https://github.com/salesforce-misc/trellis/issues/1118). |
| `SUM` or `AVG` over a float (`real`, `double precision`), an `interval`, or any type but `smallint`, `integer`, `bigint` and `numeric`, in an aggregate or a `JOIN` transform | Yes (`DeferredAggregate`, family `float and interval SUM/AVG`, for a float or an `interval`; another type is refused naming its type). A running float sum drifts from Postgres's, which adds the group's values in scan order. | The argument column re-typed to a float (`integer` → `double precision`) pauses the transform on the staging worker's next pass, naming the column in `status()`'s `capture_failure`; `RESUME` refuses with define's error until the type is admitted again. | Sum an integer or `numeric(p,s)` column. Not supported yet: [#1119](https://github.com/salesforce-misc/trellis/issues/1119). |
| `array_agg`, `string_agg` or `jsonb_agg` in an aggregate or a `JOIN` transform | Yes (`DeferredAggregate`, family `array_agg/string_agg/jsonb_agg`). | n/a | Not supported yet: [#1040](https://github.com/salesforce-misc/trellis/issues/1040). |
| `SUM` or `AVG` over an unconstrained `numeric` column, a `numeric` column of a table Trellis creates (a transform's target: Trellis creates its columns with no modifier, so the column has no scale whatever the scale it copies), or an expression whose result is `numeric`, in an aggregate or a `JOIN` transform | Yes (`UnconstrainedNumericAggregate`, `ChainedNumericAggregate` for a target's column). Postgres shows a sum with as many decimal places as the most precise value in the group, which a running sum can't track; a declared scale removes the mismatch, and an expression's `numeric` result has none. | The argument column re-typed to an unconstrained `numeric`. In a source, the staging worker's next pass pauses the transform, naming the column in `status()`'s `capture_failure`. In a 1-1 target, where Trellis makes the re-type: a `RESUME` of the upstream that does it (its source column moved from `integer` to `numeric`) pauses the reader with the refusal and the re-type as its `capture_failure`. The reader's own `RESUME` refuses until the argument has a scale again, which a target's column never has: drop the reader. A change of scale between two `numeric(p,s)` types pauses nothing: the staging worker's next pass rebuilds the readers. | Sum the `numeric(p,s)` column itself, declare a scale (`ALTER TABLE … ALTER COLUMN … TYPE numeric(p,s)`), sum the source's column in the transform that reads the source, or wait for [#1120](https://github.com/salesforce-misc/trellis/issues/1120). |
| A `WHERE` other than `TRUE` | Yes (parse error). Partial data is not supported ([#804](https://github.com/salesforce-misc/trellis/issues/804)). | n/a | None. |
| Chaining off a transform that is not `live` (`TransformNotLive`) | Yes. | n/a | Wait for the upstream to go `live`, then define. |
| A target whose name is taken: a relation of any kind already exists in the target schema under the target's name or the name of a table Trellis keeps beside it (`<target>__ledger`). That includes another definition's target, in this instance or in another instance sharing the target schema. | Yes (`TargetTableExists`, a `conflict` error). Trellis writes only to target tables it creates, and `DROP TRANSFORM` drops the target, so it never takes over an existing relation. | n/a | Choose another target name, give the instance its own target schema ([instance-identity](instance-identity.md)), or drop or rename the existing relation. |
| A target in a Trellis catalog schema: `TRANSFORM <schema>.<target>` naming this instance's catalog schema, or a schema holding another instance's `trellis_instance` table (matched by schema name exactly as written, and by a table of that name, even one the role can't read). The target and its ledger would live there, and the other instance's uninstall (`drop schema … cascade`) would drop them. | Yes (`TargetInCatalogSchema`, a `validation` error), before anything is created. An unqualified target takes `Config::target_schema`, which an attach already refuses to be either ([instance-identity](instance-identity.md#a-catalog-schema-that-belongs-to-another-instance)). | A target schema that gains a `trellis_instance` table after define (another instance attached to it) pauses nothing, and the transform keeps running. `RESUME` re-validates and refuses with the same error, leaving the transform paused. | Put the target in an ordinary schema (`TRANSFORM reporting.t`). After define, `DROP TRANSFORM` it (paused) and define it again in an ordinary schema: removing the other instance's catalog from the schema would drop the target with it. |

A refusal after define is a pause, not a loss: `RESUME` rebuilds the target from
source once the condition is fixed ([Status](#status)). `status()`'s
`capture_failure` carries the reason. Every `RESUME`, of a transform or of one
field, first runs define's validation against the live schema, and refuses,
leaving the transform paused, while define would refuse it; the error names the
column and what to change. A resume also refuses while the target's own tables
are no longer what define would create (a 1-1 target whose source key was
redefined, or whose ledger table is missing; [operations — `PAUSE` and `RESUME`](operations.md#pause-and-resume)), and
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
the rest of any build then scans the source and `self_check` reads the source
by scan rather than by index (it pages under the target key's collation).

### Partitioned sources

A declaratively partitioned table (`RANGE`, `LIST` or `HASH`, at any depth, with
or without a `DEFAULT`) is a source on PostgreSQL 15 or later, with up to 1,000
leaves: the `FROM` of an aggregate or either table of a `JOIN`. A target is always a plain table. Define accepts it
when the group key holds every partition-key column at every level: an
aggregate's `GROUP BY`, or a `JOIN` table's `GROUP BY` and `ON` columns from
that table. Each group then lives in exactly one
partition, so a change to the tree touches only the groups that route to one
partition, and the catalog says which those are. A fact table partitioned on
the column it joins by needs nothing more in its `GROUP BY`, while one
partitioned on its own id is refused. Everything else is refused at define or
pauses a definition later, as the table above lists
([ADR-0017](decisions/0017-partitioned-sources.md)).

```text
-- order_line_items is partitioned BY HASH (order_id)
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  COUNT(*) AS line_count,
  SUM(amount) AS total_amount
```

Every partition carries the capture triggers, so a write named on the parent or
on any partition is counted once, and an `UPDATE` through the parent that moves
a row to another partition counts as a delete and an insert. A write to a table
that has left the tree counts for nothing.

Partition maintenance needs no operator step. The staging worker's reconcile
pass (every 5 seconds by default) detects each change to the tree, installs
capture on a new partition, and queues a correction for the partition the
change touched, which recomputes the groups that route to it. A `DEFAULT`
whose siblings changed is corrected like a new partition. Until the pass sees
the DDL, at most one pass, the definition still reads `live`; from then until
the correction has drained it reads `backfilling`, with the partition in
`correcting` ([Status](#status)). From the DDL to the end of the correction,
the **window**, counts are wrong in one direction:

| Operation | Window | Direction | Avoiding the window |
|---|---|---|---|
| `ATTACH PARTITION` of a table that holds rows | At most one pass, plus the correction of that partition. | Rows added: their groups are undercounted. | Create the partition empty and move the rows with DML through the parent: about 4-5 s and 0.5-0.6 GB of WAL per 1M-row partition. |
| A new partition written to directly before the pass installs capture on it | At most one pass, plus the correction of that partition. | Rows added: undercounted. | Create the partition empty and write through the parent. That has no window and costs nothing. |
| `DETACH PARTITION` (also `CONCURRENTLY`, whose correction starts once the detach completes) or `DROP` of a partition | At most one pass, plus the correction of that partition, routed by the partition's recorded bound. | Rows removed: their groups are overcounted. | Delete the partition's rows with DML through the parent first. |
| `TRUNCATE` of a partition or of a middle partitioned table | At most one pass, plus the correction of that partition. | Rows removed: overcounted. | Delete the rows with DML through the parent first. |
| Groups that route to no current partition, with no `DEFAULT` to take them (found by the pass's sweep of the bounds' complement) | At most one pass, plus the correction. | Rows removed: overcounted. | The operations above. |
| `INSERT`, `UPDATE` and `DELETE` through the parent or straight to a partition, including an `UPDATE` that moves a row to another partition | None. | n/a | n/a |

A `TRUNCATE` of the parent clears the target as it does for a plain table.
The cases that leave a target stale with no correction are in
[known correctness gaps](known-correctness-gaps.md#a-merge-that-moves-a-row-to-another-partition)
and the entries after it.
What a correction
costs, and what a dump and restore, a `pg_upgrade` and pg_partman do, is in
[operations — Partitioned sources](operations.md#partitioned-sources).

## Granularity

Granularity determines the target's primary-key space — what a single target row
represents relative to its source row(s). The grammar has three: 1-1, aggregate and join aggregate.

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
wrapped in exactly one aggregate: `COUNT(*)`, which counts the rows in the
group, `COUNT(<expr>)`, which counts the rows where `<expr>` isn't null, and
`SUM` and `AVG` over `smallint`, `integer`, `bigint` or `numeric(p,s)`. An
argument may be a column or an expression over the row (`SUM(qty + 1)`), and a
field may compose aggregates (`COALESCE(SUM(amount), 0) + COUNT(*)`). These are
the aggregates Trellis keeps exact from each change alone, without reading the
source again. Every other aggregate is refused at define, named by its family
([Supported sources and targets](#supported-sources-and-targets)). Result
types match Postgres's: `COUNT` is `bigint`, `SUM` over `smallint` or `integer`
is `bigint`, and `SUM` over `bigint` or `numeric`, and `AVG`, are `numeric`.

Each key becomes a target column named after its bare column, and keys can't be
aliased. The target holds exactly the declared columns: the state that keeps
it exact (each group's row count and running sums) lives in a table Trellis
keeps in its own catalog schema, named by the transform's internal id, which
readers get no grant on. A group has a row in the target only while it has
rows in the source. Name limits are in
[Supported sources and targets](#supported-sources-and-targets).

```
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  COUNT(*) AS line_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount
```

**Throughput falls as more distinct groups receive writes at once.** A page
locks every group it touches until it commits, so drain workers whose pages
share groups queue behind each other for those rows. On a disk-backed
benchmark (`benchmark group-contention`, 8 writer threads on a 16-core box), a
single `SUM`/`COUNT(*)` aggregate applied about 233k to 308k source rows/sec
over 10 to 400 groups, 190k to 200k over 4,000 and 75k to 90k over 40,000.
Throughput rises with writer threads up to about 8 (about 70k rows/sec with
one, 230k with eight) and stays flat beyond that. If you're planning an
aggregate with tens of thousands of groups written concurrently, benchmark it
on your own storage first (`benchmark group-contention --groups <n>`).

Values read before `await_converged` may be inconsistent, and are exact after
it; on small or hot groups that can include a combination no source state
produces, such as a negative `COUNT(x)`. See
[Reading derived data](data-flow.md#reading-derived-data).

### Join aggregate

An aggregate over the join of two tables (`FROM a [LEFT|INNER] JOIN b ON …
GROUP BY …`). Its target holds one row per group: a column per `GROUP BY` key
and per field, and nothing else. The state that keeps it exact lives in tables
Trellis keeps in its own catalog schema, named by the transform's internal id,
which readers get no grant on. Define registers it and creates its target, and
Trellis builds it from both tables, then applies every change captured on
either. Its aggregates are a plain aggregate's, each argument reading one
table's columns.
[Supported sources and targets](#supported-sources-and-targets) lists the
checks.

Both tables are read the way a `FROM` is: each is captured, and a column the
transform reads that either table loses, or a key column either retypes,
pauses the whole transform on the staging worker's next pass, naming that
table. `RESUME` checks both tables again.

The same read contract applies: before `await_converged` a small or hot group
can show a combination no source state produces, and after it values are exact
([Reading derived data](data-flow.md#reading-derived-data)).

## Calculated Fields

Every target table has a set of **calculated fields** — non-key columns populated
by a formula rather than copied from a source column. A formula may reference:

* Source columns feeding the calculated row, following the shape its granularity
  requires (see [Granularity](#granularity)).
* Other calculated columns on the same target table, by name.

A bare name means a field when no source column has that name, and the source
column when no field does. A name that is both a source column and a field whose
expression is something else is refused at define as ambiguous, with both
meanings in the error ([Supported sources and targets](#supported-sources-and-targets)).
A 1-1 target refuses such a field outright, whether or not anything reads it.
Two reads of such a name are not ambiguous:

* A field reading its own name reads the source column (`SUM(val) AS val`), and
  a field that passes its column through (`val AS val`, or qualified,
  `src.val AS val`) is that column.
* In an aggregate target, an aggregate's argument never reads a column that is
  itself an aggregate: a name there means the source column, as in SQL.
  `SUM(val) AS val, COUNT(val) AS n` counts the source column `val`, not the
  sum. A column that isn't an aggregate (`(id + 1) AS adj,
  SUM(adj)`) is still read by name, as long as no source column is named `adj`.

A **qualified name** always reads the source column: `src.val`, or
`public.src.val`, where `src` is the `FROM` table. So `(grp + 1) AS val,
SUM(src.val) AS total` sums the source column beside the field that shadows it.
A two-part name whose head is not the `FROM` table is refused at define; a
second table is read with a `JOIN`. A three-part name must name the `FROM`
table and its schema. A field has no qualified form: to
read a field a source column shadows, rename the field.

A formula compares and combines values with `=`, `<>` (or `!=`), `<`, `<=`,
`>`, `>=`, `IS [NOT] DISTINCT FROM`, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, the
searched `CASE WHEN <boolean> THEN <value> [WHEN ...] [ELSE <value>] END` and
`NULLIF(a, b)`, over the literals `TRUE`, `FALSE` and `NULL` among other
operands. Postgres evaluates them, so `NULL` follows its three-valued
logic: `NULL = NULL` is `NULL`, `NULL IS NOT DISTINCT FROM NULL` is true,
`NULL AND FALSE` is false, `NaN = NaN` is true. A `NULL` takes its type from
its context, as in Postgres: the other operand's (`int_col + NULL` is
`integer`), the other `CASE` results' or `COALESCE` and `NULLIF` arguments',
`boolean` under `AND`, `OR`, `NOT` and `WHEN`, and `CAST(NULL AS date)` names
it; a field that is only `NULL` is a `text` column ([type
support](type-support.md#comparison-and-logic)). A comparison, `IS NULL` or logic field is a `boolean`
column, and another field may read it. Binding, loosest first: `OR`, `AND`,
`NOT`, `IS ...`, the comparisons, `+` and `-`, `*` `/` `%`, prefix `-`; so
`NOT a = b` is `NOT (a = b)` and `a = b IS NULL` is `(a = b) IS NULL`. The
comparisons do not chain (`a < b < c` is refused, as in Postgres). Postgres
promises no evaluation order inside `AND` and `OR`, so `y <> 0 AND x / y > 1`
can still divide by zero; guard a division with `CASE WHEN y <> 0 THEN x / y
END` or `x / NULLIF(y, 0)`. A failure inside a `CASE` branch is raised only
for the rows that take the branch, except a constant Postgres folds while
planning (a `1.5 / 0` in a branch fails every row), which define refuses. In
an aggregate, `COUNT(CASE WHEN c THEN 1 END)` and `SUM(CASE WHEN c THEN qty
ELSE 0 END)` over an integer define; `SUM(CASE WHEN c THEN amount END)` over a
`numeric(10,2)` column does not, because the `CASE` has no declared scale.
`NOT` and `CASE` are keywords wherever an operand follows (a `-` begins one:
`NOT -x > 0` is `NOT ((-x) > 0)`, as in Postgres). A word Postgres reserves is
not a column name in an expression at all (`not`, `case`, `end`, `and`, `order`,
`user`...); a source column with such a name is read qualified with its
table (`orders.order`), as in Postgres; see
[Supported sources and targets](#supported-sources-and-targets). Families, types and
refusals are in [Supported sources and
targets](#supported-sources-and-targets) and [type
support](type-support.md#comparison-and-logic).

A formula's arithmetic is Postgres's: `+`, `-`, `*`, `/`, `%`, prefix `-`,
`ABS`, `CEIL`, `FLOOR`, `ROUND`, `TRUNC` and the widening `CAST` of an integer
(to `integer` or `int`/`int4`, `bigint` or `int8`, `numeric` or `decimal`; result types in [type support](type-support.md#arithmetic)). `*`, `/` and `%`
bind tighter than `+` and `-`, prefix `-` tighter than all of them, and
operators of one tier group left to right: `a - b * c` is `a - (b * c)`,
`-a * b` is `(-a) * b`, `a - b - c` is `(a - b) - c`. A minus before a number
literal is one negative literal, typed by its value as Postgres types it, so
`-2147483648` is an `integer`. What Postgres would do differently from what a
reader expects, or fail on every row, is refused at define
([Supported sources and targets](#supported-sources-and-targets)). A value an
operation cannot compute for one row (an overflow, a division by zero) raises
`22003` or `22012` for that row's record, which is quarantined and charged to
the field.

A formula's dates are Postgres's too. `date + n`, `n + date` and `date - n` add
or subtract a count of days (a `smallint` or `integer`) and give a `date`;
`date - date` is the days between, an `integer`. `DATE_TRUNC('<unit>', ts)`
truncates a `timestamp` (`week` to Monday); `EXTRACT(<field> FROM x)` reads a
part of a `date` or `timestamp` as a `numeric`; `tz AT TIME ZONE 'UTC'` turns a
`timestamptz` into the `timestamp` it is in UTC, and `CAST(d AS timestamp)`
turns a `date` into midnight. `AT TIME ZONE` binds tighter than `*` and looser
than prefix `-`. A duration is `EXTRACT(EPOCH FROM b) - EXTRACT(EPOCH FROM a)`,
in exact `numeric` seconds; there is no `interval`. `infinity` and `-infinity`
follow Postgres (`infinity + 1` is `infinity`, `EXTRACT(year FROM 'infinity'::date)`
is `Infinity`, `EXTRACT(day FROM ...)` of it is `NULL`), and a row whose date
leaves the range (`date + n` past 5874897-12-31, two dates one of which is
infinite, a date past 294276-12-31 cast to `timestamp`) raises `22008` for that
row's record, which is quarantined; a constant that does is refused at define.
A bucket for an aggregate is computed in a 1-1 transform and grouped by in
another (`DATE_TRUNC('month', ts) AS bucket`, then `GROUP BY bucket`), since a
`GROUP BY` key is a column; `EXTRACT` is an unconstrained `numeric`, so it is
counted or grouped by, not summed. `AT` is a keyword only after an operand and
before `TIME ZONE`, and `EXTRACT` only before `(`, so columns of those names
(and `year`, `time`, `zone`) are still columns. Types and refusals are in
[Supported sources and targets](#supported-sources-and-targets) and [type
support](type-support.md#dates-and-times).

Formulas may only use **immutable** functions and operators — those whose output
depends solely on their inputs. Anything depending on database state outside the
row (current time, timezone, collation, session settings, random values) is
disallowed: incremental maintenance requires that re-evaluating a formula on
unchanged inputs always yields the same result.

### Chaining and cycle detection

Calculated columns may reference each other, letting users build derivations out
of small named steps (a `margin` column defined in terms of `revenue` and `cost`).
Transforms also chain: a target table may itself be a source for another —
aggregated or joined. Trellis builds a
dependency graph spanning columns within a table and tables across chained
transforms, and computes in dependency order.

A transform can only chain off a target once that target's own transform is
`live`. Defining it while the upstream is still building (for example, a plain
1-1 target whose chunked backfill hasn't finished) is refused with
`TransformNotLive`. Each write to a target reaches the transforms reading it inside the
same transaction as the write; Trellis never installs capture triggers on a
target. That in-transaction hand-off
is why an aggregate target, which has no primary key, can be chained off at all.
It does not cross into another instance.

**Cycles are rejected at definition time across the whole graph.** A definition
that would introduce a cycle, directly or transitively, is invalid and rejected
before it runs, keeping the computation order well-defined.

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
undefined ([known correctness gaps — Rows written into a target, a hidden table or Trellis's schema](known-correctness-gaps.md#rows-written-into-a-target-a-hidden-table-or-trelliss-schema)). Treat a target as read-only, and specifically:

* **Do not insert, update or delete rows.** An edited row stays wrong until the
  next event that recomputes it, and a row added or re-keyed by hand stays
  until a rebuild. `self_check`
  ([0013](decisions/0013-self-check-production-recompute-audit.md)) reports the
  divergence; it does not repair it. On a 1-1 target, `PAUSE` then `RESUME`
  rebuilds the table from source and deletes the rows the source doesn't back,
  including rows added or re-keyed by hand; on an aggregate target it keeps
  group rows edited by hand, and `PAUSE`, `DROP TRANSFORM` and define again
  repairs it ([operations — `PAUSE` and `RESUME`](operations.md#pause-and-resume)).
* **Do not add, drop, rename or retype columns.** Trellis addresses its columns
  by name and type. Redefining the transform is how a column changes
  ([Changing a definition](#changing-a-definition)).
* **Do not change the key constraints.** A 1-1 target's key (its primary key,
  or its unique constraint over a nullable-keyed source) and an aggregate
  target's unique grouping constraint are how Trellis addresses a row when it
  upserts or deletes it.
* **Do not `TRUNCATE` or `DROP` the table yourself.** `DROP TRANSFORM` removes
  the table and its definition together, and refuses while another definition
  still chains off it. A hand `TRUNCATE` leaves the table empty until a 1-1
  transform is paused and resumed, or an aggregate dropped and defined again.
  A resume refuses while the table is missing, and the repair is
  `DROP TRANSFORM` and defining it again.
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
A unique index on a column other than the key is the likely case: changes to
different keys apply in any order, so two rows can hold one value for a moment
even when the source never does, and the index refuses one of them.
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
  An aggregate, a `JOIN`, and a plain 1-1 transform (over a table rather than
  another transform's target) are built while their live changes are applied,
  and go from here straight to `live`
  ([data-flow — The Re-derive build](data-flow.md#the-re-derive-build),
  [An aggregate's or a `JOIN`'s build](data-flow.md#an-aggregates-or-a-joins-build)).
  A `live` 1-1 transform comes back here while an `ALTER TRANSFORM` that adds
  or changes a field, or a resumed column, is rebuilt in the background
  ([Changing a definition](#changing-a-definition)). A `live` transform also comes back here, from the call's return, while
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
* **`catching_up`** — reported, never stored, while a transform that is
  otherwise `live` reads an upstream transform (or a paused column of one) that
  isn't `live`: live changes are applied, but the target doesn't reflect its
  source yet. It reads `live` again once the upstream does.
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
  would create ([operations — `PAUSE` and `RESUME`](operations.md#pause-and-resume)).

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
status of each transform reading a table on the page reports it as `drain_failure`, with the error, the tables, and
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

PAUSE  TRANSFORM <target>[.<column>]
RESUME TRANSFORM <target>[.<column>]
DROP   TRANSFORM <target>
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
field is `ALTER TRANSFORM a DROP b`.

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
subject, so a chain is retired from the leaves inward.

`DROP TRANSFORM <target>.<column>` is not a statement: removing one
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

An `ALTER` of a field compares the column's current type with the type `DEFINE` would
give the altered field: a bare passthrough takes its source column's type with no
modifier (so `varchar` and `text`, or `numeric(10,2)` and `numeric`, are one type and need
no change), any other expression its value family's. An `ALTER` never re-types a column.
One that would (a change of integer or float width, or of type family) would rewrite the
target under `ACCESS EXCLUSIVE`, so it is refused, naming the remedy: `DROP` the field,
then `ADD` it again.

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

## Grammar versions

Every stored definition records the grammar version it was written in. A
breaking release that changes what accepted text means bumps that version and
keeps reading the older one under its old rules, so an upgrade never
re-interprets a stored definition. Each major version reads every grammar
version the previous two majors wrote. A transform written under an older
version than that is paused until you drop it and define it again, and a version
newer than the binary knows, from a downgrade, is refused. A release that
refuses a form Postgres rejects or reads differently keeps the version: a
transform whose stored text uses that form is paused when the upgraded binary
attaches, its `capture_failure` naming the refusal, until you drop it and
define it again; the other transforms keep running. A stored definition
can be rewritten to the current version without a rebuild. The rules are in
[0018-grammar-versioning](decisions/0018-grammar-versioning.md).

## Scope

This document describes the logical model only, not the physical storage or
timing of derived data; see [data-flow](data-flow.md) for how changes propagate
and [0002-async-data-flow](decisions/0002-async-data-flow.md) for why that flow
is asynchronous.
