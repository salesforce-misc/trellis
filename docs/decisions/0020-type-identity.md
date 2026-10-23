---
status: accepted
date: 2026-10-22
deciders: Michael Ries
---

# Type Identity of the Columns Trellis Creates and the Values It Casts

Trellis copies source columns into the tables it creates: a 1-1 target's key
and passthrough columns, a `JOIN`'s `GROUP BY` and `ON` columns, and the hidden
tables behind them. It also moves every value as text and casts that text back
to a type when it applies it. So Trellis has to name each copy's type, and its
stored text has to keep meaning the same value. Three things can change under it
without a trigger firing:

- the type's **name** (`ALTER TYPE … RENAME TO`, `SET SCHEMA`);
- the column's type **modifier** (`numeric(6,2)` → `numeric`,
  `timestamp(0)` → `timestamp(3)`), often by a catalog-only `ALTER` that
  rewrites nothing;
- the type's **value identity**: an enum label (`RENAME VALUE`), or how
  existing values render after a rewrite (a `char(n)` length change).

Without one rule for each, every type finds its own way to go wrong. A copy that
keeps a modifier the source has outgrown rounds or truncates values on the way
in, and two source keys can round to the same key, so one row overwrites
another. A stored name stops resolving after a rename. Stored text names a label
that no longer exists.

This ADR settles how Trellis identifies a type, what it declares its copies as,
when stored value text is trusted, how collation fits in, and when two columns
count as the same type. It doesn't change which types are admitted in each
role ([type support](../type-support.md)), and it adds no detector for collation
changes. It extends [ADR-0019](0019-join-transforms.md)'s join-key types.

## A type is identified by its OID; a name is only a rendering

A copy's type is the source column's type OID plus its collation OID. OIDs
survive a rename, `SET SCHEMA`, `ADD VALUE`, `RENAME VALUE`, an extension
update and `pg_upgrade`. I checked each on PostgreSQL 15 and 18, and ran
`pg_upgrade` from 15 to 18. A logical dump and restore changes every OID, and
Trellis already pauses everything on a restore; `RESUME` re-records the OIDs
from the columns, which a restore matches by name.

Trellis never compares type names or type text, and never uses them as
identity. It may store a name as a cache of the rendering, and the capture pass
refreshes it on a rename. Every rendering is produced under
`search_path = pg_catalog, pg_temp`, so it is always schema-qualified.
`format_type` leaves out the schema of any type visible on the session's path.
I verified that a name rendered in one Trellis session resolves to a different
type in a session whose path finds another type of that name first.

## One rendering point turns a type identity into SQL

A single rendering point takes a definition's recorded type and collation OIDs
and returns the text used in every column declaration and cast: the type with
no modifier, schema-qualified, with its collation. Nothing else formats type
text. Today it reads the cached name. Rendering from the OID when a statement
is built, with no stored name at all, then changes only this point. That
becomes worth doing once a second family of user-defined types (domains,
composites, extension types) is supported, since each would otherwise need its
own rename handling.

## Copies carry no type modifier

Every column Trellis creates from a source column is declared as the source
type with no modifier, and every cast Trellis renders targets that same type.
This holds for every type: strings, `character(n)`, `bit(n)`, `bit varying(n)`,
`numeric(p,s)`, the temporal types with a precision, `interval` with a
precision or fields, and arrays of all of them.

A cast rule alone can't protect a copy. I wrote a value needing the wider
modifier into a copy at the old modifier three ways: by assignment, by a cast to
the copy's type, and by a cast to the unmodified type followed by assignment.
I ran each widening on PostgreSQL 15 and 18, with identical results:

- **`varchar`, `bpchar`, `bit`, `varbit`:** assignment errors, and only an
  explicit cast truncates.
- **`numeric`, `time`, `timetz`, `timestamp`, `timestamptz`, `interval`:**
  plain assignment rounds silently.

Most of these widenings are catalog-only, so nothing is rewritten and no rebuild
follows. In every same-family case, a column at the unmodified type stored the
source's exact text, and lookups by `=` against the source held.

A modifier constrains what the source can hold. It plays no part in equality,
joins or indexes. Once copies are unmodified, a catalog-only widening needs no
re-type, no pause and no rebuild. Integer and float **widths**
(`integer` → `bigint`, `real` → `double precision`) are different types with no
unmodified form. They are the only reason Trellis ever re-types a copy: the
transform pauses, and `RESUME` re-types and rebuilds.

## Stored value text is trusted only under the type's current value identity

Trellis keeps values as text in staged images, held and parked keys, cursors
and chunk bounds. That text means a value only under the type's current labels
and renderings. A change of either without a trigger pauses every transform
that reads a column of the type, with the reason in `status()`, and `RESUME`
rebuilds from the source without casting any stored text. The changes are:

- an enum `RENAME VALUE`. A new label (`ADD VALUE`) changes no stored text and
  pauses nothing.
- a rewrite that re-renders existing key text: a `character(n)` length change,
  a `bit(n)` change with `USING`, a narrower `numeric` scale or one added to an
  unconstrained `numeric` (the rewrite rounds), a narrower temporal precision.
  A wider `numeric` scale pauses nothing: a group key's stored text is still an
  equal value ([below](#an-aggregate-or-join-target-holds-an-equal-value)).

Enum values are detected by their value OIDs, recorded with the definition.
A relabel's `RESUME` waits until no change staged before the relabel is
pending, so none of their text is applied or re-derived, and a target whose
ledger keys are label text is rebuilt from empty.

This is the one place a modifier matters: a change that alters how stored
values render, not one that only widens what the column can hold.

## An aggregate or `JOIN` target holds an equal value

A group key and an aggregate result are correct when they are equal, under the
column type's `=`, to what a fresh `GROUP BY` computes. Their text is one of
the equal values' texts, as Postgres's own `GROUP BY` gives: it merges `1.25`
and `1.2500` of a `numeric` column into one group and shows one of them. So a
group row written as `1.25` is correct beside a source that a rewrite to a
wider scale re-rendered as `1.2500`, and a rebuild doesn't rewrite a key whose
text alone changed. A narrower scale changes values, not only their text, so it
pauses the readers, and `RESUME` ends with every group equal by value.

`self_check` holds these targets to the same rule: it matches their groups,
cells and join keys by the key types' equality, and compares a result whose
text differs by its type's `=`
([ADR-0013](0013-self-check-production-recompute-audit.md#comparison-semantics-exact-text-for-1-1-equal-values-for-aggregates-and-joins)).

Text identity for group keys was rejected: a fresh `GROUP BY` doesn't produce
it either, since which of the equal texts it shows depends on the rows it
read. Rewriting a key's text on a rebuild was rejected too: it costs a write
per group for a difference in rendering only.

## A key's identity includes its collation

A collation decides two things about a string key: which values are equal, and
how they sort. Trellis matches keys by exact text, so it needs equality to
mean byte equality. That is what a deterministic collation gives, and why key
columns need one. A nondeterministic collation makes `=` true for values that
render differently, so one group or row would split across several keys.

Moving a key from one deterministic collation to another leaves equality
unchanged, so key identity survives it. Ordering does not. Anything computed
from the order of keys or values under the old collation can be wrong under the
new one: a page boundary, a chunk bound, a range predicate, a `MIN` or `MAX`
over text. So ordering-dependent work reads the key's collation when it runs,
and never relies on an order recorded under an earlier one.

A collation can change with no trigger firing: by an `ALTER COLUMN … COLLATE`
that rewrites nothing, or by a collation library upgrade that changes no
catalog row at all. Trellis can see such a change only by comparing the catalog
afterwards. A pause driven by that comparison must first be shown not to fire on
routine maintenance and migrations, and must not need superuser. This ADR adds
no such detector.

## A 1-1 copy holds the source's text

A type's `=` can be coarser than its text: `numeric` `1.25` and `1.2500`,
`interval` `'1 day'` and `'24 hours'`, float `-0` and `0`, and strings that differ
only in case under a nondeterministic collation. Two shapes of
target answer "is this row right?" differently.

- **A 1-1 target is a copy.** It is right when its `::text` equals the source
  row's, so the apply, the rebuild and the field-build merges write a row
  unless the texts are equal, and `self_check` compares by text. The comparison
  is `row(target columns)::text is distinct from row(new columns)::text`. A
  record's text, not a bare `col::text`: casting a text column to text keeps its
  collation, and the comparison with it. It needs only an output function, so
  it also works for `json` and `xml`, which have no `=`. The cost is a text
  cast of both rows on the update path.
- **An aggregate or `JOIN` target holds an equal value**, as the section
  above says: a group is the set of rows whose key values are equal, and two
  renderings of one group are not two groups.

## Columns pair by type and collation; modifiers never count

Two `JOIN` `ON` columns pair when both conditions hold:

- they have the same type OID, or are both in the string family (`text` and
  `varchar`, which compare as `text`);
- they have the same deterministic collation.

The same rule applies at define, at `RESUME` and at the capture pass's re-check,
so a one-sided modifier change pauses nothing. `integer` against `bigint` stays
refused: pairing them needs a cast, and a `bigint` key cast to `integer`
errors.

This is safe because only text-stable types are join keys: their `=` agrees
with their text for every value. No modifier changes that property:

- a length is checked only on assignment;
- a temporal precision rounds on input, and stored values render the same way
  at any precision;
- two bit strings of different lengths are never equal and never render alike.

## No define-time rule depends on a copy's modifier

A rule that needs a modifier reads the root source column's, or refuses. Group
`SUM`/`AVG` over `numeric` need a declared scale, so that the running sum shows
the scale Postgres would show. A chained definition reading a Trellis target's
`numeric` copy, which has no scale, is therefore refused like any unconstrained
`numeric`.

## An exception to these rules must prove itself

Future work may make an exception: following a relabel in place instead of
rebuilding, or keeping a modifier on some copy. It takes its own decision, and
evidence that it guarantees all of the following:

- **Equivalence.** For every reader shape, the result equals what pause and
  `RESUME` would produce.
- **Completeness.** Every place that stores the affected identity is enumerated
  from one owner, and adding a new one without covering it fails a test.
- **Atomicity.** No drain or reader sees a mix of the old and new identity.
- **Detection.** The signal that triggers it has a false-positive study and
  needs no superuser.
- **Evidence.** The type matrix test covers it, and mutation checks show each
  guarantee is load-bearing.

## Example

A 1-1 transform keyed by a `timestamp(0)`:

```sql
CREATE TABLE events (occurred_at timestamp(0) PRIMARY KEY, kind text);
```

```text
TRANSFORM event_kinds FROM events SELECT occurred_at AS occurred_at, kind AS kind
```

The target's `occurred_at` is `timestamp`. A migration runs
`ALTER TABLE events ALTER COLUMN occurred_at TYPE timestamp(3)`, which changes only the
catalog. A row at `…:00.4` is then written beside the existing `…:00`. Both
reach the target as two rows, exactly, with no pause and no rebuild. A
`timestamp(0)` copy would have rounded `…:00.4` onto `…:00` and overwritten that
row.

## Consequences

- Trellis's target and hidden tables show no modifiers in `\d`. Anything that
  learns precision or length from a target's catalog sees none; the source
  column holds it.
- Most source type changes need neither a pause nor a rebuild. The ones that
  still pause are width changes, relabels and re-rendering rewrites. Each pauses
  every reader of the column or type, and `RESUME` is a full rebuild.
- A chained `SUM`/`AVG` over an enriched `numeric` column is refused until group
  sums track their display scale themselves.
- One generated test owns this seam. Its axes:
  - every type with a modifier;
  - each kind of change: catalog-only widen, rewrite, narrow, rename;
  - each place the column is used: key, passthrough, `GROUP BY`, `JOIN` `ON`,
    hidden tables;
  - one-sided pairing changes.

  Each cell is exact, or paused with a reason and exact after `RESUME`. A new
  type with a modifier is a new row in it, not a new design question.
