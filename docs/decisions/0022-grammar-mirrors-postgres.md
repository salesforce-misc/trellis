---
status: draft
date: 2026-10-28
deciders: Michael Ries
---

# The Expression Grammar Is a Subset of Postgres's

Trellis parses transform definitions with its own grammar
([ADR-0004](0004-transform-definition-grammar.md)) and renders each expression
into SQL that Postgres evaluates. A user writes that expression the way they
would write it in a query, and checks a target against the query they had in
mind. If Trellis's lexer or parser reads the text differently from Postgres's
(splits a token Postgres keeps whole, binds an operator at another tier, or
treats a word as a column where Postgres treats it as a keyword), the target
holds values for an expression the user didn't write, and nothing flags it: the
renderer faithfully emits Trellis's reading, and Postgres faithfully computes
it.

This ADR settles what the expression grammar may accept and how it may change:
Postgres is the oracle for every expression's syntax and meaning, and Trellis
may refuse more than Postgres does but never less. The statement language
around the expressions (`TRANSFORM`, `FROM`, `GROUP BY`, `SELECT ... AS`,
`ALTER TRANSFORM` and the rest) is Trellis's own and out of scope; so is how a
stored definition keeps its meaning across releases, which is
[ADR-0018](0018-grammar-versioning.md)'s.

## Decisions

### Postgres is the oracle for every expression

An expression Trellis accepts means what Postgres computes for the same text:
the same type, the same value on every row, the same `NULL`s and the same
errors. A target table holds what Postgres's own query over the source would
return. Trellis computes no value itself, so the oracle holds as long as the
parse is Postgres's and the rendering says the same thing; the grammar is where
the two can drift apart, so the grammar is where this ADR binds.

### Every grammar rule mirrors Postgres's lexer and parser

Each rule the expression grammar has is the rule Postgres's `scan.l` and
`gram.y` have for that construct:

- **Token boundaries.** An operator is the longest run of adjacent operator
  characters (`` + - * / % < > = ~ ! @ # ^ & | ` ? ``), cut before a `--` or
  `/*` and trimmed as Postgres trims it: a run ending in `+` or `-` gives back
  those characters unless it holds one of `` ~ ! @ # % ^ & | ` ? ``. So
  `a<-1` is `a < -1` and `a>=-1` is `a >= -1`, while `a!=-1` and `a%-1` are
  the single operators `!=-` and `%-`, which Postgres doesn't have, and
  Trellis refuses each whole rather than splitting it. A multi-character
  operator is one token only when its characters are adjacent (`<=`, `<>`,
  `!=`, `>=`); `a < = b` is two operators, which Postgres refuses, so Trellis
  refuses it by name. String quoting and numeric literals lex as Postgres
  lexes them, and a lexical form Trellis doesn't implement (`1e3`, `.5`,
  `1_000`, `0x1F`, `E'...'`, a `/* */` or `--` comment) is refused, never read
  as something else.
  [OPEN: a number directly followed by a letter (`a>1AND b<2`, `1IS NULL`,
  `a+1AS x`) lexes as the number and then a word, though PostgreSQL 15 and
  later reject it as trailing junk after a numeric literal; the 0.1.0-alpha
  accepted `a+1AS x`. Recommendation: refuse it, as Postgres does.]
- **Precedence and associativity.** Each operator sits at Postgres's tier and
  associates as Postgres's does, including non-associativity: the comparisons
  don't chain (`a < b < c` is refused), `NOT` binds looser than the comparisons
  and `IS`, and `AT TIME ZONE` binds tighter than `*` and looser than prefix
  `-`.
- **Keywords.** `NOT` and `CASE`, which Postgres reserves, are keywords
  before every operand the grammar has, `-` included: `not - 1` is `NOT (-1)`, as in
  Postgres. Trellis has no quoted identifiers, so a source column named `not`
  or `case` can't be referenced before an operand; Postgres would need
  `"not"` there.
  [OPEN: whether every word Postgres reserves is a keyword everywhere. A
  reserved word with no operand after it, or one the grammar has no use for,
  reads as a column name today: `not AS c`, `not + 1` (which Postgres reads as
  `NOT (+1)`), `case * b`, `end + 1`, and `true`, `false` and `null`, which
  are literals in Postgres. Each resolves only to a source column of that
  name, which Postgres would need quoted, and is refused as an unresolved
  column otherwise. The 0.1.0-alpha read `not AS c`, `not + 1` and `true AS c`
  this way. Recommendation: the rule covers the keywords the grammar uses, not
  every reserved word.]
- **Spellings.** An alternative spelling is accepted only where Postgres
  accepts it, and means what it means there. `CAST(x AS int)` (and `int4`,
  `int8`, `decimal`) is accepted because Postgres spells those types so; `!=`
  is `<>` because Postgres rewrites it so.

### Trellis never accepts text Postgres rejects, nor gives it another meaning

Text that Postgres rejects as an expression is refused at define, and text
Trellis accepts parses to the tree Postgres's parser would build. This is a
statement about every supported Postgres version: text one of them rejects, or
reads differently, is refused.

### Settings Postgres reads are pinned, or the construct is refused

Where Postgres's result for a construct depends on a setting, either every
Trellis session pins the setting or define refuses the construct. Every
connection Trellis opens pins the output settings
[type support](../type-support.md#text-rendering) lists (`DateStyle`,
`IntervalStyle`, `TimeZone`, `extra_float_digits` and the rest),
`timezone_abbreviations`, so `AT TIME ZONE 'UTC'` reads `'UTC'` the same way
everywhere, and `search_path`, to Trellis's schema, the target schema and
`public`, after the implicit `pg_catalog`. What can't be pinned per session, such as
a column's collation for text ordering, or the time zone database behind
`'America/New_York'`, is refused.

### Types, `NULL`s, errors and constant folding follow Postgres, checked against it

A field's declared type is the type `CREATE TABLE AS` gives the expression.
`NULL` follows Postgres's three-valued logic, an overflow or division by zero
raises Postgres's SQLSTATE for the row, and a constant that Postgres folds and
fails while planning is refused at define, since Postgres would fail every row.
The conformance harness
([generative test suite](../generative-test-suite.md#expression-conformance))
checks each of these against a live Postgres: type, value, round trip through
the parser and renderer, and a near miss for every refusal, so a refusal that
drifts into acceptance fails a run.

### Trellis may be stricter than Postgres, never looser

Trellis refuses a form Postgres accepts when it can't keep the result exact:
an `interval`-producing `-`, a `STABLE` overload Postgres picks silently,
`integer / integer`'s truncation, text ordering under a collation, a relative
typed literal. Each refusal is named in the error, carries a remedy, and is
listed in the
[Supported sources and targets](../transforms.md#supported-sources-and-targets)
table, which is the only reference for them. The narrowing runs one way: a
refusal can later become an acceptance of Postgres's meaning, but Trellis never
accepts text Postgres refuses, or gives a meaning Postgres doesn't, to work
around one.

### A Trellis-only syntax needs its own ADR and must not look like Postgres's

The statement language is Trellis's, and grows by its own ADRs. Inside an
expression, a construct Postgres has no syntax for needs an ADR of its own, and
must be spelled so that no reader can take it for Postgres syntax with a
different meaning: a word or operator Postgres gives a meaning is never reused
for another.

### Reviewers check a grammar change against Postgres itself

A change to the lexer or the expression parser is checked three ways:

- The new syntax, and the forms around it a reader might confuse with it, are
  run through a scratch PostgreSQL 17 cluster, and the reviewer confirms
  Postgres accepts, rejects and binds each one as Trellis does.
- Lexer and parser tests pin both sides: each accepted spelling with the tree
  it parses to, and each refused neighbour (the spaced-out operator, the
  chained comparison, the keyword read as a column) with its error.
- The harness learns the construct, and every refusal it adds gets a near miss.

## Example

```text
TRANSFORM orders_flags FROM public.orders
SELECT (qty <= 10) AS small, NOT -qty < 0 AS not_positive
```

`qty <= 10` lexes `<=` as one operator because its characters are adjacent;
`qty < = 10` is refused, as Postgres refuses it. `NOT -qty < 0` parses as
`NOT ((-qty) < 0)`: `NOT` is a keyword before `-`, and binds looser than `<`,
which binds looser than prefix `-`, exactly as Postgres parses it.

## Relationship to ADR-0018

[ADR-0018](0018-grammar-versioning.md) protects text a release has accepted:
once stored, it keeps its meaning across upgrades, and a change in meaning bumps
the grammar version with a compatibility rule. This ADR says which way the
grammar may move: toward Postgres. Admitting a Postgres form that was refused is
additive and needs no bump. Correcting a rule that read accepted text
differently from Postgres changes what that text means, so it is a bump under
ADR-0018, and its compatibility rule keeps each stored definition on the meaning
it was built with. Neither ADR permits a change that moves the grammar away from
Postgres.

## Consequences

- The grammar is stricter than Postgres in places a SQL user will notice: the
  refusals in the transforms table, and no quoted identifiers, so a column whose
  name Postgres reserves can't be referenced.
- The oracle is every supported Postgres version, PostgreSQL 15 or later. Where
  majors differ (PostgreSQL 16 and later read `1_000` as 1000 and `0x1F` as
  31; 15 rejects both as trailing junk), Trellis accepts only what every
  supported major accepts with one meaning, so it refuses both. Supporting a new major means running the reviewer's checks and
  the harness against it; a rule it changes for text Trellis accepts is refused
  until the older majors leave the supported range, then follows the new rule
  under ADR-0018. Dropping a major can only widen what Trellis accepts.
- Every grammar change costs a check against a live Postgres and tests on both
  sides of the rule, on top of the paired grammar-and-renderer change ADR-0004
  requires.
