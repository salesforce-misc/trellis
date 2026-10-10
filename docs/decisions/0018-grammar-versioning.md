---
status: draft
date: 2026-10-09
deciders: Michael Ries
---

# Grammar Versions on Stored Definitions

## Context

Trellis stores a definition as text, not as a serialized AST:
`transform_definitions.definition_text` and
`relationship_definitions.definition_text`. A `TRANSFORM` or `RELATIONSHIP`
statement is stored verbatim, and an `ALTER TRANSFORM` stores the edited
definition rendered back into the grammar. Every read parses the text again
with the parser a new statement goes through
([ADR-0004](0004-transform-definition-grammar.md)). Validation, row
evaluation and the build's SQL then resolve the names in the parsed tree, each
on its own, and a `RESUME` validates the stored definition again.

That keeps one format for a definition, but it ties a stored definition's
meaning to whichever binary reads it. If a release changes what some accepted
text means (how a name resolves, what an expression evaluates to, whether it is
accepted at all), every stored definition containing that text silently takes
the new meaning the next time it is loaded, while its target still holds rows
computed under the old one. Nothing today records which grammar a text was
written in.

The definition version of [ADR-0015](0015-transform-redefinition.md) doesn't
cover this: it counts edits to one definition's fields, not changes to the
language. The catalog's own table schema is out of scope here too, since the
ordered migrations in `trellis/migrations/` already version it.

## Decision

1. **A marker on every stored definition.** Each transform and relationship
   definition carries a `grammar_version`, an integer, written whenever its text
   is written: at define, at declare, at `ALTER TRANSFORM` and at a rewrite.
   Nothing else writes the text, so a pause, a resume or a rebuild leaves both
   as they are. One counter covers the whole grammar. The binary has a current
   version; grammar 1 is the grammar as it stands when the marker is
   introduced.
2. **A change in meaning bumps the version; additive syntax doesn't.** A change
   bumps it when some text the current version accepts would parse, resolve or
   evaluate differently, or be refused. A change that only accepts text the
   current version refuses, such as a new function, needs no bump. A new keyword
   that was a valid identifier is a change in meaning, not an addition. A bump
   that retires a meaning also gives it a spelling in the new grammar, so every
   older text can be translated (decision 3).
   [OPEN: whether a fix that makes evaluation match what the documentation
   already says is a change in meaning. Recommendation: no; it ships with the
   definitions to rebuild, since a compatibility rule would keep the wrong
   values and a rewrite couldn't spell them.]
3. **Loading dispatches on the version, in one place.** Every read of stored
   transform text goes through one entry point that takes the text and its
   version and returns the definition in the current grammar's terms, so
   nothing after it sees a version:
   - the current version parses as today;
   - an older version inside the support window parses under that version's
     rules, and its compatibility rule translates the tree into one that means
     the same under the current rules;
   - an older version outside the window is not read: the load fails, and the
     attach pauses the transform, with `capture_failure` naming the stored
     version, the oldest this binary reads, and the fix (drop it and define it
     again). `RESUME` refuses until then, and `DROP TRANSFORM` works without
     reading the text;
   - a version newer than the binary knows is refused: the load fails, and the
     definition is neither read nor written. Stored text that fails to parse
     at a version the binary does know fails the same way, which is how a
     downgrade past an additive change shows up.

   Attaching (`migrate`) checks every stored definition's version before
   anything else runs. It pauses each one outside the window, and refuses the
   attach if any is newer, naming the definitions and their versions. The
   load-time refusal still stands, since a process can connect without
   attaching and a newer binary can write after the attach.

   A relationship needs none of this. Its name, both endpoints and both join
   columns each have a catalog column of their own, and loading reads those
   rather than the text, so a grammar change can't change what a stored
   relationship means and one is never outside the window. Its text and version
   are kept as written.

   Old text is never read under new rules.
4. **Rewriting moves a definition to the current grammar.** A client can
   rewrite a stored definition's text to the current version: Trellis renders
   the definition decision 3 loads back into the current grammar. The rewrite
   must keep its meaning: the same target columns and types, and the same
   values. Trellis checks that by loading the new text and comparing its tree
   with the old text's, and refuses a rewrite that differs. A rewrite changes no
   column, so it needs no rebuild and doesn't bump the definition version. An
   `ALTER TRANSFORM` of an older-version definition is a rewrite plus the edit:
   it loads the definition, applies the edit and renders the result under the
   current grammar.
5. **The compatibility rule.** *Each major version reads every grammar version
   written by the previous two major versions.* An operator can upgrade across
   up to two major versions in one step, then rewrite. Before 1.0, each 0.x
   minor counts as a major: 0.9 reads what 0.7 and 0.8 wrote, and 1.0 reads what
   0.8 and 0.9 wrote. Support for a grammar version may be dropped in the third
   major after the last one that writes it, so the oldest version a binary reads
   is the one written by the major two before it. [OPEN: whether a version bump
   may ship in a minor release. Recommendation: only in a major (a 0.x minor
   before 1.0), since a bump refuses text the previous release accepted, and
   the window above assumes one grammar version per major.]
6. **Each bump ships its own rules.** A change that bumps the version brings,
   with it, the previous version's compatibility rule (its parser rules and the
   translation from its tree, which loading and rewriting share), and stored
   fixtures at every version still in the window. How to translate a particular
   change is decided with that change, not here.

### Worked example

Take a grammar in which a bare name that names both a source column and a
field whose expression is something else reads as the field, so that

```text
TRANSFORM t FROM public.src GROUP BY grp
SELECT grp AS grp, (val * 2) AS val, SUM(val) AS total
```

means `SUM(val * 2)`. Changing it to refuse such a name, and to read a
qualified `src.val` as the source column, changes what accepted text means, so
it bumps the version. Grammar 1 already reads names the second way
([Calculated Fields](../transforms.md#calculated-fields)). The older version's
compatibility rule translates a bare name that names both into the field's
expression, so every stored definition keeps reading `SUM(val * 2)`, and a
rewrite renders that tree as `SUM(src.val * 2)`. Loading the rewritten text
gives the same tree, so the check passes. Admitting a new function under the
growth policy of
[ADR-0004](0004-transform-definition-grammar.md#growth-policy), by contrast, is
additive: no text an earlier version accepted calls it.

### Why

**A marker, not nothing.** Without one, a meaning change silently re-interprets
every stored definition while its target holds rows computed the old way, and no
rebuild is triggered to reconcile them. A marker makes the reader choose the
rules the text was written under.

**Stamped when written, never when read.** The version records the rules the
text was written in. Loading never changes it, so reading a definition can't
move it to rules it wasn't written for.

**Translated where it is read.** Names are resolved in several places after the
parse: validation, row evaluation, the build's SQL and a resume's
re-validation. Translating once, at load, keeps every one of them on the current
rules alone, lets an older definition pass a resume's re-validation, and makes
the translation a rewrite needs the same code that loading runs.

**One counter.** One parser reads both kinds of statement. Separate counters
would version one language twice.

**Relationships load from their columns.** Everything a relationship statement
says is already stored in columns the migrations version, and nothing changes
them after the declare. Reading the columns ties a relationship's meaning to the
catalog schema rather than the grammar, so it never needs a paused state it
doesn't have.

**Additive changes don't bump.** Every stored text keeps its meaning, so a bump
would only force rewrites for nothing. A downgrade past new syntax or a new
function still can't misread anything: the older parser refuses the text, and
the load fails.

**Rewriting is checked, and needs no rebuild.** The text is the only thing that
changes. Comparing what both texts load to proves the target's rows are still
correct, so they stay as they are.

**Two major versions.** Reading two majors back lets an operator skip a major,
and bounds the compatibility rules a binary carries to the versions of three
majors. A definition that is never rewritten leaves the window after two
majors, which is why a rewrite ships with every bump.

**Too old pauses; too new refuses.** A version older than the window is
something this binary knows it can't read, and the operator can fix it here by
redefining. A newer version means the binary is the wrong one, and the fix is to
run the right one, not to touch the definition.

## Consequences

- Every stored definition carries its grammar version from the first release, so
  a later grammar change has a defined path rather than a silent one.
- A meaning change to the grammar costs a version bump, a compatibility rule for
  the version it replaces, and fixtures, carried for two majors.
- Upgrades may skip one major version. Skipping more, without rewriting in
  between, pauses the definitions written before the window.
- A downgrade across a grammar change is refused rather than run on
  re-interpreted definitions, and so is an older binary running beside a newer
  one once the newer one writes a newer version.
