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
definition rendered back into the grammar. Every read parses the text again and
resolves its names again, through the same front end a new statement goes
through ([ADR-0004](0004-transform-definition-grammar.md)).

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
   is written: at define, at declare, at `ALTER TRANSFORM` and at a rewrite. One
   counter covers the whole grammar. The binary has a current version; grammar
   1 is the grammar as it stands when the marker is introduced.
2. **A change in meaning bumps the version; additive syntax doesn't.** A change
   bumps it when some text the current version accepts would parse, resolve or
   evaluate differently, or be refused. A change that only accepts text the
   current version refuses, such as a new function, needs no bump. A new keyword
   that was a valid identifier is a change in meaning, not an addition.
3. **Loading dispatches on the version.** Every read of stored text, the
   parse and the name resolution after it, goes through one entry point that
   takes the text and its version:
   - the current version reads under today's rules;
   - an older version inside the support window reads under that version's
     compatibility rules;
   - an older version outside the window is not read: the transform is paused,
     with `capture_failure` naming the stored version, the oldest this binary
     reads, and the fix (redefine it under the current grammar). `RESUME`
     refuses until then, and `DROP TRANSFORM` works without reading the text.
     [OPEN: a relationship has no paused state. Options: pause every transform
     that reads through it, as a join-column type mismatch does, or refuse the
     attach. Recommendation: pause its readers, which matches the existing
     precedent and leaves the rest of the instance running.]
   - a version newer than the binary knows (a downgrade) is refused, and the
     definition is neither read nor written. Stored text that fails to parse at
     a version the binary does know is refused the same way, which is how a
     downgrade past an additive change shows up. [OPEN: refuse the whole attach,
     or only that definition. Recommendation: refuse the attach, naming the
     definitions and their versions. The fix is to run a binary that reads them,
     and a binary can't pause or skip a definition it can't read without
     writing to it.]

   Old text is never read under new rules.
4. **Rewriting moves a definition to the current grammar.** A client can
   rewrite a stored definition's text to the current version. The rewrite must
   keep its meaning: the same target columns and types, and the same values.
   Trellis checks that by resolving both texts, each under its own version, and
   comparing the resolved ASTs (or the validated plans), and refuses a rewrite
   that differs. A rewrite changes no column, so it needs no rebuild and doesn't
   bump the definition version. An `ALTER TRANSFORM` of an older-version
   definition renders it under the current grammar, so it is a rewrite plus the
   edit, under the same check. [OPEN: whether `RESUME` of an older-version
   definition rewrites it. Options: keep the text and version and rebuild under
   that version's rules, or rewrite to the current grammar first, since the
   resume rebuilds anyway. Recommendation: keep them. A resume changes nothing
   about the definition itself, and a text that can't be rewritten still
   resumes.]
5. **The compatibility rule.** *Each major version reads every grammar version
   written by the previous two major versions.* An operator can upgrade across
   up to two major versions in one step, then rewrite. Before 1.0, each 0.x
   minor counts as a major: 0.9 reads what 0.7 and 0.8 wrote, and 1.0 reads what
   0.8 and 0.9 wrote. Support for a grammar version may be dropped in the third
   major after the last one that writes it, so the oldest version a binary reads
   is the one written by the major two before it. [OPEN: whether a version bump
   may ship in a minor release. Recommendation: only in a major (a 0.x minor
   before 1.0), since a bump refuses text the previous release accepted. It
   also keeps a downgrade within a major from meeting a newer version.]
6. **Each bump ships its own rules.** A change that bumps the version brings,
   with it, the previous version's compatibility rule, the translation a rewrite
   uses, and stored fixtures at every version still in the window. How to
   translate a particular change is decided with that change, not here.

### Worked example

Grammar 1 refuses a bare name that names both a source column and a field whose
expression is something else, and reads a qualified `src.val` as the source
column ([Calculated Fields](../transforms.md#calculated-fields)). Suppose a
grammar instead read such a name as the field, so that

```text
TRANSFORM t FROM public.src GROUP BY grp
SELECT grp AS grp, (val * 2) AS val, SUM(val) AS total
```

meant `SUM(val * 2)`. Introducing the refusal changes what accepted text means,
so it bumps the version. The older version's compatibility rule is "a bare name
that names both resolves to the field", which keeps every stored definition
reading `SUM(val * 2)`. A rewrite spells that meaning `SUM(src.val * 2)` under
the new version, and the check confirms both texts resolve to the same AST.
Admitting a new function under the growth policy of
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

**One counter.** One parser reads both kinds of statement, and a transform's
text resolves relationship paths. Separate counters would version one language
twice.

**Additive changes don't bump.** Every stored text keeps its meaning, so a bump
would only force rewrites for nothing. A downgrade past such a change still
can't misread anything: the older binary fails to parse the new syntax and
refuses the definition.

**Rewriting is checked, and needs no rebuild.** The text is the only thing that
changes. Comparing what both texts resolve to proves the target's rows are still
correct, so they stay as they are.

**Two major versions.** Reading two majors back lets an operator skip a major,
and bounds the compatibility rules a binary carries to the versions of three
majors. A
definition that is never rewritten leaves the window after two majors, which is
why a rewrite ships with every bump.

**Too old pauses; too new refuses.** A version older than the window is
something this binary knows it can't read, and the operator can fix it here by
redefining. A newer version means the binary is the wrong one, and the fix is to
run the right one, not to touch the definition.

## Consequences

- Every stored definition carries its grammar version from the first release, so
  a later grammar change has a defined path rather than a silent one.
- A meaning change to the grammar costs a version bump, a compatibility rule for
  the version it replaces, a rewrite translation and fixtures, carried for two
  majors.
- Upgrades may skip one major version. Skipping more, without rewriting in
  between, pauses the definitions written before the window.
- A downgrade across a grammar change is refused rather than run on
  re-interpreted definitions.
