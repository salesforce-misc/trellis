---
status: draft
date: 2026-10-09
deciders: Michael Ries
---

# Grammar Versions on Stored Definitions

Trellis stores each definition as text (`transform_definitions.definition_text`,
`relationship_definitions.definition_text`) and parses it again on every read,
with the parser new statements go through
([ADR-0004](0004-transform-definition-grammar.md)). An `ALTER TRANSFORM` stores
the edited definition rendered back into the grammar. Validation, row
evaluation, the build's SQL and a `RESUME`'s re-validation each resolve names in
the parsed tree on their own.

So a stored definition means whatever the binary reading it says it means. If a
release changes what accepted text means (how a name resolves, what an
expression evaluates to, whether it is accepted at all), every stored definition
containing that text silently takes the new meaning on its next load, while its
target still holds rows computed under the old one.

This ADR settles how a stored definition keeps its meaning across grammar
changes. The definition version of
[ADR-0015](0015-transform-redefinition.md) counts edits to one definition, not
changes to the language, and the ordered migrations in `trellis/migrations/`
version the catalog's own schema; both are out of scope.

## Decisions

### Every stored definition carries a grammar version, stamped when its text is written

Each transform and relationship definition stores an integer `grammar_version`
beside its text, written whenever the text is: at define, declare,
`ALTER TRANSFORM` and rewrite. A pause, resume or rebuild writes neither. The
binary has a current version, and grammar 1 is the grammar when the marker is
introduced. One counter covers the whole grammar, since one parser reads both
kinds of statement.

Because only a write stamps it, reading a definition can never move it to rules
it wasn't written under.

### A change in meaning bumps the version; additive syntax doesn't

A change bumps the version when some text the current version accepts would
parse, resolve or evaluate differently, or be refused. A new keyword that was a
valid identifier is such a change. A change that only accepts text the current
version refuses, such as a new function under
[ADR-0004's growth policy](0004-transform-definition-grammar.md#growth-policy),
needs no bump: every stored text keeps its meaning, and an older binary refuses
the new text rather than misreading it.

A bump that retires a meaning gives it a spelling in the new grammar, so every
older text can be translated. A bump ships only in a release semver allows to
break compatibility (a 0.x minor before 1.0, a major after), because it refuses
text the previous release accepted.

[OPEN: whether a fix that makes evaluation match what the documentation already
says is a change in meaning. Recommendation: no; it ships with the definitions
to rebuild, since a compatibility rule would keep the wrong values and a rewrite
couldn't spell them.]

### Loading translates older text into the current grammar, in one place

Every read of stored transform text goes through one entry point that takes the
text and its version and returns the definition in the current grammar's terms:

- **Current version:** parses as today.
- **Older, inside the support window:** parses under that version's rules, and
  its compatibility rule translates the tree into one that means the same under
  the current rules.
- **Older, outside the window:** not read. The load fails and the transform
  pauses, its `capture_failure` naming the stored version, the oldest version
  this binary reads, and the fix: drop the transform and define it again.
  `RESUME` refuses; `DROP TRANSFORM` works without reading the text.
- **Newer than the binary knows:** refused, and the definition is neither read
  nor written. Text that fails to parse at a known version fails the same way,
  which is how a downgrade past an additive change shows up.

Nothing after the entry point sees a version. Names are resolved in several
places after the parse; translating once keeps every one of them on the current
rules, lets an older definition pass a resume's re-validation, and gives
rewriting the same code.

Attaching (`migrate`) checks every stored definition's version before anything
else runs. It pauses each one outside the window, and refuses the attach if any
is newer, naming those definitions. The load-time check stands on its own, since
a process can connect without attaching and a newer binary can write after the
attach.

Too old pauses and too new refuses because the fixes differ: the operator can
fix a too-old definition here by redefining it, while a too-new one means the
wrong binary is running.

### Relationships load from their catalog columns, not their text

A relationship's name, endpoints and join columns each have a catalog column,
set at declare and never changed. Loading reads those columns, so a grammar
change can't alter what a stored relationship means, and none is ever outside
the window. Its text and version are kept as written.

### Rewriting moves a definition to the current grammar without a rebuild

A client can rewrite a stored definition's text to the current version. Trellis
renders the loaded definition in the current grammar, loads the new text, and
refuses the rewrite unless the two trees match. Matching trees mean the same
target columns, types and values, so the target's rows stay correct: a rewrite
needs no rebuild and doesn't bump the definition version. An `ALTER TRANSFORM`
of an older-version definition is a rewrite plus the edit.

### Each major version reads the grammar versions of the previous two

*Each major version reads every grammar version written by the previous two
major versions.* Before 1.0, each 0.x minor counts as a major: 0.9 reads what
0.7 and 0.8 wrote, and 1.0 reads what 0.8 and 0.9 wrote. An operator can skip
one major in an upgrade, and a binary carries compatibility rules for at most
three majors' versions. A definition that is never rewritten leaves the window
after two majors, which is why rewriting ships with every bump.

### Each bump ships its compatibility rule and fixtures

A bump brings the previous version's compatibility rule (its parser rules and
the tree translation that loading and rewriting share), and stored fixtures at
every version still in the window. How to translate a particular change is
decided with that change.

## Example

Suppose a grammar read a bare name that names both a source column and a field
as the field, so that

```text
TRANSFORM t FROM public.src GROUP BY grp
SELECT grp AS grp, (grp + 1) AS val, SUM(val) AS total
```

means `SUM(grp + 1)`. Changing that grammar to refuse such a name, and to read a
qualified `src.val` as the source column, changes what accepted text means, so
it bumps the version. The older version's compatibility rule translates the bare
name into the field's expression, so every stored definition keeps meaning
`SUM(grp + 1)`, and a rewrite renders that tree as `SUM(grp + 1)`, which loads
to the same tree. (Grammar 1 already reads names the new way; see
[Calculated Fields](../transforms.md#calculated-fields).)

## Consequences

- A meaning change to the grammar costs a breaking release, a version bump, a
  compatibility rule for the version it replaces, and fixtures, all carried for
  two majors.
- Upgrades may skip one major. Skipping more without rewriting in between pauses
  the definitions written before the window.
- A downgrade across a grammar change is refused rather than run on
  re-interpreted definitions, and so is an older binary running beside a newer
  one once the newer one writes a newer version.
