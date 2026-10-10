---
status: accepted
date: 2026-08-14
deciders: Michael Ries
consulted: 
informed:
---

# Use ADR System

Data-intensive tooling is a web of tradeoffs: one pattern is CPU-bound,
the next is limited by disk IOPS. We use Architecture Decision Records to
capture the key decisions, the tradeoffs they make, and the use-cases they
optimize for.

## Writing an ADR

Each ADR is `NNNN-short-title.md` in this directory, taking the next unused
number. A deleted ADR's number is never reused: 0007 and 0016 are retired. The
frontmatter carries `status`, `date` (the day the decision was made or last
changed) and `deciders`. The status is `draft` while any point is undecided, and
`accepted` once none is; the code then follows the ADR or is being changed to.

- **Record the decision, not the journey.** Describe the design as it is, in the
  present tense, and edit the ADR in place when the design changes. Earlier
  designs, PRs, branches and follow-up plans belong in git history and issues;
  an ADR doesn't link to or cite issues, pull requests or experiment ids, so it
  stands on its own.
- **Open with the problem.** A paragraph or two on what breaks without a
  decision, ending with what this ADR settles and what it leaves out of scope.
- **Make each decision a heading that asserts it** ("Drops go in reverse
  dependency order — no cascade"), with its reason directly beneath it in a
  paragraph or less: enough to persuade, not an exhaustive proof.
- **State evidence in the first person, with the numbers that decided it**
  ("I measured…", "I ran … on PostgreSQL 15 through 18"). Describe an
  experiment by what it did.
- **Name the costs accepted.** End with a Consequences section stating what the
  decision commits the project to, including what it makes harder.
- **Mention an alternative only when a reader would otherwise ask "why not
  X?"** One short paragraph, ending with why not.
- **Write about concepts and contracts, not code.** Name the public surface;
  leave internal functions, migration files and call-site inventories to the
  code and the PR.
- **Ground it with one concrete example**, checked against the current code and
  `docs/` so it is one the system actually accepts.
- **Drafts may carry `[OPEN: question. Recommendation: …]` markers**, for
  example while an experiment or benchmark is pending. Resolve them before the
  status becomes `accepted`. A question the ADR leaves open past that becomes
  an issue, not an ADR section.
- **When an ADR extends another, both say so in one line with a link.** A
  superseded ADR is deleted, not kept as a stub, and whatever of it survives
  is restated where it now lives.
- **Keep caveats and refusals in their reference docs.** Correctness gaps live
  only in [known correctness gaps](../known-correctness-gaps.md), cited by
  title; an ADR has no caveat list or limitations section. Define-time
  refusals are listed in the
  [Supported sources and targets](../transforms.md#supported-sources-and-targets)
  table; an ADR may decide to refuse, and the table is the reference.

## More Information

* [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
* [Architecture Decision Record](https://github.com/joelparkerhenderson/architecture-decision-record)
