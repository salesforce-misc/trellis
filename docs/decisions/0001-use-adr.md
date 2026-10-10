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

- **Record the decision, not the journey.** Describe the design as it is, in the
  present tense, and edit the ADR in place when the design changes. Earlier
  designs, PRs, branches and follow-up plans belong in git history and issues.
- **Open with the problem.** A paragraph or two on what breaks without a
  decision, ending with what this ADR settles and what it leaves out of scope.
- **Make each decision a heading that asserts it** ("Drops go in reverse
  dependency order — no cascade"), with its reason directly beneath it in a
  paragraph or less: enough to persuade, not an exhaustive proof.
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
  status becomes `accepted`.

## More Information

* [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
* [Architecture Decision Record](https://github.com/joelparkerhenderson/architecture-decision-record)
