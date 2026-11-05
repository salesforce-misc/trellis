# Contributing Guide For Trellis

This page lists the operational governance model of this project, as well as the recommendations and requirements for how to best contribute to trellis. We strive to obey these as best as possible. As always, thanks for contributing – we hope these guidelines make it easier and shed some light on our approach and processes.

# Governance Model - Community Based

The intent and goal of open sourcing this project is to increase the contributor and user base. The governance model is one where new project leads (`admins`) will be added to the project based on their contributions and efforts, a so-called "do-acracy" or "meritocracy" similar to that used by all Apache Software Foundation projects.

# Getting started

Feel free to clone the project, run some [examples](https://github.com/mmmries/trellis-examples) or go build your own.
The `docs` directory is a great place to get to grips with the current state of the project and the general direction we're headed.

# Issues, requests & ideas

Use GitHub Issues page to submit issues, enhancement requests and discuss ideas.

### Bug Reports and Fixes
-  If you find a bug, please search for it in the [Issues](https://github.com/salesforce-misc/trellis/issues), and if it isn't already tracked,
   [create a new issue](https://github.com/salesforce-misc/trellis/issues/new). Describe what you ran, what you expected and what happened, with the Trellis and PostgreSQL versions. Even if an Issue is closed, feel free to comment and add details, it will still
   be reviewed.
-  Issues that have already been identified as a bug (note: able to reproduce) will be labelled `bug`.
-  If you'd like to submit a fix for a bug, [send a Pull Request](#creating-a-pull-request) and mention the Issue number.
  -  Include tests that isolate the bug and verifies that it was fixed.

### New Features
-  If you'd like to add new functionality to this project, describe the problem you want to solve in a [new Issue](https://github.com/salesforce-misc/trellis/issues/new).
-  Issues that have been identified as a feature request will be labelled `enhancement`.
-  If you'd like to implement the new feature, please wait for feedback from the project
   maintainers before spending too much time writing the code. In some cases, `enhancement`s may
   not align well with the project objectives at the time.

### Tests, Documentation, Miscellaneous
-  If you'd like to improve the tests, you want to make the documentation clearer, you have an
   alternative implementation of something that may have advantages over the way it's currently
   done, or you have any other change, we would be happy to hear about it!
  -  If it's a trivial change, go ahead and [send a Pull Request](#creating-a-pull-request) with the changes you have in mind.
  -  If not, [open an Issue](https://github.com/salesforce-misc/trellis/issues/new) to discuss the idea first.

If you're new to our project and looking for some way to make your first contribution, look for
Issues labelled `good first contribution`.

# Contribution Checklist

- [x] Clean, simple, well styled code
- [x] Commits should be atomic and messages must be descriptive. Related issues should be mentioned by Issue number.
- [x] Comments
  - Module-level & function-level comments.
  - Comments on complex blocks of code or algorithms (include references to sources).
- [x] Tests
  - The test suite, if provided, must be complete and pass
  - Increase code coverage, not versa.
  - Use any of our testkits that contains a bunch of testing facilities you would need. For example: `import com.salesforce.op.test._` and borrow inspiration from existing tests.
- [x] Dependencies
  - Minimize number of dependencies.
  - Prefer Apache 2.0, BSD3, MIT, ISC and MPL licenses.
- [x] Reviews
  - Changes must be approved via peer code review

# Grammar Compatibility

Each major version reads every transform-grammar version written by the previous
two major versions (each 0.x minor counts as a major before 1.0). A change to
what accepted definition text means bumps the grammar version, ships only in a
breaking release, and brings a compatibility rule for the version it replaces;
see [ADR-0018](docs/decisions/0018-grammar-versioning.md).

# Compatibility Floors

A database can be rolled back to an earlier release when that release meets
its floors: the catalog floor, the oldest release that can read the catalog,
and each definition's floor, the oldest release that can run it
([ADR-0024](docs/decisions/0024-compatibility-floors-and-rollback.md),
[upgrading](docs/upgrading.md)). Each change an older release can't read
declares the release it arrives in, and CI checks the declarations.

| Change | Declaration | Checked by |
|---|---|---|
| A migration | Its opening comment says, on a line of its own, `-- Declares no catalog floor` (an additive change an older release ignores: a nullable or defaulted column, a new table, a `CHECK` every release's writes already meet) or `-- Raises the catalog floor to X.Y.Z` (anything an older release can't read, and any change to the shape of the capture objects on source tables). A raise carries, in its SQL, `update catalog_floor set version = 'X.Y.Z' where string_to_array(version, '.')::int[] < '{X,Y,Z}'`. | `.github/scripts/check-floors.py check`, in CI's `lint` job: it fails a migration without the declaration, a raise its SQL doesn't carry, any reference to `catalog_floor` under "Declares no catalog floor", a raise above the release being developed, a raise to the newest released tag's release or below, and a file refinery would run that isn't a `V<n>__<name>.sql` directly under `trellis/migrations/` |
| A new expression node, operator, special form, key space, `JOIN` kind, option or hidden-table layout | A variant of `defs::floor::Feature` or of the enum it holds, whose release `Feature::since` gives | The compiler (an exhaustive match), and the manifest below |
| A new function or overload, typed literal or `CAST` target | `since` in its registry entry | The manifest below |
| A new argument value: an `EXTRACT` field, a `DATE_TRUNC` unit, an `AT TIME ZONE` zone | `since` in its `ArgValueSpec` | The manifest below |
| Anything else the validator newly admits: a pair of operand types for an operator, a comparison, a cast, `COALESCE`/`CASE` unification, `EXTRACT`'s operand, an aggregate's argument | A new case of `registry::TypeRule`, with its release, returned with the result type (`Admitted`) | The manifest below, which fails a new admission that reuses an existing case: the older release that case dates from would run the definition and read it differently |
| A new key type: a `GROUP BY` key, a `JOIN ... ON` or source primary key, a partition key | A new case of `registry::GroupByKeyRule`, or `since` in its entry of the join-key allowlist (`defs::join_key_types`) or the partition-key type list (`defs::partition_tree`) | The manifest below |
| A new value of a stored enum-like word | A raise of the floor of what stores it: the catalog floor (in the migration relaxing its `CHECK`) or the definition's (a `Feature`) | Review |

**The feature-floor manifest.** `.github/feature-floors.txt` lists every
registry entry and type-rule case with its release, and every admission the
validator's type rules make over every operand type and every key type the
`GROUP BY`, join-key and primary-key, and partition-key gates admit, with the
case or entry that admits it. The `defs::floor_manifest` unit test fails when the file and the code
differ, and when an admission records no type rule or key-type entry; regenerate the file with
`TRELLIS_BLESS_FEATURE_FLOORS=1 cargo test -p trellis --lib floor_manifest`
and review its diff. `check-floors.py check` then fails a line whose release
is above the release being developed, a line the newest released tag's manifest lacks whose
release is that tag's or below, and a line given an earlier release than the
tag's manifest gives it. Every addition the validator admits is a
feature with its own release, even one that renders the same under older
releases: that costs rollback availability, not correctness.

The release being developed is `defs::floor::DEVELOPING`, the release a build
of the tree runs as (ADR-0024): between releases `main`'s manifests keep the
last release's version, and `DEVELOPING` names the next one. A feature
merged on `main` after a release declares the release `DEVELOPING` names.
`check-floors.py check` also fails `main` while `DEVELOPING` is at or below
the newest final release tag ([releasing](docs/releasing.md#cutting-a-release),
step 4). `check-floors.py self-test` plants a violation of
each rule. Each release's notes list the floors it raised
(`check-floors.py notes`, [releasing](docs/releasing.md)).

# Creating a Pull Request

1. **Ensure the bug/feature was not already reported** by searching on GitHub under Issues.  If none exists, create a new issue so that other contributors can keep track of what you are trying to add/fix and offer suggestions (or let you know if there is already an effort in progress).
2. **Clone** the forked repo to your machine.
3. **Create** a new branch to contain your work (e.g. `git br fix-issue-11`)
4. **Commit** changes to your own branch.
5. **Push** your work back up to your fork. (e.g. `git push fix-issue-11`)
6. **Submit** a Pull Request against the `main` branch and refer to the issue(s) you are fixing. Try not to pollute your pull request with unintended changes. Keep it simple and small.
7. **Sign** the Salesforce CLA (you will be prompted to do so when submitting the Pull Request)

> **NOTE**: Be sure to [sync your fork](https://help.github.com/articles/syncing-a-fork/) before making a pull request.

# Contributor License Agreement ("CLA")
In order to accept your pull request, we need you to submit a CLA. You only need
to do this once to work on any of Salesforce's open source projects.

Complete your CLA here: <https://cla.salesforce.com/sign-cla>

# Issues
We use GitHub issues to track public bugs. Please ensure your description is
clear and has sufficient instructions to be able to reproduce the issue.

# Code of Conduct
Please follow our [Code of Conduct](CODE_OF_CONDUCT.md).

# License
By contributing your code, you agree to license your contribution under the terms of our project [LICENSE](LICENSE.txt) and to sign the [Salesforce CLA](https://cla.salesforce.com/sign-cla)