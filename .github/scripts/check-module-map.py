#!/usr/bin/env python3
"""Fails when the module map and the source tree disagree.

docs/architecture.md names modules as backticked paths (`staging::fold`,
`defs::catalog`, `staging::build::orphans`). Two checks:
  * every `*.rs` file directly in trellis/src/staging and trellis/src/defs, except mod.rs,
    is named in the map;
  * every `staging::...` or `defs::...` path the map names is a module (a file, or a
    directory with a mod.rs).

Run: python3 .github/scripts/check-module-map.py   (tests: test_check_module_map.py)
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MAP = "docs/architecture.md"
SRC = "trellis/src"
LISTED_DIRS = ("staging", "defs")
NAME = re.compile(r"`((?:staging|defs)(?:::\w+)+)`")


def named(text):
    """The module paths `text` names, as `staging::fold` strings."""
    return set(NAME.findall(text))


def is_module(root, path):
    """Whether `staging::a::b` is trellis/src/staging/a/b.rs or .../b/mod.rs."""
    base = os.path.join(root, SRC, *path.split("::"))
    return os.path.isfile(base + ".rs") or os.path.isfile(os.path.join(base, "mod.rs"))


def check(root):
    """The problems found in the map under `root` against its source tree."""
    try:
        with open(os.path.join(root, MAP), encoding="utf-8") as f:
            listed = named(f.read())
    except FileNotFoundError:
        return [f"{MAP}: the module map is missing"]
    problems = []
    for d in LISTED_DIRS:
        folder = os.path.join(root, SRC, d)
        for name in sorted(os.listdir(folder)):
            stem = name[:-3]
            if name.endswith(".rs") and stem != "mod" and f"{d}::{stem}" not in listed:
                problems.append(f"{SRC}/{d}/{name}: not listed in {MAP} (as `{d}::{stem}`)")
    for path in sorted(listed):
        if not is_module(root, path):
            problems.append(f"{MAP}: `{path}` is not a module under {SRC}")
    return problems


def main():
    problems = check(ROOT)
    for p in problems:
        print(p)
    if problems:
        print(f"{len(problems)} module map problem(s)")
        return 1
    print(f"ok: {MAP} lists every module of {', '.join(LISTED_DIRS)} and names only modules")
    return 0


if __name__ == "__main__":
    sys.exit(main())
