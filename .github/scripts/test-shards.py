#!/usr/bin/env python3
"""Splits the workspace test suite across the CI `test` shards.

A *unit* is what one `cargo test` selector names:

  lib          every library unit-test binary (`--lib`; cargo cannot pick one
               package's lib out of a `--workspace` run without changing the
               feature set, which rebuilds everything)
  bin:NAME     a binary target's unit tests (`--bin NAME`)
  test:NAME    an integration-test binary (`--test NAME`)
  doc          the doc-tests (`--doc`, a separate cargo invocation)

The unit list comes from `cargo metadata`, so a new test file is a new unit
with no edit anywhere. Assignment is greedy longest-first over the per-unit
seconds in .github/test-shards.txt; a unit the file does not list gets the
median weight, so it still runs (and a warning annotation says to refresh the
file). Correctness never depends on the weights: the partition is a pure
function of the unit list, and `run` fails if the binaries cargo built for a
shard differ from the shard's units.

  test-shards.py check              partition invariants + ci.yml shard matrix
  test-shards.py plan               the table: units per shard and its weight
  test-shards.py run SHARD          build, verify, then run one shard (CI)
  test-shards.py weights LOG...     a fresh test-shards.txt from `cargo test`
                                    output (see docs/testing-strategy.md)

The cargo flags are the ones `cargo test --workspace` needs here: the benchmark binary's `engine-access` feature and `--no-fail-fast`.
"""

import json
import os
import re
import statistics
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WEIGHTS = os.path.join(ROOT, ".github", "test-shards.txt")
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "ci.yml")
# What the old single `Test` step ran: `--features benchmark/engine-access`
# because the benchmark binary has `required-features` (issue #509);
# `--no-fail-fast` so one run shows every failure (issue #665).
CARGO_FLAGS = ["--workspace", "--features", "benchmark/engine-access", "--no-fail-fast"]
DOC_DEFAULT_SECONDS = 15.0


def die(msg):
    print(f"::error::{msg}", flush=True)
    sys.exit(1)


def units():
    """Every unit the workspace's tests are made of, from cargo metadata."""
    meta = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"], cwd=ROOT
        )
    )
    out = set()
    for pkg in meta["packages"]:
        for t in pkg["targets"]:
            if not t.get("test", False):
                continue
            kind = t["kind"][0]
            if kind == "lib":
                out.add("lib")
                if t.get("doctest", False):
                    out.add("doc")
            elif kind == "bin":
                out.add(f"bin:{t['name']}")
            elif kind == "test":
                out.add(f"test:{t['name']}")
    return sorted(out)


def read_weights():
    w = {}
    with open(WEIGHTS) as f:
        for n, line in enumerate(f, 1):
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) != 2:
                die(f"{WEIGHTS}:{n}: expected `<unit> <seconds>`")
            w[parts[0]] = float(parts[1])
    return w


def assign(all_units, weights, n_shards):
    """Greedy longest-first, deterministic: ties break on unit name, then on
    the lowest shard index. Returns (shards, unweighted units)."""
    known = [weights[u] for u in all_units if u in weights]
    fallback = statistics.median(known) if known else 1.0
    missing = [u for u in all_units if u not in weights]
    w = {u: weights.get(u, fallback) for u in all_units}
    shards = [{"units": [], "seconds": 0.0} for _ in range(n_shards)]
    for u in sorted(all_units, key=lambda u: (-w[u], u)):
        s = min(range(n_shards), key=lambda i: (shards[i]["seconds"], i))
        shards[s]["units"].append(u)
        shards[s]["seconds"] += w[u]
    return shards, missing, w


def shard_count():
    with open(WORKFLOW) as f:
        text = f.read()
    m = re.search(r"^\s*TEST_SHARDS:\s*(\d+)\s*$", text, re.M)
    if not m:
        die(f"{WORKFLOW}: no `TEST_SHARDS: <n>` env entry")
    n = int(m.group(1))
    m = re.search(r"^\s*shard:\s*\[([^\]]*)\]\s*$", text, re.M)
    listed = [int(x) for x in m.group(1).split(",")] if m else []
    if listed != list(range(1, n + 1)):
        die(
            f"{WORKFLOW}: the `shard:` matrix {listed} must be exactly 1..TEST_SHARDS "
            f"(= {n}), or a shard's tests would never run"
        )
    return n


def selectors(unit_list):
    args = []
    for u in unit_list:
        if u == "lib":
            args += ["--lib"]
        elif u.startswith("bin:"):
            args += ["--bin", u[4:]]
        elif u.startswith("test:"):
            args += ["--test", u[5:]]
    return args


def plan():
    n = shard_count()
    all_units = units()
    weights = read_weights()
    shards, missing, w = assign(all_units, weights, n)
    stale = sorted(set(weights) - set(all_units))
    return n, all_units, shards, missing, stale, w


def cmd_check():
    n, all_units, shards, missing, stale, w = plan()
    seen = {}
    for i, s in enumerate(shards, 1):
        for u in s["units"]:
            if u in seen:
                die(f"unit {u} is in shards {seen[u]} and {i}")
            seen[u] = i
    if sorted(seen) != all_units:
        die(f"shards cover {len(seen)} units, the workspace has {len(all_units)}")
    for i, s in enumerate(shards, 1):
        if not s["units"]:
            die(f"shard {i} has no units; lower TEST_SHARDS")
    annotate(missing, stale)
    print(f"ok: {len(all_units)} units in {n} disjoint shards covering all of them")


def annotate(missing, stale):
    if missing:
        print(
            f"::warning::{len(missing)} test unit(s) are not in .github/test-shards.txt and "
            f"run at the median weight (refresh it, see docs/testing-strategy.md): "
            + ", ".join(missing)
        )
    if stale:
        print(
            f"::warning::.github/test-shards.txt names {len(stale)} unit(s) that no longer "
            f"exist: " + ", ".join(stale)
        )


def cmd_plan():
    n, all_units, shards, missing, stale, w = plan()
    for i, s in enumerate(shards, 1):
        print(f"shard {i}: {len(s['units']):3d} units  {s['seconds']:7.1f} s")
        if "-v" in sys.argv:
            for u in s["units"]:
                print(f"    {w[u]:7.1f}  {u}")
    annotate(missing, stale)


def built_units(json_lines):
    """The units whose test executables a `--message-format=json*` build made."""
    got = set()
    for line in json_lines:
        try:
            m = json.loads(line)
        except ValueError:
            continue
        if m.get("reason") != "compiler-artifact" or not m.get("executable"):
            continue
        if not m.get("profile", {}).get("test"):
            continue
        kind, name = m["target"]["kind"][0], m["target"]["name"]
        if kind == "lib":
            got.add("lib")
        elif kind == "bin":
            got.add(f"bin:{name}")
        elif kind == "test":
            got.add(f"test:{name}")
    return got


def cmd_run(shard):
    n, all_units, shards, missing, stale, w = plan()
    if not 1 <= shard <= n:
        die(f"shard {shard} is outside 1..{n}")
    annotate(missing, stale)
    mine = shards[shard - 1]["units"]
    print(f"shard {shard}/{n}: {len(mine)} units, ~{shards[shard - 1]['seconds']:.0f} s", flush=True)
    sel = selectors([u for u in mine if u != "doc"])
    rc = 0
    if sel:
        # Build first, so the binaries cargo made can be checked against the
        # assignment before any test runs. The run below reuses this build.
        # `json-render-diagnostics`: plain `json` would bury compiler errors
        # and warnings in the captured stdout, leaving the log only "could
        # not compile".
        build = subprocess.run(
            [
                "cargo", "test", *CARGO_FLAGS, *sel, "--no-run",
                "--message-format=json-render-diagnostics",
            ],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            text=True,
        )
        if build.returncode != 0:
            die("the shard's test build failed")
        want = {u for u in mine if u != "doc"}
        got = built_units(build.stdout.splitlines())
        if got != want:
            die(
                f"shard {shard} was assigned {sorted(want)} but cargo built {sorted(got)}; "
                f"missing {sorted(want - got)}, extra {sorted(got - want)} (a target's "
                f"`required-features` not enabled by CARGO_FLAGS?)"
            )
        rc = subprocess.run(["cargo", "test", *CARGO_FLAGS, *sel], cwd=ROOT).returncode
    if "doc" in mine:
        rc = subprocess.run(["cargo", "test", *CARGO_FLAGS, "--doc"], cwd=ROOT).returncode or rc
    sys.exit(rc)


RUNNING = re.compile(r"Running(?:(?:\x1b|\^\[)\[[0-9;]*m|\s)+(unittests (\S+)|(\S+))\s+\((?:\S*/)?deps/([^/\s]+)-[0-9a-f]{16}\)")
FINISHED = re.compile(r"finished in ([0-9.]+)s")


def cmd_weights(paths):
    """Parse the output of `cargo test --workspace ...` (CI log prefixes and
    colour codes tolerated) into a weights file on stdout."""
    all_units = units()
    by_norm = {u.split(":", 1)[-1].replace("-", "_"): u for u in all_units if u != "lib"}
    secs = {}
    current = None
    for path in paths:
        with open(path, errors="replace") as f:
            for line in f:
                m = RUNNING.search(line)
                if m:
                    src = m.group(2) or m.group(3)
                    exe = m.group(4).replace("-", "_")
                    if m.group(2) and src.endswith("src/lib.rs"):
                        current = "lib"
                    elif m.group(2):
                        current = next((u for u in all_units if u.startswith("bin:") and u[4:].replace("-", "_") == exe), None)
                    else:
                        current = by_norm.get(exe)
                    continue
                m = FINISHED.search(line)
                if m and current:
                    secs[current] = secs.get(current, 0.0) + float(m.group(1))
                    current = None
    print("# Seconds each test unit took on a CI runner (`finished in`, summed), used only to")
    print("# balance the `test` shards in ci.yml; see docs/testing-strategy.md to refresh.")
    print("# A unit missing here still runs, at the median weight, with a CI warning.")
    print(f"doc {DOC_DEFAULT_SECONDS:g}")
    for u in sorted(secs, key=lambda u: (-secs[u], u)):
        print(f"{u} {secs[u]:.1f}")
    for u in all_units:
        if u not in secs and u != "doc":
            print(f"{u} 1.0  # not in the log (no tests ran, or the log is partial)")


def main():
    a = sys.argv[1:]
    if a[:1] == ["check"]:
        cmd_check()
    elif a[:1] == ["plan"]:
        cmd_plan()
    elif a[:1] == ["run"] and len(a) == 2:
        cmd_run(int(a[1]))
    elif a[:1] == ["weights"] and len(a) >= 2:
        cmd_weights(a[1:])
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
