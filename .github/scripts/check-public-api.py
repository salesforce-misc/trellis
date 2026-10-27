#!/usr/bin/env python3
"""Checks that the `trellis` crate's public API is exactly the committed list.

Every `pub` item reachable from the crate root in the published build (no
cargo features) is listed in .github/public-api.txt, one `kind path` per line
(enum variants and struct fields are left out; the types they belong to are
listed). Adding or removing a public item is therefore a visible diff in
review.

  check-public-api.py            compare the crate's API with the list, and
                                 require `#[non_exhaustive]` where it applies
  check-public-api.py --update   rewrite the list from the crate

The API comes from rustdoc's JSON output, which needs a nightly flag
(`RUSTC_BOOTSTRAP=1` on the pinned stable toolchain) and so is read only
here, never by a build.

Every public enum, and every public struct with a `pub` field, must be
`#[non_exhaustive]`, so a variant or field can be added without a breaking
change. A struct that embedders build with a struct literal is the exception;
it is listed in INPUT_STRUCTS with the reason.
"""
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LIST = os.path.join(ROOT, ".github", "public-api.txt")


# Public structs with `pub` fields that are options an embedder builds with a
# struct literal (docs/embedding.md shows `TrellisOptions { .., ..Default::default() }`),
# so `#[non_exhaustive]` would break the documented use. Name -> reason.
INPUT_STRUCTS = {
    "TrellisOptions": "input: embedders build it with a struct literal and `..Default::default()`",
    "ClientOptions": "input: embedders build it with a struct literal and `..Default::default()`",
    "HeldKeyRetry": "input: a field of ClientOptions, built the same way",
}


def rustdoc_json():
    env = dict(os.environ, RUSTC_BOOTSTRAP="1")
    build = subprocess.run(
        ["cargo", "rustdoc", "-p", "trellis", "--",
         "-Z", "unstable-options", "--output-format", "json"],
        cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    if build.returncode != 0:
        sys.stderr.write(build.stderr)
        sys.exit("check-public-api: building the rustdoc JSON failed")
    metadata = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    )
    target = json.loads(metadata.stdout)["target_directory"]
    with open(os.path.join(target, "doc", "trellis.json")) as f:
        return json.load(f)


def public_api(doc):
    """The sorted `kind path` lines, and the ids of the listed structs and enums."""
    index = doc["index"]
    out = set()
    nameable = set()

    def get(i):
        return index.get(str(i))

    def kind(item):
        return next(iter(item["inner"]))

    def members(item, path):
        inner = item["inner"][kind(item)]
        for impl_id in inner.get("impls", []):
            impl = get(impl_id)
            if impl is None:
                continue
            body = impl["inner"]["impl"]
            if body["trait"] is not None:
                continue
            for member_id in body["items"]:
                member = get(member_id)
                if member and member["visibility"] == "public":
                    out.add((kind(member), tuple(path + [member["name"]])))

    def walk(module_id, path):
        for child_id in get(module_id)["inner"]["module"]["items"]:
            item = get(child_id)
            if item is None or item["visibility"] != "public":
                continue
            k = kind(item)
            if k == "use":
                use = item["inner"]["use"]
                target = get(use["id"]) if use.get("id") is not None else None
                name = use["name"]
                if use["is_glob"]:
                    if target is not None and kind(target) == "module":
                        walk(use["id"], path)
                    continue
                if target is None:
                    out.add(("extern", tuple(path + [name])))
                    continue
                tk = kind(target)
                out.add((tk, tuple(path + [name])))
                if tk in ("struct", "enum"):
                    nameable.add(use["id"])
                if tk == "module":
                    walk(use["id"], path + [name])
                elif tk in ("struct", "enum", "trait", "union"):
                    members(target, path + [name])
            elif k == "module":
                out.add(("module", tuple(path + [item["name"]])))
                walk(child_id, path + [item["name"]])
            else:
                out.add((k, tuple(path + [item["name"]])))
                if k in ("struct", "enum"):
                    nameable.add(child_id)
                if k in ("struct", "enum", "trait", "union"):
                    members(item, path + [item["name"]])

    walk(doc["root"], [])
    return sorted(f"{k} {'::'.join(p)}" for k, p in out), nameable


def non_exhaustive_violations(doc, nameable):
    """Listed enums and pub-field structs lacking `#[non_exhaustive]`.

    Only items an embedder can name by path are checked: a type reachable only
    through another type's variants or fields (an engine type inside an error
    payload) cannot be matched or built by name, so the attribute would buy
    nothing there.
    """
    index = doc["index"]
    problems = []
    seen_inputs = set()
    for item_id in sorted(nameable, key=str):
        item = index[str(item_id)]
        kind = next(iter(item["inner"]))
        name = item["name"]
        if kind == "enum":
            needs = True
        elif kind == "struct":
            shape = item["inner"]["struct"]["kind"]
            if isinstance(shape, dict) and "plain" in shape:
                fields = shape["plain"]["fields"]
            elif isinstance(shape, dict) and "tuple" in shape:
                # A tuple struct lists each private field as null.
                fields = [f for f in shape["tuple"] if f is not None]
            else:
                fields = []
            needs = any(index[str(f)]["visibility"] == "public" for f in fields)
            if needs and name in INPUT_STRUCTS:
                seen_inputs.add(name)
                needs = False
        else:
            continue
        if needs and not any("non_exhaustive" in a for a in item["attrs"]):
            problems.append(f"{kind} {name} ({item['span']['filename']}) is public but not #[non_exhaustive]")
    for name in sorted(set(INPUT_STRUCTS) - seen_inputs):
        problems.append(f"INPUT_STRUCTS lists {name}, which is not a public struct with pub fields; remove it")
    return sorted(problems)


def main():
    doc = rustdoc_json()
    lines, nameable = public_api(doc)
    if "--update" in sys.argv:
        with open(LIST, "w") as f:
            f.write("\n".join(lines) + "\n")
        print(f"wrote {len(lines)} items to {os.path.relpath(LIST, ROOT)}")
        return 0
    with open(LIST) as f:
        committed = f.read().split("\n")[:-1]
    added = sorted(set(lines) - set(committed))
    removed = sorted(set(committed) - set(lines))
    problems = non_exhaustive_violations(doc, nameable)
    for problem in problems:
        print(f"! {problem}")
    if problems:
        sys.stdout.flush()
        print(
            "\nA public enum or a public struct with `pub` fields must be "
            "`#[non_exhaustive]`; an options struct embedders build with a "
            "literal goes in INPUT_STRUCTS in this script, with the reason.",
            file=sys.stderr,
        )
    if not added and not removed and not problems:
        print(f"public API matches .github/public-api.txt ({len(lines)} items)")
        return 0
    if not added and not removed:
        return 1
    for line in added:
        print(f"+ {line}")
    for line in removed:
        print(f"- {line}")
    sys.stdout.flush()
    print(
        "\nThe trellis crate's public API changed. If that is intended, run\n"
        "  .github/scripts/check-public-api.py --update\n"
        "and commit the list; otherwise make the item `pub(crate)`, or put it "
        "behind the `dev` module.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
