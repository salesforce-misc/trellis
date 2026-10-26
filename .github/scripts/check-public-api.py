#!/usr/bin/env python3
"""Checks that the `trellis` crate's public API is exactly the committed list.

Every `pub` item reachable from the crate root in the published build (no
cargo features) is listed in .github/public-api.txt, one `kind path` per line
(enum variants and struct fields are left out; the types they belong to are
listed). Adding or removing a public item is therefore a visible diff in
review.

  check-public-api.py            compare the crate's API with the list
  check-public-api.py --update   rewrite the list from the crate

The API comes from rustdoc's JSON output, which needs a nightly flag
(`RUSTC_BOOTSTRAP=1` on the pinned stable toolchain) and so is read only
here, never by a build.
"""
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LIST = os.path.join(ROOT, ".github", "public-api.txt")


def rustdoc_json():
    env = dict(os.environ, RUSTC_BOOTSTRAP="1")
    subprocess.run(
        ["cargo", "rustdoc", "-p", "trellis", "--",
         "-Z", "unstable-options", "--output-format", "json"],
        cwd=ROOT, env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    target = os.environ.get("CARGO_TARGET_DIR", os.path.join(ROOT, "target"))
    with open(os.path.join(target, "doc", "trellis.json")) as f:
        return json.load(f)


def public_api(doc):
    index = doc["index"]
    out = set()

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
                if tk == "module":
                    walk(use["id"], path + [name])
                elif tk in ("struct", "enum", "trait", "union"):
                    members(target, path + [name])
            elif k == "module":
                out.add(("module", tuple(path + [item["name"]])))
                walk(child_id, path + [item["name"]])
            else:
                out.add((k, tuple(path + [item["name"]])))
                if k in ("struct", "enum", "trait", "union"):
                    members(item, path + [item["name"]])

    walk(doc["root"], [])
    return sorted(f"{k} {'::'.join(p)}" for k, p in out)


def main():
    lines = public_api(rustdoc_json())
    if "--update" in sys.argv:
        with open(LIST, "w") as f:
            f.write("\n".join(lines) + "\n")
        print(f"wrote {len(lines)} items to {os.path.relpath(LIST, ROOT)}")
        return 0
    with open(LIST) as f:
        committed = f.read().split("\n")[:-1]
    added = sorted(set(lines) - set(committed))
    removed = sorted(set(committed) - set(lines))
    if not added and not removed:
        print(f"public API matches .github/public-api.txt ({len(lines)} items)")
        return 0
    for line in added:
        print(f"+ {line}")
    for line in removed:
        print(f"- {line}")
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
