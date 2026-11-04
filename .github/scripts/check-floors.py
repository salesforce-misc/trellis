#!/usr/bin/env python3
"""Checks the compatibility-floor declarations (ADR-0024) CI can't leave to review.

  check-floors.py check            the migration headers and the feature-floor
                                   manifest (below); exit 1 naming each violation
  check-floors.py notes [TAG]      the floors raised since TAG (default: the
                                   newest released tag), for the release notes
  check-floors.py self-test        plants each violation and expects `check`'s
                                   rules to fail it, and the repository's own
                                   files to pass

Migrations. Every file under trellis/migrations/ from the floor migration on
(the one that creates `catalog_floor`) opens with a comment block declaring,
on a line of its own, one of:

  -- Declares no catalog floor ...
  -- Raises the catalog floor to X.Y.Z ...

A raise carries, in its SQL, the update the floor migration documents, to the
greater of the stored floor and its own:

  update catalog_floor set version = 'X.Y.Z'
   where string_to_array(version, '.')::int[] < '{X,Y,Z}';

`check` fails a migration without the declaration; a raise without that
update, or with another version; a write to `catalog_floor` under "Declares no
catalog floor"; a raise above the crate's release; and a raise, in a migration
the newest released tag lacks, to that tag's release or below, which that
release would read as a catalog it can.

Feature floors. `.github/feature-floors.txt` lists every registry entry and
type-rule case with its release, and every admission the validator's type
rules make (trellis's `defs::floor_manifest` test keeps it equal to the code).
`check` fails a line whose release is above the crate's, and a line the newest
released tag's copy lacks (a new feature, overload or admission) whose release
is that tag's or below: that release would run a definition using it and read
it differently.

The crate's release is trellis/Cargo.toml's `major.minor.patch`, never below
0.2.0 (`defs::floor::BASELINE`: a development build whose manifest still
carries 0.1.0 belongs to 0.2.0, the release floors arrive in). The newest
released tag is the highest `vX.Y.Z` tag (no pre-release) at or below it. The
tags must be fetched (CI fetches them in the `lint` job).
"""
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MIGRATIONS = "trellis/migrations"
MANIFEST = ".github/feature-floors.txt"
BASELINE = (0, 2, 0)

# Migrations from the floor migration on that predate the header rule. Their
# text can't change: every database that ran them recorded its checksum, and
# a checksum mismatch refuses the attach (ADR-0024 rule 2). Their declaration
# is recorded here instead, and checked against their SQL like a header.
GRANDFATHERED = {
    12: None,  # V12__definition_floor.sql: an additive column, no catalog floor
}

DECLARE_NONE = re.compile(r"^--\s*Declares no catalog floor\b")
DECLARE_RAISE = re.compile(r"^--\s*Raises the catalog floor to (\S+?)[.,:;)]?(\s|$)")
TOUCH = re.compile(
    r"\b(update|insert\s+into|delete\s+from|alter\s+table|drop\s+table|truncate(\s+table)?)"
    r"\s+(\w+\.)?catalog_floor\b",
    re.I,
)
RAISE_SQL = re.compile(
    r"\bupdate\s+catalog_floor\s+set\s+version\s*=\s*'([^']*)'\s+"
    r"where\s+string_to_array\(\s*version\s*,\s*'\.'\s*\)\s*::\s*int\[\]\s*<\s*"
    r"'\{\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\}'",
    re.I,
)
SEED_SQL = re.compile(r"\binsert\s+into\s+catalog_floor\s*\(\s*version\s*\)\s*values\s*\(\s*'([^']*)'", re.I)
CREATES_FLOOR = re.compile(r"\bcreate\s+table\s+catalog_floor\b", re.I)


def release(text):
    """`major.minor.patch` as a tuple, or None (a pre-release included)."""
    m = re.fullmatch(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)", text or "")
    return tuple(int(p) for p in m.groups()) if m else None


def show(r):
    return ".".join(map(str, r))


def migration_version(name):
    m = re.match(r"V(\d+)__.*\.sql$", name)
    return int(m.group(1)) if m else None


def header(text):
    """The opening comment block's lines."""
    lines = []
    for line in text.splitlines():
        if line.startswith("--"):
            lines.append(line)
        elif line.strip():
            break
    return lines


def strip_comments(text):
    return "\n".join(line.split("--", 1)[0] for line in text.splitlines())


def declaration(version, text):
    """("none", None), ("raise", release-or-text), or an error string."""
    found = []
    for line in header(text):
        if DECLARE_NONE.match(line):
            found.append(("none", None))
        m = DECLARE_RAISE.match(line)
        if m:
            found.append(("raise", m.group(1)))
    if not found and version in GRANDFATHERED:
        g = GRANDFATHERED[version]
        return ("none", None) if g is None else ("raise", g)
    if len(found) != 1:
        return (
            "no declaration: its opening comment needs a line '-- Declares no catalog floor' "
            "or '-- Raises the catalog floor to X.Y.Z' (CONTRIBUTING.md)"
            if not found
            else "more than one catalog-floor declaration"
        )
    return found[0]


def check_migrations(files, crate, tag, tag_files):
    """files: {name: text} at HEAD. tag_files: the names the newest released
    tag carries. Returns (violations, [(name, kind, floor)])."""
    bad, floors = [], []
    ordered = sorted(
        (n for n in files if migration_version(n) is not None), key=migration_version
    )
    start = next((n for n in ordered if CREATES_FLOOR.search(strip_comments(files[n]))), None)
    if start is None:
        return bad, floors
    for name in ordered[ordered.index(start):]:
        version, sql = migration_version(name), strip_comments(files[name])
        where = f"{MIGRATIONS}/{name}"
        if name == start:
            seed = SEED_SQL.search(sql)
            if not seed or not release(seed.group(1)):
                bad.append(f"{where}: the floor migration seeds no release into catalog_floor")
                continue
            floors.append((name, "seed", release(seed.group(1))))
            continue
        decl = declaration(version, files[name])
        if isinstance(decl, str):
            bad.append(f"{where}: {decl}")
            continue
        kind, value = decl
        touches = [m.group(0) for m in TOUCH.finditer(sql)]
        if kind == "none":
            if touches:
                bad.append(
                    f"{where}: declares no catalog floor but writes catalog_floor "
                    f"({touches[0]!r}); declare the raise in its header"
                )
            floors.append((name, "none", None))
            continue
        floor = release(value)
        if floor is None:
            bad.append(f"{where}: raises the catalog floor to {value!r}, which is not a release X.Y.Z")
            continue
        raises = RAISE_SQL.findall(sql)
        if not raises:
            bad.append(
                f"{where}: declares a raise to {show(floor)} but its SQL has no "
                f"`update catalog_floor set version = '{show(floor)}' where "
                f"string_to_array(version, '.')::int[] < '{{{','.join(map(str, floor))}}}'`"
            )
        elif len(touches) != len(raises) or any(
            release(v) != floor or tuple(int(p) for p in g) != floor for v, *g in raises
        ):
            bad.append(
                f"{where}: declares a raise to {show(floor)} but its SQL writes catalog_floor "
                f"otherwise ({', '.join(repr(t) for t in touches)})"
            )
        if floor > crate:
            bad.append(
                f"{where}: raises the catalog floor to {show(floor)}, above the crate's release "
                f"{show(crate)}"
            )
        if tag is not None and name not in tag_files and floor <= tag[1]:
            bad.append(
                f"{where}: raises the catalog floor to {show(floor)}, but {tag[0]} was released "
                f"without this migration and would attach to the catalog it leaves; raise it "
                f"after {show(tag[1])}"
            )
        floors.append((name, "raise", floor))
    return bad, floors


def manifest_lines(text):
    """{identity: (line, release)} of a manifest's text. The identity is the
    line without its release, and for an admission, without the case either:
    an admission is new when its operands are, whatever case admits it."""
    out = {}
    for line in (text or "").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        body, _, since = line.rpartition(" ")
        identity = body.split(" by ", 1)[0] if line.startswith("admit ") else body
        out[identity] = (line, release(since))
    return out


def check_manifest(text, crate, tag, tag_text):
    bad = []
    old = manifest_lines(tag_text)
    for identity, (line, since) in manifest_lines(text).items():
        if since is None:
            bad.append(f"{MANIFEST}: {line!r} has no release")
            continue
        if since > crate:
            bad.append(
                f"{MANIFEST}: {line!r} arrives in {show(since)}, above the crate's release "
                f"{show(crate)}"
            )
        if tag is not None and identity not in old and since <= tag[1]:
            bad.append(
                f"{MANIFEST}: {line!r} is new since {tag[0]} but claims release {show(since)}; "
                f"{tag[0]} would run a definition using it and read it differently. Give it "
                f"a release after {show(tag[1])} (a new type rule case for a new admission)"
            )
    return bad


# ------------------------------------------------------------------ git

def git(*args):
    return subprocess.run(
        ["git", "-C", ROOT, *args], check=True, capture_output=True, text=True
    ).stdout


def git_show(rev, path):
    r = subprocess.run(
        ["git", "-C", ROOT, "show", f"{rev}:{path}"], capture_output=True, text=True
    )
    return r.stdout if r.returncode == 0 else None


def crate_release():
    with open(os.path.join(ROOT, "trellis", "Cargo.toml")) as f:
        m = re.search(r'^version = "(\d+\.\d+\.\d+)', f.read(), re.M)
    return max(release(m.group(1)), BASELINE)


def released_tags():
    tags = []
    for t in git("tag", "--list", "v*").split():
        r = release(t[1:])
        if r:
            tags.append((t, r))
    return sorted(tags, key=lambda t: t[1])


def newest_tag(crate):
    tags = [t for t in released_tags() if t[1] <= crate]
    if not tags:
        sys.exit(
            "check-floors: no released tag vX.Y.Z at or below the crate's release "
            f"{show(crate)}; fetch the tags (git fetch --tags)"
        )
    return tags[-1]


def head_migrations():
    d = os.path.join(ROOT, MIGRATIONS)
    out = {}
    for n in os.listdir(d):
        with open(os.path.join(d, n)) as f:
            out[n] = f.read()
    return out


def tag_migrations(tag):
    return {
        os.path.basename(p): git_show(tag, p)
        for p in git("ls-tree", "--name-only", tag, f"{MIGRATIONS}/").split()
    }


def read(path):
    with open(os.path.join(ROOT, path)) as f:
        return f.read()


def cmd_check():
    crate = crate_release()
    tag = newest_tag(crate)
    bad, _ = check_migrations(head_migrations(), crate, tag, set(tag_migrations(tag[0])))
    bad += check_manifest(read(MANIFEST), crate, tag, git_show(tag[0], MANIFEST))
    for b in bad:
        print(f"FAIL {b}")
    print(
        f"check-floors: {len(bad)} violation(s); crate release {show(crate)}, "
        f"newest released tag {tag[0]}"
    )
    return 1 if bad else 0


def cmd_notes(tag_name):
    crate = crate_release()
    if tag_name:
        r = release(tag_name.lstrip("v"))
        if not r:
            sys.exit(f"check-floors: {tag_name} is not a release tag vX.Y.Z")
        tag = (tag_name, r)
    else:
        tag = newest_tag(crate)
    old_files = tag_migrations(tag[0])
    _, old_floors = check_migrations(old_files, crate, None, set())
    _, floors = check_migrations(head_migrations(), crate, None, set())
    old_floor = max((f for _, k, f in old_floors if f), default=None)
    print(f"Compatibility floors raised since {tag[0]} (ADR-0024):")
    print("- Catalog floor:")
    raised = [(n, k, f) for n, k, f in floors if n not in old_files and k != "none"]
    for name, kind, floor in raised:
        verb = "records the catalog floor at" if kind == "seed" else "raises the catalog floor to"
        print(f"  - {name} {verb} {show(floor)}")
    new_floor = max((f for _, k, f in floors if f), default=None)
    if new_floor is None:
        print("  - not recorded")
    elif old_floor is None:
        print(
            f"  - the catalog floor is {show(new_floor)}; {tag[0]} records none, so it can't check "
            f"it: rolling back to {tag[0]} means restoring a backup taken before the upgrade"
        )
    elif new_floor > old_floor:
        print(
            f"  - raised from {show(old_floor)} to {show(new_floor)}: {tag[0]} refuses this "
            f"release's catalog (version_floor); rolling back means restoring a backup"
        )
    else:
        print(f"  - unchanged at {show(new_floor)}: {tag[0]} attaches to this release's catalog")
    print("- Definition floors:")
    old_text = git_show(tag[0], MANIFEST)
    lines = manifest_lines(read(MANIFEST))
    if old_text is None:
        releases = sorted({s for _, s in lines.values() if s})
        print(
            f"  - definition floors arrive in this release: every feature ({len(lines)} manifest "
            f"lines) has floor {', '.join(map(show, releases))}"
        )
        return 0
    old = manifest_lines(old_text)
    newer = [
        line for identity, (line, s) in sorted(lines.items())
        if s and s > tag[1] and (identity not in old or old[identity][0] != line)
    ]
    for line in newer:
        print(f"  - {line}")
    if not newer:
        print(f"  - none: {tag[0]} runs every definition this release can define")
    else:
        print(f"  - a definition using one of these pauses under {tag[0]} until rolled forward")
    return 0


# ------------------------------------------------------------------ self-test

def cmd_self_test():
    crate, tag = (0, 3, 0), ("v0.2.0", (0, 2, 0))
    floor_mig = (
        "-- The catalog floor.\ncreate table catalog_floor (version text);\n"
        "insert into catalog_floor (version) values ('0.2.0');\n"
    )
    none = "-- A column.\n--\n-- Declares no catalog floor: nullable.\nalter table t add column c int;\n"
    # As #1606 writes V14: an additive CHECK, declaring no floor.
    v14 = (
        "-- `partition_corrections.cause` takes only the words a release writes.\n--\n"
        "-- Declares no catalog floor: every release writes only these words (0.1.0\n"
        "-- included), so the check holds on every existing row.\n"
        "alter table partition_corrections\n    add constraint partition_corrections_cause_check "
        "check (cause in ('created', 'complement'));\n"
    )

    def raise_to(v, sql_v=None, guard=None):
        sql_v = sql_v or v
        guard = guard or "{" + sql_v.replace(".", ",") + "}"
        return (
            f"-- A new format.\n--\n-- Raises the catalog floor to {v}: older releases can't read it.\n"
            f"alter table t add column d int not null;\n"
            f"update catalog_floor set version = '{sql_v}'\n"
            f" where string_to_array(version, '.')::int[] < '{guard}';\n"
        )

    base = {"V11__floor.sql": floor_mig, "V13__a.sql": none, "V14__cause.sql": v14}
    cases = [
        ("passes: none, V14 as #1606 writes it, a raise to the crate's release",
         {"V15__r.sql": raise_to("0.3.0")}, True),
        ("a migration without the header",
         {"V15__r.sql": "-- A column.\nalter table t add column c int;\n"}, False),
        ("a raise without the floor update",
         {"V15__r.sql": "-- Raises the catalog floor to 0.3.0.\nalter table t add column c int;\n"}, False),
        ("a floor update without the header",
         {"V15__r.sql": none + "update catalog_floor set version = '0.3.0' where string_to_array(version, '.')::int[] < '{0,3,0}';\n"}, False),
        ("a floor update to another version than the header's",
         {"V15__r.sql": raise_to("0.3.0", sql_v="0.2.5")}, False),
        ("a floor update whose guard names another version",
         {"V15__r.sql": raise_to("0.3.0", guard="{0,2,0}")}, False),
        ("a raise above the crate's release",
         {"V15__r.sql": raise_to("0.4.0")}, False),
        ("a raise, new since the tag, to the tag's release",
         {"V15__r.sql": raise_to("0.2.0")}, False),
        ("a V12 header-less file is grandfathered as no floor",
         {"V12__def.sql": "-- Each definition's floor.\nalter table d add column f text;\n"}, True),
        ("a grandfathered file that writes the floor",
         {"V12__def.sql": "-- x\nupdate catalog_floor set version = '0.3.0';\n"}, False),
    ]
    failures = []
    for label, extra, ok in cases:
        files = {**base, **extra}
        bad, _ = check_migrations(files, crate, tag, set(base))
        if (not bad) != ok:
            failures.append(f"migrations: {label}: expected {'pass' if ok else 'a violation'}, got {bad}")
    old = (
        "feature Function(\"ABS\") 0.2.0\n"
        "admit cast(Integer(Int4) as Numeric) = Some(Numeric) by Cast(IntegerToNumeric) 0.2.0\n"
    )
    mcases = [
        ("passes: the tag's lines, and new ones after the tag", old
         + "feature Function(\"SIGN\") 0.3.0\n"
         + "admit cast(Integer(Int2) as Numeric) = Some(Numeric) by Cast(Int2ToNumeric) 0.3.0\n", True),
        ("a since above the crate's release", old + "feature Function(\"SIGN\") 0.4.0\n", False),
        ("a new registry entry claiming the tag's release", old + "feature Function(\"SIGN\") 0.2.0\n", False),
        ("a new overload claiming an old release", old + "overload ABS[Integer(Int2)] = Integer(Int2) 0.2.0\n", False),
        ("a new admission reusing a case that existed at the tag", old
         + "admit cast(Integer(Int2) as Numeric) = Some(Numeric) by Cast(IntegerToNumeric) 0.2.0\n", False),
        ("an existing admission moved to another old case passes",
         "feature Function(\"ABS\") 0.2.0\n"
         "admit cast(Integer(Int4) as Numeric) = Some(Numeric) by Cast(IntegerWidening) 0.2.0\n", True),
    ]
    for label, text, ok in mcases:
        bad = check_manifest(text, crate, tag, old)
        if (not bad) != ok:
            failures.append(f"manifest: {label}: expected {'pass' if ok else 'a violation'}, got {bad}")
    # A tag before the manifest existed: every line is new, and none may claim it.
    if check_manifest(old, crate, ("v0.1.0", (0, 1, 0)), None):
        failures.append("manifest: lines at 0.2.0 against v0.1.0 (no manifest) should pass")
    if not check_manifest("feature X 0.1.0\n", crate, ("v0.1.0", (0, 1, 0)), None):
        failures.append("manifest: a line claiming 0.1.0 against v0.1.0 should fail")
    for f in failures:
        print(f"FAIL {f}")
    print(f"check-floors self-test: {len(cases) + len(mcases) + 2 - len(failures)} passed, {len(failures)} failed")
    return 1 if failures else 0


def main(argv):
    if len(argv) >= 2 and argv[1] == "check":
        return cmd_check()
    if len(argv) >= 2 and argv[1] == "notes":
        return cmd_notes(argv[2] if len(argv) > 2 else None)
    if len(argv) >= 2 and argv[1] == "self-test":
        return cmd_self_test()
    print(__doc__.strip())
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
