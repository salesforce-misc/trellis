"""Unit tests for check-module-map.py.

Run: PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s .github/scripts -p 'test_*.py'
"""
import importlib.machinery
import importlib.util
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
_loader = importlib.machinery.SourceFileLoader("check_module_map", os.path.join(HERE, "check-module-map.py"))
_spec = importlib.util.spec_from_loader("check_module_map", _loader)
mm = importlib.util.module_from_spec(_spec)
_loader.exec_module(mm)


def run(files, doc):
    """Writes `files` (paths under trellis/src) and the map `doc` under a temp dir; returns check()'s problems."""
    with tempfile.TemporaryDirectory() as root:
        for rel in files:
            path = os.path.join(root, mm.SRC, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            open(path, "w").close()
        os.makedirs(os.path.join(root, "docs"))
        if doc is not None:
            with open(os.path.join(root, mm.MAP), "w", encoding="utf-8") as f:
                f.write(doc)
        return mm.check(root)


FILES = ["staging/mod.rs", "staging/fold.rs", "staging/build.rs", "staging/build/orphans.rs", "defs/mod.rs", "defs/catalog.rs"]
DOC = "| `staging::fold`, `staging::build`, `staging::build::orphans` | x |\n| `defs::catalog` | y |\n"


class ModuleMap(unittest.TestCase):
    def test_a_complete_map_passes(self):
        self.assertEqual(run(FILES, DOC), [])

    def test_a_file_missing_from_the_map_is_reported(self):
        self.assertEqual(
            run(FILES + ["defs/lexer.rs"], DOC),
            ["trellis/src/defs/lexer.rs: not listed in docs/architecture.md (as `defs::lexer`)"],
        )

    def test_mod_rs_and_subdirectory_files_need_no_row(self):
        self.assertEqual(run(FILES + ["staging/build/extra.rs"], DOC), [])

    def test_a_name_that_is_not_a_module_is_reported(self):
        self.assertEqual(
            run(FILES, DOC + "| `staging::gone` | z |\n"),
            ["docs/architecture.md: `staging::gone` is not a module under trellis/src"],
        )

    def test_a_nested_name_must_exist_too(self):
        self.assertEqual(
            run(FILES, DOC + "| `staging::build::nope` | z |\n"),
            ["docs/architecture.md: `staging::build::nope` is not a module under trellis/src"],
        )

    def test_a_prefix_is_not_a_module_name(self):
        # `staging::fold` is listed; `staging::fol` must not satisfy anything, and a bare
        # `staging::` mentioned in prose names nothing.
        self.assertEqual(mm.named("`staging::` and `defs::catalog` and `capture::install`"), {"defs::catalog"})

    def test_a_missing_map_is_reported(self):
        self.assertEqual(run(FILES, None), ["docs/architecture.md: the module map is missing"])


if __name__ == "__main__":
    unittest.main()
