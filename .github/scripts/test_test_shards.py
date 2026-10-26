"""Unit tests for `test-shards.py check`.

Run: PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s .github/scripts -p 'test_*.py'
"""
import contextlib
import importlib.machinery
import importlib.util
import io
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
_loader = importlib.machinery.SourceFileLoader("test_shards", os.path.join(HERE, "test-shards.py"))
_spec = importlib.util.spec_from_loader("test_shards", _loader)
shards = importlib.util.module_from_spec(_spec)
_loader.exec_module(shards)


def run_check(units, weights):
    """Runs `check` against the given units and weights; returns (exit code or None, output)."""
    shards.units = lambda: units
    shards.read_weights = lambda: weights
    shards.shard_count = lambda: 2
    out = io.StringIO()
    code = None
    with contextlib.redirect_stdout(out):
        try:
            shards.cmd_check()
        except SystemExit as e:
            code = e.code
    return code, out.getvalue()


class Check(unittest.TestCase):
    UNITS = ["doc", "lib", "test:a", "test:b"]

    def test_accepts_matching_weights(self):
        code, out = run_check(self.UNITS, {"doc": 1.0, "lib": 2.0, "test:a": 3.0, "test:b": 4.0})
        self.assertIsNone(code)
        self.assertIn("ok: 4 units", out)

    def test_warns_but_passes_for_an_unweighted_unit(self):
        code, out = run_check(self.UNITS, {"lib": 2.0, "test:a": 3.0})
        self.assertIsNone(code)
        self.assertIn("test:b", out)

    def test_fails_on_a_stale_entry(self):
        code, out = run_check(self.UNITS, {"lib": 2.0, "test:a": 3.0, "test:gone": 9.0})
        self.assertEqual(code, 1)
        self.assertIn("no longer exist", out)
        self.assertIn("test:gone", out)


if __name__ == "__main__":
    unittest.main()
