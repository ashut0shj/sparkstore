"""tests/unit/test_persona_access_spec.py — validates the STRUCTURE of
tests/security/test_persona_access.py's EXPECTATIONS table (at least 3
personas, each with at least one check), without needing a real
Databricks SQL connection. The actual persona behavior can only be
proven on-workspace (see that file's docstring)."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

_spec = importlib.util.spec_from_file_location(
    "test_persona_access", REPO_ROOT / "tests" / "security" / "test_persona_access.py"
)
persona_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(persona_module)  # type: ignore[union-attr]


class TestPersonaExpectationsStructure(unittest.TestCase):
    def test_at_least_three_personas_defined(self):
        """Capstone requirement 70: at least three persona tests."""
        self.assertGreaterEqual(len(persona_module.EXPECTATIONS), 3)

    def test_every_persona_has_at_least_one_check(self):
        for persona, spec in persona_module.EXPECTATIONS.items():
            self.assertGreaterEqual(len(spec["checks"]), 1, f"{persona} has no checks defined")

    def test_at_least_one_persona_demonstrates_a_denial(self):
        """At least one check across all personas must expect an outright
        DENIAL (check=None), not just a masked value -- requirement 70
        wants both allowed AND denied/masked behavior demonstrated."""
        has_denial = any(
            check is None
            for spec in persona_module.EXPECTATIONS.values()
            for _, check, _ in spec["checks"]
        )
        self.assertTrue(has_denial)

    def test_at_least_one_persona_demonstrates_masking(self):
        """And at least one check must expect a masked (non-None, non-error)
        value rather than an outright denial."""
        mask_checks = [
            text for spec in persona_module.EXPECTATIONS.values()
            for _, check, text in spec["checks"]
            if check is not None and "mask" in text.lower()
        ]
        self.assertGreater(len(mask_checks), 0)


if __name__ == "__main__":
    unittest.main()
