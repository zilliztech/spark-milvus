"""Publication consumers follow the tested candidate, including Scala upgrades."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import candidate_scala


class CandidateScalaTest(unittest.TestCase):
    def test_selection_follows_patch_version_from_each_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for version in ("2.13.16", "2.13.99"):
                plan = {"jvm": [{"line": "4.0", "scalaBinary": "2.13", "scala": version}]}
                (root / "plan.json").write_text(json.dumps(plan))
                (root / "release-manifest.json").write_text(json.dumps({"plan": plan}))
                for source in (("--plan", str(root / "plan.json")), ("--candidate", str(root))):
                    result = subprocess.run([sys.executable, candidate_scala.__file__, *source,
                                             "--line", "4.0", "--scala-binary", "2.13"],
                                            capture_output=True, text=True, check=True)
                    self.assertEqual(result.stdout.strip(), version)

    def test_missing_duplicate_or_mismatched_selection_is_rejected(self):
        row = {"line": "3.5", "scalaBinary": "2.13", "scala": "2.13.16"}
        for rows in ([], [row, row], [dict(row, scala="2.12.20")]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                candidate_scala.select({"jvm": rows}, "3.5", "2.13")
        self.assertEqual(candidate_scala.select({"jvm": [row,
            dict(row, scalaBinary="2.12", scala="2.12.20")]}, "3.5", "2.12"), "2.12.20")


if __name__ == "__main__":
    unittest.main()
