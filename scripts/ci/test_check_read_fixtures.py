"""Regression guards for corruption and accidental writer reintroduction."""
import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("check_read_fixtures", Path(__file__).with_name("check_read_fixtures.py"))
checks = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checks)


class ReadFixtureChecksTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "README.md").write_text("Synthetic data, expected rows: 2")
        (self.root / "data.bin").write_bytes(b"fixed")
        self.manifest = self.root / "SHA256SUMS"
        self.manifest.write_text("".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n"
                                         for p in sorted(self.root.iterdir())))

    def test_complete_fixture_passes(self):
        self.assertEqual(checks.check_group(self.root), 2)

    def test_corruption_missing_and_unlisted_files_fail(self):
        data = self.root / "data.bin"
        data.write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "mismatch"):
            checks.check_group(self.root)
        data.unlink()
        with self.assertRaisesRegex(ValueError, "Missing"):
            checks.check_group(self.root)
        data.write_bytes(b"fixed")
        (self.root / "extra").write_bytes(b"unexpected")
        with self.assertRaisesRegex(ValueError, "Unlisted"):
            checks.check_group(self.root)

    def test_duplicate_and_escape_paths_fail(self):
        original = self.manifest.read_text()
        self.manifest.write_text(original + original.splitlines()[0] + "\n")
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            checks.check_group(self.root)
        self.manifest.write_text("0" * 64 + "  ../outside\n")
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            checks.check_group(self.root)

    def test_writer_dependency_fails_but_neutral_fixture_is_allowed(self):
        source = self.root / "Read.scala"
        source.write_text("// Previously used V3SegmentWriter\nimport org.apache.avro.generic.GenericDatumWriter\n")
        checks.check_source(source)
        source.write_text("frame.nearestByJoin(other, distance, 3, mode, direction)")
        checks.check_source(source)
        for code in (
            "import V3SegmentWriter",
            "import IndexBuilder",
            "import com.zilliz.milvus.client.api.MilvusClient",
            'frame.buildIndex("vector", output)',
            "frame.\n  writeSnapshot(output)",
        ):
            source.write_text(code)
            with self.assertRaisesRegex(ValueError, "production write/control"):
                checks.check_source(source)

    def test_package_check_includes_unselected_main_and_test_shims(self):
        for scope, shim in (("main", "spark-4.2"), ("test", "spark-4.0-4.2")):
            path = self.root / "spark/src" / scope / "scala-shims" / shim / "example/Reader.scala"
            path.parent.mkdir(parents=True)
            path.write_text("package wrong\nclass Reader\n")
            with self.assertRaisesRegex(ValueError, "package does not match"):
                checks.check_source_layout(self.root)
            path.write_text("package example\nclass Reader\n")
        self.assertEqual(checks.check_source_layout(self.root), 2)

    def test_package_object_counts_its_own_directory(self):
        source_root = self.root / "scala"
        path = source_root / "example/reader/package.scala"
        path.parent.mkdir(parents=True)
        path.write_text("package example\npackage object reader {}\n")
        checks.check_package(path, source_root)


if __name__ == "__main__":
    unittest.main()
