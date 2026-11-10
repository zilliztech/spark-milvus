"""The public example accepts only fixed input and checks results independently."""
import importlib.util
import contextlib
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("demo_common", Path(__file__).parents[1] / "python/common.py")
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)


class DemoCommonTest(unittest.TestCase):
    def setUp(self):
        path = mock.patch.object(sys, "path", [str(Path(__file__).parents[1] / "python"), *sys.path])
        path.start()
        self.addCleanup(path.stop)

    def file(self, content):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / "input.json"
        path.write_text(json.dumps(content))
        return path

    def test_explicit_snapshot_preserves_input_identity(self):
        options = {"milvus.snapshot.path": "snapshot.json", "fs.storage_type": "local", "fs.root_path": "/data"}
        self.assertEqual(demo.read_options(self.file(options)), options)

    def test_fixture_reads_are_checked_against_frozen_rows_and_deletes(self):
        options, expected = demo.fixture_input()
        self.assertEqual(options["fs.root_path"], str(demo.FIXTURE))
        self.assertEqual(len(expected), 14)
        demo.check_fixture_rows(expected, expected)
        for rows in (expected[:-1], [*expected, {"id": 3002, "vector": [2.0, 0.0], "category": 1}],
                     [dict(expected[0], vector=[99.0, 0.0]), *expected[1:]]):
            with self.subTest(rows=rows), self.assertRaisesRegex(AssertionError, "frozen fixture"):
                demo.check_fixture_rows(rows, expected)

    def test_session_supplies_fixture_expectations_and_reports_only_success(self):
        spark = mock.Mock(version="4.0.1")
        spark.sparkContext.master = "local[2]"
        sql = mock.Mock()
        sql.SparkSession.builder.appName.return_value.getOrCreate.return_value = spark
        with mock.patch.dict(sys.modules, {"pyspark.sql": sql}):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                with demo.demo_session("Read snapshot", []) as (session, args, options):
                    self.assertIs(session, spark)
                    self.assertEqual(len(args.expected_rows), 14)
                    self.assertEqual(options["milvus.snapshot.path"], "snapshot.json")
                    demo.check_fixture_rows(args.expected_rows, args.expected_rows)
            self.assertIn("Demo passed", output.getvalue())
            spark.stop.assert_called_once()
            spark.reset_mock()
            with contextlib.redirect_stdout(io.StringIO()) as output:
                with self.assertRaisesRegex(ValueError, "query failed"):
                    with demo.demo_session("Read snapshot", []):
                        raise ValueError("query failed")
            self.assertNotIn("Demo passed", output.getvalue())
            spark.stop.assert_called_once()

    def test_corrupt_or_missing_fixture_is_rejected_without_repair(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "nearest"
            shutil.copytree(demo.FIXTURE, fixture)
            snapshot = fixture / "snapshot.json"
            snapshot.write_text("{}")
            with mock.patch.object(demo, "FIXTURE", fixture):
                with self.assertRaisesRegex(ValueError, "SHA256 mismatch"):
                    demo.fixture_input()
                self.assertEqual(snapshot.read_text(), "{}")
                snapshot.unlink()
                with self.assertRaisesRegex(ValueError, "Missing file"):
                    demo.fixture_input()
                self.assertFalse(snapshot.exists())

    def test_fixture_and_custom_input_arguments_are_unambiguous(self):
        self.assertTrue(demo.parse_args("read", []).fixture)
        custom_read = demo.parse_args("read", ["--options", "options.json"])
        self.assertFalse(custom_read.fixture)
        for arguments in (["--queries", "queries.json"], ["--id-column", "other"],
                          ["--k", "0"], ["--max-rows", "10001"]):
            with self.subTest(arguments=arguments), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                demo.parse_args("read", arguments)

    def test_unknown_options_are_rejected(self):
        for key in ("unexpected", "milvus.read.unknown", "spark.option", "milvus.search.params"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                demo.read_options(self.file({"milvus.snapshot.path": "snapshot.json", key: "value"}))
        with self.assertRaises(ValueError):
            demo.read_options(self.file({"fs.root_path": "/data"}))

    def test_where_is_available_only_when_the_example_applies_it(self):
        for parameters in ({},):
            with self.subTest(parameters=parameters), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                demo.parse_args("example", ["--where", "category = 1"], **parameters)
        for parameters in ({"filtering": True},):
            self.assertEqual(demo.parse_args("example", ["--where", "category = 1"], **parameters).where,
                             "category = 1")


if __name__ == "__main__":
    unittest.main()
