"""The public example accepts only fixed input and checks results independently."""
import importlib.util
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

    def test_search_parameters_reach_the_connector_unchanged(self):
        options = {"milvus.snapshot.path": "snapshot.json", "milvus.search.params": "ef=32"}
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
        custom_search = demo.parse_args("search", ["--options", "options.json", "--queries", "queries.json"],
                                        search=True)
        self.assertFalse(custom_search.fixture)
        self.assertFalse(demo.parse_args("vectors", [], snapshot=False).fixture)
        for arguments in (["--options", "options.json"], ["--id-column", "other"],
                          ["--k", "0"], ["--max-rows", "10001"], ["--id-column", "id; SELECT"]):
            with self.subTest(arguments=arguments), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                demo.parse_args("search", arguments, search=True)

    def test_unknown_options_are_rejected(self):
        for key in ("unexpected", "milvus.read.unknown", "spark.option"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                demo.read_options(self.file({"milvus.snapshot.path": "snapshot.json", key: "value"}))
        with self.assertRaises(ValueError):
            demo.read_options(self.file({"fs.root_path": "/data"}))

    def test_where_is_available_only_when_the_example_applies_it(self):
        for parameters in ({}, {"snapshot": False}):
            with self.subTest(parameters=parameters), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                demo.parse_args("example", ["--where", "category = 1"], **parameters)
        for parameters in ({"search": True}, {"filtering": True}):
            self.assertEqual(demo.parse_args("example", ["--where", "category = 1"], **parameters).where,
                             "category = 1")

    def test_query_identity_and_dimensions_are_checked(self):
        self.assertEqual(demo.read_queries(self.file([{"qid": 1, "vector": [1, 2]}])), [(1, [1.0, 2.0])])
        for queries in ([{"qid": True, "vector": [1]}], [{"qid": 1, "vector": [float("nan")]}],
                        [{"qid": 1, "vector": [1]}, {"qid": 1, "vector": [2]}],
                        [{"qid": 1, "vector": [1]}, {"qid": 2, "vector": [2, 3]}]):
            with self.subTest(queries=queries), self.assertRaises(ValueError):
                demo.read_queries(self.file(queries))

    def test_oracle_uses_the_float32_values_sent_to_spark(self):
        queries = demo.read_queries(self.file([{"qid": 1, "vector": [16777217.1, 0.0]}]))
        self.assertEqual(queries, [(1, [16777218.0, 0.0])])
        base = [(1, [16777216.0, 0.0]), (2, [16777218.0, 1.0])]
        demo.check_hits(base, queries, {1: [2]}, 1, True)

    def test_exact_rejects_wrong_neighbors_but_accepts_distance_ties(self):
        base, queries = [(1, [1.0]), (2, [-1.0]), (3, [5.0])], [(10, [0.0])]
        demo.check_hits(base, queries, {10: [2]}, 1, True)
        with self.assertRaisesRegex(AssertionError, "independent oracle"):
            demo.check_hits(base, queries, {10: [3]}, 1, True)
        for hits in ({10: []}, {10: [1, 1]}, {10: [99]}, {10: [1], 11: [2]}):
            with self.subTest(hits=hits), self.assertRaises(AssertionError):
                demo.check_hits(base, queries, hits, 1, True)

    def test_approx_reports_recall_without_claiming_exact_ranking(self):
        result = demo.check_hits([(1, [1.0]), (2, [5.0])], [(10, [0.0])], {10: [2]}, 1, False)
        self.assertEqual(result[10]["recallAtK"], 0.0)


if __name__ == "__main__":
    unittest.main()
