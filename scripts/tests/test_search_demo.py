"""The public example accepts only fixed input and checks results independently."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("search_demo", Path(__file__).parents[1] / "search_demo.py")
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)


class SearchDemoTest(unittest.TestCase):
    def file(self, content):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / "input.json"
        path.write_text(json.dumps(content))
        return path

    def test_explicit_snapshot_preserves_input_identity(self):
        options = {"milvus.snapshot.path": "snapshot.json", "fs.storage_type": "local", "fs.root_path": "/data"}
        self.assertEqual(demo.read_options(self.file(options)), options)

    def test_online_backup_and_unknown_options_are_rejected(self):
        for key in ("milvus.uri", "milvus.token", "milvus.collection", "milvus.backup.dir", "unexpected"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                demo.read_options(self.file({"milvus.snapshot.path": "snapshot.json", key: "value"}))
        with self.assertRaises(ValueError):
            demo.read_options(self.file({"fs.root_path": "/data"}))

    def test_query_identity_and_dimensions_are_checked(self):
        self.assertEqual(demo.read_queries(self.file([{"qid": 1, "vector": [1, 2]}])), [(1, [1.0, 2.0])])
        for queries in ([{"qid": True, "vector": [1]}], [{"qid": 1, "vector": [float("nan")]}],
                        [{"qid": 1, "vector": [1]}, {"qid": 1, "vector": [2]}],
                        [{"qid": 1, "vector": [1]}, {"qid": 2, "vector": [2, 3]}]):
            with self.subTest(queries=queries), self.assertRaises(ValueError):
                demo.read_queries(self.file(queries))

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
