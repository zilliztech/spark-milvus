"""Exercise the describe-only REST helper against a local HTTP server."""

from contextlib import contextmanager, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import threading
import unittest
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[1] / "python/00_snapshot_path.py"
SPEC = importlib.util.spec_from_file_location("snapshot_path", SOURCE)
snapshot_path = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(snapshot_path)


@contextmanager
def server(payload=None, status=200, headers=None, raw=None):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append((self.path, dict(self.headers), json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(raw if raw is not None else json.dumps(payload).encode())

        def log_message(self, *_args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=httpd.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}", requests
    finally:
        httpd.shutdown()
        httpd.server_close()
        worker.join()


class SnapshotPathTest(unittest.TestCase):
    def test_describes_existing_snapshot_with_prefix_and_preserves_path(self):
        location = "s3://bucket/prefix/snapshot%20metadata.json"
        with server({"code": 0, "data": {"s3Location": location}}) as (uri, requests):
            actual = snapshot_path.describe_snapshot(uri + "/proxy/", "books", "pages", "fixed", token="private-token")
        self.assertEqual(actual, location)
        self.assertEqual(len(requests), 1)
        path, headers, body = requests[0]
        self.assertEqual(path, "/proxy/v2/vectordb/snapshots/describe")
        self.assertEqual(headers["Authorization"], "Bearer private-token")
        self.assertEqual(body, {"dbName": "books", "collectionName": "pages", "snapshotName": "fixed"})

    def test_output_merges_storage_replaces_placeholder_and_is_private(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = Path(directory) / "storage.json"
            storage.write_text(json.dumps({"milvus.snapshot.path": "placeholder", "fs.bucket_name": "bucket", "extfs.aws.region": "us-west-2"}))
            output = Path(directory) / "options.json"
            with server({"code": 0, "data": {"s3Location": "s3://bucket/fixed.json"}}) as (uri, requests):
                snapshot_path.main(["--uri", uri, "--collection", "pages", "--snapshot", "fixed",
                                    "--storage-options", str(storage), "--output", str(output)])
                with self.assertRaisesRegex(ValueError, "already exists"):
                    snapshot_path.main(["--uri", uri, "--collection", "pages", "--snapshot", "fixed", "--output", str(output)])
                self.assertEqual(len(requests), 1)
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
            self.assertEqual(json.loads(output.read_text()), {"milvus.snapshot.path": "s3://bucket/fixed.json", "fs.bucket_name": "bucket", "extfs.aws.region": "us-west-2"})

    def test_environment_and_stdout(self):
        with server({"code": 0.0, "data": {"s3Location": "s3://bucket/fixed.json"}}) as (uri, _):
            stdout = io.StringIO()
            with patch.dict(os.environ, {"MILVUS_URI": uri, "MILVUS_TOKEN": "test-token"}), redirect_stdout(stdout):
                snapshot_path.main(["--collection", "pages", "--snapshot", "fixed"])
            self.assertEqual(json.loads(stdout.getvalue()), {"milvus.snapshot.path": "s3://bucket/fixed.json"})
            self.assertNotIn("test-token", stdout.getvalue())

    def test_redirect_does_not_forward_bearer_token(self):
        with server({"code": 0}) as (destination, received):
            with server(status=307, headers={"Location": destination + "/redirected"}) as (uri, _):
                with self.assertRaisesRegex(ValueError, "status 307"):
                    snapshot_path.describe_snapshot(uri, "default", "pages", "fixed", token="private-token")
            self.assertEqual(received, [])

    def test_http_and_api_errors_do_not_echo_response_secrets(self):
        for status, payload, expected in [(403, {"message": "private-token"}, "status 403"),
                                          (200, {"code": 1800, "message": "private-token"}, "code 1800")]:
            with self.subTest(status=status), server(payload, status=status) as (uri, _):
                with self.assertRaises(ValueError) as error:
                    snapshot_path.describe_snapshot(uri, "default", "pages", "fixed", token="private-token")
                self.assertIn(expected, str(error.exception))
                self.assertNotIn("private-token", str(error.exception))

    def test_malformed_success_is_rejected(self):
        for payload in [{"code": "0"}, {"code": False}, {"data": {}}, {"code": 0, "data": {}},
                        {"code": 0, "data": {"s3Location": " "}}, {"code": 0, "data": {"s3Location": 123}}]:
            with self.subTest(payload=payload), server(payload) as (uri, _):
                with self.assertRaises(ValueError):
                    snapshot_path.describe_snapshot(uri, "default", "pages", "fixed")

    def test_invalid_json_is_rejected_without_body(self):
        with server(raw=b"private-token: not json") as (uri, _):
            with self.assertRaisesRegex(ValueError, "invalid JSON"):
                snapshot_path.describe_snapshot(uri, "default", "pages", "fixed")

    def test_nonzero_code_cannot_round_to_success(self):
        with server(raw=b'{"code":1e-999,"data":{"s3Location":"s3://bucket/fixed.json"}}') as (uri, _):
            with self.assertRaisesRegex(ValueError, "error code"):
                snapshot_path.describe_snapshot(uri, "default", "pages", "fixed")

    def test_invalid_inputs_fail_without_network(self):
        for uri in [None, "", "file:///tmp/input", "https://user:password@host", "https://host/#fragment", "https://host/?query=1", "https://host:bad"]:
            with self.subTest(uri=uri), self.assertRaises(ValueError):
                snapshot_path.endpoint(uri)
        for timeout in [0, -1, float("inf"), float("nan")]:
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                snapshot_path.describe_snapshot("http://localhost", "default", "pages", "fixed", timeout)
        with tempfile.TemporaryDirectory() as directory:
            storage = Path(directory) / "storage.json"
            storage.write_text('{"milvus.filter": "id > 0"}')
            with self.assertRaisesRegex(ValueError, "only fs"):
                snapshot_path.storage_options(storage)


if __name__ == "__main__":
    unittest.main()
