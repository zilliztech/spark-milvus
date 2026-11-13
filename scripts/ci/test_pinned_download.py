"""Pinned downloads recover transport faults without trusting cached corruption."""

import hashlib
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import urllib.error

import pinned_download


class PinnedDownloadTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.url = "https://archive.apache.org/dist/example/source.tar.gz"
        self.content = b"pinned release bytes"
        self.pin = hashlib.sha512(self.content).hexdigest()

    def fetch(self, **options):
        return pinned_download.fetch(self.url, self.pin, cache_dir=self.root, **options)

    def test_cached_bytes_are_rehashed_and_reused_without_network(self):
        with mock.patch.object(pinned_download.urllib.request, "urlopen",
                               return_value=io.BytesIO(self.content)) as network:
            path = self.fetch()
        network.assert_called_once_with(self.url, timeout=120)
        self.assertEqual(path, self.root / "sha512" / self.pin)
        self.assertEqual(path.read_bytes(), self.content)
        with mock.patch.object(pinned_download.urllib.request, "urlopen") as network:
            self.assertEqual(self.fetch(), path)
        network.assert_not_called()

    def test_corrupt_cache_is_replaced_only_with_verified_bytes(self):
        cached = self.root / "sha512" / self.pin
        cached.parent.mkdir()
        cached.write_bytes(b"corrupt cache")
        with mock.patch.object(pinned_download.urllib.request, "urlopen",
                               return_value=io.BytesIO(self.content)) as network:
            self.assertEqual(self.fetch().read_bytes(), self.content)
        network.assert_called_once()
        self.assertEqual(list(cached.parent.iterdir()), [cached])

    def test_wrong_download_checksum_is_neither_cached_nor_retried(self):
        with mock.patch.object(pinned_download.urllib.request, "urlopen",
                               return_value=io.BytesIO(b"wrong bytes")) as network, \
                self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.fetch()
        network.assert_called_once()
        self.assertEqual(list((self.root / "sha512").iterdir()), [])

    def test_transient_transport_failure_retries_with_bounded_backoff(self):
        with mock.patch.object(pinned_download.urllib.request, "urlopen", side_effect=[
                urllib.error.URLError("temporary network failure"),
                urllib.error.HTTPError(self.url, 503, "Unavailable", {}, None),
                io.BytesIO(self.content)]) as network, \
                mock.patch.object(pinned_download.time, "sleep") as sleep:
            self.assertEqual(self.fetch().read_bytes(), self.content)
        self.assertEqual(network.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])

    def test_partial_download_is_removed_before_retry(self):
        class Interrupted(io.BytesIO):
            def read(self, size=-1):
                if self.tell():
                    raise TimeoutError("connection stalled")
                return super().read(4)
        with mock.patch.object(pinned_download.urllib.request, "urlopen", side_effect=[
                Interrupted(self.content), io.BytesIO(self.content)]), \
                mock.patch.object(pinned_download.time, "sleep"):
            result = self.fetch()
        self.assertEqual(result.read_bytes(), self.content)
        self.assertEqual(list(result.parent.iterdir()), [result])

    def test_failed_retry_budget_leaves_no_cached_or_partial_file(self):
        with mock.patch.object(pinned_download.urllib.request, "urlopen",
                               side_effect=TimeoutError("unavailable")) as network, \
                mock.patch.object(pinned_download.time, "sleep") as sleep, \
                self.assertRaisesRegex(RuntimeError, "after 3 attempt"):
            self.fetch()
        self.assertEqual(network.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        self.assertEqual(list((self.root / "sha512").iterdir()), [])

    def test_permanent_http_error_does_not_retry(self):
        with mock.patch.object(pinned_download.urllib.request, "urlopen", side_effect=
                urllib.error.HTTPError(self.url, 404, "Not Found", {}, None)) as network, \
                mock.patch.object(pinned_download.time, "sleep") as sleep, \
                self.assertRaisesRegex(RuntimeError, "after 1 attempt"):
            self.fetch()
        network.assert_called_once()
        sleep.assert_not_called()

    def test_cache_location_obeys_explicit_argument_then_environment(self):
        with mock.patch.dict(os.environ, {pinned_download.CACHE_ENV: str(self.root / "environment")}):
            self.assertEqual(pinned_download.cache_directory(), self.root / "environment")
            self.assertEqual(pinned_download.cache_directory(self.root / "explicit"), self.root / "explicit")

    def test_bad_pin_fails_before_opening_network(self):
        with mock.patch.object(pinned_download.urllib.request, "urlopen") as network, \
                self.assertRaisesRegex(ValueError, "pinned lowercase SHA-512"):
            pinned_download.fetch(self.url, "not a checksum", cache_dir=self.root)
        network.assert_not_called()


if __name__ == "__main__":
    unittest.main()
