"""Verify that fixture image caches cannot bypass the pinned release checksums."""
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import minio_image


class MinioImageTest(unittest.TestCase):
    def test_identity_is_independent_and_binds_build_inputs(self):
        first = minio_image.identity()
        first['binaries']['minio']['sha256'] = 'changed'
        self.assertEqual(minio_image.identity()['binaries']['minio']['sha256'],
                         minio_image.BINARIES['minio']['sha256'])
        self.assertIn(minio_image.BASE_IMAGE, first['dockerfile'])
        self.assertEqual(first['image'], 'spark-milvus-minio-fixture:' + first['recipeSha256'])

    def test_valid_image_cache_needs_no_network_or_build(self):
        with mock.patch.object(minio_image, 'docker', side_effect=['sha256:image', minio_image.RECIPE_SHA256]), \
                mock.patch.object(minio_image.urllib.request, 'urlopen') as download:
            self.assertEqual(minio_image.ensure_image(Path('unused')), minio_image.IMAGE)
        download.assert_not_called()

    def test_wrong_image_recipe_is_rejected(self):
        with mock.patch.object(minio_image, 'docker', side_effect=['sha256:image', 'wrong-recipe']):
            with self.assertRaisesRegex(RuntimeError, 'recipe does not match'):
                minio_image.ensure_image(Path('unused'))

    def test_corrupt_cached_binary_is_rejected_without_downloading(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / 'minio').write_bytes(b'corrupt')
            with mock.patch.object(minio_image.urllib.request, 'urlopen') as download:
                with self.assertRaisesRegex(RuntimeError, 'Cached.*SHA-256 mismatch'):
                    minio_image.verified_binary(directory, 'minio')
            download.assert_not_called()

    def test_bad_download_never_becomes_an_executable_cached_binary(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with mock.patch.object(minio_image.urllib.request, 'urlopen', return_value=io.BytesIO(b'corrupt')):
                with self.assertRaisesRegex(RuntimeError, 'Downloaded.*SHA-256 mismatch'):
                    minio_image.verified_binary(directory, 'minio')
            self.assertEqual(list(directory.iterdir()), [])

    def test_verified_download_is_cached_and_checked_before_reuse(self):
        binary = b'official-release-bytes'
        artifact = {'url': 'https://github.com/minio/minio/releases/download/pinned/minio',
                    'sha256': hashlib.sha256(binary).hexdigest()}
        with tempfile.TemporaryDirectory() as temporary, \
                mock.patch.dict(minio_image.BINARIES, {'minio': artifact}):
            directory = Path(temporary)
            with mock.patch.object(minio_image.urllib.request, 'urlopen', return_value=io.BytesIO(binary)) as download:
                path = minio_image.verified_binary(directory, 'minio')
                self.assertEqual(path.read_bytes(), binary)
                self.assertEqual(path.stat().st_mode & 0o777, 0o755)
                self.assertEqual(minio_image.verified_binary(directory, 'minio'), path)
            download.assert_called_once_with(artifact['url'], timeout=60)

    def test_missing_image_builds_only_from_verified_binaries(self):
        with tempfile.TemporaryDirectory() as temporary, \
                mock.patch.object(minio_image, 'verified_binary') as verify, \
                mock.patch.object(minio_image, 'docker', side_effect=['', '', minio_image.RECIPE_SHA256]) as docker:
            self.assertEqual(minio_image.ensure_image(Path(temporary)), minio_image.IMAGE)
            self.assertEqual([call.args[1] for call in verify.call_args_list], ['minio', 'mc'])
            build = docker.call_args_list[1].args
            self.assertEqual(build[:5], ('build', '--platform', 'linux/amd64', '--tag', minio_image.IMAGE))
            directory = Path(build[5])
            self.assertIn(minio_image.BASE_IMAGE, (directory / 'Dockerfile').read_text())
            self.assertEqual((directory / '.dockerignore').read_text(), '*\n!Dockerfile\n!minio\n!mc\n')


if __name__ == '__main__':
    unittest.main()
