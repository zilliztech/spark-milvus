"""Verify read fixture authorization and cleanup without a Docker daemon."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
import read_storage


class ReadStorageTest(unittest.TestCase):
    def test_users_cannot_read_each_others_snapshot_or_data(self):
        for name in ('a', 'b'):
            statements = read_storage.policy('bucket', name)['Statement']
            objects = statements[1]
            self.assertEqual(objects['Action'], ['s3:GetObject'])
            segment = 30 if name == 'a' else 31
            self.assertEqual(objects['Resource'], [
                f'arn:aws:s3:::bucket/files/insert_log/10/20/{segment}/*',
                f'arn:aws:s3:::bucket/files/index_files/{1000 + segment}/*',
                f'arn:aws:s3:::bucket/files/snapshots/10/manifests/1/{segment}.avro',
                f'arn:aws:s3:::bucket/snapshot-{name}.json'])
            self.assertFalse(any('Put' in action or 'Delete' in action
                                 for statement in statements for action in statement['Action']))

    def test_cleanup_removes_credentials_even_when_docker_fails_and_keeps_retry_id(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'container').write_text('owned-id')
            for name in ('a.json', 'b.json'):
                (root / name).write_text('credential')
            with mock.patch.object(read_storage, 'docker', side_effect=RuntimeError('offline')):
                with self.assertRaises(RuntimeError):
                    read_storage.stop(root)
            self.assertTrue((root / 'container').exists())
            self.assertFalse((root / 'a.json').exists())
            self.assertFalse((root / 'b.json').exists())

    def test_existing_state_cannot_be_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'container').write_text('owned-id')
            with self.assertRaisesRegex(RuntimeError, 'state exists'):
                read_storage.start(root)


if __name__ == '__main__':
    unittest.main()
