"""Verify read fixture authorization and cleanup without a Docker daemon."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
import read_storage


class ReadStorageTest(unittest.TestCase):
    def test_initial_run_diagnostic_redacts_credentials_and_omits_command_and_input(self):
        result = subprocess.CompletedProcess([], 125, stdout='private stdout',
                                             stderr='pull access denied; user=admin-key password=secret-key\n')
        with mock.patch.object(read_storage.subprocess, 'run', return_value=result):
            with self.assertRaises(RuntimeError) as failure:
                read_storage.docker('run', 'private argument', data='private input',
                                    diagnostic_secrets=('admin-key', 'secret-key'))
        self.assertEqual(str(failure.exception),
                         'Read fixture Docker run failed (exit 125): '
                         'pull access denied; user=[REDACTED] password=[REDACTED]')

    def test_admin_diagnostics_are_suppressed_by_default(self):
        result = subprocess.CompletedProcess([], 1, stdout='private stdout', stderr='private credentials')
        with mock.patch.object(read_storage.subprocess, 'run', return_value=result):
            with self.assertRaises(RuntimeError) as failure:
                read_storage.docker('exec', 'private argument', data='private input')
        self.assertEqual(str(failure.exception), 'Read fixture Docker exec failed (exit 1)')

    def test_empty_diagnostic_still_reports_operation_and_exit_code(self):
        result = subprocess.CompletedProcess([], 125, stdout='', stderr=' \n')
        with mock.patch.object(read_storage.subprocess, 'run', return_value=result):
            with self.assertRaises(RuntimeError) as failure:
                read_storage.docker('run', diagnostic_secrets=('admin-key', 'secret-key'))
        self.assertEqual(str(failure.exception), 'Read fixture Docker run failed (exit 125)')

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
