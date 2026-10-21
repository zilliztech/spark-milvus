#!/usr/bin/env python3
"""Run an isolated MinIO fixture for V3 reads; never contact a Milvus service."""
import argparse
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time
import urllib.request

from check_read_fixtures import check_group

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / 'core/src/test/resources/read-fixtures/collections/nearest'
IMAGE = 'minio/minio:RELEASE.2024-11-07T00-52-20Z'


def docker(*args, data=None):
    result = subprocess.run(['docker', *args], input=data, text=True, capture_output=True)
    if result.returncode:
        # Admin arguments and diagnostics may contain generated credentials.
        raise RuntimeError('Read fixture Docker operation failed')
    return result.stdout.strip()


def policy(bucket, name):
    segment = 30 if name == 'a' else 31
    return {'Version': '2012-10-17', 'Statement': [
        {'Effect': 'Allow', 'Action': ['s3:GetBucketLocation', 's3:ListBucket'],
         'Resource': [f'arn:aws:s3:::{bucket}']},
        {'Effect': 'Allow', 'Action': ['s3:GetObject'],
         'Resource': [f'arn:aws:s3:::{bucket}/files/insert_log/10/20/{segment}/*',
                      f'arn:aws:s3:::{bucket}/files/index_files/{1000 + segment}/*',
                      f'arn:aws:s3:::{bucket}/files/snapshots/10/manifests/1/{segment}.avro',
                      f'arn:aws:s3:::{bucket}/snapshot-{name}.json']},
    ]}


def verified_fixture():
    check_group(FIXTURE)
    return FIXTURE


def stop(directory):
    state = directory / 'container'
    try:
        if state.exists():
            container = state.read_text().strip()
            # The exact ID belongs to this invocation; never remove by name/prefix.
            found = docker('ps', '-aq', '--no-trunc', '--filter', 'id=' + container)
            if found:
                try:
                    docker('rm', '-fv', container)
                except RuntimeError:
                    if docker('ps', '-aq', '--no-trunc', '--filter', 'id=' + container):
                        raise
            state.unlink()
    finally:
        for name in ('a.json', 'b.json'):
            (directory / name).unlink(missing_ok=True)


def start(directory, network_container=None):
    directory.mkdir(parents=True, exist_ok=True)
    if any((directory / name).exists() for name in ('container', 'a.json', 'b.json')):
        raise RuntimeError('Fixture state exists; stop that fixture before starting another')
    directory.chmod(0o700)
    fixture = verified_fixture()
    admin, password = 'root' + secrets.token_hex(8), secrets.token_hex(24)
    port = 9000
    network = ['--publish', '127.0.0.1::9000']
    if network_container:
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        network = ['--network', 'container:' + network_container]
    container = docker('run', '-d', '--rm', *network,
                       '-e', 'MINIO_ROOT_USER=' + admin, '-e', 'MINIO_ROOT_PASSWORD=' + password,
                       IMAGE, 'server', '--address', f'{"127.0.0.1" if network_container else "0.0.0.0"}:{port}', '/data')
    try:
        (directory / 'container').write_text(container + '\n')
        internal = f'http://127.0.0.1:{port}'
        for attempt in range(60):
            try:
                docker('exec', container, 'mc', 'alias', 'set', 'read-fixture', internal, admin, password)
                break
            except RuntimeError:
                if attempt == 59:
                    raise
                time.sleep(0.5)
        address = f'127.0.0.1:{port}' if network_container else docker('port', container, '9000/tcp').splitlines()[0]
        with urllib.request.urlopen('http://' + address + '/minio/health/ready', timeout=10) as response:
            if response.status != 200:
                raise RuntimeError('Read fixture is not reachable')
        bucket = 'v3-read-fixture'
        docker('exec', container, 'mc', 'mb', 'read-fixture/' + bucket)
        docker('cp', str(fixture), container + ':/tmp/v3-input')
        docker('exec', container, 'mc', 'cp', '--recursive', '/tmp/v3-input/files', 'read-fixture/' + bucket + '/')
        for name in ('a', 'b'):
            access, secret = name + secrets.token_hex(12), secrets.token_hex(24)
            if os.environ.get('GITHUB_ACTIONS') == 'true':
                for value in (access, secret):
                    print('::add-mask::' + value, flush=True)
            segment = 30 if name == 'a' else 31
            snapshot = json.loads((fixture / 'snapshot.json').read_text())
            snapshot['manifest_list'] = [f'files/snapshots/10/manifests/1/{segment}.avro']
            snapshot['storagev2_manifest_list'] = [item for item in snapshot['storagev2_manifest_list']
                                                 if item['segmentID'] == segment]
            snapshot['build_ids'] = [1000 + segment]
            docker('exec', '-i', container, 'sh', '-c', 'cat > /tmp/selected-snapshot.json',
                   data=json.dumps(snapshot))
            docker('exec', container, 'mc', 'cp', '/tmp/selected-snapshot.json',
                   f'read-fixture/{bucket}/snapshot-{name}.json')
            docker('exec', '-i', container, 'sh', '-c', 'cat > /tmp/read-policy.json',
                   data=json.dumps(policy(bucket, name)))
            docker('exec', container, 'mc', 'admin', 'user', 'add', 'read-fixture', access, secret)
            docker('exec', container, 'mc', 'admin', 'policy', 'create', 'read-fixture', name, '/tmp/read-policy.json')
            docker('exec', container, 'mc', 'admin', 'policy', 'attach', 'read-fixture', name, '--user', access)
            options = {'fs.storage_type': 'remote', 'fs.cloud_provider': 'aws',
                       'fs.address': address, 'fs.bucket_name': bucket, 'fs.root_path': 'files',
                       'fs.region': 'us-east-1', 'fs.use_ssl': 'false', 'fs.use_iam': 'false',
                       'fs.use_virtual_host': 'false', 'fs.access_key_id': access,
                       'fs.access_key_value': secret, 'milvus.snapshot.path': f'snapshot-{name}.json'}
            with open(directory / f'{name}.json', 'x', opener=lambda path, flags: os.open(path, flags, 0o600)) as out:
                json.dump(options, out)
        print('V3 object-storage fixture ready (two read-only identities; no Milvus service).')
    except BaseException as failure:
        # Even a state-file write failure must clean up the newly created ID.
        try:
            docker('rm', '-fv', container)
            (directory / 'container').unlink(missing_ok=True)
        except BaseException as cleanup_failure:
            failure.add_note(f'Fixture cleanup failed for {container}: {type(cleanup_failure).__name__}; retry stop.')
        finally:
            for name in ('a.json', 'b.json'):
                (directory / name).unlink(missing_ok=True)
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('start', 'stop'))
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--network-container', help='Share the local workspace container network')
    args = parser.parse_args()
    if args.action == 'start':
        start(args.directory.resolve(), args.network_container)
    else:
        stop(args.directory.resolve())
