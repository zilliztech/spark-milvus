#!/usr/bin/env python3
"""Build the local MinIO test image from checksum-pinned official releases."""
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import tempfile
import urllib.request

BASE_IMAGE = 'debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8'
RELEASES = {
    # SDK 2.30 sends CRC32 for DeleteObjects; older MinIO requires Content-MD5.
    # https://github.com/minio/minio/issues/20849
    'minio': 'RELEASE.2025-04-22T22-12-26Z',
    'mc': 'RELEASE.2024-10-29T15-34-59Z',
}
CHECKSUMS = {
    'amd64': {
        'minio': '53e2a2cb16c5366ea6fbbc479c19ddb4c6a0948273e752f740fb1fbf27bb817c',
        'mc': 'a8a31621b4ad257583fb569fe31cfd72cf40e0b9b636971963c4815852c26914',
    },
    'arm64': {
        'minio': '6c2f3142c94240206123177f4ba1e360daa5d1e0a4962e90757ef4f92c3ab57c',
        'mc': 'a0312659c887da8e7f2427a7829bca34cdd29bd4c4af19bd7c4155156d00f543',
    },
}


def host_architecture(machine=None):
    selected = machine or platform.machine()
    aliases = {'x86_64': 'amd64', 'amd64': 'amd64', 'aarch64': 'arm64', 'arm64': 'arm64'}
    if selected not in aliases:
        raise ValueError('Unsupported MinIO fixture architecture: ' + selected)
    return aliases[selected]


def binaries(architecture):
    return {name: {
        'url': f'https://github.com/minio/{name}/releases/download/{release}/'
               f'{name}.linux-{architecture}.{release}',
        'sha256': CHECKSUMS[architecture][name],
    } for name, release in RELEASES.items()}


ARCHITECTURE = host_architecture()
PLATFORM = 'linux/' + ARCHITECTURE
BINARIES = binaries(ARCHITECTURE)
DOCKERFILE = (f'FROM {BASE_IMAGE}\n'
              'COPY minio mc /usr/local/bin/\n'
              'ENTRYPOINT ["/usr/local/bin/minio"]\n')
RECIPE = {'platform': PLATFORM, 'baseImage': BASE_IMAGE,
          'binaries': BINARIES, 'dockerfile': DOCKERFILE}
RECIPE_SHA256 = hashlib.sha256(json.dumps(RECIPE, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
IMAGE = 'spark-milvus-minio-fixture:' + RECIPE_SHA256
RECIPE_LABEL = 'com.zilliz.milvus.minio-fixture.recipe-sha256'


def identity():
    """Return the immutable inputs to record beside the actual Docker image ID."""
    return json.loads(json.dumps({'image': IMAGE, 'recipeSha256': RECIPE_SHA256, **RECIPE}))


def docker(*args):
    result = subprocess.run(['docker', *args], text=True, capture_output=True)
    if result.returncode:
        # Image provisioning has no generated service credentials or secret arguments.
        raise RuntimeError(f'MinIO fixture image Docker {args[0]} failed (exit {result.returncode}): '
                           + result.stderr.strip())
    return result.stdout.strip()


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def verified_binary(directory, name):
    expected = BINARIES[name]
    path = directory / name
    if path.exists():
        if sha256(path) != expected['sha256']:
            raise RuntimeError(f'Cached MinIO fixture {name} SHA-256 mismatch')
        path.chmod(0o755)
        return path
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=directory, prefix=name + '.', delete=False) as output:
            temporary = Path(output.name)
            with urllib.request.urlopen(expected['url'], timeout=60) as response:
                for block in iter(lambda: response.read(1024 * 1024), b''):
                    output.write(block)
        if sha256(temporary) != expected['sha256']:
            raise RuntimeError(f'Downloaded MinIO fixture {name} SHA-256 mismatch')
        temporary.chmod(0o755)
        temporary.replace(path)
        return path
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def ensure_image(cache_directory):
    """Use an exact recipe cache or build it locally; never pull a MinIO image."""
    if docker('image', 'ls', '--quiet', '--no-trunc', IMAGE):
        actual = docker('image', 'inspect', '--format',
                        '{{ index .Config.Labels "' + RECIPE_LABEL + '" }}', IMAGE)
        if actual != RECIPE_SHA256:
            raise RuntimeError('Cached MinIO fixture image recipe does not match its content tag')
        return IMAGE
    directory = Path(cache_directory) / RECIPE_SHA256
    directory.mkdir(parents=True, exist_ok=True)
    for name in BINARIES:
        verified_binary(directory, name)
    (directory / 'Dockerfile').write_text(DOCKERFILE + f'LABEL {RECIPE_LABEL}="{RECIPE_SHA256}"\n')
    # Restrict the context to verified inputs even if the cache holds other files.
    (directory / '.dockerignore').write_text('*\n!Dockerfile\n!minio\n!mc\n')
    docker('build', '--platform', PLATFORM, '--tag', IMAGE, str(directory))
    actual = docker('image', 'inspect', '--format',
                    '{{ index .Config.Labels "' + RECIPE_LABEL + '" }}', IMAGE)
    if actual != RECIPE_SHA256:
        raise RuntimeError('Built MinIO fixture image does not match its recipe')
    return IMAGE
