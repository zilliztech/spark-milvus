"""Fetch pinned build inputs into a verified, content-addressed local cache."""

import hashlib
import http.client
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request


CACHE_ENV = "SPARK_MILVUS_DOWNLOAD_CACHE"


def cache_directory(explicit=None):
    return Path(explicit or os.environ.get(CACHE_ENV) or
                Path.home() / ".cache/spark-milvus/downloads").expanduser().resolve()


def checksum(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha512").hexdigest()


def transient(error):
    if isinstance(error, urllib.error.HTTPError):
        return error.code in (408, 429) or 500 <= error.code <= 599
    return isinstance(error, (urllib.error.URLError, TimeoutError, ConnectionError,
                              http.client.IncompleteRead))


def fetch(url, sha512, *, cache_dir=None, attempts=3, timeout=120):
    """Return verified bytes; cached entries are rehashed before every reuse.

    Downloads become visible only after verification and an atomic rename.
    A corrupt cache entry is discarded and fetched again. Checksum mismatches
    in new downloads fail immediately; retries only address transient transport
    failures and always restart from an empty temporary file.
    """
    if not re.fullmatch(r"[a-f0-9]{128}", sha512):
        raise ValueError("A pinned lowercase SHA-512 checksum is required")
    if not url.startswith("https://") or attempts < 1 or attempts > 5:
        raise ValueError("Pinned downloads require HTTPS and between 1 and 5 attempts")
    directory = cache_directory(cache_dir) / "sha512"
    directory.mkdir(parents=True, exist_ok=True)
    cached = directory / sha512
    if cached.exists():
        if not cached.is_symlink() and cached.is_file() and checksum(cached) == sha512:
            return cached
        cached.unlink()
    for attempt in range(1, attempts + 1):
        with tempfile.NamedTemporaryFile(dir=directory, prefix=".download-", delete=False) as stream:
            temporary = Path(stream.name)
            try:
                try:
                    with urllib.request.urlopen(url, timeout=timeout) as response:
                        shutil.copyfileobj(response, stream)
                except (urllib.error.URLError, TimeoutError, ConnectionError,
                        http.client.IncompleteRead) as error:
                    if attempt == attempts or not transient(error):
                        raise RuntimeError(f"Download failed after {attempt} attempt(s): {url}: {error}") from error
                    print(f"Download interrupted ({attempt}/{attempts}); retrying {url}: {error}",
                          file=sys.stderr, flush=True)
                    time.sleep(min(2 ** (attempt - 1), 4))
                    continue
                stream.flush()
                if checksum(temporary) != sha512:
                    raise ValueError("Pinned archive checksum mismatch: " + url)
                temporary.replace(cached)
                return cached
            finally:
                temporary.unlink(missing_ok=True)
