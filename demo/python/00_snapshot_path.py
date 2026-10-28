#!/usr/bin/env python3
"""Resolve an existing snapshot into connector options through the Milvus REST API.

The deployed server must expose the snapshot REST endpoint; not every Milvus
3.0 build or managed deployment does. This helper only describes a snapshot.
It never creates, exports, pins or deletes one and needs no Spark or Milvus SDK.
"""

import argparse
from decimal import Decimal
from http.client import HTTPException
import json
import math
import os
from pathlib import Path
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


# REST implementation: https://github.com/milvus-io/milvus/blob/master/internal/distributed/proxy/httpserver/snapshot_handler_v2.go
# Pinned RPC response: milvus-proto/proto/milvus.proto, DescribeSnapshotResponse.s3_location.
DESCRIBE_PATH = "/v2/vectordb/snapshots/describe"


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        return None


def endpoint(uri):
    if not isinstance(uri, str) or not uri or any(char.isspace() for char in uri):
        raise ValueError("MILVUS_URI or --uri must be an HTTP(S) base URL")
    try:
        parsed = urlsplit(uri)
        valid = (parsed.scheme in {"http", "https"} and parsed.hostname
                 and parsed.username is None and parsed.password is None
                 and not parsed.query and not parsed.fragment)
        parsed.port  # Validate a supplied port without printing the URL.
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("Use an HTTP(S) base URL without credentials, query or fragment")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/") + DESCRIBE_PATH, "", ""))


def storage_options(path):
    if path is None:
        return {}
    try:
        values = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        raise ValueError("Cannot read --storage-options as a JSON object") from None
    if not isinstance(values, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        or (key != "milvus.snapshot.path" and not key.startswith(("fs.", "extfs.")))
        for key, value in values.items()
    ):
        raise ValueError("Storage options must contain only fs.*/extfs.* string values and an optional milvus.snapshot.path")
    # Templates may already contain a snapshot placeholder; the API replaces it.
    values.pop("milvus.snapshot.path", None)
    return values


def describe_snapshot(uri, database, collection, snapshot, timeout=30.0, token=None):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("--timeout must be a positive finite number of seconds")
    if any(not isinstance(value, str) or not value.strip() for value in (database, collection, snapshot)):
        raise ValueError("Database, collection and snapshot names must be nonblank")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        if any(ord(char) < 32 or ord(char) > 126 for char in token):
            raise ValueError("MILVUS_TOKEN contains invalid header characters")
        headers["Authorization"] = "Bearer " + token
    body = json.dumps({"dbName": database, "collectionName": collection, "snapshotName": snapshot}).encode()
    request = Request(endpoint(uri), data=body, headers=headers, method="POST")
    try:
        with build_opener(NoRedirects()).open(request, timeout=timeout) as response:
            raw = response.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise ValueError("Milvus returned an oversized response")
    except HTTPError as error:
        raise ValueError(f"Milvus HTTP request failed with status {error.code}; redirects are not followed") from None
    except (URLError, OSError, OverflowError, HTTPException):
        raise ValueError("Milvus HTTP request failed or timed out") from None
    try:
        response = json.loads(raw, parse_float=Decimal)
    except (ValueError, UnicodeError):
        raise ValueError("Milvus returned invalid JSON") from None
    code = response.get("code") if isinstance(response, dict) else None
    if type(code) not in (int, Decimal):
        raise ValueError("Milvus response must contain a numeric code")
    if code != 0:
        raise ValueError(f"Milvus returned error code {code}")
    data = response.get("data")
    location = data.get("s3Location") if isinstance(data, dict) else None
    if not isinstance(location, str) or not location.strip():
        raise ValueError("Milvus returned no data.s3Location for this snapshot")
    return location


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default=os.environ.get("MILVUS_URI"), help="Milvus REST base URL; defaults to MILVUS_URI")
    parser.add_argument("--database", default="default")
    parser.add_argument("--collection", required=True)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--timeout", type=float, default=30.0, help="Positive finite HTTP timeout in seconds")
    parser.add_argument("--storage-options", type=Path, help="JSON storage settings to merge with the returned snapshot path")
    parser.add_argument("--output", type=Path, help="Create a private options JSON file; never overwrite an existing file")
    args = parser.parse_args(argv)
    if args.output is not None and os.path.lexists(args.output):
        raise ValueError("--output already exists; choose a new file")
    options = storage_options(args.storage_options)
    options["milvus.snapshot.path"] = describe_snapshot(
        args.uri, args.database, args.collection, args.snapshot, args.timeout, os.environ.get("MILVUS_TOKEN")
    )
    encoded = json.dumps(options, indent=2) + "\n"
    if args.output is None:
        print(encoded, end="")
    else:
        try:
            with os.fdopen(os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
                output.write(encoded)
        except OSError:
            raise ValueError("Cannot create --output as a new private file") from None


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        print(f"Snapshot path lookup failed: {error}", file=sys.stderr)
        sys.exit(1)
