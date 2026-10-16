#!/usr/bin/env python3
"""Verify a downloaded native artifact before reusing its bundle."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys


PLATFORMS = ("linux-aarch64", "linux-x86_64")
SHA256 = re.compile(r"[0-9a-f]{64}")


class VerificationError(ValueError):
    """A reusable artifact failed a provenance or file-safety check."""


class InvalidJson(ValueError):
    """JSON is malformed, non-standard, or contains duplicate object keys."""


def require(condition, message):
    if not condition:
        raise VerificationError(message)


def json_object(path, description):
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise InvalidJson
            result[key] = value
        return result

    def invalid_constant(_value):
        raise InvalidJson

    try:
        try:
            value = json.loads(
                path.read_text(encoding="utf-8"),
                object_pairs_hook=object_pairs,
                parse_constant=invalid_constant,
            )
        except (ValueError, RecursionError) as error:
            raise InvalidJson from error
    except (OSError, UnicodeError, InvalidJson) as error:
        raise VerificationError(f"{description} is not valid JSON") from error
    require(isinstance(value, dict), f"{description} must be a JSON object")
    return value


def same_json(left, right):
    """Compare parsed JSON values without Python's bool/int coercion."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(
            same_json(left[key], right[key]) for key in left
        )
    if isinstance(left, list):
        return len(left) == len(right) and all(
            same_json(left_value, right_value)
            for left_value, right_value in zip(left, right)
        )
    return left == right


def properties(path):
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise VerificationError("Native checksum sidecar is not valid UTF-8 text") from error
    result = {}
    for line in lines:
        if not line:
            continue
        require(line.count("=") == 1, "Native checksum sidecar is malformed")
        key, value = line.split("=", 1)
        require(key and key == key.strip() and value and value == value.strip(),
                "Native checksum sidecar is malformed")
        require(key not in result, "Native checksum sidecar contains duplicate keys")
        result[key] = value
    require(set(result) == {"jar.sha256", "platform"},
            "Native checksum sidecar has unexpected keys")
    return result


def digest(path):
    value = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                value.update(block)
    except OSError as error:
        raise VerificationError("Native JAR could not be read") from error
    return value.hexdigest()


def artifact_files(artifact, platform):
    require(artifact.is_dir() and not artifact.is_symlink(),
            "Native artifact must be a regular directory")
    jar_name = f"milvus-native-{platform}.jar"
    expected = {jar_name, jar_name + ".properties", "abi.json", "cache-inputs.json"}
    try:
        entries = list(artifact.iterdir())
    except OSError as error:
        raise VerificationError("Native artifact inventory could not be read") from error
    require(len(entries) == len(expected) and {entry.name for entry in entries} == expected,
            "Native artifact inventory does not match the expected files")
    result = {entry.name: entry for entry in entries}
    require(all(path.is_file() and not path.is_symlink() for path in result.values()),
            "Native artifact entries must be regular non-symlink files")
    return result


def regular_file(path, description):
    require(path.is_file() and not path.is_symlink(),
            f"{description} must be a regular non-symlink file")
    return path


def verify(artifact, expected_cache_inputs, platform):
    require(platform in PLATFORMS, "Unsupported native platform")
    files = artifact_files(artifact, platform)
    expected_inputs = json_object(
        regular_file(expected_cache_inputs, "Expected cache inputs"), "Expected cache inputs"
    )
    artifact_inputs = json_object(files["cache-inputs.json"], "Artifact cache inputs")
    require(same_json(artifact_inputs, expected_inputs),
            "Artifact cache inputs do not exactly match the current build")

    jar_name = f"milvus-native-{platform}.jar"
    jar_digest = digest(files[jar_name])
    sidecar = properties(files[jar_name + ".properties"])
    require(sidecar["platform"] == platform,
            "Native checksum sidecar platform does not match")
    require(SHA256.fullmatch(sidecar["jar.sha256"]) is not None,
            "Native checksum sidecar digest is malformed")
    require(sidecar["jar.sha256"] == jar_digest,
            "Native checksum sidecar digest does not match the JAR")

    abi = json_object(files["abi.json"], "Native ABI report")
    require(abi.get("platform") == platform,
            "Native ABI report platform does not match")
    require(abi.get("jar.sha256") == jar_digest,
            "Native ABI report digest does not match the JAR")
    return files[jar_name]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--expected-cache-inputs", type=Path, required=True)
    parser.add_argument("--platform", choices=PLATFORMS, required=True)
    args = parser.parse_args(argv)
    try:
        verify(args.artifact, args.expected_cache_inputs, args.platform)
    except VerificationError as error:
        print(f"Native artifact reuse rejected: {error}", file=sys.stderr)
        return 1
    print(f"Verified reusable native artifact for {args.platform}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
