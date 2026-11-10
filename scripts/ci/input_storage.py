#!/usr/bin/env python3
"""Provision an isolated same-bucket, two-identity MinIO fixture for input tests."""

import argparse
import json
import os
from pathlib import Path
import secrets
import shlex
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

import minio_image


def docker(*arguments: str, stdin: str | None = None) -> str:
    result = subprocess.run(["docker", *arguments], input=stdin, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        # Commands and server diagnostics can contain generated credentials.
        raise RuntimeError("Input storage Docker operation failed")
    return result.stdout.strip()


def remove_container(container: str) -> None:
    def exists() -> bool:
        return bool(docker("ps", "--all", "--quiet", "--no-trunc", "--filter", "id=" + container))

    if not exists():
        return
    try:
        docker("rm", "-f", "-v", container)
    except RuntimeError:
        # --rm can remove a stopped container between the existence check and
        # explicit removal. Other Docker failures must remain visible.
        if exists():
            raise


def stop(env_file: Path, container: str | None = None) -> None:
    state = env_file.with_suffix(".container")
    try:
        owned = container or (state.read_text().strip() if state.exists() else None)
        if owned:
            remove_container(owned)
        state.unlink(missing_ok=True)
    finally:
        # Remove credentials even when Docker is unavailable. Retain the
        # container state on a removal failure so cleanup can be retried.
        env_file.unlink(missing_ok=True)


def health_check(endpoint: str) -> None:
    for attempt in range(30):
        try:
            with urllib.request.urlopen(endpoint + "/minio/health/ready", timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.5)
    raise RuntimeError("Input storage is not reachable from the test process")


def unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def mask_workflow_credentials(values: dict[str, str]) -> None:
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    for key, value in values.items():
        if key.endswith(("_ACCESS_KEY", "_SECRET_KEY")):
            # Escape workflow-command data, not shell input. Local runs must
            # never print secrets, including as apparent masking commands.
            escaped = value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
            print(f"::add-mask::{escaped}", flush=True)


def start(env_file: Path, network_container: str | None = None) -> None:
    state = env_file.with_suffix(".container")
    if env_file.exists() or state.exists():
        raise RuntimeError("Input storage fixture already exists; stop it first")
    env_file.parent.mkdir(parents=True, exist_ok=True)
    image = minio_image.ensure_image(env_file.parent / "minio-image")
    root_user, root_secret = "root" + secrets.token_hex(8), secrets.token_hex(24)
    if network_container:
        if docker("inspect", "--format", "{{.State.Running}}", network_container) != "true":
            raise RuntimeError("The requested workspace network container is not running")
        port = unused_port()
        network = ("--network", "container:" + network_container)
        server = ("server", "--address", f"127.0.0.1:{port}", "/data")
    else:
        port = 9000
        network = ("--publish", "127.0.0.1::9000")
        server = ("server", "/data")
    container = docker("run", "--detach", "--rm", *network,
                       "--env", "MINIO_ROOT_USER=" + root_user,
                       "--env", "MINIO_ROOT_PASSWORD=" + root_secret,
                       image, *server)
    try:
        state.write_text(container + "\n")
        state.chmod(0o600)
        for attempt in range(60):
            try:
                docker("exec", container, "mc", "alias", "set", "fixture",
                       f"http://127.0.0.1:{port}", root_user, root_secret)
                break
            except RuntimeError:
                if attempt == 59:
                    raise
                time.sleep(0.5)
        bucket = "input-identities"
        docker("exec", container, "mc", "mb", "fixture/" + bucket)
        endpoint = (f"http://127.0.0.1:{port}" if network_container else
                    "http://" + docker("port", container, "9000/tcp").splitlines()[0])
        health_check(endpoint)
        values = {"STORAGE_TEST_S3_ENDPOINT": endpoint, "STORAGE_TEST_S3_BUCKET": bucket}
        for identity in ("source", "target"):
            access, secret = identity + secrets.token_hex(8), secrets.token_hex(24)
            policy = {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": ["s3:GetBucketLocation"],
                 "Resource": [f"arn:aws:s3:::{bucket}"]},
                # HeadBucket shares ListBucket authorization. The fixture's
                # callers disable Hadoop bucket probing instead of granting
                # prefix-less bucket listing to either identity.
                {"Effect": "Allow", "Action": ["s3:ListBucket"],
                 "Resource": [f"arn:aws:s3:::{bucket}"],
                 "Condition": {"StringLike": {"s3:prefix": [identity, identity + "/*"]}}},
                {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                                               "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
                 "Resource": [f"arn:aws:s3:::{bucket}/{identity}/*"]},
            ]}
            docker("exec", "-i", container, "sh", "-c", "cat > /tmp/input-policy.json",
                   stdin=json.dumps(policy))
            docker("exec", container, "mc", "admin", "user", "add", "fixture", access, secret)
            docker("exec", container, "mc", "admin", "policy", "create", "fixture", identity,
                   "/tmp/input-policy.json")
            docker("exec", container, "mc", "admin", "policy", "attach", "fixture", identity,
                   "--user", access)
            values[f"STORAGE_TEST_S3_{identity.upper()}_ACCESS_KEY"] = access
            values[f"STORAGE_TEST_S3_{identity.upper()}_SECRET_KEY"] = secret
        with open(env_file, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as output:
            output.writelines(f"export {key}={shlex.quote(value)}\n" for key, value in values.items())
        if os.environ.get("GITHUB_ENV"):
            mask_workflow_credentials(values)
            with open(os.environ["GITHUB_ENV"], "a") as output:
                output.writelines(f"{key}={value}\n" for key, value in values.items())
        print("Isolated input storage is ready; credentials are in the private environment file.")
    except BaseException as failure:
        try:
            # The known id also covers failure to write the state file.
            stop(env_file, container)
        except BaseException as cleanup_failure:
            note = (
                f"Input storage cleanup failed for container {container}: "
                f"{type(cleanup_failure).__name__}; retry fixture cleanup."
            )
            if callable(getattr(failure, "add_note", None)):
                failure.add_note(note)
            else:
                print(note, file=sys.stderr)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop"))
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--network-container",
                        help="share a local workspace container's network, binding only its loopback")
    arguments = parser.parse_args()
    if arguments.action == "start":
        start(arguments.env_file.resolve(), arguments.network_container)
    else:
        stop(arguments.env_file.resolve())
