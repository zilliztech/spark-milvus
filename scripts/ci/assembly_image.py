#!/usr/bin/env python3
"""Stage and verify the Spark 4.0 assembly that the release image packages.

``stage`` records the assembly next to a manifest that names the release plan it
was built from. ``verify`` checks a downloaded staging directory against the
release tag before any credential is used, and exports the image tags.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
from zipfile import BadZipFile, ZipFile


SPARK_LINE = "4.0"
PLATFORMS = {
    "linux-x86_64": {"dockerPlatform": "linux/amd64", "architecture": "amd64"},
    "linux-aarch64": {"dockerPlatform": "linux/arm64", "architecture": "arm64"},
}
MANIFEST = "assembly-manifest.json"
PLUGIN = "com/zilliz/spark/connector/extensions/MilvusSparkPlugin.class"
LIBRARIES = ("libmilvus-storage-jni.so", "libknowhere_jni.so")
SHA = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9][A-Za-z0-9.-]*)?")
LIBRARY_NAME = re.compile(r"[A-Za-z0-9_.+-]+")


class VerificationError(ValueError):
    """The assembly or its manifest does not describe the expected release."""


def require(condition, message):
    if not condition:
        raise VerificationError(message)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path, description):
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as error:
        raise VerificationError(f"{description} is not valid JSON") from error
    require(isinstance(value, dict), f"{description} must be a JSON object")
    return value


def platform_entry(platform):
    require(platform in PLATFORMS, "Image platform must be one of " + ", ".join(sorted(PLATFORMS)))
    return PLATFORMS[platform]


def jar_name(version):
    return f"spark-milvus-{SPARK_LINE}-bundle-{version}.jar"


def image_tag(version, architecture=None):
    tag = f"{version}-spark{SPARK_LINE}"
    return tag if architecture is None else f"{tag}-{architecture}"


def check_assembly(jar, platform):
    """Require the connector plugin and this platform's native libraries."""
    prefix = f"native/milvus/1/{platform}/"
    try:
        with ZipFile(jar) as archive:
            entries = {info.filename: info.file_size for info in archive.infolist()}
            require(entries.get(PLUGIN, 0) > 0, "Assembly does not contain MilvusSparkPlugin")
            manifest = prefix + "manifest.properties"
            require(manifest in entries, "Assembly lacks native resources for " + platform)
            properties = {}
            for line in archive.read(manifest).decode("ascii").splitlines():
                key, separator, value = line.partition("=")
                if separator:
                    properties[key.strip()] = value.strip()
    except (OSError, BadZipFile, UnicodeError) as error:
        raise VerificationError("Assembly is not a readable JAR: " + str(error)) from error
    require(properties.get("platform") == platform and properties.get("format.version") == "1",
            "Native manifest does not describe platform " + platform)
    for library in LIBRARIES:
        name = library if prefix + library in entries else properties.get("alias." + library, "")
        require(LIBRARY_NAME.fullmatch(name) is not None and entries.get(prefix + name, 0) > 0,
                f"Assembly lacks native library {library} for {platform}")


def stage(assembly, plan_path, platform, native_bundle, output):
    entry = platform_entry(platform)
    plan = load_json(plan_path, "Release plan")
    version, source = plan.get("version", ""), plan.get("sourceSha", "")
    require(VERSION.fullmatch(version) is not None and not version.endswith("-SNAPSHOT"),
            "Release plan lacks an explicit release version")
    require(SHA.fullmatch(source) is not None, "Release plan lacks a source commit")
    require(assembly.is_file(), f"Assembly is not a file: {assembly}")
    require(assembly.name == jar_name(version),
            f"Assembly {assembly.name} does not carry release version {version}")
    require(native_bundle.is_file(), f"Native bundle is not a file: {native_bundle}")
    check_assembly(assembly, platform)
    output.mkdir(parents=True, exist_ok=True)
    staged = output / assembly.name
    shutil.copyfile(assembly, staged)
    staged.chmod(0o644)
    manifest = {
        "schemaVersion": 1,
        "version": version,
        "sourceSha": source,
        "sparkLine": SPARK_LINE,
        "platform": platform,
        "dockerPlatform": entry["dockerPlatform"],
        "architecture": entry["architecture"],
        "jar": staged.name,
        "sha256": sha256(staged),
        "nativeBundleSha256": sha256(native_bundle),
    }
    (output / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                                   encoding="utf-8")
    return manifest


def verify(directory, platform, version, source):
    entry = platform_entry(platform)
    manifest = load_json(directory / MANIFEST, "Assembly manifest")
    expected = {
        "schemaVersion": 1,
        "version": version,
        "sourceSha": source,
        "sparkLine": SPARK_LINE,
        "platform": platform,
        "dockerPlatform": entry["dockerPlatform"],
        "architecture": entry["architecture"],
        "jar": jar_name(version),
    }
    for key, value in expected.items():
        require(manifest.get(key) == value, f"Assembly manifest {key} differs from the release")
    for key in ("sha256", "nativeBundleSha256"):
        require(isinstance(manifest.get(key), str) and SHA256.fullmatch(manifest[key]) is not None,
                f"Assembly manifest lacks {key}")
    jar = directory / manifest["jar"]
    require(jar.is_file() and not jar.is_symlink(), "Staged assembly is missing")
    require(sha256(jar) == manifest["sha256"], "Staged assembly checksum differs from its manifest")
    check_assembly(jar, platform)
    return {
        "assembly": str(jar.resolve()),
        "docker_platform": entry["dockerPlatform"],
        "image_tag": image_tag(version, entry["architecture"]),
        "manifest_tag": image_tag(version),
    }


def write_outputs(values, github_output):
    if github_output is None:
        return
    with open(github_output, "a", encoding="utf-8") as out:
        for key, value in values.items():
            require("\n" not in value, f"Output {key} must be a single line")
            print(f"{key}={value}", file=out)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    staging = commands.add_parser("stage", help="Record the assembly built from a release plan")
    staging.add_argument("--assembly", required=True, type=Path)
    staging.add_argument("--plan", required=True, type=Path)
    staging.add_argument("--platform", required=True)
    staging.add_argument("--native-bundle", required=True, type=Path)
    staging.add_argument("--output", required=True, type=Path)
    checking = commands.add_parser("verify", help="Check a staged assembly against the release tag")
    checking.add_argument("--input", required=True, type=Path)
    checking.add_argument("--platform", required=True)
    checking.add_argument("--version", required=True)
    checking.add_argument("--source-sha", required=True)
    checking.add_argument("--github-output")
    args = parser.parse_args(argv)
    try:
        if args.command == "stage":
            manifest = stage(args.assembly, args.plan, args.platform, args.native_bundle, args.output)
            print(json.dumps(manifest, indent=2, sort_keys=True))
        else:
            values = verify(args.input, args.platform, args.version, args.source_sha)
            write_outputs(values, args.github_output)
            print(json.dumps(values, indent=2, sort_keys=True))
    except VerificationError as error:
        print(f"Assembly image check failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
