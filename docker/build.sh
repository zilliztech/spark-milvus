#!/usr/bin/env bash
# Stage the public image inputs and package a caller-supplied assembly into the connector image.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON="${PYTHON:-python3}"
ASSEMBLY=""
IMAGE=""
BASE_IMAGE="spark:4.0.1-scala2.13-java21-python3-ubuntu"
PLATFORM=""
PUSH=false

usage() {
  cat <<'HELP'
Usage: ./docker/build.sh --assembly PATH --image TAG [options]

Package an existing Spark 4.0 / Scala 2.13 assembly containing the native
libraries for the selected Linux platform. This does not build native sources.

  --assembly PATH       Required spark/assembly JAR
  --image TAG           Required output image name/tag
  --base-image REF      Compatible Spark 4.0.1 / Scala 2.13 / Java 21 base
                        Default: spark:4.0.1-scala2.13-java21-python3-ubuntu
  --platform PLATFORM   linux/amd64 or linux/arm64; defaults to this machine
  --push                Push the image after a successful build
  --help                Show this help without requiring Docker or an assembly

The script validates the JAR, prints its SHA256 and creates an isolated build
context from an explicit public-file whitelist. It removes that temporary
context on exit. Images remain local unless --push is supplied.
HELP
}

value_required() {
  if [[ $# -lt 2 || -z "$2" || "$2" == --* ]]; then
    echo "$1 requires a value" >&2
    exit 2
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --assembly) value_required "$@"; ASSEMBLY="$2"; shift 2 ;;
    --image) value_required "$@"; IMAGE="$2"; shift 2 ;;
    --base-image) value_required "$@"; BASE_IMAGE="$2"; shift 2 ;;
    --platform) value_required "$@"; PLATFORM="$2"; shift 2 ;;
    --push) PUSH=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1. Run --help for usage." >&2; exit 2 ;;
  esac
done
if [[ -z "$ASSEMBLY" || -z "$IMAGE" ]]; then
  echo "Both --assembly and --image are required. Run --help for usage." >&2
  exit 2
fi
if [[ -z "$PLATFORM" ]]; then
  case "$(uname -m)" in
    x86_64|amd64) PLATFORM=linux/amd64 ;;
    aarch64|arm64) PLATFORM=linux/arm64 ;;
    *) echo "Unsupported machine; select --platform linux/amd64 or linux/arm64" >&2; exit 2 ;;
  esac
fi
case "$PLATFORM" in
  linux/amd64|linux/arm64) ;;
  *) echo "--platform must be linux/amd64 or linux/arm64" >&2; exit 2 ;;
esac
for reference in "$IMAGE" "$BASE_IMAGE"; do
  if [[ "$reference" == -* || "$reference" == *[[:space:]]* ]]; then
    echo "Image references cannot start with '-' or contain whitespace" >&2
    exit 2
  fi
done

BUILD_CONTEXT="$(mktemp -d "${TMPDIR:-/tmp}/spark-milvus-image.XXXXXXXX")"
trap 'rm -rf -- "$BUILD_CONTEXT"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

ASSEMBLY_SHA256="$("$PYTHON" - "$REPO_DIR" "$ASSEMBLY" "$PLATFORM" "$BUILD_CONTEXT" <<'PY'
import hashlib
from pathlib import Path
import re
import runpy
import shutil
import sys
from zipfile import BadZipFile, ZipFile


def copy_public(relative, destination=None):
    source = root / relative
    if not source.is_file() or any(path.is_symlink() for path in (source, *source.parents)):
        raise ValueError("Missing public file or symlink: " + relative)
    target = context / (destination or relative)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    target.chmod(0o644)


try:
    root, assembly, platform, context = sys.argv[1:]
    root, context = Path(root), Path(context)
    assembly = Path(assembly).resolve(strict=True)
    if not assembly.is_file():
        raise ValueError("--assembly must name a regular JAR file")
    staged_jar = context / "connector.jar"
    shutil.copyfile(assembly, staged_jar)
    staged_jar.chmod(0o644)
    native_platform = {"linux/amd64": "linux-x86_64", "linux/arm64": "linux-aarch64"}[platform]
    prefix = "native/milvus/1/" + native_platform + "/"
    with ZipFile(staged_jar) as archive:
        entries = set(archive.namelist())
        plugin = "com/zilliz/spark/connector/extensions/MilvusSparkPlugin.class"
        if plugin not in entries or archive.getinfo(plugin).file_size == 0:
            raise ValueError("Assembly does not contain MilvusSparkPlugin")
        manifest_path = prefix + "manifest.properties"
        if manifest_path not in entries:
            raise ValueError("Assembly lacks native resources for " + native_platform)
        properties = {}
        for line in archive.read(manifest_path).decode("ascii").splitlines():
            key, separator, value = line.partition("=")
            if separator:
                properties[key.strip()] = value.strip()
        if properties.get("platform") != native_platform or properties.get("format.version") != "1":
            raise ValueError("Native manifest does not describe platform " + native_platform)
        for library in ("libmilvus-storage-jni.so", "libknowhere_jni.so"):
            name = library if prefix + library in entries else properties.get("alias." + library, "")
            if (not re.fullmatch(r"[A-Za-z0-9_.+-]+", name)
                    or prefix + name not in entries or archive.getinfo(prefix + name).file_size == 0):
                raise ValueError("Assembly lacks native library " + library + " for " + native_platform)

    # Keep this list explicit: private options, caches, target outputs and future
    # files do not enter the build context just because they share a directory.
    copy_public("docker/Dockerfile", "Dockerfile")
    copy_public("LICENSE")
    copy_public("demo/run.sh")
    for name in (
        "00_snapshot_path.py", "01_read_snapshot.py", "02_filter_project.py",
        "03_sql_analytics.py", "04_exact_search.py", "05_filtered_search.py",
        "06_index_search.py", "07_vector_functions.py", "08_partition_selection.py", "common.py",
    ):
        copy_public("demo/python/" + name)
    for name in ("local-options.json", "s3-options.json", "queries.json"):
        copy_public("demo/config/" + name)
    for name in (
        "DemoSupport.java", "SnapshotPath.java", "ReadSnapshot.java", "FilterProject.java",
        "SqlAnalytics.java", "ExactSearch.java", "FilteredSearch.java", "IndexSearch.java",
        "VectorFunctions.java", "PartitionSelection.java", "package-info.java",
    ):
        copy_public("demo/java/src/main/java/com/zilliz/spark/connector/demo/" + name)
    copy_public("scripts/ci/check_read_fixtures.py")
    fixture = "core/src/test/resources/read-fixtures/collections/nearest"
    check = runpy.run_path(str(root / "scripts/ci/check_read_fixtures.py"))
    check["check_group"](root / fixture)
    copy_public(fixture + "/SHA256SUMS")
    for line in (root / fixture / "SHA256SUMS").read_text().splitlines():
        copy_public(fixture + "/" + line[66:])

    digest = hashlib.sha256()
    with staged_jar.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    print(digest.hexdigest())
except (OSError, ValueError, UnicodeError, BadZipFile) as error:
    sys.exit("Image setup failed: " + str(error))
PY
)"

printf 'Assembly SHA256: %s\n' "$ASSEMBLY_SHA256"
printf 'Building %s for %s\n' "$IMAGE" "$PLATFORM"
docker build --platform "$PLATFORM" \
  --build-arg "BASE_IMAGE=$BASE_IMAGE" --build-arg "ASSEMBLY_SHA256=$ASSEMBLY_SHA256" \
  --tag "$IMAGE" --file "$BUILD_CONTEXT/Dockerfile" "$BUILD_CONTEXT"
if [[ "$PUSH" == true ]]; then
  docker push "$IMAGE"
fi
