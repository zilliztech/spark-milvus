#!/usr/bin/env python3
"""Validate and package a unified native dependency directory, without modifying it."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "native-build"))
import platforms  # noqa: E402  -- the path above makes this importable

FORMAT = platforms.host()


REQUIRED_ENTRIES = (
    FORMAT.library_name("milvus-storage-jni"),
    FORMAT.library_name("milvus-storage"),
    FORMAT.library_name("knowhere_jni"),
    FORMAT.library_name("knowhere_c", "1"),
    FORMAT.library_name("knowhere"),
)
LOAD_ENTRIES = (FORMAT.library_name("milvus-storage-jni"),
                FORMAT.library_name("knowhere_jni"))
AUDIT_DLOPEN_ENTRIES = REQUIRED_ENTRIES
PLUGIN_PARENTS = {FORMAT.library_name("cardinalv1"): FORMAT.library_name("knowhere"),
                  FORMAT.library_name("cardinalv2"): FORMAT.library_name("knowhere")}
# The DiskANN acceptance fixture exists where DiskANN does.
C_API_TESTS = sorted(
    ["knowhere_c_api", "knowhere_c_api_concurrency"]
    + (["knowhere_c_api_diskann_acceptance"] if FORMAT.builds_diskann() else [])
)
EVIDENCE_ENTRIES = {
    "NativeLoadCheck.java",
    "jvm_load.py",
    "stage.py",
    *("build/" + name for name in (
        "build-source-files.json",
        "cargo.txt",
        "cmake.txt",
        "compiler-native-target.txt",
        "compiler.txt",
        "conan-lock.sha256",
        "conan.lock",
        "conan.txt",
        "corrosion-source-files.json",
        "cpu.txt",
        "dependency-input.json",
        "dependency-version-selection.json",
        "direct-references.json",
        "host-profile",
        "java.txt",
        "knowhere-source-files.json",
        "knowhere-source-identity.json",
        "rustc.txt",
        "storage-source-files.json",
        "storage-source-identity.json",
        "storage-working-tree.patch",
    )),
}
SYSTEM_LIBRARIES = FORMAT.system_libraries()


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def command(*arguments):
    environment = os.environ.copy()
    for name in ("LD_LIBRARY_PATH", "LD_PRELOAD", "LD_AUDIT", "LD_DEBUG", "LD_BIND_NOW"):
        environment.pop(name, None)
    environment["LC_ALL"] = "C"
    result = subprocess.run(arguments, env=environment, text=True, capture_output=True, check=False)
    if result.returncode:
        raise ValueError(f"Command failed ({result.returncode}): {arguments}\n{result.stdout}{result.stderr}")
    return result.stdout + result.stderr


def safe_name(value):
    return value and all(re.fullmatch(r"[A-Za-z0-9_+.-]+", part) and part not in (".", "..")
                         for part in value.split("/"))


def inventory(directory, platform=None):
    """Group actual aliases by SONAME and bytes; reject conflicting implementations."""
    directory = directory.resolve(strict=True)
    groups = {}
    records = {}
    for path in sorted(directory.rglob("*")):
        if path.is_symlink() and not path.is_file():
            raise ValueError(f"Native alias does not resolve to a file: {path.relative_to(directory)}")
        if not path.is_file():
            continue
        name = path.relative_to(directory).as_posix()
        if not safe_name(name) or not path.resolve().is_relative_to(directory):
            raise ValueError(f"Invalid native path: {name}")
        inspected = FORMAT.inspect(path)
        if inspected is None:
            raise ValueError(f"Not a shared library of this platform: {name}")
        if platform is not None and FORMAT.architecture(path) != platform.split("-", 1)[1]:
            raise ValueError(f"Binary architecture does not match {platform}: {name}")
        sonames = [inspected["soname"]] if inspected["soname"] != path.name else []
        soname = inspected["soname"]
        # Modules without DT_SONAME are opened by path. Keep distinct module
        # paths while using the same filename fallback as the staging record.
        identity = sonames[0] if sonames else name
        if SYSTEM_LIBRARIES.fullmatch(soname):
            raise ValueError(f"System ABI library must not be bundled: {soname}")
        sha = digest(path)
        record = {"path": path, "sha256": sha, "soname": soname,
                  "needed": inspected["needed"]}
        records[name] = record
        if identity in groups and groups[identity][0][1]["sha256"] != sha:
            raise ValueError(f"Different binaries share SONAME {soname}")
        groups.setdefault(identity, []).append((name, record))
        recorded = FORMAT.read_runtime_path(path)
        paths = [recorded] if recorded else []
        depth = len(Path(name).parts) - 1
        origin = FORMAT.loader_origin
        permitted = {origin, origin + "/.." * depth}
        if record["needed"] and (not paths or any(item not in permitted for item in paths[0].split(":"))):
            raise ValueError(f"Library must resolve its dependencies from the bundle: {name}: {paths}")
    for entry in REQUIRED_ENTRIES:
        if entry not in records:
            raise ValueError(f"Missing JNI/engine entry: {entry}")
    libraries, aliases = {}, {}
    for soname, items in groups.items():
        canonical, record = next((item for item in items if item[0] == soname), items[0])
        libraries[canonical] = record
        aliases.update((name, canonical) for name, _ in items if name != canonical)
        # Validate the directory that was actually tested. The packager must not
        # repair a missing runtime filename or invent a different dependency set.
        if "/" not in soname and (soname not in records or records[soname]["sha256"] != record["sha256"]):
            raise ValueError(f"Missing or conflicting SONAME filename: {soname}")
    for name, record in libraries.items():
        for needed in record["needed"]:
            if needed not in records and needed not in aliases and not SYSTEM_LIBRARIES.fullmatch(needed):
                raise ValueError(f"Missing non-system dependency {needed}, required by {name}")
    return libraries, aliases


def validate_provenance(provenance, libraries, aliases):
    """Bind the packaged bytes and feature flags to the directory that passed testing."""
    if provenance.get("auditPolicy") != "jvm-load":
        raise ValueError("Provenance must identify the jvm-load audit policy")
    if provenance.get("audit") != "passed":
        raise ValueError("Provenance must record a passed native audit")
    validate_jvm_load_tests(provenance.get("jvmLoadTests"), "Provenance", exact_fields=True)
    test_exit = provenance.get("knowhereCApiTestsExit")
    if type(test_exit) is not int or test_exit != 0:
        raise ValueError("Provenance must record successful Knowhere C API tests")
    if provenance.get("knowhereCApiTests") != C_API_TESTS:
        raise ValueError("Provenance must record the complete expected Knowhere C API test set")
    if provenance.get("dlopenEntries") != list(AUDIT_DLOPEN_ENTRIES):
        raise ValueError("Provenance must record every native load entry")

    recorded_libraries = provenance.get("libraries")
    if not isinstance(recorded_libraries, dict) or set(recorded_libraries) != set(libraries):
        raise ValueError("Native library file set differs from the audited provenance")
    if provenance.get("relocationRoots") != sorted(libraries):
        raise ValueError("Provenance must record every relocated library")
    for name, actual in libraries.items():
        recorded = recorded_libraries[name]
        if not isinstance(recorded, dict):
            raise ValueError(f"Invalid provenance library record: {name}")
        for field in ("sha256", "soname", "needed"):
            if recorded.get(field) != actual[field]:
                raise ValueError(f"Native library {field} differs from the audited provenance: {name}")
    if provenance.get("aliases") != aliases:
        raise ValueError("Native aliases differ from the audited provenance")

    actual_cardinal = set(libraries).intersection(PLUGIN_PARENTS)
    expected_cardinal = set(PLUGIN_PARENTS) if provenance["with_cardinal"] else set()
    if actual_cardinal != expected_cardinal:
        raise ValueError("Cardinal libraries do not match the provenance with_cardinal flag")


def expected_jvm_load_tests():
    return [
        {"entries": list(LOAD_ENTRIES), "exit": 0},
        {"entries": list(reversed(LOAD_ENTRIES)), "exit": 0},
    ]


def validate_jvm_load_tests(tests, source, exact_fields=False):
    expected = expected_jvm_load_tests()
    if not isinstance(tests, list) or len(tests) != len(expected):
        raise ValueError(f"{source} must record both JVM load orders")
    for index, (record, expected_record) in enumerate(zip(tests, expected)):
        if not isinstance(record, dict):
            raise ValueError(f"{source} JVM load record {index} must be an object")
        if exact_fields and set(record) != set(expected_record):
            raise ValueError(f"{source} JVM load record {index} has unexpected fields")
        if record.get("entries") != expected_record["entries"]:
            raise ValueError(f"{source} must record the expected JVM load order at index {index}")
        if type(record.get("exit")) is not int or record["exit"] != 0:
            raise ValueError(f"{source} JVM load record {index} must record exit 0: {record}")


def check_jvm_loads(directory, output=None):
    """Load the shared JVM probe lazily so this script remains directly executable."""
    helper_path = Path(__file__).resolve().parents[1] / "native-build" / "jvm_load.py"
    spec = importlib.util.spec_from_file_location("milvus_native_jvm_load", helper_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Cannot load JVM native validation helper: {helper_path}")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper.check_jvm_loads(directory, output=output)


def put(archive, name, data):
    entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    entry.compress_type = zipfile.ZIP_DEFLATED
    entry.external_attr = 0o100644 << 16
    archive.writestr(entry, data)


def add_directory(archive, directory, prefix, allowed=None):
    directory = directory.resolve(strict=True)
    for path in sorted(directory.rglob("*")):
        name = path.relative_to(directory).as_posix()
        # Source evidence is not an ELF name or a properties key. Upstream
        # source trees contain ordinary names such as @protos and !Compiler.
        safe = all(part not in ("", ".", "..") and not re.search(r"[\x00-\x1f\x7f\\:]", part)
                   for part in name.split("/"))
        if not safe or not path.resolve().is_relative_to(directory):
            raise ValueError(f"Invalid evidence path: {name}")
        if path.is_file():
            if allowed is not None and name not in allowed:
                raise ValueError(f"Unexpected delivery evidence: {name}")
            put(archive, prefix + name, path.read_bytes())


def reject_absolute_paths(value, location="provenance"):
    if isinstance(value, dict):
        for key, child in value.items():
            reject_absolute_paths(child, f"{location}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_absolute_paths(child, f"{location}[{index}]")
    elif isinstance(value, str):
        parsed = urlsplit(value)
        if value.startswith("/") or re.match(r"^[A-Za-z]:[\\/]", value) or parsed.scheme == "file":
            raise ValueError(f"Delivered provenance contains an absolute build path at {location}")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError(f"Delivered provenance contains URL user information at {location}")


def package(directory, provenance_path, output, licenses=None, evidence=None):
    directory = directory.resolve(strict=True)
    provenance_bytes = provenance_path.read_bytes()
    provenance = json.loads(provenance_bytes)
    reject_absolute_paths(provenance)
    for key in ("storage.revision", "knowhere.revision"):
        if not re.fullmatch(r"[a-f0-9]{40}", provenance.get(key, "")):
            raise ValueError(f"Missing exact source pin: {key}")
    platform = provenance.get("platform")
    # A platform is packaged when the build covers it, which is what having a
    # Conan profile means; the driver checks the same thing.
    profiles = Path(__file__).resolve().parents[1] / "native-build" / "profiles"
    if not platform or not (profiles / platform).is_file():
        raise ValueError("Unsupported native bundle platform: " + str(platform))
    for feature in ("with_cardinal", "with_diskann"):
        if not isinstance(provenance.get(feature), bool):
            raise ValueError("Provenance must record the actual " + feature + " boolean")
    if provenance["with_diskann"] != FORMAT.builds_diskann():
        raise ValueError("Provenance with_diskann does not match this platform's Knowhere build")
    if provenance.get("dependency.mode") != "shared":
        raise ValueError("Provenance must identify the unified shared dependency build")
    libraries, aliases = inventory(directory, platform)
    validate_provenance(provenance, libraries, aliases)
    current_jvm_loads = check_jvm_loads(directory, output=None)
    validate_jvm_load_tests(current_jvm_loads, "Current", exact_fields=False)
    manifest = {"format.version": "1", "platform": platform,
                "storage.revision": provenance["storage.revision"],
                "knowhere.revision": provenance["knowhere.revision"],
                "with_cardinal": str(provenance["with_cardinal"]).lower(),
                # The platforms differ in what Knowhere carries, and the bundle
                # says so rather than a consumer inferring it from the platform.
                "with_diskann": str(provenance["with_diskann"]).lower(),
                "provenance.sha256": hashlib.sha256(provenance_bytes).hexdigest(),
                "load.entries": ",".join(LOAD_ENTRIES),
                "libraries": ",".join(sorted(libraries)),
                "aliases": ",".join(sorted(aliases))}
    manifest.update((f"sha256.{name}", record["sha256"]) for name, record in libraries.items())
    manifest.update((f"alias.{name}", canonical) for name, canonical in aliases.items())
    prefix = f"native/milvus/1/{platform}/"
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=output.parent, suffix=".jar", delete=False) as stream:
        temporary = Path(stream.name)
    try:
        with zipfile.ZipFile(temporary, "w", allowZip64=True) as archive:
            put(archive, prefix + "manifest.properties",
                "".join(f"{key}={value}\n" for key, value in sorted(manifest.items())).encode("ascii"))
            for name, record in sorted(libraries.items()):
                contents = record["path"].read_bytes()
                if hashlib.sha256(contents).hexdigest() != record["sha256"]:
                    raise ValueError(f"Native library changed while packaging: {name}")
                put(archive, prefix + name, contents)
            put(archive, "META-INF/milvus-native/provenance.json", provenance_bytes)
            put(archive, "META-INF/milvus-native/binaries.json", json.dumps(
                {name: {key: value for key, value in record.items() if key != "path"}
                 for name, record in sorted(libraries.items())}, indent=2).encode())
            if licenses is not None:
                add_directory(archive, licenses, "META-INF/milvus-native/licenses/")
            if evidence is not None:
                add_directory(archive, evidence, "META-INF/milvus-native/provenance/", EVIDENCE_ENTRIES)
        temporary.replace(output)
        output.chmod(0o644)
    finally:
        temporary.unlink(missing_ok=True)
    sidecar = Path(str(output) + ".properties")
    sidecar.write_text(
        f"jar.sha256={digest(output)}\nplatform={platform}\n", encoding="ascii")
    sidecar.chmod(0o644)
    print(f"Packaged {len(libraries)} libraries and {len(aliases)} aliases: {output}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib-dir", type=Path, required=True)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--licenses", type=Path)
    parser.add_argument("--evidence", type=Path,
                        help="Build locks, profiles, dependency pins, source manifests and source patches to include")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    package(args.lib_dir, args.provenance, args.output, args.licenses, args.evidence)


if __name__ == "__main__":
    main()
