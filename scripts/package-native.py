#!/usr/bin/env python3
"""Validate and package a unified native dependency directory, without modifying it."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit
import zipfile


REQUIRED_ENTRIES = (
    "libmilvus-storage-jni.so",
    "libmilvus-storage.so",
    "libknowhere_jni.so",
    "libknowhere_c.so.1",
    "libknowhere.so",
)
LOAD_ENTRIES = ("libmilvus-storage-jni.so", "libknowhere_jni.so")
AUDIT_DLOPEN_ENTRIES = REQUIRED_ENTRIES
PLUGIN_PARENTS = {"libcardinalv1.so": "libknowhere.so", "libcardinalv2.so": "libknowhere.so"}
C_API_TESTS = [
    "knowhere_c_api",
    "knowhere_c_api_concurrency",
    "knowhere_c_api_diskann_acceptance",
]
EVIDENCE_ENTRIES = {
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
SYSTEM_LIBRARIES = re.compile(
    r"^(?:ld-linux[^/]*|lib(?:c|m|mvec|pthread|dl|rt|resolv|util|gcc_s|stdc\+\+)\.so(?:\..*)?|libz\.so\.1)$"
)


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def command(*arguments, preload=None):
    environment = os.environ.copy()
    for name in ("LD_LIBRARY_PATH", "LD_PRELOAD", "LD_AUDIT", "LD_DEBUG", "LD_BIND_NOW"):
        environment.pop(name, None)
    environment["LC_ALL"] = "C"
    if preload is not None:
        environment["LD_PRELOAD"] = str(preload)
        environment["LD_BIND_NOW"] = "1"
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
        with path.open("rb") as stream:
            header = stream.read(20)
            if header[:4] != b"\x7fELF":
                raise ValueError(f"Non-ELF file in native library directory: {name}")
            if platform is not None:
                machine = {"linux-x86_64": 62, "linux-aarch64": 183}[platform]
                if len(header) < 20 or header[4:6] != b"\x02\x01" or int.from_bytes(header[18:20], "little") != machine:
                    raise ValueError(f"ELF architecture does not match {platform}: {name}")
        dynamic = command("readelf", "-dW", str(path))
        sonames = re.findall(r"\(SONAME\).*\[(.*?)\]", dynamic)
        soname = sonames[0] if sonames else path.name
        # Modules without DT_SONAME are opened by path. Keep distinct module
        # paths while using the same filename fallback as the staging record.
        identity = sonames[0] if sonames else name
        if SYSTEM_LIBRARIES.fullmatch(soname):
            raise ValueError(f"System ABI library must not be bundled: {soname}")
        sha = digest(path)
        record = {"path": path, "sha256": sha, "soname": soname,
                  "needed": re.findall(r"\(NEEDED\).*\[(.*?)\]", dynamic)}
        records[name] = record
        if identity in groups and groups[identity][0][1]["sha256"] != sha:
            raise ValueError(f"Different binaries share SONAME {soname}")
        groups.setdefault(identity, []).append((name, record))
        paths = re.findall(r"\((?:RUNPATH|RPATH)\).*\[(.*?)\]", dynamic)
        depth = len(Path(name).parts) - 1
        permitted = {"$ORIGIN", "$ORIGIN" + "/.." * depth}
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
    if provenance.get("audit") != "passed":
        raise ValueError("Provenance must record a passed native audit")
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


def validate_relocations(libraries, aliases):
    def canonical(name):
        return aliases.get(name, name)

    for name, parent_name in PLUGIN_PARENTS.items():
        if name not in libraries:
            continue
        record = libraries[name]
        output = command("ldd", "-r", str(record["path"]))
        if "undefined symbol:" in output:
            # Cardinal implements callbacks into its loading Knowhere engine.
            # No other library may inherit this plugin loading contract.
            if parent_name not in libraries or any(marker in output for marker in ("not found", "Relink `")):
                raise ValueError(f"Invalid plugin loading context for {name}:\n{output}")
            parent = libraries[parent_name]["path"]
            symbols = command("nm", "-D", "--defined-only", "--format=posix", str(parent))
            defined = {line.split()[0].split("@", 1)[0] for line in symbols.splitlines()}
            missing = set(re.findall(r"undefined symbol: ([^\s]+)", output))
            if not missing or not missing.issubset(defined):
                raise ValueError(f"Plugin {name} has symbols outside its {parent_name} parent: {sorted(missing - defined)}")
            output = command("ldd", "-r", str(record["path"]), preload=parent)
        if any(marker in output for marker in ("undefined symbol:", "not found", "Relink `")):
            raise ValueError(f"Unresolved native relocations in {name}:\n{output}")
    for name, record in libraries.items():
        if name in PLUGIN_PARENTS:
            continue
        output = command("ldd", "-r", str(record["path"]))
        if any(marker in output for marker in ("undefined symbol:", "not found", "Relink `")):
            raise ValueError(f"Unresolved native relocations in {name}:\n{output}")
    for entry in AUDIT_DLOPEN_ENTRIES:
        name = canonical(entry)
        if name not in libraries:
            raise ValueError(f"Missing native load entry: {entry}")
        command(
            sys.executable,
            "-c",
            "import ctypes,os,resource,sys; "
            "resource.setrlimit(resource.RLIMIT_CORE,(0,0)); "
            "ctypes.CDLL(sys.argv[1],mode=os.RTLD_NOW|os.RTLD_LOCAL)",
            str(libraries[name]["path"]),
        )


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
    provenance_bytes = provenance_path.read_bytes()
    provenance = json.loads(provenance_bytes)
    reject_absolute_paths(provenance)
    for key in ("storage.revision", "knowhere.revision"):
        if not re.fullmatch(r"[a-f0-9]{40}", provenance.get(key, "")):
            raise ValueError(f"Missing exact source pin: {key}")
    platform = provenance.get("platform")
    if platform not in ("linux-x86_64", "linux-aarch64"):
        raise ValueError("Unsupported native bundle platform")
    if not isinstance(provenance.get("with_cardinal"), bool):
        raise ValueError("Provenance must record the actual with_cardinal boolean")
    if provenance.get("dependency.mode") != "shared":
        raise ValueError("Provenance must identify the unified shared dependency build")
    libraries, aliases = inventory(directory, platform)
    validate_provenance(provenance, libraries, aliases)
    validate_relocations(libraries, aliases)
    manifest = {"format.version": "1", "platform": platform,
                "storage.revision": provenance["storage.revision"],
                "knowhere.revision": provenance["knowhere.revision"],
                "with_cardinal": str(provenance["with_cardinal"]).lower(),
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
            put(archive, "META-INF/milvus-native/elf.json", json.dumps(
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
