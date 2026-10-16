#!/usr/bin/env python3
"""Audit the Linux native JAR's required ELF symbol versions without loading it."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import struct
import subprocess
import sys
import tempfile
import zipfile


BASELINE = {"distribution": "Ubuntu 22.04", "compiler": "GCC 12",
            "GLIBC": "2.35", "GLIBCXX": "3.4.30", "CXXABI": "1.3.13"}
MACHINES = {"linux-x86_64": 62, "linux-aarch64": 183}
# GCC 12's libstdc++ version maps also contain these non-numeric capabilities:
# gcc/releases/gcc-12.1.0/libstdc++-v3/config/abi/pre/{gnu,float128}.ver.
NAMED_CAPABILITIES = {"CXXABI_TM_1", "CXXABI_FLOAT128"}
VERSION = re.compile(r"^(GLIBC|GLIBCXX|CXXABI)_([0-9]+(?:\.[0-9]+)*)$")


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def safe_path(name, directory=False):
    value = name[:-1] if directory and name.endswith("/") else name
    if (not value or any(ord(character) < 32 for character in value)
            or "\\" in value or ":" in value or value.startswith("/")
            or any(part in ("", ".", "..") for part in value.split("/"))):
        raise ValueError("Unsafe ZIP or library path: " + repr(name))
    return value


def manifest_properties(contents):
    result = {}
    for line in contents.decode("ascii").splitlines():
        if not line or line.startswith(("#", "!")):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key or key in result or "\\" in line:
            raise ValueError("Invalid or duplicate native manifest property: " + line)
        result[key] = value
    return result


def parse_readelf(text):
    """Read only DT_NEEDED/SONAME and Version needs; definitions are not imports."""
    needed = re.findall(r"\(NEEDED\).*?\[([^\]]+)\]", text)
    sonames = re.findall(r"\(SONAME\).*?\[([^\]]+)\]", text)
    if len(sonames) > 1 or len(needed) != len(set(needed)):
        raise ValueError("Duplicate ELF SONAME or NEEDED entry")
    for name in needed + sonames:
        safe_path(name)
        if "/" in name:
            raise ValueError("ELF dependency must be a library name: " + name)
    requirements = {}
    declared_providers = None
    provider = None
    remaining = 0
    in_needs = False
    for line in text.splitlines():
        if line.startswith("Version ") and " section " in line:
            if remaining:
                raise ValueError("Incomplete ELF Version needs entries")
            in_needs = line.startswith("Version needs section ")
            if in_needs:
                count = re.search(r"contains (\d+) entr(?:y|ies):", line)
                if count is None or declared_providers is not None:
                    raise ValueError("Invalid ELF Version needs section")
                declared_providers = int(count.group(1))
            continue
        if not in_needs:
            continue
        file_entry = re.search(r"\bFile:\s+(\S+)\s+Cnt:\s+(\d+)", line)
        if file_entry:
            if remaining:
                raise ValueError("Incomplete ELF Version needs entries")
            provider = file_entry.group(1)
            if provider not in needed or provider in requirements:
                raise ValueError("Invalid ELF Version needs provider: " + provider)
            requirements[provider] = []
            remaining = int(file_entry.group(2))
            continue
        name_entry = re.search(r"\bName:\s+(\S+)\s+Flags:", line)
        if name_entry:
            if provider is None or remaining <= 0:
                raise ValueError("Unexpected ELF Version needs name")
            requirements[provider].append(name_entry.group(1))
            remaining -= 1
    if remaining or (declared_providers is not None and declared_providers != len(requirements)):
        raise ValueError("Incomplete ELF Version needs section")
    if "(VERNEED)" in text and declared_providers is None:
        raise ValueError("ELF VERNEED has no readable Version needs section")
    return {"soname": sonames[0] if sonames else None, "needed": sorted(needed),
            "versionNeeds": {name: sorted(set(versions))
                             for name, versions in sorted(requirements.items())}}


def version_tuple(value):
    return tuple(int(part) for part in value.split("."))


def requirement_summary(requirements):
    maximum = {family: None for family in ("GLIBC", "GLIBCXX", "CXXABI")}
    violations = []
    for name in sorted({value for versions in requirements.values() for value in versions}):
        match = VERSION.fullmatch(name)
        if match:
            family, value = match.groups()
            if maximum[family] is None or version_tuple(value) > version_tuple(maximum[family]):
                maximum[family] = value
            if version_tuple(value) > version_tuple(BASELINE[family]):
                violations.append(name + " exceeds " + family + "_" + BASELINE[family])
        elif name.startswith(("GLIBC_", "GLIBCXX_", "CXXABI_")) and name not in NAMED_CAPABILITIES:
            violations.append(name + " is not part of the declared ABI baseline")
    return maximum, violations


def inspect_elf(path, platform):
    with path.open("rb") as source:
        header = source.read(64)
    if len(header) < 64 or header[:4] != b"\x7fELF":
        raise ValueError("Native resource is not ELF")
    if header[4:7] != b"\x02\x01\x01":
        raise ValueError("Native resource must be ELF64 little endian version 1")
    elf_type, machine = struct.unpack_from("<HH", header, 16)
    if elf_type != 3:
        raise ValueError("Native resource must be an ELF shared object")
    if machine != MACHINES[platform]:
        raise ValueError(f"ELF architecture {machine} does not match {platform}")
    environment = os.environ.copy()
    environment["LC_ALL"] = "C"
    for key in ("LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT"):
        environment.pop(key, None)
    process = subprocess.run(["readelf", "--wide", "--dynamic", "--version-info", str(path)],
                             env=environment, text=True, capture_output=True, timeout=120)
    if process.returncode or process.stderr.strip():
        raise ValueError("readelf could not inspect ELF: " + process.stderr.strip())
    record = parse_readelf(process.stdout)
    record["elfMachine"] = machine
    record["maxRequired"], record["violations"] = requirement_summary(record["versionNeeds"])
    return record


def audit_bundle(bundle, platform):
    if platform not in MACHINES:
        raise ValueError("Unsupported ABI audit platform: " + platform)
    before = digest(bundle)
    prefix = f"native/milvus/1/{platform}/"
    libraries = {}
    with zipfile.ZipFile(bundle) as archive, tempfile.TemporaryDirectory(prefix="native-abi-") as directory:
        entries = {}
        for entry in archive.infolist():
            safe_path(entry.orig_filename, entry.is_dir())
            mode = stat.S_IFMT(entry.external_attr >> 16)
            if mode not in (0, stat.S_IFREG, stat.S_IFDIR):
                raise ValueError("ZIP entry is not a regular file or directory: " + entry.filename)
            if entry.filename in entries:
                raise ValueError("Duplicate ZIP entry: " + entry.filename)
            entries[entry.filename] = entry
        manifest_name = prefix + "manifest.properties"
        if manifest_name not in entries:
            raise ValueError("Missing native manifest: " + manifest_name)
        manifest = manifest_properties(archive.read(manifest_name))
        if manifest.get("format.version") != "1" or manifest.get("platform") != platform:
            raise ValueError("Native manifest format or platform does not match ABI audit")
        names = manifest.get("libraries", "").split(",")
        if not all(names) or len(names) != len(set(names)):
            raise ValueError("Native manifest must declare unique libraries")
        for name in names:
            safe_path(name)
        expected = {prefix + name for name in names}
        actual = {name for name, entry in entries.items()
                  if name.startswith("native/") and not entry.is_dir() and name != manifest_name}
        if expected != actual:
            raise ValueError("Native ZIP resources differ from the manifest library set")
        for name, entry in sorted(entries.items()):
            if entry.is_dir() or name == manifest_name:
                continue
            with archive.open(entry) as source:
                header = source.read(64)
                if name not in expected:
                    if header.startswith(b"\x7fELF"):
                        raise ValueError("ELF resource outside the declared native platform: " + name)
                    continue
                # Never extract a ZIP-controlled path or symlink into the filesystem.
                path = Path(directory) / "library.elf"
                checksum = hashlib.sha256(header)
                with path.open("wb") as destination:
                    destination.write(header)
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        checksum.update(chunk)
                        destination.write(chunk)
            library = name[len(prefix):]
            if manifest.get("sha256." + library) != checksum.hexdigest():
                raise ValueError("Native library checksum mismatch: " + library)
            try:
                record = inspect_elf(path, platform)
            except ValueError as error:
                raise ValueError(library + ": " + str(error)) from error
            record["sha256"] = checksum.hexdigest()
            libraries[library] = record
        aliases = {}
        for alias in filter(None, manifest.get("aliases", "").split(",")):
            safe_path(alias)
            target = manifest.get("alias." + alias)
            if alias in aliases or alias in libraries or target not in libraries:
                raise ValueError("Invalid native library alias: " + alias)
            aliases[alias] = target
    if digest(bundle) != before:
        raise ValueError("Native bundle changed during ABI audit")
    provided = {PurePosixPath(name).name for name in libraries}
    provided.update(record["soname"] for record in libraries.values() if record["soname"])
    provided.update(PurePosixPath(name).name for name in aliases)
    system = {}
    all_requirements = {}
    violations = []
    for name, record in sorted(libraries.items()):
        record["systemDependencies"] = sorted(set(record["needed"]) - provided)
        for provider in record["systemDependencies"]:
            dependency = system.setdefault(provider, {"consumers": [], "versions": set()})
            dependency["consumers"].append(name)
            dependency["versions"].update(record["versionNeeds"].get(provider, []))
        all_requirements[name] = [version for versions in record["versionNeeds"].values() for version in versions]
        violations.extend(name + ": " + error for error in record["violations"])
    for dependency in system.values():
        dependency["versions"] = sorted(dependency["versions"])
    maximum, _ = requirement_summary(all_requirements)
    return {"schemaVersion": 1, "status": "failed" if violations else "passed", "passed": not violations,
            "jar.sha256": before, "platform": platform, "baseline": BASELINE,
            "maxRequired": maximum, "libraries": libraries,
            "systemDependencies": dict(sorted(system.items())), "violations": violations}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--platform", choices=sorted(MACHINES), required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.output.resolve() == arguments.bundle.resolve():
        parser.error("--output must not overwrite --bundle")
    try:
        report = audit_bundle(arguments.bundle, arguments.platform)
    except (OSError, ValueError, zipfile.BadZipFile, subprocess.SubprocessError) as error:
        report = {"schemaVersion": 1, "status": "failed", "passed": False, "platform": arguments.platform,
                  "baseline": BASELINE, "errors": [str(error)]}
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if report["status"] != "passed":
        print("Native ABI audit failed: " + "; ".join(report.get("errors", report.get("violations", []))),
              file=sys.stderr)
        return 1
    print(f"Native ABI audit passed: {len(report['libraries'])} ELF libraries, {arguments.platform}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
