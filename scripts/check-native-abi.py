#!/usr/bin/env python3
"""Audit a native JAR's system requirements without loading it.

Linux bundles are ELF: the audit reads each library's imported symbol versions
and rejects anything above the Ubuntu 22.04 / GCC 12 baseline. The macOS bundle
is Mach-O: the audit reads each library's deployment target and dependency
paths, and rejects a target above macOS 14.0 or a dependency that is neither a
system library nor carried by the bundle.
"""

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


BASELINES = {
    "linux": {"distribution": "Ubuntu 22.04", "compiler": "GCC 12",
              "GLIBC": "2.35", "GLIBCXX": "3.4.30", "CXXABI": "1.3.13"},
    # The deployment target every macOS library is built for; a consumer on
    # that release or later maps the bundle. The system libc++ comes with it.
    "darwin": {"system": "macOS", "compiler": "Apple clang 17", "MACOS": "14.0"},
}
BASELINE = BASELINES["linux"]
# Platform -> (binary family, machine): the ELF e_machine, or the Mach-O cputype.
PLATFORMS = {"linux-x86_64": ("linux", 62), "linux-aarch64": ("linux", 183),
             "darwin-aarch64": ("darwin", 0x0100000C), "darwin-x86_64": ("darwin", 0x01000007)}
MACHINES = {platform: machine for platform, (family, machine) in PLATFORMS.items() if family == "linux"}
MACHO_MAGICS = (b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf", b"\xca\xfe\xba\xbe")
MH_DYLIB = 6
# Where macOS keeps what it supplies; a dependency there is resolved by dyld's
# shared cache and is never bundled.
MACHO_SYSTEM_PREFIXES = ("/usr/lib/", "/System/Library/")
MACHO_BUNDLE_PREFIXES = ("@rpath/", "@loader_path/")
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


def parse_otool(text):
    """Load commands from `otool -l`: install name, dependency paths, run paths
    and the deployment target, which a library states exactly once."""
    record = {"installName": None, "needPaths": [], "rpaths": [], "minos": None, "sdk": None}
    targets = 0
    for block in re.split(r"^Load command \d+\n", text, flags=re.M)[1:]:
        command = re.search(r"^\s*cmd (\S+)", block, re.M)
        if not command:
            raise ValueError("Mach-O load command without a cmd field")
        name = command.group(1)
        if name in ("LC_LOAD_DYLIB", "LC_LOAD_WEAK_DYLIB", "LC_REEXPORT_DYLIB", "LC_ID_DYLIB"):
            path = re.search(r"^\s*name (\S+) \(offset \d+\)", block, re.M)
            if not path:
                raise ValueError(name + " without a library path")
            if name == "LC_ID_DYLIB":
                if record["installName"] is not None:
                    raise ValueError("Mach-O library states two install names")
                record["installName"] = path.group(1)
            else:
                record["needPaths"].append(path.group(1))
        elif name == "LC_RPATH":
            path = re.search(r"^\s*path (\S+) \(offset \d+\)", block, re.M)
            if not path:
                raise ValueError("LC_RPATH without a path")
            record["rpaths"].append(path.group(1))
        elif name == "LC_BUILD_VERSION":
            fields = dict(re.findall(r"^\s*(platform|minos|sdk) (\S+)", block, re.M))
            if fields.get("platform") != "1":
                raise ValueError("Mach-O library is not built for macOS (platform "
                                 + fields.get("platform", "?") + ")")
            if "minos" not in fields:
                raise ValueError("LC_BUILD_VERSION without minos")
            record["minos"], record["sdk"] = fields["minos"], fields.get("sdk")
            targets += 1
        elif name == "LC_VERSION_MIN_MACOSX":
            fields = dict(re.findall(r"^\s*(version|sdk) (\S+)", block, re.M))
            if "version" not in fields:
                raise ValueError("LC_VERSION_MIN_MACOSX without version")
            record["minos"], record["sdk"] = fields["version"], fields.get("sdk")
            targets += 1
    if targets != 1:
        raise ValueError("Mach-O library must state exactly one deployment target; found " + str(targets))
    if len(record["needPaths"]) != len(set(record["needPaths"])):
        raise ValueError("Duplicate Mach-O dependency")
    return record


def macos_requirements(record, baseline=None):
    """The deployment target the library needs, and what exceeds the baseline or
    is resolved from neither the system nor the bundle."""
    baseline = baseline or BASELINES["darwin"]
    violations = []
    if version_tuple(record["minos"]) > version_tuple(baseline["MACOS"]):
        violations.append("deployment target " + record["minos"] + " exceeds macOS " + baseline["MACOS"])
    for path in record["needPaths"]:
        if not path.startswith(MACHO_SYSTEM_PREFIXES + MACHO_BUNDLE_PREFIXES):
            violations.append("dependency " + path + " is neither a system library nor bundled")
    return {"MACOS": record["minos"]}, violations


def inspect_macho(path, platform):
    with path.open("rb") as source:
        header = source.read(32)
    if len(header) < 32 or header[:4] != b"\xcf\xfa\xed\xfe":
        raise ValueError("Native resource is not a 64-bit little endian Mach-O file")
    cputype, _, filetype = struct.unpack_from("<IIi", header, 4)
    if filetype != MH_DYLIB:
        raise ValueError("Native resource must be a Mach-O dynamic library")
    if cputype != PLATFORMS[platform][1]:
        raise ValueError(f"Mach-O cputype {cputype:#x} does not match {platform}")
    environment = os.environ.copy()
    environment["LC_ALL"] = "C"
    for key in ("DYLD_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES", "DYLD_FRAMEWORK_PATH",
                "DYLD_FALLBACK_LIBRARY_PATH"):
        environment.pop(key, None)
    process = subprocess.run(["otool", "-l", str(path)], env=environment, text=True,
                             capture_output=True, timeout=120)
    if process.returncode or process.stderr.strip():
        raise ValueError("otool could not inspect Mach-O: " + process.stderr.strip())
    record = parse_otool(process.stdout)
    record["machoCpuType"] = cputype
    install_name = record["installName"]
    record["soname"] = PurePosixPath(install_name).name if install_name else path.name
    record["needed"] = [PurePosixPath(name).name for name in record["needPaths"]
                        if not name.startswith(MACHO_SYSTEM_PREFIXES)]
    record["systemPaths"] = [name for name in record["needPaths"] if name.startswith(MACHO_SYSTEM_PREFIXES)]
    # The field the Linux report also carries, so consumers of both read one shape.
    record["versionNeeds"] = {}
    record["maxRequired"], record["violations"] = macos_requirements(record)
    return record


def is_native_binary(header):
    return header.startswith(b"\x7fELF") or header.startswith(MACHO_MAGICS)


def audit_bundle(bundle, platform):
    if platform not in PLATFORMS:
        raise ValueError("Unsupported ABI audit platform: " + platform)
    family = PLATFORMS[platform][0]
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
                    if is_native_binary(header):
                        raise ValueError("Native binary outside the declared native platform: " + name)
                    continue
                # Never extract a ZIP-controlled path or symlink into the filesystem.
                path = Path(directory) / "library.bin"
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
                record = inspect_elf(path, platform) if family == "linux" else inspect_macho(path, platform)
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
        if family == "linux":
            # ELF names a dependency by SONAME; whatever the bundle does not
            # provide comes from the system and is held to the baseline.
            record["systemDependencies"] = sorted(set(record["needed"]) - provided)
            for provider in record["systemDependencies"]:
                dependency = system.setdefault(provider, {"consumers": [], "versions": set()})
                dependency["consumers"].append(name)
                dependency["versions"].update(record["versionNeeds"].get(provider, []))
            all_requirements[name] = [version for versions in record["versionNeeds"].values()
                                      for version in versions]
        else:
            # Mach-O records a path: a system path is supplied by macOS, and an
            # @rpath/@loader_path name has to be carried by the bundle itself.
            record["systemDependencies"] = sorted(record["systemPaths"])
            for provider in record["systemDependencies"]:
                dependency = system.setdefault(provider, {"consumers": [], "versions": set()})
                dependency["consumers"].append(name)
            for dependency in record["needed"]:
                if dependency not in provided:
                    violations.append(name + ": bundled dependency " + dependency + " is not in the bundle")
            all_requirements[name] = record["minos"]
        violations.extend(name + ": " + error for error in record["violations"])
    for dependency in system.values():
        dependency["versions"] = sorted(dependency["versions"])
    if family == "linux":
        maximum, _ = requirement_summary(all_requirements)
    else:
        maximum = {"MACOS": max(all_requirements.values(), key=version_tuple) if all_requirements else None}
    return {"schemaVersion": 1, "status": "failed" if violations else "passed", "passed": not violations,
            "jar.sha256": before, "platform": platform, "baseline": BASELINES[family],
            "maxRequired": maximum, "libraries": libraries,
            "systemDependencies": dict(sorted(system.items())), "violations": violations}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--platform", choices=sorted(PLATFORMS), required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.output.resolve() == arguments.bundle.resolve():
        parser.error("--output must not overwrite --bundle")
    try:
        report = audit_bundle(arguments.bundle, arguments.platform)
    except (OSError, ValueError, zipfile.BadZipFile, subprocess.SubprocessError) as error:
        report = {"schemaVersion": 1, "status": "failed", "passed": False, "platform": arguments.platform,
                  "baseline": BASELINES[PLATFORMS[arguments.platform][0]], "errors": [str(error)]}
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if report["status"] != "passed":
        print("Native ABI audit failed: " + "; ".join(report.get("errors", report.get("violations", []))),
              file=sys.stderr)
        return 1
    print(f"Native ABI audit passed: {len(report['libraries'])} libraries, {arguments.platform}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
