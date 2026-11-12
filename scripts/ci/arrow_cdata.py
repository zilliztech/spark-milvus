#!/usr/bin/env python3
"""Supply Arrow 12's missing Linux ARM64 C Data JNI resource for Spark 3.5.

The official Arrow 12.0.1 JAR already supplies the other published platforms.
Build this one library from the exact Apache release, keep its upstream loader
path, and audit it independently of the Milvus engine's native bundle. Release
consumer tests, rather than this static ABI audit, establish runtime acceptance.
"""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
import zipfile


VERSION = "12.0.1"
PLATFORM = "linux-aarch64"
REVISION = "6af660f48472b8b45a5e01b7136b9b040b185eb1"
SOURCE_URL = f"https://archive.apache.org/dist/arrow/arrow-{VERSION}/apache-arrow-{VERSION}.tar.gz"
SOURCE_SHA512 = (
    "551ae200551fcc73b7deddcc5f0b06633159ab1308506901a9086e4e2e34e4437f26d"
    "609fdbacba0ebe7d1fe83bdb8e92a268e9e41575d655d5b2d4fbef7a7ce"
)
RESOURCE = "aarch_64/libarrow_cdata_jni.so"
PREFIX = f"META-INF/spark-milvus/arrow-cdata/{VERSION}/"
PROVENANCE = PREFIX + "linux-aarch64.json"
SOURCE_FILES = (
    "LICENSE.txt", "NOTICE.txt", "java/CMakeLists.txt", "java/c/CMakeLists.txt",
    "java/c/src/main/cpp/abi.h", "java/c/src/main/cpp/jni_wrapper.cc",
    *("java/c/src/main/java/org/apache/arrow/c/jni/" + name + ".java" for name in (
        "CDataJniException", "JniLoader", "JniWrapper", "PrivateData")),
)
# Hashes of these exact entries in the SHA-512-verified Apache source archive.
# Pin individual files too: rehashing an edited embedded source must not turn
# an unchanged upstream archive identity into a false provenance claim.
SOURCE_SHA256 = {
    "LICENSE.txt": "6d26daef86611d1ea2fdf74d900886aa27c46249f018417a993449eb4a414eac",
    "NOTICE.txt": "f5b6a58f5d2082e403c49663d87566328d40cf03f16abb01f52632a09b97c55e",
    "java/CMakeLists.txt": "7b7a7aefcc135c04e6953d9f6461eae915d312e528bf3b699c6270c5f203186d",
    "java/c/CMakeLists.txt": "99448b9dec2254be86330cde409dcd950164b5b015a09eafe3443fe106060ad4",
    "java/c/src/main/cpp/abi.h": "fd03d1d22d8fd5cb0fd3b4a8730ed7d8a390674e5c2e5252cb09edbfa45d0cff",
    "java/c/src/main/cpp/jni_wrapper.cc": "f34ccb06b24c1ae3f9b6e6c02b057f227339e41d4b46d610927a1339abb9a487",
    "java/c/src/main/java/org/apache/arrow/c/jni/CDataJniException.java":
        "f6f082afc791bb7ddad58f281ce6420c324fe5a57e6c66d7bce31ee0f2166148",
    "java/c/src/main/java/org/apache/arrow/c/jni/JniLoader.java":
        "a82d2787d80566c5008726bb5b99dfc688c9c8e45da654a10c09a357f6be3957",
    "java/c/src/main/java/org/apache/arrow/c/jni/JniWrapper.java":
        "0cfb33721a819c668ba15208ab98fb4a85bbd820cfb09799205fe5227e5d6a15",
    "java/c/src/main/java/org/apache/arrow/c/jni/PrivateData.java":
        "8c4023cfb4baa5546c7840e16fc179ffd7ddc377eb4a37568463b04ccbefbc37",
}
SYSTEM_LIBRARIES = {
    "libc.so.6", "libm.so.6", "libpthread.so.0", "libdl.so.2", "librt.so.1",
    "libstdc++.so.6", "libgcc_s.so.1", "ld-linux-aarch64.so.1",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(contents):
    return hashlib.sha256(contents).hexdigest()


def source_identity():
    return {"version": VERSION, "revision": REVISION,
            "url": SOURCE_URL, "sha512": SOURCE_SHA512}


def abi_module():
    script = Path(__file__).resolve().parents[1] / "check-native-abi.py"
    spec = importlib.util.spec_from_file_location("arrow_cdata_abi", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def inspect_library(path):
    abi = abi_module().inspect_elf(path, PLATFORM)
    require(not abi["violations"], "Arrow C Data JNI exceeds the Linux ABI baseline: " +
            ", ".join(abi["violations"]))
    require(set(abi["needed"]) <= SYSTEM_LIBRARIES,
            "Arrow C Data JNI must depend only on system libraries")
    require(abi["soname"] == "libarrow_cdata_jni.so", "Unexpected Arrow C Data JNI SONAME")
    return abi


def verify_library(contents, recorded):
    """Bind audited ELF evidence to bytes on every consumer operating system.

    Linux build/staging hosts also repeat readelf inspection. macOS consumers
    cannot load this Linux resource and validate its recorded ABI and checksum;
    the candidate's preceding Linux audit covers imported symbol versions.
    """
    require(len(contents) >= 64 and contents[:7] == b"\x7fELF\x02\x01\x01" and
            struct.unpack_from("<HH", contents, 16) == (3, 183),
            "Arrow C Data resource must be an ARM64 ELF shared library")
    require(isinstance(recorded, dict) and recorded.get("elfMachine") == 183 and
            recorded.get("soname") == "libarrow_cdata_jni.so" and
            set(recorded.get("needed", [])) <= SYSTEM_LIBRARIES,
            "Arrow C Data ABI evidence has the wrong architecture or dependencies")
    maximum, violations = abi_module().requirement_summary(recorded.get("versionNeeds", {}))
    require(not violations and recorded.get("violations") == [] and
            recorded.get("maxRequired") == maximum, "Arrow C Data ABI evidence exceeds the baseline")
    if platform.system() == "Linux":
        with tempfile.TemporaryDirectory(prefix="arrow-cdata-audit-") as temporary:
            binary = Path(temporary) / "libarrow_cdata_jni.so"
            binary.write_bytes(contents)
            require(recorded == inspect_library(binary), "Arrow C Data ABI evidence mismatch")


def zip_entries(archive):
    entries = archive.infolist()
    names = [entry.filename for entry in entries]
    require(len(names) == len(set(names)), "Duplicate supplement ZIP entry")
    for entry in entries:
        parts = entry.filename.rstrip("/").split("/")
        require(not entry.filename.startswith("/") and "\\" not in entry.filename and
                all(part not in ("", ".", "..") for part in parts), "Unsafe supplement ZIP entry")
        require(not entry.flag_bits & 1, "Encrypted supplement ZIP entry")
    return {entry.filename for entry in entries if not entry.is_dir()}


def verify_jar(path, embedded=False):
    """Verify a standalone supplement or its identical entries in a JVM bundle."""
    with zipfile.ZipFile(path) as archive:
        names = zip_entries(archive)
        expected = {RESOURCE, PROVENANCE, *(PREFIX + "source/" + name for name in SOURCE_FILES)}
        selected = {name for name in names if name.startswith(PREFIX) or name == RESOURCE}
        require(selected == expected, "Missing or unexpected Arrow C Data supplement resources")
        require(embedded or names == expected, "Standalone supplement contains unrelated entries")
        record = json.loads(archive.read(PROVENANCE))
        require(record.get("schemaVersion") == 1 and record.get("platform") == PLATFORM and
                record.get("source") == source_identity(), "Arrow C Data source identity mismatch")
        sources = record.get("sourceFiles", {})
        require(sources == SOURCE_SHA256, "Arrow C Data source inventory differs from the Apache release")
        for name, checksum in sources.items():
            require(checksum == sha256(archive.read(PREFIX + "source/" + name)),
                    "Arrow C Data source checksum mismatch: " + name)
        library = archive.read(RESOURCE)
        require(record.get("library", {}).get("path") == RESOURCE and
                record["library"].get("sha256") == sha256(library) and
                record["library"].get("size") == len(library),
                "Arrow C Data library checksum mismatch")
        verify_library(library, record.get("abi"))
        require(isinstance(record.get("build"), dict) and
                all(record["build"].get(key) for key in ("compiler", "compilerTarget", "cmake", "javac")),
                "Arrow C Data build evidence is missing")
        return record


def write_jar(output, sources, library, build_evidence):
    """Package only audited bytes and exact source inputs, with stable ZIP metadata."""
    payload = library.read_bytes()
    source_bytes = {name: (sources / name).read_bytes() for name in SOURCE_FILES}
    source_checksums = {name: sha256(data) for name, data in source_bytes.items()}
    require(source_checksums == SOURCE_SHA256, "Arrow C Data build sources differ from the Apache release")
    record = {
        "schemaVersion": 1, "platform": PLATFORM, "source": source_identity(),
        "sourceFiles": source_checksums,
        "library": {"path": RESOURCE, "sha256": sha256(payload), "size": len(payload)},
        "abi": inspect_library(library), "build": build_evidence,
    }
    entries = {RESOURCE: payload,
               PROVENANCE: (json.dumps(record, sort_keys=True, indent=2) + "\n").encode(),
               **{PREFIX + "source/" + name: data for name, data in source_bytes.items()}}
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=output.parent, suffix=".jar", delete=False) as temporary:
        candidate = Path(temporary.name)
    try:
        with zipfile.ZipFile(candidate, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, data in sorted(entries.items()):
                entry = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
                entry.compress_type = zipfile.ZIP_DEFLATED
                entry.external_attr = 0o100644 << 16
                archive.writestr(entry, data)
        verify_jar(candidate)
        candidate.replace(output)
    finally:
        candidate.unlink(missing_ok=True)
    return record


def extract_sources(archive_path, destination):
    checksum = hashlib.sha512()
    with archive_path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    require(checksum.hexdigest() == SOURCE_SHA512, "Apache Arrow source archive checksum mismatch")
    prefix = f"apache-arrow-{VERSION}/"
    with tarfile.open(archive_path, "r:gz") as archive:
        for name in SOURCE_FILES:
            member = archive.getmember(prefix + name)
            require(member.isfile() and not member.issym() and not member.islnk(),
                    "Arrow source input must be a regular file: " + name)
            output = destination / name
            output.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source:
                output.write_bytes(source.read())


def command(arguments, environment):
    result = subprocess.run(arguments, env=environment, text=True, check=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return result.stdout.strip()


def build(work, output, cxx=None):
    require(platform.system() == "Linux", "Build the supplement on Linux")
    require(cxx or platform.machine() in ("aarch64", "arm64"),
            "Use a Linux ARM64 runner, or explicitly select an ARM64 cross compiler with --cxx")
    require(not work.exists(), "Arrow C Data build directory already exists")
    require(not output.exists(), "Arrow C Data supplement already exists")
    work.mkdir(parents=True)
    source_archive = work / "apache-arrow.tar.gz"
    with urllib.request.urlopen(SOURCE_URL, timeout=120) as response, source_archive.open("wb") as stream:
        shutil.copyfileobj(response, stream)
    sources = work / "source"
    extract_sources(source_archive, sources)
    environment = os.environ.copy()
    for name in ("LD_LIBRARY_PATH", "LD_PRELOAD", "LD_AUDIT", "JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS"):
        environment.pop(name, None)
    environment["LC_ALL"] = "C"
    compiler = cxx or shutil.which("g++")
    require(compiler, "g++ is required to build Arrow C Data JNI")
    java_home = environment.get("JAVA_HOME")
    require(java_home and (Path(java_home) / "bin/javac").is_file(), "JAVA_HOME must provide a JDK")
    compiler_target = command([compiler, "-dumpmachine"], environment)
    require(compiler_target.startswith("aarch64"), "Compiler must target Linux ARM64")
    configure = [
        "cmake", "-S", str(sources / "java"), "-B", str(work / "build"),
        "-DARROW_JAVA_JNI_ENABLE_C=ON", "-DARROW_JAVA_JNI_ENABLE_DEFAULT=OFF",
        "-DBUILD_TESTING=OFF", "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_INSTALL_LIBDIR=lib/aarch_64",
        "-DCMAKE_INSTALL_PREFIX=" + str(work / "install"), "-DCMAKE_CXX_COMPILER=" + compiler,
    ]
    (work / "configure.log").write_text(command(configure, environment) + "\n")
    (work / "build.log").write_text(command(
        ["cmake", "--build", str(work / "build"), "--target", "install", "--config", "Release"],
        environment) + "\n")
    evidence = {
        "compiler": command([compiler, "--version"], environment).splitlines()[0],
        "compilerTarget": compiler_target,
        "cmake": command(["cmake", "--version"], environment).splitlines()[0],
        "javac": command([str(Path(java_home) / "bin/javac"), "-version"], environment),
    }
    return write_jar(output, sources, work / "install/lib/aarch_64/libarrow_cdata_jni.so", evidence)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    builder = commands.add_parser("build")
    builder.add_argument("--work-dir", type=Path, required=True)
    builder.add_argument("--output", type=Path, required=True)
    builder.add_argument("--cxx", help="Explicit Linux ARM64 cross compiler, for build/ABI checks only")
    verifier = commands.add_parser("verify")
    verifier.add_argument("--input", type=Path, required=True)
    verifier.add_argument("--embedded", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            record = build(args.work_dir.resolve(), args.output.resolve(), args.cxx)
        else:
            record = verify_jar(args.input, embedded=args.embedded)
        print(json.dumps({"version": VERSION, "platform": PLATFORM,
                          "librarySha256": record["library"]["sha256"]}))
        return 0
    except (ValueError, OSError, KeyError, zipfile.BadZipFile, tarfile.TarError,
            subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
