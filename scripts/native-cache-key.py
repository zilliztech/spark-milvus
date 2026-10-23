#!/usr/bin/env python3
"""Identify reusable OSS native bundles without depending on the JVM release.

Run after preparing the native toolchain and initializing engine submodules.
Cache only the final JAR and checksum sidecar under this exact key. A hit still
needs the current ABI audit and the complete candidate consumer gate.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform as host_platform
import re
import shlex
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
CACHE_FORMAT = 2
ENGINES = ("milvus-storage", "knowhere")
PLATFORMS = {"linux-x86_64": "x86_64", "linux-aarch64": "aarch64"}
EXPLICIT_INPUTS = (
    "Makefile",
    "scripts/build-native.sh",
    "scripts/package-native.py",
    "scripts/check-native-abi.py",
    "scripts/native-cache-key.py",
    "scripts/ci/prepare_native.sh",
)
BUILD_OVERRIDES = (
    "CFLAGS", "CXXFLAGS", "CPPFLAGS", "LDFLAGS", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS",
    "NATIVE_BUILD_OPTIONS", "NATIVE_BUNDLE", "CMAKE_TOOLCHAIN_FILE",
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def command(arguments, *, cwd=None):
    environment = dict(os.environ, LC_ALL="C")
    for name in ("LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT"):
        environment.pop(name, None)
    return subprocess.check_output(arguments, cwd=cwd, env=environment,
                                   stderr=subprocess.STDOUT, text=True).strip()


def validate_pins(root, plan):
    """The builder consumes the index and engine worktrees, not just plan text."""
    pins = {}
    for engine in ENGINES:
        expected = plan.get("pins", {}).get(engine, "")
        require(re.fullmatch(r"[0-9a-f]{40}", expected), f"Invalid plan pin: {engine}")
        head = command(["git", "ls-tree", "HEAD", "--", engine], cwd=root).split()
        index = command(["git", "ls-files", "--stage", "--", engine], cwd=root).split()
        require(head == ["160000", "commit", expected, engine]
                and index == ["160000", expected, "0", engine],
                f"Plan pin differs from the recorded gitlink: {engine}")
        source = root / engine
        require((source / ".git").exists(), f"Engine submodule is not initialized: {engine}")
        require(command(["git", "rev-parse", "HEAD"], cwd=source) == expected,
                f"Engine checkout differs from the plan pin: {engine}")
        require(not command(["git", "status", "--porcelain", "--untracked-files=all",
                             "--ignore-submodules=none"], cwd=source),
                f"Engine checkout must be clean for native caching: {engine}")
        pins[engine] = expected
    return pins


def source_inputs(root):
    # ls-files excludes local build outputs and includes newly staged build
    # inputs. Explicit entry scripts also work before their first commit.
    names = command(["git", "ls-files", "-z", "--", "native/build"], cwd=root).split("\0")
    names = {name for name in names if name} | set(EXPLICIT_INPUTS)
    require(any(name.startswith("native/build/") for name in names), "No tracked native build inputs")
    result = {}
    for name in sorted(names):
        path = root / name
        require(path.is_file() and not path.is_symlink()
                and path.resolve().is_relative_to(root.resolve()), f"Invalid native build input: {name}")
        result[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def os_release(path=Path("/etc/os-release")):
    values = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        require(separator, "Malformed /etc/os-release")
        words = shlex.split(value)
        require(len(words) <= 1, "Malformed /etc/os-release value")
        values[key] = words[0] if words else ""
    return {"id": values.get("ID"), "versionId": values.get("VERSION_ID")}


def toolchain_identity(platform):
    require(host_platform.system() == "Linux" and host_platform.machine() == PLATFORMS[platform],
            "Native cache runner architecture does not match " + platform)
    require(not any(os.environ.get(name) for name in BUILD_OVERRIDES),
            "Native release caching requires the checked-in build options without environment overrides")
    java_home = Path(os.environ.get("JAVA_HOME", ""))
    require(java_home.is_absolute() and (java_home / "bin/java").is_file(), "JAVA_HOME must name the prepared JDK")
    probes = {
        "gcc": ["gcc-12", "--version"],
        "g++": ["g++-12", "--version"],
        "gfortran": ["gfortran-12", "--version"],
        "compilerTarget": ["gcc-12", "-dumpmachine"],
        "cmake": ["cmake", "--version"],
        "conan": ["conan", "--version"],
        "rustc": ["rustc", "--version", "--verbose"],
        "cargo": ["cargo", "--version"],
        "java": [str(java_home / "bin/java"), "-version"],
        "javac": [str(java_home / "bin/javac"), "-version"],
        "ninja": ["ninja", "--version"],
        "patchelf": ["patchelf", "--version"],
        "readelf": ["readelf", "--version"],
        "python": [sys.executable, "--version"],
        "systemPackages": ["dpkg-query", "-W", "-f=${binary:Package}=${Version}\n",
                           "libc6", "libc6-dev", "libstdc++6", "libaio-dev", "libclang-dev"],
    }
    tools = {name: command(arguments) for name, arguments in probes.items()}
    require(all(tools.values()), "A native toolchain version probe returned no identity")
    return {"os": os_release(), "glibc": command(["getconf", "GNU_LIBC_VERSION"]), "tools": tools}


def fingerprint(root, plan, platform, identity):
    rows = [row for row in plan["native"] if row["platform"] == platform]
    require(len(rows) == 1, "Expected exactly one native plan row for " + platform)
    row = rows[0]
    require(identity["os"] == {"id": "ubuntu", "versionId": "22.04"}
            and identity["glibc"] == "glibc " + row["glibc"] and row["libc"] == "glibc",
            "Native cache runner does not match the Ubuntu 22.04 glibc build baseline")
    inputs = {
        "cacheFormat": CACHE_FORMAT,
        "platform": platform,
        "runner": row["runner"],
        "baseline": {name: row[name] for name in ("libc", "glibc", "glibcxx", "cxxabi")},
        "features": {"with_cardinal": False},
        "pins": validate_pins(root, plan),
        "files": source_inputs(root),
        "toolchain": identity,
    }
    def identity_digest(value):
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    digest = identity_digest(inputs)
    # Profiles invalidate complete bundles. Dependency caches retain multiple
    # Conan package IDs (including declared CPU targets and thread capacity),
    # while ccache distinguishes compiler flags and Cargo caches downloads.
    toolchain_digest = identity_digest({name: inputs[name] for name in
                                       ("cacheFormat", "platform", "runner", "baseline", "toolchain")})
    return {"schemaVersion": 1, "key": f"native-bundle-v{CACHE_FORMAT}-{platform}-{digest}",
            "keyDigest": digest, "toolchainDigest": toolchain_digest,
            "dependencyKeyPrefix": f"native-deps-v{CACHE_FORMAT}-{platform}-{toolchain_digest}-", "inputs": inputs}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--platform", choices=sorted(PLATFORMS), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text())
    result = fingerprint(ROOT, plan, args.platform, toolchain_identity(args.platform))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as stream:
            stream.write(f"key={result['key']}\nkey_digest={result['keyDigest']}\n"
                         f"toolchain_digest={result['toolchainDigest']}\n"
                         f"dependency_key_prefix={result['dependencyKeyPrefix']}\n")
    print(result["key"])


if __name__ == "__main__":
    main()
