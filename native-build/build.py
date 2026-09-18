#!/usr/bin/env python3
"""Build both upstream JNI implementations with one independent CMake project."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

import platforms


KNOWHERE_C_API_TESTS = [
    "knowhere_c_api",
    "knowhere_c_api_concurrency",
    "knowhere_c_api_diskann_acceptance",
]


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def run(command, cwd=None, output=None, env=None):
    print("+ " + " ".join(map(str, command)), flush=True)
    if output:
        with Path(output).open("w") as destination:
            subprocess.run(list(map(str, command)), cwd=cwd, env=env, stdout=destination, check=True)
    else:
        subprocess.run(list(map(str, command)), cwd=cwd, env=env, check=True)


def git_value(source, *args):
    return subprocess.check_output(["git", "-C", str(source), *args], text=True).strip()


def clone(source, revision, destination, recorded_files=None):
    if destination.exists():
        if git_value(destination, "rev-parse", "HEAD") != revision:
            raise RuntimeError("Existing source checkout differs from pinned revision: " + str(destination))
        if recorded_files is not None:
            if tree_hashes(destination) != recorded_files:
                raise RuntimeError("Recorded source checkout was modified: " + str(destination))
        elif git_value(destination, "status", "--porcelain", "--untracked-files=no"):
            raise RuntimeError("Pinned source has tracked modifications: " + str(destination))
        return
    run(["git", "init", "-q", destination])
    run(["git", "-C", destination, "fetch", "--depth=1", str(source), revision])
    run(["git", "-C", destination, "checkout", "-q", "--detach", "FETCH_HEAD"])


def tree_hashes(directory):
    ignored = {".git", "build", "target", "__pycache__"}
    result = {str(path.relative_to(directory)): digest(path) for path in sorted(directory.rglob("*"))
              if path.is_file() and not ignored.intersection(path.relative_to(directory).parts)
              and not path.name.endswith(".so") and ".so." not in path.name}
    if not result:
        raise RuntimeError("Source manifest is empty: " + str(directory))
    return result


def submodule_source(repository, path, label):
    """Return a superproject gitlink revision and its initialized checkout."""
    pinned_source = (repository / path).resolve()
    if not pinned_source.is_dir() or not (pinned_source / ".git").exists():
        raise ValueError(f"Initialize the root {path} Git submodule before building native libraries")
    entry = git_value(repository, "ls-files", "--stage", "--", path).split()
    if len(entry) != 4 or entry[0] != "160000" or entry[2] != "0" or entry[3] != path:
        raise ValueError(f"{label} must be recorded as one Git submodule entry")
    revision = entry[1]
    if len(revision) != 40 or any(value not in "0123456789abcdef" for value in revision):
        raise ValueError(f"{label} gitlink must contain a complete Git hash")
    if git_value(pinned_source, "rev-parse", "HEAD") != revision:
        raise ValueError(f"{label} submodule HEAD differs from the recorded gitlink")
    return revision, pinned_source


def storage_revision(repository, source, override):
    """Bind a storage source tree to the superproject's recorded gitlink."""
    revision, _ = submodule_source(repository, "milvus-storage", "Storage")
    source = source.resolve()
    if not source.is_dir():
        raise ValueError("--storage-source must name a local source directory")
    if (source / ".git").exists():
        if git_value(source, "rev-parse", "HEAD") != revision:
            raise ValueError("Storage object source HEAD differs from the recorded gitlink")
        if override and override != revision:
            raise ValueError("Storage revision override differs from the recorded gitlink")
    elif override != revision:
        raise ValueError("An exported storage source requires --storage-revision matching the recorded gitlink")
    return revision


def knowhere_source(repository, override):
    """Return the recorded submodule revision and a checkout that supplies that Git object."""
    revision, pinned_source = submodule_source(repository, "knowhere", "Knowhere")
    object_source = (override or pinned_source).resolve()
    if not object_source.is_dir() or not (object_source / ".git").exists():
        raise ValueError("--knowhere-source must name a local Git checkout")
    if git_value(object_source, "rev-parse", "HEAD") != revision:
        raise ValueError("Knowhere object source HEAD differs from the recorded gitlink")
    return revision, object_source


def check_cmake_trace(path, upstream_sources):
    """Reject configuration that executes an engine's upstream build files."""
    roots = [Path(source).resolve() for source in upstream_sources]
    count = 0
    for line in path.read_text().splitlines():
        event = json.loads(line)
        if "file" not in event:
            continue
        filename = Path(event["file"]).resolve()
        if any(filename.is_relative_to(root) for root in roots):
            raise RuntimeError("Independent build executed upstream CMake: " + str(filename))
        count += 1
    if not count:
        raise RuntimeError("CMake trace contains no executed commands")
    return count


def platform_tool_requirements(profile):
    section = None
    references = set()
    for line in profile.read_text().splitlines():
        line = line.strip()
        if line.startswith("["):
            section = line
        elif section == "[platform_tool_requires]" and line and not line.startswith("#"):
            references.add(line)
    return references


def validate_conan_lock(path, references, platform_tools=()):
    """Require concrete recipe revisions; Conan checks the complete graph next."""
    lock = json.loads(path.read_text())
    if lock.get("version") != "0.5":
        raise ValueError("Unsupported Conan lockfile version: " + str(path))
    for section in ("requires", "build_requires", "python_requires", "config_requires"):
        entries = lock.get(section, [])
        if not isinstance(entries, list):
            raise ValueError("Invalid Conan lock section: " + section)
        for value in entries:
            # Platform tools come from the explicit profile, not a Conan recipe.
            if isinstance(value, str) and section == "build_requires" and value in platform_tools:
                continue
            if not isinstance(value, str) or "#" not in value or not value.split("#", 1)[1].split("%", 1)[0]:
                raise ValueError("Conan lock must pin every recipe revision in " + section)
    locked = {value.split("%", 1)[0] for value in lock.get("requires", [])}
    missing = set(references.values()) - locked
    if missing:
        raise ValueError("Conan lock does not match selected direct recipes: " + ", ".join(sorted(missing)))
    return lock


def prepare_conan_lock(provided, destination, identity, references, platform_tools=()):
    """Preserve a supplied lock byte-for-byte and reject changed resume inputs."""
    if provided is not None:
        validate_conan_lock(provided, references, platform_tools)
        if destination.exists() and digest(destination) != digest(provided):
            raise ValueError("Conan lock changed; use a new work directory")
        if not destination.exists():
            shutil.copy2(provided, destination)
    if destination.exists():
        validate_conan_lock(destination, references, platform_tools)
        expected = digest(destination)
        if identity.exists() and identity.read_text().strip() != expected:
            raise ValueError("Recorded Conan lock was modified; use a new work directory")
        identity.write_text(expected + "\n")


def validate_locked_graph(graph, lock):
    """The lock must cover every resolved host and build dependency."""
    sections = {"host": "requires", "build": "build_requires"}
    locked = {section: {value.split("%", 1)[0] for value in lock.get(section, [])}
              for section in sections.values()}
    for identifier, node in graph.items():
        context = node.get("context")
        if context not in sections or node.get("recipe") == "Consumer":
            continue
        reference = node.get("ref")
        if not isinstance(reference, str) or not reference:
            raise ValueError("Resolved dependency has no reference: " + str(identifier))
        section = sections[context]
        if reference.split("%", 1)[0] not in locked[section]:
            raise ValueError("Resolved dependency is absent from the Conan lock: " + reference)


def snapshot_corrosion(specification, local_source, destination):
    revision = specification["revision"]
    if len(revision) != 40 or any(value not in "0123456789abcdef" for value in revision):
        raise ValueError("Corrosion must pin a complete Git revision")
    source = specification["repository"]
    if local_source is not None:
        if git_value(local_source, "rev-parse", "HEAD") != revision:
            raise ValueError("Corrosion source does not match the pinned revision")
        if git_value(local_source, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("Corrosion source must be a clean pinned checkout")
        source = str(local_source.resolve())
    clone(source, revision, destination)
    if git_value(destination, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("Corrosion snapshot was modified")
    return tree_hashes(destination)


def promote_bundle(candidate, work):
    """Keep both failed candidates and previous successful output for diagnosis."""
    metadata = json.loads((candidate / "provenance.json").read_text())
    entries = ["libmilvus-storage-jni.so", "libknowhere_jni.so"]
    orders = metadata.get("jvmLoadTests")
    jvm_passed = (isinstance(orders, list) and len(orders) == 2
                  and all(isinstance(record, dict) and record.get("entries") == expected
                          and type(record.get("exit")) is int and record["exit"] == 0
                          for record, expected in zip(orders, (entries, entries[::-1]))))
    if (metadata.get("audit") != "passed" or metadata.get("knowhereCApiTestsExit") != 0
            or sorted(metadata.get("knowhereCApiTests", [])) != KNOWHERE_C_API_TESTS
            or metadata.get("auditPolicy") != "jvm-load" or not jvm_passed):
        raise ValueError("Native candidate has not passed all required validation: " + str(candidate))
    bundle = work / "bundle"
    previous = None
    if bundle.exists():
        history = work / "bundle-history"
        history.mkdir(exist_ok=True)
        previous = history / candidate.name
        bundle.rename(previous)
    try:
        candidate.rename(bundle)
    except BaseException:
        if previous is not None:
            previous.rename(bundle)
        raise


def main():
    repository = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--storage-source", type=Path, default=repository / "milvus-storage")
    parser.add_argument("--storage-revision", help="Base revision for an exported, non-Git storage working tree")
    parser.add_argument("--storage-patch", type=Path, help="Git diff for an exported storage working tree")
    parser.add_argument("--knowhere-source", type=Path,
                        help="Local Git object source whose HEAD matches the root knowhere submodule")
    parser.add_argument("--cardinal-repository", help="Authorized Cardinal Git repository or local checkout")
    parser.add_argument("--with-cardinal", action="store_true")
    parser.add_argument("--jobs", type=int, default=8)
    parser.add_argument("--cargo-cache", type=Path, help="Optional existing Cargo target cache, copied before compiling")
    parser.add_argument("--corrosion-source", type=Path, help="Optional local Corrosion v0.5 source cache")
    parser.add_argument("--conan-lock", type=Path, help="Reuse a complete, reviewed Conan dependency lock")
    parser.add_argument("--dependencies-only", action="store_true")
    parser.add_argument("--no-remote", action="store_true", help="Resolve Conan recipes/binaries from cache; source downloads remain enabled")
    args = parser.parse_args()
    if not 1 <= args.jobs <= 50:
        parser.error("--jobs must be between 1 and 50")
    try:
        target = platforms.host_platform()
    except ValueError as error:
        parser.error(str(error))
    profile = repository / "native-build/profiles" / target
    if not profile.is_file():
        parser.error("No build profile for " + target + "; add native-build/profiles/" + target)
    if not platforms.host().available():
        parser.error("This platform's binary tools are missing: "
                     + ", ".join(platforms.host().tools))
    java_home = Path(os.environ.get("JAVA_HOME", ""))
    if not (java_home / "bin/javac").is_file():
        parser.error("Set JAVA_HOME to the selected JDK")
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    with (work / ".build.lock").open("a") as build_lock:
        try:
            fcntl.flock(build_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("Another native build is already using this work directory")
        build(args, repository, java_home, work, target, profile)


def build(args, repository, java_home, work, target, profile):
    provenance = work / "provenance"
    provenance.mkdir(exist_ok=True)
    sources = work / "sources"
    sources.mkdir(exist_ok=True)
    env = os.environ.copy()
    env.update(**platforms.host().toolchain(),
               CMAKE_BUILD_PARALLEL_LEVEL=str(args.jobs), MAKEFLAGS="-j" + str(args.jobs),
               CARGO_BUILD_JOBS=str(args.jobs), OMP_NUM_THREADS=str(args.jobs),
               OPENBLAS_NUM_THREADS=str(args.jobs))
    env["PATH"] = str(java_home / "bin") + os.pathsep + env["PATH"]
    status = {"started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    def phase(name):
        status["phase"] = name
        status["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        (work / "status.json").write_text(json.dumps(status, indent=2) + "\n")
        print("PHASE " + name, flush=True)

    try:
        phase("snapshot-sources")
        constraints = json.loads((repository / "native-build/dependencies.json").read_text())
        knowhere_pin, knowhere_object_source = knowhere_source(repository, args.knowhere_source)
        storage_pin = storage_revision(repository, args.storage_source, args.storage_revision)
        storage = sources / "milvus-storage"
        source_files = tree_hashes(args.storage_source)
        source_identity = {"revision": storage_pin, "files": source_files}
        identity_file = provenance / "storage-source-identity.json"
        if storage.exists():
            if not identity_file.exists() or json.loads(identity_file.read_text()) != source_identity:
                raise RuntimeError("Storage source changed or the snapshot is unverified; use a new work directory")
            if tree_hashes(storage) != source_files:
                raise RuntimeError("Storage snapshot was modified; use a new work directory")
        if not storage.exists():
            shutil.copytree(args.storage_source, storage,
                            ignore=shutil.ignore_patterns(".git", "target", "build", "__pycache__", "*.so", "*.so.*"))
            (provenance / "storage-source-files.json").write_text(json.dumps(tree_hashes(storage), indent=2) + "\n")
            identity_file.write_text(json.dumps(source_identity, indent=2) + "\n")
            if (args.storage_source / ".git").exists():
                run(["git", "-C", args.storage_source, "diff", "--binary", "HEAD"],
                    output=provenance / "storage-working-tree.patch")
            elif args.storage_patch:
                shutil.copy2(args.storage_patch, provenance / "storage-working-tree.patch")
        knowhere = sources / "knowhere"
        knowhere_identity = provenance / "knowhere-source-files.json"
        recorded_knowhere = json.loads(knowhere_identity.read_text()) if knowhere_identity.exists() else None
        clone(knowhere_object_source, knowhere_pin, knowhere, recorded_knowhere)
        if args.with_cardinal:
            for generation, cardinal in constraints["cardinal"].items():
                local_source = knowhere_object_source / "thirdparty" / ("cardinal" + generation)
                origin = args.cardinal_repository or (
                    str(local_source) if (local_source / ".git").exists() else "https://github.com/zilliztech/cardinal.git")
                clone(origin, cardinal["revision"], knowhere / "thirdparty" / ("cardinal" + generation))
        knowhere_files = tree_hashes(knowhere)
        if knowhere_identity.exists() and json.loads(knowhere_identity.read_text()) != knowhere_files:
            raise RuntimeError("Knowhere/Cardinal source snapshot changed; use a new work directory")
        knowhere_identity.write_text(json.dumps(knowhere_files, indent=2) + "\n")
        knowhere_source_identity = provenance / "knowhere-source-identity.json"
        knowhere_identity_record = {"revision": knowhere_pin, "files": knowhere_files}
        if (knowhere_source_identity.exists()
                and json.loads(knowhere_source_identity.read_text()) != knowhere_identity_record):
            raise RuntimeError("Knowhere source changed or the snapshot is unverified; use a new work directory")
        knowhere_source_identity.write_text(json.dumps(knowhere_identity_record, indent=2) + "\n")
        metadata = {"format.version": "1", "platform": target, "dependency.mode": "shared",
                    "build.system": "independent-cmake",
                    "storage.revision": storage_pin, "knowhere.revision": knowhere_pin,
                    "with_cardinal": args.with_cardinal, "with_diskann": True,
                    "storageSourceManifestSha256": digest(provenance / "storage-source-files.json"),
                    "knowhereSourceManifestSha256": digest(knowhere_identity),
                    "knowhereSourceIdentitySha256": digest(knowhere_source_identity),
                    "sourceDirectories": {"storage": str(storage), "knowhere": str(knowhere)},
                    "sourceObjectDirectories": {"storage": str(args.storage_source.resolve()),
                                                "knowhere": str(knowhere_object_source)},
                    "cardinal": constraints["cardinal"] if args.with_cardinal else {},
                    "featureOptions": {"storage.jemalloc": False, "storage.fiu": False,
                                       "storage.crt": False, "storage.talon": False,
                                       "storage.rust.openssl.shared": True}, "jobs": args.jobs}
        adapter = platforms.host()
        for name, command in (*adapter.toolchain_versions(), ("cmake", ["cmake", "--version"]),
                              ("conan", ["conan", "--version"]), ("java", [str(java_home / "bin/java"), "-version"]),
                              ("rustc", ["rustc", "--version", "--verbose"]), ("cargo", ["cargo", "--version"]),
                              ("cpu", adapter.cpu_report())):
            with (provenance / (name + ".txt")).open("w") as log:
                subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True, env=env)
        metadata["cpu.portability"] = ("Host-native upstream compiler flags; "
                                      + target + " is not an ISA baseline")
        phase("validate-upstream-dependencies")
        build_source_files = tree_hashes(repository / "native-build")
        build_source_manifest = provenance / "build-source-files.json"
        if build_source_manifest.exists() and json.loads(build_source_manifest.read_text()) != build_source_files:
            raise RuntimeError("Native build implementation changed; use a new work directory")
        build_source_manifest.write_text(json.dumps(build_source_files, indent=2) + "\n")
        build_inputs = provenance / "build-input"
        if not build_inputs.exists():
            shutil.copytree(repository / "native-build", build_inputs / "native-build",
                            ignore=shutil.ignore_patterns("__pycache__"))
            shutil.copy2(repository / "scripts/build-native.sh", build_inputs / "build-native.sh")
        from dependency_versions import validate_upstream_versions
        storage_recipe = (git_value(args.storage_source, "show", storage_pin + ":cpp/conanfile.py")
                          if (args.storage_source / ".git").exists()
                          else (storage / "cpp/conanfile.py").read_text())
        knowhere_recipe = git_value(knowhere, "show", knowhere_pin + ":conanfile.py")
        version_selection = validate_upstream_versions(constraints, storage_recipe, knowhere_recipe)
        version_selection["sourceRevisions"] = {"storage": storage_pin, "knowhere": knowhere_pin}
        version_selection["recipeSha256"] = {
            name: hashlib.sha256(source.encode()).hexdigest()
            for name, source in (("storage", storage_recipe), ("knowhere", knowhere_recipe))}
        (provenance / "dependency-version-selection.json").write_text(
            json.dumps(version_selection, indent=2) + "\n")
        references = dict(constraints["references"])
        consumer = work / "dependency-input"
        consumer.mkdir(exist_ok=True)
        shutil.copy2(repository / "native-build/conanfile.py", consumer / "conanfile.py")
        shutil.copy2(repository / "native-build/dependencies.json", consumer / "dependencies.json")
        (provenance / "direct-references.json").write_text(json.dumps(references, indent=2) + "\n")
        platform_tools = platform_tool_requirements(profile)
        shutil.copy2(profile, provenance / "host-profile")
        # Host options must not turn build-only protoc/tool packages into runtime libraries.
        build_profile = work / "build-profile"
        build_profile.write_text(profile.read_text().split("[options]")[0])
        dependencies = work / "dependencies"
        options = ["-pr:h", profile, "-pr:b", build_profile,
                   "-c:h", "tools.build:jobs=" + str(args.jobs), "-c:b", "tools.build:jobs=" + str(args.jobs)]
        inputs = {"references": references, "hostProfile": profile.read_text(),
                  "consumerSha256": digest(consumer / "conanfile.py"),
                  "dependencySelectionSha256": digest(consumer / "dependencies.json"),
                  "jobs": args.jobs}
        input_file = provenance / "dependency-input.json"
        lockfile = provenance / "conan.lock"
        lock_identity = provenance / "conan-lock.sha256"
        prepare_conan_lock(args.conan_lock, lockfile, lock_identity, references, platform_tools)
        if input_file.exists() and json.loads(input_file.read_text()) != inputs:
            raise RuntimeError("Dependency constraints changed; use a new work directory")
        input_file.write_text(json.dumps(inputs, indent=2) + "\n")
        phase("lock-unified-dependencies")
        if not lockfile.exists():
            run(["conan", "lock", "create", consumer, *options,
                 *(["--no-remote"] if args.no_remote else []), "--lockfile-out", lockfile], env=env)
        prepare_conan_lock(None, lockfile, lock_identity, references, platform_tools)
        metadata["conanLockSha256"] = digest(lockfile)
        phase("build-unified-dependencies")
        run(["conan", "install", consumer, "-of", dependencies, *options,
             "--lockfile", lockfile, "--build=missing", "--format=json",
             *(["--no-remote"] if args.no_remote else [])],
            output=provenance / "conan-graph.json", env=env)
        graph = json.loads((provenance / "conan-graph.json").read_text())["graph"]["nodes"]
        validate_locked_graph(graph, validate_conan_lock(lockfile, references, platform_tools))
        non_shared = [node.get("ref") for node in graph.values()
                      if node.get("context") == "host" and "shared" in node.get("options", {})
                      and str(node["options"]["shared"]).lower() != "true"]
        if non_shared:
            raise RuntimeError("Host dependencies are not shared: " + ", ".join(non_shared))
        metadata["dependencyGraphSha256"] = digest(provenance / "conan-graph.json")
        (provenance / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        if args.dependencies_only:
            phase("dependencies-complete")
            return
        # Capture Conan's build environment without evaluating shell output as code in Python.
        capture = subprocess.check_output(["bash", "-c",
                                           'source "$1/conanbuild.sh" && source "$1/conanrun.sh" && env -0',
                                           "native-build", str(dependencies)], env=env)
        build_env = dict(part.decode().split("=", 1) for part in capture.split(b"\0") if b"=" in part)
        openssl_packages = [node["package_folder"] for node in graph.values()
                            if node.get("context") == "host" and str(node.get("ref", "")).startswith("openssl/")]
        if len(openssl_packages) != 1:
            raise RuntimeError("The unified graph must select exactly one OpenSSL package")
        # openssl-sys otherwise probes system headers or compiles another vendored copy.
        build_env.update(OPENSSL_DIR=openssl_packages[0], OPENSSL_STATIC="0", OPENSSL_NO_VENDOR="1")
        toolchain = dependencies / "conan_toolchain.cmake"
        native_build = work / "cmake-build"
        corrosion = sources / "corrosion"
        corrosion_files = snapshot_corrosion(constraints["corrosion"], args.corrosion_source, corrosion)
        corrosion_manifest = provenance / "corrosion-source-files.json"
        if corrosion_manifest.exists() and json.loads(corrosion_manifest.read_text()) != corrosion_files:
            raise RuntimeError("Corrosion source changed; use a new work directory")
        corrosion_manifest.write_text(json.dumps(corrosion_files, indent=2) + "\n")
        metadata["corrosion"] = {**constraints["corrosion"], "sourceManifestSha256": digest(corrosion_manifest)}
        install = work / "install"
        common = ["-G", "Ninja", "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_TOOLCHAIN_FILE=" + str(toolchain),
                  "-DCMAKE_C_COMPILER=" + platforms.host().toolchain()["CC"],
                  "-DCMAKE_CXX_COMPILER=" + platforms.host().toolchain()["CXX"],
                  "-DCMAKE_INSTALL_PREFIX=" + str(install),
                  "-DMILVUS_STORAGE_SOURCE_DIR=" + str(storage), "-DKNOWHERE_SOURCE_DIR=" + str(knowhere),
                  "-DWITH_CARDINAL=" + ("ON" if args.with_cardinal else "OFF")]
        if args.cargo_cache and not (native_build / "cargo/build").exists():
            phase("seed-cargo-cache")
            (native_build / "cargo").mkdir(parents=True, exist_ok=True)
            run(["cp", "-a", "--reflink=auto", args.cargo_cache, native_build / "cargo/build"])
        phase("configure-independent-cmake")
        common.append("-DFETCHCONTENT_SOURCE_DIR_CORROSION=" + str(corrosion))
        trace = provenance / "cmake-trace.jsonl"
        run(["cmake", "-S", repository / "native-build", "-B", native_build, *common,
             "--trace-expand", "--trace-format=json-v1", "--trace-redirect=" + str(trace)], env=build_env)
        metadata["cmakeTraceCommands"] = check_cmake_trace(trace, [storage, knowhere])
        metadata["cmakeTraceSha256"] = digest(trace)
        # Cargo owns its own parallel scheduler; finish it before scheduling C++.
        phase("build-storage-rust")
        run(["cmake", "--build", native_build, "--target", "milvus-storage-rust", "--parallel", "1"], env=build_env)
        phase("build-engines-and-jni")
        cpp_env = dict(build_env, CARGO_BUILD_JOBS="1")
        run(["cmake", "--build", native_build, "--parallel", args.jobs], env=cpp_env)
        phase("install-engines-and-jni")
        run(["cmake", "--install", native_build], env=build_env)
        phase("test-knowhere-c-api")
        discovery = subprocess.check_output(
            ["ctest", "--test-dir", str(native_build), "--show-only=json-v1", "-R", "^knowhere_c_api"],
            env=build_env, text=True)
        (provenance / "knowhere-c-api-discovery.json").write_text(discovery)
        test_names = sorted(test["name"] for test in json.loads(discovery)["tests"])
        if test_names != KNOWHERE_C_API_TESTS:
            raise RuntimeError("Unexpected Knowhere C API test set: " + repr(test_names))
        with (provenance / "knowhere-c-api-tests.log").open("w") as log:
            test_result = subprocess.run(
                ["ctest", "--test-dir", str(native_build), "--output-on-failure", "-R", "^knowhere_c_api"],
                env=build_env, stdout=log, stderr=subprocess.STDOUT, check=False)
        metadata["knowhereCApiTestsExit"] = test_result.returncode
        metadata["knowhereCApiTests"] = test_names
        shutil.copy2(native_build / "compile_commands.json", provenance / "compile_commands.json")
        run(["ninja", "-C", native_build, "-t", "commands"], output=provenance / "native-build-commands.txt", env=build_env)
        metadata["compileCommandsSha256"] = digest(provenance / "compile_commands.json")
        metadata["nativeBuildCommandsSha256"] = digest(provenance / "native-build-commands.txt")
        metadata["knowhereCApiDiscoverySha256"] = digest(provenance / "knowhere-c-api-discovery.json")
        metadata["knowhereCApiTestLogSha256"] = digest(provenance / "knowhere-c-api-tests.log")
        if (tree_hashes(storage) != source_files or tree_hashes(knowhere) != knowhere_files
                or tree_hashes(corrosion) != corrosion_files):
            raise RuntimeError("Native build modified its upstream source inputs")
        (provenance / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        phase("stage-unified-libraries")
        if tree_hashes(repository / "native-build") != build_source_files:
            raise RuntimeError("Native build implementation changed while compiling; preserve this build for diagnosis")
        candidates = work / "bundle-candidates"
        candidates.mkdir(exist_ok=True)
        candidate = candidates / (time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8])
        status["candidate"] = str(candidate)
        run([sys.executable, repository / "native-build/stage.py", "--graph", provenance / "conan-graph.json",
             "--storage-build", install / "lib", "--knowhere-build", install / "lib",
             "--output", candidate, "--metadata", provenance / "metadata.json"], env=env)
        if test_result.returncode:
            raise RuntimeError("Knowhere C API tests failed; staged candidate is diagnostic only")
        promote_bundle(candidate, work)
        status["bundle"] = str(work / "bundle")
        phase("complete")
    except BaseException as error:
        status["error"] = str(error)
        status["failed_phase"] = status.get("phase")
        phase("failed")
        raise


if __name__ == "__main__":
    main()
