#!/usr/bin/env python3
"""Build Storage and explicit external components in one native dependency graph."""
import argparse
import fcntl
import hashlib
import json
import re
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

import jvm_load
import platforms


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def ctest_pattern(names):
    """CTest uses its own regex dialect, which has no non-capturing groups."""
    return "^(" + "|".join(re.escape(name) for name in names) + ")$"


def run(command, cwd=None, output=None, env=None):
    print("+ " + " ".join(map(str, command)), flush=True)
    if output:
        with Path(output).open("w") as destination:
            subprocess.run(list(map(str, command)), cwd=cwd, env=env, stdout=destination, check=True)
    else:
        subprocess.run(list(map(str, command)), cwd=cwd, env=env, check=True)


def git_value(source, *args):
    return subprocess.check_output(["git", "-C", str(source), *args], text=True).strip()


def clone_ref(source, ref, destination):
    """Check out a tag or branch and return the commit it resolved to."""
    if destination.exists():
        if git_value(destination, "status", "--porcelain", "--untracked-files=no"):
            raise RuntimeError("Pinned source has tracked modifications: " + str(destination))
        return git_value(destination, "rev-parse", "HEAD")
    run(["git", "init", "-q", destination])
    run(["git", "-C", destination, "fetch", "--depth=1", str(source), ref])
    run(["git", "-C", destination, "checkout", "-q", "--detach", "FETCH_HEAD"])
    return git_value(destination, "rev-parse", "HEAD")


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


def replace_requires_section(references):
    """Pin the build context to the recipe revisions selected for the host context.

    The consumer's force=True requirements shape the host graph only. Tools such as
    protoc and grpc_cpp_plugin are built in the build context, where their zlib or
    openssl version ranges would resolve to the newest remote revision. Conan records
    the consumer overrides in the lock without a context, so applying that lock would
    rewrite the build-context ranges to the host revisions and then fail because those
    revisions are absent from build_requires. Declaring the same revisions in both
    profiles makes one graph resolve identically with and without the lock.
    """
    lines = ["[replace_requires]"]
    for reference in sorted(references.values()):
        lines.append(f"{reference.split('/', 1)[0]}/*: {reference}")
    return "\n".join(lines) + "\n"


def macos_openmp_redirect(references, generators):
    """Conan host options that answer milvus-common's find_package(OpenMP) with
    the graph's llvm-openmp on macOS.

    milvus-common's CMakeLists presets Homebrew libomp paths for CMake's
    FindOpenMP module and its recipe names Homebrew libomp as a system
    requirement, so a Homebrew bottle built for the runner's own macOS release
    would end up in the bundle. Conan's toolchain prefers config mode, so the
    OpenMP config Conan generates for llvm-openmp (``generators``) answers that
    call instead, and the Homebrew check only reports. Nothing to redirect on a
    platform whose graph has no llvm-openmp.
    """
    if "llvm-openmp" not in references:
        return []
    variables = {"OpenMP_DIR": {"value": str(generators), "cache": True, "type": "PATH"},
                 "CMAKE_FIND_PACKAGE_PREFER_CONFIG": "ON"}
    # Where OpenMP came from decides the binary, so it identifies the package:
    # a cached milvus-common linked against a Homebrew libomp is not this one.
    # Conan evaluates a conf value as a Python literal, so the dict and the list
    # are written as Python, not JSON: JSON's lowercase booleans leave a string.
    return ["-c:h", "milvus-common/*:tools.cmake.cmaketoolchain:extra_variables=" + repr(variables),
            "-c:h", "milvus-common/*:tools.info.package_id:confs="
                    + repr(["tools.cmake.cmaketoolchain:extra_variables"]),
            "-c:h", "milvus-common/*:tools.system.package_manager:mode=report"]


def dependency_linkage_conf(references, hook, *, conan_os):
    """Apply reviewed missing link edges in both Conan contexts.

    The hook path is build-local. Its bytes, rather than that incidental path,
    identify each rebuilt package; the exact recipe guard lives in the hook.
    """
    if conan_os != "Linux":
        return []
    identity = digest(hook)
    result = []
    for package in ("grpc", "opentelemetry-cpp"):
        if package not in references:
            continue
        variables = {"CMAKE_PROJECT_INCLUDE": str(hook.resolve()),
                     "MILVUS_DEPENDENCY_RECIPE": references[package]}
        for context in ("h", "b"):
            for key, value in (
                    ("tools.cmake.cmaketoolchain:extra_variables", variables),
                    ("user.spark_milvus:dependency_linkage_sha256", identity),
                    ("tools.info.package_id:confs", ["user.spark_milvus:dependency_linkage_sha256"])):
                result.extend(("-c:" + context, package + "/*:" + key + "=" + repr(value)))
    return result


def write_recipe_seed(path, references):
    """Constrain first resolution to reviewed recipes without forcing dependency versions.

    A partial Conan lock selects exact recipe revisions for requirements that
    exist in the graph. It does not add dependencies or override incompatible
    upstream requirements, unlike consumer force=True or profile replacements.
    """
    path.write_text(json.dumps({"version": "0.5", "requires": sorted(references.values()),
                                "build_requires": [], "python_requires": [], "config_requires": []},
                               indent=2) + "\n")
    validate_conan_lock(path, {}, transitive_references=references)


def validate_conan_lock(path, references, platform_tools=(), *, transitive_references=None):
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
    reviewed = transitive_references or {}
    missing = set(reviewed.values()) - locked
    if missing:
        raise ValueError("Conan lock does not match reviewed transitive recipes: " + ", ".join(sorted(missing)))
    selected = {**reviewed, **references}
    for section in ("requires", "build_requires"):
        changed = {value.split("%", 1)[0] for value in lock.get(section, [])
                   if value.split("/", 1)[0] in selected
                   and value.split("%", 1)[0] != selected[value.split("/", 1)[0]]}
        if changed:
            raise ValueError("Conan lock contains unreviewed recipes in " + section + ": "
                             + ", ".join(sorted(changed)))
    return lock


def prepare_conan_lock(provided, destination, identity, references, platform_tools=(), *, transitive_references=None):
    """Preserve a supplied lock byte-for-byte and reject changed resume inputs."""
    if provided is not None:
        validate_conan_lock(provided, references, platform_tools, transitive_references=transitive_references)
        if destination.exists() and digest(destination) != digest(provided):
            raise ValueError("Conan lock changed; use a new work directory")
        if not destination.exists():
            shutil.copy2(provided, destination)
    if destination.exists():
        validate_conan_lock(destination, references, platform_tools, transitive_references=transitive_references)
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
    entries = metadata["loadEntries"]
    expected_orders = [list(order) for order in dict.fromkeys((tuple(entries), tuple(reversed(entries))))]
    orders = metadata.get("jvmLoadTests")
    jvm_passed = (isinstance(orders, list) and len(orders) == len(expected_orders)
                  and all(isinstance(record, dict) and record.get("entries") == expected
                          and type(record.get("exit")) is int and record["exit"] == 0
                          for record, expected in zip(orders, expected_orders)))
    if (metadata.get("audit") != "passed" or any(record["exit"] != 0 for record in metadata.get("componentTests", []))
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
    repository = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--storage-source", type=Path, default=repository / "milvus-storage")
    parser.add_argument("--storage-revision", help="Base revision for an exported, non-Git storage working tree")
    parser.add_argument("--storage-patch", type=Path, help="Git diff for an exported storage working tree")
    parser.add_argument("--components", type=Path,
                        help="External component descriptor; Storage is always built by this driver")
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
    profile = repository / "native/build/profiles" / target
    if not profile.is_file():
        parser.error("No build profile for " + target + "; add native/build/profiles/" + target)
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
        constraints = json.loads((repository / "native/build/dependencies.json").read_text())
        extension = json.loads(args.components.read_text()) if args.components else {}
        components = extension.get("components", [])
        component_names = [component["name"] for component in components]
        from component_contract import merge
        constraints = merge(constraints, extension, profile.read_text())
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
        adapter = platforms.host()
        metadata = {"format.version": "2", "platform": target, "dependency.mode": "shared",
                    "build.system": "independent-cmake", "components": ["storage", *component_names],
                    "storage.revision": storage_pin,
                    "storageSourceManifestSha256": digest(provenance / "storage-source-files.json"),
                    "sourceDirectories": {"storage": str(storage)},
                    "sourceObjectDirectories": {"storage": str(args.storage_source.resolve())},
                    "sourceLicenseDocuments": {"storage": ["LICENSE"]},
                    "loadEntries": [adapter.library_name("milvus-storage-jni")],
                    "dlopenEntries": [adapter.library_name("milvus-storage-jni"), adapter.library_name("milvus-storage")],
                    "parentProviders": {}, "componentTests": [],
                    "featureOptions": {"storage.jemalloc": False, "storage.fiu": False,
                                       "storage.crt": False, "storage.talon": True,
                                       "storage.rust.openssl.shared": True}, "jobs": args.jobs}
        overlay = extension.get("metadata", {})
        if set(overlay).intersection(metadata):
            raise ValueError("External metadata cannot override driver-owned identity")
        metadata.update(overlay)
        metadata["features"] = extension.get("featureOptions", {})
        if any(type(value) is not bool for value in metadata["features"].values()):
            raise ValueError("Native component features must be booleans")
        component_files = {}
        for component in components:
            name = component["name"]
            if not re.fullmatch(r"[a-f0-9]{40}", component["revision"]):
                raise ValueError("External component needs an exact revision: " + name)
            source = Path(component["sourceDirectory"]).resolve(strict=True)
            files = tree_hashes(source)
            component_files[name] = files
            for suffix, value in (("files", files), ("identity", {"revision": component["revision"], "files": files})):
                record = provenance / (name + "-source-" + suffix + ".json")
                if record.exists() and json.loads(record.read_text()) != value:
                    raise ValueError("External component source changed: " + name)
                record.write_text(json.dumps(value, indent=2) + "\n")
            metadata[name + ".revision"] = component["revision"]
            metadata[name + "SourceManifestSha256"] = digest(provenance / (name + "-source-files.json"))
            metadata[name + "SourceIdentitySha256"] = digest(provenance / (name + "-source-identity.json"))
            metadata["sourceDirectories"][name] = str(source)
            metadata["sourceObjectDirectories"][name] = component.get("sourceObjectDirectory", str(source))
            licenses = component.get("sourceLicenseDirectories", ["LICENSE"])
            if isinstance(licenses, dict):
                for owner, record in licenses.items():
                    metadata["sourceDirectories"][owner] = record["directory"]
                    metadata["sourceLicenseDocuments"][owner] = record["files"]
            else:
                metadata["sourceLicenseDocuments"][name] = licenses
            extension_files = tree_hashes(Path(component["cmake"]).parent)
            extension_record = provenance / (name + "-extension-files.json")
            if extension_record.exists() and json.loads(extension_record.read_text()) != extension_files:
                raise ValueError("External component build implementation changed: " + name)
            extension_record.write_text(json.dumps(extension_files, indent=2) + "\n")
            metadata[name + "ExtensionManifestSha256"] = digest(extension_record)
            metadata["loadEntries"].extend(component["loadEntries"])
            metadata["dlopenEntries"].extend(component["dlopenEntries"])
            metadata["parentProviders"].update(component.get("parentProviders", {}))
        for field in ("loadEntries", "dlopenEntries"):
            metadata[field] = list(dict.fromkeys(metadata[field]))
        for name, command in (*adapter.toolchain_versions(), ("cmake", ["cmake", "--version"]),
                              ("conan", ["conan", "--version"]), ("java", [str(java_home / "bin/java"), "-version"]),
                              ("rustc", ["rustc", "--version", "--verbose"]), ("cargo", ["cargo", "--version"]),
                              ("cpu", adapter.cpu_report())):
            with (provenance / (name + ".txt")).open("w") as log:
                subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True, env=env)
        metadata["cpu.portability"] = ("CPU requirements depend on dependency targets and compiler flags; "
                                      + target + " alone does not specify an ISA baseline")
        phase("validate-upstream-dependencies")
        build_source_files = tree_hashes(repository / "native/build")
        build_source_manifest = provenance / "build-source-files.json"
        if build_source_manifest.exists() and json.loads(build_source_manifest.read_text()) != build_source_files:
            raise RuntimeError("Native build implementation changed; use a new work directory")
        build_source_manifest.write_text(json.dumps(build_source_files, indent=2) + "\n")
        build_inputs = provenance / "build-input"
        if not build_inputs.exists():
            shutil.copytree(repository / "native/build", build_inputs / "native/build",
                            ignore=shutil.ignore_patterns("__pycache__"))
            shutil.copy2(repository / "scripts/build-native.sh", build_inputs / "build-native.sh")
        from dependency_versions import selected_references, selected_transitive_references, validate_upstream_versions
        storage_recipe = (git_value(args.storage_source, "show", storage_pin + ":cpp/conanfile.py")
                          if (args.storage_source / ".git").exists()
                          else (storage / "cpp/conanfile.py").read_text())
        conan_os = adapter.conan_os
        recipes = {"storage": (storage_recipe, {"with_crt": False, "with_ut": False, "with_benchmark": False})}
        recipes.update({component["name"]: (Path(component["recipe"]).read_text(), component["recipeOptions"])
                        for component in components})
        version_selection = validate_upstream_versions(constraints, recipes, conan_os=conan_os)
        version_selection["sourceRevisions"] = {name: metadata[name + ".revision"] for name in metadata["components"]}
        version_selection["recipeSha256"] = {name: hashlib.sha256(recipe.encode()).hexdigest()
                                              for name, (recipe, _) in recipes.items()}
        (provenance / "dependency-version-selection.json").write_text(
            json.dumps(version_selection, indent=2) + "\n")
        # Validate the lock against the same platform and feature selection as Conan.
        references = selected_references(
            constraints, conan_os=conan_os)
        transitive_references = selected_transitive_references(constraints, conan_os=conan_os)
        consumer = work / "dependency-input"
        consumer.mkdir(exist_ok=True)
        shutil.copy2(repository / "native/build/conanfile.py", consumer / "conanfile.py")
        (consumer / "dependencies.json").write_text(json.dumps(constraints, indent=2) + "\n")
        (provenance / "direct-references.json").write_text(json.dumps(references, indent=2) + "\n")
        platform_tools = platform_tool_requirements(profile)
        replacements = replace_requires_section(references)
        host_profile = work / "host-profile"
        host_profile.write_text(profile.read_text().rstrip("\n") + "\n\n" + replacements)
        shutil.copy2(host_profile, provenance / "host-profile")
        # Host options must not turn build-only protoc/tool packages into runtime libraries.
        build_profile = work / "build-profile"
        build_profile.write_text(profile.read_text().split("[options]")[0].rstrip("\n") + "\n\n" + replacements)
        dependencies = work / "dependencies"
        profiles = ["-pr:h", host_profile, "-pr:b", build_profile,
                    "-c:h", "tools.build:jobs=" + str(args.jobs), "-c:b", "tools.build:jobs=" + str(args.jobs)]
        openmp_generators = work / "openmp-config"
        linkage_hook = consumer / "DependencyLinkage.cmake"
        shutil.copy2(repository / "native/build/cmake/DependencyLinkage.cmake", linkage_hook)
        options = [*profiles, *macos_openmp_redirect(references, openmp_generators),
                   *dependency_linkage_conf(references, linkage_hook, conan_os=conan_os)]
        for key, value in sorted(extension.get("conanOptions", {}).items()):
            options.extend(("-o:h", key + "=" + str(value)))
        for key, value in sorted(extension.get("conanConf", {}).items()):
            options.extend(("-c:h", key + "=" + (json.dumps(value) if not isinstance(value, str) else value)))
        inputs = {"references": references, "transitiveReferences": transitive_references,
                  "hostProfile": host_profile.read_text(),
                  "consumerSha256": digest(consumer / "conanfile.py"),
                  "dependencyLinkageSha256": digest(linkage_hook),
                  "dependencySelectionSha256": digest(consumer / "dependencies.json"),
                  "components": metadata["components"],
                  "componentOptions": extension.get("conanOptions", {}),
                  "componentConf": extension.get("conanConf", {}),
                  "features": metadata["features"],
                  "jobs": args.jobs}
        input_file = provenance / "dependency-input.json"
        lockfile = provenance / "conan.lock"
        lock_identity = provenance / "conan-lock.sha256"
        prepare_conan_lock(args.conan_lock, lockfile, lock_identity, references, platform_tools,
                           transitive_references=transitive_references)
        if input_file.exists() and json.loads(input_file.read_text()) != inputs:
            raise RuntimeError("Dependency constraints changed; use a new work directory")
        input_file.write_text(json.dumps(inputs, indent=2) + "\n")
        phase("lock-unified-dependencies")
        if not lockfile.exists():
            recipe_seed = provenance / "recipe-seed.lock"
            write_recipe_seed(recipe_seed, transitive_references)
            run(["conan", "lock", "create", consumer, *options,
                 "--lockfile", recipe_seed, "--lockfile-partial",
                 *(["--no-remote"] if args.no_remote else []), "--lockfile-out", lockfile], env=env)
        prepare_conan_lock(None, lockfile, lock_identity, references, platform_tools,
                           transitive_references=transitive_references)
        metadata["conanLockSha256"] = digest(lockfile)
        phase("build-unified-dependencies")
        if "llvm-openmp" in references:
            # The OpenMP config milvus-common is pointed at has to exist before
            # Conan configures milvus-common, which does not depend on llvm-openmp.
            run(["conan", "install", "--requires=" + references["llvm-openmp"], "-of", openmp_generators,
                 "-g", "CMakeDeps", *profiles, "--lockfile", lockfile, "--build=missing",
                 *(["--no-remote"] if args.no_remote else [])], env=env)
        run(["conan", "install", consumer, "-of", dependencies, *options,
             "--lockfile", lockfile, "--build=missing", "--format=json",
             *(["--no-remote"] if args.no_remote else [])],
            output=provenance / "conan-graph.json", env=env)
        graph = json.loads((provenance / "conan-graph.json").read_text())["graph"]["nodes"]
        validate_locked_graph(graph, validate_conan_lock(
            lockfile, references, platform_tools, transitive_references=transitive_references))
        # A recipe that cannot build a shared library on this system is static
        # here too; the engines' own conanfiles select the same option, and
        # dependencies.json records which references those are.
        static_on = constraints.get("static_on", {})
        static_allowed = {name for name, systems in static_on.items()
                          if name != "_comment" and conan_os in systems}
        non_shared = [node.get("ref") for node in graph.values()
                      if node.get("context") == "host" and "shared" in node.get("options", {})
                      and str(node["options"]["shared"]).lower() != "true"
                      and str(node.get("ref", "")).split("/", 1)[0] not in static_allowed]
        if non_shared:
            raise RuntimeError("Host dependencies are not shared: " + ", ".join(non_shared))
        metadata["dependency.static"] = sorted(
            ref for ref in (node.get("ref") for node in graph.values())
            if ref and str(ref).split("/", 1)[0] in static_allowed)
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
        # conanrun.sh also exports the loader's primary search path, which comes
        # before a library's recorded location: cargo, started by CMake with it,
        # took Conan's libiconv.2.dylib for the system one and aborted on its
        # first git dependency. Only the fallback variable below may name
        # package directories.
        for variable in platforms.host().library_override_variables:
            build_env.pop(variable, None)
        # Conan's build environment carries PATH but no library search path, so
        # a packaged build tool finds its own dependencies only through the
        # runtime path recorded in it. That holds for ELF and not for Mach-O,
        # where the recorded @rpath points at the package's build location. The
        # fallback variable is used rather than the primary one: it is consulted
        # only after the recorded path fails, so a packaged OpenSSL or zlib
        # cannot shadow the copy cargo, git and the compiler already link.
        loader_variable = platforms.host().library_fallback_variable
        package_library_dirs = []
        for node in graph.values():
            folder = node.get("package_folder")
            if folder and (Path(folder) / "lib").is_dir():
                package_library_dirs.append(str(Path(folder) / "lib"))
        if package_library_dirs:
            existing = build_env.get(loader_variable)
            build_env[loader_variable] = os.pathsep.join(
                dict.fromkeys(package_library_dirs + (existing.split(os.pathsep) if existing else [])))
        openssl_packages = [node["package_folder"] for node in graph.values()
                            if node.get("context") == "host" and str(node.get("ref", "")).startswith("openssl/")]
        if len(openssl_packages) != 1:
            raise RuntimeError("The unified graph must select exactly one OpenSSL package")
        # openssl-sys otherwise probes system headers or compiles another vendored copy.
        build_env.update(OPENSSL_DIR=openssl_packages[0], OPENSSL_STATIC="0", OPENSSL_NO_VENDOR="1")
        # prost's build script runs the packaged protoc; on a platform whose
        # loader variables are stripped from the environment it is reached
        # through a launcher that sets the search path itself.
        protoc = next((Path(node["package_folder"]) / "bin" / "protoc"
                       for node in graph.values()
                       if str(node.get("ref", "")).startswith("protobuf/")
                       and node.get("package_folder")
                       and (Path(node["package_folder"]) / "bin" / "protoc").is_file()), None)
        if protoc and platforms.host().needs_tool_launcher():
            launcher = work / "protoc"
            launcher.write_text(
                "#!/bin/sh\n"
                + loader_variable + "=\"" + build_env[loader_variable] + "\" export "
                + loader_variable + "\n"
                + 'exec "' + str(protoc) + '" "$@"\n')
            launcher.chmod(0o755)
            build_env["PROTOC"] = str(launcher)
        elif protoc:
            build_env["PROTOC"] = str(protoc)

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
                  "-DMILVUS_STORAGE_SOURCE_DIR=" + str(storage),
                  "-DMILVUS_COMPONENT_CMAKE=" + ";".join(component["cmake"] for component in components)]
        for component in components:
            for key, value in component.get("cmakeVariables", {}).items():
                if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
                    raise ValueError("Invalid component CMake variable: " + key)
                common.append("-D" + key + "=" + ("ON" if value is True else "OFF" if value is False else str(value)))
        if args.cargo_cache and not (native_build / "cargo/build").exists():
            phase("seed-cargo-cache")
            (native_build / "cargo").mkdir(parents=True, exist_ok=True)
            # Copied in-process: the cheap clone flag is spelled --reflink=auto
            # by GNU cp and -c by the BSD cp macOS ships, so neither command
            # line runs everywhere.
            shutil.copytree(args.cargo_cache, native_build / "cargo/build", symlinks=True)
        phase("configure-independent-cmake")
        common.append("-DFETCHCONTENT_SOURCE_DIR_CORROSION=" + str(corrosion))
        trace = provenance / "cmake-trace.jsonl"
        run(["cmake", "-S", repository / "native/build", "-B", native_build, *common,
             "--trace-expand", "--trace-format=json-v1", "--trace-redirect=" + str(trace)], env=build_env)
        metadata["cmakeTraceCommands"] = check_cmake_trace(trace, [storage, *(Path(component["sourceDirectory"]) for component in components)])
        metadata["cmakeTraceSha256"] = digest(trace)
        # Cargo owns its own parallel scheduler; finish it before scheduling C++.
        phase("build-storage-rust")
        run(["cmake", "--build", native_build, "--target", "milvus-storage-rust", "--parallel", "1"], env=build_env)
        phase("build-engines-and-jni")
        cpp_env = dict(build_env, CARGO_BUILD_JOBS="1")
        run(["cmake", "--build", native_build, "--parallel", args.jobs], env=cpp_env)
        phase("install-engines-and-jni")
        run(["cmake", "--install", native_build], env=build_env)
        for component in components:
            expected_tests = sorted(component.get("testNames", []))
            if not expected_tests:
                continue
            pattern = ctest_pattern(expected_tests)
            discovery = subprocess.check_output(["ctest", "--test-dir", str(native_build), "--show-only=json-v1", "-R", pattern],
                                                env=build_env, text=True)
            actual_tests = sorted(test["name"] for test in json.loads(discovery)["tests"])
            if actual_tests != expected_tests:
                raise RuntimeError("Incomplete component native tests: " + component["name"])
            test_log = provenance / (component["name"] + "-tests.log")
            with test_log.open("w") as log:
                result = subprocess.run(["ctest", "--test-dir", str(native_build), "--output-on-failure", "-R", pattern],
                                        env=build_env, stdout=log, stderr=subprocess.STDOUT, check=False)
            metadata["componentTests"].append({"component": component["name"], "tests": actual_tests,
                                                "exit": result.returncode, "logSha256": digest(test_log)})
        shutil.copy2(native_build / "compile_commands.json", provenance / "compile_commands.json")
        run(["ninja", "-C", native_build, "-t", "commands"], output=provenance / "native-build-commands.txt", env=build_env)
        metadata["compileCommandsSha256"] = digest(provenance / "compile_commands.json")
        metadata["nativeBuildCommandsSha256"] = digest(provenance / "native-build-commands.txt")
        if (tree_hashes(storage) != source_files or tree_hashes(corrosion) != corrosion_files
                or any(tree_hashes(Path(component["sourceDirectory"])) != component_files[component["name"]]
                       for component in components)):
            raise RuntimeError("Native build modified its upstream source inputs")
        (provenance / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        phase("stage-unified-libraries")
        if tree_hashes(repository / "native/build") != build_source_files:
            raise RuntimeError("Native build implementation changed while compiling; preserve this build for diagnosis")
        candidates = work / "bundle-candidates"
        candidates.mkdir(exist_ok=True)
        candidate = candidates / (time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8])
        status["candidate"] = str(candidate)
        run([sys.executable, repository / "native/build/stage.py", "--graph", provenance / "conan-graph.json",
             "--storage-build", install / "lib",
             "--output", candidate, "--metadata", provenance / "metadata.json"], env=env)
        if any(record["exit"] for record in metadata["componentTests"]):
            raise RuntimeError("Native component tests failed; staged candidate is diagnostic only")
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
