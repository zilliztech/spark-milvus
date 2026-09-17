#!/usr/bin/env python3
"""Stage and audit the runtime closure of the unified native build."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


SYSTEM = re.compile(r"^(?:ld-linux[^/]*|lib(?:c|m|mvec|pthread|dl|rt|resolv|util|gcc_s|stdc\+\+)\.so(?:\..*)?|libz\.so\.1)$")
COMPILER_RUNTIME = {"libatomic.so.1", "libgomp.so.1", "libgfortran.so.5", "libquadmath.so.0"}
SYSTEM_PACKAGES = {"libaio.so.1"}
PARENT_PROVIDERS = {"libcardinalv1.so": "libknowhere.so", "libcardinalv2.so": "libknowhere.so"}
JVM_LOAD_ENTRIES = ("libmilvus-storage-jni.so", "libknowhere_jni.so")
AUDIT_DLOPEN_ENTRIES = (
    "libmilvus-storage-jni.so",
    "libmilvus-storage.so",
    "libknowhere_jni.so",
    "libknowhere_c.so.1",
    "libknowhere.so",
)
DELIVERY_EVIDENCE = (
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
)


def library_name(name):
    if not re.fullmatch(r"[A-Za-z0-9_+.-]+", name) or name in (".", ".."):
        raise ValueError("Invalid flat native library name: " + name)
    return name


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def clean_environment():
    environment = os.environ.copy()
    for name in ("LD_LIBRARY_PATH", "LD_PRELOAD", "LD_AUDIT", "LD_DEBUG", "LD_BIND_NOW"):
        environment.pop(name, None)
    environment["LC_ALL"] = "C"
    return environment


def command(*arguments):
    return subprocess.check_output(list(map(str, arguments)), text=True, env=clean_environment())


def elf(path):
    with path.open("rb") as source:
        if source.read(4) != b"\x7fELF":
            return None
    output = command("readelf", "-dW", path)
    soname = re.findall(r"\(SONAME\).*\[(.*?)\]", output)
    return {"soname": library_name(soname[0] if soname else path.name),
            "needed": re.findall(r"\(NEEDED\).*\[(.*?)\]", output), "sourceSha256": digest(path)}


def inventory(graph, roots):
    """Resolve only the selected host graph, never another Conan package revision."""
    providers = {}
    packages = []

    def add(path, origin):
        if not path.is_file():
            return
        record = elf(path)
        if record is None:
            return
        record.update(source=str(path.resolve()), origin=origin)
        library_name(path.name)
        for needed in record["needed"]:
            library_name(needed)
        existing = providers.get(record["soname"])
        if existing and existing["sourceSha256"] != record["sourceSha256"]:
            raise ValueError("Conflicting implementations of " + record["soname"])
        if existing:
            existing["aliases"].add(path.name)
        else:
            record["aliases"] = {path.name, record["soname"]}
            providers[record["soname"]] = record

    for node in graph["graph"]["nodes"].values():
        if node.get("context") != "host" or not node.get("package_folder"):
            continue
        package = Path(node["package_folder"])
        packages.append({"reference": node.get("ref"), "packageId": node.get("package_id"),
                         "packageRevision": node.get("prev"), "directory": str(package),
                         "options": node.get("options", {})})
        directories = {package / "lib", package / "lib64"}
        for component in node.get("cpp_info", {}).values():
            if isinstance(component, dict):
                directories.update(Path(directory) if Path(directory).is_absolute() else package / directory
                                   for directory in component.get("libdirs", []))
        for directory in sorted(directories):
            if directory.is_dir():
                if not directory.resolve().is_relative_to(package.resolve()):
                    raise ValueError("Conan library directory escapes its package: " + str(directory))
                for path in sorted(directory.glob("*.so*")):
                    if not path.resolve().is_relative_to(package.resolve()):
                        raise ValueError("Conan library symlink escapes its package: " + str(path))
                    add(path, {"type": "conan", "reference": node.get("ref"), "packageId": node.get("package_id")})
    for path in roots:
        add(path, {"type": "project-build"})
    return providers, packages


def system_provider(name):
    """Collect explicitly approved compiler/system runtime dependencies with provenance."""
    if name in COMPILER_RUNTIME:
        path = Path(command("gcc-12", "-print-file-name=" + name).strip())
    elif name in SYSTEM_PACKAGES:
        matches = [line.split(" => ", 1)[1] for line in command("ldconfig", "-p").splitlines()
                   if line.strip().startswith(name + " ") and "x86-64" in line and " => " in line]
        if len(matches) != 1:
            raise ValueError("Ambiguous or absent system runtime " + name)
        path = Path(matches[0])
    else:
        raise ValueError("Dependency is outside the unified Conan graph: " + name)
    if not path.is_absolute() or not path.is_file():
        raise ValueError("Missing approved compiler/system runtime " + name)
    record = elf(path)
    try:
        package = command("dpkg-query", "-S", path.resolve()).strip()
        package_name = package.splitlines()[0].rsplit(": ", 1)[0]
        package_version = command("dpkg-query", "-W", "-f=${binary:Package} ${Version}", package_name).strip()
        copyright_file = Path("/usr/share/doc") / package_name.split(":", 1)[0] / "copyright"
    except subprocess.CalledProcessError:
        package = "compiler runtime: " + command("gcc-12", "-dumpfullversion").strip()
        package_version = package
        copyright_file = None
    record.update(source=str(path.resolve()), aliases={name, record["soname"]},
                  origin={"type": "system-runtime", "package": package, "packageVersion": package_version,
                          "copyrightFile": str(copyright_file) if copyright_file and copyright_file.is_file() else None})
    return record


def stage(providers, entries, directory):
    selected = {}
    selected_sonames = set()
    pending = list(entries)
    while pending:
        name = pending.pop()
        if SYSTEM.fullmatch(name):
            continue
        record = providers.get(name)
        if record is None:
            matches = [candidate for candidate in providers.values() if name in candidate["aliases"]]
            if len(matches) > 1:
                raise ValueError("Conflicting providers for native alias: " + name)
            record = matches[0] if matches else system_provider(name)
        if record["soname"] in selected_sonames:
            continue
        selected[name] = record
        selected_sonames.add(record["soname"])
        pending.extend(record["needed"])
    directory.mkdir(parents=True)
    for name, record in sorted(selected.items()):
        destination = directory / name
        shutil.copy2(record["source"], destination)
        subprocess.run(["patchelf", "--set-rpath", "$ORIGIN", str(destination)], check=True)
    alias_targets = {}
    for name, record in selected.items():
        for alias in sorted(record["aliases"] - {name}):
            if alias in selected or alias in alias_targets:
                if alias == name or alias_targets.get(alias) == name:
                    continue
                raise ValueError("Conflicting library alias: " + alias)
            (directory / alias).symlink_to(name)
            alias_targets[alias] = name
    return selected, alias_targets


def system_zlib_requirements(directory, names):
    """Record the system ABI used by the JDK before any JNI bundle is extracted."""
    matches = [line.split(" => ", 1)[1] for line in command("ldconfig", "-p").splitlines()
               if line.strip().startswith("libz.so.1 ") and "x86-64" in line and " => " in line]
    if len(matches) != 1:
        raise ValueError("The system must supply exactly one x86-64 libz.so.1")
    provider = Path(matches[0]).resolve()
    available = set(re.findall(r"Name: (ZLIB_\S+)", command("readelf", "-VW", provider)))
    exported = {line.split()[0].split("@", 1)[0] for line in
                command("nm", "-D", "--defined-only", "--format=posix", provider).splitlines()}
    consumers = {}
    consumer_symbols = {}
    for name in names:
        path = directory / name
        imported = {line.split()[0].split("@", 1)[0] for line in
                    command("nm", "-D", "--undefined-only", "--format=posix", path).splitlines()}
        symbols = sorted(imported.intersection(exported))
        if not symbols and "libz.so.1" not in elf(path)["needed"]:
            continue
        requirements = set(re.findall(r"Name: (ZLIB_\S+)", command("readelf", "-VW", path)))
        if not requirements.issubset(available):
            raise ValueError("System zlib lacks symbol versions required by " + name)
        consumers[name] = sorted(requirements)
        consumer_symbols[name] = symbols
    try:
        package = command("dpkg-query", "-W", "-f=${binary:Package} ${Version}", "zlib1g").strip()
    except subprocess.CalledProcessError:
        package = None
    return {"provider": "libz.so.1", "sha256": digest(provider), "package": package,
            "providedVersions": sorted(available), "consumerRequiredVersions": consumers,
            "consumerImportedSymbols": consumer_symbols}


def copy_delivery_evidence(source, destination):
    """Copy only path-independent records needed to audit a delivered bundle."""
    destination.mkdir()
    for name in DELIVERY_EVIDENCE:
        path = source / name
        if not path.exists():
            continue
        if not path.is_file() or path.is_symlink():
            raise ValueError("Invalid delivery evidence: " + str(path))
        shutil.copy2(path, destination / name)


def public_origin(origin):
    kind = origin.get("type")
    fields = {
        "conan": ("type", "reference", "packageId"),
        "conan-provider": ("type", "reference"),
        "project-build": ("type",),
        "system-runtime": ("type", "package", "packageVersion"),
    }.get(kind)
    if fields is None:
        raise ValueError("Unknown native library origin: " + str(kind))
    return {name: origin[name] for name in fields if origin.get(name) is not None}


def public_package(package):
    return {name: package[name] for name in
            ("reference", "packageId", "packageRevision", "options") if package.get(name) is not None}


def public_metadata(metadata):
    result = dict(metadata)
    result.pop("sourceDirectories", None)
    result.pop("sourceObjectDirectories", None)
    return result


def copy_source_licenses(metadata, destination):
    """Carry notices for embedded source dependencies absent from DT_NEEDED."""
    documents = {
        "storage": ["LICENSE"],
        "knowhere": ["LICENSE", "thirdparty/faiss/LICENSE", "thirdparty/faiss/THIRD_PARTY_NOTICES",
                     "thirdparty/hnswlib/LICENSE", "thirdparty/DiskANN/LICENSE",
                     "thirdparty/DiskANN/NOTICE.txt"],
    }
    source_directories = metadata.get("sourceDirectories", {})
    if metadata.get("with_cardinal"):
        for generation in ("v1", "v2"):
            cardinal = "thirdparty/cardinal" + generation
            documents["knowhere"].append(cardinal + "/third_party/smalltopk/LICENSE")
            knowhere = source_directories.get("knowhere")
            if knowhere:
                documents["knowhere"].extend(
                    str(path.relative_to(Path(knowhere)))
                    for path in (Path(knowhere) / cardinal).glob("LICENSE*") if path.is_file())
    for name, required in documents.items():
        source = source_directories.get(name)
        if source is None:
            raise ValueError("Source directory is missing for license collection: " + name)
        for relative in required:
            document = Path(source) / relative
            if not document.is_file():
                raise ValueError("Required source license document is missing: " + str(document))
            target = destination / name / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(document, target)


def audit(directory, names, jvm_entries, dlopen_entries, output):
    output.mkdir()
    jvm_sequence = tuple(jvm_entries)
    jvm_entries = set(jvm_sequence)
    dlopen_sequence = tuple(dlopen_entries)
    required_entries = jvm_entries.union(dlopen_sequence)
    if not required_entries.issubset(names):
        raise ValueError("Native audit is missing load entries: "
                         + ", ".join(sorted(required_entries - set(names))))
    results = {}
    failed = []
    for name in names:
        path = directory / name
        stack = [line.strip() for line in command("readelf", "-lW", path).splitlines() if "GNU_STACK" in line]
        passed = len(stack) == 1 and "RWE" not in stack[0]
        results[name] = {"context": "dependency", "gnuStack": stack, "passed": passed}
        if name in PARENT_PROVIDERS:
            parent = PARENT_PROVIDERS[name]
            result = subprocess.run(["ldd", "-r", str(path)], env=clean_environment(), text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
            (output / (name + ".ldd-r.txt")).write_text(result.stdout)
            definitions = {line.split()[0].split("@", 1)[0] for line in
                           command("nm", "-D", "--defined-only", "--format=posix", directory / parent).splitlines()}
            unresolved = re.findall(r"undefined symbol: (\S+)", result.stdout)
            environment = clean_environment()
            # Cardinal registers with and calls its parent engine; it is not a standalone entry.
            environment["LD_PRELOAD"] = str((directory / parent).resolve())
            context = subprocess.run(["ldd", "-r", str(path)], env=environment, text=True,
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
            (output / (name + ".parent-context.ldd-r.txt")).write_text(context.stdout)
            context_passed = context.returncode == 0 and not any(
                marker in context.stdout for marker in ("undefined symbol:", "not found", "Relink `"))
            parent_only = all(symbol.split("@", 1)[0] in definitions for symbol in unresolved)
            passed = (result.returncode == 0 and len(stack) == 1 and "RWE" not in stack[0]
                      and not any(marker in result.stdout for marker in ("not found", "Relink `"))
                      and parent_only and context_passed)
            results[name].update(context="plugin", standaloneRelocationsPassed=not unresolved,
                                 contextProvider=parent,
                                 parentResolvedSymbols=unresolved if parent_only else [],
                                 contextPassed=context_passed, passed=passed)
        else:
            result = subprocess.run(["ldd", "-r", str(path)], env=clean_environment(), text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
            label = name.replace("/", "__")
            (output / (label + ".ldd-r.txt")).write_text(result.stdout)
            relocation_passed = result.returncode == 0 and not any(
                marker in result.stdout for marker in ("undefined symbol:", "not found", "Relink `"))
            passed = passed and relocation_passed
            context = ("jvm-load-entry" if name in jvm_entries else
                       "native-load-entry" if name in dlopen_sequence else
                       "runtime-module" if name.startswith("ossl-modules/") else "dependency")
            results[name].update(context=context, lddExit=result.returncode,
                                 relocationsPassed=relocation_passed, passed=passed)
        if name in ("libmilvus-storage.so", "libmilvus-storage-jni.so"):
            symbols = command("nm", "-D", "--defined-only", "--format=posix", path)
            private = [line.split()[0] for line in symbols.splitlines()
                       if re.match(r"^(?:LZ4|XXH|ZSTD|ZDICT|SSL_|OPENSSL_|EVP_|aws_lc_)", line)]
            results[name]["exportedPrivateRustSymbols"] = private
            (output / (name + ".private-symbols.txt")).write_text("\n".join(private) + ("\n" if private else ""))
            passed = passed and not private
            results[name]["passed"] = passed
        if not passed:
            failed.append(name)
    dynamic_entries = [name for name in names if name.startswith("ossl-modules/")]
    for name in (*dlopen_sequence, *dynamic_entries):
        result = subprocess.run([sys.executable, "-c", "import ctypes,os,resource,sys; "
                                 "resource.setrlimit(resource.RLIMIT_CORE,(0,0)); "
                                 "ctypes.CDLL(sys.argv[1], mode=os.RTLD_NOW|os.RTLD_LOCAL)", str(directory / name)],
                                env=clean_environment(), text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=120)
        label = name.replace("/", "__")
        (output / (label + ".dlopen.txt")).write_text(result.stdout)
        results[name]["dlopenExit"] = result.returncode
        results[name]["passed"] = results[name]["passed"] and result.returncode == 0
        if result.returncode:
            failed.append(name)
    load_orders = []
    for order in dict.fromkeys((jvm_sequence, tuple(reversed(jvm_sequence)))):
        result = subprocess.run(
            [sys.executable, "-c", "import ctypes,os,resource,sys; "
             "resource.setrlimit(resource.RLIMIT_CORE,(0,0)); "
             "handles=[ctypes.CDLL(path,mode=os.RTLD_NOW|os.RTLD_LOCAL) for path in sys.argv[1:]]",
             *(str(directory / name) for name in order)],
            env=clean_environment(), text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=120)
        label = "--then--".join(name.removeprefix("lib").removesuffix(".so") for name in order)
        (output / ("load-order-" + label + ".dlopen.txt")).write_text(result.stdout)
        load_orders.append({"entries": list(order), "exit": result.returncode})
        if result.returncode:
            for name in order:
                results[name]["passed"] = False
                failed.append(name)
    (output / "load-orders.json").write_text(json.dumps(load_orders, indent=2) + "\n")
    (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    if failed:
        raise ValueError("Strict native audit failed: " + ", ".join(sorted(set(failed))))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("graph", "storage-build", "knowhere-build", "output", "metadata"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Output exists; preserve it and select a fresh staging directory")
    graph = json.loads(args.graph.read_text())
    metadata = json.loads(args.metadata.read_text())
    roots = list(args.storage_build.glob("libmilvus-storage*.so*"))
    roots.extend(args.knowhere_build.rglob("libknowhere*.so*"))
    roots.extend(args.knowhere_build.rglob("libcardinal*.so*"))
    providers, packages = inventory(graph, roots)
    available = {elf(path)["soname"] for path in roots if path.is_file()}
    required = set(AUDIT_DLOPEN_ENTRIES)
    if not required.issubset(available):
        raise ValueError("Native build did not produce all required JNI/engine libraries")
    entries = set(AUDIT_DLOPEN_ENTRIES)
    selected, aliases = stage(providers, entries, args.output / "lib")
    # OpenSSL loads providers by module path, without a DT_NEEDED edge.
    for package in packages:
        if not package["reference"].startswith("openssl/"):
            continue
        for module in Path(package["directory"]).rglob("ossl-modules/*.so"):
            target = args.output / "lib/ossl-modules" / module.name
            target.parent.mkdir(exist_ok=True)
            shutil.copy2(module, target)
            subprocess.run(["patchelf", "--set-rpath", "$ORIGIN:$ORIGIN/..", str(target)], check=True)
            record = elf(module)
            record.update(source=str(module), origin={"type": "conan-provider", "reference": package["reference"]})
            selected["ossl-modules/" + module.name] = record
    licenses = args.output / "licenses"
    licenses.mkdir()
    copy_source_licenses(metadata, licenses)
    provenance = args.output / "provenance"
    # Full graphs, compiler commands, source snapshots and expanded traces stay
    # in the external work directory. The delivered records contain stable
    # identities and hashes, never build-machine paths.
    copy_delivery_evidence(args.metadata.parent, provenance / "build")
    shutil.copy2(Path(__file__).resolve(), provenance / "stage.py")
    metadata = public_metadata(metadata)
    metadata["stagingImplementationSha256"] = digest(Path(__file__).resolve())
    for name, record in selected.items():
        copyright_file = record.get("origin", {}).get("copyrightFile")
        if copyright_file:
            destination = licenses / "system-runtime" / name
            destination.mkdir(parents=True)
            shutil.copy2(copyright_file, destination / "copyright")
    for package in packages:
        source = Path(package["directory"]) / "licenses"
        name = package["reference"].split("/", 1)[0]
        if source.is_dir():
            shutil.copytree(source, licenses / name)
    records = {}
    for name, record in sorted(selected.items()):
        records[name] = {key: record[key] for key in ("sourceSha256", "soname", "needed")}
        records[name]["origin"] = public_origin(record["origin"])
        records[name]["sha256"] = digest(args.output / "lib" / name)
    metadata.update(libraries=records, aliases=aliases,
                    resolvedPackages=[public_package(package) for package in packages],
                    dependencyGraphSha256=digest(args.graph), audit="pending",
                    relocationRoots=sorted(selected), dlopenEntries=list(AUDIT_DLOPEN_ENTRIES),
                    systemDependencies={"libz.so.1": system_zlib_requirements(args.output / "lib", selected)})
    shutil.copy2(args.graph, args.output / "conan-graph.json")
    metadata_path = args.output / "provenance.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    audit(args.output / "lib", sorted(selected), JVM_LOAD_ENTRIES,
          AUDIT_DLOPEN_ENTRIES, args.output / "audit")
    metadata["audit"] = "passed"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
