#!/usr/bin/env python3
"""Check immutable public read samples and public reader source boundaries."""
import hashlib
from pathlib import Path, PurePosixPath
import re

ROOT = Path(__file__).resolve().parents[2]
RESOURCE_ROOTS = (
    "core/src/test/resources/read-fixtures",
    "core/src/test/data",
    "native-vector/src/test/resources/read-fixtures",
)
# Every retained public source tree is checked, including unselected Spark shims.
READ_SOURCES = (
    "core/src/main/scala/**/*.scala",
    "core/src/test/scala/**/*.scala",
    "native-vector/src/test/java/com/zilliz/milvus/jni/vector/NativeVectorLibrarySmoke.java",
    "spark/src/main/scala/**/*.scala",
    "spark/src/main/scala-shims/**/*.scala",
    "spark/src/test/scala/**/*.scala",
    "spark/src/test/scala-shims/**/*.scala",
    "integration/src/test/scala/**/*.scala",
)
FORBIDDEN = re.compile(
    r"com\.zilliz\.milvus\.(?:storage\.write|client)\b|"
    r"\b(?:V3SegmentWriter|MilvusStorageWriter|ManifestTransaction|IndexBuilder|"
    r"NativeVectorIndexBuilder|PrimaryKeyStatsBuilder|PrimaryKeyStatsEncoder|"
    r"ArrowWriter|ArrayEncoder|SparseFloatVectorEncoder|WritableObjectStore|"
    r"MilvusSparkSessionExtensions|"
    r"SnapshotSegmentWriter|SegmentIndexObjects|"
    r"NativeWritableObjectStore)\b|"
    r"\.\s*(?:buildIndex|writeSnapshot)\s*\("
)


def check_group(directory):
    manifest = directory / "SHA256SUMS"
    entries = {}
    for line in manifest.read_text().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9._/-]+)", line)
        if not match:
            raise ValueError(f"Malformed checksum entry: {manifest}")
        checksum, name = match.groups()
        path = PurePosixPath(name)
        if path.is_absolute() or any(p in ("", ".", "..") for p in name.split("/")) or name == "SHA256SUMS":
            raise ValueError(f"Unsafe fixture path: {name}")
        if name in entries:
            raise ValueError(f"Duplicate fixture path: {name}")
        target = directory / name
        if not target.is_file() or any(p.is_symlink() for p in (target, *target.parents)):
            raise ValueError(f"Missing file or symlink: {target}")
        if hashlib.sha256(target.read_bytes()).hexdigest() != checksum:
            raise ValueError(f"Fixture SHA256 mismatch: {target}")
        entries[name] = checksum
    actual = {p.relative_to(directory).as_posix() for p in directory.rglob("*") if p.is_file() and p != manifest}
    if not entries or set(entries) != actual:
        raise ValueError(f"Unlisted fixture files: {directory}: {sorted(actual - set(entries))}")
    if "README.md" not in entries:
        raise ValueError(f"Missing fixture provenance: {directory}")
    return len(entries)


def check_source(path):
    # Comments may describe the old producer. Code must not use it.
    source = re.sub(r"/\*.*?\*/|//[^\n]*", "", path.read_text(), flags=re.S)
    match = FORBIDDEN.search(source)
    if match:
        raise ValueError(f"Read test depends on production write/control code: {path}: {match.group()}")


def check_package(path, source_root):
    source = re.sub(r"/\*.*?\*/|//[^\n]*", "", path.read_text(), flags=re.S)
    declarations = re.findall(r"(?m)^package\s+(?!object\b)([A-Za-z_][\w.]*)", source)
    package_object = re.search(r"(?m)^package\s+object\s+([A-Za-z_][\w]*)", source)
    actual = ".".join(declarations + ([package_object.group(1)] if package_object else []))
    expected = ".".join(path.relative_to(source_root).parent.parts)
    if actual != expected:
        raise ValueError(f"Source package does not match its directory: {path}: {actual} != {expected}")


def check_source_layout(root):
    count = 0
    for module in ("native-runtime", "native-storage", "native-vector", "core", "spark", "integration"):
        for scope in ("main", "test"):
            base = root / module / "src" / scope
            source_roots = [base / "scala", base / "java"]
            # Inspect every shim directory even when the current sbt selection
            # does not compile it. Package paths start beneath the shim name.
            source_roots.extend(sorted((base / "scala-shims").glob("*")))
            for source_root in source_roots:
                for path in sorted(source_root.rglob("*")):
                    if path.is_file() and path.suffix in {".scala", ".java"}:
                        check_package(path, source_root)
                        count += 1
    return count


def main():
    groups = files = sources = 0
    for relative in RESOURCE_ROOTS:
        root = ROOT / relative
        manifests = sorted(root.rglob("SHA256SUMS"))
        if not manifests:
            raise ValueError(f"Missing read sample manifests: {root}")
        for manifest in manifests:
            files += check_group(manifest.parent)
            groups += 1
        covered = {p for m in manifests for p in m.parent.rglob("*") if p.is_file()}
        unlisted = [p for p in root.rglob("*") if p.is_file() and p not in covered and p.name != "README.md"]
        if unlisted:
            raise ValueError(f"Files outside checksum groups: {unlisted}")
    for pattern in READ_SOURCES:
        paths = sorted(ROOT.glob(pattern))
        if not paths:
            raise ValueError(f"Missing read test boundary: {pattern}")
        for path in paths:
            check_source(path)
            sources += 1
    packages = check_source_layout(ROOT)
    print(f"PASS {groups} read fixture groups, {files} files, {sources} read source boundaries, {packages} source packages")


if __name__ == "__main__":
    main()
