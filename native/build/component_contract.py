"""Merge external component inputs without transferring ownership of Storage.

This is a build-data contract, not a sandbox for component CMake code. Shared
recipe versions still go through upstream version selection; source, licenses,
platform policy and existing dependency options remain driver-owned.
"""
import copy
import fnmatch
from pathlib import Path
import re


def merge(constraints, extension, profile):
    result = copy.deepcopy(constraints)
    components = extension.get("components", [])
    names = [item["name"] for item in components]
    if (len(set(names)) != len(names) or "storage" in names
            or any(not re.fullmatch(r"[a-z][a-z0-9-]*", name) for name in names)):
        raise ValueError("External components must have distinct names other than storage")
    protected = {**constraints["references"], **constraints.get("transitive_references", {})}
    for source, destination in (("references", "references"), ("transitiveReferences", "transitive_references")):
        for name, reference in extension.get(source, {}).items():
            if not re.fullmatch(re.escape(name) + r"/[^#\s]+#[a-f0-9]{32}", reference):
                raise ValueError("External recipe needs an exact revision: " + name)
            previous = protected.get(name)
            version = lambda value: value.split("/", 1)[1].split("@", 1)[0].split("#", 1)[0]
            if previous and version(previous) == version(reference) and previous != reference:
                raise ValueError("External component cannot replace a reviewed recipe revision: " + name)
            # A shared newer version is legal only if version selection proves
            # that one of the pinned upstream component recipes requires it.
            result.setdefault(destination, {})[name] = reference
            if previous and previous != reference:
                other = "transitive_references" if destination == "references" else "references"
                result.get(other, {}).pop(name, None)
    for source, destination in (("platformScope", "platform_scope"), ("staticOn", "static_on")):
        for name, value in extension.get(source, {}).items():
            if name in protected and result[destination].get(name) != value:
                raise ValueError("External component cannot replace Storage platform policy: " + name)
            result[destination][name] = value
    for name, value in extension.get("versionPolicy", {}).items():
        current = result["version_policy"].get(name)
        if isinstance(value, dict):
            for package, rule in value.items():
                if package in (current or {}) and current[package] != rule:
                    raise ValueError("External component cannot replace reviewed version policy: " + package)
                if name == "excluded_requirements" and package in protected:
                    raise ValueError("External component cannot exclude a Storage dependency: " + package)
            result["version_policy"].setdefault(name, {}).update(value)
        elif current != value:
            raise ValueError("External component cannot replace dependency conflict policy")
    # Existing profile settings cover transitive packages as well as direct refs.
    profile_patterns = re.findall(r"^([^#\s:]+/[^:]*):[^=]+=", profile, re.MULTILINE)
    for field in ("conanOptions", "conanConf"):
        for key in extension.get(field, {}):
            match = re.fullmatch(r"([a-zA-Z0-9_-]+)/\*:([a-zA-Z0-9_.:]+)", key)
            if not match:
                raise ValueError("External dependency settings require an exact package owner: " + key)
            package = match[1]
            if package in protected or any(fnmatch.fnmatch(package + "/1", pattern) for pattern in profile_patterns):
                raise ValueError("External component cannot override Storage dependency settings: " + key)
    variables = set()
    for item in components:
        name = item["name"]
        source = Path(item["sourceDirectory"]).resolve()
        for key in item.get("cmakeVariables", {}):
            if (not re.fullmatch(r"[A-Z][A-Z0-9_]*", key)
                    or key.startswith(("CMAKE_", "MILVUS_", "STORAGE_", "FETCHCONTENT_"))
                    or key in variables):
                raise ValueError("External component cannot override driver CMake settings: " + key)
            variables.add(key)
        licenses = item.get("sourceLicenseDirectories", ["LICENSE"])
        if isinstance(licenses, dict):
            if not licenses:
                raise ValueError("External component must retain source licenses: " + name)
            records = []
            for owner, record in licenses.items():
                if owner != name and not owner.startswith(name + "-"):
                    raise ValueError("External component cannot replace another component's licenses: " + owner)
                directory = Path(record["directory"]).resolve()
                if not directory.is_relative_to(source):
                    raise ValueError("External source license directory escapes its component: " + owner)
                records.append(record["files"])
        else:
            records = [licenses]
        for documents in records:
            if (not isinstance(documents, list) or not documents
                    or any(not isinstance(path, str) or not path or Path(path).is_absolute()
                           or ".." in Path(path).parts for path in documents)):
                raise ValueError("External component must retain relative source licenses: " + name)
        for entry in (*item.get("loadEntries", []), *item.get("dlopenEntries", []),
                      *item.get("parentProviders", {})):
            if "milvus-storage" in entry:
                raise ValueError("External component cannot replace Storage load entries")
    if any(key == "storage" or key.startswith("storage.") for key in extension.get("featureOptions", {})):
        raise ValueError("External component cannot declare Storage features")
    return result
