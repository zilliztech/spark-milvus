"""Validate the dependency versions selected by the top-level Conan recipe."""
import ast
import re


def _requirements(source):
    result = {}
    for node in ast.walk(ast.parse(source)):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name) and node.func.value.id == "self"
                and node.func.attr == "requires" and node.args
                and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)):
            reference = node.args[0].value
            name, coordinate = reference.split("/", 1)
            version = coordinate.split("@", 1)[0].split("#", 1)[0]
            if name in result and result[name] != version:
                raise ValueError("Multiple upstream versions require explicit feature selection: " + name)
            result[name] = version
    return result


def validate_upstream_versions(constraints, storage_recipe, knowhere_recipe):
    """Require the top-level graph to select the newer pinned engine version."""
    upstream = {"storage": _requirements(storage_recipe), "knowhere": _requirements(knowhere_recipe)}
    selected = _requirements("\n".join("self.requires(" + repr(reference) + ")"
                                      for reference in constraints["references"].values()))
    policy = constraints["version_policy"]
    excluded = policy["excluded_requirements"]
    conflicts = {}
    for name in sorted(set().union(*(set(values) for values in upstream.values())) - set(excluded)):
        candidates = {engine: values[name] for engine, values in upstream.items() if name in values}
        if name not in selected:
            raise ValueError("Upstream dependency is missing from the unified graph: " + name)
        versions = set(candidates.values()) | {selected[name]}
        if len(versions) > 1:
            ordering = policy.get("ordered_versions", {}).get(name)
            if ordering:
                if not versions.issubset(ordering):
                    raise ValueError("Update the reviewed commit version ordering for " + name)
                key = ordering.index
            else:
                if any(not re.fullmatch(r"\d+(?:\.\d+)*", version) for version in versions):
                    raise ValueError("Cannot infer upstream release ordering for " + name)
                key = lambda version: tuple(map(int, version.split(".")))
            newest = max(candidates.values(), key=key)
            if selected[name] != newest:
                raise ValueError("Select the newer upstream version for " + name + ": " + newest)
            conflicts[name] = {**candidates, "selected": selected[name]}
    return {"policy": policy["conflicts"], "conflicts": conflicts, "requirements": upstream}
