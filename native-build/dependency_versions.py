"""Validate the dependency versions selected by the top-level Conan recipe."""
import ast
import re


def selected_references(constraints, *, conan_os, with_cardinal):
    """The direct graph selected by the platform and explicit engine feature."""
    if type(with_cardinal) is not bool:
        raise ValueError("with_cardinal must be an explicit boolean")
    features = {"with_cardinal": with_cardinal}
    scope = constraints.get("platform_scope", {})
    feature_scope = constraints.get("feature_scope", {})
    for name, feature in feature_scope.items():
        if name != "_comment" and (name not in constraints["references"] or feature not in features):
            raise ValueError("Unknown dependency feature scope: " + name)
    return {name: reference for name, reference in constraints["references"].items()
            if (not scope.get(name) or conan_os in scope[name])
            and (name not in feature_scope or features[feature_scope[name]])}


def _requirements(source, *, conan_os, options):
    """Read the pinned recipes' literal requirements without executing Conan code.

    Only the condition forms used by these recipes are supported. Parse both
    branches even when disabled, so a new dynamic expression requires review.
    """
    tree = ast.parse(source)
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef)]
    methods = [node for cls in classes for node in cls.body
               if isinstance(node, ast.FunctionDef) and node.name == "requirements"]
    if classes and len(methods) != 1:
        raise ValueError("Expected one upstream requirements method")
    statements = methods[0].body if classes else tree.body
    result = {}

    def value(node):
        if isinstance(node, ast.Constant) and type(node.value) in (bool, str):
            return node.value
        if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Attribute)
                and isinstance(node.value.value, ast.Name) and node.value.value.id == "self"):
            if node.value.attr == "options" and node.attr in options:
                return options[node.attr]
            if node.value.attr == "settings" and node.attr == "os":
                return conan_os
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not condition(node.operand)
        if isinstance(node, ast.Compare) and len(node.ops) == 1:
            left, right = value(node.left), value(node.comparators[0])
            if isinstance(node.ops[0], ast.Eq):
                return left == right
            if isinstance(node.ops[0], ast.NotEq):
                return left != right
        raise ValueError("Unsupported upstream requirement condition: " + ast.dump(node))

    def condition(node):
        resolved = value(node)
        if type(resolved) is not bool:
            raise ValueError("Upstream requirement condition must be boolean")
        return resolved

    def visit(body, active=True):
        for node in body:
            if isinstance(node, ast.If):
                enabled = condition(node.test)
                visit(node.body, active and enabled)
                visit(node.orelse, active and not enabled)
            elif isinstance(node, ast.Pass):
                continue
            elif isinstance(node, ast.Expr):
                call = node.value
                if isinstance(call, ast.Constant) and isinstance(call.value, str):
                    continue
                if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                        and isinstance(call.func.value, ast.Name) and call.func.value.id == "self"
                        and call.func.attr == "requires" and len(call.args) == 1
                        and isinstance(call.args[0], ast.Constant)
                        and isinstance(call.args[0].value, str)
                        and all(keyword.arg in ("force", "override")
                                and isinstance(keyword.value, ast.Constant)
                                and type(keyword.value.value) is bool for keyword in call.keywords)):
                    raise ValueError("Unsupported upstream requirement statement: " + ast.dump(node))
                name, coordinate = call.args[0].value.split("/", 1)
                version = coordinate.split("@", 1)[0].split("#", 1)[0]
                if active:
                    if name in result and result[name] != version:
                        raise ValueError("Multiple upstream versions require explicit feature selection: " + name)
                    result[name] = version
            else:
                raise ValueError("Unsupported upstream requirement statement: " + ast.dump(node))

    visit(statements)
    return result


def validate_upstream_versions(constraints, storage_recipe, knowhere_recipe, *, conan_os, with_cardinal):
    """Require the top-level graph to select the newer pinned engine version."""
    references = selected_references(constraints, conan_os=conan_os, with_cardinal=with_cardinal)
    options = {
        "storage": {"with_crt": False, "with_ut": False, "with_benchmark": False},
        "knowhere": {"with_cardinal": with_cardinal, "with_light": False,
                     "with_ut": False, "with_benchmark": False, "with_faiss_tests": False},
    }
    upstream = {name: _requirements(source, conan_os=conan_os, options=options[name])
                for name, source in (("storage", storage_recipe), ("knowhere", knowhere_recipe))}
    selected = _requirements("\n".join("self.requires(" + repr(reference) + ")"
                                      for reference in references.values()), conan_os=conan_os, options={})
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
    return {"policy": policy["conflicts"], "conflicts": conflicts, "requirements": upstream,
            "settings": {"os": conan_os}, "options": options}
