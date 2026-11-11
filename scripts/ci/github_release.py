#!/usr/bin/env python3
"""Publish verified release downloads without replacing previously uploaded bytes.

The workflow calls this only after Central consumption and image publication.
Preparation is credential-free; publication creates a draft, verifies every
uploaded asset, and only then makes the release public. Interrupted drafts can
be resumed, but an existing asset with different bytes is always an error.
"""

import argparse
import importlib.util
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

import assembly_image


SPEC = importlib.util.spec_from_file_location("release", Path(__file__).resolve().parents[1] / "release.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
REPOSITORY = "zilliztech/spark-milvus"
PLATFORMS = tuple(assembly_image.PLATFORMS)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def asset_name(version, platform):
    return f"spark-milvus-4.0-bundle-{version}-{platform}.jar"


def asset_names(version):
    return {"release-manifest.json", "SHA256SUMS"} | {
        name for platform in PLATFORMS
        for name in (asset_name(version, platform), f"assembly-manifest-{platform}.json")
    }


def check_version(version, source):
    require(release.VERSION.fullmatch(version) and "central-test" not in version,
            "GitHub releases require a final or alpha/beta/rc 3.x version")
    require(release.SHA.fullmatch(source), "Release source must be an exact commit")


def prepare(candidate, assemblies, output, version, source):
    check_version(version, source)
    manifest = release.verify(candidate)
    require(manifest["version"] == version and manifest["plan"]["sourceSha"] == source,
            "Maven candidate differs from the release tag")
    require(not output.exists(), "Release asset directory already exists")
    inputs = [(candidate / "release-manifest.json", "release-manifest.json")]
    download_manifests = {}
    for platform in PLATFORMS:
        directory = assemblies / ("assembly-" + platform)
        verified = assembly_image.verify(directory, platform, version, source)
        metadata = assembly_image.load_json(directory / assembly_image.MANIFEST, "Assembly manifest")
        natives = [item for item in manifest["artifacts"]
                   if item["kind"] == "native" and item["platform"] == platform
                   and item["extension"] == "jar" and not item["classifier"]]
        require(len(natives) == 1 and natives[0]["sha256"] == metadata["nativeBundleSha256"],
                "Assembly native bundle differs from the Maven candidate: " + platform)
        inputs.extend(((Path(verified["assembly"]), asset_name(version, platform)),
                       (directory / assembly_image.MANIFEST, f"assembly-manifest-{platform}.json")))
        # Both original assemblies have the same basename. Reflect the public
        # platform suffix in the download manifest while retaining its identity.
        download_manifests[f"assembly-manifest-{platform}.json"] = dict(
            metadata, jar=asset_name(version, platform))
    output.mkdir(parents=True)
    for path, name in inputs:
        if name in download_manifests:
            release.write_json(output / name, download_manifests[name])
        else:
            shutil.copyfile(path, output / name)
    (output / "SHA256SUMS").write_text("".join(
        f"{release.digest(output / name)}  {name}\n" for _, name in sorted(inputs, key=lambda item: item[1])),
        encoding="utf-8")
    return verify_assets(output, version, source)


def verify_assets(directory, version, source):
    check_version(version, source)
    paths = {path.name: path for path in directory.iterdir()}
    require(set(paths) == asset_names(version), "Release download inventory differs from the expected assets")
    require(all(path.is_file() and not path.is_symlink() for path in paths.values()),
            "Release downloads must be regular files")
    expected = "".join(f"{release.digest(paths[name])}  {name}\n"
                       for name in sorted(paths) if name != "SHA256SUMS")
    require(paths["SHA256SUMS"].read_text(encoding="utf-8") == expected,
            "Release download checksum mismatch")
    manifest = assembly_image.load_json(paths["release-manifest.json"], "Maven manifest")
    require(manifest["version"] == version and manifest["plan"]["sourceSha"] == source,
            "Release download manifest differs from the tag")
    for platform in PLATFORMS:
        metadata = assembly_image.load_json(paths[f"assembly-manifest-{platform}.json"], "Assembly manifest")
        require(metadata["version"] == version and metadata["sourceSha"] == source
                and metadata["platform"] == platform
                and metadata["jar"] == asset_name(version, platform)
                and metadata["sha256"] == release.digest(paths[asset_name(version, platform)]),
                "Release assembly differs from its manifest: " + platform)
    return paths


class GitHub:
    """Use gh authentication without putting a token in arguments or logs."""

    def api(self, path, method="GET", data=None):
        command = ["gh", "api", "--include", "--method", method,
                   "repos/" + REPOSITORY + "/" + path]
        if data is not None:
            command.extend(("--input", "-"))
        result = subprocess.run(command, input=None if data is None else json.dumps(data),
                                text=True, capture_output=True)
        headers, _, body = result.stdout.partition("\n\n")
        status = re.match(r"HTTP/\S+ (\d+)", headers)
        code = int(status[1]) if status else None
        require(result.returncode == 0 and code is not None and 200 <= code < 300,
                f"GitHub {method} {path} failed (HTTP {code})")
        return json.loads(body) if body.strip() else None

    def assets(self, release_id):
        result, page = [], 1
        while True:
            rows = self.api(f"releases/{release_id}/assets?per_page=100&page={page}")
            result.extend(rows)
            if len(rows) < 100:
                return result
            page += 1

    def upload(self, tag, path):
        subprocess.run(["gh", "release", "upload", tag, str(path), "--repo", REPOSITORY], check=True)

    def digest(self, asset):
        # Download and hash even when the API supplies a digest: verification
        # also proves that the stored asset is downloadable after an upload.
        with tempfile.TemporaryDirectory(prefix="github-release-check-") as temporary:
            path = Path(temporary) / "asset"
            with path.open("wb") as output:
                subprocess.run(["gh", "api", "repos/" + REPOSITORY + "/releases/assets/" + str(asset["id"]),
                                "-H", "Accept: application/octet-stream"], stdout=output, check=True)
            return release.digest(path)


def verify_tag(client, tag, source):
    obj = client.api("git/ref/tags/" + tag)["object"]
    # Annotated (including signed) tags resolve through a tag object.
    for _ in range(5):
        if obj["type"] != "tag":
            break
        obj = client.api("git/tags/" + obj["sha"])["object"]
    require(obj["type"] == "commit" and obj["sha"] == source,
            "Remote release tag differs from the validated commit")


def find_release(client, tag):
    # The by-tag endpoint does not find drafts. The authenticated release list
    # includes them and must be fully paginated before deciding to create one.
    matches, page = [], 1
    while True:
        rows = client.api(f"releases?per_page=100&page={page}")
        matches.extend(row for row in rows if row["tag_name"] == tag)
        if len(rows) < 100:
            break
        page += 1
    require(len(matches) <= 1, "Multiple GitHub releases have the selected tag; leave them unchanged")
    return matches[0] if matches else None


def publish(directory, version, source, notes, client=None):
    paths = verify_assets(directory, version, source)
    body = notes.read_text(encoding="utf-8").replace("{{VERSION}}", version)
    require(body.strip(), "Release notes must not be empty")
    client = client or GitHub()
    tag, prerelease = "v" + version, "-" in version
    verify_tag(client, tag, source)
    expected = {"tag_name": tag, "name": "Spark-Milvus " + version,
                "body": body, "prerelease": prerelease}
    remote = find_release(client, tag)
    if remote is None:
        remote = client.api("releases", "POST", dict(expected, target_commitish=source, draft=True))
    require(all(remote.get(key) == value for key, value in expected.items()),
            "Existing GitHub release metadata differs; leave it unchanged")
    wanted = {name: release.digest(path) for name, path in paths.items()}

    def check_existing(allow_starters=False):
        assets = client.assets(remote["id"])
        names = [item["name"] for item in assets]
        require(len(names) == len(set(names)) and set(names) <= set(paths),
                "Existing GitHub release has unexpected or duplicate assets")
        starters = []
        for item in assets:
            # GitHub can leave this empty placeholder after an upload 502.
            # It has no accepted bytes; only an unfinished draft may remove it.
            if allow_starters and item.get("state") == "starter" and item.get("size") == 0:
                starters.append(item)
                continue
            require(item.get("state") == "uploaded" and client.digest(item) == wanted[item["name"]],
                    "Existing GitHub release asset has different bytes: " + item["name"])
        return set(names) - {item["name"] for item in starters}, starters

    existing, starters = check_existing(allow_starters=remote["draft"])
    require(remote["draft"] or existing == set(paths),
            "Published GitHub release is incomplete; leave it unchanged")
    # Do not remove even an empty placeholder until every existing uploaded
    # asset has been checked against the accepted bytes.
    for item in starters:
        client.api("releases/assets/" + str(item["id"]), "DELETE")
    for name in sorted(set(paths) - existing):
        client.upload(tag, paths[name])
    require(check_existing()[0] == set(paths), "GitHub release upload is incomplete")
    # Recheck the remote tag before making a completed draft visible.
    verify_tag(client, tag, source)
    if remote["draft"]:
        remote = client.api("releases/" + str(remote["id"]), "PATCH", {"draft": False})
    print(remote["html_url"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("prepare", "publish"):
        command = commands.add_parser(name)
        command.add_argument("--version", required=True)
        command.add_argument("--source-sha", required=True)
        command.add_argument("--assets", type=Path, required=True)
        if name == "prepare":
            command.add_argument("--candidate", type=Path, required=True)
            command.add_argument("--assemblies", type=Path, required=True)
        else:
            command.add_argument("--notes", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.candidate, args.assemblies, args.assets, args.version, args.source_sha)
    else:
        publish(args.assets, args.version, args.source_sha, args.notes)


if __name__ == "__main__":
    main()
