"""Exercise Docker's actual Storage gitlink reads across mount ownership.

The public build snapshots Storage files rather than fetching its object store.
Its pin validation still reads both the superproject and initialized submodule.
Trust is limited to Dockerfile's explicit workspace paths, never all repositories.
"""
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class DockerGitOwnershipTest(unittest.TestCase):
    def test_storage_pin_reads_work_but_unrelated_repository_stays_untrusted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "workspace"
            storage = root / "storage-origin"
            unrelated = root / "unrelated"
            env = {**os.environ, "GIT_CONFIG_GLOBAL": str(root / "gitconfig"), "GIT_CONFIG_NOSYSTEM": "1"}
            env.pop("GIT_TEST_ASSUME_DIFFERENT_OWNER", None)

            def git(*args, check=True, environment=env):
                return subprocess.run(["git", *map(str, args)], env=environment, text=True,
                                      capture_output=True, check=check)

            for repository in (storage, workspace, unrelated):
                git("init", "-q", repository)
                (repository / "source.txt").write_text("fixture\n")
                git("-C", repository, "add", ".")
                git("-C", repository, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                    "commit", "-qm", "fixture")
            git("-C", workspace, "-c", "protocol.file.allow=always", "submodule", "add", "-q", storage, "milvus-storage")
            pin = git("-C", storage, "rev-parse", "HEAD").stdout.strip()
            mounted = {**env, "GIT_TEST_ASSUME_DIFFERENT_OWNER": "1"}
            probe = ("import sys; from pathlib import Path; sys.path.insert(0, sys.argv[1]); "
                     "from build import storage_revision; p=Path(sys.argv[2]); "
                     "print(storage_revision(p,p/'milvus-storage',None))")
            command = [sys.executable, "-c", probe, str(ROOT / "native/build"), str(workspace)]
            before = subprocess.run(command, env=mounted, text=True, capture_output=True)
            self.assertNotEqual(0, before.returncode)
            self.assertIn("dubious ownership", before.stderr)
            trusted = re.findall(r"safe\.directory\s+(/workspace(?:/[\w.-]+)*)", (ROOT / "Dockerfile").read_text())
            self.assertIn("/workspace", trusted)
            self.assertIn("/workspace/milvus-storage", trusted)
            self.assertNotIn("*", trusted)
            for entry in trusted:
                git("config", "--global", "--add", "safe.directory", str(root / entry.lstrip("/")))
            after = subprocess.run(command, env=mounted, text=True, capture_output=True)
            self.assertEqual(0, after.returncode, after.stderr)
            self.assertEqual(pin, after.stdout.strip())
            rejected = git("-C", unrelated, "rev-parse", "HEAD", check=False, environment=mounted)
            self.assertNotEqual(0, rejected.returncode)
            self.assertIn("dubious ownership", rejected.stderr)


if __name__ == "__main__":
    unittest.main()
