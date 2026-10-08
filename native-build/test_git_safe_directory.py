"""Exercise the Docker build's local submodule fetch across Git ownership checks."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


class DockerGitTrustTest(unittest.TestCase):
    def test_local_fetch_trusts_the_submodule_git_directory_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            environment = {
                **os.environ,
                "GIT_CONFIG_GLOBAL": str(root / "gitconfig"),
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_TERMINAL_PROMPT": "0",
                "LC_ALL": "C",
            }

            def git(*arguments, check=True):
                return subprocess.run(
                    ["git", *map(str, arguments)], env=environment, check=check,
                    capture_output=True, text=True,
                )

            source = root / "source"
            workspace = root / "workspace"
            destination = workspace / "target/native-build/sources/knowhere"
            git("init", "-q", source)
            (source / "fixture.txt").write_text("native source fixture\n")
            git("-C", source, "add", "fixture.txt")
            git("-C", source, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.com",
                "-c", "commit.gpgsign=false", "commit", "-qm", "fixture")
            revision = git("-C", source, "rev-parse", "HEAD").stdout.strip()
            git("init", "-q", workspace)
            git("-C", workspace, "-c", "protocol.file.allow=always", "submodule", "add", "-q",
                source, "knowhere")
            git("init", "-q", destination)

            # Simulate copied Jenkins-owned source directories without needing root/chown.
            environment["GIT_TEST_ASSUME_DIFFERENT_OWNER"] = "1"
            # The native build creates this destination as the current user; exempt it
            # from the simulation, which otherwise treats every repository as foreign.
            for path in (workspace, workspace / "knowhere", destination):
                git("config", "--global", "--add", "safe.directory", path)
            # Recent upload-pack versions accept any owner. Check the resolved Git
            # directory explicitly to exercise the ownership check Jenkins enforces.
            ownership_check = ("-C", workspace / ".git/modules/knowhere", "rev-parse", "--git-dir")
            rejected = git(*ownership_check, check=False)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("dubious ownership", rejected.stderr)
            self.assertIn(str(workspace / ".git/modules/knowhere"), rejected.stderr)

            dockerfile = (Path(__file__).resolve().parents[1] / "Dockerfile").read_text()
            trusted_paths = re.findall(r"git config --global --add safe\.directory (\S+)", dockerfile)
            self.assertTrue(trusted_paths)
            for path in trusted_paths:
                # Apply exactly the Dockerfile's trust entries to the temporary build context.
                git("config", "--global", "--add", "safe.directory", path.replace("/workspace", str(workspace), 1))
            git(*ownership_check)
            git("-C", destination, "fetch", "--depth=1", workspace / "knowhere", revision)
            self.assertEqual(git("-C", destination, "rev-parse", "FETCH_HEAD").stdout.strip(), revision)

            # The fix must not disable ownership checks for arbitrary repositories.
            unrelated = git("-C", source, "rev-parse", "HEAD", check=False)
            self.assertNotEqual(unrelated.returncode, 0)
            self.assertIn("dubious ownership", unrelated.stderr)


if __name__ == "__main__":
    unittest.main()
