"""Regression tests for the legacy standalone Knowhere build wrapper."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "build-knowhere.sh"


@unittest.skipUnless(
    sys.platform.startswith("linux") and os.uname().machine in ("x86_64", "aarch64", "arm64"),
    "The Knowhere build wrapper supports Linux only",
)
class BuildKnowhereTest(unittest.TestCase):
    def setUp(self):
        self.git_executable = shutil.which("git")
        self.assertIsNotNone(self.git_executable, "Git is required by the build wrapper tests")
        self.temporary = tempfile.TemporaryDirectory(prefix="build-knowhere-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "connector"
        self.root.mkdir()
        self.git(self.root, "init", "--quiet")

        scripts = self.root / "scripts"
        scripts.mkdir()
        shutil.copy2(SCRIPT, scripts / SCRIPT.name)

        self.knowhere = self.root / "knowhere"
        self.knowhere.mkdir()
        self.git(self.knowhere, "init", "--quiet")
        self.git(self.knowhere, "config", "user.name", "Knowhere Build Test")
        self.git(self.knowhere, "config", "user.email", "knowhere-build-test@example.invalid")
        (self.knowhere / "source.cc").write_text("int knowhere = 1;\n", encoding="ascii")
        self.git(self.knowhere, "add", "source.cc")
        self.git(self.knowhere, "commit", "--quiet", "-m", "Initial Knowhere source")
        self.secret_remote = "https://local-token@example.invalid/knowhere.git"
        self.git(self.knowhere, "remote", "add", "origin", self.secret_remote)

        self.reviewed_repository = "https://github.com/example/reviewed-knowhere.git"
        self.write_gitmodules(self.reviewed_repository)
        self.git(self.root, "add", ".gitmodules", "knowhere", "scripts/build-knowhere.sh")

        self.java_home = Path(self.temporary.name) / "java"
        (self.java_home / "bin").mkdir(parents=True)
        (self.java_home / "lib").mkdir()
        (self.java_home / "lib" / "libjsig.so").touch()
        for command in ("java", "javac", "gh", "jq"):
            executable = self.java_home / "bin" / command
            executable.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
            executable.chmod(0o755)

        self.git_calls = Path(self.temporary.name) / "git-calls.log"
        wrappers = Path(self.temporary.name) / "bin"
        wrappers.mkdir()
        git_wrapper = wrappers / "git"
        git_wrapper.write_text(
            "#!/bin/sh\n"
            'printf \'%s\\n\' "$*" >> "$GIT_CALL_LOG"\n'
            f'exec "{self.git_executable}" "$@"\n',
            encoding="ascii",
        )
        git_wrapper.chmod(0o755)
        self.environment = dict(
            os.environ,
            JAVA_HOME=str(self.java_home),
            GIT_CALL_LOG=str(self.git_calls),
            PATH=str(wrappers) + os.pathsep + os.environ["PATH"],
        )

    def git(self, directory, *arguments):
        return subprocess.run(
            [self.git_executable, "-C", str(directory), *arguments],
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip()

    def write_gitmodules(self, repository):
        (self.root / ".gitmodules").write_text(
            "[submodule \"knowhere\"]\n"
            "\tpath = knowhere\n"
            f"\turl = {repository}\n",
            encoding="ascii",
        )

    def invoke(self):
        return subprocess.run(
            [str(self.root / "scripts" / "build-knowhere.sh"), "import-ci"],
            env=self.environment,
            text=True,
            capture_output=True,
        )

    def test_provenance_repository_comes_from_tracked_gitmodules(self):
        result = self.invoke()

        # The fixture revision has no historical CI artifact, so the wrapper
        # deliberately stops after reporting the inputs it selected.
        self.assertNotEqual(0, result.returncode)
        output = result.stdout + result.stderr
        self.assertIn(f"Knowhere repository: {self.reviewed_repository}", output)
        self.assertNotIn(self.secret_remote, output)
        git_calls = self.git_calls.read_text(encoding="ascii")
        self.assertIn("config --file", git_calls)
        self.assertNotIn("remote get-url", git_calls)

    def test_gitmodules_repository_must_be_non_empty_https_without_user_information(self):
        for repository in ("", "git@example.invalid:knowhere.git", "https://token@example.invalid/knowhere.git"):
            with self.subTest(repository=repository):
                self.write_gitmodules(repository)

                result = self.invoke()

                self.assertNotEqual(0, result.returncode)
                self.assertIn(
                    "submodule.knowhere.url must be a non-empty HTTPS URL without user information",
                    result.stdout + result.stderr,
                )


if __name__ == "__main__":
    unittest.main()
