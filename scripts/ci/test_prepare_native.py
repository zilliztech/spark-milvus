"""Exercise runner cleanup guards without invoking sudo or package installs."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class NativePreparationTest(unittest.TestCase):
    def test_low_disk_stops_before_install_and_cleanup_is_hosted_only(self):
        for actions, runner, cleans in (
            ("true", "github-hosted", True),
            ("true", "self-hosted", False),
            ("false", "github-hosted", False),
            ("", "", False),
        ):
            with self.subTest(actions=actions, runner=runner), tempfile.TemporaryDirectory() as directory:
                work = Path(directory)
                commands = {
                    "uname": '#!/bin/sh\ncase "$1" in -s) echo Linux;; -m) echo x86_64;; esac\n',
                    "df": '#!/bin/sh\nprintf "Filesystem 1024-blocks Used Available Capacity Mounted\\nmock 1000 900 100 90%% /\\n"\n',
                    "sudo": '#!/bin/sh\nprintf "%s\\n" "$*" >> "$PREPARE_TEST_LOG"\n',
                }
                for name, text in commands.items():
                    executable = work / name
                    executable.write_text(text)
                    executable.chmod(0o755)
                log = work / "commands.log"
                result = subprocess.run(
                    ["bash", str(ROOT / "scripts/ci/prepare_native.sh"), "linux-x86_64"],
                    env={**os.environ, "PATH": str(work) + os.pathsep + os.environ["PATH"],
                         "RUNNER_TEMP": directory, "GITHUB_ACTIONS": actions,
                         "RUNNER_ENVIRONMENT": runner, "PREPARE_TEST_LOG": str(log)},
                    text=True, capture_output=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("at least 30 GiB free", result.stderr)
                invocations = log.read_text().splitlines() if log.exists() else []
                self.assertEqual(invocations, [
                    "rm -rf -- /usr/local/lib/android /usr/share/dotnet /opt/ghc",
                ] if cleans else [])


if __name__ == "__main__":
    unittest.main()
