"""Downloaded native bundles are reused only with exact, safe provenance."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from unittest import mock


SCRIPT = Path(__file__).with_name("verify_native_reuse.py")
SPEC = importlib.util.spec_from_file_location("verify_native_reuse", SCRIPT)
reuse = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(reuse)


class NativeReuseVerificationTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.artifact = self.root / "artifact"
        self.artifact.mkdir()
        self.platform = "linux-x86_64"
        self.jar = self.artifact / f"milvus-native-{self.platform}.jar"
        self.jar.write_bytes(b"native bundle")
        self.digest = hashlib.sha256(self.jar.read_bytes()).hexdigest()
        self.sidecar = Path(str(self.jar) + ".properties")
        self.sidecar.write_text(
            f"jar.sha256={self.digest}\nplatform={self.platform}\n", encoding="utf-8"
        )
        (self.artifact / "abi.json").write_text(json.dumps({
            "jar.sha256": self.digest,
            "libraries": {},
            "platform": self.platform,
        }), encoding="utf-8")
        self.inputs = {
            "inputs": {"files": {"Makefile": "a" * 64}, "platform": self.platform},
            "key": "native-bundle-v2-linux-x86_64-example",
            "schemaVersion": 1,
        }
        (self.artifact / "cache-inputs.json").write_text(
            json.dumps(self.inputs, indent=2, sort_keys=True), encoding="utf-8"
        )
        self.expected = self.root / "expected-cache-inputs.json"
        # Deliberately use a different key order and whitespace.
        self.expected.write_text(
            json.dumps({key: self.inputs[key] for key in reversed(self.inputs)}), encoding="utf-8"
        )

    def verify(self):
        return reuse.verify(self.artifact, self.expected, self.platform)

    def test_accepts_exact_semantic_cache_inputs_and_bound_metadata(self):
        self.assertEqual(self.verify(), self.jar)

    def test_cache_inputs_must_match_exactly(self):
        changed = dict(self.inputs)
        changed["secret"] = "do-not-print-this-value"
        (self.artifact / "cache-inputs.json").write_text(json.dumps(changed), encoding="utf-8")
        with self.assertRaisesRegex(reuse.VerificationError, "exactly match") as raised:
            self.verify()
        self.assertNotIn("do-not-print-this-value", str(raised.exception))

    def test_json_types_are_not_coerced_during_comparison(self):
        self.inputs["schemaVersion"] = True
        self.expected.write_text(json.dumps(self.inputs), encoding="utf-8")
        with self.assertRaisesRegex(reuse.VerificationError, "exactly match"):
            self.verify()

    def test_inventory_is_exact(self):
        (self.artifact / "unexpected.txt").write_text("untrusted", encoding="utf-8")
        with self.assertRaisesRegex(reuse.VerificationError, "inventory"):
            self.verify()

    def test_every_artifact_entry_must_be_a_regular_non_symlink_file(self):
        outside = self.root / "outside"
        outside.write_bytes(b"replacement")
        for name in (
            self.jar.name,
            self.sidecar.name,
            "abi.json",
            "cache-inputs.json",
        ):
            with self.subTest(name=name):
                path = self.artifact / name
                original = path.read_bytes()
                path.unlink()
                path.symlink_to(outside)
                with self.assertRaisesRegex(reuse.VerificationError, "non-symlink"):
                    self.verify()
                path.unlink()
                path.write_bytes(original)

    def test_sidecar_requires_unique_exact_keys(self):
        cases = {
            "duplicate": (
                f"jar.sha256={self.digest}\nplatform={self.platform}\n"
                f"platform={self.platform}\n"
            ),
            "extra": (
                f"jar.sha256={self.digest}\nplatform={self.platform}\nsecret=do-not-print\n"
            ),
            "missing": f"jar.sha256={self.digest}\n",
        }
        for name, contents in cases.items():
            with self.subTest(name=name):
                self.sidecar.write_text(contents, encoding="utf-8")
                with self.assertRaises(reuse.VerificationError) as raised:
                    self.verify()
                self.assertNotIn("do-not-print", str(raised.exception))

    def test_sidecar_platform_and_jar_digest_are_bound(self):
        for name, contents in (
            ("platform", f"jar.sha256={self.digest}\nplatform=linux-aarch64\n"),
            ("digest", f"jar.sha256={'0' * 64}\nplatform={self.platform}\n"),
            ("malformed", f"jar.sha256=not-a-digest\nplatform={self.platform}\n"),
        ):
            with self.subTest(name=name):
                self.sidecar.write_text(contents, encoding="utf-8")
                with self.assertRaises(reuse.VerificationError):
                    self.verify()

    def test_abi_report_is_bound_to_the_same_platform_and_jar(self):
        report = self.artifact / "abi.json"
        for field, value in (("platform", "linux-aarch64"), ("jar.sha256", "0" * 64)):
            with self.subTest(field=field):
                current = {"platform": self.platform, "jar.sha256": self.digest}
                current[field] = value
                report.write_text(json.dumps(current), encoding="utf-8")
                with self.assertRaises(reuse.VerificationError):
                    self.verify()

    def test_duplicate_json_keys_are_rejected(self):
        (self.artifact / "cache-inputs.json").write_text(
            '{"schemaVersion":1,"schemaVersion":1}', encoding="utf-8"
        )
        with self.assertRaisesRegex(reuse.VerificationError, "not valid JSON"):
            self.verify()

    def test_json_parser_value_errors_are_wrapped_and_rejected_on_one_line(self):
        self.assert_parser_error_is_wrapped_and_rejected_on_one_line(ValueError)

    def test_json_parser_recursion_errors_are_wrapped_and_rejected_on_one_line(self):
        self.assert_parser_error_is_wrapped_and_rejected_on_one_line(RecursionError)

    def assert_parser_error_is_wrapped_and_rejected_on_one_line(self, error_type):
        detail = "parser detail that must not appear"
        with mock.patch.object(reuse.json, "loads", side_effect=error_type(detail)):
            with self.assertRaises(reuse.VerificationError) as raised:
                reuse.json_object(self.expected, "Expected cache inputs")
        self.assertIsInstance(raised.exception.__cause__, reuse.InvalidJson)
        self.assertIsInstance(raised.exception.__cause__.__cause__, error_type)

        stderr = io.StringIO()
        with mock.patch.object(reuse.json, "loads", side_effect=error_type(detail)):
            with redirect_stderr(stderr):
                result = reuse.main([
                    "--artifact",
                    str(self.artifact),
                    "--expected-cache-inputs",
                    str(self.expected),
                    "--platform",
                    self.platform,
                ])
        self.assertEqual(result, 1)
        self.assertEqual(
            stderr.getvalue().splitlines(),
            ["Native artifact reuse rejected: Expected cache inputs is not valid JSON"],
        )
        self.assertNotIn(detail, stderr.getvalue())

    def test_cli_failure_does_not_expose_artifact_contents(self):
        secret = "credential-that-must-not-appear"
        self.sidecar.write_text(
            f"jar.sha256={self.digest}\nplatform={self.platform}\nsecret={secret}\n",
            encoding="utf-8",
        )
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--artifact",
                str(self.artifact),
                "--expected-cache-inputs",
                str(self.expected),
                "--platform",
                self.platform,
            ],
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("Native artifact reuse rejected", result.stderr)
        self.assertNotIn(secret, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
