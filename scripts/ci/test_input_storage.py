"""Exercise fixture credentials, restricted policies, and failure cleanup without Docker."""

import contextlib
import io
import json
import os
from pathlib import Path
import shlex
import tempfile
import unittest
from unittest.mock import patch

import input_storage


class InputStorageTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.env_file = Path(self.directory.name) / "fixture.env"
        self.calls = []
        self.container_exists = False
        image = patch.object(input_storage.minio_image, "ensure_image", return_value="milvus-ci-minio:test-recipe")
        self.ensure_image = image.start()
        self.addCleanup(image.stop)

    def docker(self, *arguments, stdin=None):
        self.calls.append((arguments, stdin))
        if arguments[0] == "run":
            self.container_exists = True
            return "isolated-container"
        if arguments[0] == "ps":
            return "isolated-container" if self.container_exists else ""
        if arguments[0] == "rm":
            self.container_exists = False
            return ""
        if arguments[0] == "port":
            return "127.0.0.1:34567"
        if arguments[0] == "inspect":
            return "true"
        return ""

    def test_creates_private_shell_environment_and_prefix_limited_policies(self):
        with patch.object(input_storage, "docker", self.docker), \
                patch.object(input_storage, "health_check"), \
                patch.dict(os.environ, {}, clear=True), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            input_storage.start(self.env_file)
            values = dict(shlex.split(line)[1].split("=", 1)
                          for line in self.env_file.read_text().splitlines())
            self.assertEqual(self.env_file.stat().st_mode & 0o777, 0o600)
            self.assertEqual(values["STORAGE_TEST_S3_ENDPOINT"], "http://127.0.0.1:34567")
            self.assertNotEqual(values["STORAGE_TEST_S3_SOURCE_ACCESS_KEY"], values["STORAGE_TEST_S3_TARGET_ACCESS_KEY"])
            self.assertNotIn(values["STORAGE_TEST_S3_SOURCE_SECRET_KEY"], output.getvalue())
            self.ensure_image.assert_called_once_with(self.env_file.parent / "minio-image")
            launch = next(arguments for arguments, _ in self.calls if arguments[0] == "run")
            self.assertIn("milvus-ci-minio:test-recipe", launch)
            policies = [stdin for _, stdin in self.calls if stdin is not None]
            self.assertEqual(len(policies), 2)
            self.assertIn("/source/*", policies[0])
            self.assertNotIn("/target/*", policies[0])
            self.assertIn("/target/*", policies[1])
            for identity, raw_policy in zip(("source", "target"), policies):
                statements = json.loads(raw_policy)["Statement"]
                listing = next(item for item in statements if "StringLike" in item.get("Condition", {}))
                self.assertEqual(listing["Condition"]["StringLike"]["s3:prefix"],
                                 [identity, identity + "/*"])
                self.assertFalse(any("Null" in item.get("Condition", {}) for item in statements))
                objects = next(item for item in statements if "s3:GetObject" in item["Action"])
                self.assertEqual(objects["Resource"], [f"arn:aws:s3:::input-identities/{identity}/*"])
            input_storage.stop(self.env_file)
        self.assertFalse(self.env_file.exists())
        self.assertFalse(self.env_file.with_suffix(".container").exists())
        self.assertIn((("rm", "-f", "-v", "isolated-container"), None), self.calls)

    def test_failure_creating_environment_removes_container_and_state(self):
        with patch.object(input_storage, "docker", self.docker), \
                patch.object(input_storage, "health_check"), \
                patch("builtins.open", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                input_storage.start(self.env_file)
        self.assertFalse(self.env_file.exists())
        self.assertFalse(self.env_file.with_suffix(".container").exists())
        self.assertIn((("rm", "-f", "-v", "isolated-container"), None), self.calls)

    def test_failure_recording_container_state_still_removes_container(self):
        for operation in ("write_text", "chmod"):
            with self.subTest(operation=operation), \
                    patch.object(input_storage, "docker", self.docker), \
                    patch.object(Path, operation, side_effect=PermissionError("state denied")):
                with self.assertRaisesRegex(PermissionError, "state denied"):
                    input_storage.start(self.env_file)
            self.assertFalse(self.container_exists)
            self.assertFalse(self.env_file.exists())
            self.assertFalse(self.env_file.with_suffix(".container").exists())

    def test_stop_is_idempotent_after_auto_removal(self):
        self.env_file.write_text("temporary credentials")
        self.env_file.with_suffix(".container").write_text("isolated-container")
        with patch.object(input_storage, "docker", self.docker):
            input_storage.stop(self.env_file)
            input_storage.stop(self.env_file)
        self.assertFalse(self.env_file.exists())
        self.assertFalse(self.env_file.with_suffix(".container").exists())
        self.assertFalse(any(arguments[0] == "rm" for arguments, _ in self.calls))

    def test_stop_tolerates_auto_removal_racing_explicit_removal(self):
        self.env_file.write_text("temporary credentials")
        self.env_file.with_suffix(".container").write_text("isolated-container")
        self.container_exists = True

        def racing_docker(*arguments, **kwargs):
            if arguments[0] == "rm":
                self.container_exists = False
                raise RuntimeError("already removed")
            return self.docker(*arguments, **kwargs)

        with patch.object(input_storage, "docker", racing_docker):
            input_storage.stop(self.env_file)
        self.assertFalse(self.env_file.exists())
        self.assertFalse(self.env_file.with_suffix(".container").exists())

    def test_failed_removal_keeps_retry_state_but_removes_credentials(self):
        self.env_file.write_text("temporary credentials")
        state = self.env_file.with_suffix(".container")
        state.write_text("isolated-container")
        self.container_exists = True

        def failing_docker(*arguments, **kwargs):
            if arguments[0] == "rm":
                raise RuntimeError("Docker unavailable")
            return self.docker(*arguments, **kwargs)

        with patch.object(input_storage, "docker", failing_docker):
            with self.assertRaisesRegex(RuntimeError, "Docker unavailable"):
                input_storage.stop(self.env_file)
        self.assertFalse(self.env_file.exists())
        self.assertTrue(state.exists())

    def test_cleanup_failure_does_not_replace_original_setup_failure(self):
        def failing_docker(*arguments, **kwargs):
            if arguments[0] == "rm":
                raise RuntimeError("Docker unavailable")
            return self.docker(*arguments, **kwargs)

        with patch.object(input_storage, "docker", failing_docker), \
                patch.object(input_storage, "health_check", side_effect=ValueError("setup failed")), \
                contextlib.redirect_stderr(io.StringIO()) as diagnostics:
            with self.assertRaisesRegex(ValueError, "setup failed") as caught:
                input_storage.start(self.env_file)
        notes = "\n".join(getattr(caught.exception, "__notes__", [])) + diagnostics.getvalue()
        self.assertIn("cleanup failed", notes)
        self.assertFalse(self.env_file.exists())
        self.assertTrue(self.env_file.with_suffix(".container").exists())

    def test_python310_cleanup_failure_preserves_original_exception(self):
        class Python310Error(ValueError):
            add_note = None

        original = Python310Error("setup failed")
        with patch.object(input_storage, "docker", self.docker), \
                patch.object(input_storage, "health_check", side_effect=original), \
                patch.object(input_storage, "stop", side_effect=RuntimeError("secret credential")), \
                contextlib.redirect_stderr(io.StringIO()) as diagnostics:
            with self.assertRaises(Python310Error) as caught:
                input_storage.start(self.env_file)
        self.assertIs(caught.exception, original)
        self.assertIn("cleanup failed", diagnostics.getvalue())
        self.assertNotIn("secret credential", diagnostics.getvalue())

    def test_existing_fixture_is_not_overwritten(self):
        self.env_file.write_text("existing")
        with patch.object(input_storage, "docker") as docker:
            with self.assertRaisesRegex(RuntimeError, "already exists"):
                input_storage.start(self.env_file)
        docker.assert_not_called()
        self.ensure_image.assert_not_called()
        self.assertEqual(self.env_file.read_text(), "existing")

    def test_image_build_failure_does_not_start_a_fixture(self):
        self.ensure_image.side_effect = ValueError("binary checksum mismatch")
        with patch.object(input_storage, "docker") as docker:
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                input_storage.start(self.env_file)
        docker.assert_not_called()
        self.assertFalse(self.env_file.exists())
        self.assertFalse(self.env_file.with_suffix(".container").exists())

    def test_github_masks_credentials_before_exporting_environment(self):
        github_env = Path(self.directory.name) / "github.env"
        output = io.StringIO()
        import builtins
        real_open = builtins.open

        def checked_open(path, *args, **kwargs):
            if Path(path) == github_env:
                self.assertEqual(output.getvalue().count("::add-mask::"), 4)
            return real_open(path, *args, **kwargs)

        with patch.object(input_storage, "docker", self.docker), \
                patch.object(input_storage, "health_check"), \
                patch.dict(os.environ, {"GITHUB_ACTIONS": "true", "GITHUB_ENV": str(github_env)}, clear=True), \
                patch("builtins.open", side_effect=checked_open), \
                contextlib.redirect_stdout(output):
            input_storage.start(self.env_file)
        values = dict(line.split("=", 1) for line in github_env.read_text().splitlines())
        masks = [line.removeprefix("::add-mask::") for line in output.getvalue().splitlines()
                 if line.startswith("::add-mask::")]
        credentials = [value for key, value in values.items()
                       if key.endswith(("_ACCESS_KEY", "_SECRET_KEY"))]
        self.assertCountEqual(masks, credentials)
        self.assertNotIn(values["STORAGE_TEST_S3_ENDPOINT"], masks)

    def test_local_environment_export_does_not_print_secrets(self):
        github_env = Path(self.directory.name) / "local-export.env"
        with patch.object(input_storage, "docker", self.docker), \
                patch.object(input_storage, "health_check"), \
                patch.dict(os.environ, {"GITHUB_ENV": str(github_env)}, clear=True), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            input_storage.start(self.env_file)
        self.assertNotIn("::add-mask::", output.getvalue())
        for line in github_env.read_text().splitlines():
            key, value = line.split("=", 1)
            if key.endswith(("_ACCESS_KEY", "_SECRET_KEY")):
                self.assertNotIn(value, output.getvalue())

    def test_masks_escape_workflow_command_data(self):
        with patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}, clear=True), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            input_storage.mask_workflow_credentials({
                "STORAGE_TEST_S3_SOURCE_SECRET_KEY": "value%\r\n::warning::not-a-command"})
        self.assertEqual(output.getvalue(),
                         "::add-mask::value%25%0D%0A::warning::not-a-command\n")

    def test_workspace_network_binds_only_its_loopback_without_port_publication(self):
        with patch.object(input_storage, "docker", self.docker), \
                patch.object(input_storage, "health_check") as health, \
                patch.object(input_storage, "unused_port", return_value=23456), \
                patch.dict(os.environ, {}, clear=True):
            input_storage.start(self.env_file, "workspace-container")
            health.assert_called_once_with("http://127.0.0.1:23456")
        launch = next(arguments for arguments, _ in self.calls if arguments[0] == "run")
        self.assertIn("container:workspace-container", launch)
        self.assertIn("127.0.0.1:23456", launch)
        self.assertNotIn("--publish", launch)
        self.assertIn("http://127.0.0.1:23456", self.env_file.read_text())

    def test_unreachable_endpoint_is_cleaned_before_credentials_are_delivered(self):
        with patch.object(input_storage, "docker", self.docker), \
                patch.object(input_storage, "health_check", side_effect=RuntimeError("unreachable")):
            with self.assertRaisesRegex(RuntimeError, "unreachable"):
                input_storage.start(self.env_file)
        self.assertFalse(self.env_file.exists())
        self.assertFalse(self.env_file.with_suffix(".container").exists())
        self.assertIn((("rm", "-f", "-v", "isolated-container"), None), self.calls)


if __name__ == "__main__":
    unittest.main()
