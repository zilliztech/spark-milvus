"""CPU gate diagnostics preserve native failures and reject changed libraries."""

import hashlib
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock
import zipfile


SPEC = importlib.util.spec_from_file_location("native_cpu", Path(__file__).parents[1] / "check-native-cpu.py")
cpu = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cpu)


class NativeCpuTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def test_qemu_without_avx_emulation_support_is_rejected(self):
        for version in ("qemu-x86_64 version 6.2.0", "qemu-x86_64 version 7.2.0", "unknown"):
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, "QEMU 8 or newer"):
                cpu.require_qemu_version(version)
        cpu.require_qemu_version("qemu-x86_64 version 8.2.2 (Debian)")
        cpu.require_qemu_version("qemu-x86_64 version 10.0.0")

    def test_emulator_does_not_inherit_loader_or_cpu_overrides(self):
        poison = {"LD_LIBRARY_PATH": "/wrong-libraries", "LD_PRELOAD": "/wrong.so",
                  "QEMU_CPU": "max", "QEMU_SET_ENV": "OPENBLAS_CORETYPE=HASWELL",
                  "OPENBLAS_CORETYPE": "HASWELL", "FAISS_SIMD_LEVEL": "generic"}
        with mock.patch.dict(os.environ, poison):
            environment = cpu.clean_environment()
        self.assertFalse(set(poison) & set(environment))
        self.assertEqual(environment["OPENBLAS_NUM_THREADS"], "1")

    def test_native_signal_is_not_converted_to_success_and_log_survives(self):
        log = self.root / "EPYC.log"
        result, text = cpu.execute(
            [sys.executable, "-u", "-c", "import os,signal; print('before trap'); os.kill(os.getpid(),signal.SIGILL)"],
            log, cpu.clean_environment(), 10)
        self.assertEqual(result["signal"], "SIGILL")
        self.assertNotEqual(result["exitCode"], 0)
        self.assertFalse(result["timedOut"])
        self.assertIn("before trap", text)
        self.assertEqual(result["log.sha256"], hashlib.sha256(log.read_bytes()).hexdigest())

    def test_timeout_is_recorded_with_partial_output(self):
        result, text = cpu.execute(
            [sys.executable, "-u", "-c", "import time; print('started'); time.sleep(60)"],
            self.root / "timeout.log", cpu.clean_environment(), 0.2)
        self.assertTrue(result["timedOut"])
        self.assertIsNone(result["exitCode"])
        self.assertIn("started", text)
        self.assertIn("exceeded", text)

    def test_library_changed_since_abi_audit_is_rejected_before_execution(self):
        bundle = self.root / "changed.jar"
        prefix = "native/milvus/1/linux-x86_64/"
        with zipfile.ZipFile(bundle, "w") as archive:
            archive.writestr(prefix + "manifest.properties", "format.version=1\n")
            archive.writestr(prefix + "libknowhere_c.so.1", b"changed")
        audit = {"libraries": {"libknowhere_c.so.1": {"sha256": hashlib.sha256(b"audited").hexdigest()}}}
        with self.assertRaisesRegex(ValueError, "changed after ABI validation"):
            cpu.extract_libraries(bundle, self.root / "lib", audit)


if __name__ == "__main__":
    unittest.main()
