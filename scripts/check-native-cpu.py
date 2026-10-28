#!/usr/bin/env python3
"""Check the released x86 native libraries on three QEMU CPU models."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import zipfile


ROOT = Path(__file__).resolve().parents[1]
PLATFORM = "linux-x86_64"
MODELS = ("Nehalem", "Haswell", "EPYC")
PASS = "PASS native CPU probe: L2 nq=64 nb=4995 dim=32 top_k=10"
SPEC = importlib.util.spec_from_file_location("native_abi", Path(__file__).with_name("check-native-abi.py"))
ABI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ABI)


def clean_environment():
    # In particular, never let a bundle's LD_LIBRARY_PATH affect the emulator
    # itself, or inherit QEMU/BLAS/Faiss CPU overrides from the caller.
    return {"PATH": os.defpath, "LC_ALL": "C", "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}


def disable_core_dump():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def require_qemu_version(version):
    match = re.search(r"\bversion (\d+)\.(\d+)", version)
    if match is None or int(match[1]) < 8:
        raise ValueError("QEMU 8 or newer is required for AVX/AVX2/FMA CPU coverage")


def execute(command, log, environment, timeout):
    started = time.monotonic()
    with log.open("w") as stream:
        stream.write("Command: " + json.dumps([str(value) for value in command]) + "\n")
        stream.flush()
        try:
            process = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT,
                                     env=environment, timeout=timeout, preexec_fn=disable_core_dump)
            code, timed_out = process.returncode, False
        except subprocess.TimeoutExpired:
            code, timed_out = None, True
            stream.write(f"\nCPU probe exceeded {timeout} seconds\n")
    text = log.read_text(errors="replace")
    target_signal = re.search(r"uncaught target signal (\d+)", text)
    number = -code if code is not None and code < 0 else int(target_signal[1]) if target_signal else None
    return {"exitCode": code, "timedOut": timed_out,
            "signal": signal.Signals(number).name if number is not None else None,
            "elapsedSeconds": round(time.monotonic() - started, 3), "log": log.name,
            "log.sha256": ABI.digest(log)}, text


def extract_libraries(bundle, directory, audit):
    """Reuse the ABI audit's validated manifest set, paths and checksums."""
    prefix = f"native/milvus/1/{PLATFORM}/"
    with zipfile.ZipFile(bundle) as archive:
        manifest = ABI.manifest_properties(archive.read(prefix + "manifest.properties"))
        for name, record in audit["libraries"].items():
            path = directory / name
            path.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(prefix + name) as source, path.open("wb") as destination:
                shutil.copyfileobj(source, destination)
            if ABI.digest(path) != record["sha256"]:
                raise ValueError("Native library changed after ABI validation: " + name)
        for alias in filter(None, manifest.get("aliases", "").split(",")):
            path = directory / ABI.safe_path(alias)
            target = manifest["alias." + alias]
            if target not in audit["libraries"]:
                raise ValueError("Native alias changed after ABI validation: " + alias)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.symlink_to(os.path.relpath(directory / target, path.parent))
    return manifest


def check_bundle(bundle, output, qemu, timeout):
    environment = clean_environment()
    log_directory = output.parent / (output.stem + "-logs")
    log_directory.mkdir(parents=True, exist_ok=True)
    report = {"schemaVersion": 1, "platform": PLATFORM, "passed": False, "status": "failed",
              "models": list(MODELS), "probe": {"metric": "L2", "nq": 64, "nb": 4995, "dim": 32, "k": 10},
              "results": [], "logs": log_directory.name}
    try:
        report["jar.sha256"] = ABI.digest(bundle)
        audit = ABI.audit_bundle(bundle, PLATFORM)
        if not audit["passed"]:
            raise ValueError("CPU probe requires a bundle that passes the native ABI audit")
        qemu_path = shutil.which(str(qemu))
        if qemu_path is None:
            raise ValueError("qemu-x86_64 is required; install qemu-user")
        report["qemuVersion"] = subprocess.check_output(
            [qemu_path, "--version"], env=environment, text=True, stderr=subprocess.STDOUT).strip()
        require_qemu_version(report["qemuVersion"])
        report["compilerVersion"] = subprocess.check_output(
            ["gcc-12", "--version"], env=environment, text=True, stderr=subprocess.STDOUT).strip()
        with tempfile.TemporaryDirectory(prefix="native-cpu-") as temporary:
            work = Path(temporary)
            libraries = work / "lib"
            manifest = extract_libraries(bundle, libraries, audit)
            revision = subprocess.check_output(
                ["git", "-C", str(ROOT / "knowhere"), "rev-parse", "HEAD"],
                env=environment, text=True).strip()
            if manifest.get("knowhere.revision") != revision:
                raise ValueError("CPU probe headers/tests do not match the bundle's Knowhere revision")
            report["knowhere.revision"] = revision
            probe = work / "native-cpu-probe"
            source = ROOT / "scripts/ci/native_cpu_probe.c"
            report["probeSource.sha256"] = ABI.digest(source)
            report["upstreamTest.sha256"] = ABI.digest(ROOT / "knowhere/tests/c_api/test_c_api.c")
            command = ["gcc-12", "-std=c11", "-O2", "-march=x86-64", "-mtune=generic",
                       "-I", str(ROOT / "knowhere/include"), str(source), "-L", str(libraries),
                       "-Wl,-rpath-link," + str(libraries), "-lknowhere_c", "-lm", "-pthread", "-o", str(probe)]
            compilation, _ = execute(command, log_directory / "compile.log", environment, 120)
            report["compilation"] = compilation
            if compilation["exitCode"] != 0 or compilation["timedOut"]:
                raise ValueError("Could not compile the CPU probe; see compile.log")
            report["probe.sha256"] = ABI.digest(probe)
            for model in MODELS:
                command = [qemu_path, "-cpu", model, "-E", "LD_LIBRARY_PATH=" + str(libraries), str(probe), model]
                result, text = execute(command, log_directory / (model + ".log"), environment, timeout)
                result.update(cpu=model, passed=result["exitCode"] == 0 and PASS in text)
                report["results"].append(result)
                print(f"Native CPU {model}: {'passed' if result['passed'] else 'FAILED'} "
                      f"(exit={result['exitCode']}, signal={result['signal']})", flush=True)
        if ABI.digest(bundle) != report["jar.sha256"]:
            raise ValueError("Native bundle changed during CPU validation")
        report["passed"] = all(result["passed"] for result in report["results"])
        report["status"] = "passed" if report["passed"] else "failed"
    except (OSError, ValueError, zipfile.BadZipFile, subprocess.SubprocessError) as error:
        report["errors"] = [str(error)]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--platform", choices=(PLATFORM,), default=PLATFORM)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--qemu", default="qemu-x86_64")
    parser.add_argument("--timeout", type=int, default=120, help="Timeout in seconds for each CPU model")
    args = parser.parse_args()
    if args.timeout <= 0 or args.output.resolve() == args.bundle.resolve():
        parser.error("--timeout must be positive and --output must not overwrite --bundle")
    report = check_bundle(args.bundle.resolve(), args.output.resolve(), args.qemu, args.timeout)
    if not report["passed"]:
        print("Native CPU audit failed; see " + str(args.output), file=sys.stderr)
        return 1
    print("Native CPU audit passed: " + ", ".join(MODELS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
