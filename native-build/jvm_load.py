#!/usr/bin/env python3
"""Verify both JNI load orders in fresh JVM processes."""

import argparse
import json
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import sys
import tempfile

import platforms


_FORMAT = platforms.host()
JVM_LOAD_ENTRIES = (_FORMAT.library_name("milvus-storage-jni"),
                    _FORMAT.library_name("knowhere_jni"))
JAVA_TIMEOUT_SECONDS = _FORMAT.jvm_load_timeout_seconds()
# Options a JVM reads from the environment. The loader's own variables are the
# platform's business and the adapter removes them.
JAVA_ENVIRONMENT_OVERRIDES = (
    "CLASSPATH",
    "JAVA_TOOL_OPTIONS",
    "JDK_JAVA_OPTIONS",
    "_JAVA_OPTIONS",
)
MARKER_FAILURE_EXIT = 125


def _clean_environment():
    environment = _FORMAT.clean_environment()
    for name in JAVA_ENVIRONMENT_OVERRIDES:
        environment.pop(name, None)
    return environment


def _disable_core_dumps():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def _java_executable():
    configured = os.environ.get("JAVA_HOME")
    if configured:
        java = Path(configured) / "bin/java"
        if not java.is_file() or not os.access(java, os.X_OK):
            raise RuntimeError("JAVA_HOME does not provide an executable bin/java")
        return java.resolve()
    discovered = shutil.which("java")
    if discovered is None:
        raise RuntimeError("java is absent from PATH and JAVA_HOME is not set")
    return Path(discovered).resolve()


def _java_runtime():
    java = _java_executable()
    result = subprocess.run(
        [str(java), "-XshowSettings:properties", "-version"],
        env=_clean_environment(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=30,
        check=False,
    )
    if result.returncode:
        raise RuntimeError("Selected java failed while resolving java.home: " + result.stdout.strip())
    match = re.search(r"^\s*java\.home\s*=\s*(.+?)\s*$", result.stdout, re.MULTILINE)
    if match is None:
        raise RuntimeError("Selected java did not report java.home")
    java_home = Path(match.group(1))
    if not java_home.is_absolute():
        raise RuntimeError("Selected java reported a non-absolute java.home")
    java_home = java_home.resolve()
    libjsig = java_home / _FORMAT.jsig_library()
    if not libjsig.is_file():
        raise RuntimeError("Selected JRE does not provide " + _FORMAT.jsig_library())
    return java, java_home, libjsig.resolve()


def _timeout_output(error):
    output = error.stdout or ""
    if isinstance(output, bytes):
        output = output.decode(errors="replace")
    return output + "\nJVM load check timed out after " + str(JAVA_TIMEOUT_SECONDS) + " seconds\n"


def _validated_exit(exit_code, log, order):
    expected = [*("LOADED " + name for name in order), "PASS"]
    markers = [line for line in log.splitlines() if line == "PASS" or line.startswith("LOADED ")]
    if exit_code == 0 and markers != expected:
        message = "JVM exited 0 without the required load markers: " + repr(markers) + "\n"
        return MARKER_FAILURE_EXIT, log + ("" if log.endswith("\n") else "\n") + message
    return exit_code, log


def check_jvm_loads(directory, output=None):
    """Return the exit code from each fresh-JVM JNI load order."""
    directory = Path(directory).resolve(strict=True)
    if not directory.is_dir():
        raise ValueError("Native library directory is not a directory: " + str(directory))
    source = Path(__file__).with_name("NativeLoadCheck.java").resolve(strict=True)
    entries = {name: directory / name for name in JVM_LOAD_ENTRIES}
    missing = [name for name, path in entries.items() if not path.is_file()]
    if missing:
        raise ValueError("Native library directory is missing JVM load entries: " + ", ".join(missing))

    log_directory = None
    if output is not None:
        log_directory = Path(output)
        log_directory.mkdir(parents=True, exist_ok=True)

    java, java_home, libjsig = _java_runtime()
    environment = _clean_environment()
    environment["JAVA_HOME"] = str(java_home)
    environment[_FORMAT.preload_variable] = str(libjsig)
    orders = (JVM_LOAD_ENTRIES, tuple(reversed(JVM_LOAD_ENTRIES)))
    records = []
    with tempfile.TemporaryDirectory(prefix="milvus-native-load-check-") as temporary:
        for index, order in enumerate(orders):
            working_directory = Path(temporary) / str(index)
            working_directory.mkdir()
            error_file = working_directory / "hs_err_pid%p.log"
            command = [str(java), "-Xcheck:jni", "-XX:ErrorFile=" + str(error_file), str(source),
                       *(str(entries[name]) for name in order)]
            try:
                result = subprocess.run(
                    command,
                    cwd=working_directory,
                    env=environment,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    timeout=JAVA_TIMEOUT_SECONDS,
                    check=False,
                    preexec_fn=_disable_core_dumps,
                )
                exit_code = result.returncode
                log = result.stdout
            except subprocess.TimeoutExpired as error:
                exit_code = 124
                log = _timeout_output(error)
            crash_logs = sorted(working_directory.glob("hs_err_pid*.log"))
            for crash_log in crash_logs:
                log += ("" if log.endswith("\n") else "\n") + crash_log.read_text(errors="replace")
            exit_code, log = _validated_exit(exit_code, log, order)
            label = "--then--".join(_FORMAT.library_stem(name) for name in order)
            if log_directory is not None:
                (log_directory / ("jvm-load-" + label + ".log")).write_text(log)
            elif exit_code != 0:
                sys.stderr.write(log)
                if not log.endswith("\n"):
                    sys.stderr.write("\n")
            records.append({"entries": list(order), "exit": exit_code})
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    records = check_jvm_loads(args.lib_dir, args.output)
    print(json.dumps(records, indent=2))
    return 0 if all(record["exit"] == 0 for record in records) else 1


if __name__ == "__main__":
    sys.exit(main())
