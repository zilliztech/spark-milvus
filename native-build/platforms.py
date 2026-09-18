"""The platform adapter: everything about a native artifact that depends on the
executable format.

The staging and audit code above this module works on file names, digests and
dependency names. Reading those names out of a binary, rewriting where it looks
for its dependencies, knowing which libraries the system supplies, and scrubbing
the loader's environment are the parts that differ per platform, so they live
here. Adding a platform means adding one class and naming it in ``host``.

The library naming rules must agree with ``project/NativePlatform.scala`` and
``native-runtime``'s ``NativeLibraries``; a bundle is usable only on a host whose
platform string matches the one its manifest declares.
"""

import os
import platform
import re
import shutil
import subprocess
from pathlib import Path


class Format:
    """What the staging code needs from an executable format."""

    #: The classifier segment, joined with the architecture to name a platform.
    operating_system = None
    #: Executables the adapter shells out to, checked before a build starts.
    tools = ()

    def library_name(self, base, version=None):
        raise NotImplementedError

    def system_zlib(self):
        """The zlib the JVM has already loaded before JNI initialization."""
        raise NotImplementedError

    def system_libraries(self):
        """Matches a library the target system supplies, which is not bundled."""
        raise NotImplementedError

    def compiler_runtime(self):
        """Compiler runtimes that are bundled with their source recorded."""
        return frozenset()

    def system_packages(self):
        """Distribution libraries that are bundled with their source recorded."""
        return frozenset()

    def inspect(self, path):
        """``{"soname", "needed", "sourceSha256"}`` for a shared library of this
        format, or ``None`` when the file is not one. ``soname`` is the name
        other binaries record when they depend on it.
        """
        raise NotImplementedError

    def set_runtime_path(self, path, entries):
        """Make ``path`` look for its dependencies in ``entries``, each relative
        to the loaded file's own directory.
        """
        raise NotImplementedError

    def read_runtime_path(self, path):
        """The runtime search path recorded in ``path``, for checks and tests."""
        raise NotImplementedError

    def clean_environment(self, environment=None):
        """A copy of the environment with the loader's overrides removed."""
        raise NotImplementedError

    def compile_library(self, source, output, install_name, dependencies=()):
        """Build one shared library, used by fixtures rather than by the build."""
        raise NotImplementedError

    def available(self):
        return all(shutil.which(tool) for tool in self.tools)


class Elf(Format):
    operating_system = "linux"
    tools = ("readelf", "patchelf", "ldd")

    def library_name(self, base, version=None):
        return "lib" + base + ".so" + ("." + version if version else "")

    def system_zlib(self):
        return "libz.so.1"

    def system_libraries(self):
        return re.compile(
            r"^(?:ld-linux[^/]*"
            r"|lib(?:c|m|mvec|pthread|dl|rt|resolv|util|gcc_s|stdc\+\+)\.so(?:\..*)?"
            r"|libz\.so\.1)$"
        )

    def compiler_runtime(self):
        return frozenset({"libatomic.so.1", "libgomp.so.1", "libgfortran.so.5", "libquadmath.so.0"})

    def system_packages(self):
        return frozenset({"libaio.so.1", "libaio.so.1t64"})

    def inspect(self, path):
        path = Path(path)
        with path.open("rb") as source:
            if source.read(4) != b"\x7fELF":
                return None
        output = _command(self, "readelf", "-dW", path)
        soname = re.findall(r"\(SONAME\).*\[(.*?)\]", output)
        return {
            "soname": soname[0] if soname else path.name,
            "needed": re.findall(r"\(NEEDED\).*\[(.*?)\]", output),
        }

    def set_runtime_path(self, path, entries):
        joined = ":".join("$ORIGIN" + ("/" + entry if entry else "") for entry in entries)
        subprocess.run(["patchelf", "--set-rpath", joined, str(path)], check=True)

    def read_runtime_path(self, path):
        return subprocess.check_output(
            ["patchelf", "--print-rpath", str(path)], text=True
        ).strip()

    def clean_environment(self, environment=None):
        result = dict(os.environ if environment is None else environment)
        for name in ("LD_LIBRARY_PATH", "LD_PRELOAD", "LD_AUDIT", "LD_DEBUG", "LD_BIND_NOW"):
            result.pop(name, None)
        result["LC_ALL"] = "C"
        return result

    def compile_library(self, source, output, install_name, dependencies=()):
        subprocess.run(
            ["gcc", "-shared", "-fPIC", str(source), "-o", str(output),
             "-Wl,-soname," + install_name, "-Wl,-rpath,/unusable/conan/cache",
             *map(str, dependencies)],
            check=True,
        )
        return Path(output)


class MachO(Format):
    operating_system = "darwin"
    tools = ("otool", "install_name_tool", "codesign")

    def library_name(self, base, version=None):
        return "lib" + base + ("." + version if version else "") + ".dylib"

    def system_zlib(self):
        return "libz.1.dylib"

    def system_libraries(self):
        # macOS supplies these from the shared cache; nothing else is assumed.
        return re.compile(
            r"^(?:libSystem\.B|libc\+\+(?:abi)?\.1|libobjc\.A|libz\.1|libiconv\.2"
            r"|libcharset\.1|libresolv\.9|libbsm\.0)\.dylib$"
        )

    def inspect(self, path):
        path = Path(path)
        with path.open("rb") as source:
            magic = source.read(4)
        # 64-bit Mach-O, both byte orders, and the universal binary wrapper.
        if magic not in (b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf", b"\xca\xfe\xba\xbe"):
            return None
        install_name = _command(self, "otool", "-D", path).splitlines()
        recorded = install_name[-1].strip() if len(install_name) > 1 else path.name
        needed = [
            Path(name).name
            for name in re.findall(r"^\s+(\S+) \(compatibility version", _command(self, "otool", "-L", path), re.M)
        ]
        own = Path(recorded).name
        return {"soname": own, "needed": [name for name in needed if name != own]}

    def set_runtime_path(self, path, entries):
        for existing in self.read_runtime_path(path).split(":"):
            if existing:
                subprocess.run(
                    ["install_name_tool", "-delete_rpath", existing, str(path)],
                    check=False, capture_output=True,
                )
        for entry in entries:
            subprocess.run(
                ["install_name_tool", "-add_rpath",
                 "@loader_path" + ("/" + entry if entry else ""), str(path)],
                check=True,
            )
        # Every install_name_tool rewrite invalidates the signature that arm64
        # requires, so the file is re-signed ad hoc before anything loads it.
        subprocess.run(["codesign", "--force", "--sign", "-", str(path)],
                       check=True, capture_output=True)

    def read_runtime_path(self, path):
        output = _command(self, "otool", "-l", path)
        return ":".join(re.findall(r"path (\S+) \(offset \d+\)", output))

    def clean_environment(self, environment=None):
        result = dict(os.environ if environment is None else environment)
        for name in ("DYLD_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES", "DYLD_FRAMEWORK_PATH",
                     "DYLD_FALLBACK_LIBRARY_PATH", "DYLD_PRINT_LIBRARIES"):
            result.pop(name, None)
        result["LC_ALL"] = "C"
        return result

    def compile_library(self, source, output, install_name, dependencies=()):
        subprocess.run(
            ["clang", "-shared", "-fPIC", str(source), "-o", str(output),
             "-install_name", install_name, "-Wl,-rpath,/unusable/conan/cache",
             *map(str, dependencies)],
            check=True,
        )
        return Path(output)


FORMATS = {"linux": Elf, "darwin": MachO}


def _command(binary_format, *arguments):
    return subprocess.check_output(
        list(map(str, arguments)), text=True, env=binary_format.clean_environment()
    )


def host():
    """The adapter for the machine running the build."""
    name = platform.system().lower()
    if name not in FORMATS:
        raise ValueError("Unsupported native platform: " + platform.system())
    return FORMATS[name]()


def host_platform():
    """The classifier the manifest declares, as the JVM loader spells it."""
    machine = platform.machine().lower()
    architecture = {"x86_64": "x86_64", "amd64": "x86_64",
                    "aarch64": "aarch64", "arm64": "aarch64"}.get(machine)
    if architecture is None:
        raise ValueError("Unsupported native architecture: " + platform.machine())
    return host().operating_system + "-" + architecture
