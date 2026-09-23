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
    #: The value Conan settings and upstream recipes use for this system.
    conan_os = None
    #: The environment variable that names extra directories for the dynamic
    #: loader, consulted only after a recorded path fails, so it adds providers
    #: without shadowing the ones the system supplies.
    library_fallback_variable = None
    #: The environment variables that name directories the dynamic loader
    #: searches before a library's recorded location, where the fallback
    #: variable is a separate one. Conan's run environment exports them, and a
    #: packaged library found there replaces the system one of the same name.
    library_override_variables = ()
    #: The environment variable that loads a library into a process before its
    #: own dependencies, which is how HotSpot's signal chaining reaches a JVM:
    #: the loader has to map libjsig before the VM installs its handlers, so no
    #: Java-level call can replace it.
    preload_variable = None

    def library_name(self, base, version=None):
        raise NotImplementedError

    def system_zlib(self):
        """The zlib the JVM has already loaded before JNI initialization."""
        raise NotImplementedError

    def system_libraries(self):
        """Matches a library the target system supplies, which is not bundled."""
        raise NotImplementedError

    def system_library_path(self, name):
        """Where this host's dynamic loader resolves a library the system
        supplies, for the checks that read its symbols.
        """
        raise NotImplementedError

    def compiler_runtime(self):
        """Compiler runtimes that are bundled with their source recorded."""
        return frozenset()

    def locate_compiler_runtime(self, name):
        """The absolute path of one compiler runtime on this host."""
        raise NotImplementedError

    def compiler_runtime_origin(self, path):
        """What supplied that runtime, for the bundle's provenance."""
        raise NotImplementedError

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

    #: How a binary names the directory it was loaded from.
    loader_origin = None

    def point_at_system_libraries(self, path):
        """Make the dependencies the system supplies resolve to the system copy
        rather than to the bundle directory. A no-op where the recorded name
        already reaches the system loader.
        """
        return

    def clean_environment(self, environment=None):
        """A copy of the environment with the loader's overrides removed."""
        raise NotImplementedError

    def compile_library(self, source, output, install_name, dependencies=()):
        """Build one shared library, used by fixtures rather than by the build."""
        raise NotImplementedError

    def toolchain(self):
        """``{"CC", "CXX", "FC"}`` for the compilers the engines are built with."""
        raise NotImplementedError

    def library_glob(self, stem="*"):
        """A glob matching this platform's shared libraries, versions included."""
        raise NotImplementedError

    def jsig_library(self):
        """The JDK's signal-chaining library, relative to JAVA_HOME."""
        raise NotImplementedError

    def jvm_load_timeout_seconds(self):
        """How long one JNI load order may take before it counts as hung.

        It bounds a deadlock, so it is far above what a load costs. What a
        load costs is the platform's business: the first ``dlopen`` of a
        freshly written library is the operating system's chance to inspect
        it, and the bundle holds 221 of them.
        """
        return 120

    def builds_diskann(self):
        """Whether the Knowhere build includes DiskANN here.

        Its only aligned reader is built on libaio and io_uring, so a platform
        that has neither cannot carry the index or its acceptance fixture. The
        bundle declares the answer, rather than a consumer inferring it from the
        platform name.
        """
        return True

    def library_stem(self, name):
        """A library file name without its lib prefix, version and suffix."""
        raise NotImplementedError

    def needs_tool_launcher(self):
        """Whether a packaged build tool must be invoked through a launcher that
        restores its library search path.
        """
        return False

    def toolchain_versions(self):
        """``(name, command)`` pairs recorded in the build's provenance."""
        raise NotImplementedError

    def cpu_report(self):
        """The command whose output records the build host's CPU."""
        raise NotImplementedError

    def available(self):
        return all(shutil.which(tool) for tool in self.tools)


class Elf(Format):
    operating_system = "linux"
    conan_os = "Linux"
    library_fallback_variable = "LD_LIBRARY_PATH"
    preload_variable = "LD_PRELOAD"
    loader_origin = "$ORIGIN"
    tools = ("readelf", "patchelf", "ldd")

    def library_name(self, base, version=None):
        return "lib" + base + ".so" + ("." + version if version else "")

    def system_zlib(self):
        return "libz.so.1"

    def library_glob(self, stem="*"):
        # The stem is a family, not one file: "libknowhere" has to reach
        # libknowhere_c.so.1 and libknowhere_jni.so as well, the way the
        # Mach-O pattern does.
        return ("" if stem == "*" else stem) + "*.so*"

    def jsig_library(self):
        return "lib/libjsig.so"

    def library_stem(self, name):
        return re.sub(r"\.so(?:\..*)?$", "", name.removeprefix("lib"))

    def system_libraries(self):
        return re.compile(
            r"^(?:ld-linux[^/]*"
            r"|lib(?:c|m|mvec|pthread|dl|rt|resolv|util|gcc_s|stdc\+\+)\.so(?:\..*)?"
            r"|libz\.so\.1)$"
        )

    def compiler_runtime(self):
        return frozenset({"libatomic.so.1", "libgomp.so.1", "libgfortran.so.5", "libquadmath.so.0"})

    def locate_compiler_runtime(self, name):
        return Path(_command(self, "gcc-12", "-print-file-name=" + name).strip())

    def compiler_runtime_origin(self, path):
        return "compiler runtime: " + _command(self, "gcc-12", "-dumpfullversion").strip()

    def system_packages(self):
        return frozenset({"libaio.so.1", "libaio.so.1t64"})

    #: How ``ldconfig -p`` tags an entry built for this machine, by
    #: ``platform.machine()``. A multiarch host caches other architectures'
    #: copies under the same name, so the tag is what tells them apart.
    LDCONFIG_TAGS = {"x86_64": "x86-64", "aarch64": "AArch64"}

    def system_library_path(self, name):
        machine = platform.machine().lower()
        if machine not in self.LDCONFIG_TAGS:
            raise ValueError("No ldconfig architecture tag for " + platform.machine())
        tag = self.LDCONFIG_TAGS[machine]
        matches = []
        for line in _command(self, "ldconfig", "-p").splitlines():
            entry = line.strip()
            if not entry.startswith(name + " ") or " => " not in entry:
                continue
            tags = entry.split("(", 1)[1].split(")", 1)[0].split(",") if "(" in entry else []
            if tag in tags:
                matches.append(entry.split(" => ", 1)[1])
        if len(matches) != 1:
            raise ValueError("The system must supply exactly one " + tag + " " + name)
        return Path(matches[0])

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

    def toolchain(self):
        return {"CC": "gcc-12", "CXX": "g++-12", "FC": "gfortran-12"}

    def toolchain_versions(self):
        return (("compiler", ["gcc-12", "--version"]),
                ("compiler-native-target", ["gcc-12", "-march=native", "-Q", "--help=target"]))

    def cpu_report(self):
        return ["lscpu"]


class MachO(Format):
    #: Where macOS keeps what it supplies; nothing under these is bundled.
    SYSTEM_PREFIXES = ("/System/", "/usr/lib/")

    operating_system = "darwin"
    conan_os = "Macos"
    library_fallback_variable = "DYLD_FALLBACK_LIBRARY_PATH"
    library_override_variables = ("DYLD_LIBRARY_PATH", "DYLD_FRAMEWORK_PATH")
    preload_variable = "DYLD_INSERT_LIBRARIES"
    loader_origin = "@loader_path"
    tools = ("otool", "install_name_tool", "codesign")

    def library_name(self, base, version=None):
        return "lib" + base + ("." + version if version else "") + ".dylib"

    def system_zlib(self):
        return "libz.1.dylib"

    def library_glob(self, stem="*"):
        return stem + "*.dylib"

    def jsig_library(self):
        return "lib/libjsig.dylib"

    def jvm_load_timeout_seconds(self):
        # Gatekeeper inspects each library the first time it is mapped, and a
        # staged bundle is 221 libraries nothing has mapped before: measured at
        # 197 seconds on an Apple M-series laptop, against 120 for every later
        # load of the same files. Linux has no such step, so its number does
        # not carry over.
        return 600

    def library_stem(self, name):
        return re.sub(r"(?:\.[0-9][^.]*)*\.dylib$", "", name.removeprefix("lib"))

    def builds_diskann(self):
        # macOS has neither libaio nor io_uring, and DiskANN's aligned reader is
        # built on them; cmake/Knowhere.cmake drops the index sources here the
        # way upstream does when the option is off.
        return False

    def compiler_runtime(self):
        # Apple Clang has no OpenMP runtime of its own; libomp is the macOS
        # counterpart of libgomp, which the Linux bundle already carries.
        return frozenset({"libomp.dylib"})

    def locate_compiler_runtime(self, name):
        prefix = None
        try:
            prefix = _command(self, "brew", "--prefix", "libomp").strip()
        except Exception:
            prefix = None
        for candidate in (prefix, "/opt/homebrew/opt/libomp", "/usr/local/opt/libomp"):
            if candidate and (Path(candidate) / "lib" / name).is_file():
                return Path(candidate) / "lib" / name
        raise ValueError("Missing OpenMP runtime " + name + "; install libomp")

    def compiler_runtime_origin(self, path):
        version = "unknown"
        receipt = path.resolve().parents[1] / "INSTALL_RECEIPT.json"
        if receipt.is_file():
            version = path.resolve().parents[1].name
        return "homebrew libomp " + version

    def system_libraries(self):
        # Names a binary can record without a path; anything with a system path
        # is dropped by inspect before it reaches this rule.
        # The ELF list's counterpart: what the operating system supplies and
        # the dependency graph does not. libiconv and libcharset are Conan
        # packages here, so they are bundled like any other dependency.
        return re.compile(
            r"^(?:libSystem\.B|libc\+\+(?:abi)?\.1|libobjc\.A|libresolv\.9"
            r"|libz\.1)\.dylib$"
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
        # What the operating system supplies is recognised by where it lives,
        # not by its name: a framework has no suffix at all. Those are neither
        # bundled nor resolved here, so they are not dependencies to carry.
        needed = [
            Path(name).name
            for name in re.findall(r"^\s+(\S+) \(compatibility version",
                                   _command(self, "otool", "-L", path), re.M)
            if not name.startswith(self.SYSTEM_PREFIXES)
        ]
        own = Path(recorded).name
        return {"soname": own, "needed": [name for name in needed if name != own]}

    def set_runtime_path(self, path, entries):
        # A library whose only @rpath entry is its own install name has no
        # dependency to find, and ld64 reserves no room to add a load command
        # unless the link asked for it. Leave such a file untouched.
        record = self.inspect(path)
        if record is not None and not any(
                name.startswith("@rpath/") for name in self.needed_paths(path)):
            return
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

    def point_at_system_libraries(self, path):
        # ELF resolves a dependency by its SONAME: the system copy and the
        # bundled copy are both found without rewriting anything. Mach-O
        # records a path, so each kind is pointed at explicitly -- a system
        # library at its absolute path, which the dyld shared cache resolves,
        # and a bundled one at @rpath, which the loader-relative search finds.
        system = self.system_libraries()
        changed = False
        for recorded in self.needed_paths(path):
            name = Path(recorded).name
            if recorded.startswith(self.SYSTEM_PREFIXES):
                # Already an absolute path into what the system supplies,
                # frameworks included; the loader resolves it as recorded.
                continue
            if system.fullmatch(name):
                target = "/usr/lib/" + name
            elif recorded.startswith("/"):
                target = "@rpath/" + name
            else:
                continue
            if target != recorded:
                subprocess.run(["install_name_tool", "-change", recorded,
                                target, str(path)], check=True)
                changed = True
        if changed:
            subprocess.run(["codesign", "--force", "--sign", "-", str(path)],
                           check=True, capture_output=True)

    def needed_paths(self, path):
        """Every dependency as the file records it, its own install name apart."""
        own = _command(self, "otool", "-D", path).splitlines()
        recorded = own[-1].strip() if len(own) > 1 else Path(path).name
        return [name for name in
                re.findall(r"^\s+(\S+) \(compatibility version",
                           _command(self, "otool", "-L", path), re.M)
                if name != recorded]

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
             # Packaged libraries record their install name against @rpath, so
             # a fixture that stands in for one records it the same way.
             "-install_name", "@rpath/" + install_name,
             "-Wl,-rpath,/unusable/conan/cache", "-Wl,-headerpad_max_install_names",
             *map(str, dependencies)],
            check=True,
        )
        return Path(output)

    def toolchain(self):
        # Apple Clang has no Fortran; OpenBLAS is not built here, and the
        # engines that would need one are not selected on this platform.
        return {"CC": "clang", "CXX": "clang++"}

    def needs_tool_launcher(self):
        # System Integrity Protection removes every DYLD_* variable when a
        # protected binary is executed, and Ninja runs each command through
        # /bin/sh. A launcher sets the variable inside that shell instead, where
        # it survives into the tool it execs.
        return True

    def toolchain_versions(self):
        # Clang names the host CPU with -mcpu on arm64 and -march on x86_64,
        # and rejects the other spelling for that target.
        native = "-mcpu=native" if platform.machine().lower() in ("arm64", "aarch64") else "-march=native"
        return (("compiler", ["clang", "--version"]),
                ("compiler-native-target", ["clang", "-E", native, "-###", "-x", "c", "/dev/null"]))

    def cpu_report(self):
        return ["sysctl", "-a", "machdep.cpu", "hw"]


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
