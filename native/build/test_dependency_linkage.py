"""Build shared fixtures whose missing providers reproduce the pinned recipe defects."""
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import unittest


HOOK = Path(__file__).parent / "cmake/DependencyLinkage.cmake"
RECIPES = {
    "opentelemetry-cpp": "opentelemetry-cpp/1.23.0@milvus/dev#11bc565ec6e82910ae8f7471da756720",
    "grpc": "grpc/1.67.1@milvus/dev#efeaa484b59bffaa579004d5e82ec4fd",
}
DESCRIPTOR = "src/core/ext/upb-gen/google/protobuf/descriptor.upb_minitable.c"


@unittest.skipUnless(platform.system() == "Linux" and all(shutil.which(tool) for tool in ("cmake", "cc", "ldd", "nm")),
                     "Shared ELF linkage fixtures require Linux and CMake/compiler/binutils")
class DependencyLinkageTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.environment = {key: value for key, value in os.environ.items()
                            if key not in ("LD_LIBRARY_PATH", "LD_PRELOAD", "LD_AUDIT", "LD_DEBUG", "LD_BIND_NOW")}
        self.environment["LC_ALL"] = "C"

    def command(self, *arguments, check=True):
        result = subprocess.run(list(map(str, arguments)), env=self.environment, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
        if check:
            self.assertEqual(0, result.returncode, result.stdout)
        return result

    def source(self, name, contents):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)

    def configure(self, project, contents, *, fixed, recipe=None, toolchain=False):
        self.source("CMakeLists.txt", "cmake_minimum_required(VERSION 3.21)\n"
                    + f"project({project} LANGUAGES C)\n" + contents)
        destination = self.root / ("fixed" if fixed else "broken")
        options = [f"-DCMAKE_PROJECT_INCLUDE={HOOK.resolve()}",
                   "-DMILVUS_DEPENDENCY_RECIPE=" + (recipe or RECIPES[project])] if fixed else []
        if toolchain:
            self.source("toolchain.cmake", f'set(CMAKE_PROJECT_INCLUDE "{HOOK.resolve()}")\n'
                        + f'set(MILVUS_DEPENDENCY_RECIPE "{recipe or RECIPES[project]}")\n'
                        + 'get_property(in_try_compile GLOBAL PROPERTY IN_TRY_COMPILE)\n'
                        + 'if(in_try_compile)\n'
                        + f'  file(APPEND "{self.root / "compiler-checks.txt"}" "try_compile\\n")\n'
                        + 'endif()\n')
            options = ["-DCMAKE_TOOLCHAIN_FILE=" + str(self.root / "toolchain.cmake")]
        return destination, self.command("cmake", "-S", self.root, "-B", destination, *options, check=False)

    def build(self, project, contents, *, fixed, toolchain=False):
        destination, result = self.configure(project, contents, fixed=fixed, toolchain=toolchain)
        self.assertEqual(0, result.returncode, result.stdout)
        self.command("cmake", "--build", destination, "--parallel", "2")
        return destination

    def relocations(self, directory, library):
        return self.command("ldd", "-r", directory / ("lib" + library + ".so"), check=False).stdout

    def assert_resolved(self, directory, library):
        result = self.command("ldd", "-r", directory / ("lib" + library + ".so"))
        for failure in ("undefined symbol:", "not found", "Relink `"):
            self.assertNotIn(failure, result.stdout)

    def otel_fixture(self):
        self.source("grpc.c", "int grpc_status(void) { return 7; }\n")
        self.source("proto.c", "int proto_message(void) { return 5; }\n")
        self.source("proto_grpc.c", "extern int grpc_status(void); extern int proto_message(void);\n"
                    "int export_span(void) { return grpc_status() + proto_message(); }\n")
        return """
add_library(grpc_provider SHARED grpc.c)
add_library(gRPC::grpc++ INTERFACE IMPORTED)
set_property(TARGET gRPC::grpc++ PROPERTY INTERFACE_LINK_LIBRARIES grpc_provider)
add_library(opentelemetry_proto SHARED proto.c)
add_library(opentelemetry_proto_grpc SHARED proto_grpc.c)
target_link_libraries(opentelemetry_proto_grpc PUBLIC opentelemetry_proto)
get_target_property(grpc_lib_type gRPC::grpc++ TYPE)
if(grpc_lib_type STREQUAL "SHARED_LIBRARY")
  target_link_libraries(opentelemetry_proto_grpc PUBLIC gRPC::grpc++)
endif()
"""

    def grpc_fixture(self):
        self.source(DESCRIPTOR, "const int google__protobuf__FileOptions_msg_init = 17;\n")
        self.source("mini.c", "int mini_descriptor(void) { return 3; }\n")
        self.source("json.c", "extern const int google__protobuf__FileOptions_msg_init;\n"
                    "int json_descriptor(void) { return google__protobuf__FileOptions_msg_init; }\n")
        self.source("text.c", "extern const int google__protobuf__FileOptions_msg_init;\n"
                    "int text_descriptor(void) { return google__protobuf__FileOptions_msg_init; }\n")
        self.source("grpc.c", "extern int json_descriptor(void); extern int text_descriptor(void);\n"
                    "int grpc_api(void) { return json_descriptor() + text_descriptor(); }\n")
        self.source("unsecure.c", "extern const int google__protobuf__FileOptions_msg_init;\n"
                    "int grpc_unsecure_api(void) { return google__protobuf__FileOptions_msg_init; }\n")
        return f"""
add_library(upb_mini_descriptor_lib SHARED mini.c)
add_library(upb_json_lib SHARED json.c)
target_link_libraries(upb_json_lib upb_mini_descriptor_lib)
add_library(upb_textformat_lib SHARED text.c)
target_link_libraries(upb_textformat_lib upb_mini_descriptor_lib)
add_library(grpc SHARED grpc.c {DESCRIPTOR})
target_link_libraries(grpc upb_json_lib upb_textformat_lib)
add_library(grpc_unsecure SHARED unsecure.c {DESCRIPTOR})
target_link_libraries(grpc_unsecure upb_mini_descriptor_lib)
"""

    def test_conan_interface_wrapper_keeps_grpc_provider_in_otel_shared_library(self):
        fixture = self.otel_fixture()
        broken = self.build("opentelemetry-cpp", fixture, fixed=False)
        self.assertIn("undefined symbol: grpc_status", self.relocations(broken, "opentelemetry_proto_grpc"))
        fixed = self.build("opentelemetry-cpp", fixture, fixed=True)
        self.assert_resolved(fixed, "opentelemetry_proto_grpc")

    def test_upb_consumers_resolve_one_lower_level_descriptor_provider(self):
        fixture = self.grpc_fixture()
        broken = self.build("grpc", fixture, fixed=False)
        for target in ("upb_json_lib", "upb_textformat_lib"):
            self.assertIn("undefined symbol: google__protobuf__FileOptions_msg_init", self.relocations(broken, target))
        fixed = self.build("grpc", fixture, fixed=True)
        for target in ("upb_mini_descriptor_lib", "upb_json_lib", "upb_textformat_lib", "grpc", "grpc_unsecure"):
            self.assert_resolved(fixed, target)
            definitions = self.command("nm", "-D", "--defined-only", fixed / ("lib" + target + ".so")).stdout
            self.assertEqual(target == "upb_mini_descriptor_lib", "google__protobuf__FileOptions_msg_init" in definitions)

    def test_toolchain_hook_skips_real_compiler_try_compile_projects(self):
        fixed = self.build("opentelemetry-cpp", self.otel_fixture(), fixed=True, toolchain=True)
        self.assertIn("try_compile\n", (self.root / "compiler-checks.txt").read_text())
        self.assert_resolved(fixed, "opentelemetry_proto_grpc")

    def test_static_build_tool_keeps_descriptor_in_parent_archives(self):
        fixture = self.grpc_fixture().replace(" SHARED ", " STATIC ")
        self.source("grpc.c", "extern int json_descriptor(void); extern int text_descriptor(void);\n"
                    "extern const int google__protobuf__FileOptions_msg_init;\n"
                    "int grpc_api(void) { return json_descriptor() + text_descriptor()"
                    " + google__protobuf__FileOptions_msg_init; }\n")
        self.source("tool.c", "extern int grpc_api(void); extern int grpc_unsecure_api(void);\n"
                    "int main(void) { return grpc_api() == 51 && grpc_unsecure_api() == 17 ? 0 : 1; }\n")
        fixture += "\nadd_executable(grpc_tool tool.c)\ntarget_link_libraries(grpc_tool PRIVATE grpc grpc_unsecure)\n"
        fixed = self.build("grpc", fixture, fixed=True, toolchain=True)
        self.command(fixed / "grpc_tool")
        for target in ("upb_mini_descriptor_lib", "grpc", "grpc_unsecure"):
            definitions = self.command("nm", "--defined-only", fixed / ("lib" + target + ".a")).stdout
            self.assertEqual(target != "upb_mini_descriptor_lib", "google__protobuf__FileOptions_msg_init" in definitions)

    def test_mixed_grpc_library_kinds_are_rejected(self):
        fixture = self.grpc_fixture().replace("add_library(upb_mini_descriptor_lib SHARED",
                                               "add_library(upb_mini_descriptor_lib STATIC")
        _, result = self.configure("grpc", fixture, fixed=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Reviewed gRPC/upb library kinds are mixed", result.stdout)

    def test_changed_recipe_is_rejected_before_target_corrections(self):
        _, result = self.configure("opentelemetry-cpp", self.otel_fixture(), fixed=True,
                                   recipe=RECIPES["opentelemetry-cpp"].replace("1.23.0", "1.24.0"))
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Unreviewed dependency linkage recipe/project", result.stdout)

    def test_changed_descriptor_ownership_is_rejected(self):
        fixture = self.grpc_fixture().replace("add_library(grpc SHARED grpc.c " + DESCRIPTOR + ")",
                                               "add_library(grpc SHARED grpc.c)")
        _, result = self.configure("grpc", fixture, fixed=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("descriptor must occur once in grpc sources", result.stdout)


if __name__ == "__main__":
    unittest.main()
