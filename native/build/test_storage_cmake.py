"""Configure the storage adapter against pinned sources without downloading dependencies.

External packages and Corrosion are stand-ins; this checks the selected source
graph and bridge inputs, not compilation or runtime authentication.
"""
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class StorageSourceGraphTest(unittest.TestCase):
    def test_upstream_talon_and_credentials_are_in_the_native_graph(self):
        repository = Path(__file__).resolve().parents[2]
        storage = repository / "milvus-storage"
        if not (storage / "cpp/src/format/bridge/rust/Cargo.lock").is_file():
            self.skipTest("Initialize milvus-storage to configure the pinned source graph")
        if not shutil.which("cmake") or not shutil.which("c++"):
            self.skipTest("CMake and a C++ compiler are required for source graph validation")

        with tempfile.TemporaryDirectory(prefix="storage-cmake-") as directory:
            source = Path(directory)
            corrosion = source / "corrosion"
            corrosion.mkdir()
            (source / "dummy.cpp").write_text("int storage_graph_fixture;\n")
            (corrosion / "CMakeLists.txt").write_text("""
function(corrosion_import_crate)
  cmake_parse_arguments(CRATE "LOCKED" "MANIFEST_PATH" "CRATES;FEATURES" ${ARGN})
  set_property(GLOBAL PROPERTY STORAGE_RUST_FEATURES "${CRATE_FEATURES}")
  set_property(GLOBAL PROPERTY STORAGE_CARGO_LOCKED "${CRATE_LOCKED}")
  add_library(rust_bridge STATIC "${PROJECT_SOURCE_DIR}/dummy.cpp")
  add_custom_target(cargo-build_rust_bridge)
endfunction()
function(corrosion_set_env_vars)
endfunction()
function(corrosion_add_cxxbridge target)
  cmake_parse_arguments(BRIDGE "" "CRATE" "FILES" ${ARGN})
  set_property(GLOBAL PROPERTY STORAGE_BRIDGE_FILES "${BRIDGE_FILES}")
  add_library(${target} STATIC "${PROJECT_SOURCE_DIR}/dummy.cpp")
endfunction()
""")
            (source / "CMakeLists.txt").write_text("""
cmake_minimum_required(VERSION 3.27)
project(storage_source_graph LANGUAGES CXX)
# Packages are represented only by their imported targets. Compile and link
# validation belongs to the real native build, which resolves Conan packages.
foreach(target IN ITEMS Boost::boost arrow::arrow milvus-common::milvus-common
    protobuf::protobuf nlohmann_json::nlohmann_json AWS::aws-sdk-cpp-identity-management
    google-cloud-cpp::storage avro-cpp::avrocpp_s Folly::folly fmt::fmt
    azure-sdk-for-cpp::azure-sdk-for-cpp glog::glog CURL::libcurl
    prometheus-cpp::core opentelemetry-cpp::opentelemetry_proto)
  add_library(${target} INTERFACE IMPORTED)
endforeach()
function(find_package name)
  if(NOT name IN_LIST known_packages)
    message(FATAL_ERROR "Unrepresented external package: ${name}")
  endif()
endfunction()
set(known_packages Boost fmt folly glog prometheus-cpp Arrow Azure Protobuf
    nlohmann_json opentelemetry-cpp google-cloud-cpp CURL AWSSDK milvus-common
    libavrocpp avro-cpp JNI)
include("${STORAGE_ADAPTER}")
milvus_add_storage()
foreach(target IN ITEMS milvus-storage prsbridge milvus-storage-jni)
  get_target_property(sources ${target} SOURCES)
  file(WRITE "${CMAKE_BINARY_DIR}/${target}.sources" "${sources}")
endforeach()
get_property(features GLOBAL PROPERTY STORAGE_RUST_FEATURES)
get_property(locked GLOBAL PROPERTY STORAGE_CARGO_LOCKED)
get_property(bridges GLOBAL PROPERTY STORAGE_BRIDGE_FILES)
file(WRITE "${CMAKE_BINARY_DIR}/features" "${features}")
file(WRITE "${CMAKE_BINARY_DIR}/locked" "${locked}")
file(WRITE "${CMAKE_BINARY_DIR}/bridges" "${bridges}")
""")
            build = source / "build"
            result = subprocess.run([
                "cmake", "-S", str(source), "-B", str(build),
                "-DSTORAGE_ADAPTER=" + str(repository / "native/build/cmake/Storage.cmake"),
                "-DMILVUS_STORAGE_SOURCE_DIR=" + str(storage),
                "-DFETCHCONTENT_SOURCE_DIR_CORROSION=" + str(corrosion),
            ], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
            self.assertEqual(result.returncode, 0, result.stdout)

            def selected_sources(target):
                return {Path(path).relative_to(storage).as_posix()
                        for path in (build / (target + ".sources")).read_text().split(";")}

            cpp_sources = selected_sources("milvus-storage")
            self.assertIn("cpp/src/filesystem/talon/talon_file_system_producer.cpp", cpp_sources)
            self.assertIn("cpp/src/filesystem/talon/talon_file_system.cpp", cpp_sources)
            credential_sources = {path.relative_to(storage).as_posix()
                                  for path in storage.rglob("TencentCloudOIDCAssumeRoleChainProvider.cpp")}
            self.assertTrue(credential_sources, "Pinned storage must contain the Tencent role-chain provider")
            self.assertTrue(credential_sources <= cpp_sources)
            self.assertFalse(any("/jni/" in path or "/format/bridge/" in path
                                 for path in cpp_sources))
            self.assertIn("cpp/src/format/bridge/rust/src/talon/talon_bridge.cpp",
                          selected_sources("prsbridge"))
            self.assertTrue(all("/jni/" in path for path in selected_sources("milvus-storage-jni")))
            self.assertIn("talon", (build / "features").read_text().split(";"))
            self.assertEqual((build / "locked").read_text(), "TRUE")
            self.assertEqual(set((build / "bridges").read_text().split(";")),
                             {"lib.rs", "talon/talon_bridge.rs"})


if __name__ == "__main__":
    unittest.main()
