"""Validate pinned Knowhere source groups and SIMD dispatch at CMake configure time.

External packages are imported stand-ins. Architecture and compiler capabilities
are simulated; compilation, linking and CPU execution belong to the native build.
"""
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


FIXTURE = r"""
cmake_minimum_required(VERSION 3.27)
project(knowhere_source_graph LANGUAGES C CXX)
enable_testing()
foreach(target IN ITEMS Boost::boost OpenMP::OpenMP_CXX Folly::folly
    nlohmann_json::nlohmann_json glog::glog prometheus-cpp::core
    prometheus-cpp::push fmt::fmt xxHash::xxhash milvus-common::milvus-common
    simde::simde opentelemetry-cpp::opentelemetry_trace OpenBLAS::OpenBLAS
    Threads::Threads)
  add_library(${target} INTERFACE IMPORTED)
endforeach()
set(known_packages Boost OpenMP folly nlohmann_json glog prometheus-cpp fmt
    xxHash simde opentelemetry-cpp milvus-common double-conversion OpenBLAS
    LAPACK BLAS Threads Java JNI)
function(find_package name)
  if(NOT name IN_LIST known_packages)
    message(FATAL_ERROR "Unrepresented external package: ${name}")
  endif()
endfunction()
set(MILVUS_KNOWHERE_AIO_LIBRARY "${CMAKE_CURRENT_LIST_FILE}")
set(Java_JAVAC_EXECUTABLE "${CMAKE_COMMAND}")

# Evaluate only upstream source-group selection, with every architecture branch
# disabled. This supplies an independent reference for our explicit source list.
function(knowhere_file_glob mode name)
  set(patterns)
  foreach(pattern IN LISTS ARGN)
    list(APPEND patterns "${KNOWHERE_SOURCE_DIR}/${pattern}")
  endforeach()
  file(${mode} sources ${patterns})
  set(${name} "${sources}" PARENT_SCOPE)
endfunction()
function(record_upstream_groups)
  foreach(arch X86_64 AARCH64 RISCV64 LOONGARCH64 PPC64)
    set(__${arch} FALSE)
  endforeach()
  set(WITH_SVS FALSE)
  include("${KNOWHERE_SOURCE_DIR}/cmake/libs/libfaiss.cmake")
  foreach(group FAISS FAISS_FASTSCAN FAISS_AVX2 FAISS_AVX512
      FAISS_AVX512_VPOPCNT FAISS_AVX512_SPR FAISS_NEON)
    file(WRITE "${CMAKE_BINARY_DIR}/upstream-${group}" "${${group}_SRCS}")
  endforeach()
endfunction()
record_upstream_groups()
include("${KNOWHERE_ADAPTER}")
milvus_add_knowhere()
foreach(target faiss faiss_avx2 faiss_avx512 faiss_avx512_vpopcnt
    faiss_avx512_spr knowhere knowhere_utils)
  if(TARGET ${target})
    foreach(property SOURCES COMPILE_OPTIONS COMPILE_DEFINITIONS)
      get_target_property(value ${target} ${property})
      file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/${target}.${property}" CONTENT "${value}"
          CONDITION "$<COMPILE_LANGUAGE:CXX>" TARGET ${target})
    endforeach()
  endif()
endforeach()
include("${KNOWHERE_ADAPTER_DIR}/knowhere/Sources.cmake")
foreach(group FAISS FAISS_FASTSCAN FAISS_AVX2 FAISS_AVX512
    FAISS_AVX512_VPOPCNT FAISS_AVX512_SPR FAISS_NEON)
  file(WRITE "${CMAKE_BINARY_DIR}/adapter-${group}" "${MILVUS_${group}_SOURCES}")
endforeach()
"""


class KnowhereSourceGraphTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repository = Path(__file__).resolve().parents[2]
        cls.knowhere = cls.repository / "knowhere"
        if not (cls.knowhere / "cmake/libs/libfaiss.cmake").is_file():
            raise unittest.SkipTest("Initialize Knowhere to configure its pinned source graph")
        if not shutil.which("cmake") or not shutil.which("c++"):
            raise unittest.SkipTest("CMake and a C++ compiler are required")

    def configure(self, architecture="x86_64", unavailable=(), apple=False):
        directory = tempfile.TemporaryDirectory(prefix="knowhere-cmake-")
        self.addCleanup(directory.cleanup)
        source = Path(directory.name)
        (source / "CMakeLists.txt").write_text(FIXTURE)
        build = source / "build"
        adapter = self.repository / "native/build/cmake/Knowhere.cmake"
        command = [
            "cmake", "-S", str(source), "-B", str(build),
            "-DKNOWHERE_ADAPTER=" + str(adapter),
            "-DKNOWHERE_ADAPTER_DIR=" + str(adapter.parent),
            "-DKNOWHERE_SOURCE_DIR=" + str(self.knowhere),
            "-DMILVUS_KNOWHERE_X86_64=" + str(architecture == "x86_64"),
            "-DMILVUS_KNOWHERE_AARCH64=" + str(architecture == "aarch64"),
            "-DAPPLE=" + str(apple),
        ]
        for feature in ("AVX512VPOPCNTDQ", "AVX512BITALG", "AVX512VNNI",
                        "AVX512FP16", "AVX512BF16"):
            command.append("-DMILVUS_HAS_" + feature + "=" + str(feature not in unavailable))
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout)
        return build

    def values(self, build, name):
        return set((build / name).read_text().split(";"))

    def test_sources_match_upstream_groups_and_include_new_implementations(self):
        build = self.configure()
        for group in ("FAISS", "FAISS_FASTSCAN", "FAISS_AVX2", "FAISS_AVX512",
                      "FAISS_AVX512_VPOPCNT", "FAISS_AVX512_SPR", "FAISS_NEON"):
            with self.subTest(group=group):
                self.assertEqual(self.values(build, "adapter-" + group),
                                 self.values(build, "upstream-" + group))
        expected = {str(path) for root in ("common", "index", "cluster", "io")
                    for path in (self.knowhere / "src" / root).rglob("*.cc")
                    if not any(part in path.parts for part in ("gpu", "gpu_cuvs", "cuvs", "svs"))}
        expected.update(str(path) for path in
                        (self.knowhere / "src/index/sparse/codec").glob("*.c"))
        self.assertEqual(self.values(build, "knowhere.SOURCES"), expected)

    def test_x86_dispatch_objects_link_without_leaking_isa_flags_to_baseline(self):
        build = self.configure()
        baseline = self.values(build, "faiss.COMPILE_OPTIONS")
        self.assertTrue({"-msse4.2", "-mno-avx", "-mno-avx2"} <= baseline)
        self.assertFalse(any(flag.startswith("-mavx") for flag in baseline))
        definitions = self.values(build, "faiss.COMPILE_DEFINITIONS")
        self.assertTrue({"COMPILE_SIMD_AVX512_VPOPCNT", "COMPILE_SIMD_AVX512_SPR"}
                        <= definitions)
        baseline_sources = self.values(build, "faiss.SOURCES")
        for target in ("faiss_avx512_vpopcnt", "faiss_avx512_spr"):
            # Evaluated TARGET_OBJECTS must feed the final archive, not merely
            # create an otherwise unused SIMD target.
            self.assertTrue(any(("/" + target + ".dir/") in path
                                for path in baseline_sources), target)
            self.assertTrue({"-mavx512vpopcntdq", "-mavx512bitalg", "-mavx512cd"}
                            <= self.values(build, target + ".COMPILE_OPTIONS"))
        self.assertIn("-mavx512cd", self.values(build, "faiss_avx512.COMPILE_OPTIONS"))
        fastscan = self.values(build, "upstream-FAISS_FASTSCAN")
        self.assertTrue(fastscan <= self.values(build, "faiss_avx2.SOURCES"))
        self.assertFalse(fastscan & baseline_sources)

    def test_vpopcnt_does_not_require_sapphire_rapids(self):
        for apple in (False, True):
            with self.subTest(apple=apple):
                build = self.configure(unavailable=("AVX512FP16",), apple=apple)
                self.assertTrue((build / "faiss_avx512_vpopcnt.SOURCES").exists())
                self.assertFalse((build / "faiss_avx512_spr.SOURCES").exists())
                definitions = self.values(build, "faiss.COMPILE_DEFINITIONS")
                self.assertIn("COMPILE_SIMD_AVX512_VPOPCNT", definitions)
                self.assertNotIn("COMPILE_SIMD_AVX512_SPR", definitions)

    def test_missing_vpopcnt_feature_disables_both_optional_dispatch_levels(self):
        for feature in ("AVX512VPOPCNTDQ", "AVX512BITALG"):
            with self.subTest(feature=feature):
                build = self.configure(unavailable=(feature,))
                definitions = self.values(build, "faiss.COMPILE_DEFINITIONS")
                for suffix in ("VPOPCNT", "SPR"):
                    self.assertNotIn("COMPILE_SIMD_AVX512_" + suffix, definitions)
                    self.assertFalse((build / ("faiss_avx512_" + suffix.lower() + ".SOURCES")).exists())

    def test_arm_keeps_neon_baseline_without_unlinked_sve_dispatch(self):
        for apple in (False, True):
            with self.subTest(apple=apple):
                build = self.configure(architecture="aarch64", apple=apple)
                definitions = self.values(build, "faiss.COMPILE_DEFINITIONS")
                self.assertIn("COMPILE_SIMD_ARM_NEON", definitions)
                self.assertNotIn("COMPILE_SIMD_ARM_SVE", definitions)
                self.assertFalse(any("AVX" in definition for definition in definitions))
                self.assertFalse((build / "faiss_avx2.SOURCES").exists())
                sources = self.values(build, "faiss.SOURCES")
                self.assertTrue(self.values(build, "upstream-FAISS_NEON") <= sources)
                self.assertTrue(self.values(build, "upstream-FAISS_FASTSCAN") <= sources)
                self.assertFalse(any("sve" in flag for flag in self.values(build, "faiss.COMPILE_OPTIONS")))


if __name__ == "__main__":
    unittest.main()
