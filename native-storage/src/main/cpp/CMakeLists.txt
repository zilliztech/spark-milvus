# Builds libnative-storage-jni, the JNI wrapper over milvus-storage's loon_*
# interface: the filesystem entry points, plus the segment read path.
#
# It links against the libmilvus-storage produced by the submodule build, so run
# `make build-milvus-storage` first. The artifact lands next to that library so
# `make copy-native-libs` picks both up in one step.

cmake_minimum_required(VERSION 3.20)
project(native-storage-jni CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(JNI REQUIRED)

# Where the submodule build left libmilvus-storage. The lib/ subdirectory is
# only produced on non-Apple platforms, so accept either.
set(MILVUS_STORAGE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../../../milvus-storage"
    CACHE PATH "milvus-storage checkout")
find_library(MILVUS_STORAGE_LIB
  NAMES milvus-storage
  PATHS "${MILVUS_STORAGE_ROOT}/cpp/build/Release"
        "${MILVUS_STORAGE_ROOT}/cpp/build/Release/lib"
  NO_DEFAULT_PATH
  REQUIRED)

add_library(native-storage-jni SHARED
  storage_native_jni.cpp
  segment_reader_jni.cpp
  writer_jni.cpp
)

# Arrow C++ headers. segment_reader_jni.cpp calls arrow::Concatenate,
# arrow::ImportRecordBatchReader and arrow::ExportRecordBatch, which
# libmilvus-storage exports. Arrow C++ has no stable ABI, so these headers must
# come from the same Arrow the submodule statically linked — a mismatch is not a
# compile error, it is a crash at runtime. Set ARROW_INCLUDE_DIR to the conan
# package the milvus-storage build used. The whole dependency goes away with the
# interim implementation at the top of segment_reader_jni.cpp.
# Read the path out of the conan files the milvus-storage build itself
# generated, so the headers are by construction the ones it linked. Several
# Arrow packages of the same version can sit in the conan cache with different
# options, and picking the wrong one compiles clean and crashes at runtime.
if(NOT DEFINED ARROW_INCLUDE_DIR)
  file(GLOB _arrow_data
    "${MILVUS_STORAGE_ROOT}/cpp/build/Release/generators/Arrow-release-*-data.cmake")
  if(NOT _arrow_data)
    message(FATAL_ERROR
      "Arrow package info not found under "
      "${MILVUS_STORAGE_ROOT}/cpp/build/Release/generators. Run "
      "`make build-milvus-storage` first, or pass "
      "-DARROW_INCLUDE_DIR=<conan arrow package>/include")
  endif()
  list(GET _arrow_data 0 _arrow_data_file)
  file(STRINGS "${_arrow_data_file}" _arrow_folder_line
    REGEX "^set\\(arrow_PACKAGE_FOLDER_RELEASE ")
  string(REGEX REPLACE "^set\\(arrow_PACKAGE_FOLDER_RELEASE \"(.*)\"\\)$" "\\1"
    _arrow_folder "${_arrow_folder_line}")
  if(NOT _arrow_folder OR NOT EXISTS "${_arrow_folder}/include/arrow/api.h")
    message(FATAL_ERROR
      "Could not read the Arrow package folder from ${_arrow_data_file}. "
      "Pass -DARROW_INCLUDE_DIR=<conan arrow package>/include")
  endif()
  set(ARROW_INCLUDE_DIR "${_arrow_folder}/include")
endif()
message(STATUS "Arrow headers: ${ARROW_INCLUDE_DIR}")

target_include_directories(native-storage-jni PRIVATE
  ${JNI_INCLUDE_DIRS}
  "${MILVUS_STORAGE_ROOT}/cpp/include"
  "${ARROW_INCLUDE_DIR}"
)

target_link_libraries(native-storage-jni PRIVATE ${MILVUS_STORAGE_LIB})

# Only the JNI entry points are exported. Everything statically linked into
# libmilvus-storage stays hidden, which is also what lets the linker drop code
# nothing calls.
set_target_properties(native-storage-jni PROPERTIES
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON
)

if(APPLE)
  set_target_properties(native-storage-jni PROPERTIES
    INSTALL_RPATH "@loader_path"
    BUILD_WITH_INSTALL_RPATH ON
  )
else()
  set_target_properties(native-storage-jni PROPERTIES
    BUILD_RPATH "$ORIGIN"
    INSTALL_RPATH "$ORIGIN"
  )
endif()

# Sit beside libmilvus-storage so one copy step collects both.
get_filename_component(MILVUS_STORAGE_LIB_DIR "${MILVUS_STORAGE_LIB}" DIRECTORY)
set_target_properties(native-storage-jni PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${MILVUS_STORAGE_LIB_DIR}"
)
