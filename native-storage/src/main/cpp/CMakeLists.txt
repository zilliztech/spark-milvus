# Builds libnative-storage-jni, the JNI wrapper over milvus-storage's
# loon_filesystem_* interface.
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

add_library(native-storage-jni SHARED storage_native_jni.cpp)

target_include_directories(native-storage-jni PRIVATE
  ${JNI_INCLUDE_DIRS}
  "${MILVUS_STORAGE_ROOT}/cpp/include"
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
