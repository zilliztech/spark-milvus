cmake_minimum_required(VERSION 3.27)
project(spark_milvus_native LANGUAGES C CXX)

# The covered platforms; each also needs a Conan profile and a platforms.py
# adapter, which the build driver checks before it reaches CMake.
if(NOT ((CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|aarch64|arm64)$")
        OR (APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(arm64|aarch64)$")))
  message(FATAL_ERROR "This native source build covers Linux x86_64, Linux aarch64 and macOS arm64, not "
                      "${CMAKE_SYSTEM_NAME} ${CMAKE_SYSTEM_PROCESSOR}")
endif()

set(MILVUS_STORAGE_SOURCE_DIR "" CACHE PATH "Pinned milvus-storage source checkout")
set(KNOWHERE_SOURCE_DIR "" CACHE PATH "Pinned Knowhere source checkout")
option(WITH_CARDINAL "Build the pinned Cardinal plugins" OFF)

foreach(source IN ITEMS MILVUS_STORAGE_SOURCE_DIR KNOWHERE_SOURCE_DIR)
  if(NOT IS_DIRECTORY "${${source}}")
    message(FATAL_ERROR "${source} must identify an existing source directory")
  endif()
  file(REAL_PATH "${${source}}" ${source})
endforeach()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_BUILD_RPATH_USE_ORIGIN ON)
if(APPLE)
  set(milvus_loader_path "@loader_path")
  # Folly's async I/O, and therefore libaio, is Linux-only.
  set(storage_platform_libraries "")
else()
  set(milvus_loader_path "$ORIGIN")
  set(storage_platform_libraries aio)
endif()
set(CMAKE_INSTALL_RPATH "${milvus_loader_path}")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH OFF)

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
  set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
  set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
endif()

enable_testing()
include(cmake/Storage.cmake)
include(cmake/Knowhere.cmake)
milvus_add_storage()
milvus_add_knowhere()
include(cmake/Install.cmake)
