# Copyright 2026 Zilliz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_guard(GLOBAL)

function(milvus_add_storage)
  set(storage_cpp "${MILVUS_STORAGE_SOURCE_DIR}/cpp")
  set(storage_rust "${storage_cpp}/src/format/bridge/rust")
  if(NOT EXISTS "${storage_rust}/Cargo.lock")
    message(FATAL_ERROR "MILVUS_STORAGE_SOURCE_DIR must contain the pinned storage sources and Cargo.lock")
  endif()

  foreach(dependency IN ITEMS Boost fmt folly glog prometheus-cpp Arrow Azure
      Protobuf nlohmann_json opentelemetry-cpp google-cloud-cpp CURL AWSSDK milvus-common)
    find_package(${dependency} REQUIRED CONFIG)
  endforeach()
  find_package(libavrocpp QUIET CONFIG)
  if(NOT libavrocpp_FOUND)
    find_package(avro-cpp REQUIRED CONFIG)
  endif()
  if(TARGET libavrocpp::libavrocpp)
    set(storage_avro libavrocpp::libavrocpp)
  elseif(TARGET avro-cpp::avrocpp)
    set(storage_avro avro-cpp::avrocpp)
  else()
    set(storage_avro avro-cpp::avrocpp_s)
  endif()
  find_package(JNI REQUIRED COMPONENTS JVM)

  # Corrosion is a build toolkit. Neither storage's top-level CMake nor its
  # Rust CMake is included; the targets below consume upstream source files.
  include(FetchContent)
  file(READ "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../dependencies.json" storage_dependencies)
  string(JSON storage_corrosion_repository GET "${storage_dependencies}" corrosion repository)
  string(JSON storage_corrosion_revision GET "${storage_dependencies}" corrosion revision)
  FetchContent_Declare(Corrosion
    GIT_REPOSITORY "${storage_corrosion_repository}"
    GIT_TAG "${storage_corrosion_revision}"
  )
  FetchContent_MakeAvailable(Corrosion)
  corrosion_import_crate(
    MANIFEST_PATH "${storage_rust}/Cargo.toml"
    CRATES rust-bridge
    LOCKED
  )
  corrosion_set_env_vars(rust_bridge
    "RUSTFLAGS=-C force-frame-pointers=yes"
    "OPENSSL_STATIC=0"
    "OPENSSL_NO_VENDOR=1"
  )
  if(DEFINED ENV{OPENSSL_DIR})
    corrosion_set_env_vars(rust_bridge "OPENSSL_DIR=$ENV{OPENSSL_DIR}")
  endif()
  corrosion_add_cxxbridge(rust-bridge CRATE rust_bridge FILES lib.rs)

  # The generator may itself require cargo install. Finish that before the
  # bridge's Cargo build so two independent Cargo job pools cannot overlap.
  get_property(storage_directory_targets DIRECTORY PROPERTY BUILDSYSTEM_TARGETS)
  foreach(storage_target IN LISTS storage_directory_targets)
    if(storage_target MATCHES "^cxxbridge_v")
      add_dependencies(cargo-prebuild_rust_bridge "${storage_target}")
    endif()
  endforeach()
  add_custom_target(milvus-storage-rust DEPENDS cargo-build_rust_bridge)

  set(storage_bridge_includes
    "${CMAKE_CURRENT_BINARY_DIR}/corrosion_generated/cxxbridge/rust-bridge/include"
    "${storage_rust}/include"
    "${storage_cpp}/include"
  )
  target_include_directories(rust-bridge PUBLIC ${storage_bridge_includes})
  target_link_libraries(rust-bridge PRIVATE arrow::arrow)

  file(GLOB_RECURSE storage_bridge_sources CONFIGURE_DEPENDS "${storage_rust}/src/*.cpp")
  list(FILTER storage_bridge_sources EXCLUDE REGEX "/talon_bridge\\.cpp$")
  add_library(prsbridge STATIC ${storage_bridge_sources})
  target_include_directories(prsbridge PUBLIC ${storage_bridge_includes})
  target_link_libraries(prsbridge PUBLIC rust-bridge PRIVATE arrow::arrow)
  target_include_directories(prsbridge PRIVATE
    $<TARGET_PROPERTY:milvus-common::milvus-common,INTERFACE_INCLUDE_DIRECTORIES>
  )

  file(GLOB_RECURSE storage_sources CONFIGURE_DEPENDS
    "${storage_cpp}/src/*.cpp"
    "${storage_cpp}/src/*.cc"
  )
  list(FILTER storage_sources EXCLUDE REGEX "/src/format/bridge/.*\\.(cpp|cc)$")
  list(FILTER storage_sources EXCLUDE REGEX "/src/jni/.*\\.cpp$")
  list(FILTER storage_sources EXCLUDE REGEX "/src/filesystem/talon/.*\\.cpp$")
  add_library(milvus-storage SHARED ${storage_sources})
  target_compile_features(milvus-storage PUBLIC cxx_std_20)
  target_include_directories(milvus-storage PUBLIC
    "${storage_cpp}/include"
    "${storage_cpp}/src"
    $<TARGET_PROPERTY:prsbridge,INTERFACE_INCLUDE_DIRECTORIES>
  )
  target_link_libraries(milvus-storage
    PUBLIC
      milvus-common::milvus-common
      arrow::arrow
      Boost::boost
      protobuf::protobuf
      nlohmann_json::nlohmann_json
      AWS::aws-sdk-cpp-identity-management
      google-cloud-cpp::storage
      ${storage_avro}
      Folly::folly
      fmt::fmt
      azure-sdk-for-cpp::azure-sdk-for-cpp
      glog::glog
      CURL::libcurl
      prometheus-cpp::core
      opentelemetry-cpp::opentelemetry_proto
      aio
    PRIVATE prsbridge
  )
  # The upstream Folly recipe publishes aio to consumers but does not put it
  # in libfolly's DT_NEEDED list. Keep the provider in this runtime root even
  # though --as-needed sees no unresolved aio reference in storage's objects.
  target_link_libraries(milvus-storage PRIVATE
    "-Wl,--no-as-needed" aio "-Wl,--as-needed")

  # Rust vendors these C implementations. Keep their symbols private while
  # retaining the public storage C, C++, and CXX bridge interfaces.
  set(storage_symbol_map "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/storage-private-symbols.map")
  target_link_options(milvus-storage PRIVATE
    "LINKER:--version-script=${storage_symbol_map}"
    "LINKER:-z,noexecstack"
  )
  set_property(TARGET milvus-storage APPEND PROPERTY LINK_DEPENDS "${storage_symbol_map}")

  file(GLOB storage_jni_sources CONFIGURE_DEPENDS "${storage_cpp}/src/jni/*.cpp")
  add_library(milvus-storage-jni SHARED ${storage_jni_sources})
  target_include_directories(milvus-storage-jni PRIVATE ${JNI_INCLUDE_DIRS})
  target_link_libraries(milvus-storage-jni PRIVATE milvus-storage arrow::arrow)
  target_link_options(milvus-storage-jni PRIVATE "LINKER:-z,noexecstack")

  set_target_properties(prsbridge rust-bridge milvus-storage milvus-storage-jni PROPERTIES
    POSITION_INDEPENDENT_CODE ON
  )
  set_target_properties(milvus-storage milvus-storage-jni PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    BUILD_RPATH "$ORIGIN"
    INSTALL_RPATH "$ORIGIN"
  )
endfunction()
