cmake_minimum_required(VERSION 3.20.0)

project(milvus-jni VERSION 0.1.0)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Enable position independent code for shared library
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Find Java and JNI
find_package(Java REQUIRED)
find_package(JNI REQUIRED)

# Find milvus-storage
find_path(MILVUS_STORAGE_INCLUDE_DIR 
    NAMES milvus-storage/storage/space.h
    PATHS ${CMAKE_CURRENT_SOURCE_DIR}/milvus-storage/cpp/include
    REQUIRED)

find_library(MILVUS_STORAGE_LIB
    NAMES milvus-storage
    PATHS ${CMAKE_CURRENT_SOURCE_DIR}/milvus-storage/cpp/build/Release
    NO_DEFAULT_PATH
    REQUIRED)

# Find Conan dependencies
find_package(Boost REQUIRED)
find_package(Arrow REQUIRED)
find_package(Protobuf REQUIRED)
find_package(glog REQUIRED)
find_package(google-cloud-cpp REQUIRED)

# Include directories
include_directories(${JNI_INCLUDE_DIRS})
include_directories(${MILVUS_STORAGE_INCLUDE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/milvus-storage/cpp/src)
include_directories(src/main/c/include)

# Source files
set(JNI_SOURCES src/main/c/native/milvus_storage_jni.cpp)

# Create shared library
add_library(milvus_storage_jni SHARED ${JNI_SOURCES})

# Link libraries
target_link_libraries(milvus_storage_jni 
    PRIVATE
    ${MILVUS_STORAGE_LIB}
    arrow::arrow
    Boost::boost
    protobuf::protobuf
    glog::glog
    google-cloud-cpp::storage
    dl
)

# 解决 TLS 内存分配问题
target_link_options(milvus_storage_jni PRIVATE
    -Wl,--no-as-needed
    -Wl,-z,lazy
)

# Set library properties
set_target_properties(milvus_storage_jni PROPERTIES
    OUTPUT_NAME "milvus_storage_jni"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/target/native"
)

# Install target
install(TARGETS milvus_storage_jni
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
) 