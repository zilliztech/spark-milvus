# Build the pinned Knowhere sources without executing their CMake files.
include_guard(GLOBAL)

function(milvus_knowhere_compile target)
    target_compile_features(${target} PRIVATE cxx_std_20)
    set_target_properties(${target} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    target_compile_options(${target} PRIVATE
        "$<$<COMPILE_LANGUAGE:CXX>:-Wall;${MILVUS_KNOWHERE_BASELINE_OPTIONS}>")
    target_compile_definitions(${target} PRIVATE
        NOT_COMPILE_FOR_SWIG OPENTELEMETRY_STL_VERSION=2017
        ${MILVUS_KNOWHERE_DISKANN_DEFINITION})
    if(WITH_CARDINAL)
        target_compile_definitions(${target} PRIVATE KNOWHERE_WITH_CARDINAL)
    endif()
    if(TARGET opentelemetry-cpp::opentelemetry_exporter_otlp_grpc)
        target_compile_definitions(${target} PRIVATE HAVE_OTLP_GRPC_EXPORTER)
    endif()
    target_include_directories(${target} PRIVATE
        "${KNOWHERE_SOURCE_DIR}/include"
        "${KNOWHERE_SOURCE_DIR}/src"
        "${KNOWHERE_SOURCE_DIR}/thirdparty/hnswlib"
        "${KNOWHERE_SOURCE_DIR}/thirdparty/faiss"
        "${KNOWHERE_SOURCE_DIR}/thirdparty/DiskANN/include"
        ${Boost_INCLUDE_DIRS} ${folly_INCLUDE_DIRS}
        ${xxHash_INCLUDE_DIRS} ${opentelemetry-cpp_INCLUDE_DIRS})
    target_link_libraries(${target} PRIVATE OpenMP::OpenMP_CXX)
endfunction()

function(milvus_add_knowhere)
    include(CheckSymbolExists)
    check_symbol_exists(__x86_64__ "" MILVUS_KNOWHERE_X86_64)
    check_symbol_exists(__aarch64__ "" MILVUS_KNOWHERE_AARCH64)
    if(NOT MILVUS_KNOWHERE_X86_64 AND NOT MILVUS_KNOWHERE_AARCH64)
        message(FATAL_ERROR "Knowhere is built for x86_64 and aarch64, not ${CMAKE_SYSTEM_PROCESSOR}")
    endif()

    # NEON is the aarch64 baseline and needs no flag; SSE 4.2 is not (upstream
    # compile_flags.cmake 19-24). DiskANN's only aligned reader is built on
    # libaio, so it is a Linux target (upstream CMakeLists 45, 189).
    if(MILVUS_KNOWHERE_X86_64)
        set(MILVUS_KNOWHERE_BASELINE_OPTIONS "-msse4.2")
    else()
        set(MILVUS_KNOWHERE_BASELINE_OPTIONS "")
    endif()
    if(APPLE)
        set(MILVUS_KNOWHERE_WITH_DISKANN OFF)
        set(MILVUS_KNOWHERE_DISKANN_DEFINITION "")
        set(MILVUS_KNOWHERE_DISKANN_TARGET "")
    else()
        set(MILVUS_KNOWHERE_WITH_DISKANN ON)
        set(MILVUS_KNOWHERE_DISKANN_DEFINITION KNOWHERE_WITH_DISKANN)
        set(MILVUS_KNOWHERE_DISKANN_TARGET diskann)
    endif()

    # Apple Clang ships no OpenMP runtime; knowhere's own conanfile points CMake
    # at Homebrew libomp the same way (knowhere/conanfile.py 257-277).
    if(APPLE AND NOT OpenMP_CXX_FLAGS)
        execute_process(COMMAND brew --prefix libomp
            OUTPUT_VARIABLE milvus_libomp OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET)
        foreach(candidate "${milvus_libomp}" /opt/homebrew/opt/libomp /usr/local/opt/libomp)
            if(candidate AND EXISTS "${candidate}/lib/libomp.dylib")
                set(milvus_libomp "${candidate}")
                break()
            endif()
        endforeach()
        if(NOT EXISTS "${milvus_libomp}/lib/libomp.dylib")
            message(FATAL_ERROR "Apple Clang has no OpenMP; install libomp (brew install libomp)")
        endif()
        set(OpenMP_C_FLAGS "-Xpreprocessor -fopenmp -I${milvus_libomp}/include")
        set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I${milvus_libomp}/include")
        set(OpenMP_C_LIB_NAMES "omp")
        set(OpenMP_CXX_LIB_NAMES "omp")
        set(OpenMP_omp_LIBRARY "${milvus_libomp}/lib/libomp.dylib")
    endif()

    set(knowhere_packages Boost OpenMP folly nlohmann_json glog prometheus-cpp fmt
                          xxHash simde opentelemetry-cpp milvus-common double-conversion)
    foreach(package IN LISTS knowhere_packages)
        find_package(${package} REQUIRED)
    endforeach()

    # faiss needs BLAS and LAPACK. Upstream takes Accelerate on Apple and the
    # OpenBLAS package elsewhere (upstream libfaiss.cmake 360-368); LAPACK is
    # resolved first because FindLAPACK consumes BLAS_LIBRARIES.
    if(APPLE)
        set(BLA_VENDOR Apple)
        find_package(LAPACK REQUIRED)
        find_package(BLAS REQUIRED)
    else()
        find_package(OpenBLAS REQUIRED)
        set(BLAS_LIBRARIES OpenBLAS::OpenBLAS)
        set(LAPACK_LIBRARIES OpenBLAS::OpenBLAS)
    endif()
    find_package(Threads REQUIRED)
    find_package(Java 11 REQUIRED COMPONENTS Development)
    find_package(JNI REQUIRED COMPONENTS JVM)
    if(NOT APPLE)
        find_library(MILVUS_KNOWHERE_AIO_LIBRARY NAMES aio REQUIRED)
    endif()
    include(CheckCXXCompilerFlag)
    include("${CMAKE_CURRENT_FUNCTION_LIST_DIR}/knowhere/Sources.cmake")

  if(MILVUS_KNOWHERE_X86_64)
    # Separate SIMD objects preserve the upstream runtime dispatch. The baseline
    # Faiss translation units must never inherit AVX2 or AVX512 compiler flags.
    foreach(target utils_sse utils_avx utils_avx512 utils_avx512icx sparse_simd_avx512)
        string(TOUPPER "${target}" source_group)
        add_library(${target} OBJECT ${MILVUS_${source_group}_SOURCES})
        milvus_knowhere_compile(${target})
    endforeach()
    target_compile_options(utils_sse PRIVATE -mpopcnt)
    target_compile_options(utils_avx PRIVATE -mfma -mf16c -mavx2 -mpopcnt)
    target_compile_options(utils_avx512 PRIVATE
        -mfma -mf16c -mavx512f -mavx512dq -mavx512bw -mpopcnt -mavx512vl)
    target_compile_options(utils_avx512icx PRIVATE
        -mfma -mf16c -mavx512f -mavx512dq -mavx512bw -mpopcnt -mavx512vl
        -mavx512vpopcntdq)
    target_compile_options(sparse_simd_avx512 PRIVATE -mavx512f -mavx512dq)
    target_link_libraries(sparse_simd_avx512 PRIVATE milvus-common::milvus-common)

    add_library(knowhere_utils STATIC ${MILVUS_KNOWHERE_UTILS_SOURCES}
        $<TARGET_OBJECTS:utils_sse> $<TARGET_OBJECTS:utils_avx>
        $<TARGET_OBJECTS:utils_avx512> $<TARGET_OBJECTS:utils_avx512icx>
        $<TARGET_OBJECTS:sparse_simd_avx512>)
    milvus_knowhere_compile(knowhere_utils)
    target_link_libraries(knowhere_utils PUBLIC
        glog::glog xxHash::xxhash milvus-common::milvus-common)

    foreach(target faiss_avx2 faiss_avx512)
        string(TOUPPER "${target}" source_group)
        set(group_sources ${MILVUS_${source_group}_SOURCES})
        if(target STREQUAL faiss_avx2)
            list(APPEND group_sources ${MILVUS_FAISS_FASTSCAN_SOURCES})
        endif()
        add_library(${target} OBJECT ${group_sources})
        milvus_knowhere_compile(${target})
        target_compile_definitions(${target} PRIVATE COMPILE_SIMD_AVX2)
        target_compile_options(${target} PRIVATE -mavx2 -mfma -mf16c -mpopcnt)
        target_link_libraries(${target} PRIVATE milvus-common::milvus-common)
    endforeach()
    target_compile_options(faiss_avx512 PRIVATE
        -mavx512f -mavx512dq -mavx512bw -mavx512vl)
    target_compile_definitions(faiss_avx512 PRIVATE COMPILE_SIMD_AVX512)

    set(spr_supported TRUE)
    foreach(extension avx512vpopcntdq avx512vnni avx512fp16 avx512bf16)
        string(TOUPPER "${extension}" flag_name)
        check_cxx_compiler_flag("-m${extension}" "MILVUS_HAS_${flag_name}")
        if(NOT MILVUS_HAS_${flag_name})
            set(spr_supported FALSE)
        endif()
    endforeach()
    if(spr_supported)
        add_library(faiss_avx512_spr OBJECT ${MILVUS_FAISS_AVX512_SPR_SOURCES})
        milvus_knowhere_compile(faiss_avx512_spr)
        target_compile_options(faiss_avx512_spr PRIVATE
            -mavx2 -mfma -mf16c -mpopcnt -mavx512f -mavx512cd -mavx512vl
            -mavx512dq -mavx512bw -mavx512vpopcntdq -mavx512vnni
            -mavx512fp16 -mavx512bf16)
        target_compile_definitions(faiss_avx512_spr PRIVATE
            COMPILE_SIMD_AVX2 COMPILE_SIMD_AVX512 COMPILE_SIMD_AVX512_SPR)
        target_link_libraries(faiss_avx512_spr PRIVATE milvus-common::milvus-common)
    endif()

    add_library(faiss STATIC ${MILVUS_FAISS_SOURCES}
        $<TARGET_OBJECTS:faiss_avx2> $<TARGET_OBJECTS:faiss_avx512>)
    milvus_knowhere_compile(faiss)
    target_compile_options(faiss PRIVATE
        -mpopcnt -mno-avx -mno-avx2 -Wno-sign-compare -Wno-unused-variable
        -Wno-reorder -Wno-unused-local-typedefs -Wno-unused-function -Wno-strict-aliasing)
    target_compile_definitions(faiss PRIVATE
        FINTEGER=int FAISS_ENABLE_DD COMPILE_SIMD_AVX2 COMPILE_SIMD_AVX512)
    if(spr_supported)
        target_sources(faiss PRIVATE $<TARGET_OBJECTS:faiss_avx512_spr>)
        target_compile_definitions(faiss PRIVATE COMPILE_SIMD_AVX512_SPR)
    endif()
  else()
    # aarch64: NEON is the baseline, so there are no SIMD object libraries and
    # no per-file -march. Upstream disables SVE on Apple outright, and the SVE
    # translation units then compile to nothing (libfaiss.cmake 262-329,
    # 500-554).
    add_library(knowhere_utils STATIC ${MILVUS_KNOWHERE_UTILS_SOURCES}
        ${MILVUS_UTILS_NEON_SOURCES})
    milvus_knowhere_compile(knowhere_utils)
    target_compile_options(knowhere_utils PRIVATE -march=armv8-a)
    target_link_libraries(knowhere_utils PUBLIC
        glog::glog xxHash::xxhash milvus-common::milvus-common)

    add_library(faiss STATIC ${MILVUS_FAISS_SOURCES}
        ${MILVUS_FAISS_NEON_SOURCES} ${MILVUS_FAISS_FASTSCAN_SOURCES})
    milvus_knowhere_compile(faiss)
    target_compile_options(faiss PRIVATE
        -Wno-sign-compare -Wno-unused-variable -Wno-reorder
        -Wno-unused-local-typedefs -Wno-unused-function -Wno-strict-aliasing)
    target_compile_definitions(faiss PRIVATE
        FINTEGER=int FAISS_ENABLE_DD COMPILE_SIMD_ARM_NEON)
  endif()
    target_link_libraries(faiss PUBLIC OpenMP::OpenMP_CXX
        ${LAPACK_LIBRARIES} ${BLAS_LIBRARIES} knowhere_utils)

  if(MILVUS_KNOWHERE_WITH_DISKANN)
    add_library(diskann STATIC ${MILVUS_DISKANN_SOURCES})
    milvus_knowhere_compile(diskann)
    target_compile_options(diskann PRIVATE
        -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free)
    target_link_libraries(diskann PUBLIC
        "${MILVUS_KNOWHERE_AIO_LIBRARY}" nlohmann_json::nlohmann_json
        milvus-common::milvus-common Folly::folly fmt::fmt
        prometheus-cpp::core prometheus-cpp::push glog::glog)
    target_include_directories(diskann PRIVATE ${double-conversion_INCLUDE_DIRS})
  endif()

  # Upstream removes the DiskANN index sources when the option is off
  # (upstream CMakeLists 189-194).
  set(knowhere_sources ${MILVUS_KNOWHERE_SOURCES})
  if(NOT MILVUS_KNOWHERE_WITH_DISKANN)
    list(FILTER knowhere_sources EXCLUDE REGEX "/src/index/diskann/")
  endif()

    add_library(knowhere SHARED ${knowhere_sources})
    milvus_knowhere_compile(knowhere)
    target_include_directories(knowhere PUBLIC "${KNOWHERE_SOURCE_DIR}/include")
    target_link_libraries(knowhere PUBLIC
        Boost::boost faiss ${MILVUS_KNOWHERE_DISKANN_TARGET}
        glog::glog nlohmann_json::nlohmann_json
        prometheus-cpp::core prometheus-cpp::push fmt::fmt Folly::folly
        milvus-common::milvus-common simde::simde
        opentelemetry-cpp::opentelemetry_trace)
  if(MILVUS_KNOWHERE_X86_64)
    set_property(SOURCE
        "${KNOWHERE_SOURCE_DIR}/src/index/sparse/sindi_simd_avx2.cc"
        APPEND PROPERTY COMPILE_OPTIONS -mavx2 -mfma -mf16c)
    set_property(SOURCE
        "${KNOWHERE_SOURCE_DIR}/src/index/sparse/sindi_simd_avx512.cc"
        APPEND PROPERTY COMPILE_OPTIONS
        -mavx512f -mavx512bw -mavx512vl -mavx512dq -mavx512cd -mavx2 -mfma -mf16c)
    set_property(SOURCE
        "${KNOWHERE_SOURCE_DIR}/src/index/sparse/codec/varintdecode.c"
        "${KNOWHERE_SOURCE_DIR}/src/index/sparse/codec/streamvbyte_0124_decode.c"
        "${KNOWHERE_SOURCE_DIR}/src/index/sparse/codec/streamvbyte_0124_encode.c"
        APPEND PROPERTY COMPILE_OPTIONS -msse4.1)
  endif()

    if(WITH_CARDINAL)
        include("${CMAKE_CURRENT_FUNCTION_LIST_DIR}/Cardinal.cmake")
        milvus_add_cardinal()
        target_compile_definitions(knowhere PRIVATE
            SPARSE_INDEX_VERSION_USE_RAW_DATA_THRESHOLD=7
            SPARSE_INDEX_VERSION_SUPPORT_FP16_QUANT_FOR_IP=0)
        # Cardinal registers its implementations during shared-library loading.
        # Keep these two DT_NEEDED entries even without a direct symbol reference.
        if(APPLE)
            target_link_libraries(knowhere PRIVATE cardinalv1 cardinalv2)
        else()
            target_link_libraries(knowhere PRIVATE
                "-Wl,--no-as-needed" cardinalv1 cardinalv2 "-Wl,--as-needed")
        endif()
    endif()

    add_library(knowhere_c SHARED "${KNOWHERE_SOURCE_DIR}/src/c_api/c_api.cc")
    milvus_knowhere_compile(knowhere_c)
    target_include_directories(knowhere_c PUBLIC "${KNOWHERE_SOURCE_DIR}/include")
    target_link_libraries(knowhere_c PRIVATE knowhere)
    target_compile_definitions(knowhere_c PRIVATE KNOWHERE_C_API_BUILD)
    set_target_properties(knowhere_c PROPERTIES VERSION 1.0.0 SOVERSION 1
        CXX_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES)

    set(jni_generated "${CMAKE_CURRENT_BINARY_DIR}/knowhere-java/generated")
    set(java_classes "${CMAKE_CURRENT_BINARY_DIR}/knowhere-java/classes")
    set(jni_header "${jni_generated}/io_knowhere_NativeBindings.h")
    file(GLOB java_sources CONFIGURE_DEPENDS
        "${KNOWHERE_SOURCE_DIR}/java/src/main/java/io/knowhere/*.java")
    add_custom_command(OUTPUT "${jni_header}"
        COMMAND "${CMAKE_COMMAND}" -E make_directory "${jni_generated}" "${java_classes}"
        COMMAND "${Java_JAVAC_EXECUTABLE}" --release 11 -h "${jni_generated}"
                -d "${java_classes}" ${java_sources}
        DEPENDS ${java_sources} VERBATIM)
    add_custom_target(knowhere_jni_headers DEPENDS "${jni_header}")
    add_library(knowhere_jni SHARED
        "${KNOWHERE_SOURCE_DIR}/java/src/main/cpp/knowhere_jni.cc" "${jni_header}")
    add_dependencies(knowhere_jni knowhere_jni_headers)
    target_include_directories(knowhere_jni PRIVATE
        "${jni_generated}" ${JNI_INCLUDE_DIRS} "${KNOWHERE_SOURCE_DIR}/include")
    target_link_libraries(knowhere_jni PRIVATE knowhere_c)
    set_target_properties(knowhere_jni PROPERTIES
        CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON
        CXX_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES)

    # Exercise the upstream general and concurrency C API contracts unchanged.
    foreach(test_case c_api concurrency)
        if(test_case STREQUAL "c_api")
            set(test_name knowhere_c_api)
            set(test_source test_c_api.c)
        else()
            set(test_name "knowhere_c_api_${test_case}")
            set(test_source "test_${test_case}.c")
        endif()
        add_executable("${test_name}_test" "${KNOWHERE_SOURCE_DIR}/tests/c_api/${test_source}")
        target_link_libraries("${test_name}_test" PRIVATE knowhere_c Threads::Threads)
        add_test(NAME "${test_name}" COMMAND "${test_name}_test")
    endforeach()

    # Cardinal needs an explicit unquantized refinement mode for exact-distance
    # assertions. Keep that acceptance contract here instead of patching Knowhere.
    # The fixture exercises DiskANN, so it exists only where DiskANN is built.
    if(MILVUS_KNOWHERE_WITH_DISKANN)
        add_executable(knowhere_c_api_diskann_acceptance_test
            "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tests/knowhere/diskann_c_api_acceptance.c")
        target_link_libraries(knowhere_c_api_diskann_acceptance_test PRIVATE knowhere_c)
        add_test(NAME knowhere_c_api_diskann_acceptance COMMAND knowhere_c_api_diskann_acceptance_test)
        set_tests_properties(knowhere_c_api_diskann_acceptance PROPERTIES TIMEOUT 300)
    endif()
endfunction()
