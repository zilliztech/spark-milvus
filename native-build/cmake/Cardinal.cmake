# Targets for the Cardinal revisions paired with the pinned Knowhere source.
include_guard(GLOBAL)

function(milvus_add_cardinal)
    include("${CMAKE_CURRENT_FUNCTION_LIST_DIR}/cardinal/Sources.cmake")
    # Cardinal's own CMakeLists.txt branches on CMAKE_SYSTEM_PROCESSOR: the
    # x86_64 block adds AVX-512 kernels, the arm block adds SVE kernels. The
    # same selector picks the per-architecture source list and flags here.
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)$")
        set(cardinal_arch X86_64)
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
        set(cardinal_arch AARCH64)
    else()
        message(FATAL_ERROR "Cardinal is built for x86_64 and aarch64, not ${CMAKE_SYSTEM_PROCESSOR}")
    endif()
    # Upstream compiles the SVE kernels with these flags on top of the global
    # -march=native (kmeans_fast, smalltopk and pipnn blocks of both tags).
    set(cardinal_sve_options -march=native -mtune=native -ffast-math)

    foreach(version 1 2)
        set(target "cardinalv${version}")
        set(cardinal_root "${KNOWHERE_SOURCE_DIR}/thirdparty/${target}")
        if(NOT IS_DIRECTORY "${cardinal_root}/know")
            message(FATAL_ERROR "Missing pinned Cardinal source directory: ${cardinal_root}")
        endif()
        add_library(${target} SHARED
            ${MILVUS_CARDINALV${version}_SOURCES}
            ${MILVUS_CARDINALV${version}_${cardinal_arch}_SOURCES})
        milvus_knowhere_compile(${target})
        target_compile_definitions(${target} PRIVATE
            ENABLE_CARDINAL_DISKANN ENABLE_COMPILE_PRUNE CARDINAL_WITH_OPENBLAS)
        # Keep the existing host-specific Cardinal baseline. The bundle records
        # this CPU requirement; the platform name alone is not an ISA guarantee.
        # -mprefer-vector-width is an x86 option; GCC on aarch64 rejects it.
        if(cardinal_arch STREQUAL "X86_64")
            target_compile_options(${target} PRIVATE -march=native -mprefer-vector-width=512)
        else()
            target_compile_options(${target} PRIVATE -march=native)
            # Upstream sets this on aarch64 so Cardinal's folly headers select
            # the same F14 intrinsics mode as the folly shared library it links.
            target_compile_definitions(${target} PRIVATE FOLLY_ARM_FEATURE_CRC32=0)
        endif()
        target_include_directories(${target} PRIVATE
            "${cardinal_root}" "${cardinal_root}/include"
            "${cardinal_root}/third_party/smalltopk")
        if(version EQUAL 1)
            target_include_directories(${target} PRIVATE
                "${cardinal_root}/src" "${cardinal_root}/third_party/helpa"
                "${cardinal_root}/third_party/rabitq")
        endif()
        target_link_libraries(${target} PRIVATE
            glog::glog fmt::fmt Folly::folly
            opentelemetry-cpp::opentelemetry-cpp
            prometheus-cpp::prometheus-cpp nlohmann_json::nlohmann_json
            simde::simde milvus-common::milvus-common OpenBLAS::OpenBLAS
            OpenMP::OpenMP_CXX "${MILVUS_KNOWHERE_AIO_LIBRARY}")

        set(smalltopk "${cardinal_root}/third_party/smalltopk/smalltopk")
        if(cardinal_arch STREQUAL "X86_64")
            set_property(SOURCE
                "${smalltopk}/x86/avx512_sorting_fp32hack.cpp"
                "${smalltopk}/x86/avx512_sorting_fp32hack_approx.cpp"
                "${smalltopk}/x86/avx512_getmink_fp32.cpp"
                "${smalltopk}/x86/avx512_getmink_fp32hack.cpp"
                APPEND PROPERTY COMPILE_OPTIONS
                -mavx512f -mavx512bw -mavx512vl -mavx512dq -mavx512cd -ffast-math)
        elseif(version EQUAL 1)
            set_property(SOURCE
                "${smalltopk}/arm/sve_sorting_fp32hack.cpp"
                "${smalltopk}/arm/sve_sorting_fp32hack_approx.cpp"
                "${smalltopk}/arm/sve_getmink_fp32.cpp"
                "${smalltopk}/arm/sve_getmink_fp32hack.cpp"
                APPEND PROPERTY COMPILE_OPTIONS ${cardinal_sve_options})
        else()
            # v3.0.8 names arm_sorting_fp32hack.cpp, a file that does not exist;
            # the kernel it compiles is sve_sorting_fp32hack.cpp.
            set_property(SOURCE "${smalltopk}/arm/sve_sorting_fp32hack.cpp"
                APPEND PROPERTY COMPILE_OPTIONS ${cardinal_sve_options})
        endif()
    endforeach()

    set(cardinal_v2 "${KNOWHERE_SOURCE_DIR}/thirdparty/cardinalv2")
    if(cardinal_arch STREQUAL "X86_64")
        set_property(SOURCE
            "${cardinal_v2}/third_party/kmeans_fast/blas_sq8_avx512.cpp"
            "${cardinal_v2}/third_party/pipnn/sq8_avx512_dataset.cpp"
            "${cardinal_v2}/third_party/pipnn/sq8_dataset.cpp"
            "${cardinal_v2}/third_party/pipnn/sq8_dp_dataset.cpp"
            "${cardinal_v2}/third_party/pipnn/sq8_ads_dataset.cpp"
            "${cardinal_v2}/third_party/pipnn/sq8_ads_dp_dataset.cpp"
            APPEND PROPERTY COMPILE_OPTIONS
            -mavx512f -mavx512bw -mavx512vl -mavx512dq -mavx512cd -mavx512vnni -ffast-math)
    else()
        set_property(SOURCE
            "${cardinal_v2}/third_party/kmeans_fast/blas_sq8_sve.cpp"
            "${cardinal_v2}/third_party/kmeans_fast/blas_sq8_mmla_sve.cpp"
            "${cardinal_v2}/third_party/pipnn/sq8_dataset.cpp"
            "${cardinal_v2}/third_party/pipnn/sq8_dp_dataset.cpp"
            "${cardinal_v2}/third_party/pipnn/sq8_ads_dataset.cpp"
            "${cardinal_v2}/third_party/pipnn/sq8_ads_dp_dataset.cpp"
            "${cardinal_v2}/third_party/pipnn/sq8_mmla_dataset.cpp"
            APPEND PROPERTY COMPILE_OPTIONS ${cardinal_sve_options})
    endif()

    # Plugins call the parent Knowhere API. Deliberately do not create a
    # circular link edge to knowhere; validate unresolved callbacks with the
    # completed parent library during the bundle's relocation checks.
endfunction()
