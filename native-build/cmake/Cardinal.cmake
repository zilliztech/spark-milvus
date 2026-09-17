# Targets for the Cardinal revisions paired with the pinned Knowhere source.
include_guard(GLOBAL)

function(milvus_add_cardinal)
    include("${CMAKE_CURRENT_FUNCTION_LIST_DIR}/cardinal/Sources.cmake")
    foreach(version 1 2)
        set(target "cardinalv${version}")
        set(cardinal_root "${KNOWHERE_SOURCE_DIR}/thirdparty/${target}")
        if(NOT IS_DIRECTORY "${cardinal_root}/know")
            message(FATAL_ERROR "Missing pinned Cardinal source directory: ${cardinal_root}")
        endif()
        add_library(${target} SHARED ${MILVUS_CARDINALV${version}_SOURCES})
        milvus_knowhere_compile(${target})
        target_compile_definitions(${target} PRIVATE
            ENABLE_CARDINAL_DISKANN ENABLE_COMPILE_PRUNE CARDINAL_WITH_OPENBLAS)
        # Keep the existing host-specific Cardinal baseline. The bundle records
        # this CPU requirement; the platform name alone is not an ISA guarantee.
        target_compile_options(${target} PRIVATE -march=native -mprefer-vector-width=512)
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

        set(smalltopk "${cardinal_root}/third_party/smalltopk/smalltopk/x86")
        set_property(SOURCE
            "${smalltopk}/avx512_sorting_fp32hack.cpp"
            "${smalltopk}/avx512_sorting_fp32hack_approx.cpp"
            "${smalltopk}/avx512_getmink_fp32.cpp"
            "${smalltopk}/avx512_getmink_fp32hack.cpp"
            APPEND PROPERTY COMPILE_OPTIONS
            -mavx512f -mavx512bw -mavx512vl -mavx512dq -mavx512cd -ffast-math)
    endforeach()

    set(cardinal_v2 "${KNOWHERE_SOURCE_DIR}/thirdparty/cardinalv2")
    set_property(SOURCE
        "${cardinal_v2}/third_party/kmeans_fast/blas_sq8_avx512.cpp"
        "${cardinal_v2}/third_party/pipnn/sq8_avx512_dataset.cpp"
        "${cardinal_v2}/third_party/pipnn/sq8_dataset.cpp"
        "${cardinal_v2}/third_party/pipnn/sq8_dp_dataset.cpp"
        "${cardinal_v2}/third_party/pipnn/sq8_ads_dataset.cpp"
        "${cardinal_v2}/third_party/pipnn/sq8_ads_dp_dataset.cpp"
        APPEND PROPERTY COMPILE_OPTIONS
        -mavx512f -mavx512bw -mavx512vl -mavx512dq -mavx512cd -mavx512vnni -ffast-math)

    # Plugins call the parent Knowhere API. Deliberately do not create a
    # circular link edge to knowhere; validate unresolved callbacks with the
    # completed parent library during the bundle's relocation checks.
endfunction()
