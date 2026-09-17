/**
 * Executor-side initialization and search through the upstream Knowhere binding.
 * NativeVectorLibrary reports the loaded C ABI and index format versions;
 * io.knowhere owns JNI, extraction and native resource lifetimes.
 *
 * NativeVectorSearch delegates synchronous brute-force search on borrowed buffers.
 *
 * NativeVectorIndex delegates BinarySet loading and persisted index search.
 * File-format interpretation and query planning belong to core and Spark.
 * Index building (W6) will delegate to the same upstream build and serialize
 * calls. Search and build thread-pool sizes will be set here once the upstream
 * C and Java API exposes them (design README section 5).
 * <p>Capabilities: V1, V5 (see docs/design/capabilities.md).
 */
package com.zilliz.milvus.jni.vector;
