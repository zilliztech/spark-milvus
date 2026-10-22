/**
 * Executor-side initialization and search through the upstream
 * Knowhere binding. NativeVectorLibrary reports the loaded C ABI and index
 * format versions; io.knowhere owns JNI loading; native-runtime owns unified
 * resource extraction. Without a bundle the adapter calls the upstream loader
 * unchanged, which falls back to its own packaged resource and then the system
 * library path; an explicit {@code knowhere.native.path} that names another
 * file is rejected.
 *
 * NativeVectorSearch delegates the upstream synchronous brute-force entries
 * on borrowed buffers. Each method preserves the upstream name and documents
 * its element types, exclusions, output and thread behavior.
 *
 * NativeVectorIndex delegates BinarySet loading and persisted index search.
 * Private offline jobs call the upstream index build and serialize API. File-format
 * interpretation and query planning belong to core and Spark.
 *
 * The pinned io.knowhere.Knowhere also exposes resizeSearchThreadPool and
 * resizeBuildThreadPool; the connector does not call them until the G4 session
 * options exist. No GPU switch is exposed upstream.
 */
package com.zilliz.milvus.jni.vector;
