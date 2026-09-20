/**
 * Executor-side initialization, search and index building through the upstream
 * Knowhere binding. NativeVectorLibrary reports the loaded C ABI and index
 * format versions; io.knowhere owns JNI loading; native-runtime owns unified
 * resource extraction.
 *
 * NativeVectorSearch delegates synchronous brute-force search on borrowed buffers.
 *
 * NativeVectorIndex delegates BinarySet loading and persisted index search, and
 * NativeVectorIndex.build delegates index building and serialization (W6) to
 * the upstream KnowhereIndex.build and serialize calls. File-format
 * interpretation and query planning belong to core and Spark.
 *
 * The pinned io.knowhere.Knowhere also exposes resizeSearchThreadPool and
 * resizeBuildThreadPool; the connector does not call them until the G4 session
 * options exist. No GPU switch is exposed upstream.
 * <p>Capabilities: V1, V5, W6 (see docs/design/capabilities.md).
 */
package com.zilliz.milvus.jni.vector;
