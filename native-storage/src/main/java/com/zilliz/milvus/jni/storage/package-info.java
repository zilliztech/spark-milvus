/**
 * Hands the verified shared native entry to the upstream storage loader.
 * Native extraction is shared with vector JNI; System.load remains owned by
 * io.milvus.storage.
 * <p>Capabilities: R4 (see docs/design/capabilities.md).
 */
package com.zilliz.milvus.jni.storage;
