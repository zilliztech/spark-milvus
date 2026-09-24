/**
 * Hands the verified shared native entry to the upstream storage loader.
 * Native extraction is shared with vector JNI; System.load remains owned by
 * io.milvus.storage. Without a bundle the adapter calls the upstream loader
 * unchanged, which falls back to its own packaged resource and then the system
 * library path; an explicit {@code milvus.storage.native.path} that names
 * another file is rejected.
 * <p>Capabilities: R4 (see docs/design/capabilities.md).
 */
package com.zilliz.milvus.jni.storage;
