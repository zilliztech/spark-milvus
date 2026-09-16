/**
 * StorageNative: one native method per loon_* entry point. Handles are longs
 * and result codes are turned into exceptions. The batch reader's holder
 * counts the batches it hands over and the columns it had to copy, read back
 * through recordBatchReaderStats; that is layer 1's share of the metrics
 * (docs/design/architecture/storage-io.html section 5).
 *
 * <p>Capabilities: R4, G5 (see docs/design/capabilities.md).
 */
package com.zilliz.milvus.jni.storage;
