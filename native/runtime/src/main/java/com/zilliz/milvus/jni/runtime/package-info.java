/**
 * Verifies and extracts the shared native bundle against per-component binding contracts
 * before a JNI loader opens its entry library. Extraction is shared, but every binding
 * lookup revalidates the contracts visible to that binding's own class loader, including
 * contracts added after the first extraction. This package does not load native code.
 * Only an absent bundle yields empty; an unsupported platform or an incomplete
 * bundle fails.
 */
package com.zilliz.milvus.jni.runtime;
