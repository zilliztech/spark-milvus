/**
 * Verifies and extracts the shared native bundle before either upstream JNI
 * loader opens its entry library. This package does not load native code.
 * Only an absent bundle yields empty; an unsupported platform or an incomplete
 * bundle fails.
 */
package com.zilliz.milvus.jni.runtime;
