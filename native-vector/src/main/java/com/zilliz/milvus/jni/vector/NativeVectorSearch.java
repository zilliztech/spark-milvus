package com.zilliz.milvus.jni.vector;

import io.knowhere.DType;
import io.knowhere.Knowhere;
import java.nio.ByteBuffer;

/** Synchronous search over borrowed float32 buffers using the upstream JNI. */
public final class NativeVectorSearch {
    private NativeVectorSearch() {}

    /** Buffers remain owned by the caller and must remain valid until return. */
    public static void bruteForce(ByteBuffer vectors, int rows, ByteBuffer query,
            int dimension, int topK, ByteBuffer excluded, ByteBuffer ids,
            ByteBuffer distances, String parameters) {
        NativeVectorLibrary.load();
        Knowhere.bruteForce(DType.FLOAT32, vectors, rows, query, 1, dimension, topK,
                excluded, rows, ids, distances, parameters);
    }
}
