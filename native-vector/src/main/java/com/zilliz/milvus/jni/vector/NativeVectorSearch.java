package com.zilliz.milvus.jni.vector;

import io.knowhere.DType;
import io.knowhere.Knowhere;
import java.nio.ByteBuffer;

/** Synchronous search over borrowed buffers using the upstream JNI. */
public final class NativeVectorSearch {
    private NativeVectorSearch() {}

    /**
     * Buffers remain owned by the caller and must remain valid until return.
     * One call answers every query in {@code queries}; the exclusion bitmap
     * covers all {@code rows} base rows and may be null when nothing is
     * excluded. Output holds {@code queryRows * topK} ids and distances.
     */
    public static void bruteForce(DType dtype, ByteBuffer vectors, long rows, ByteBuffer queries,
            long queryRows, int dimension, int topK, ByteBuffer excluded, ByteBuffer ids,
            ByteBuffer distances, String parameters) {
        NativeVectorLibrary.load();
        Knowhere.bruteForce(dtype, vectors, rows, queries, queryRows, dimension, topK,
                excluded, excluded == null ? 0L : rows, ids, distances, parameters);
    }
}
