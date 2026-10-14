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

    /**
     * The batched entry for FLOAT32 vectors without an exclusion bitmap: every query goes
     * to the bundled faiss in one call, which takes its BLAS path, and the call runs
     * single-threaded on the calling thread (decision 27). The caller excludes rows by
     * leaving them out of {@code vectors}. Buffers and output follow {@link #bruteForce}.
     */
    public static void bruteForceBatched(DType dtype, ByteBuffer vectors, long rows,
            ByteBuffer queries, long queryRows, int dimension, int topK, ByteBuffer ids,
            ByteBuffer distances, String parameters) {
        NativeVectorLibrary.load();
        Knowhere.bruteForceBatched(dtype, vectors, rows, queries, queryRows, dimension, topK,
                ids, distances, parameters);
    }

    /**
     * {@link #bruteForceBatched} for L2, followed by a direct pass over its candidates
     * (decision 37): the ids and the estimated distances are written as there, and then,
     * for each query, the candidates whose estimate is below the query's float32 cutoff,
     * taken in order from the first, receive their squared distance computed pair by pair
     * in the same slot of {@code direct}; {@code counts} receives one int32 per query with
     * how many were computed. A null {@code cutoffs} computes every candidate. Buffers
     * follow {@link #bruteForce}.
     */
    public static void bruteForceBatchedL2Rescored(DType dtype, ByteBuffer vectors, long rows,
            ByteBuffer queries, long queryRows, int dimension, int topK, ByteBuffer cutoffs,
            ByteBuffer ids, ByteBuffer distances, ByteBuffer direct, ByteBuffer counts,
            String parameters) {
        NativeVectorLibrary.load();
        Knowhere.bruteForceBatchedL2Rescored(dtype, vectors, rows, queries, queryRows, dimension,
                topK, cutoffs, ids, distances, direct, counts, parameters);
    }
}
