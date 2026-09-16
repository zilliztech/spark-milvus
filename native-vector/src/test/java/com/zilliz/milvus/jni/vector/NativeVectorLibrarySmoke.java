package com.zilliz.milvus.jni.vector;

import io.knowhere.DType;
import io.knowhere.Knowhere;
import io.knowhere.KnowhereIndex;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Explicit native smoke: requires the real platform JAR and pre-JVM libjsig. */
public final class NativeVectorLibrarySmoke {
    public static void main(String[] args) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        NativeVectorLibrary.RuntimeInfo info;
        try {
            List<Callable<NativeVectorLibrary.RuntimeInfo>> calls = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                calls.add(NativeVectorLibrary::load);
            }
            List<Future<NativeVectorLibrary.RuntimeInfo>> results = pool.invokeAll(calls);
            info = results.get(0).get();
            for (Future<NativeVectorLibrary.RuntimeInfo> result : results) {
                if (result.get() != info) {
                    throw new AssertionError("Concurrent loads did not return the same runtime");
                }
            }
        } finally {
            pool.shutdownNow();
        }
        if (info.cAbiVersion() != 1) {
            throw new AssertionError("Unexpected C ABI: " + info);
        }
        // An actual JNI operation verifies more than a successful System.load.
        ByteBuffer vectors = ByteBuffer.allocateDirect(16).order(ByteOrder.nativeOrder());
        vectors.putFloat(0).putFloat(0).putFloat(1).putFloat(0).flip();
        ByteBuffer query = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder());
        ByteBuffer ids = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder());
        ByteBuffer scores = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());
        try (KnowhereIndex index = Knowhere.createIndex("FLAT", DType.FLOAT32, info.currentIndexVersion())) {
            index.build(vectors, 2, 2, "{\"metric_type\":\"L2\"}");
            index.search(query, 1, 2, 1, null, 0, ids, scores, "{\"metric_type\":\"L2\"}");
            if (index.rows() != 2 || index.dimensions() != 2
                    || ids.getLong(0) != 0 || scores.getFloat(0) != 0f) {
                throw new AssertionError("Incorrect result from loaded JNI library");
            }
        }
        System.out.println("PASS: " + info + "; concurrent initialization and native search");
    }
}
