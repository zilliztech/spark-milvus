package com.zilliz.milvus.jni.vector;

import io.knowhere.DType;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Explicit native smoke: requires the real platform JAR and pre-JVM libjsig. */
public final class NativeVectorLibrarySmoke {
    public static void main(String[] args) throws Exception {
        String originalPath = System.getProperty("knowhere.native.path");
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
        if (!Objects.equals(originalPath, System.getProperty("knowhere.native.path"))) {
            throw new AssertionError("Successful initialization changed the application's native path override");
        }
        // An actual JNI operation verifies more than a successful System.load.
        Map<String, byte[]> sample = fixture();
        Properties metadata = new Properties();
        metadata.load(new ByteArrayInputStream(sample.get("index.properties")));
        int version = Integer.parseInt(metadata.getProperty("version"));
        byte[] payload = Objects.requireNonNull(sample.get("FLAT"), "FLAT fixture payload");
        ByteBuffer bytes = ByteBuffer.allocateDirect(payload.length);
        bytes.put(payload).flip();
        ByteBuffer query = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder());
        ByteBuffer ids = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder());
        ByteBuffer scores = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());
        try (NativeVectorIndex.Loader loader = new NativeVectorIndex.Loader(version, 2, 2,
                "{\"metric_type\":\"L2\"}")) {
            loader.allocate("FLAT", payload.length);
            loader.write("FLAT", 0L, bytes);
            try (NativeVectorIndex index = loader.load("FLAT", DType.FLOAT32)) {
                index.search(query, 1, 1, null, ids, scores, "{\"metric_type\":\"L2\"}");
                if (index.rows() != 2 || index.dimension() != 2
                    || ids.getLong(0) != 0 || scores.getFloat(0) != 0f) {
                    throw new AssertionError("Incorrect result from loaded JNI library");
                }
            }
        }
        System.out.println("PASS: " + info + "; concurrent initialization and native search");
    }
    private static Map<String, byte[]> fixture() throws Exception {
        String prefix = "/read-fixtures/vector-flat/";
        String manifest = new String(resource(prefix + "SHA256SUMS"), StandardCharsets.UTF_8);
        Map<String, byte[]> result = new HashMap<>();
        for (String line : manifest.split("\n")) {
            if (line.isEmpty()) continue;
            String[] entry = line.split("  ", 2);
            if (entry.length != 2 || !entry[0].matches("[0-9a-f]{64}")
                    || !entry[1].matches("[A-Za-z0-9._-]+")
                    || entry[1].equals(".") || entry[1].equals("..")) {
                throw new AssertionError("Invalid vector fixture checksum entry: " + line);
            }
            byte[] bytes = resource(prefix + entry[1]);
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder actual = new StringBuilder();
            for (byte value : digest) actual.append(String.format("%02x", value & 0xff));
            if (!entry[0].contentEquals(actual) || result.put(entry[1], bytes) != null) {
                throw new AssertionError("Changed or repeated vector fixture: " + entry[1]);
            }
        }
        return result;
    }

    private static byte[] resource(String path) throws Exception {
        try (InputStream input = NativeVectorLibrarySmoke.class.getResourceAsStream(path)) {
            if (input == null) throw new AssertionError("Missing vector fixture: " + path);
            return input.readAllBytes();
        }
    }

}
