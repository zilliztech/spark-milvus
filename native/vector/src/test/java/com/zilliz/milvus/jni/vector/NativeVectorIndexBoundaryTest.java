package com.zilliz.milvus.jni.vector;

import com.zilliz.milvus.jni.runtime.NativeLibraries;
import io.knowhere.Knowhere;
import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Public artifacts contain read/search APIs and no offline builder. */
public class NativeVectorIndexBoundaryTest {
    @Test
    public void publicClasspathContainsReadSearchButNoBuilder() throws Exception {
        String builderName = "com.zilliz.milvus.jni.vector.NativeVectorIndexBuilder";
        URL[] classpath = {
            NativeVectorIndex.class.getProtectionDomain().getCodeSource().getLocation(),
            Knowhere.class.getProtectionDomain().getCodeSource().getLocation(),
            NativeLibraries.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (URLClassLoader loader = new URLClassLoader(classpath, ClassLoader.getPlatformClassLoader())) {
            assertThrows(ClassNotFoundException.class, () -> loader.loadClass(builderName));
            assertThrows(ClassNotFoundException.class, () -> loader.loadClass(builderName + "$Built"));
            Class<?> index = Class.forName(NativeVectorIndex.class.getName(), true, loader);
            Class<?> payloadLoader = Class.forName(NativeVectorIndex.Loader.class.getName(), true, loader);
            assertEquals(index, payloadLoader.getMethod("load", String.class,
                    loader.loadClass("io.knowhere.DType")).getReturnType());
            assertEquals(void.class, payloadLoader.getMethod("write", String.class, long.class,
                    ByteBuffer.class).getReturnType());
            assertEquals(void.class, index.getMethod("search", ByteBuffer.class, long.class,
                    int.class, ByteBuffer.class, ByteBuffer.class, ByteBuffer.class,
                    String.class).getReturnType());
            Class<?> search = Class.forName(NativeVectorSearch.class.getName(), true, loader);
            assertEquals(void.class, search.getMethod("bruteForce", loader.loadClass("io.knowhere.DType"),
                    ByteBuffer.class, long.class, ByteBuffer.class, long.class, int.class,
                    int.class, ByteBuffer.class, ByteBuffer.class, ByteBuffer.class,
                    String.class).getReturnType());
        }
    }
}
