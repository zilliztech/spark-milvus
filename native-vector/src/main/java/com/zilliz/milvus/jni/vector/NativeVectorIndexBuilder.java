package com.zilliz.milvus.jni.vector;

import io.knowhere.BinarySet;
import io.knowhere.DType;
import io.knowhere.Knowhere;
import io.knowhere.KnowhereIndex;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Builds and serializes indexes through the upstream JNI for offline write jobs. */
public final class NativeVectorIndexBuilder {
    private NativeVectorIndexBuilder() {
    }

    /**
     * Builds an index over vectors already in memory and serializes it.
     *
     * <p>The buffer is borrowed for the length of the call. Which index types and element
     * types this connector builds is decided above this layer; Knowhere refuses a
     * combination it does not register.
     */
    public static Built build(String indexType, DType dataType, int version, ByteBuffer vectors,
            long rows, int dimension, String parameters) {
        Objects.requireNonNull(indexType, "indexType");
        Objects.requireNonNull(dataType, "dataType");
        Objects.requireNonNull(vectors, "vectors");
        Objects.requireNonNull(parameters, "parameters");
        if (rows <= 0 || dimension <= 0) {
            throw new IllegalArgumentException("An index is built over positive rows and dimensions");
        }
        NativeVectorLibrary.RuntimeInfo runtime = NativeVectorLibrary.load();
        if (version < runtime.minimumIndexVersion() || version > runtime.maximumIndexVersion()) {
            throw new IllegalArgumentException("Index format version " + version
                    + " is outside the loaded Knowhere range " + runtime.minimumIndexVersion()
                    + ".." + runtime.maximumIndexVersion());
        }
        KnowhereIndex index = Knowhere.createIndex(indexType, dataType, version);
        try {
            index.build(vectors, rows, dimension, parameters);
            return new Built(index.serialize(), rows, dimension);
        } finally {
            index.close();
        }
    }

    /**
     * Builds an index over vectors at a native address and serializes it.
     *
     * <p>The memory is borrowed for the length of the call. A ByteBuffer addresses at most
     * Integer.MAX_VALUE bytes; a segment's vectors can be larger, and Knowhere's C ABI takes
     * 64-bit sizes, so a segment build passes the address and the byte length instead.
     */
    public static Built build(String indexType, DType dataType, int version, long address, long bytes,
            long rows, int dimension, String parameters) {
        Objects.requireNonNull(indexType, "indexType");
        Objects.requireNonNull(dataType, "dataType");
        Objects.requireNonNull(parameters, "parameters");
        if (address == 0L) {
            throw new IllegalArgumentException("A native address is required");
        }
        if (rows <= 0 || dimension <= 0) {
            throw new IllegalArgumentException("An index is built over positive rows and dimensions");
        }
        NativeVectorLibrary.RuntimeInfo runtime = NativeVectorLibrary.load();
        if (version < runtime.minimumIndexVersion() || version > runtime.maximumIndexVersion()) {
            throw new IllegalArgumentException("Index format version " + version
                    + " is outside the loaded Knowhere range " + runtime.minimumIndexVersion()
                    + ".." + runtime.maximumIndexVersion());
        }
        KnowhereIndex index = Knowhere.createIndex(indexType, dataType, version);
        try {
            index.build(address, bytes, rows, dimension, parameters);
            return new Built(index.serialize(), rows, dimension);
        } finally {
            index.close();
        }
    }

    /** The payloads a built index serializes to, named the way Knowhere names them. */
    public static final class Built implements AutoCloseable {
        private final BinarySet data;
        private final long rows;
        private final int dimension;
        private boolean closed;

        private Built(BinarySet data, long rows, int dimension) {
            this.data = data;
            this.rows = rows;
            this.dimension = dimension;
        }

        public long rows() {
            return rows;
        }

        public int dimension() {
            return dimension;
        }

        public String[] names() {
            active();
            return data.names();
        }

        public long length(String name) {
            active();
            return data.length(Objects.requireNonNull(name, "name"));
        }

        /** Copies part of a payload into {@code destination}, which the caller owns. */
        public void read(String name, long offset, ByteBuffer destination) {
            active();
            data.read(Objects.requireNonNull(name, "name"), offset, destination);
        }

        private void active() {
            if (closed) {
                throw new IllegalStateException("The built index is closed");
            }
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                data.close();
            }
        }
    }

}
