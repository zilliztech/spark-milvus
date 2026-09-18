package com.zilliz.milvus.jni.vector;

import io.knowhere.BinarySet;
import io.knowhere.DType;
import io.knowhere.Knowhere;
import io.knowhere.KnowhereIndex;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** A persisted float32 index loaded and searched through the upstream JNI. */
public final class NativeVectorIndex implements AutoCloseable {
    private final KnowhereIndex index;
    private final int dimension;
    private final long rows;

    private NativeVectorIndex(KnowhereIndex index, int dimension, long rows) {
        this.index = index;
        this.dimension = dimension;
        this.rows = rows;
    }

    public int dimension() {
        return dimension;
    }

    public long rows() {
        return rows;
    }

    /**
     * Buffers are borrowed until this synchronous call returns. One call
     * answers every query in {@code queries}; output holds
     * {@code queryRows * topK} ids and distances.
     */
    public void search(ByteBuffer queries, long queryRows, int topK, ByteBuffer excluded,
            ByteBuffer ids, ByteBuffer distances, String parameters) {
        index.search(queries, queryRows, dimension, topK, excluded, excluded == null ? 0 : rows,
                ids, distances, parameters);
    }

    @Override
    public void close() {
        index.close();
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

    /** Collects decoded named payloads, then deserializes them without training. */
    public static final class Loader implements AutoCloseable {
        private final int version;
        private final int dimension;
        private final long rows;
        private final String parameters;
        private final BinarySet data;
        private final Set<String> names = new HashSet<>();
        private boolean closed;
        private boolean attempted;

        public Loader(int version, int dimension, long rows, String parameters) {
            if (dimension <= 0 || rows <= 0) {
                throw new IllegalArgumentException("Persisted index dimension and row count must be positive");
            }
            if (version < 0) {
                throw new IllegalArgumentException("Persisted index format version must be explicit and nonnegative");
            }
            this.version = version;
            this.dimension = dimension;
            this.rows = rows;
            this.parameters = Objects.requireNonNull(parameters, "parameters");
            NativeVectorLibrary.RuntimeInfo runtime = NativeVectorLibrary.load();
            if (version < runtime.minimumIndexVersion() || version > runtime.maximumIndexVersion()) {
                throw new IllegalArgumentException("Persisted index format version " + version
                        + " is outside the loaded Knowhere range " + runtime.minimumIndexVersion()
                        + ".." + runtime.maximumIndexVersion());
            }
            data = BinarySet.create();
        }

        private void active() {
            if (closed || attempted) {
                throw new IllegalStateException("Persisted index loader is closed or loading was already attempted");
            }
        }

        public void allocate(String name, long bytes) {
            active();
            Objects.requireNonNull(name, "name");
            if (!names.add(name)) {
                throw new IllegalArgumentException("Duplicate persisted index payload: " + name);
            }
            data.allocate(name, bytes);
        }

        public void write(String name, long offset, ByteBuffer bytes) {
            active();
            if (!names.contains(name)) {
                throw new IllegalArgumentException("Persisted index payload was not allocated: " + name);
            }
            data.write(name, offset, bytes);
        }

        /**
         * Deserializes the payloads into the index the caller names.
         *
         * <p>The engine name is the index type Knowhere registers, which is also the name its
         * Serialize gives the payload; an IVF index additionally answers to the names Knowhere 1.x
         * wrote. Which engines and element types this connector accepts is decided above this
         * layer.
         */
        public NativeVectorIndex load(String engineType, DType dataType) {
            active();
            attempted = true;
            KnowhereIndex loaded = null;
            try {
                Objects.requireNonNull(engineType, "engineType");
                Objects.requireNonNull(dataType, "dataType");
                String payload = "HNSW_DEPRECATED".equals(engineType) ? "HNSW" : engineType;
                if (!names.contains(payload) && !names.contains("IVF") && !names.contains("BinaryIVF")) {
                    throw new IllegalArgumentException(
                            "Persisted index is missing its " + payload + " payload; it carries " + names);
                }
                loaded = Knowhere.createIndex(engineType, dataType, version);
                loaded.deserialize(data, parameters);
                if (loaded.dimensions() != dimension || loaded.rows() != rows) {
                    throw new IllegalArgumentException("Persisted index shape differs from segment metadata: "
                            + "index=" + loaded.rows() + "x" + loaded.dimensions()
                            + ", expected=" + rows + "x" + dimension);
                }
                return new NativeVectorIndex(loaded, dimension, rows);
            } catch (RuntimeException | Error failure) {
                if (loaded != null) {
                    loaded.close();
                }
                throw failure;
            } finally {
                // The upstream C API owns an independent BinarySet snapshot after
                // deserialize; the source payload allocation can now be released.
                close();
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
