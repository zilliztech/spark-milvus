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

    /** Buffers are borrowed until this synchronous call returns. */
    public void search(ByteBuffer query, int topK, ByteBuffer excluded,
            ByteBuffer ids, ByteBuffer distances, String parameters) {
        index.search(query, 1, dimension, topK, excluded, excluded == null ? 0 : rows,
                ids, distances, parameters);
    }

    @Override
    public void close() {
        index.close();
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

        public NativeVectorIndex load(String engineType) {
            active();
            attempted = true;
            KnowhereIndex loaded = null;
            try {
                if (!"HNSW".equals(engineType) && !"HNSW_DEPRECATED".equals(engineType)) {
                    throw new IllegalArgumentException("Persisted vector engine is unsupported: " + engineType);
                }
                if (!names.contains("HNSW")) {
                    throw new IllegalArgumentException("Persisted index is missing its HNSW payload");
                }
                loaded = Knowhere.createIndex(engineType, DType.FLOAT32, version);
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
