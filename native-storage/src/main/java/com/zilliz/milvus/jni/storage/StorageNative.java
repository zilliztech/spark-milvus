package com.zilliz.milvus.jni.storage;

import java.util.Map;

/**
 * One native method per {@code loon_*} entry point.
 *
 * <p>Handles are longs. A {@code LoonFFIResult} that is not success becomes a
 * {@link StorageNativeException}; no result code reaches Java.
 *
 * <p>Nothing here interprets a property key or names a cloud. Which backend
 * runs is {@code fs.cloud_provider}, resolved inside the C layer.
 */
public final class StorageNative {

    private StorageNative() {}

    static {
        NativeStorageLibrary.load();
    }

    /**
     * Opens a filesystem for {@code path} using {@code properties}.
     *
     * <p>The path selects among {@code extfs.<name>.*} registrations by address
     * and bucket; an empty path uses the default {@code fs.*} configuration.
     * The returned handle is a pointer inside this process: it is not
     * serializable and must not travel to another JVM.
     */
    public static native long filesystemGet(Map<String, String> properties, String path);

    public static native void filesystemDestroy(long handle);

    /** Reads a whole file. The C side allocates, this copies and frees. */
    public static native byte[] readFileAll(long handle, String path);

    public static native void writeFile(long handle, String path, byte[] data);

    public static native void deleteFile(long handle, String path);

    public static native void createDir(long handle, String path, boolean recursive);

    /** Size in bytes. Throws if the path does not exist. */
    public static native long fileSize(long handle, String path);

    /**
     * Lists a directory.
     *
     * <p>Returns one {@link FileEntry} per child. An empty path lists the
     * filesystem root.
     */
    public static native FileEntry[] listDir(long handle, String path, boolean recursive);

    /**
     * Opens a reader for ranged access.
     *
     * <p>{@code fileSize} is passed in rather than looked up so a caller that
     * already knows it does not pay for a second round trip.
     */
    public static native long openReader(long handle, String path, long fileSize);

    public static native byte[] readerReadAt(long readerHandle, long offset, long length);

    public static native void readerDestroy(long readerHandle);

    // ---------------------------------------------------------------------
    // Segment read path: column groups, the reader, and the per-batch
    // RecordBatchReader. Implemented in segment_reader_jni.cpp.
    // ---------------------------------------------------------------------

    /**
     * Builds a {@code LoonColumnGroups} from a caller-supplied layout, for
     * segments whose column groups were recovered outside the native manifest
     * (V2 packed, where the layout comes from the snapshot AVRO plus each
     * parquet footer's {@code group_field_id_list}).
     *
     * <p>The three arrays are parallel and per column group. Row counts give
     * each file's row span within its group, which is how the packed reader
     * knows where one file ends and the next begins.
     *
     * <p>The returned handle owns every string it was given; release it with
     * {@link #columnGroupsDestroy} once no reader needs it.
     */
    public static native long columnGroupsCreate(
            String[][] columnsPerGroup,
            String[][] filesPerGroup,
            long[][] fileRowCountsPerGroup,
            String format);

    public static native void columnGroupsDestroy(long columnGroupsHandle);

    /**
     * Opens a reader over the column groups.
     *
     * <p>{@code arrowSchemaAddress} is the address of an {@code ArrowSchema}
     * the caller exported and keeps alive until the reader is destroyed.
     * {@code neededColumns} may be null or empty to read every column.
     */
    public static native long readerNew(
            long columnGroupsHandle,
            long arrowSchemaAddress,
            String[] neededColumns,
            Map<String, String> properties);

    /** Named apart from {@link #readerDestroy}, which closes a file reader. */
    public static native void readerDestroySegment(long readerHandle);

    /**
     * Opens the per-batch reader.
     *
     * <p>Deliberately not the {@code ArrowArrayStream} form: Arrow Java's
     * importer ignores {@code ArrowArray.offset}, so once the packed reader
     * returns a sliced batch every later batch re-reads from buffer position
     * zero. {@code predicate} may be null.
     */
    public static native long recordBatchReaderNew(long readerHandle, String predicate);

    /**
     * Fills two caller-allocated C structs with the next batch.
     *
     * <p>Pass the addresses of a zero-initialized {@code ArrowArray} and
     * {@code ArrowSchema} — in Java, {@code ArrowArray.allocateNew} and
     * {@code ArrowSchema.allocateNew}. Returns false at EOF, leaving both
     * structs' release fields null.
     */
    public static native boolean recordBatchReaderReadNext(
            long recordBatchReaderHandle, long arrayAddress, long schemaAddress);

    public static native void recordBatchReaderDestroy(long recordBatchReaderHandle);
}
