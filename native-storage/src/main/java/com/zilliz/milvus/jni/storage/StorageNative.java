package com.zilliz.milvus.jni.storage;

import com.zilliz.milvus.jni.storage.loader.NativeStorageLibrary;

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

    /**
     * Opens a reader over C-allocated column groups, which is what
     * {@link #manifestOpen} and the writers hand back. Otherwise identical to
     * {@link #readerNew}.
     */
    public static native long readerNewNative(
            long nativeColumnGroupsAddress,
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

    /**
     * Counters the batch reader keeps for metrics (capability G5): {@code
     * {batches, copies, copiedBytes}}. A copy is one column materialized by
     * {@code arrow::Concatenate} in {@link #recordBatchReaderReadNext} because
     * it arrived sliced; the bytes are what that copy produced.
     */
    public static native long[] recordBatchReaderStats(long recordBatchReaderHandle);

    public static native void recordBatchReaderDestroy(long recordBatchReaderHandle);

    // ---------------------------------------------------------------------
    // Write path: the dataset writer, the segment writer, and the manifest
    // transaction. Implemented in writer_jni.cpp.
    //
    // Two kinds of column-group handle appear below and they are not
    // interchangeable. {@link #columnGroupsCreate} returns one built here from
    // a Java layout, released with {@link #columnGroupsDestroy}. The writers
    // return one the C layer allocated, released with
    // {@link #nativeColumnGroupsDestroy}; only that kind can be appended to a
    // transaction.
    // ---------------------------------------------------------------------

    /**
     * Opens a writer at {@code basePath}.
     *
     * <p>{@code arrowSchemaAddress} is the address of an {@code ArrowSchema}
     * the caller exported and keeps alive until the writer is destroyed.
     */
    public static native long writerNew(
            String basePath, long arrowSchemaAddress, Map<String, String> properties);

    /** Writes one batch, given the address of an exported {@code ArrowArray}. */
    public static native void writerWrite(long writerHandle, long arrowArrayAddress);

    public static native void writerFlush(long writerHandle);

    /**
     * Closes the writer and returns the column groups it produced.
     *
     * <p>The returned handle is C-allocated; release it with
     * {@link #nativeColumnGroupsDestroy}. Both metadata arrays may be null.
     */
    public static native long writerClose(
            long writerHandle, String[] metadataKeys, String[] metadataValues);

    public static native void writerDestroy(long writerHandle);

    /** Opens the packed segment writer. TEXT columns are out of scope, so no
     * LOB configuration is exposed. */
    public static native long segmentWriterNew(
            long arrowSchemaAddress, String segmentPath, Map<String, String> properties);

    public static native void segmentWriterWrite(long segmentWriterHandle, long arrowArrayAddress);

    public static native void segmentWriterFlush(long segmentWriterHandle);

    /**
     * Closes the segment writer.
     *
     * @return two elements: the C-allocated column groups handle, and the
     *     number of rows written. One array because close runs once.
     */
    public static native long[] segmentWriterClose(long segmentWriterHandle);

    public static native void segmentWriterDestroy(long segmentWriterHandle);

    /**
     * Opens the V2 packed writer, one file per column group.
     *
     * <p>{@code groupOffsets} and {@code groupIndices} are the flattened
     * per-group column index lists the C layer takes: group {@code g} owns
     * {@code groupIndices[groupOffsets[g] .. groupOffsets[g+1])}.
     */
    public static native long packedWriterNew(
            String[] paths,
            int[] groupOffsets,
            int[] groupIndices,
            long arrowSchemaAddress,
            Map<String, String> properties,
            long bufferSize);

    public static native void packedWriterWrite(long packedWriterHandle, long arrowArrayAddress);

    public static native void packedWriterClose(long packedWriterHandle);

    public static native void packedWriterDestroy(long packedWriterHandle);

    /**
     * Opens a manifest transaction.
     *
     * <p>{@code readVersion} is the manifest version the caller read, or 0 to
     * let the C layer pick the latest. {@code resolveId} selects the conflict
     * policy: 0 fails on conflict, 2 overwrites.
     */
    public static native long transactionBegin(
            String basePath,
            Map<String, String> properties,
            long readVersion,
            int resolveId,
            int retryLimit);

    /** @return the committed manifest version. */
    public static native long transactionCommit(long transactionHandle);

    public static native long transactionReadVersion(long transactionHandle);

    public static native void transactionDropColumn(long transactionHandle, String column);

    /** Appends every group of a C-allocated column groups handle. */
    public static native void transactionAppendFiles(
            long transactionHandle, long nativeColumnGroupsHandle);

    /** Adds one group of a C-allocated column groups handle, by index. */
    public static native void transactionAddColumnGroup(
            long transactionHandle, long nativeColumnGroupsHandle, int index);

    /** Adds every group of a C-allocated column groups handle. */
    public static native void transactionAddColumnGroups(
            long transactionHandle, long nativeColumnGroupsHandle);

    public static native void transactionAddDeltaLog(
            long transactionHandle, String path, long numEntries);

    /**
     * Records a statistics entry in the transaction: {@code key} (such as
     * {@code bloom_filter.100}) with its files and metadata. Replaces an
     * existing entry of the same key, as loon_transaction_update_stat does.
     */
    public static native void transactionUpdateStat(
            long transactionHandle,
            String key,
            String[] files,
            String[] metadataKeys,
            String[] metadataValues);

    public static native void transactionDestroy(long transactionHandle);

    // Reading back what a writer produced, which is how a caller learns the
    // paths and row counts it has to register.

    public static native int nativeColumnGroupsCount(long nativeColumnGroupsHandle);

    public static native String[] nativeColumnGroupFiles(
            long nativeColumnGroupsHandle, int index);

    public static native long[] nativeColumnGroupRowCounts(
            long nativeColumnGroupsHandle, int index);

    public static native String[] nativeColumnGroupColumns(
            long nativeColumnGroupsHandle, int index);

    public static native void nativeColumnGroupsDestroy(long nativeColumnGroupsHandle);

    /**
     * Reads the manifest at {@code basePath}.
     *
     * @param readVersion the version to read, or -1 for the latest
     * @return three elements: the manifest handle to release with
     *     {@link #manifestDestroy}, the address of the column groups inside it
     *     for {@link #readerNewNative}, and the version actually read
     */
    public static native long[] manifestOpen(
            String basePath, Map<String, String> properties, long readVersion);

    public static native void manifestDestroy(long manifestHandle);
}
