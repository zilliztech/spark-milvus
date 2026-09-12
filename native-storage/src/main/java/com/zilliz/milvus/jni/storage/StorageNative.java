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
}
