package com.zilliz.milvus.jni.storage;

/** One entry of a directory listing, mirroring {@code LoonFileInfo}. */
public final class FileEntry {

    private final String path;
    private final boolean directory;
    private final long size;
    private final long modifiedNanos;

    public FileEntry(String path, boolean directory, long size, long modifiedNanos) {
        this.path = path;
        this.directory = directory;
        this.size = size;
        this.modifiedNanos = modifiedNanos;
    }

    public String path() {
        return path;
    }

    public boolean isDirectory() {
        return directory;
    }

    public long size() {
        return size;
    }

    public long modifiedNanos() {
        return modifiedNanos;
    }

    @Override
    public String toString() {
        return "FileEntry[" + path + (directory ? ", dir" : ", size=" + size) + "]";
    }
}
