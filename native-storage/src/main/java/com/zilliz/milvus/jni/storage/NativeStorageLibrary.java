package com.zilliz.milvus.jni.storage;

import java.io.File;

/**
 * Loads {@code libnative-storage-jni}.
 *
 * <p>It links against {@code libmilvus-storage}, which the upstream binding's
 * loader already extracts from {@code native/<platform>/} into a temp
 * directory. Loading that one first puts the dependency in the process and on
 * disk next to ours, which the {@code @loader_path} / {@code $ORIGIN} rpath
 * then resolves. When the JNI wrapper replaces that binding this class extracts
 * for itself and the dependency on it goes away.
 */
final class NativeStorageLibrary {

    private static final String LIBRARY_NAME = "native-storage-jni";
    private static boolean loaded;

    private NativeStorageLibrary() {}

    static synchronized void load() {
        if (loaded) {
            return;
        }
        io.milvus.storage.NativeLibraryLoader.loadLibrary();

        File extracted = new File(
                new File(System.getProperty("java.io.tmpdir"), "milvus-storage-native"),
                System.mapLibraryName(LIBRARY_NAME));
        if (extracted.exists()) {
            System.load(extracted.getAbsolutePath());
        } else {
            System.loadLibrary(LIBRARY_NAME);
        }
        loaded = true;
    }
}
