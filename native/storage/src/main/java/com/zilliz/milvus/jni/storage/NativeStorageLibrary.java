package com.zilliz.milvus.jni.storage;

import com.zilliz.milvus.jni.runtime.NativeLibraries;
import com.zilliz.milvus.jni.runtime.NativeLibraries.Bundle;
import io.milvus.storage.NativeLibraryLoader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/** Initializes the upstream milvus-storage JNI binding from the shared native bundle. */
public final class NativeStorageLibrary {
    private static final String NATIVE_PATH = "milvus.storage.native.path";
    private static boolean loaded;

    private NativeStorageLibrary() {}

    /**
     * Loads storage JNI once through its upstream loader.
     *
     * <p>When a unified bundle is present, this method temporarily hands its
     * verified storage JNI path to the upstream loader. A conflicting explicit
     * path, corrupt bundle, or native linking error remains visible to the caller.
     */
    public static synchronized void load() {
        if (loaded) return;
        Optional<Bundle> bundle = NativeLibraries.bundled();
        if (bundle.isPresent()) {
            if (NativeLibraryLoader.isLoaded()) {
                throw new UnsatisfiedLinkError(
                        "milvus-storage JNI was initialized before the unified native bundle handoff");
            }
            loadBundle(bundle.get());
        } else {
            NativeLibraryLoader.loadLibrary();
        }
        loaded = true;
    }

    private static void loadBundle(Bundle bundle) {
        Path entry = bundle.entry("milvus-storage-jni");
        synchronized (System.getProperties()) {
            String explicit = System.getProperty(NATIVE_PATH);
            if (explicit != null) verifySameEntry(entry, explicit);
            System.setProperty(NATIVE_PATH, entry.toString());
            try {
                NativeLibraryLoader.loadLibrary();
            } finally {
                if (explicit == null) System.clearProperty(NATIVE_PATH);
                else System.setProperty(NATIVE_PATH, explicit);
            }
        }
    }

    private static void verifySameEntry(Path entry, String explicit) {
        Path selected = Paths.get(explicit);
        try {
            if (!selected.isAbsolute() || !Files.isRegularFile(selected) || !Files.isSameFile(entry, selected)) {
                throw new UnsatisfiedLinkError(
                        NATIVE_PATH + " conflicts with the unified Milvus native bundle: " + explicit);
            }
        } catch (IOException error) {
            UnsatisfiedLinkError failure = new UnsatisfiedLinkError(
                    "Cannot verify " + NATIVE_PATH + ": " + explicit);
            failure.initCause(error);
            throw failure;
        }
    }
}
