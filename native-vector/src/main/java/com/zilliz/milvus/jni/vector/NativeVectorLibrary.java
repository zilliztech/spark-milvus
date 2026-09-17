package com.zilliz.milvus.jni.vector;

import com.zilliz.milvus.jni.runtime.NativeLibraries;
import com.zilliz.milvus.jni.runtime.NativeLibraries.Bundle;
import io.knowhere.Knowhere;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;

/**
 * Initializes the upstream Knowhere JNI binding when vector execution needs it.
 *
 * <p>The shared runtime verifies and extracts a unified bundle when supplied;
 * the upstream binding owns native loading and standalone platform packages.
 * Loading this class alone does not initialize JNI. Call {@link #load()} on the
 * executor; native libraries and their state belong to that JVM and class loader.
 * HotSpot must start with its own JRE's libjsig preloaded, as required by Knowhere.
 */
public final class NativeVectorLibrary {
    private static final String NATIVE_PATH = "knowhere.native.path";
    private static RuntimeInfo runtime;

    private NativeVectorLibrary() {}

    /**
     * Loads once and reports the versions supported by the actual native library.
     *
     * <p>Failures from the upstream loader propagate to the caller. A missing or
     * incompatible library must not be reported as an available vector engine.
     */
    public static synchronized RuntimeInfo load() {
        if (runtime == null) {
            Optional<Bundle> bundle = NativeLibraries.bundled();
            runtime = bundle.isPresent() ? loadBundle(bundle.get()) : initialize(cardinalEnabled());
        }
        return runtime;
    }

    private static RuntimeInfo initialize(boolean cardinal) {
        // This call triggers NativeBindings initialization and its C ABI check.
        int abi = Knowhere.cAbiVersion();
        int minimum = Knowhere.minimumIndexVersion();
        int current = Knowhere.currentIndexVersion();
        int maximum = Knowhere.maximumIndexVersion();
        if (minimum > current || current > maximum) {
            throw new UnsatisfiedLinkError(
                    "Invalid Knowhere index version range: " + minimum + " <= "
                            + current + " <= " + maximum);
        }
        return new RuntimeInfo(abi, minimum, current, maximum, cardinal);
    }

    private static RuntimeInfo loadBundle(Bundle bundle) {
        Path entry = bundle.library("libknowhere_jni.so");
        // Knowhere reads a process property during class initialization. Scope
        // that handoff to this call, including failures, instead of retaining
        // our temporary path as an application override. The monitor is shared
        // across class loaders; this does not make duplicate JNI bindings safe.
        synchronized (System.getProperties()) {
            String explicit = System.getProperty(NATIVE_PATH);
            if (explicit != null) {
                Path selected = Paths.get(explicit);
                try {
                    if (!selected.isAbsolute() || !Files.isRegularFile(selected) || !Files.isSameFile(entry, selected)) {
                        throw new UnsatisfiedLinkError(NATIVE_PATH + " conflicts with the unified Milvus native bundle: "
                                + explicit);
                    }
                } catch (IOException error) {
                    UnsatisfiedLinkError failure = new UnsatisfiedLinkError("Cannot verify " + NATIVE_PATH + ": " + explicit);
                    failure.initCause(error);
                    throw failure;
                }
            }
            System.setProperty(NATIVE_PATH, entry.toString());
            try {
                return initialize(bundle.cardinalEnabled());
            } finally {
                if (explicit == null) System.clearProperty(NATIVE_PATH);
                else System.setProperty(NATIVE_PATH, explicit);
            }
        }
    }

    private static boolean cardinalEnabled() {
        // An arbitrary externally supplied library has no verified build feature record.
        if (System.getProperty(NATIVE_PATH) != null) return false;
        try (InputStream input = NativeVectorLibrary.class.getResourceAsStream(
                "/META-INF/milvus/knowhere-runtime.properties")) {
            if (input == null) return false;
            Properties features = new Properties();
            features.load(input);
            return "true".equals(features.getProperty("with_cardinal"));
        } catch (IOException failure) {
            throw new IllegalStateException("Cannot read Knowhere build features", failure);
        }
    }

    /** Native version values; these do not establish Milvus index-file compatibility. */
    public static final class RuntimeInfo {
        private final int cAbiVersion;
        private final int minimumIndexVersion;
        private final int currentIndexVersion;
        private final int maximumIndexVersion;
        private final boolean cardinalSupported;

        private RuntimeInfo(int abi, int minimum, int current, int maximum, boolean cardinalSupported) {
            this.cAbiVersion = abi;
            this.minimumIndexVersion = minimum;
            this.currentIndexVersion = current;
            this.maximumIndexVersion = maximum;
            this.cardinalSupported = cardinalSupported;
        }

        public int cAbiVersion() {
            return cAbiVersion;
        }

        public int minimumIndexVersion() {
            return minimumIndexVersion;
        }

        public int currentIndexVersion() {
            return currentIndexVersion;
        }

        public int maximumIndexVersion() {
            return maximumIndexVersion;
        }

        public boolean cardinalSupported() {
            return cardinalSupported;
        }

        @Override
        public String toString() {
            return "Knowhere C ABI " + cAbiVersion + ", index versions "
                    + minimumIndexVersion + ".." + maximumIndexVersion
                    + " (current " + currentIndexVersion + ", Cardinal " + cardinalSupported + ")";
        }
    }
}
