package com.zilliz.milvus.jni.storage.loader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.CopyOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Loads {@code libnative-storage-jni} and the {@code libmilvus-storage} it
 * links against.
 *
 * <p>Both ship inside this module's jar under {@code native/<os>-<arch>/}.
 * Loading needs them on a real filesystem, so the platform's directory is
 * copied to a temp directory and the dependency is loaded first: the JNI
 * library's rpath is {@code @loader_path} / {@code $ORIGIN}, which resolves it
 * from the same directory.
 *
 * <p>Extraction is skipped entirely when {@code java.library.path} already
 * offers the library, which is how the build's own test JVMs run.
 */
public final class NativeStorageLibrary {

    private static final String LIBRARY_NAME = "native-storage-jni";
    private static final String DEPENDENCY_NAME = "milvus-storage";
    private static final String RESOURCE_PREFIX = "native/";
    private static final String TEMP_DIR_NAME = "zilliz-native-storage";

    private static boolean loaded;

    private NativeStorageLibrary() {}

    public static synchronized void load() {
        if (loaded) {
            return;
        }
        try {
            System.loadLibrary(LIBRARY_NAME);
            loaded = true;
            return;
        } catch (UnsatisfiedLinkError fromLibraryPath) {
            // Not on java.library.path; fall through to the jar.
        }

        File directory;
        try {
            directory = extract();
        } catch (IOException e) {
            throw new UnsatisfiedLinkError(
                    "failed to extract the native libraries from the jar: " + e.getMessage());
        }

        // The dependency first: the JNI library names it and resolves it from
        // its own directory.
        File dependency = new File(directory, System.mapLibraryName(DEPENDENCY_NAME));
        if (dependency.isFile()) {
            System.load(dependency.getAbsolutePath());
        }
        File library = new File(directory, System.mapLibraryName(LIBRARY_NAME));
        if (!library.isFile()) {
            throw new UnsatisfiedLinkError(
                    "no " + library.getName() + " for this platform in " + platform()
                            + "; the jar carries " + listing(directory));
        }
        System.load(library.getAbsolutePath());
        loaded = true;
    }

    /** {@code <os>-<arch>}, matching the directory names the build produces. */
    private static String platform() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String arch = System.getProperty("os.arch", "").toLowerCase();
        String normalizedArch =
                (arch.contains("aarch64") || arch.contains("arm64")) ? "aarch64" : "x86_64";
        if (os.contains("mac") || os.contains("darwin")) {
            return "darwin-" + normalizedArch;
        }
        if (os.contains("win")) {
            return "windows-" + normalizedArch;
        }
        return "linux-" + normalizedArch;
    }

    /**
     * Copies this platform's directory out of the jar, or points straight at it
     * when the classes are loose on disk, which is how the build runs.
     */
    private static File extract() throws IOException {
        String prefix = RESOURCE_PREFIX + platform() + "/";
        URL url = NativeStorageLibrary.class.getClassLoader().getResource(prefix);
        if (url == null) {
            throw new IOException("no " + prefix + " in this jar");
        }
        if ("file".equals(url.getProtocol())) {
            return new File(URI.create(url.toString()));
        }

        File target = new File(System.getProperty("java.io.tmpdir"), TEMP_DIR_NAME);
        if (!target.isDirectory() && !target.mkdirs()) {
            throw new IOException("cannot create " + target);
        }
        String jarPath = url.getPath();
        int separator = jarPath.indexOf("!/");
        if (separator < 0) {
            throw new IOException("unexpected resource url: " + url);
        }
        Path jarFile = Paths.get(URI.create(jarPath.substring(0, separator)));
        try (JarFile jar = new JarFile(jarFile.toFile())) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (entry.isDirectory() || !entry.getName().startsWith(prefix)) {
                    continue;
                }
                File out = new File(target, entry.getName().substring(prefix.length()));
                File parent = out.getParentFile();
                if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
                    throw new IOException("cannot create " + parent);
                }
                try (InputStream in = jar.getInputStream(entry)) {
                    Files.copy(in, out.toPath(), (CopyOption) StandardCopyOption.REPLACE_EXISTING);
                }
                out.deleteOnExit();
            }
        }
        return target;
    }

    /** Names what is actually there, so a platform mismatch says so. */
    private static String listing(File directory) {
        List<String> names = new ArrayList<>();
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                names.add(file.getName());
            }
        }
        return names.toString();
    }
}
