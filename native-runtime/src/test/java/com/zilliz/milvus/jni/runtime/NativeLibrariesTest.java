package com.zilliz.milvus.jni.runtime;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Uses inert fixture bytes to test extraction without requiring native libraries. */
public class NativeLibrariesTest {
    private static final String PLATFORM = platform();
    private static final String STORAGE = libraryName("milvus-storage-jni");
    private static final String KNOWHERE = libraryName("knowhere_jni");
    private static final String PREFIX = "native/milvus/1/" + PLATFORM + "/";

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    private final List<URLClassLoader> classLoaders = new ArrayList<>();

    @After
    public void closeClassLoaders() throws Exception {
        for (URLClassLoader loader : classLoaders) loader.close();
    }

    @Test
    public void absentManifestDoesNotCreateAnExtractionDirectory() throws Exception {
        Path extraction = temporary.newFolder().toPath();
        assertFalse(loader(extraction).get().isPresent());
        assertEquals(0, children(extraction));
    }

    @Test
    public void markedJarForAnotherPlatformFailsWithoutExtraction() throws Exception {
        String otherPlatform = PLATFORM.equals("linux-x86_64") ? "linux-aarch64" : "linux-x86_64";
        Map<String, byte[]> libraries = libraries();
        Path resource = jar("native/milvus/1/" + otherPlatform + "/",
                manifest(libraries).replace("platform=" + PLATFORM, "platform=" + otherPlatform), libraries, true);
        Path extraction = temporary.newFolder().toPath();
        NativeLibraries.Loader loader = loader(extraction, resource);
        UnsatisfiedLinkError failure = failure(loader);
        assertTrue(failure.getMessage(), failure.getMessage().contains("has no manifest for " + PLATFORM));
        assertSame(failure, failure(loader));
        assertEquals(0, children(extraction));
    }

    @Test
    public void bundleMarkerWithoutAnyManifestFailsWithoutFallback() throws Exception {
        Path extraction = temporary.newFolder().toPath();
        NativeLibraries.Loader loader = loader(extraction, jar(PREFIX, null, Map.of(), true));
        UnsatisfiedLinkError failure = failure(loader);
        assertTrue(failure.getMessage(), failure.getMessage().contains("has no manifest for " + PLATFORM));
        assertEquals(0, children(extraction));
    }

    @Test
    public void markedJarForTheCurrentPlatformExtractsNormally() throws Exception {
        Map<String, byte[]> libraries = libraries();
        Path extraction = temporary.newFolder().toPath();
        NativeLibraries.Bundle bundle = loader(extraction, jar(PREFIX, manifest(libraries), libraries, true)).get().get();
        assertArrayEquals(libraries.get(STORAGE), Files.readAllBytes(bundle.library(STORAGE)));
        assertArrayEquals(libraries.get(KNOWHERE), Files.readAllBytes(bundle.library(KNOWHERE)));
        assertEquals(1, children(extraction));
    }

    @Test
    public void unsupportedPlatformFailsOnlyWhenABundleMarkerIsPresent() throws Exception {
        Path extraction = temporary.newFolder().toPath();
        NativeLibraries.Loader absent = loader(extraction);
        NativeLibraries.Loader marked = loader(extraction, jar(PREFIX, null, Map.of(), true));
        String original = System.getProperty("os.arch");
        try {
            System.setProperty("os.arch", "unsupported-test-architecture");
            assertFalse(absent.get().isPresent());
            UnsatisfiedLinkError failure = failure(marked);
            assertTrue(failure.getMessage(), failure.getMessage().contains("platform is unsupported"));
            assertTrue(failure.getMessage(), failure.getMessage().contains("unsupported-test-architecture"));
            assertEquals(0, children(extraction));
        } finally {
            System.setProperty("os.arch", original);
        }
    }

    @Test
    public void bothEntriesAndNestedDependenciesShareOneVerifiedDirectory() throws Exception {
        Map<String, byte[]> libraries = libraries();
        libraries.put("ossl-modules/legacy.so", bytes("provider"));
        String manifest = manifest(libraries) + "aliases=libcommon.so\nalias.libcommon.so=libcommon.so.1\n";
        Path extraction = temporary.newFolder().toPath();
        NativeLibraries.Bundle bundle = loader(extraction, jar(manifest, libraries)).get().get();

        assertEquals(bundle.library(STORAGE).getParent(), bundle.library(KNOWHERE).getParent());
        assertEquals(bundle.directory(), bundle.library(KNOWHERE).getParent());
        assertArrayEquals(bytes("provider"), Files.readAllBytes(bundle.library("ossl-modules/legacy.so")));
        assertTrue(Files.isSameFile(bundle.library("libcommon.so"), bundle.library("libcommon.so.1")));
        assertTrue(bundle.cardinalEnabled());
        assertEquals("1".repeat(40), bundle.storageRevision());
        assertEquals("2".repeat(40), bundle.knowhereRevision());
        assertEquals(1, children(extraction));
        assertThrows(UnsatisfiedLinkError.class, () -> bundle.library("../undeclared.so"));
    }

    @Test
    public void explodedResourcesUseTheManifestDirectory() throws Exception {
        Path classes = temporary.newFolder().toPath();
        Map<String, byte[]> libraries = libraries();
        write(classes.resolve(PREFIX + "manifest.properties"), bytes(manifest(libraries)));
        for (Map.Entry<String, byte[]> library : libraries.entrySet()) {
            write(classes.resolve(PREFIX + library.getKey()), library.getValue());
        }
        NativeLibraries.Bundle bundle = loader(temporary.newFolder().toPath(), classes).get().get();
        assertNotEquals(classes.resolve(PREFIX), bundle.directory());
        assertArrayEquals(libraries.get(STORAGE), Files.readAllBytes(bundle.library(STORAGE)));
    }

    @Test
    public void entryLibraryAliasesReferToTheCanonicalInode() throws Exception {
        Map<String, byte[]> libraries = libraries();
        libraries.put(STORAGE + ".1", libraries.remove(STORAGE));
        libraries.put(KNOWHERE + ".1", libraries.remove(KNOWHERE));
        String manifest = manifest(libraries) + "aliases=" + STORAGE + "," + KNOWHERE
                + "\nalias." + STORAGE + "=" + STORAGE + ".1\nalias." + KNOWHERE + "=" + KNOWHERE + ".1\n";
        NativeLibraries.Bundle bundle = loader(temporary.newFolder().toPath(), jar(manifest, libraries)).get().get();
        assertTrue(Files.isSameFile(bundle.library(STORAGE), bundle.library(STORAGE + ".1")));
        assertTrue(Files.isSameFile(bundle.library(KNOWHERE), bundle.library(KNOWHERE + ".1")));
        assertEquals(bundle.library(STORAGE).getParent(), bundle.library(KNOWHERE).getParent());
    }

    @Test
    public void concurrentEntryRequestsExtractOnceAndPublishOneBundle() throws Exception {
        Path extraction = temporary.newFolder().toPath();
        Map<String, byte[]> libraries = libraries();
        NativeLibraries.Loader loader = loader(extraction, jar(manifest(libraries), libraries));
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Callable<NativeLibraries.Bundle>> calls = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                String entry = i % 2 == 0 ? STORAGE : KNOWHERE;
                calls.add(() -> {
                    NativeLibraries.Bundle bundle = loader.get().get();
                    assertArrayEquals(libraries.get(entry), Files.readAllBytes(bundle.library(entry)));
                    return bundle;
                });
            }
            List<Future<NativeLibraries.Bundle>> results = pool.invokeAll(calls, 10, TimeUnit.SECONDS);
            NativeLibraries.Bundle first = results.get(0).get();
            for (Future<NativeLibraries.Bundle> result : results) assertSame(first, result.get());
            assertEquals(1, children(extraction));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void identicalManifestsInTwoJarsAreRejected() throws Exception {
        Map<String, byte[]> libraries = libraries();
        Path extraction = temporary.newFolder().toPath();
        NativeLibraries.Loader loader = loader(extraction,
                jar(manifest(libraries), libraries), jar(manifest(libraries), libraries));
        assertTrue(failure(loader).getMessage().contains("Multiple Milvus native manifests"));
        assertEquals(0, children(extraction));
    }

    @Test
    public void checksumFailureRemovesPartialFilesAndRemainsAFailure() throws Exception {
        Map<String, byte[]> libraries = libraries();
        String manifest = manifest(libraries);
        libraries.put(KNOWHERE, bytes("corrupted"));
        Path extraction = temporary.newFolder().toPath();
        NativeLibraries.Loader loader = loader(extraction, jar(manifest, libraries));
        UnsatisfiedLinkError failure = failure(loader);
        assertTrue(failure.getMessage().contains("Checksum mismatch for " + KNOWHERE));
        assertSame(failure, failure(loader));
        assertEquals(0, children(extraction));
    }

    @Test
    public void missingFileCannotBeSuppliedByAnotherClasspathJar() throws Exception {
        Map<String, byte[]> libraries = libraries();
        String manifest = manifest(libraries);
        byte[] missing = libraries.remove(KNOWHERE);
        Path supplyingJar = jar(null, Map.of(KNOWHERE, missing));
        Path extraction = temporary.newFolder().toPath();
        NativeLibraries.Loader loader = loader(extraction, jar(manifest, libraries), supplyingJar);
        assertTrue(failure(loader).getMessage().contains(KNOWHERE));
        assertEquals(0, children(extraction));
    }

    @Test
    public void missingEntryAndDuplicateLibrariesAreRejectedBeforeExtraction() throws Exception {
        Map<String, byte[]> libraries = libraries();
        String good = manifest(libraries);
        assertRejected(good.replace("libraries=" + STORAGE + ",", "libraries="), libraries,
                "both JNI entry libraries");
        assertRejected(good.replace("libraries=", "libraries=" + STORAGE + ","), libraries,
                "Duplicate native library");
    }

    @Test
    public void bundledSystemZlibIsRejectedBeforeExtraction() throws Exception {
        String zlib = systemZlib();
        Map<String, byte[]> libraries = libraries();
        libraries.put(zlib, bytes("a second zlib"));
        assertRejected(manifest(libraries), libraries, "System zlib must not be included");
        libraries.remove(zlib);
        assertRejected(manifest(libraries) + "aliases=" + zlib + "\nalias." + zlib + "=libcommon.so.1\n",
                libraries, "System zlib must not be included");
    }

    @Test
    public void entryLibraryNamesFollowThePlatform() throws Exception {
        String foreign = PLATFORM.startsWith("darwin-") ? "libmilvus-storage-jni.so" : "libmilvus-storage-jni.dylib";
        Map<String, byte[]> libraries = new LinkedHashMap<>();
        libraries.put(foreign, bytes("storage"));
        libraries.put(KNOWHERE, bytes("knowhere"));
        libraries.put("libcommon.so.1", bytes("common"));
        String manifest = manifest(libraries).replace("load.entries=" + STORAGE, "load.entries=" + foreign);
        assertRejected(manifest, libraries, "both JNI entry libraries");
    }

    @Test
    public void formatPlatformRevisionsFeatureAndDigestAreValidated() throws Exception {
        Map<String, byte[]> libraries = libraries();
        String good = manifest(libraries);
        assertRejected(good.replace("format.version=1", "format.version=2"), libraries, "format.version");
        assertRejected(good.replace("platform=" + PLATFORM, "platform=other"), libraries, "platform");
        assertRejected(good.replace("storage.revision=" + "1".repeat(40), "storage.revision=short"), libraries,
                "storage.revision");
        assertRejected(good.replace("knowhere.revision=" + "2".repeat(40), "knowhere.revision=short"), libraries,
                "knowhere.revision");
        assertRejected(good.replace("with_cardinal=true", "with_cardinal=yes"), libraries, "with_cardinal");
        assertRejected(good.replace("load.entries=" + STORAGE + "," + KNOWHERE,
                "load.entries=" + STORAGE), libraries, "two JVM load entries");
        assertRejected(good.replace("load.entries=", "ignored.load.entries="), libraries, "load.entries");
        assertRejected(good.replace("sha256." + STORAGE + "=", "ignored="), libraries, "sha256." + STORAGE);
        assertRejected(good + "with_cardinal=false\n", libraries, "Duplicate native manifest property");
    }

    @Test
    public void invalidPathsAreRejectedWithoutWritingOutsideTheDirectory() throws Exception {
        for (String path : List.of("../escape.so", "/absolute.so", "a//b.so", "a/./b.so", "a\\b.so",
                "%2e%2e/escape.so", "C:/escape.so", "")) {
            Map<String, byte[]> libraries = libraries();
            String good = manifest(libraries);
            assertRejected(good.replace("libraries=", "libraries=" + path.replace("\\", "\\\\") + ","), libraries,
                    "Invalid native resource path");
        }
    }

    @Test
    public void invalidAliasTargetsAndPathConflictsAreRejected() throws Exception {
        Map<String, byte[]> libraries = libraries();
        String good = manifest(libraries);
        assertRejected(good + "aliases=../escape.so\nalias.../escape.so=libcommon.so.1\n", libraries,
                "Invalid native resource path");
        assertRejected(good + "aliases=libcommon.so\nalias.libcommon.so=missing.so\n", libraries,
                "no canonical library");
        assertRejected(good + "aliases=" + STORAGE + "\nalias." + STORAGE + "=libcommon.so.1\n", libraries,
                "duplicates a library");
        assertRejected(good + "aliases=libcommon.so.1/nested.so\nalias.libcommon.so.1/nested.so=" + KNOWHERE + "\n",
                libraries, "also a directory");
    }

    private void assertRejected(String manifest, Map<String, byte[]> libraries, String expected) throws Exception {
        Path extraction = temporary.newFolder().toPath();
        UnsatisfiedLinkError failure = failure(loader(extraction, jar(manifest, libraries)));
        assertTrue(failure.getMessage(), failure.getMessage().contains(expected));
        assertEquals(0, children(extraction));
    }

    private static UnsatisfiedLinkError failure(NativeLibraries.Loader loader) {
        return assertThrows(UnsatisfiedLinkError.class, loader::get);
    }

    private NativeLibraries.Loader loader(Path extraction, Path... resources) throws Exception {
        URL[] urls = new URL[resources.length];
        for (int i = 0; i < resources.length; i++) urls[i] = resources[i].toUri().toURL();
        URLClassLoader loader = new URLClassLoader(urls, null);
        classLoaders.add(loader);
        return new NativeLibraries.Loader(loader, extraction);
    }

    private Path jar(String manifest, Map<String, byte[]> libraries) throws Exception {
        return jar(PREFIX, manifest, libraries, false);
    }

    private Path jar(String prefix, String manifest, Map<String, byte[]> libraries, boolean marker) throws Exception {
        Path path = temporary.newFile().toPath();
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(path))) {
            if (marker) entry(jar, "META-INF/milvus-native/provenance.json", bytes("{}"));
            if (manifest != null) entry(jar, prefix + "manifest.properties", bytes(manifest));
            for (Map.Entry<String, byte[]> library : libraries.entrySet()) {
                entry(jar, prefix + library.getKey(), library.getValue());
            }
        }
        return path;
    }

    private static void entry(JarOutputStream jar, String path, byte[] bytes) throws IOException {
        jar.putNextEntry(new JarEntry(path));
        jar.write(bytes);
        jar.closeEntry();
    }

    private static Map<String, byte[]> libraries() {
        Map<String, byte[]> result = new LinkedHashMap<>();
        result.put(STORAGE, bytes("storage"));
        result.put(KNOWHERE, bytes("knowhere"));
        result.put("libcommon.so.1", bytes("common"));
        return result;
    }

    private static String manifest(Map<String, byte[]> libraries) throws Exception {
        StringBuilder result = new StringBuilder("format.version=1\nplatform=" + PLATFORM + "\nstorage.revision="
                + "1".repeat(40) + "\nknowhere.revision=" + "2".repeat(40) + "\nwith_cardinal=true\nlibraries="
                + String.join(",", libraries.keySet())
                + "\nload.entries=" + STORAGE + "," + KNOWHERE + "\n");
        for (Map.Entry<String, byte[]> library : libraries.entrySet()) {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(library.getValue());
            StringBuilder hex = new StringBuilder();
            for (byte value : digest) hex.append(String.format("%02x", value & 0xff));
            result.append("sha256.").append(library.getKey()).append('=').append(hex).append('\n');
        }
        return result.toString();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void write(Path path, byte[] bytes) throws IOException {
        Files.createDirectories(path.getParent());
        Files.write(path, bytes);
    }

    private static long children(Path directory) throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.count();
        }
    }

    private static String platform() {
        String os = System.getProperty("os.name").toLowerCase();
        String arch = System.getProperty("os.arch").toLowerCase();
        if (os.contains("linux")) os = "linux";
        else if (os.contains("mac") || os.contains("darwin")) os = "darwin";
        else if (os.contains("windows")) os = "windows";
        if (arch.equals("amd64")) arch = "x86_64";
        else if (arch.equals("arm64")) arch = "aarch64";
        return os + "-" + arch;
    }

    /** The file name a shared library carries on the platform under test. */
    private static String libraryName(String base) {
        if (PLATFORM.startsWith("windows-")) return base + ".dll";
        if (PLATFORM.startsWith("darwin-")) return "lib" + base + ".dylib";
        return "lib" + base + ".so";
    }

    /** The system zlib name on the platform under test. */
    private static String systemZlib() {
        if (PLATFORM.startsWith("windows-")) return "zlib1.dll";
        if (PLATFORM.startsWith("darwin-")) return "libz.1.dylib";
        return "libz.so.1";
    }
}
