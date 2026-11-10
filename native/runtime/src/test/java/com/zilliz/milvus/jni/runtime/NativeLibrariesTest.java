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
    private static final String COMPUTE = libraryName("compute_jni");
    private static final String PREFIX = "native/milvus/2/" + PLATFORM + "/";
    private static final String STORAGE_CONTRACT = "META-INF/milvus-native/requirements/storage.properties";

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
        Path resource = jar("native/milvus/2/" + otherPlatform + "/",
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
        assertArrayEquals(libraries.get(COMPUTE), Files.readAllBytes(bundle.library(COMPUTE)));
        assertEquals(1, children(extraction));
    }

    @Test
    public void storageOnlyBundleNeedsOnlyItsBindingContract() throws Exception {
        Map<String, byte[]> libraries = Map.of(STORAGE, bytes("storage"));
        String manifest = manifest(libraries).replace("components=storage,compute", "components=storage")
                .replace("load.entries=" + STORAGE + "," + COMPUTE, "load.entries=" + STORAGE);
        Path contracts = temporary.newFile().toPath();
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(contracts))) {
            entry(jar, "META-INF/milvus-native/requirements.list", bytes("storage\n"));
            entry(jar, STORAGE_CONTRACT, bytes(contract()));
        }
        NativeLibraries.Bundle bundle = resourceLoader(temporary.newFolder().toPath(), contracts,
                jar(manifest, libraries)).get().get();
        assertEquals("1".repeat(40), bundle.revision("storage"));
        assertEquals(bundle.library(STORAGE), bundle.entry("milvus-storage-jni"));
    }

    @Test
    public void otherPlatformsCanCoexistWithoutSupplyingCurrentPlatformResources() throws Exception {
        String otherPlatform = PLATFORM.equals("linux-x86_64") ? "linux-aarch64" : "linux-x86_64";
        Map<String, byte[]> libraries = libraries();
        Path other = jar("native/milvus/2/" + otherPlatform + "/",
                manifest(libraries).replace("platform=" + PLATFORM, "platform=" + otherPlatform),
                Map.of(STORAGE, bytes("other platform")), true);
        Path current = jar(PREFIX, manifest(libraries), libraries, true);
        Path extraction = temporary.newFolder().toPath();
        NativeLibraries.Bundle bundle = loader(extraction, other, current).get().get();
        assertArrayEquals(libraries.get(STORAGE), Files.readAllBytes(bundle.library(STORAGE)));
        assertEquals(1, children(extraction));
    }

    @Test
    public void mismatchedEngineRevisionsAreRejectedBeforeExtraction() throws Exception {
        Map<String, byte[]> libraries = libraries();
        String good = manifest(libraries);
        assertRejected(good.replace("storage.revision=" + "1".repeat(40),
                "storage.revision=" + "3".repeat(40)), libraries,
                "storage.revision does not match JVM bindings");
        assertRejected(good.replace("compute.revision=" + "2".repeat(40),
                "compute.revision=" + "3".repeat(40)), libraries,
                "compute.revision does not match JVM bindings");
    }

    @Test
    public void bundleRequiresOneValidJvmComponentContractResource() throws Exception {
        Map<String, byte[]> libraries = libraries();
        Path bundle = jar(manifest(libraries), libraries);
        Path extraction = temporary.newFolder().toPath();
        assertTrue(failure(resourceLoader(extraction, bundle)).getMessage()
                .contains("Missing JVM native component requirements"));
        Path first = contractJar(contract());
        Path second = contractJar(contract());
        assertTrue(failure(resourceLoader(extraction, first, second, bundle)).getMessage()
                .contains("Multiple JVM native component contracts"));
        for (String invalid : List.of(
                contract().replace("format.version=2", "format.version=3"),
                contract().replace("revision=", "unknown="),
                contract().replace("revision=" + "1".repeat(40), "revision=short"))) {
            String message = failure(resourceLoader(extraction, contractJar(invalid), bundle)).getMessage();
            assertTrue(message, message.contains("JVM native component"));
        }
        assertEquals(0, children(extraction));
    }

    @Test
    public void absentBundleDoesNotRequireJvmComponentContractMetadata() throws Exception {
        Path extraction = temporary.newFolder().toPath();
        assertFalse(resourceLoader(extraction, contractJar("invalid metadata")).get().isPresent());
        assertEquals(0, children(extraction));
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

        assertEquals(bundle.library(STORAGE).getParent(), bundle.library(COMPUTE).getParent());
        assertEquals(bundle.directory(), bundle.library(COMPUTE).getParent());
        assertArrayEquals(bytes("provider"), Files.readAllBytes(bundle.library("ossl-modules/legacy.so")));
        assertTrue(Files.isSameFile(bundle.library("libcommon.so"), bundle.library("libcommon.so.1")));
        assertTrue(bundle.featureEnabled("extension"));
        assertEquals("1".repeat(40), bundle.revision("storage"));
        assertEquals("2".repeat(40), bundle.revision("compute"));
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
        libraries.put(COMPUTE + ".1", libraries.remove(COMPUTE));
        String manifest = manifest(libraries) + "aliases=" + STORAGE + "," + COMPUTE
                + "\nalias." + STORAGE + "=" + STORAGE + ".1\nalias." + COMPUTE + "=" + COMPUTE + ".1\n";
        NativeLibraries.Bundle bundle = loader(temporary.newFolder().toPath(), jar(manifest, libraries)).get().get();
        assertTrue(Files.isSameFile(bundle.library(STORAGE), bundle.library(STORAGE + ".1")));
        assertTrue(Files.isSameFile(bundle.library(COMPUTE), bundle.library(COMPUTE + ".1")));
        assertEquals(bundle.library(STORAGE).getParent(), bundle.library(COMPUTE).getParent());
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
                String entry = i % 2 == 0 ? STORAGE : COMPUTE;
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
    public void childBindingMustValidateItsOwnContractAfterParentExtraction() throws Exception {
        Path extraction = temporary.newFolder().toPath();
        Map<String, byte[]> libraries = libraries();
        Path storageContracts = temporary.newFolder().toPath();
        write(storageContracts.resolve("META-INF/milvus-native/requirements.list"), bytes("storage\n"));
        write(storageContracts.resolve(STORAGE_CONTRACT), bytes(contract()));
        URLClassLoader parent = new URLClassLoader(new URL[] {
                storageContracts.toUri().toURL(), jar(manifest(libraries), libraries).toUri().toURL()}, null);
        classLoaders.add(parent);
        NativeLibraries.Loader loader = new NativeLibraries.Loader(parent, extraction);
        NativeLibraries.Bundle prepared = loader.get(parent, "storage").get();
        Path childContracts = temporary.newFolder().toPath();
        write(childContracts.resolve("META-INF/milvus-native/requirements.list"), bytes("compute\n"));
        write(childContracts.resolve("META-INF/milvus-native/requirements/compute.properties"),
                bytes("format.version=2\nrevision=" + "3".repeat(40) + "\nentry=compute_jni\n"));
        URLClassLoader child = new URLClassLoader(new URL[] {childContracts.toUri().toURL()}, parent);
        classLoaders.add(child);
        UnsatisfiedLinkError error = assertThrows(UnsatisfiedLinkError.class, () -> loader.get(child, "compute"));
        assertTrue(error.getMessage(), error.getMessage().contains("compute.revision does not match"));
        assertSame(prepared, loader.get(parent, "storage").get());
        assertEquals(1, children(extraction));
    }

    @Test
    public void laterContractAndMissingExplicitBindingCannotUseCachedBundle() throws Exception {
        Path extraction = temporary.newFolder().toPath();
        Path contracts = temporary.newFolder().toPath();
        write(contracts.resolve("META-INF/milvus-native/requirements.list"), bytes("storage\n"));
        write(contracts.resolve(STORAGE_CONTRACT), bytes(contract()));
        Map<String, byte[]> libraries = libraries();
        URLClassLoader resources = new URLClassLoader(new URL[] {
                contracts.toUri().toURL(), jar(manifest(libraries), libraries).toUri().toURL()}, null);
        classLoaders.add(resources);
        NativeLibraries.Loader loader = new NativeLibraries.Loader(resources, extraction);
        loader.get(resources, "storage");
        assertTrue(assertThrows(UnsatisfiedLinkError.class, () -> loader.get(resources, "compute"))
                .getMessage().contains("contract for binding: compute"));
        write(contracts.resolve("META-INF/milvus-native/requirements.list"), bytes("storage\ncompute\n"));
        write(contracts.resolve("META-INF/milvus-native/requirements/compute.properties"),
                bytes("format.version=2\nrevision=" + "3".repeat(40) + "\nentry=compute_jni\n"));
        assertTrue(assertThrows(UnsatisfiedLinkError.class, loader::get)
                .getMessage().contains("compute.revision does not match"));
        assertEquals(1, children(extraction));
    }

    @Test
    public void absenceIsNotCachedWhenBindingResourcesAppearLater() throws Exception {
        Path classes = temporary.newFolder().toPath();
        NativeLibraries.Loader loader = loader(temporary.newFolder().toPath(), classes);
        assertFalse(loader.get().isPresent());
        Map<String, byte[]> libraries = libraries();
        write(classes.resolve(PREFIX + "manifest.properties"), bytes(manifest(libraries)));
        for (Map.Entry<String, byte[]> library : libraries.entrySet()) {
            write(classes.resolve(PREFIX + library.getKey()), library.getValue());
        }
        assertTrue(loader.get().isPresent());
    }

    @Test
    public void identicalManifestsInTwoJarsAreRejected() throws Exception {
        Map<String, byte[]> libraries = libraries();
        Path extraction = temporary.newFolder().toPath();
        Path first = jar(manifest(libraries), libraries);
        Path second = jar(manifest(libraries), libraries);
        String message = failure(loader(extraction, first, second)).getMessage();
        assertTrue(message, message.contains("Multiple Milvus native manifests"));
        assertTrue(message, message.contains(first.getFileName().toString()));
        assertTrue(message, message.contains(second.getFileName().toString()));
        assertTrue(message, message.contains("spark.executor.userClassPathFirst"));
        assertEquals(0, children(extraction));
    }

    @Test
    public void checksumFailureRemovesPartialFilesAndRemainsAFailure() throws Exception {
        Map<String, byte[]> libraries = libraries();
        String manifest = manifest(libraries);
        libraries.put(COMPUTE, bytes("corrupted"));
        Path extraction = temporary.newFolder().toPath();
        NativeLibraries.Loader loader = loader(extraction, jar(manifest, libraries));
        UnsatisfiedLinkError failure = failure(loader);
        assertTrue(failure.getMessage().contains("Checksum mismatch for " + COMPUTE));
        assertSame(failure, failure(loader));
        assertEquals(0, children(extraction));
    }

    @Test
    public void missingFileCannotBeSuppliedByAnotherClasspathJar() throws Exception {
        Map<String, byte[]> libraries = libraries();
        String manifest = manifest(libraries);
        byte[] missing = libraries.remove(COMPUTE);
        Path supplyingJar = jar(null, Map.of(COMPUTE, missing));
        Path extraction = temporary.newFolder().toPath();
        NativeLibraries.Loader loader = loader(extraction, jar(manifest, libraries), supplyingJar);
        assertTrue(failure(loader).getMessage().contains(COMPUTE));
        assertEquals(0, children(extraction));
    }

    @Test
    public void missingEntryAndDuplicateLibrariesAreRejectedBeforeExtraction() throws Exception {
        Map<String, byte[]> libraries = libraries();
        String good = manifest(libraries);
        assertRejected(good.replace("libraries=" + STORAGE + ",", "libraries="), libraries,
                "load entries must name declared libraries");
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
        libraries.put(COMPUTE, bytes("compute"));
        libraries.put("libcommon.so.1", bytes("common"));
        String manifest = manifest(libraries).replace("load.entries=" + STORAGE, "load.entries=" + foreign);
        assertRejected(manifest, libraries, "JVM load entry for storage");
    }

    @Test
    public void formatPlatformRevisionsFeatureAndDigestAreValidated() throws Exception {
        Map<String, byte[]> libraries = libraries();
        String good = manifest(libraries);
        assertRejected(good.replace("format.version=2", "format.version=3"), libraries, "format.version");
        assertRejected(good.replace("platform=" + PLATFORM, "platform=other"), libraries, "platform");
        assertRejected(good.replace("storage.revision=" + "1".repeat(40), "storage.revision=short"), libraries,
                "component revision: storage");
        assertRejected(good.replace("compute.revision=" + "2".repeat(40), "compute.revision=short"), libraries,
                "component revision: compute");
        assertRejected(good.replace("feature.extension=true", "feature.extension=yes"), libraries, "feature.extension");
        assertRejected(good.replace("load.entries=" + STORAGE + "," + COMPUTE,
                "load.entries=" + STORAGE), libraries, "JVM load entry for compute");
        assertRejected(good.replace("load.entries=", "ignored.load.entries="), libraries, "load.entries");
        assertRejected(good.replace("sha256." + STORAGE + "=", "ignored="), libraries, "sha256." + STORAGE);
        assertRejected(good + "feature.extension=false\n", libraries, "Duplicate native manifest property");
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
        assertRejected(good + "aliases=libcommon.so.1/nested.so\nalias.libcommon.so.1/nested.so=" + COMPUTE + "\n",
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
        Path[] classpath = new Path[resources.length + 1];
        classpath[0] = contractJar(contract());
        System.arraycopy(resources, 0, classpath, 1, resources.length);
        return resourceLoader(extraction, classpath);
    }

    private NativeLibraries.Loader resourceLoader(Path extraction, Path... resources) throws Exception {
        URL[] urls = new URL[resources.length];
        for (int i = 0; i < resources.length; i++) urls[i] = resources[i].toUri().toURL();
        URLClassLoader loader = new URLClassLoader(urls, null);
        classLoaders.add(loader);
        return new NativeLibraries.Loader(loader, extraction);
    }

    private Path contractJar(String content) throws Exception {
        Path path = temporary.newFile().toPath();
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(path))) {
            entry(jar, "META-INF/milvus-native/requirements.list", bytes("storage\ncompute\n"));
            entry(jar, STORAGE_CONTRACT, bytes(content));
            entry(jar, "META-INF/milvus-native/requirements/compute.properties",
                    bytes("format.version=2\nrevision=" + "2".repeat(40) + "\nentry=compute_jni\n"));
        }
        return path;
    }

    private static String contract() {
        return "format.version=2\nrevision=" + "1".repeat(40) + "\nentry=milvus-storage-jni\n";
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
        result.put(COMPUTE, bytes("compute"));
        result.put("libcommon.so.1", bytes("common"));
        return result;
    }

    private static String manifest(Map<String, byte[]> libraries) throws Exception {
        StringBuilder result = new StringBuilder("format.version=2\ncomponents=storage,compute\nplatform=" + PLATFORM + "\nstorage.revision="
                + "1".repeat(40) + "\ncompute.revision=" + "2".repeat(40) + "\nfeature.extension=true\nlibraries="
                + String.join(",", libraries.keySet())
                + "\nload.entries=" + STORAGE + "," + COMPUTE + "\n");
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
