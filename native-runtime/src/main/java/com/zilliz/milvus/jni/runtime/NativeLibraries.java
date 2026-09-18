package com.zilliz.milvus.jni.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

/** Verifies and extracts one complete native bundle without loading JNI. */
public final class NativeLibraries {
    private static final String BUNDLE_MARKER = "META-INF/milvus-native/provenance.json";
    private static final Loader LOADER = new Loader(
            NativeLibraries.class.getClassLoader(), Paths.get(System.getProperty("java.io.tmpdir")));

    private NativeLibraries() {}

    /**
     * Returns the bundle shared by this defining class loader. Only an absent
     * bundle returns empty; unsupported platforms and incomplete bundles fail.
     */
    public static Optional<Bundle> bundled() {
        return LOADER.get();
    }

    /** An immutable result published only after every library has been verified. */
    public static final class Bundle {
        private final Path directory;
        private final Set<String> names;
        private final boolean cardinal;
        private final String storageRevision;
        private final String knowhereRevision;

        private Bundle(Path directory, Set<String> names, Properties manifest) {
            this.directory = directory;
            this.names = Collections.unmodifiableSet(new HashSet<>(names));
            this.cardinal = Boolean.parseBoolean(manifest.getProperty("with_cardinal"));
            this.storageRevision = manifest.getProperty("storage.revision");
            this.knowhereRevision = manifest.getProperty("knowhere.revision");
        }

        public Path directory() {
            return directory;
        }

        /** Returns only a library or alias declared in this verified bundle. */
        public Path library(String mappedLibraryName) {
            if (!names.contains(mappedLibraryName)) {
                throw new UnsatisfiedLinkError("Native bundle does not declare " + mappedLibraryName);
            }
            return directory.resolve(mappedLibraryName);
        }

        public boolean cardinalEnabled() {
            return cardinal;
        }

        public String storageRevision() {
            return storageRevision;
        }

        public String knowhereRevision() {
            return knowhereRevision;
        }
    }

    // The injectable resource loader also lets tests verify JAR and directory
    // resources without initializing either JNI API or mutating the classpath.
    static final class Loader {
        private final ClassLoader resources;
        private final Path temporaryRoot;
        private boolean initialized;
        private Optional<Bundle> result;
        private UnsatisfiedLinkError failure;

        Loader(ClassLoader resources, Path temporaryRoot) {
            this.resources = resources;
            this.temporaryRoot = temporaryRoot;
        }

        synchronized Optional<Bundle> get() {
            if (!initialized) {
                try {
                    result = discover();
                } catch (IOException | RuntimeException error) {
                    failure = new UnsatisfiedLinkError("Cannot prepare Milvus native bundle: " + error.getMessage());
                    failure.initCause(error);
                }
                initialized = true;
            }
            if (failure != null) throw failure;
            return result;
        }

        private Optional<Bundle> discover() throws IOException {
            String platform = platform();
            boolean bundleMarkerPresent = resources.getResources(BUNDLE_MARKER).hasMoreElements();
            if (platform == null) {
                require(!bundleMarkerPresent, "Milvus native bundle is present but the platform is unsupported: "
                        + System.getProperty("os.name") + "/" + System.getProperty("os.arch"));
                return Optional.empty();
            }
            String name = "native/milvus/1/" + platform + "/manifest.properties";
            Enumeration<URL> manifests = resources.getResources(name);
            if (!manifests.hasMoreElements()) {
                require(!bundleMarkerPresent, "Milvus native bundle is present but has no manifest for " + platform);
                return Optional.empty();
            }
            URL manifest = manifests.nextElement();
            if (manifests.hasMoreElements()) {
                throw new IOException("Multiple Milvus native manifests for " + platform);
            }
            return Optional.of(extract(manifest, platform, temporaryRoot));
        }
    }

    private static String platform() {
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch").toLowerCase(Locale.ROOT);
        if (os.contains("linux")) os = "linux";
        else if (os.contains("mac") || os.contains("darwin")) os = "darwin";
        else if (os.contains("windows")) os = "windows";
        else return null;
        if (arch.equals("amd64") || arch.equals("x86_64")) arch = "x86_64";
        else if (arch.equals("aarch64") || arch.equals("arm64")) arch = "aarch64";
        else if (arch.equals("x86") || arch.matches("i[3-6]86")) arch = "x86";
        else return null;
        return os + "-" + arch;
    }

    /** The file name a shared library carries on the named platform. */
    private static String libraryName(String platform, String base) {
        if (platform.startsWith("windows-")) return base + ".dll";
        if (platform.startsWith("darwin-")) return "lib" + base + ".dylib";
        return "lib" + base + ".so";
    }

    /** The system zlib the JVM has already loaded, which the bundle must not carry. */
    private static String systemZlib(String platform) {
        if (platform.startsWith("windows-")) return "zlib1.dll";
        if (platform.startsWith("darwin-")) return "libz.1.dylib";
        return "libz.so.1";
    }

    private static Bundle extract(URL resource, String platform, Path temporaryRoot) throws IOException {
        validateResource(resource);
        Properties manifest = new UniqueProperties();
        try (InputStream input = resource.openStream()) {
            manifest.load(input);
        }
        require("1".equals(manifest.getProperty("format.version")), "Unsupported native bundle format.version");
        require(platform.equals(manifest.getProperty("platform")), "Native bundle platform does not match " + platform);
        require(required(manifest, "storage.revision").matches("[a-f0-9]{40}"), "Invalid storage.revision");
        require(required(manifest, "knowhere.revision").matches("[a-f0-9]{40}"), "Invalid knowhere.revision");
        String cardinal = required(manifest, "with_cardinal");
        require(cardinal.equals("true") || cardinal.equals("false"), "Invalid with_cardinal");
        Set<String> libraries = names(required(manifest, "libraries"));
        Set<String> loadEntries = names(required(manifest, "load.entries"));
        Map<String, String> checksums = new LinkedHashMap<>();
        for (String name : libraries) {
            String checksum = required(manifest, "sha256." + name);
            require(checksum.matches("[a-f0-9]{64}"), "Invalid SHA-256 for " + name);
            checksums.put(name, checksum);
        }
        Map<String, String> aliases = new LinkedHashMap<>();
        String aliasList = manifest.getProperty("aliases", "");
        if (!aliasList.isEmpty()) {
            for (String alias : names(aliasList)) {
                require(!libraries.contains(alias), "Native alias duplicates a library: " + alias);
                String target = required(manifest, "alias." + alias);
                require(libraries.contains(target), "Native alias has no canonical library: " + alias);
                aliases.put(alias, target);
            }
        }
        Set<String> allNames = new LinkedHashSet<>(libraries);
        allNames.addAll(aliases.keySet());
        require(!allNames.contains(systemZlib(platform)), "System zlib must not be included in the native bundle");
        String storageEntry = libraryName(platform, "milvus-storage-jni");
        String knowhereEntry = libraryName(platform, "knowhere_jni");
        require(allNames.contains(storageEntry) && allNames.contains(knowhereEntry),
                "Native bundle must declare both JNI entry libraries");
        require(loadEntries.size() == 2 && loadEntries.contains(storageEntry)
                        && loadEntries.contains(knowhereEntry),
                "Native bundle must declare the two JVM load entries");
        for (String name : allNames) {
            int slash = name.indexOf('/');
            while (slash >= 0) {
                require(!allNames.contains(name.substring(0, slash)), "Native file is also a directory: " + name);
                slash = name.indexOf('/', slash + 1);
            }
        }

        Path directory = Files.createTempDirectory(temporaryRoot, "milvus-native-").toAbsolutePath();
        try {
            // Resolve every file relative to the selected manifest URL, never
            // through another classpath lookup that could mix resource JARs.
            URL base = new URL(resource, ".");
            for (Map.Entry<String, String> library : checksums.entrySet()) {
                Path output = directory.resolve(library.getKey());
                Files.createDirectories(output.getParent());
                MessageDigest digest = sha256();
                try (InputStream input = new URL(base, library.getKey()).openStream();
                        OutputStream destination = Files.newOutputStream(output)) {
                    byte[] bytes = new byte[65536];
                    int count;
                    while ((count = input.read(bytes)) != -1) {
                        digest.update(bytes, 0, count);
                        destination.write(bytes, 0, count);
                    }
                }
                require(library.getValue().equals(hex(digest.digest())), "Checksum mismatch for " + library.getKey());
            }
            for (Map.Entry<String, String> alias : aliases.entrySet()) {
                Path path = directory.resolve(alias.getKey());
                Files.createDirectories(path.getParent());
                Files.createLink(path, directory.resolve(alias.getValue()));
            }
            // Register parents before children; deleteOnExit removes in reverse
            // order, after this process is finished using the loaded libraries.
            try (Stream<Path> paths = Files.walk(directory)) {
                paths.forEach(path -> path.toFile().deleteOnExit());
            }
            return new Bundle(directory, allNames, manifest);
        } catch (IOException | RuntimeException error) {
            try {
                remove(directory);
            } catch (IOException cleanup) {
                error.addSuppressed(cleanup);
            }
            throw error;
        }
    }

    private static void validateResource(URL resource) throws IOException {
        if (resource.getProtocol().equals("file")) return;
        require(resource.getProtocol().equals("jar"), "Unsupported native resource protocol: " + resource.getProtocol());
        JarURLConnection connection = (JarURLConnection) resource.openConnection();
        connection.setUseCaches(false);
        String prefix = connection.getEntryName();
        prefix = prefix.substring(0, prefix.lastIndexOf('/') + 1);
        try (JarFile jar = connection.getJarFile()) {
            Set<String> entries = new HashSet<>();
            Enumeration<JarEntry> enumeration = jar.entries();
            while (enumeration.hasMoreElements()) {
                String name = enumeration.nextElement().getName();
                if (name.startsWith(prefix)) {
                    require(entries.add(name), "Duplicate native JAR entry: " + name);
                }
            }
        }
    }

    private static Set<String> names(String value) throws IOException {
        Set<String> names = new LinkedHashSet<>();
        for (String name : value.split(",", -1)) {
            for (String component : name.split("/", -1)) {
                require(component.matches("[A-Za-z0-9_+.-]+") && !component.equals(".") && !component.equals(".."),
                        "Invalid native resource path: " + name);
            }
            require(names.add(name), "Duplicate native library: " + name);
        }
        return names;
    }

    private static String required(Properties values, String name) throws IOException {
        String value = values.getProperty(name);
        require(value != null && !value.isEmpty(), "Missing native manifest property: " + name);
        return value;
    }

    private static void require(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("JVM does not provide SHA-256", error);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 15, 16));
            result.append(Character.forDigit(value & 15, 16));
        }
        return result.toString();
    }

    private static void remove(Path directory) throws IOException {
        List<Path> files = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(directory)) {
            paths.forEach(files::add);
        }
        Collections.reverse(files);
        for (Path path : files) Files.delete(path);
    }

    private static final class UniqueProperties extends Properties {
        private static final long serialVersionUID = 1L;

        @Override
        public synchronized Object put(Object key, Object value) {
            if (containsKey(key)) throw new IllegalArgumentException("Duplicate native manifest property: " + key);
            return super.put(key, value);
        }
    }
}
