package com.zilliz.milvus.jni.storage;

import com.zilliz.milvus.jni.runtime.NativeLibraries;
import io.milvus.storage.NativeLibraryLoader;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Forks isolate the process-wide upstream loader and shared runtime state. */
public class NativeStorageLibraryTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void unifiedBundleHandsOffItsStorageEntryAndRestoresTheProperty() throws Exception {
        String output = runProbe(bundle(false), "unified-path");
        assertTrue(output, output.contains("UNIFIED_PATH_OK"));
        assertTrue(output, output.contains("PATH_RESTORED"));
        assertTrue(output, output.contains("STORAGE_FAILED_CLOSED"));
    }

    @Test
    public void explicitPathToTheSameVerifiedEntryIsAcceptedAndPreserved() throws Exception {
        String output = runProbe(bundle(false), "unified-same-path");
        assertTrue(output, output.contains("UNIFIED_PATH_OK"));
        assertTrue(output, output.contains("PATH_RESTORED"));
        assertTrue(output, !output.contains("conflicts with the unified"));
    }

    @Test
    public void conflictingExplicitPathIsRejectedBeforeStorageLoads() throws Exception {
        Path external = temporary.newFile("external.so").toPath();
        String output = runProbe(bundle(false), "failure", "-Dmilvus.storage.native.path=" + external);
        assertTrue(output, output.contains("milvus.storage.native.path conflicts with the unified Milvus native bundle"));
        assertTrue(output, output.contains("STORAGE_FAILED_CLOSED"));
    }

    @Test
    public void invalidBundleDoesNotFallBackToAnotherStorageLibrary() throws Exception {
        String output = runProbe(bundle(true), "failure");
        assertTrue(output, output.contains("Checksum mismatch for libknowhere_jni.so"));
        assertTrue(output, output.contains("STORAGE_FAILED_CLOSED"));
    }

    private String runProbe(Path bundle, String mode, String... options) throws Exception {
        String classpath = location(NativeLibraryLoader.class) + File.pathSeparator
                + location(NativeLibraries.class) + File.pathSeparator + location(NativeStorageLibrary.class)
                + File.pathSeparator + location(Probe.class) + File.pathSeparator + bundle;
        Path log = temporary.newFile().toPath();
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Dos.name=Linux");
        command.add("-Dos.arch=amd64");
        command.addAll(List.of(options));
        command.add("-Djava.io.tmpdir=" + temporary.newFolder());
        command.add("-cp");
        command.add(classpath);
        command.add(Probe.class.getName());
        command.add(mode);
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().remove("JAVA_TOOL_OPTIONS");
        builder.environment().remove("JDK_JAVA_OPTIONS");
        builder.environment().remove("_JAVA_OPTIONS");
        Process process = builder.start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("Storage loader probe timed out");
        }
        String output = Files.readString(log);
        assertEquals(output, 0, process.exitValue());
        return output;
    }

    private Path bundle(boolean corrupt) throws Exception {
        Path jar = temporary.newFile("unified-" + System.nanoTime() + ".jar").toPath();
        String prefix = "native/milvus/1/linux-x86_64/";
        String libraries = "libmilvus-storage-jni.so,libknowhere_jni.so";
        StringBuilder manifest = new StringBuilder("format.version=1\nplatform=linux-x86_64\n"
                + "storage.revision=" + "1".repeat(40) + "\nknowhere.revision=" + "2".repeat(40)
                + "\nwith_cardinal=false\nlibraries=" + libraries
                + "\nload.entries=libmilvus-storage-jni.so,libknowhere_jni.so\n");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            for (String library : libraries.split(",")) {
                byte[] checksum = MessageDigest.getInstance("SHA-256")
                        .digest(library.getBytes(StandardCharsets.UTF_8));
                StringBuilder hex = new StringBuilder();
                for (byte value : checksum) hex.append(String.format("%02x", value & 0xff));
                manifest.append("sha256.").append(library).append('=').append(hex).append('\n');
                entry(output, prefix + library,
                        corrupt && library.equals("libknowhere_jni.so") ? "corrupt" : library);
            }
            entry(output, prefix + "manifest.properties", manifest.toString());
        }
        return jar;
    }

    private static void entry(JarOutputStream output, String path, String content) throws Exception {
        output.putNextEntry(new JarEntry(path));
        output.write(content.getBytes(StandardCharsets.UTF_8));
        output.closeEntry();
    }

    private static String location(Class<?> type) throws Exception {
        return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
    }

    public static final class Probe {
        public static void main(String[] arguments) {
            if (arguments[0].equals("unified-same-path")) {
                System.setProperty("milvus.storage.native.path",
                        NativeLibraries.bundled().get().directory().resolve(".")
                                .resolve("libmilvus-storage-jni.so").toString());
            }
            String originalPath = System.getProperty("milvus.storage.native.path");
            try {
                NativeStorageLibrary.load();
                throw new AssertionError("Inert storage JNI fixture unexpectedly loaded");
            } catch (UnsatisfiedLinkError expected) {
                if (!Objects.equals(originalPath, System.getProperty("milvus.storage.native.path"))) {
                    throw new AssertionError("Storage initialization changed the application's native path override");
                }
                System.out.println("PATH_RESTORED");
                if (!NativeLibraryLoader.isLoaded()) System.out.println("STORAGE_FAILED_CLOSED");
                if (arguments[0].startsWith("unified-")) {
                    Path entry = NativeLibraries.bundled().get().library("libmilvus-storage-jni.so");
                    if (!expected.getMessage().contains(entry.toString())) {
                        throw new AssertionError("milvus-storage did not receive the verified shared entry path");
                    }
                    System.out.println("UNIFIED_PATH_OK");
                }
                expected.printStackTrace(System.out);
            }
        }
    }
}
