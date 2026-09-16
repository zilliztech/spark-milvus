package com.zilliz.milvus.jni.vector;

import io.knowhere.Knowhere;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Loader failures run in fresh JVMs because upstream JNI uses class initialization. */
public class NativeVectorLibraryTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void initializingConnectorClassDoesNotLoadJni() throws Exception {
        assertTrue(runProbe(null, "class-only").contains("CLASS_ONLY_OK"));
    }

    @Test
    public void missingNativeJarFailsWithItsResourcePath() throws Exception {
        String output = runProbe(null, "failure", "-Dos.name=Linux", "-Dos.arch=amd64");
        assertTrue(output, output.contains("native/knowhere/1/linux-x86_64/manifest.properties"));
        assertTrue(output, output.contains("add the matching native platform JAR"));
    }

    @Test
    public void explicitLibraryPathMustBeAbsolute() throws Exception {
        String output = runProbe(null, "failure", "-Dknowhere.native.path=relative.so");
        assertTrue(output, output.contains("existing absolute library path"));
    }

    @Test
    public void unsupportedBundledPlatformFailsClearly() throws Exception {
        String output = runProbe(null, "failure", "-Dos.name=Windows");
        assertTrue(output, output.contains("No bundled Knowhere native package for windows"));
    }

    @Test
    public void corruptedNativeResourceIsRejectedBeforeLoading() throws Exception {
        Path jar = temporary.newFile("corrupt-native.jar").toPath();
        String prefix = "native/knowhere/1/linux-x86_64/";
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            entry(output, prefix + "manifest.properties", "cAbiVersion=1\n"
                    + "entryLibrary=libknowhere_jni.so\nlibraries=libknowhere_jni.so\n"
                    + "sha256.libknowhere_jni.so=" + "0".repeat(64) + "\n");
            entry(output, prefix + "libknowhere_jni.so", "corrupt native data");
        }
        String output = runProbe(jar, "failure", "-Dos.name=Linux", "-Dos.arch=amd64");
        assertTrue(output, output.contains("Checksum mismatch for libknowhere_jni.so"));
    }

    @Test
    public void incompatibleNativeManifestAbiIsRejected() throws Exception {
        Path jar = temporary.newFile("incompatible-abi.jar").toPath();
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            entry(output, "native/knowhere/1/linux-x86_64/manifest.properties",
                    "cAbiVersion=2\nentryLibrary=libknowhere_jni.so\n"
                            + "libraries=libknowhere_jni.so\n");
        }
        String output = runProbe(jar, "failure", "-Dos.name=Linux", "-Dos.arch=amd64");
        assertTrue(output, output.contains("Invalid Knowhere native manifest"));
    }

    private static void entry(JarOutputStream output, String name, String value) throws Exception {
        output.putNextEntry(new JarEntry(name));
        output.write(value.getBytes(StandardCharsets.UTF_8));
        output.closeEntry();
    }

    private String runProbe(Path resourceJar, String mode, String... options) throws Exception {
        // Build an API-only classpath even when the parent build has a native JAR.
        String classpath = location(NativeVectorLibrary.class) + File.pathSeparator
                + location(Knowhere.class) + File.pathSeparator + location(LoaderProbe.class);
        if (resourceJar != null) {
            classpath += File.pathSeparator + resourceJar;
        }
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.addAll(Arrays.asList(options));
        command.add("-Djava.io.tmpdir=" + temporary.newFolder("extract-" + System.nanoTime()));
        command.add("-cp");
        command.add(classpath);
        command.add(LoaderProbe.class.getName());
        command.add(mode);
        Path log = temporary.newFile("probe-" + System.nanoTime() + ".log").toPath();
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(log.toFile());
        builder.environment().remove("JAVA_TOOL_OPTIONS");
        builder.environment().remove("JDK_JAVA_OPTIONS");
        builder.environment().remove("_JAVA_OPTIONS");
        Process process = builder.start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("Loader probe timed out: " + Files.readString(log));
        }
        String output = Files.readString(log);
        assertEquals(output, 0, process.exitValue());
        return output;
    }

    private static String location(Class<?> type) throws Exception {
        return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
    }

    public static final class LoaderProbe {
        public static void main(String[] args) throws Exception {
            if (args[0].equals("class-only")) {
                Class.forName(NativeVectorLibrary.class.getName());
                System.out.println("CLASS_ONLY_OK");
                return;
            }
            try {
                NativeVectorLibrary.load();
            } catch (LinkageError expected) {
                expected.printStackTrace(System.out);
                return;
            }
            throw new AssertionError("Loading without a valid native library unexpectedly succeeded");
        }
    }
}
