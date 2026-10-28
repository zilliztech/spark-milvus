package com.zilliz.spark.connector.demo;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Describes an existing snapshot through REST without starting Spark or changing server data.
 * The deployed server must expose this endpoint; not every Milvus 3.0 build does.
 * REST: https://github.com/milvus-io/milvus/blob/master/internal/distributed/proxy/httpserver/snapshot_handler_v2.go
 * RPC: milvus-proto/proto/milvus.proto, DescribeSnapshotResponse.s3_location.
 */
public final class SnapshotPath {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String DESCRIBE_PATH = "/v2/vectordb/snapshots/describe";

    private SnapshotPath() {}

    private static URI endpoint(String value) {
        try {
            URI base = URI.create(value);
            if (!("http".equalsIgnoreCase(base.getScheme()) || "https".equalsIgnoreCase(base.getScheme()))
                    || base.getHost() == null || base.getRawUserInfo() != null
                    || base.getRawQuery() != null || base.getRawFragment() != null
                    || base.getPort() > 65535) {
                throw new IllegalArgumentException();
            }
            // Append to the original raw URL so an optional proxy prefix is retained.
            return URI.create(value.replaceAll("/+$", "") + DESCRIBE_PATH);
        } catch (IllegalArgumentException | NullPointerException error) {
            throw new IllegalArgumentException("Use an HTTP(S) base URL without credentials, query or fragment");
        }
    }

    private static Map<String, String> arguments(String[] args) {
        Set<String> flags = Set.of("--uri", "--database", "--collection", "--snapshot", "--timeout", "--storage-options", "--output");
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!flags.contains(args[i]) || i + 1 >= args.length || result.containsKey(args[i])) {
                throw new IllegalArgumentException("Use each documented option once followed by its value; see --help");
            }
            result.put(args[i], args[i + 1]);
        }
        return result;
    }

    private static String nonblank(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(label + " is required and must be nonblank");
        }
        return value;
    }

    private static ObjectNode storageOptions(String filename) {
        if (filename == null) {
            return JSON.createObjectNode();
        }
        try {
            JsonNode values = JSON.readTree(Files.readString(Path.of(filename)));
            if (values == null || !values.isObject()) {
                throw new IllegalArgumentException("Storage options must be a JSON object");
            }
            var fields = values.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                if (!entry.getValue().isTextual() || !(entry.getKey().startsWith("fs.")
                        || entry.getKey().startsWith("extfs.") || entry.getKey().equals("milvus.snapshot.path"))) {
                    throw new IllegalArgumentException("Storage options must contain only fs.*/extfs.* strings and an optional milvus.snapshot.path");
                }
            }
            ObjectNode options = (ObjectNode) values;
            options.remove("milvus.snapshot.path");
            return options;
        } catch (IOException error) {
            throw new IllegalArgumentException("Cannot read --storage-options as a JSON object");
        }
    }

    private static String describe(URI uri, String database, String collection, String snapshot,
            Duration timeout, String token) throws InterruptedException {
        ObjectNode body = JSON.createObjectNode();
        body.put("dbName", database).put("collectionName", collection).put("snapshotName", snapshot);
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri).timeout(timeout)
                .header("Content-Type", "application/json").header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8));
        if (token != null && !token.isEmpty()) {
            if (token.chars().anyMatch(character -> character < 32 || character > 126)) {
                throw new IllegalArgumentException("MILVUS_TOKEN contains invalid header characters");
            }
            builder.header("Authorization", "Bearer " + token);
        }
        HttpClient client = HttpClient.newBuilder().connectTimeout(timeout)
                .followRedirects(HttpClient.Redirect.NEVER).build();
        HttpResponse<String> response;
        try {
            response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException error) {
            throw new IllegalArgumentException("Milvus HTTP request failed or timed out");
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalArgumentException("Milvus HTTP request failed with status " + response.statusCode()
                    + "; redirects are not followed");
        }
        JsonNode result;
        try {
            result = JSON.readTree(response.body());
        } catch (IOException error) {
            throw new IllegalArgumentException("Milvus returned invalid JSON");
        }
        JsonNode code = result == null ? null : result.get("code");
        if (code == null || !code.isNumber()) {
            throw new IllegalArgumentException("Milvus response must contain a numeric code");
        }
        if (code.decimalValue().compareTo(BigDecimal.ZERO) != 0) {
            throw new IllegalArgumentException("Milvus returned error code " + code.asText());
        }
        JsonNode location = result.path("data").path("s3Location");
        if (!location.isTextual() || location.textValue().isBlank()) {
            throw new IllegalArgumentException("Milvus returned no data.s3Location for this snapshot");
        }
        return location.textValue();
    }

    private static void run(String[] args) throws IOException, InterruptedException {
        Map<String, String> options = arguments(args);
        URI uri = endpoint(options.getOrDefault("--uri", System.getenv("MILVUS_URI")));
        String database = nonblank(options.getOrDefault("--database", "default"), "Database");
        String collection = nonblank(options.get("--collection"), "--collection");
        String snapshot = nonblank(options.get("--snapshot"), "--snapshot");
        double seconds;
        try {
            seconds = Double.parseDouble(options.getOrDefault("--timeout", "30"));
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException("--timeout must be a positive finite number of seconds");
        }
        if (!Double.isFinite(seconds) || seconds <= 0) {
            throw new IllegalArgumentException("--timeout must be a positive finite number of seconds");
        }
        Duration timeout = Duration.ofNanos(Math.max(1, Math.round(seconds * 1_000_000_000)));
        Path output = options.containsKey("--output") ? Path.of(options.get("--output")) : null;
        if (output != null && Files.exists(output, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("--output already exists; choose a new file");
        }
        ObjectNode result = storageOptions(options.get("--storage-options"));
        result.put("milvus.snapshot.path", describe(uri, database, collection, snapshot, timeout, System.getenv("MILVUS_TOKEN")));
        String encoded = JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result) + "\n";
        if (output == null) {
            System.out.print(encoded);
        } else {
            try (SeekableByteChannel channel = Files.newByteChannel(output,
                    Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW),
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))) {
                ByteBuffer buffer = StandardCharsets.UTF_8.encode(encoded);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            } catch (IOException error) {
                throw new IllegalArgumentException("Cannot create --output as a new private file");
            }
        }
    }

    public static void main(String[] args) {
        for (String arg : args) {
            if (arg.equals("--help") || arg.equals("-h")) {
                System.out.println("""
                        Describe an existing snapshot and print connector options as JSON.
                        Requires a deployed server exposing POST /v2/vectordb/snapshots/describe.
                        No snapshot creation, export, pinning or deletion is performed.
                        Options: --collection NAME --snapshot NAME [--database default]
                                 [--uri URL] [--timeout SECONDS] [--storage-options FILE] [--output FILE]
                        Environment: MILVUS_URI, optional MILVUS_TOKEN (Bearer authentication).
                        --output creates a new private file; existing files are never overwritten.
                        """);
                return;
            }
        }
        try {
            run(args);
        } catch (IllegalArgumentException error) {
            System.err.println("Snapshot path lookup failed: " + error.getMessage());
            System.exit(1);
        } catch (IOException error) {
            System.err.println("Snapshot path lookup failed: cannot encode connector options");
            System.exit(1);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            System.err.println("Snapshot path lookup failed: request interrupted");
            System.exit(1);
        }
    }
}
