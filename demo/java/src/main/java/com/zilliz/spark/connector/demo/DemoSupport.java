package com.zilliz.spark.connector.demo;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Shared input, Spark lifecycle and bounded checks; examples own their queries. */
public final class DemoSupport implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> READ_OPTIONS = Set.of(
        "milvus.snapshot.path", "milvus.filter", "milvus.partitions",
        "milvus.segments", "fieldIDs", "milvus.extra.columns", "milvus.read.apply.deletes",
        "milvus.read.vector.raw", "milvus.read.columnar", "milvus.read.batch.max.rows",
        "milvus.read.batch.max.bytes", "milvus.read.arrow.max.bytes");

    public SparkSession spark;
    public Map<String, String> options = new LinkedHashMap<>();
    public boolean fixture;
    public boolean explain;
    public String where;
    public int maxRows = 10000;
    private final Map<Long, JsonNode> expectedRows = new HashMap<>();

    private DemoSupport() {}

    public static DemoSupport open(String[] argv, String name, boolean snapshot,
                                   boolean filtering) throws Exception {
        DemoSupport demo = new DemoSupport();
        Set<String> allowed = new HashSet<>(Set.of("--explain"));
        if (snapshot) {
            allowed.addAll(Set.of("--options", "--max-rows"));
        }
        if (filtering) {
            allowed.add("--where");
        }

        Map<String, String> args = new HashMap<>();
        for (int i = 0; i < argv.length; i++) {
            String[] parts = argv[i].split("=", 2);
            String key = parts[0];
            valid(allowed.contains(key), "Unknown option: " + key);
            if (key.equals("--explain")) {
                valid(parts.length == 1, "--explain takes no value");
                demo.explain = true;
            } else {
                String value;
                if (parts.length == 2) {
                    value = parts[1];
                } else {
                    valid(i + 1 < argv.length && !argv[i + 1].startsWith("--"), "Missing value: " + key);
                    value = argv[++i];
                }
                valid(!value.isBlank(), "Empty value: " + key);
                args.put(key, value);
            }
        }
        demo.fixture = snapshot && !args.containsKey("--options");
        demo.where = args.get("--where");
        demo.maxRows = Integer.parseInt(args.getOrDefault("--max-rows", "10000"));
        valid(demo.maxRows > 0 && demo.maxRows <= 10000, "Require 0 < max-rows <= 10000");
        if (snapshot) {
            if (demo.fixture) {
                demo.loadFixture();
            } else {
                demo.options = readOptions(Path.of(args.get("--options")));
            }
        }

        demo.spark = SparkSession.builder().appName(name).getOrCreate();
        demo.spark.sparkContext().setLogLevel("WARN");
        try {
            valid(demo.spark.version().startsWith("4.0."), "These examples require Spark 4.0 / Scala 2.13");
            String master = demo.spark.sparkContext().master();
            valid(!demo.fixture || master.equals("local") || master.startsWith("local["),
                "The sample requires local Spark; use --options for shared storage on a cluster");
            return demo;
        } catch (RuntimeException error) {
            demo.close();
            throw error;
        }
    }

    private void loadFixture() throws Exception {
        String root = System.getenv("MILVUS_DEMO_ROOT");
        valid(root != null, "Use demo/run.sh to locate the committed sample");
        Path directory = Path.of(root, "core/src/test/resources/read-fixtures/collections/basic");
        Set<String> checked = new HashSet<>();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (String line : Files.readAllLines(directory.resolve("SHA256SUMS"))) {
            valid(line.matches("[0-9a-f]{64}  [A-Za-z0-9._/-]+"), "Malformed fixture checksum");
            String name = line.substring(66);
            Path relative = Path.of(name);
            valid(!relative.isAbsolute() && !name.equals("SHA256SUMS") && checked.add(name),
                "Unsafe or duplicate fixture path");
            for (String part : name.split("/", -1)) {
                valid(!part.isEmpty() && !part.equals(".") && !part.equals(".."), "Unsafe fixture path");
            }
            Path file = directory.resolve(relative);
            for (Path ancestor = file; ancestor != null; ancestor = ancestor.getParent()) {
                valid(!Files.isSymbolicLink(ancestor), "Fixture paths must not be symbolic links");
            }
            String actual = HexFormat.of().formatHex(digest.digest(Files.readAllBytes(file)));
            require(actual.equals(line.substring(0, 64)), "Fixture SHA256 mismatch: " + name);
        }
        try (Stream<Path> files = Files.walk(directory)) {
            Set<String> actual = new HashSet<>();
            files.filter(Files::isRegularFile).map(directory::relativize).map(Path::toString)
                .filter(name -> !name.equals("SHA256SUMS")).forEach(actual::add);
            require(checked.contains("README.md") && checked.equals(actual), "Unlisted fixture files");
        }
        for (JsonNode segment : JSON.readTree(directory.resolve("expected.json").toFile()).get("segments")) {
            Set<Long> deleted = new HashSet<>();
            segment.get("deletedIds").forEach(id -> deleted.add(id.longValue()));
            for (JsonNode row : segment.get("rows")) {
                if (!deleted.contains(row.get("id").longValue())) {
                    expectedRows.put(row.get("id").longValue(), row);
                }
            }
        }
        options.put("milvus.snapshot.path", "snapshot.json");
        options.put("fs.storage_type", "local");
        options.put("fs.root_path", directory.toString());
    }

    private static Map<String, String> readOptions(Path path) throws IOException {
        JsonNode input = JSON.readTree(path.toFile());
        valid(input != null && input.isObject(), "Options must be a JSON object");
        Map<String, String> result = new LinkedHashMap<>();
        input.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            JsonNode value = entry.getValue();
            valid(value.isTextual() && !value.textValue().isEmpty()
                && (READ_OPTIONS.contains(key) || key.startsWith("fs.") || key.startsWith("extfs.")),
                "Use documented snapshot/read and fs.*/extfs.* string options");
            result.put(key, value.textValue());
        });
        valid(result.containsKey("milvus.snapshot.path"), "Options must contain a fixed milvus.snapshot.path");
        return result;
    }





    public List<Row> boundedRows(Dataset<Row> frame) {
        List<Row> rows = frame.limit(maxRows + 1).collectAsList();
        valid(rows.size() <= maxRows, "Input exceeds --max-rows; use a smaller snapshot or add a filter");
        return rows;
    }

    public void checkFixtureRows(List<Row> rows) {
        Set<Long> ids = new HashSet<>();
        for (Row row : rows) {
            long id = row.getAs("id");
            JsonNode expected = expectedRows.get(id);
            require(expected != null && ids.add(id), "Unexpected or duplicate fixture ID");
            List<Float> vector = row.getList(row.fieldIndex("vector"));
            List<Float> wanted = new ArrayList<>();
            expected.get("vector").forEach(value -> wanted.add(value.floatValue()));
            Long category = expected.get("category").isNull() ? null : expected.get("category").longValue();
            require(vector.equals(wanted) && Objects.equals(row.getAs("category"), category),
                "Fixture vector or category differs");
        }
        require(ids.equals(expectedRows.keySet()), "Snapshot rows or deletes differ from fixture expectations");
    }







    private static void valid(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    @Override
    public void close() {
        spark.stop();
    }
}
