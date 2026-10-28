package com.zilliz.spark.connector.demo;

import scala.Tuple2;
import scala.collection.Iterator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/** Shared input, Spark lifecycle and bounded checks; examples own their queries. */
public final class DemoSupport implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> READ_OPTIONS = Set.of(
        "milvus.snapshot.path", "milvus.filter", "milvus.search.params", "milvus.partitions",
        "milvus.segments", "fieldIDs", "milvus.extra.columns", "milvus.read.apply.deletes",
        "milvus.read.vector.raw", "milvus.read.columnar", "milvus.read.batch.max.rows",
        "milvus.read.batch.max.bytes", "milvus.read.arrow.max.bytes");

    public SparkSession spark;
    public Map<String, String> options = new LinkedHashMap<>();
    public boolean fixture;
    public boolean explain;
    public String where;
    public String idColumn = "id";
    public String vectorColumn = "vector";
    public int k = 3;
    public int maxRows = 10000;
    private final Map<Long, List<Float>> queryVectors = new LinkedHashMap<>();
    private final Map<Long, JsonNode> expectedRows = new HashMap<>();

    private DemoSupport() {}

    public static DemoSupport open(String[] argv, String name, boolean snapshot,
                                   boolean search, boolean filtering) throws Exception {
        DemoSupport demo = new DemoSupport();
        Set<String> allowed = new HashSet<>(Set.of("--explain"));
        if (snapshot) {
            allowed.addAll(Set.of("--options", "--max-rows"));
        }
        if (search || filtering) {
            allowed.add("--where");
        }
        if (search) {
            allowed.addAll(Set.of("--queries", "--id-column", "--vector-column", "--k"));
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
        if (search) {
            demo.k = Integer.parseInt(args.getOrDefault("--k", "3"));
            demo.idColumn = args.getOrDefault("--id-column", "id");
            demo.vectorColumn = args.getOrDefault("--vector-column", "vector");
            valid(demo.k > 0 && demo.k <= demo.maxRows, "Require 0 < k <= max-rows");
            valid(demo.idColumn.matches("[A-Za-z_][A-Za-z_0-9]*")
                && demo.vectorColumn.matches("[A-Za-z_][A-Za-z_0-9]*"), "Use simple column names");
            valid(!demo.fixture || (demo.idColumn.equals("id") && demo.vectorColumn.equals("vector")),
                "The fixture uses id and vector columns");
            valid(!args.containsKey("--options") || args.containsKey("--queries"),
                "--queries is required with --options");
            if (args.containsKey("--queries")) {
                demo.loadQueries(Path.of(args.get("--queries")));
            } else {
                demo.queryVectors.put(1L, List.of(0.0f, 0.0f));
                demo.queryVectors.put(2L, List.of(7.0f, 1.0f));
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
        Path directory = Path.of(root, "core/src/test/resources/read-fixtures/collections/nearest");
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
                "Use documented snapshot/read/search and fs.*/extfs.* string options");
            result.put(key, value.textValue());
        });
        valid(result.containsKey("milvus.snapshot.path"), "Options must contain a fixed milvus.snapshot.path");
        return result;
    }

    private void loadQueries(Path path) throws IOException {
        JsonNode input = JSON.readTree(path.toFile());
        valid(input != null && input.isArray() && !input.isEmpty() && input.size() <= 100,
            "Supply 1 to 100 query vectors");
        int dimension = -1;
        for (JsonNode query : input) {
            valid(query.isObject() && query.size() == 2 && query.has("qid") && query.has("vector"),
                "Each query needs qid and vector");
            JsonNode id = query.get("qid");
            valid(id.isIntegralNumber() && id.canConvertToLong() && !queryVectors.containsKey(id.longValue()),
                "Query ids must be unique signed 64-bit integers");
            JsonNode vector = query.get("vector");
            valid(vector.isArray() && !vector.isEmpty(), "Queries need finite float32 vectors");
            List<Float> values = new ArrayList<>();
            for (JsonNode value : vector) {
                valid(value.isNumber() && Double.isFinite(value.doubleValue())
                    && Math.abs(value.doubleValue()) <= Float.MAX_VALUE, "Queries need finite float32 vectors");
                // Verify the same float32 values that Spark receives, including JSON rounding.
                values.add(value.floatValue());
            }
            valid(dimension == -1 || dimension == values.size(), "Query dimensions differ");
            dimension = values.size();
            queryVectors.put(id.longValue(), values);
        }
    }

    public Dataset<Row> queries() {
        List<Row> rows = new ArrayList<>();
        queryVectors.forEach((id, vector) -> rows.add(RowFactory.create(id, vector)));
        StructType schema = new StructType().add("qid", DataTypes.LongType, false)
            .add("qvec", DataTypes.createArrayType(DataTypes.FloatType, false), false);
        return spark.createDataFrame(rows, schema);
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

    public void checkSearch(Dataset<Row> frame, Dataset<Row> base, boolean exact,
                            boolean requireIndex) throws IOException {
        Map<Object, List<Number>> vectors = new HashMap<>();
        for (Row row : boundedRows(base.select(idColumn, vectorColumn))) {
            valid(!vectors.containsKey(row.get(0)), "The example needs unique primary keys");
            vectors.put(row.get(0), row.getList(1));
        }
        valid(!vectors.isEmpty(), "No rows to search; choose a nonempty snapshot or a less restrictive --where");
        if (explain) {
            frame.explain();
        }
        List<Row> rows = new ArrayList<>(frame.collectAsList());
        rows.sort(Comparator.comparingLong((Row row) -> row.getLong(0))
            .thenComparingDouble(row -> ((Number) row.get(2)).doubleValue())
            .thenComparing(row -> row.get(1).toString()));
        Map<Long, List<Object>> hits = new LinkedHashMap<>();
        System.out.printf("%6s  %12s  %12s%n", "qid", "id", "distance");
        for (Row row : rows) {
            hits.computeIfAbsent(row.getLong(0), key -> new ArrayList<>()).add(row.get(1));
            System.out.printf("%6d  %12s  %12.6f%n", row.getLong(0), row.get(1), row.get(2));
        }
        Map<String, Long> metrics = searchMetrics(frame.queryExecution().executedPlan());
        require(metrics.getOrDefault("milvus.search.knowhere.calls", 0L) > 0, "No native search execution");
        if (requireIndex) {
            require(metrics.getOrDefault("indexSegments", 0L) > 0
                && metrics.getOrDefault("milvus.search.index.bytes", 0L) > 0,
                "APPROX did not load and search a persisted index");
        }
        require(hits.keySet().equals(queryVectors.keySet()), "Search returned missing or unexpected query IDs");
        Map<Long, Object> report = new LinkedHashMap<>();
        for (Map.Entry<Long, List<Float>> query : queryVectors.entrySet()) {
            List<Double> ranked = vectors.values().stream().map(vector -> l2(vector, query.getValue()))
                .sorted().limit(k).toList();
            List<Object> found = hits.get(query.getKey());
            require(found.size() == ranked.size() && new HashSet<>(found).size() == found.size()
                && vectors.keySet().containsAll(found), "Search returned missing, duplicate or unknown hits");
            List<Double> distances = found.stream().map(id -> l2(vectors.get(id), query.getValue()))
                .sorted().toList();
            for (int i = 0; exact && i < ranked.size(); i++) {
                double a = ranked.get(i), b = distances.get(i);
                require(Math.abs(a - b) <= Math.max(1e-6, 1e-5 * Math.max(Math.abs(a), Math.abs(b))),
                    "EXACT differs from the independent oracle for query " + query.getKey());
            }
            // Equal-distance ties at k are valid regardless of the primary-key order.
            double threshold = ranked.get(ranked.size() - 1) + 1e-6;
            double recall = distances.stream().filter(distance -> distance <= threshold).count()
                / (double) ranked.size();
            report.put(query.getKey(), Map.of("ids", found, "recallAtK", recall));
        }
        System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(
            Map.of("mode", exact ? "exact" : "approx", "queries", report, "metrics", metrics)));
    }

    private static double l2(List<? extends Number> left, List<? extends Number> right) {
        valid(left.size() == right.size(), "Snapshot and query dimensions differ");
        double sum = 0;
        for (int i = 0; i < left.size(); i++) {
            double delta = left.get(i).doubleValue() - right.get(i).doubleValue();
            sum += delta * delta;
        }
        return sum;
    }

    private static Map<String, Long> searchMetrics(SparkPlan plan) {
        if (plan instanceof AdaptiveSparkPlanExec adaptive) {
            return searchMetrics(adaptive.executedPlan());
        }
        if (plan.nodeName().contains("MilvusNearestByJoin")) {
            Map<String, Long> result = new LinkedHashMap<>();
            Iterator<Tuple2<String, SQLMetric>> entries = plan.metrics().iterator();
            while (entries.hasNext()) {
                Tuple2<String, SQLMetric> entry = entries.next();
                result.put(entry._1(), entry._2().value());
            }
            return result;
        }
        Iterator<SparkPlan> children = plan.children().iterator();
        while (children.hasNext()) {
            Map<String, Long> result = searchMetrics(children.next());
            if (!result.isEmpty()) {
                return result;
            }
        }
        return Map.of();
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
