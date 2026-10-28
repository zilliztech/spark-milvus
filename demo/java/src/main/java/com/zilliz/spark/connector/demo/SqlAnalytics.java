package com.zilliz.spark.connector.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Query a snapshot through a session-local SQL view and aggregate its live rows. */
public final class SqlAnalytics {
    public static void main(String[] args) throws Exception {
        try (DemoSupport demo = DemoSupport.open(args, "SqlAnalytics", true, false, false)) {
            Dataset<Row> documents = demo.spark.read().format("milvus").options(demo.options).load();
            documents.createOrReplaceTempView("documents");

            Dataset<Row> summary = demo.spark.sql("""
                SELECT category, COUNT(*) AS row_count, MIN(id) AS first_id, MAX(id) AS last_id
                FROM documents
                GROUP BY category
                ORDER BY category NULLS FIRST
                """);
            List<Row> rows = demo.boundedRows(summary);
            summary.show(false);
            if (demo.explain) {
                summary.explain();
            }

            // COUNT(column) omits NULL; COUNT(*) counts every visible row.
            Row counts = demo.spark.sql("""
                SELECT COUNT(*) AS total_rows, COUNT(category) AS categorized_rows
                FROM documents
                """).first();
            System.out.println("Total rows: " + counts.getLong(0)
                + "; non-null categories: " + counts.getLong(1));
            demo.spark.sql("SELECT id, vector FROM documents WHERE category = 0 ORDER BY id")
                .show(false);

            if (demo.fixture) {
                Map<Integer, Long> actual = new HashMap<>();
                for (Row row : rows) {
                    Integer category = row.isNullAt(0) ? null : ((Number) row.get(0)).intValue();
                    actual.put(category, row.getLong(1));
                }
                Map<Integer, Long> wanted = new HashMap<>();
                wanted.put(null, 1L);
                wanted.put(0, 5L);
                wanted.put(1, 8L);
                DemoSupport.require(actual.equals(wanted), "Category counts differ from the fixture");
                DemoSupport.require(counts.getLong(0) == 14L && counts.getLong(1) == 13L,
                    "Total and non-null counts differ from the fixture");
            }
            System.out.println("Demo passed: SqlAnalytics");
        }
    }
}
