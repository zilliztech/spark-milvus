package com.zilliz.spark.connector.demo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Project selected fields and demonstrate Spark and Milvus scalar predicates. */
public final class FilterProject {
    public static void main(String[] args) throws Exception {
        try (DemoSupport demo = DemoSupport.open(args, "FilterProject", true, false, true)) {
            Dataset<Row> documents = demo.spark.read().format("milvus").options(demo.options).load();

            // where filters rows; select requests only these fields from the read.
            String predicate = demo.where == null ? "category = 1" : demo.where;
            Dataset<Row> selected = documents.where(predicate).select("id", "category");
            List<Row> rows = demo.boundedRows(selected);
            selected.orderBy("id").show(20, false);
            if (demo.explain) {
                selected.explain();
            }

            // SQL NULL semantics apply: category = 1 does not select null values.
            Dataset<Row> missing = documents.where("category IS NULL").select("id", "category");
            List<Row> missingRows = demo.boundedRows(missing);
            missing.show(20, false);

            // A Milvus scalar expression can also be bound to the snapshot read.
            // Its result still respects snapshot deletes and later Spark predicates.
            String nativeFilter = demo.options.getOrDefault("milvus.filter", "category == 1");
            Dataset<Row> filteredRead = demo.spark.read().format("milvus")
                .options(demo.options)
                .option("milvus.filter", nativeFilter)
                .load()
                .select("id", "category");
            List<Row> filteredRows = demo.boundedRows(filteredRead);
            filteredRead.orderBy("id").show(20, false);

            if (demo.fixture) {
                Set<Long> wanted = Set.of(3003L, 3004L, 3005L, 3006L, 3007L, 3101L, 3103L, 3107L);
                if (demo.where == null) {
                    DemoSupport.require(ids(rows).equals(wanted), "Spark predicate returned unexpected IDs");
                }
                DemoSupport.require(ids(filteredRows).equals(wanted), "Milvus filter returned unexpected IDs");
                DemoSupport.require(ids(missingRows).equals(Set.of(3001L)), "NULL category row differs");
            }
            System.out.println("Demo passed: FilterProject");
        }
    }

    private static Set<Long> ids(List<Row> rows) {
        Set<Long> result = new HashSet<>();
        for (Row row : rows) {
            result.add(row.getLong(row.fieldIndex("id")));
        }
        return result;
    }
}
