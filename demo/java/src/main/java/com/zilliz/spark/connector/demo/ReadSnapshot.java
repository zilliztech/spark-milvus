package com.zilliz.spark.connector.demo;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Read a fixed V3 snapshot and inspect its schema, vectors and live rows. */
public final class ReadSnapshot {
    public static void main(String[] args) throws Exception {
        try (DemoSupport demo = DemoSupport.open(args, "ReadSnapshot", true, false)) {
            // Spark converts the snapshot's columnar batches to rows for collectAsList() and show().
            Dataset<Row> documents = demo.spark.read().format("milvus").options(demo.options).load();
            documents.printSchema();
            List<Row> rows = demo.boundedRows(documents);
            documents.show(20, false);
            if (demo.explain) {
                documents.explain();
            }

            if (demo.fixture) {
                demo.checkFixtureRows(rows);
                for (Row row : rows) {
                    long id = row.getLong(row.fieldIndex("id"));
                    DemoSupport.require(id != 3002L && id != 3105L, "Deleted IDs must be absent");
                    List<Float> vector = row.getList(row.fieldIndex("vector"));
                    DemoSupport.require(vector.size() == 2, "The fixture has two-dimensional vectors");
                }
                Dataset<Row> stored = demo.spark.read().format("milvus").options(demo.options)
                    .option("milvus.read.mode", "physical").load();
                DemoSupport.require(stored.count() == 16, "Expected all 16 stored rows in physical mode");
                System.out.println("The sample has 16 stored rows and 14 live rows. IDs 3002 and 3105 are marked deleted.");
            }
            System.out.println("Rows read: " + rows.size());
            System.out.println("Demo passed: ReadSnapshot");
        }
    }
}
