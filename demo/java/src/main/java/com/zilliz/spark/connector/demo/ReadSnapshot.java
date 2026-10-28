package com.zilliz.spark.connector.demo;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Read a fixed V3 snapshot and inspect its schema, vectors and live rows. */
public final class ReadSnapshot {
    public static void main(String[] args) throws Exception {
        try (DemoSupport demo = DemoSupport.open(args, "ReadSnapshot", true, false, false)) {
            // The snapshot supplies the schema and segment locations. The connector
            // reads storage and applies deletes without contacting a Milvus service.
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
                System.out.println("14 live rows; snapshot deletes remove IDs 3002 and 3105.");
            }
            System.out.println("Visible rows: " + rows.size());
            System.out.println("Demo passed: ReadSnapshot");
        }
    }
}
