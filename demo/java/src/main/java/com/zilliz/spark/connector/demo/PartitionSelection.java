package com.zilliz.spark.connector.demo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Select snapshot partitions, segments and fields, and inspect physical row metadata. */
public final class PartitionSelection {
    public static void main(String[] args) throws Exception {
        try (DemoSupport demo = DemoSupport.open(args, "PartitionSelection", true, false, false)) {
            // These IDs are recorded in the snapshot. The fixture contains partition
            // 20, segments 30/31 and fields 100/101/102; custom options can replace them.
            Dataset<Row> selected = demo.spark.read().format("milvus")
                .options(demo.options)
                .option("milvus.partitions", demo.options.getOrDefault("milvus.partitions", "20"))
                .option("milvus.segments", demo.options.getOrDefault("milvus.segments", "30"))
                .option("fieldIDs", demo.options.getOrDefault("fieldIDs", "100,102"))
                .option("milvus.extra.columns", "_segment_id,_row_offset,_timestamp")
                .load();
            selected.printSchema();
            List<Row> rows = demo.boundedRows(selected);
            selected.orderBy("id").show(20, false);
            if (demo.explain) {
                selected.explain();
            }

            if (demo.fixture) {
                DemoSupport.require(!Arrays.asList(selected.columns()).contains("vector"),
                    "fieldIDs should exclude the vector column");
                Set<Long> ids = new HashSet<>();
                Set<Long> offsets = new HashSet<>();
                for (Row row : rows) {
                    ids.add(row.getLong(row.fieldIndex("id")));
                    offsets.add(((Number) row.getAs("_row_offset")).longValue());
                    DemoSupport.require(((Number) row.getAs("_segment_id")).longValue() == 30L,
                        "Only segment 30 should be selected");
                    DemoSupport.require(((Number) row.getAs("_timestamp")).longValue() == 100L,
                        "Unexpected fixture row timestamp");
                }
                DemoSupport.require(ids.equals(Set.of(3000L, 3001L, 3003L, 3004L, 3005L, 3006L, 3007L)),
                    "Segment selection returned unexpected IDs");
                // Offsets retain physical positions, including the deleted gap.
                DemoSupport.require(offsets.equals(Set.of(0L, 1L, 3L, 4L, 5L, 6L, 7L)),
                    "Row offsets should preserve the deleted row's gap");
            }
            System.out.println("Demo passed: PartitionSelection");
        }
    }
}
