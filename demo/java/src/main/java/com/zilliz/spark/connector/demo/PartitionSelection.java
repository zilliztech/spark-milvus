package com.zilliz.spark.connector.demo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Select snapshot partitions and fields, and inspect physical row metadata. */
public final class PartitionSelection {
    public static void main(String[] args) throws Exception {
        try (DemoSupport demo = DemoSupport.open(args, "PartitionSelection", true, true)) {
            // The sample contains partition 20.
            Dataset<Row> selected = demo.spark.read().format("milvus")
                .options(demo.options)
                .load()
                .where(demo.where == null ? "_partition_id IN (20)" : demo.where)
                .select("id", "category", "_partition_id", "_segment_id", "_row_offset", "_timestamp");
            selected.printSchema();
            List<Row> rows = demo.boundedRows(selected);
            selected.orderBy("id").show(20, false);
            if (demo.explain) {
                selected.explain();
            }

            if (demo.fixture && demo.where == null) {
                Set<Long> ids = new HashSet<>();
                Map<Long, Set<Long>> offsets = new HashMap<>();
                for (Row row : rows) {
                    DemoSupport.require(row.getLong(row.fieldIndex("_partition_id")) == 20L,
                        "Expected a row from Milvus partition 20");
                    ids.add(row.getLong(row.fieldIndex("id")));
                    long segmentId = ((Number) row.getAs("_segment_id")).longValue();
                    offsets.computeIfAbsent(segmentId, key -> new HashSet<>())
                        .add(((Number) row.getAs("_row_offset")).longValue());
                    DemoSupport.require(((Number) row.getAs("_timestamp")).longValue() == 100L,
                        "Unexpected fixture row timestamp");
                }
                DemoSupport.require(ids.equals(Set.of(3000L, 3001L, 3003L, 3004L, 3005L, 3006L, 3007L,
                        3100L, 3101L, 3102L, 3103L, 3104L, 3106L, 3107L)),
                    "Partition selection returned unexpected IDs");
                // Offsets retain physical positions, including the deleted gap.
                DemoSupport.require(offsets.equals(Map.of(
                        30L, Set.of(0L, 1L, 3L, 4L, 5L, 6L, 7L),
                        31L, Set.of(0L, 1L, 2L, 3L, 4L, 6L, 7L))),
                    "Row offsets should preserve the deleted row's gap");
            }
            System.out.println("Demo passed: PartitionSelection");
        }
    }
}
