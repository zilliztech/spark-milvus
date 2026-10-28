package com.zilliz.spark.connector.demo;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Evaluate the three vector functions without a snapshot or storage credentials. */
public final class VectorFunctions {
    public static void main(String[] args) throws Exception {
        try (DemoSupport demo = DemoSupport.open(args, "VectorFunctions", false, false, false)) {
            // These functions consume equally sized ARRAY<FLOAT> values. SQL's F
            // suffix gives float literals instead of double or decimal values.
            Dataset<Row> result = demo.spark.sql("""
                SELECT name,
                       vector_l2_distance(left_vector, right_vector) AS l2_distance,
                       vector_cosine_similarity(left_vector, right_vector) AS cosine_similarity,
                       vector_inner_product(left_vector, right_vector) AS inner_product
                FROM VALUES
                    ('same_direction', array(3.0F, 4.0F), array(6.0F, 8.0F)),
                    ('orthogonal', array(1.0F, 0.0F), array(0.0F, 1.0F)),
                    ('opposite', array(1.0F, 0.0F), array(-1.0F, 0.0F)),
                    ('zero_vector', array(0.0F, 0.0F), array(1.0F, 0.0F)),
                    ('null_vector', CAST(NULL AS ARRAY<FLOAT>), array(1.0F, 0.0F))
                AS pairs(name, left_vector, right_vector)
                """);
            if (demo.explain) {
                result.explain();
            }
            result.show(false);

            // L2 ranks ascending; cosine and inner product rank descending.
            // A zero vector has undefined cosine similarity, represented as NULL.
            Map<String, Double[]> expected = Map.of(
                "same_direction", new Double[] {5.0, 1.0, 50.0},
                "orthogonal", new Double[] {Math.sqrt(2.0), 0.0, 0.0},
                "opposite", new Double[] {2.0, -1.0, -1.0},
                "zero_vector", new Double[] {1.0, null, 0.0},
                "null_vector", new Double[] {null, null, null});
            for (Row row : result.collectAsList()) {
                Double[] wanted = expected.get(row.getString(0));
                for (int index = 0; index < wanted.length; index++) {
                    Object actual = row.get(index + 1);
                    if (wanted[index] == null) {
                        DemoSupport.require(actual == null, "Expected NULL for " + row.getString(0));
                    } else {
                        DemoSupport.require(actual instanceof Number
                            && Math.abs(((Number) actual).doubleValue() - wanted[index]) <= 1e-6,
                            "Unexpected vector score for " + row.getString(0));
                    }
                }
            }
            System.out.println("Demo passed: VectorFunctions");
        }
    }
}
