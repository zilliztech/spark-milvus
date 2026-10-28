package com.zilliz.spark.connector.demo;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.zilliz.spark.connector.implicits.MilvusDataFrame;

/** Find each query's exact nearest neighbors through the public Java DataFrame API. */
public final class ExactSearch {
    public static void main(String[] args) throws Exception {
        try (DemoSupport demo = DemoSupport.open(args, "ExactSearch", true, true, true)) {
            Dataset<Row> base = demo.spark.read().format("milvus").options(demo.options).load();
            if (demo.where != null) {
                base = base.where(demo.where);
            }
            Dataset<Row> queries = demo.queries();
            Column ranking = functions.call_function(
                "vector_l2_distance", queries.col("qvec"), base.col(demo.vectorColumn));

            // Java callers construct MilvusDataFrame directly. EXACT scans live
            // vectors and merges top-k across segments; L2 distance ranks ascending.
            Dataset<Row> result = new MilvusDataFrame(queries)
                .nearestByJoin(base, ranking, demo.k, "exact", "distance")
                .select(queries.col("qid"), base.col(demo.idColumn).alias("id"),
                    ranking.alias("distance"));
            demo.checkSearch(result, base, true, false);
            System.out.println("Demo passed: ExactSearch");
        }
    }
}
