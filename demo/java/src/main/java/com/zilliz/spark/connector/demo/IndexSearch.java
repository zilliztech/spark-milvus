package com.zilliz.spark.connector.demo;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.zilliz.spark.connector.implicits.MilvusDataFrame;

/** Use compatible persisted indexes and report approximate-search recall. */
public final class IndexSearch {
    public static void main(String[] args) throws Exception {
        try (DemoSupport demo = DemoSupport.open(args, "IndexSearch", true, true, true)) {
            DataFrameReader reader = demo.spark.read().format("milvus").options(demo.options);
            // The fixture has HNSW indexes. Its ef search breadth must be >= k.
            // Custom snapshots retain their own parameters and connector defaults.
            if (demo.fixture) {
                reader = reader.option("milvus.search.params", "ef=" + Math.max(32, demo.k));
            }
            Dataset<Row> base = reader.load();
            if (demo.where != null) {
                base = base.where(demo.where);
            }
            Dataset<Row> queries = demo.queries();
            Column ranking = functions.call_function(
                "vector_l2_distance", queries.col("qvec"), base.col(demo.vectorColumn));
            Dataset<Row> result = new MilvusDataFrame(queries)
                .nearestByJoin(base, ranking, demo.k, "approx", "distance")
                .select(queries.col("qid"), base.col(demo.idColumn).alias("id"),
                    ranking.alias("distance"));

            // Require persisted-index metrics and compare hits with an exact oracle.
            // Approximate search reports recall without asserting perfect recall.
            demo.checkSearch(result, base, false, true);
            System.out.println("Demo passed: IndexSearch");
        }
    }
}
