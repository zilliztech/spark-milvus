package com.zilliz.spark.connector.demo;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.zilliz.spark.connector.implicits.MilvusDataFrame;

/** Restrict candidate rows before selecting the nearest neighbors for each query. */
public final class FilteredSearch {
    public static void main(String[] args) throws Exception {
        try (DemoSupport demo = DemoSupport.open(args, "FilteredSearch", true, true, true)) {
            Dataset<Row> base = demo.spark.read().format("milvus").options(demo.options).load();
            String predicate = demo.where == null ? "category = 1" : demo.where;

            // Filtering the base before nearestByJoin selects neighbors within
            // the eligible set. Filtering the joined result could discard top-k hits.
            base = base.where(predicate);
            System.out.println("Candidate predicate: " + predicate);
            Dataset<Row> queries = demo.queries();
            Column ranking = functions.call_function(
                "vector_l2_distance", queries.col("qvec"), base.col(demo.vectorColumn));
            Dataset<Row> result = new MilvusDataFrame(queries)
                .nearestByJoin(base, ranking, demo.k, "exact", "distance")
                .select(queries.col("qid"), base.col(demo.idColumn).alias("id"),
                    ranking.alias("distance"));

            // The oracle uses the filtered base, verifying filter-before-top-k.
            demo.checkSearch(result, base, true, false);
            System.out.println("Demo passed: FilteredSearch");
        }
    }
}
