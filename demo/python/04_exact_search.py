#!/usr/bin/env python3
"""Find the exact nearest neighbors for every query vector in a fixed snapshot."""

from common import check_search, demo_session


def main(argv=None):
    with demo_session(__doc__, argv, search=True) as (spark, args, options, queries):
        base = spark.read.format("milvus").options(**options).load()
        if args.where:
            base = base.where(args.where)
        base.createOrReplaceTempView("demo_base")
        spark.createDataFrame(queries, "qid LONG, qvec ARRAY<FLOAT>").createOrReplaceTempView(
            "demo_queries"
        )

        # EXACT scans the live vectors and merges the nearest k across segments.
        # The table function exposes the input relations as `query` and `base`.
        # L2 ranks from smallest to largest; the projected distance is Euclidean.
        result = spark.sql(f"""
            SELECT query.qid, base.`{args.id_column}` AS id,
                   vector_l2_distance(query.qvec, base.`{args.vector_column}`) AS distance
            FROM nearest_by_join(
                TABLE(demo_queries), TABLE(demo_base),
                'vector_l2_distance(query.qvec, base.`{args.vector_column}`)',
                {args.k}, 'exact', 'distance')
        """)
        check_search(result, base, queries, args, exact=True)


if __name__ == "__main__":
    main()
