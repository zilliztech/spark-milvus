#!/usr/bin/env python3
"""Restrict the candidate rows before selecting each query's nearest neighbors."""

from common import check_search, demo_session


def main(argv=None):
    with demo_session(__doc__, argv, search=True) as (spark, args, options, queries):
        base = spark.read.format("milvus").options(**options).load()

        # Filter the base relation before registering it for nearest_by_join.
        # Filtering the joined result instead could discard hits after top-k.
        predicate = args.where or "category = 1"
        base = base.where(predicate)
        print(f"Candidate predicate: {predicate}")
        base.createOrReplaceTempView("demo_base")
        spark.createDataFrame(queries, "qid LONG, qvec ARRAY<FLOAT>").createOrReplaceTempView(
            "demo_queries"
        )

        result = spark.sql(f"""
            SELECT query.qid, base.`{args.id_column}` AS id,
                   vector_l2_distance(query.qvec, base.`{args.vector_column}`) AS distance
            FROM nearest_by_join(
                TABLE(demo_queries), TABLE(demo_base),
                'vector_l2_distance(query.qvec, base.`{args.vector_column}`)',
                {args.k}, 'exact', 'distance')
        """)
        # The oracle receives the same filtered base, so it checks filter-before-k.
        check_search(result, base, queries, args, exact=True)


if __name__ == "__main__":
    main()
