#!/usr/bin/env python3
"""Search a snapshot's compatible persisted index and report observed recall."""

from common import check_search, demo_session


def main(argv=None):
    with demo_session(__doc__, argv, search=True) as (spark, args, options, queries):
        # The sample contains HNSW indexes; ef controls search breadth and must
        # be at least k. Custom snapshots keep their own parameters or the
        # connector's defaults, since an IVF/FLAT index does not accept ef.
        reader = spark.read.format("milvus").options(**options)
        if args.fixture:
            reader = reader.option("milvus.search.params", f"ef={max(32, args.k)}")
        base = reader.load()
        if args.where:
            base = base.where(args.where)
        base.createOrReplaceTempView("demo_base")
        spark.createDataFrame(queries, "qid LONG, qvec ARRAY<FLOAT>").createOrReplaceTempView(
            "demo_queries"
        )

        # APPROX may use compatible persisted indexes. The checker requires
        # index metrics, so a missing or incompatible index fails this example.
        result = spark.sql(f"""
            SELECT query.qid, base.`{args.id_column}` AS id,
                   vector_l2_distance(query.qvec, base.`{args.vector_column}`) AS distance
            FROM nearest_by_join(
                TABLE(demo_queries), TABLE(demo_base),
                'vector_l2_distance(query.qvec, base.`{args.vector_column}`)',
                {args.k}, 'approx', 'distance')
        """)
        # Approximate search reports recall against an exact Python oracle.
        check_search(result, base, queries, args, exact=False, require_index=True)


if __name__ == "__main__":
    main()
