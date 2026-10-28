#!/usr/bin/env python3
"""Use a snapshot as a SQL table for filtering, grouping and aggregation."""

from common import bounded_rows, demo_session


def main():
    with demo_session(__doc__) as (spark, args, options, _):
        documents = spark.read.format("milvus").options(**options).load()
        # This registers a session-local view over the fixed input.
        documents.createOrReplaceTempView("documents")

        summary = spark.sql("""
            SELECT category, COUNT(*) AS row_count, MIN(id) AS first_id, MAX(id) AS last_id
            FROM documents
            GROUP BY category
            ORDER BY category NULLS FIRST
        """)
        rows = bounded_rows(summary, args)
        summary.show(truncate=False)
        if args.explain:
            summary.explain()

        counts = spark.sql("""
            SELECT COUNT(*) AS total_rows, COUNT(category) AS categorized_rows
            FROM documents
        """).first()
        print(f"Total rows: {counts.total_rows}; non-null categories: {counts.categorized_rows}")
        spark.sql("SELECT id, vector FROM documents WHERE category = 0 ORDER BY id").show(truncate=False)

        if args.fixture:
            assert {row.category: row.row_count for row in rows} == {None: 1, 0: 5, 1: 8}
            assert (counts.total_rows, counts.categorized_rows) == (14, 13)


if __name__ == "__main__":
    main()
