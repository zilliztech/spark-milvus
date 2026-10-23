#!/usr/bin/env python3
"""Read a fixed V3 snapshot and observe its schema, vectors and visible rows."""

from common import bounded_rows, check_fixture_rows, demo_session


def main():
    with demo_session(__doc__) as (spark, args, options, _):
        # The snapshot supplies the schema and segment locations. Storage access
        # and delete handling belong to the connector; no Milvus service is used.
        documents = spark.read.format("milvus").options(**options).load()
        documents.printSchema()
        rows = bounded_rows(documents, args)
        documents.show(20, truncate=False)
        if args.explain:
            documents.explain()

        if args.fixture:
            check_fixture_rows(rows, args.expected_rows)
            assert {row.id for row in rows}.isdisjoint({3002, 3105})
            print("14 live rows; snapshot deletes remove IDs 3002 and 3105.")
        print(f"Visible rows: {len(rows)}")


if __name__ == "__main__":
    main()
