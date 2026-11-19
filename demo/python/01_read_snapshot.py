#!/usr/bin/env python3
"""Read a fixed V3 snapshot and observe its schema, vectors and visible rows."""

from common import bounded_rows, check_fixture_rows, demo_session


def main():
    with demo_session(__doc__) as (spark, args, options):
        # Spark converts the snapshot's columnar batches to rows for collect() and show().
        documents = spark.read.format("milvus").options(**options).load()
        documents.printSchema()
        rows = bounded_rows(documents, args)
        documents.show(20, truncate=False)
        if args.explain:
            documents.explain()

        if args.fixture:
            check_fixture_rows(rows, args.expected_rows)
            assert {row.id for row in rows}.isdisjoint({3002, 3105})
            stored = (spark.read.format("milvus").options(**options)
                      .option("milvus.read.mode", "physical").load())
            assert stored.count() == 16
            print("The sample has 16 stored rows and 14 live rows. IDs 3002 and 3105 are marked deleted.")
        print(f"Rows read: {len(rows)}")


if __name__ == "__main__":
    main()
