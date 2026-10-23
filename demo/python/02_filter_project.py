#!/usr/bin/env python3
"""Project columns, apply scalar predicates, and handle nullable fields."""

from common import bounded_rows, demo_session


def main():
    with demo_session(__doc__, filtering=True) as (spark, args, options, _):
        documents = spark.read.format("milvus").options(**options).load()

        # A Spark predicate filters rows while select requests only these fields.
        selected = documents.where(args.where or "category = 1").select("id", "category")
        rows = bounded_rows(selected, args)
        selected.orderBy("id").show(20, truncate=False)
        if args.explain:
            selected.explain()

        # SQL NULL semantics apply: category = 1 does not select null values.
        missing = documents.where("category IS NULL").select("id", "category")
        missing_rows = bounded_rows(missing, args)
        missing.show(20, truncate=False)

        # A Milvus scalar expression can also be bound to the read itself.
        # Spark predicates and snapshot deletes are combined with this filter.
        native_filter = options.get("milvus.filter", "category == 1")
        filtered_read = (spark.read.format("milvus").options(**options)
                         .option("milvus.filter", native_filter).load()
                         .select("id", "category"))
        filtered_rows = bounded_rows(filtered_read, args)
        filtered_read.orderBy("id").show(20, truncate=False)

        if args.fixture:
            wanted = {row["id"] for row in args.expected_rows if row["category"] == 1}
            if args.where is None:
                assert {row.id for row in rows} == wanted
            assert {row.id for row in filtered_rows} == wanted
            assert [row.id for row in missing_rows] == [3001]


if __name__ == "__main__":
    main()
