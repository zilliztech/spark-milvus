#!/usr/bin/env python3
"""Select snapshot partitions and fields, and inspect row metadata."""

from common import bounded_rows, demo_session


def main():
    with demo_session(__doc__, filtering=True) as (spark, args, options):
        # The sample contains partition 20.
        selected = (spark.read.format("milvus").options(**options)
                    .load()
                    .where(args.where or "_partition_id IN (20)")
                    .select("id", "category", "_partition_id", "_segment_id", "_row_offset", "_timestamp"))
        selected.printSchema()
        rows = bounded_rows(selected, args)
        selected.orderBy("id").show(20, truncate=False)
        if args.explain:
            selected.explain()

        if args.fixture and args.where is None:
            assert {row._partition_id for row in rows} == {20}
            assert {row.id for row in rows} == {row["id"] for row in args.expected_rows}
            assert {row._segment_id for row in rows} == {30, 31}
            assert {row._timestamp for row in rows} == {100}
            # Row offsets remain physical positions, including the deleted gap.
            assert {row._row_offset for row in rows if row._segment_id == 30} == set(range(8)) - {2}
            assert {row._row_offset for row in rows if row._segment_id == 31} == set(range(8)) - {5}


if __name__ == "__main__":
    main()
