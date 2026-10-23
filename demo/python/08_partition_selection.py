#!/usr/bin/env python3
"""Select snapshot partitions, segments and fields, and inspect row metadata."""

from common import bounded_rows, demo_session


def main():
    with demo_session(__doc__) as (spark, args, options, _):
        # These are IDs recorded in the snapshot, not names resolved by a service.
        # The sample contains partition 20, segments 30/31 and fields 100/101/102.
        # A custom options JSON can replace these selections for another snapshot.
        selected = (spark.read.format("milvus").options(**options)
                    .option("milvus.partitions", options.get("milvus.partitions", "20"))
                    .option("milvus.segments", options.get("milvus.segments", "30"))
                    .option("fieldIDs", options.get("fieldIDs", "100,102"))
                    .option("milvus.extra.columns", "_segment_id,_row_offset,_timestamp")
                    .load())
        selected.printSchema()
        rows = bounded_rows(selected, args)
        selected.orderBy("id").show(20, truncate=False)
        if args.explain:
            selected.explain()

        if args.fixture:
            assert "vector" not in selected.columns
            assert {row.id for row in rows} == set(range(3000, 3008)) - {3002}
            assert {row._segment_id for row in rows} == {30}
            assert {row._timestamp for row in rows} == {100}
            # Row offsets remain physical positions, including the deleted gap.
            assert {row._row_offset for row in rows} == set(range(8)) - {2}


if __name__ == "__main__":
    main()
