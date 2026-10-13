#!/usr/bin/env python3
"""The Milvus side of scripts/run_backfill_demo.sh, which runs the steps of
docs/user-guide-snapshot-backfill.md against a Milvus 3.0+.

  prepare <dir>        Creates a collection of 20,000 rows, builds its vector
                       index and loads it, adds the nullable fields new_field1
                       (VARCHAR) and new_field2 (INT64), and takes a snapshot
                       that pins its files for an hour. Writes
                       <dir>/new_fields.parquet: the keys in the first and the
                       last 30% get values, the 40% in the middle none. Prints
                       the collection, the snapshot's s3a:// path and the
                       parquet path as JSON.
  verify <collection>  Queries the collection after the commit until the values
                       show: with --mode replace a key the parquet holds gets
                       its values, and every other key NULL.
  drop <collection>    Drops the snapshot and the collection.

Needs pymilvus 3.0+ (for the snapshot API), numpy and pyarrow. Reads
MILVUS_URI, MILVUS_TOKEN and S3_BUCKET, as run_backfill_demo.sh sets them.
"""

import json
import os
import sys
import time
import urllib.parse

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pymilvus import DataType, MilvusClient

ROWS = 20000
DIM = 16
BATCH = 5000
# The parquet holds keys below FIRST_END and from LAST_START on.
FIRST_END = int(ROWS * 0.3)
LAST_START = int(ROWS * 0.7)


def client():
    return MilvusClient(
        uri=os.environ.get("MILVUS_URI", "http://localhost:19530"),
        token=os.environ.get("MILVUS_TOKEN", "root:Milvus"),
    )


def snapshot_name(collection):
    return f"{collection}_backfill"


def object_key(location, bucket):
    """The key of a snapshot location, which Milvus gives as a bucket-relative
    key or as a URI such as minio://minio:9000/<bucket>/<key>."""
    if "://" not in location:
        return location.lstrip("/")
    uri = urllib.parse.urlparse(location)
    path = uri.path.lstrip("/")
    if uri.netloc == bucket:
        return path
    head, _, key = path.partition("/")
    if head != bucket:
        raise ValueError(f"The snapshot {location} is not in the bucket {bucket}")
    return key


def prepare(directory):
    milvus = client()
    name = f"backfill_demo_{int(time.time() * 1000)}"
    schema = milvus.create_schema(auto_id=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("title", DataType.VARCHAR, max_length=64)
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=DIM)
    milvus.create_collection(name, schema=schema)
    rng = np.random.default_rng(42)
    for start in range(0, ROWS, BATCH):
        milvus.insert(
            name,
            [
                {"id": i, "title": f"doc {i}", "vec": rng.random(DIM, dtype=np.float32).tolist()}
                for i in range(start, start + BATCH)
            ],
        )
    milvus.flush(name)
    index = milvus.prepare_index_params()
    index.add_index(field_name="vec", index_type="FLAT", metric_type="L2")
    milvus.create_index(name, index)
    milvus.load_collection(name)

    # 5.1 of the guide: the snapshot's schema has to hold the target fields.
    milvus.add_collection_field(name, "new_field1", DataType.VARCHAR, max_length=64, nullable=True)
    milvus.add_collection_field(name, "new_field2", DataType.INT64, nullable=True)
    milvus.create_snapshot(
        snapshot_name=snapshot_name(name),
        collection_name=name,
        compaction_protection_seconds=3600,
    )
    location = milvus.describe_snapshot(snapshot_name=snapshot_name(name), collection_name=name).s3_location
    bucket = os.environ.get("S3_BUCKET", "a-bucket")

    keys = [*range(FIRST_END), *range(LAST_START, ROWS)]
    parquet = os.path.join(directory, "new_fields.parquet")
    pq.write_table(
        pa.table(
            {
                "pk": pa.array(keys, pa.int64()),
                "new_field1": pa.array([f"value {k}" for k in keys], pa.string()),
                "new_field2": pa.array([k * 2 for k in keys], pa.int64()),
            }
        ),
        parquet,
    )
    print(
        f"Collection {name}: {ROWS} rows; new_field1 and new_field2 added; "
        f"{len(keys)} keys in {parquet}",
        file=sys.stderr,
    )
    print(
        json.dumps(
            {
                "collection": name,
                "snapshot": f"s3a://{bucket}/{object_key(location, bucket)}",
                "parquet": parquet,
            }
        )
    )


def expected(key):
    if key < FIRST_END or key >= LAST_START:
        return f"value {key}", key * 2
    return None, None


def verify(name, timeout=300):
    milvus = client()
    sample = [0, FIRST_END - 1, FIRST_END, LAST_START - 1, LAST_START, ROWS - 1]
    deadline = time.time() + timeout
    while True:
        rows = milvus.query(
            name, filter=f"id in {sample}", output_fields=["id", "new_field1", "new_field2"]
        )
        found = {row["id"]: (row.get("new_field1"), row.get("new_field2")) for row in rows}
        if found.get(0) == expected(0) or time.time() > deadline:
            break
        # The query nodes reopen the committed segments on their own.
        time.sleep(5)
    filled = milvus.query(name, filter="new_field1 is not null", output_fields=["count(*)"])[0]["count(*)"]
    wrong = [key for key in sample if found.get(key) != expected(key)]
    for key in sample:
        print(f"id {key}: {found.get(key)}, expected {expected(key)}")
    print(f"{filled} of {ROWS} rows have new_field1; the parquet holds {FIRST_END + ROWS - LAST_START} keys")
    if wrong or filled != FIRST_END + ROWS - LAST_START:
        print("Backfill demo FAILED")
        sys.exit(1)
    print("Backfill demo passed")


def drop(name):
    milvus = client()
    if milvus.has_collection(name):
        milvus.drop_snapshot(snapshot_name=snapshot_name(name), collection_name=name)
        milvus.drop_collection(name)
        print(f"Dropped {name} and its snapshot", file=sys.stderr)


def main():
    commands = {"prepare": prepare, "verify": verify, "drop": drop}
    if len(sys.argv) != 3 or sys.argv[1] not in commands:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    commands[sys.argv[1]](sys.argv[2])


if __name__ == "__main__":
    main()
