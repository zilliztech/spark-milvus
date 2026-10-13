#!/usr/bin/env python3
"""NEAREST BY over a Milvus collection that MilvusCatalog reads.

scripts/spark_submit_demo.sh submits this program. It creates a collection of
2,000 rows with pymilvus, flushes it and takes a snapshot, which the catalog
reads as the collection's latest version. It then searches the collection with
nearest_by_join, the SQL form of NEAREST BY on Spark 3.5 to 4.1, checks the
hits against a brute-force search in numpy, and drops the snapshot and the
collection.

The catalog comes from the spark-submit settings. pymilvus (3.0 or later, for
the snapshot API) reads MILVUS_URI and MILVUS_TOKEN; KEEP=1 keeps the
collection and its snapshot.
"""

import os
import sys
import time

import numpy as np
from pymilvus import DataType, MilvusClient
from pyspark.sql import SparkSession

ROWS = 2000
DIM = 16
K = 5


def create_collection(client, name, vectors):
    schema = client.create_schema(auto_id=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("title", DataType.VARCHAR, max_length=64)
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=DIM)
    client.create_collection(name, schema=schema)
    client.insert(
        name,
        [{"id": i, "title": f"doc {i}", "vec": vectors[i].tolist()} for i in range(ROWS)],
    )
    # The snapshot holds sealed segments only.
    client.flush(name)
    client.create_snapshot(snapshot_name=f"{name}_v1", collection_name=name)
    print(f"Collection {name}: {ROWS} rows of {DIM} dimensions, snapshot {name}_v1")


def search(spark, name, queries):
    spark.createDataFrame(
        [(qid, q.tolist()) for qid, q in enumerate(queries)], "qid INT, qvec ARRAY<FLOAT>"
    ).createOrReplaceTempView("queries")
    frame = spark.sql(
        f"""
        SELECT query.qid, base.id, base.title
        FROM nearest_by_join(
          TABLE(queries),
          TABLE(milvus.default.{name}),
          'vector_l2_distance(query.qvec, base.vec)',
          {K}, 'exact', 'distance')
        """
    )
    plan = frame._jdf.queryExecution().executedPlan().toString()
    node = next((line.strip() for line in plan.splitlines() if "MilvusNearestByJoin" in line), None)
    print(f"Executed by: {node or 'Spark (the connector did not take the join over)'}")
    hits = {}
    for row in frame.collect():
        hits.setdefault(row.qid, []).append(row.id)
    return hits


def main():
    name = f"search_demo_{int(time.time() * 1000)}"
    client = MilvusClient(
        uri=os.environ.get("MILVUS_URI", "http://localhost:19530"),
        token=os.environ.get("MILVUS_TOKEN", "root:Milvus"),
    )
    rng = np.random.default_rng(7)
    vectors = rng.random((ROWS, DIM), dtype=np.float32)
    queries = vectors[[10, 500, 1500]] + rng.normal(0, 0.01, (3, DIM)).astype(np.float32)

    spark = SparkSession.builder.appName("MilvusSearchDemo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        create_collection(client, name, vectors)
        hits = search(spark, name, queries)
        wrong = 0
        for qid, query in enumerate(queries):
            distance = ((vectors - query) ** 2).sum(axis=1)
            expected = np.argsort(distance)[:K].tolist()
            found = sorted(hits.get(qid, []), key=lambda i: distance[i])
            same = found == expected
            wrong += not same
            print(f"query {qid}: top {K} {found} {'equals' if same else 'DIFFERS FROM'} brute force {expected}")
        print("Search demo passed" if wrong == 0 else f"Search demo FAILED: {wrong} queries differ")
        return 1 if wrong else 0
    finally:
        spark.stop()
        if os.environ.get("KEEP") != "1" and client.has_collection(name):
            client.drop_snapshot(snapshot_name=f"{name}_v1", collection_name=name)
            client.drop_collection(name)


if __name__ == "__main__":
    sys.exit(main())
