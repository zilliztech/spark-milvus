#!/usr/bin/env python3
"""Read a supplied V3 snapshot and check a small L2 search without a Milvus service.

The options JSON binds one immutable snapshot and its storage identity. The
queries JSON is [{"qid": 1, "vector": [0.0, 0.0]}, ...]. The caller retains all
referenced files. This bounded example reads at most --max-rows rows for its
independent exact oracle; it never prepares, changes or releases the input.
"""
import argparse
import json
import math
from pathlib import Path
import re


def read_options(path):
    options = json.loads(Path(path).read_text())
    if not isinstance(options, dict) or not options.get("milvus.snapshot.path"):
        raise ValueError("Options must contain a fixed milvus.snapshot.path")
    allowed = {"milvus.snapshot.path", "milvus.filter"}
    if any(not isinstance(key, str) or not isinstance(value, str) or not value
           or (key not in allowed and not key.startswith(("fs.", "extfs.")))
           for key, value in options.items()):
        raise ValueError("Use only snapshot/filter and fs.*/extfs.* string options")
    return options


def read_queries(path):
    queries = json.loads(Path(path).read_text())
    if not isinstance(queries, list) or not queries or len(queries) > 100:
        raise ValueError("Supply 1 to 100 query vectors")
    seen, dimension, result = set(), None, []
    for query in queries:
        if not isinstance(query, dict) or set(query) != {"qid", "vector"}:
            raise ValueError("Each query needs qid and vector")
        qid, vector = query["qid"], query["vector"]
        if type(qid) is not int or not -(2 ** 63) <= qid < 2 ** 63 or qid in seen:
            raise ValueError("Query ids must be unique signed 64-bit integers")
        if not isinstance(vector, list) or not vector or any(
                type(value) not in (float, int) or not math.isfinite(value)
                or abs(value) > 3.4028234663852886e38 for value in vector):
            raise ValueError("Queries need finite float32 vectors")
        if dimension is not None and len(vector) != dimension:
            raise ValueError("Query dimensions differ")
        seen.add(qid)
        dimension = len(vector)
        result.append((qid, [float(value) for value in vector]))
    return result


def l2(left, right):
    if len(left) != len(right):
        raise ValueError("Snapshot and query dimensions differ")
    return sum((float(a) - float(b)) ** 2 for a, b in zip(left, right))


def check_hits(base, queries, hits, k, exact):
    vectors = dict(base)
    if len(vectors) != len(base) or not vectors:
        raise ValueError("The example needs nonempty data and unique primary keys")
    result = {}
    for qid, vector in queries:
        ranked = sorted(l2(value, vector) for value in vectors.values())[:k]
        found = hits.get(qid, [])
        if len(found) != len(ranked) or len(set(found)) != len(found) or any(key not in vectors for key in found):
            raise AssertionError("Search returned missing, duplicate or unknown hits")
        distances = sorted(l2(vectors[key], vector) for key in found)
        if exact and any(not math.isclose(a, b, rel_tol=1e-5, abs_tol=1e-6)
                         for a, b in zip(distances, ranked)):
            raise AssertionError(f"EXACT differs from the independent oracle for query {qid}")
        # Ties at the kth distance count as valid hits rather than imposing an
        # undocumented primary-key ordering on equal-distance results.
        recall = sum(distance <= ranked[-1] + 1e-6 for distance in distances) / len(ranked)
        result[qid] = {"ids": found, "recallAtK": recall}
    if set(hits) != {qid for qid, _ in queries}:
        raise AssertionError("Search returned an unexpected query id")
    return result


def search_metrics(plan):
    if plan.nodeName() == "AdaptiveSparkPlan":
        return search_metrics(plan.executedPlan())
    if "MilvusNearestByJoin" in plan.nodeName():
        values, entries = {}, plan.metrics().iterator()
        while entries.hasNext():
            entry = entries.next()
            values[entry._1()] = entry._2().value()
        return values
    children = plan.children().iterator()
    while children.hasNext():
        found = search_metrics(children.next())
        if found:
            return found
    return {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--options", type=Path, required=True)
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--id-column", default="id")
    parser.add_argument("--vector-column", default="vector")
    parser.add_argument("--k", type=int, default=3)
    parser.add_argument("--mode", choices=("exact", "approx"), default="exact")
    parser.add_argument("--max-rows", type=int, default=10000)
    args = parser.parse_args()
    if not 0 < args.k <= args.max_rows <= 10000:
        parser.error("Require 0 < k <= max-rows <= 10000")
    if any(not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value)
           for value in (args.id_column, args.vector_column)):
        parser.error("Use simple column names in this example")
    options, queries = read_options(args.options), read_queries(args.queries)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("MilvusFileSearchDemo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if not spark.version.startswith("4.0."):
            raise ValueError("This SQL example uses the Spark 4.0 candidate profile")
        base = spark.read.format("milvus").options(**options).load()
        rows = base.select(args.id_column, args.vector_column).limit(args.max_rows + 1).collect()
        if len(rows) > args.max_rows:
            raise ValueError("Input exceeds --max-rows; use a smaller fixed example snapshot")
        base.createOrReplaceTempView("demo_base")
        spark.createDataFrame(queries, "qid LONG, qvec ARRAY<FLOAT>").createOrReplaceTempView("demo_queries")
        frame = spark.sql(
            "SELECT query.qid, base.`" + args.id_column + "` AS id FROM nearest_by_join("
            "TABLE(demo_queries), TABLE(demo_base), 'vector_l2_distance(query.qvec, base."
            + args.vector_column + ")', " + str(args.k) + ", '" + args.mode + "', 'distance')")
        hits = {}
        for row in frame.collect():
            hits.setdefault(row.qid, []).append(row.id)
        metrics = search_metrics(frame._jdf.queryExecution().executedPlan())
        if not metrics or metrics.get("milvus.search.knowhere.calls", 0) <= 0:
            raise AssertionError("The connector did not execute native search")
        if args.mode == "approx" and (metrics.get("indexSegments", 0) <= 0
                                      or metrics.get("milvus.search.index.bytes", 0) <= 0):
            raise AssertionError("APPROX did not load and search a persisted index")
        result = check_hits([(row[0], row[1]) for row in rows], queries, hits, args.k, args.mode == "exact")
        print(json.dumps({"mode": args.mode, "queries": result, "metrics": metrics}, indent=2))
        print("Search demo passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
