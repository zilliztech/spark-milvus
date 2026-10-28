"""Shared demo input, Spark lifecycle and result checks; examples own their queries."""
import argparse
from contextlib import contextmanager
import json
import math
from pathlib import Path
import re
import struct
import sys


FIXTURE = Path(__file__).resolve().parents[2] / "core/src/test/resources/read-fixtures/collections/nearest"
FIXTURE_QUERIES = [(1, [0.0, 0.0]), (2, [7.0, 1.0])]


def fixture_input():
    # Reuse the same checksum verification as public CI. Never regenerate files.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
    from ci.check_read_fixtures import check_group
    check_group(FIXTURE)
    expected = json.loads((FIXTURE / "expected.json").read_text())
    rows = [row for segment in expected["segments"] for row in segment["rows"]
            if row["id"] not in segment["deletedIds"]]
    return {
        "milvus.snapshot.path": "snapshot.json",
        "fs.storage_type": "local",
        "fs.root_path": str(FIXTURE),
    }, rows


def check_fixture_rows(rows, expected):
    actual = sorted((row["id"], list(row["vector"]), row["category"]) for row in rows)
    wanted = sorted((row["id"], row["vector"], row["category"]) for row in expected)
    if actual != wanted:
        raise AssertionError("Snapshot rows or deletes differ from the frozen fixture expectations")


def read_options(path):
    options = json.loads(Path(path).read_text())
    if not isinstance(options, dict) or not options.get("milvus.snapshot.path"):
        raise ValueError("Options must contain a fixed milvus.snapshot.path")
    allowed = {
        "milvus.snapshot.path", "milvus.filter", "milvus.search.params",
        "milvus.partitions", "milvus.segments", "fieldIDs", "milvus.extra.columns",
        "milvus.read.apply.deletes", "milvus.read.vector.raw", "milvus.read.columnar",
        "milvus.read.batch.max.rows", "milvus.read.batch.max.bytes", "milvus.read.arrow.max.bytes",
    }
    if any(not isinstance(key, str) or not isinstance(value, str) or not value
           or (key not in allowed and not key.startswith(("fs.", "extfs.")))
           for key, value in options.items()):
        raise ValueError("Use only documented snapshot/read/search and fs.*/extfs.* string options")
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
        # Spark receives ARRAY<FLOAT>; the oracle must rank those same float32
        # values, including rounding of user-supplied JSON numbers.
        result.append((qid, [struct.unpack("f", struct.pack("f", value))[0] for value in vector]))
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


def parse_args(description, argv=None, *, search=False, snapshot=True, filtering=False):
    parser = argparse.ArgumentParser(description=description)
    if snapshot:
        parser.add_argument("--options", type=Path, help="Private snapshot options JSON; default: committed sample")
        parser.add_argument("--max-rows", type=int, default=10000, help="Driver-side verification limit (up to 10000)")
    if search or filtering:
        parser.add_argument("--where", help="Spark SQL predicate; filter examples default to category = 1")
    parser.add_argument("--explain", action="store_true", help="Print the execution plan")
    if search:
        parser.add_argument("--queries", type=Path, help="Query JSON; required with --options")
        parser.add_argument("--id-column", default="id")
        parser.add_argument("--vector-column", default="vector")
        parser.add_argument("--k", type=int, default=3)
    args = parser.parse_args(argv)
    args.fixture = snapshot and args.options is None
    if snapshot and not 0 < args.max_rows <= 10000:
        parser.error("Require 0 < max-rows <= 10000")
    if search:
        if args.options and args.queries is None:
            parser.error("--queries is required with --options")
        if not 0 < args.k <= args.max_rows:
            parser.error("Require 0 < k <= max-rows")
        if args.fixture and (args.id_column, args.vector_column) != ("id", "vector"):
            parser.error("The fixture uses id and vector columns")
        if any(not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value)
               for value in (args.id_column, args.vector_column)):
            parser.error("Use simple column names in these examples")
    return args


@contextmanager
def demo_session(description, argv=None, *, search=False, snapshot=True, filtering=False):
    """Configure the example input and close Spark even when a query fails."""
    args = parse_args(description, argv, search=search, snapshot=snapshot, filtering=filtering)
    options, args.expected_rows = ({}, None)
    if snapshot:
        options, args.expected_rows = fixture_input() if args.fixture else (read_options(args.options), None)
    queries = (read_queries(args.queries) if args.queries else FIXTURE_QUERIES) if search else []

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(description.splitlines()[0]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if not spark.version.startswith("4.0."):
            raise ValueError("These examples require Spark 4.0 / Scala 2.13")
        if args.fixture and not (spark.sparkContext.master == "local"
                                 or spark.sparkContext.master.startswith("local[")):
            raise ValueError("The sample requires local Spark; use --options for shared storage on a cluster")
        yield spark, args, options, queries
        print("Demo passed")
    finally:
        spark.stop()


def bounded_rows(frame, args):
    rows = frame.limit(args.max_rows + 1).collect()
    if len(rows) > args.max_rows:
        raise ValueError("Input exceeds --max-rows; use a smaller snapshot or add a filter")
    return rows


def check_search(frame, base, queries, args, *, exact, require_index=False):
    """Verify native execution and compare returned neighbors with a Python oracle.

    Query construction stays in each example; this helper only checks and
    displays the result. It accepts at most --max-rows base rows on the driver.
    """
    rows = bounded_rows(base.select(args.id_column, args.vector_column), args)
    if not rows:
        raise ValueError("No rows to search; choose a nonempty snapshot or a less restrictive --where")
    if args.explain:
        frame.explain()
    print(f"{'qid':>6}  {'id':>12}  {'distance':>12}")
    hits = {}
    for row in sorted(frame.collect(), key=lambda row: (row.qid, row.distance, row.id)):
        print(f"{row.qid:>6}  {str(row.id):>12}  {row.distance:>12.6f}")
        hits.setdefault(row.qid, []).append(row.id)
    metrics = search_metrics(frame._jdf.queryExecution().executedPlan())
    if not metrics or metrics.get("milvus.search.knowhere.calls", 0) <= 0:
        raise AssertionError("The connector did not execute native search")
    if require_index and (metrics.get("indexSegments", 0) <= 0
                          or metrics.get("milvus.search.index.bytes", 0) <= 0):
        raise AssertionError("APPROX did not load and search a persisted index")
    result = check_hits([(row[0], row[1]) for row in rows], queries, hits, args.k, exact)
    print(json.dumps({"mode": "exact" if exact else "approx", "queries": result, "metrics": metrics}, indent=2))
