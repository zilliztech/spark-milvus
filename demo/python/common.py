"""Shared demo input, Spark lifecycle and result checks; examples own their queries."""
import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import sys


FIXTURE = Path(__file__).resolve().parents[2] / "core/src/test/resources/read-fixtures/collections/basic"


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
        "milvus.snapshot.path", "milvus.filter",
        "milvus.partitions", "milvus.segments", "fieldIDs", "milvus.extra.columns",
        "milvus.read.apply.deletes", "milvus.read.vector.raw", "milvus.read.columnar",
        "milvus.read.batch.max.rows", "milvus.read.batch.max.bytes", "milvus.read.arrow.max.bytes",
    }
    if any(not isinstance(key, str) or not isinstance(value, str) or not value
           or (key not in allowed and not key.startswith(("fs.", "extfs.")))
           for key, value in options.items()):
        raise ValueError("Use only documented snapshot/read and fs.*/extfs.* string options")
    return options

def bounded_rows(frame, args):
    rows = frame.limit(args.max_rows + 1).collect()
    if len(rows) > args.max_rows:
        raise ValueError("Input exceeds --max-rows; use a smaller snapshot or add a filter")
    return rows



def parse_args(description, argv=None, *, filtering=False):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--options", type=Path, help="Snapshot read options JSON; default: fixed sample")
    parser.add_argument("--max-rows", type=int, default=10000)
    parser.add_argument("--explain", action="store_true")
    if filtering:
        parser.add_argument("--where", help="Spark SQL predicate; default: category = 1")
    args = parser.parse_args(argv)
    args.fixture = args.options is None
    if not 0 < args.max_rows <= 10000:
        parser.error("Require 0 < max-rows <= 10000")
    return args


@contextmanager
def demo_session(description, argv=None, *, filtering=False):
    args = parse_args(description, argv, filtering=filtering)
    if args.fixture:
        options, args.expected_rows = fixture_input()
    else:
        options, args.expected_rows = read_options(args.options), None
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(description.splitlines()[0]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if not spark.version.startswith("4.0."):
            raise ValueError("These examples require Spark 4.0 / Scala 2.13")
        if args.fixture and not (spark.sparkContext.master == "local" or spark.sparkContext.master.startswith("local[")):
            raise ValueError("The sample requires local Spark; use --options for shared storage on a cluster")
        yield spark, args, options
    finally:
        spark.stop()
    print("Demo passed")
