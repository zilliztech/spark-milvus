# Validate the demos against a disposable Milvus deployment

This procedure prepares a small collection in an isolated open-source Milvus
deployment, creates a V3 snapshot, and reads that snapshot with both demo
languages. **Preparation writes a new test collection, deletes two of its rows,
compacts its deletes, builds an index and creates a snapshot.** The existing
Python/Java path lookup, read and search examples remain read-only.

Run from the repository root. Use a disposable Milvus deployment and its own
object-storage bucket. The preparation script refuses to replace an existing
collection. It uses PyMilvus in a separate Python environment; no service client
dependency is added to the connector or its demos.

## Prepare the deployment and clients

Use the `milvusdb/milvus:v3.0.1` standalone image with its etcd and MinIO
dependencies. Before starting Milvus, merge these settings into its `user.yaml`:

```yaml
common:
  storage:
    useLoonFFI: true
indexCoord:
  segment:
    minSegmentNumRowsToEnableIndex: 1
dataCoord:
  enableCompaction: true
  compaction:
    enableAutoCompaction: false
```

`useLoonFFI` enables Storage V3 for newly written data. Lowering the index
threshold ensures the tiny flushed segments receive indexes. Compaction must
remain enabled so the preparation can explicitly compact level-zero (L0) deletes
before creating the snapshot; automatic compaction is disabled for this test. These
settings are specific to this test; the versioned
[Milvus v3.0.1 configuration](https://github.com/milvus-io/milvus/blob/v3.0.1/configs/milvus.yaml)
defines their defaults. Configure that deployment's etcd and object-storage
addresses and credentials separately.

Set the test endpoint and names, then create a private work directory. Supply
`MILVUS_TOKEN` through the environment if the test server requires authentication.
The `default` database must already exist.

```bash
export MILVUS_URI=http://localhost:19530
export MILVUS_DATABASE=default
export MILVUS_COLLECTION=spark_demo_v3
export MILVUS_SNAPSHOT=spark_demo_snapshot
export DEMO_WORK="$(mktemp -d)"
python3 -m venv "$DEMO_WORK/prepare-venv"
"$DEMO_WORK/prepare-venv/bin/python" -m pip install 'pymilvus==2.7.0rc193'
```

Configure Java 21, Spark 4.0.1 / Scala 2.13, and a connector assembly or verified
candidate as described in [Prepare the runtime](README.md#prepare-the-runtime).
Spark must reach the same object-storage files as Milvus. A container's internal
object-storage hostname may need a different reachable address in the connector
options.

## Create the input and snapshot

This snippet takes the 16 rows from the committed
[fixture expectations](../core/src/test/resources/read-fixtures/collections/nearest/expected.json),
flushes each batch of eight, and applies the same two deletes. It explicitly
compacts L0 deletes and waits for completion, then waits for HNSW indexing to
finish with no pending rows, checks the live rows through
Milvus, prints reference searches, and creates a snapshot with one hour of
compaction protection. The create and describe request fields are defined in
the [Milvus v3.0.1 snapshot REST handler](https://github.com/milvus-io/milvus/blob/v3.0.1/internal/distributed/proxy/httpserver/snapshot_handler_v2.go).

```bash
"$DEMO_WORK/prepare-venv/bin/python" - <<'PY'
import json
import os
from pathlib import Path
import time
from urllib.request import Request, urlopen

from pymilvus import DataType, MilvusClient

uri = os.environ["MILVUS_URI"].rstrip("/")
database = os.environ["MILVUS_DATABASE"]
collection = os.environ["MILVUS_COLLECTION"]
snapshot = os.environ["MILVUS_SNAPSHOT"]
token = os.environ.get("MILVUS_TOKEN", "")
client = MilvusClient(uri=uri, token=token, db_name=database)
if client.has_collection(collection):
    raise RuntimeError("Choose a new test collection; existing data is never replaced")

schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
schema.add_field("id", DataType.INT64, is_primary=True)
schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
schema.add_field("category", DataType.INT64, nullable=True)
client.create_collection(collection, schema=schema, num_shards=1, consistency_level="Strong")
sample = json.loads(Path(
    "core/src/test/resources/read-fixtures/collections/nearest/expected.json"
).read_text())
expected = []
for segment in sample["segments"]:
    inserted = client.insert(collection, segment["rows"], timeout=60)
    if inserted["insert_count"] != 8:
        raise RuntimeError("Expected to insert each eight-row batch completely")
    client.flush(collection, timeout=120)
    expected.extend(row for row in segment["rows"] if row["id"] not in segment["deletedIds"])
client.delete(collection, ids=[3002, 3105], timeout=60)
client.flush(collection, timeout=120)

index = client.prepare_index_params()
index.add_index(field_name="vector", index_name="vector_hnsw", index_type="HNSW",
                metric_type="L2", params={"M": 4, "efConstruction": 32})
client.create_index(collection, index, timeout=180)
deadline = time.monotonic() + 180
while True:
    info = client.describe_index(collection, "vector_hnsw", timeout=30)
    if info["state"] == "Finished" and info["pending_index_rows"] == 0:
        break
    if info["state"] == "Failed" or time.monotonic() >= deadline:
        raise RuntimeError("HNSW indexing did not finish: " + str(info))
    time.sleep(2)
print("Index ready:", info)

# Move flushed L0 deletes into the V3 segments before fixing the snapshot.
job_id = client.compact(collection, is_l0=True, timeout=60)
deadline = time.monotonic() + 180
while True:
    state = client.get_compaction_state(job_id, timeout=30)
    if state == "Completed":
        break
    if time.monotonic() >= deadline:
        raise RuntimeError("L0 compaction did not complete: " + state)
    time.sleep(2)
print("L0 compaction completed:", job_id)


client.load_collection(collection, timeout=120)
actual = client.query(collection, filter="id >= 0", output_fields=["id", "vector", "category"],
                      limit=100, consistency_level="Strong", timeout=30)
if sorted(actual, key=lambda row: row["id"]) != sorted(expected, key=lambda row: row["id"]):
    raise RuntimeError("Milvus live rows differ from the expected 14-row input")
queries = json.loads(Path("demo/config/queries.json").read_text())
for label, predicate in (("all", ""), ("filtered", "category == 1")):
    hits = client.search(collection, data=[query["vector"] for query in queries],
                         anns_field="vector", limit=3, filter=predicate,
                         search_params={"metric_type": "L2", "params": {"ef": 32}},
                         consistency_level="Strong", timeout=30)
    print(label, [[{"id": hit["id"], "squaredL2": hit["distance"]}
                   for hit in group] for group in hits])

headers = {"Content-Type": "application/json"}
if token:
    headers["Authorization"] = "Bearer " + token
request = Request(uri + "/v2/vectordb/snapshots/create", method="POST", headers=headers,
                  data=json.dumps({"dbName": database, "collectionName": collection,
                                   "snapshotName": snapshot,
                                   "compactionProtectionSeconds": 3600}).encode())
with urlopen(request, timeout=120) as response:
    created = json.load(response)
if created.get("code") != 0:
    raise RuntimeError("Snapshot creation failed with code " + str(created.get("code")))
client.close()
print("Prepared", len(actual), "live rows and snapshot", snapshot)
PY
```

Complete the following reads within the protection period and keep all referenced
files available. A snapshot path lookup does not renew protection. Choose fresh
collection and snapshot names when repeating the preparation.

A snapshot taken before the L0 compaction can contain a legacy delete segment
with `storageVersion=0`. The connector intentionally rejects that input because
its public read contract requires Storage V3. Wait for compaction and create a
**new snapshot**; an earlier snapshot remains fixed at its original segment set.
Use the new snapshot's API-returned path unchanged. This preparation does not
extend the connector to legacy storage formats.

## Resolve the snapshot in both languages

Create `$DEMO_WORK/storage-options.json` with this test deployment's storage
settings. For a local MinIO deployment using test-only static credentials, the
shape is below; replace every placeholder and match its TLS/region settings:

```json
{
  "fs.storage_type": "remote",
  "fs.address": "<object-store-host>:9000",
  "fs.bucket_name": "<test-bucket>",
  "fs.root_path": "files",
  "fs.cloud_provider": "aws",
  "fs.region": "us-east-1",
  "fs.use_ssl": "false",
  "fs.use_iam": "false",
  "fs.access_key_id": "<test-only-access-key>",
  "fs.access_key_value": "<test-only-secret>"
}
```

Keep this file private (`chmod 600 "$DEMO_WORK/storage-options.json"`). For an
existing IAM configuration, retain its provider settings instead; see the
[storage reference](../docs/reference-en.md#22-storage-location-and-credentials).
The Milvus REST token does not grant access to object storage.

```bash
./demo/run.sh python 00_snapshot_path.py \
  --database "$MILVUS_DATABASE" --collection "$MILVUS_COLLECTION" \
  --snapshot "$MILVUS_SNAPSHOT" --storage-options "$DEMO_WORK/storage-options.json" \
  --output "$DEMO_WORK/read-python.json"
./demo/run.sh java SnapshotPath \
  --database "$MILVUS_DATABASE" --collection "$MILVUS_COLLECTION" \
  --snapshot "$MILVUS_SNAPSHOT" --storage-options "$DEMO_WORK/storage-options.json" \
  --output "$DEMO_WORK/read-java.json"
```

Both helpers copy the returned `data.s3Location` into `milvus.snapshot.path`
without changing it and create private options files. They must resolve the same
snapshot location. An empty location or unsupported endpoint fails the lookup.
MinIO-backed snapshots can return `minio://<address>/<bucket>/<key>`; the
connector accepts this snapshot URI. Preserve it exactly and put any reachable
object-storage address needed by Spark in `fs.address`.

## Read and search through the connector

These commands consume the live snapshot rather than the committed fixture.
The query file contains the same `[0, 0]` and `[7, 1]` vectors used above.

```bash
./demo/run.sh python 01_read_snapshot.py --options "$DEMO_WORK/read-python.json"
./demo/run.sh java ReadSnapshot --options "$DEMO_WORK/read-java.json"

./demo/run.sh python 04_exact_search.py \
  --options "$DEMO_WORK/read-python.json" --queries demo/config/queries.json
./demo/run.sh java ExactSearch \
  --options "$DEMO_WORK/read-java.json" --queries demo/config/queries.json

./demo/run.sh python 05_filtered_search.py \
  --options "$DEMO_WORK/read-python.json" --queries demo/config/queries.json --explain
./demo/run.sh java FilteredSearch \
  --options "$DEMO_WORK/read-java.json" --queries demo/config/queries.json --explain

./demo/run.sh python 06_index_search.py \
  --options "$DEMO_WORK/read-python.json" --queries demo/config/queries.json --explain
./demo/run.sh java IndexSearch \
  --options "$DEMO_WORK/read-java.json" --queries demo/config/queries.json --explain
```

Check that the read returns 14 rows with IDs `3002` and `3105` absent and that
exact searches match the [expected results](README.md#run-exact-vector-search).
Filtered search must rank only rows with `category = 1`. Each search example
checks native execution against a bounded independent oracle; the index example
also requires indexed segments and index-byte metrics and reports recall.
The custom snapshot uses the connector's HNSW search defaults unless its options
file explicitly supplies `milvus.search.params`.

Milvus's `L2` search score is **squared Euclidean distance**. The Spark
`vector_l2_distance` score printed by these examples is **Euclidean distance**.
Compare IDs and compare `spark_distance ** 2` with the Milvus score, allowing
floating-point tolerance. For example, a Milvus score of `1.0625` corresponds
to a Spark distance of approximately `1.030776`. Equal distances may have
different ID ordering; approximate search can also have less than perfect
recall.

The freshly created snapshot has its own partition, segment and field IDs.
The committed sample's IDs used by `08_partition_selection.py` and
`PartitionSelection` must be replaced before running those examples against
this input. After validation, remove only the collection, snapshot and storage
resources owned by this disposable deployment.

## Recorded validation

On 2026-10-23, this flow passed against open-source Milvus **3.0.1**
(`658cbd1689`) with etcd and MinIO, Spark **4.0.1**, Scala **2.13**, and
Java **21.0.4** on Linux x86_64. The collection, files and HNSW indexes were
created by the running Milvus service. The
[measured results](validation/milvus-3.0.1.json) contain no credentials or
deployment addresses.

| Check | Result |
|---|---|
| Python and Java snapshot lookup | Both returned the same `minio://` location, used unchanged |
| Python and Java read, exact search, filtered search and index search | All eight Spark applications passed |
| Snapshot metadata | Format version 4; two data segments, both Storage V3 |
| Row and deletion checks | 16 physical rows; 14 visible rows, identical to Milvus; IDs `3002` and `3105` excluded |
| Exact and filtered search | Both query results matched Milvus IDs and squared L2 scores |
| HNSW search | Two persisted index segments, 1,950 index bytes read, four native calls, zero exact rescans |
| HNSW recall | `recall@3 = 1.0` for both test queries |

An independent Spark check also compared results with exhaustive vector
arithmetic, checked unique segment/row addresses, and verified that disabling
delete application restored exactly the two deleted rows. This small dataset
checks correctness and index use; it is not a performance or general recall
benchmark.

The run exposed a rejected `minio://` scheme at Spark option validation; the
connector now accepts that Milvus endpoint form while retaining bucket checks.
It also confirmed that a snapshot made before L0 compaction is rejected for its
legacy delete segment. The preparation above handles that prerequisite in
Milvus before creating the snapshot.
