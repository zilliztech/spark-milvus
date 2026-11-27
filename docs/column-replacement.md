# Replacing columns with a DataFrame

The connector can replace the stored values of existing fields of a fixed
Storage V3 snapshot. The caller reads every stored row with its row identity,
computes the final values with ordinary Spark code and writes them back. The
connector puts each value back at its row's stored position, writes new column
files and commits a new manifest revision for every segment. It does not
register those revisions with Milvus: making the new values visible online, and
everything else that involves the Milvus service, stays with the caller.

## Example

```scala
import org.apache.spark.sql.functions._

val snapshot = Map(
  "milvus.snapshot.path" -> "s3a://example-bucket/snapshots/before_update.json"
  // plus the storage options described in the reference
)

// Every stored row, rows marked deleted included, with its identity.
val source = spark.read
  .format("milvus")
  .options(snapshot)
  .option("milvus.read.mode", "physical")
  .load()
  .select("_milvus_row_id", "id", "score", "embedding")

// The caller decides the final values. Here, matched rows take the update,
// NULL included, and unmatched rows keep their value.
val updates = spark.read.parquet("s3a://example-bucket/updates/")
  .withColumn("_matched", lit(true))
val prepared = source
  .join(updates, Seq("id"), "left")
  .select(
    col("_milvus_row_id"),
    when(col("_matched").isNotNull, col("new_score"))
      .otherwise(col("score")).alias("score"),
    when(col("_matched").isNotNull, col("new_embedding"))
      .otherwise(col("embedding")).alias("embedding")
  )

prepared.write
  .format("milvus")
  .options(snapshot)
  .option("milvus.write.operation", "replace_columns")
  .mode("append")
  .save("s3a://example-bucket/column-writes/run-42")

// One row per committed segment: the revision each segment moved to.
val result = spark.read
  .format("milvus")
  .options(snapshot)
  .option("milvus.read.mode", "write_result")
  .load("s3a://example-bucket/column-writes/run-42")
```

The source snapshot is not changed and keeps reading the old values. A snapshot
taken after the new revisions are registered reads the new ones.
The receipt reader needs the storage options too: each read resolves its own
storage identity, and the driver must have read access to the result address.

## What the DataFrame must contain

- `_milvus_row_id`, exactly as the physical read produced it. Explicitly
  select this hidden column, since `SELECT *` omits it. It is a struct of
  the snapshot id, the segment id and the row's stored offset. Callers carry it
  through joins, filters and projections unchanged; they do not build it or
  interpret its fields.
- One column per replaced field, named after the field and typed as a read of
  that field returns it. Vector fields also accept the stored bytes
  (`BinaryType`). There is no implicit cast.
- Every stored row of every data segment of the snapshot, exactly once,
  including rows marked deleted. A row marked deleted stays deleted: the new
  column files sit beside the segment's unchanged delete records. Rows can
  arrive in any order and partitioning; the write clusters them by segment and
  sorts them by offset itself.

A missing row, a repeated row, a row read from another snapshot, a NULL in a
field that is not nullable, a vector of the wrong dimension or a value of the
wrong type fails the write before any manifest changes.

## Fields that cannot be replaced

| Field | Why |
|---|---|
| Primary key, partition key, clustering key | They place and identify rows. |
| Dynamic field, function outputs, system fields | Milvus owns their values. |
| StructArray and its members | The connector exposes them for snapshot reads starting with Milvus 3.0.6. |
| A field with an index in the snapshot, defined or built on its segments | The index would keep describing the old values. Drop the index before the replacement and build it again after the new revisions are registered. |
| A field with statistics in a segment manifest, such as a text index or JSON key statistics | The statistics would keep describing the old values. |
| Any field of a snapshot that lists no index definitions and whose segment records do not say which index files they have | The write cannot confirm that the field has no index. A Milvus snapshot omits an empty definition list, so a collection without indexes passes once its segments are described. |

The snapshot must be a Milvus snapshot file with a positive `snapshot_info.id`
and Storage V3 segments. Each collection field needs a unique physical ID.

The writer checks each segment's manifest location before starting any tasks.
For a snapshot at `<root>/snapshots/<collection>/metadata/<snapshot>.json`,
the collection and snapshot IDs in the path must match the file. Referenced
snapshots use `<root>/insert_log/<collection>/<partition>/<segment>`.
Self-contained exports use `<root>/files/insert_log/<collection>/<partition>/<segment>`.
The collection, partition and segment IDs must match the metadata. Older
snapshots that omit the layout can use either directory convention, provided
all data segments use the same one.

If you move or rename the snapshot JSON, keep the data under the same root.
For example, `<root>/snapshot.json` and `<root>/snapshots/before_update.json`
both use the layouts above. Reads can still use nonstandard manifest locations.

## Write options

Reuse the Snapshot path and storage settings from the read. Set
`milvus.read.mode=physical` on the read builder and select `_milvus_row_id`
before joining external updates.

| Option | Default | What to provide |
|---|---|---|
| `milvus.write.operation` | Unset; required for a write | `replace_columns`, the only supported write operation. |
| `milvus.snapshot.path` | Unset; required | The same Snapshot file used to produce `_milvus_row_id`. The writer reads its schema, segments and pinned manifest revisions. |
| `milvus.snapshot.max.json.bytes` | `67108864` (64 MiB) | A positive integer byte limit for the Snapshot JSON file read during planning. Increase it if the Snapshot metadata exceeds the default. |
| `fs.*` | Resolved from the supplied options and Spark's Hadoop configuration | The [storage settings](reference.md#22-storage-location-and-credentials) for the Snapshot. The job needs read access to the existing data and write access to the segment locations and result address. |
| `path` | Unset; required | Supply it with `.save(resultPath)`. It is a fresh address for the write's records and receipt; the replacement column files are stored beside the existing segment data. |

The writer takes the fields to replace from the DataFrame's columns. Its input
must cover every stored row of the snapshot, including rows marked deleted.
A source filter that excludes a partition with stored rows fails this check.

| Options rejected by the writer | Reason |
|---|---|
| `milvus.read.mode` | This configures the source read. The DataFrame must already contain `_milvus_row_id`. |
| `milvus.snapshot.manifests`, `milvus.snapshot.schema.bytes`, `milvus.snapshot.schema.json` | A write requires the original Snapshot file, not an explicit empty Snapshot. |

## Write modes

The write is a DataSource V2 batch write and accepts `mode("append")` only:
it appends one immutable result, the new column files, manifest revisions and
receipt, while `milvus.write.operation=replace_columns` says that the new
revisions replace the given columns. Spark refuses `errorifexists`, which is the
default when no mode is given, and `ignore` for such a write; `overwrite` is not
supported either.

## How the write commits

1. The driver claims the write result address given to `.save()`. An address
   belongs to one write: a second independent write with the same address
   fails before it writes anything.
2. Tasks write the replaced columns of each segment into new files beside the
   segment's data, one column group per field. File names are unique per task
   attempt, so retried and speculative attempts never overwrite each other. No
   task changes a manifest.
3. The driver takes the attempt Spark adopted for each partition, checks that
   every segment arrived complete, and records the commit plan.
4. The driver commits each segment's new manifest revision from the revision
   the snapshot pinned. If a segment's manifest moved on after the snapshot, for
   example because statistics or an index were committed, that segment's commit
   is refused rather than applied to the newer revision.
5. Once every segment committed, the driver publishes the receipt.

Segments commit separately, so there is no collection-wide atomic switch. When
the write fails after some segments committed, those segments keep their new
revisions and the failure record lists the state of every segment.

## The write result address

Every record of the write lives under the address passed to `.save()`. It is
read like `milvus.snapshot.path`: an absolute path or `file:` URI is a local
directory, a URI with a scheme is object storage in the bucket it names, and a
relative path is a key in the snapshot's storage. Object storage is reached
with the snapshot's storage identity.

| Record | Written | Contents |
|---|---|---|
| `_operation.json` | Before any task starts | Operation id, Spark application id, source snapshot, replaced fields. Conditional creation gives the address to one write. |
| `_claim/_metadata/manifest-1.avro` | Right after `_operation.json` | A guard naming the operation, created conditionally where the earlier claim protocol claimed an address |
| `_commit_plan.json` | Before any manifest changes | For every segment: manifest location, pinned revision, row count and the new files |
| `_result.json` | After every segment committed | The receipt, published by conditional creation so it cannot replace an existing receipt |
| `_failure.json` | When the write fails after the claim | The failed stage and every segment as `committed`, `not_committed` or `unknown` |

The operation record uses the storage backend's atomic create-if-absent
operation through the native filesystem. A backend without conditional writes rejects the write
before any task starts; there is no existence-check fallback. The bundled
native library supports conditional creation on `aws`, `gcp`, `azure`,
`aliyun`, `tencent` and `volcengine`, and on local storage; `huawei` does not
support it, so column replacement is unavailable there. Creating the
record succeeds only after its writer closes successfully. If creating the
operation record, its guard or the receipt fails with an uncertain outcome, the
driver reads the record back: the complete expected content confirms success,
another operation id rejects reuse, and a missing, unreadable or different
record preserves the failure. Records must have the expected format and a
nonempty operation id. The local filesystem provides exclusion only within one
process and is intended for tests; a local record synchronizes directories only
up to the write result address.

Every record on object storage is uploaded by one request, whatever
`fs.s3.use_crt` selects for other uploads, so a failed upload leaves no partial
record and the backend decides a conditional create in that request. A record
must be smaller than 5 GiB. On `azure`, the native writer creates an empty blob
when a conditional create starts and fills it when the create completes, so an
interrupted create can leave an empty `_operation.json` that keeps the address
occupied, or an empty `_result.json`; give the retried write a new address.

The earlier claim protocol committed `_claim/_metadata/manifest-1.avro` as a
manifest. Addresses holding such a claim are occupied and are rejected without
changing them. A write also creates that key, holding its guard, once its own
record exists, and proceeds only if both creates succeed: a connector using the
earlier protocol then finds the address occupied, and a write that loses the
guard to one stops before any task starts. Every independent write still needs
its own address.

A segment is `committed` when storage acknowledges its manifest commit.
If releasing native resources subsequently fails, the write stops and
`_failure.json` still records that segment's acknowledged `version` and error.
Segments left in the queue or whose transaction failed during preparation are
`not_committed`. Abort removes their task files.

A submitted commit pinned to revision `v` can only produce `v + 1`. If it
throws, the driver inspects that revision. If the revision contains none of the
write's output filenames, another writer committed it and the segment is
`not_committed`. If any filenames overlap, or the revision is missing or
unreadable, the segment is `unknown`. Its failure record includes
`candidate_version` for inspection. If the driver is interrupted while a worker
is still running, it also records that segment as `unknown`.

Abort retains task files from submitted commits, including when another writer
won the pinned revision. That revision alone cannot rule out later references
to those files.

### Receipt

`milvus.read.mode=write_result` reads `_result.json` and returns one row per
segment. It reads only a published receipt; for a write that did not complete
the error names the failure record.

| Option | Default | What to provide |
|---|---|---|
| `milvus.read.mode` | Unset | Set `write_result` to read a receipt. |
| `path` | Unset; required | Pass the write's result address to `.load(resultPath)`, not the `_result.json` file itself. |
| `fs.*` | Resolved from the supplied options and Spark's Hadoop configuration | Storage settings with read access to the result address. This read resolves its own storage identity. |

A receipt read does not need `milvus.snapshot.path` or access to the source
Snapshot. The driver reads the receipt metadata and Spark returns it with the
fixed schema below; do not supply a different schema.

| Column | Type | Meaning |
|---|---|---|
| `operation_id` | String | The write's identity, also in every record |
| `collection_id` | Long | The snapshot's collection |
| `schema_version` | Int | The collection schema version as the snapshot states it; Milvus starts it at `0` |
| `snapshot_id`, `snapshot_name` | Long, String | The source snapshot |
| `segment_id`, `partition_id` | Long | The segment |
| `manifest_base_path` | String | The segment's manifest location, as in the snapshot |
| `source_version` | Long | The revision the snapshot pinned and the write replaced columns of |
| `version` | Long | The revision the write committed |
| `row_count` | Long | Stored rows of the segment, rows marked deleted included |
| `changed_fields`, `changed_field_ids` | Array | The replaced fields |

`_result.json` itself is a JSON object with `format` `milvus-column-write`,
`format_version` `1` and `record` `result`, the same fields at the top level and
under `source_snapshot`, and a `segments` array that adds each segment's
`column_groups`: the field ids of each group, its format and its files with row
counts.

## After the write

The receipt records storage revisions; registering them is the caller's step.
A receiver that adopts a revision must check that the segment's current revision
is still `source_version` and that the collection schema still has
`schema_version`, and must not adopt a revision after either changed. Indexes
or statistics that the service keeps outside the snapshot, for example an index
created after the snapshot was taken, are the caller's to check before
registering. A response lost while registering is not proof that nothing was
registered; check the service state before retrying.
