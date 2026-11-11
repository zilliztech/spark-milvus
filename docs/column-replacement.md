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
  .option("milvus.read.include_row_id", "true")
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

- `_milvus_row_id`, exactly as the physical read produced it. It is a struct of
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
| A field with an index in the snapshot, defined or built on its segments | The index would keep describing the old values. Drop the index before the replacement and build it again after the new revisions are registered. |
| A field with statistics in a segment manifest, such as a text index or JSON key statistics | The statistics would keep describing the old values. |
| Any field of a snapshot that lists no index definitions and whose segment records do not say which index files they have | The write cannot confirm that the field has no index. A Milvus snapshot omits an empty definition list, so a collection without indexes passes once its segments are described. |

The snapshot must be a Milvus snapshot file with a positive `snapshot_info.id`
and Storage V3 segments. Only Storage V3 is written.

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
revisions and the failure record lists the state of every segment. A failed
attempt never deletes a file that a manifest references or may reference.

## The write result address

Every record of the write lives under the address passed to `.save()`. It is
read like `milvus.snapshot.path`: an absolute path or `file:` URI is a local
directory, a URI with a scheme is object storage in the bucket it names, and a
relative path is a key in the snapshot's storage. Object storage is reached
with the snapshot's storage identity.

| Record | Written | Contents |
|---|---|---|
| `_claim/` | Before any task starts | A milvus-storage manifest whose first revision holds the claim. Revision files are created with a conditional write, so of two writes only one claims the address. |
| `_operation.json` | Right after the claim | Operation id, Spark application id, source snapshot, replaced fields |
| `_commit_plan.json` | Before any manifest changes | For every segment: manifest location, pinned revision, row count and the new files |
| `_result.json` | After every segment committed | The receipt |
| `_failure.json` | When the write fails after the claim | The failed stage and every segment as `committed`, `not_committed` or `unknown` |

The claim is exactly as atomic as the manifest commits it protects: on a store
without conditional writes, milvus-storage falls back to an existence check for
both. Moving the claim to a direct conditional create once the storage JNI
supports it is tracked in
[zilliztech/spark-milvus#202](https://github.com/zilliztech/spark-milvus/issues/202).

A segment is `unknown` only when its commit failed and its manifest could not be
inspected afterwards. A commit pinned to revision `v` can only produce revision
`v + 1`, so after a failure the driver reads that revision: if it references the
write's files the segment is `committed`, otherwise `not_committed`.

### Receipt

`milvus.read.mode=write_result` reads `_result.json` and returns one row per
segment. It reads only a published receipt; for a write that did not complete
the error names the failure record.

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
