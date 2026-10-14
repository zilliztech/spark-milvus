# Migration change export (Storage V2)

`com.zilliz.spark.connector.operations.migration.MigrationExportApp` exports the
**final surviving inserts and updates** after a collection's backup cutoff.
It reads a fixed source snapshot and writes Parquet. It never inserts, upserts,
or deletes records in either Milvus instance.

## Before running

1. Record the actual backup used to restore the destination. Supply its
   `meta/full_meta.json`; the program selects `backup_timestamp` by database,
   collection name and original collection ID. This is a Milvus logical timestamp,
   not the time someone clicked Backup or the backup completion time.
2. Stop writes to the old instance, finish in-flight writes, flush, then create
   a complete collection snapshot. Keep its referenced files protected from GC
   until the job finishes. A live collection or a partition-only snapshot is not
   a valid substitute. All data segments must be Storage V2.
3. Grant the Spark job read access to source metadata/data and write access to
   the output directory. Cloud OPS binds the source instance; use a Volume for
   other buckets. Runtime Hadoop credentials and the native filesystem settings
   must refer to the same bucket and role. No static credentials are required.

Submit the connector assembly as a custom JAR, the class above as the main class,
and one argument: a JSON configuration URI accessible to the driver.

```json
{
  "sourceInstanceId": "in01-SOURCE",
  "databaseName": "default",
  "collectionName": "items",
  "collectionId": 123456789,
  "backupMetaPath": "s3a://BACKUP_BUCKET/backup/meta/full_meta.json",
  "snapshotPath": "s3a://SOURCE_BUCKET/INSTANCE_ROOT/snapshots/metadata.json",
  "outputPath": "s3a://OUTPUT_BUCKET/migration/RUN_ID/items",
  "sourceWritesStopped": true,
  "sourceOptions": {
    "fs.bucket_name": "SOURCE_BUCKET",
    "fs.root_path": "INSTANCE_ROOT",
    "fs.address": "cos.ap-shanghai.myqcloud.com",
    "fs.region": "ap-shanghai",
    "fs.cloud_provider": "tencent",
    "fs.use_iam": "true",
    "fs.use_ssl": "true",
    "fs.use_virtual_host": "true"
  }
}
```

Use one job/config per collection and a new output path for every attempt.
The reader applies deletes first; the exporter picks the latest surviving row
per PK and keeps timestamps in `(backup_timestamp, final_snapshot.create_ts]`.
Equal latest timestamps for a PK are rejected rather than choosing arbitrary
payloads. The scan may read the whole source collection; a small change count
does not imply a small scan or short runtime.

## Output and replay

- `rows/`: user columns, including the PK and vectors, ready for a customer replay
  script. System RowID and Timestamp are removed.
- `versions/`: PK and original Milvus Timestamp for auditing.
- `manifest.json`: identity, exact time window and row count. Written only after
  both Parquet outputs succeed. Treat an output without this manifest as incomplete.

This is **not a complete change log**: deleted records and intermediate versions
are absent. If deletes must be propagated, compare the actual baseline backup
against the final snapshot and produce a separate deleted-PK list before replay.
Do not infer deleted PKs from a missing row in this change export.

The destination may already have newer writes after cutover. Do not blindly
upsert all exported rows over them: the customer must coordinate replay with new
writes or exclude keys changed on the destination. Original source timestamps
alone do not establish a safe ordering across independent instances. This
exporter deliberately does not connect to or modify the destination.
