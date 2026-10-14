# Milvus 2.6 migration change export (Storage V2)

`com.zilliz.spark.connector.operations.migration.MigrationExportApp` reads a
**binlog-format backup**, exports surviving inserts/updates after the original
backup cutoff, and writes Parquet. It does not require Milvus 3.0 Snapshot APIs
or connect to either Milvus instance.

## Inputs and workflow

1. **Baseline backup B0:** the backup actually used to restore the destination.
   Keep its `meta/full_meta.json`. The exporter selects `backup_timestamp` by
   database, collection name and original collection ID. This is a Milvus logical
   timestamp, not the time someone clicked Backup or the backup completion time.
2. **Final backup B1:** after cutover, stop writes to the old instance, finish
   in-flight writes, flush, then take a complete binlog-format backup of the
   source collection. Wait for backup success and keep its metadata and data
   unchanged through the export. All data segments must be Storage V2.
3. Spark reads B1, applies its deletes, selects each PK's latest surviving row,
   and exports rows in `(B0.backup_timestamp, B1.backup_timestamp]`.
4. The customer uses the exported records in their replay script.

B1 is an additional backup, with data-copy time and storage cost. The subsequent
Spark scan may read the whole collection. A small change count does not imply a
small scan or short runtime. This path does not reconstruct a final source state
from B0 alone or from a listing of the live COS bucket.

The reader internally uses the connector's `SnapshotMode`/segment-plan options
for offline reading. They are populated from B1's `full_meta.json` and copied
binlogs; no `create_snapshot`, server snapshot JSON or AVRO manifests are needed.

## Submit

Grant the Spark job read access to B0 metadata and B1 metadata/data, plus write
access to the output directory. Binding a Milvus instance in Cloud OPS does not
by itself authorize a separate backup bucket; authorize the backup paths through
the job's Volume/storage configuration. Native and Hadoop reads use the same
runtime Tencent data role. No static credentials are required.

Submit the connector assembly as a custom JAR, the class above as the main class,
and one argument: a JSON configuration URI accessible to the driver.

```json
{
  "sourceInstanceId": "in01-SOURCE",
  "databaseName": "default",
  "collectionName": "items",
  "collectionId": 123456789,
  "backupMetaPath": "s3a://BACKUP_BUCKET/baseline/meta/full_meta.json",
  "finalBackupDir": "s3a://BACKUP_BUCKET/final",
  "outputPath": "s3a://OUTPUT_BUCKET/migration/RUN_ID/items",
  "sourceWritesStopped": true,
  "sourceOptions": {
    "fs.bucket_name": "BACKUP_BUCKET",
    "fs.root_path": "",
    "fs.address": "cos.ap-shanghai.myqcloud.com",
    "fs.region": "ap-shanghai",
    "fs.cloud_provider": "tencent",
    "fs.use_iam": "true",
    "fs.use_ssl": "true",
    "fs.use_virtual_host": "true"
  }
}
```

`sourceOptions` describes B1's backup storage, not the live instance bucket.
Use one config per collection and a fresh output path for every attempt. The
exporter rejects snapshot-format backups, unsupported data formats, missing
schema/data and ambiguous latest versions instead of silently returning partial
results. Dynamic collections require the backup to include dynamic-field schema
metadata (milvus-backup's `--backup_index_extra` with etcd access).

## Output and replay

- `rows/`: user columns, including the PK and vectors. System RowID and Timestamp
  are removed.
- `versions/`: PK and original Milvus Timestamp for auditing.
- `manifest.json`: source identity, both backup references, exact time window and
  row count. Written only after both Parquet outputs succeed. An output without
  this manifest is incomplete.

This exports **surviving inserts and updates**, not a complete change log.
Deleted records and intermediate versions are absent. If deletes must be
propagated, compare B0 and B1 and produce a separate deleted-PK list before replay.
Do not infer deleted PKs from a missing row in this change export.

The destination may already have newer writes after cutover. The customer must
coordinate replay with new writes or exclude keys changed on the destination;
blind upserts can overwrite newer records. Original source timestamps alone do
not establish a safe ordering across independent instances. The exporter never
inserts, upserts or deletes records in the destination.
