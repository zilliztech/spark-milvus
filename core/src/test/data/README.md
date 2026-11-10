# Existing public read samples

These pre-existing files are pinned as captured; nothing regenerates them.
`SHA256SUMS` covers the retained snapshot, stats and delete samples.

- `sample_snapshot.json` is the existing snapshot parser input; exact metadata
  expectations are in
  [SnapshotJsonTest](../scala/com/zilliz/milvus/storage/snapshot/json/SnapshotJsonTest.scala).
- `v3-bloom-filter-469093182140267755.json` contains Int64 primary keys 0..2999;
  `v3-delta-469093182140298464.parquet` deletes keys 0..9 at timestamp
  469093213902995459. Existing stats and delete reader tests assert those values.

The original capture did not record an immutable server image for every file.
These samples prove decoding of the pinned bytes, not a server-version matrix.
Tests and CI only read these samples; they do not connect to the original store.
