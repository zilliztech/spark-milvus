# Existing public read samples

These pre-existing files are pinned as they appeared at connector baseline
`5dd6f42dadc70ed20b7b719ed479ba83581eaaa8`; nothing regenerates them.
`SHA256SUMS` covers this directory, including the index-codec payloads.

- `sample_snapshot.json` is the existing snapshot parser input; exact metadata
  expectations are in
  [SnapshotJsonTest](../scala/com/zilliz/milvus/storage/snapshot/json/SnapshotJsonTest.scala).
- `v3-bloom-filter-469093182140267755.json` contains Int64 primary keys 0..2999;
  `v3-delta-469093182140298464.parquet` deletes keys 0..9 at timestamp
  469093213902995459. Existing stats and delete reader tests assert those values.
- The synthetic Parquet index payloads have their generation recipe and expected
  values in [index-codec/README.md](index-codec/README.md).

The original capture did not record an immutable server image for every file.
These samples prove decoding of the pinned bytes, not a server-version matrix.
Tests and CI only read these samples; they do not connect to the original store.
