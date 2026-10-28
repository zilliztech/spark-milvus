# arrays collection fixture

Fixed local V3 collection for public read/search tests. `snapshot.json` uses
relative object keys; load this directory with `fs.storage_type=local` and its
copied path as `fs.root_path`. Ordinary tests verify `SHA256SUMS` and copy these
files. They never generate a segment, delete log, index or snapshot.

## Provenance

Frozen once on 2026-10-21 using the original `LocalMilvusCollection.write` from
Spark-Milvus commit `5dd6f42dadc70ed20b7b719ed479ba83581eaaa8` and the `arrays` input
rows from `NearestByJoinTest.scala` at that commit. A temporary `Test/runMain` invoked that
helper; no generator is part of the normal test or build path. Unreferenced
`delete-fixture-*` staging files were omitted. Native manifest paths and all
referenced storage/index bytes are unchanged.

- milvus-storage: `56945200176005ce423b987f527ed7df0d18fbd4`
- Knowhere: `65e0fc53d79942ac7862454b0b762e7e40cc5df4`
- milvus-proto: `5c10244615daf28a874358eec90fa8aa8ff19cc1`
- Generator: Linux x86_64, Java 21, Spark 4.0.0, Scala 2.13.16.
- Native bundle SHA-256: `3b8bb919659bd5bc2015bad40e562ba036e342b91553b55b28c9f91fc323ad7c`.
- Indexed segments use HNSW, index version 8, M=4 and efConstruction=32.

## Expected data

`expected.json` records the original literal input rows and expected surviving
IDs, independently of the stored bytes and reader results. Inserts have timestamp
100; where present, deletes have timestamp 200. Collection ID is 10, partition ID
is 20. Field 100 is the Long primary key, 101 the FloatVector and 102 the nullable
Long category. Tests compute exact-search answers from these input rows, never
from a second connector read. Regeneration requires explicit review of the data,
expectations, pinned sources and checksums; missing or damaged samples fail.
