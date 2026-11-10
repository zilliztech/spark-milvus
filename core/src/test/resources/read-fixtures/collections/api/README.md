# api collection fixture

Fixed local V3 collection for public read tests. `snapshot.json` uses
relative object keys; load this directory with `fs.storage_type=local` and its
copied path as `fs.root_path`. Ordinary tests verify `SHA256SUMS` and copy these
files. They never generate a segment, delete log or snapshot.

## Provenance

Storage files were frozen on 2026-10-21 from native V3 writes of the rows in
`expected.json` and are unchanged since; no generator is part of this
repository or of the normal test and build path. The snapshot declares no
indexes.

- milvus-storage: `56945200176005ce423b987f527ed7df0d18fbd4`
- milvus-proto: `5c10244615daf28a874358eec90fa8aa8ff19cc1`
- Environment: Linux x86_64, Java 21, Spark 4.0.0, Scala 2.13.16.

## Expected data

`expected.json` records the original literal input rows and expected surviving
IDs, independently of the stored bytes and reader results. Inserts have timestamp
100; where present, deletes have timestamp 200. Collection ID is 10, partition ID
is 20. Field 100 is the Long primary key, 101 the FloatVector and 102 the nullable
Long category. Tests compute expected answers from these input rows, never
from a second connector read. Regeneration requires explicit review of the data,
expectations, pinned sources and checksums; missing or damaged samples fail.
