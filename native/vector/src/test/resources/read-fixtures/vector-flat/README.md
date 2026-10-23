# Persisted OSS FLAT index

Generated once on 2026-10-21 at spark-milvus commit
`5dd6f42dadc70ed20b7b719ed479ba83581eaaa8`, using Knowhere
`65e0fc53d79942ac7862454b0b762e7e40cc5df4` and OSS Linux x86_64 bundle SHA-256
`3b8bb919659bd5bc2015bad40e562ba036e342b91553b55b28c9f91fc323ad7c`.

The upstream `KnowhereIndex.build` and `serialize` ran only during this freeze.
The index has version 8, type FLAT, FLOAT32 values, dimension 2, and L2 metric.
Rows are `[0, 0]` and `[1, 0]`. A query `[0, 0]` at k=1 must return row 0 with
squared distance 0. The JVM smoke verifies every checked-in checksum, loads
this persisted payload and searches it without building an index.

This group lives in native-vector Test resources so its JDK-only smoke has
no dependency on Scala, core, or Spark.
