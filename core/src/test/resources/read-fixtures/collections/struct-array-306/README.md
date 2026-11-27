# StructArray snapshot from Milvus 3.0.6

The official `milvusdb/milvus:v3.0.6` Linux x86_64 image
(`sha256:6c606011b3d9124487344b80434d98094624ef359e8a33715263fd1206772069`)
produced these files on 2026-11-27 with Storage V3 enabled. Four rows were
inserted through the REST API into the collection `struct_array_read`, flushed
and captured with CreateSnapshot. The snapshot JSON was copied to
`snapshot.json`. The referenced segment Avro, Storage manifest, Parquet files
and primary key statistics retain their original object keys and bytes.

The nullable `chunks` field has four members. Milvus stores `text` as a
protobuf scalar array, `score` as a list of nullable Int32 values, `embedding`
as a list of nullable two-dimensional FloatVector values, and `tags` as a
list of nullable arrays of nullable Int64 values. The vector lives in a
separate column group from the scalar members.

| id | Inserted chunks in member order (text, score, embedding, tags) |
| --- | --- |
| 1 | `[("alpha", 7, [1, 2], [3, null]), ("beta", null, null, [])]` |
| 2 | `[]` |
| 3 | `null` |
| 4 | `[("delta", 0, [-1.5, 0.5], null)]` |
