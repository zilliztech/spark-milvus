# Native Arrow batch slicing

Generated once on 2026-10-21 from spark-milvus commit
`5dd6f42dadc70ed20b7b719ed479ba83581eaaa8`, using Java 21 and the baseline
`V3SegmentWriter` / `ManifestTransaction`. Normal tests only verify SHA-256,
copy these files, and read them; they never rebuild a sample.

The generation used milvus-storage
`56945200176005ce423b987f527ed7df0d18fbd4`, Knowhere
`65e0fc53d79942ac7862454b0b762e7e40cc5df4`, and the OSS Linux x86_64 bundle
with SHA-256
`3b8bb919659bd5bc2015bad40e562ba036e342b91553b55b28c9f91fc323ad7c`.
The one-off generation source had SHA-256
`796c4985e6908e007289f1a4a906ecf5ea06937b6987bebdd20b113fdf29ddd8`.

Columns are `id: Int64` and non-null `name: UTF-8`. For global row `r`,
ID is `r * 7` and name is `row-r`. Only the frozen parquet files are needed:
tests provide the explicit column-group layout and row counts.

`segment.parquet` contains 20,480 rows starting at 0, exceeding twice the 8192
default batch limit. Values, batch counts, native calls, copy metrics and
resource release must survive native Arrow slicing (milvus-storage#493).
