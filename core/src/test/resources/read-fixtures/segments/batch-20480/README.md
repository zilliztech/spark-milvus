# Native Arrow batch slicing

Generated once on 2026-10-21 with Java 21 through native V3 segment and
manifest writes. Normal tests only verify SHA-256, copy these files, and
read them; they never rebuild a sample.

The generation used milvus-storage
`56945200176005ce423b987f527ed7df0d18fbd4`.
The one-off generation source had SHA-256
`796c4985e6908e007289f1a4a906ecf5ea06937b6987bebdd20b113fdf29ddd8`.

Columns are `id: Int64` and non-null `name: UTF-8`. For global row `r`,
ID is `r * 7` and name is `row-r`. Only the frozen parquet files are needed:
tests provide the explicit column-group layout and row counts.

`segment.parquet` contains 20,480 rows starting at 0, exceeding twice the 8192
default batch limit. Values, batch counts, native calls, copy metrics and
resource release must survive native Arrow slicing (milvus-storage#493).
