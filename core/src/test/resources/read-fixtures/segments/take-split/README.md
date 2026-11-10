# Random row retrieval with split vector columns

Generated once on 2026-10-21 with Java 21 through native V3 segment and
manifest writes. Normal tests only verify SHA-256, copy these files, and
read them; they never rebuild a sample.

The generation used milvus-storage
`56945200176005ce423b987f527ed7df0d18fbd4`.
The one-off generation source had SHA-256
`796c4985e6908e007289f1a4a906ecf5ea06937b6987bebdd20b113fdf29ddd8`.

Columns are `100: Int64`, nullable `101: UTF-8`, and
`102: FixedSizeBinary(8)`. For physical row `r`, ID is `r * 11`, name is null
when `r % 7 == 0` and otherwise `row-r`, and all eight vector bytes equal
`r.toByte`. Manifest version is 1. Paths inside manifests are relative to the
fixture's filesystem root, so copying the group relocates it unchanged.

One segment contains 18,000 rows starting at 0. `^102$` was written into a
separate column group. The test removes its vector file to prove scalar-only
projection avoids reading it, and checks offsets across the 8192 batch boundary.
