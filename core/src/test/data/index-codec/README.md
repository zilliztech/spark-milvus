# Index payload fixtures

These small, uncompressed Parquet files were written with PyArrow 22.0.0.
They contain a single unnamed column, as Milvus payload writers produce.
The ordinary JVM tests read the files directly; Python is not a test dependency.

To reproduce them from this directory:

```python
import pyarrow as pa
import pyarrow.parquet as pq

fixtures = {
    "int8": pa.array([-128, -1, 0, 1, 127], type=pa.int8()),
    "string": pa.array([b"\x00\xff\x7f\x80\x01\x02"], type=pa.binary()),
    "string-multiple": pa.array([b"one", b"two"], type=pa.binary()),
    "int8-out-of-range": pa.array([-129, 128], type=pa.int32()),
    "null": pa.array([None], type=pa.binary()),
}
for name, array in fixtures.items():
    pq.write_table(pa.table({"": array}), name + ".parquet",
                   compression="NONE", use_dictionary=False,
                   row_group_size=2, version="1.0")
```

`BinlogFixture` wraps these payloads in the Milvus event format with exact
64-bit object identity. The INT8 fixture has three row groups. The STRING
fixture deliberately contains bytes that are not UTF-8. Invalid fixtures
exercise signed-byte range checks, multi-row STRING rejection and null rejection.
