# Fixed read input

Generated explicitly by `scripts/fixtures/generate_read_inputs.py` at baseline `5dd6f42dadc70ed20b7b719ed479ba83581eaaa8`, with PyArrow 22.0.0 and fastavro 1.12.2. Synthetic data; no private inputs or Milvus service. Parquet field IDs and schemaless Avro follow the public format-v4 schema. V2 physical column names follow the collection field names (with RowID and Timestamp for system fields), while PARQUET:field_id stores the numeric IDs. Normal tests only verify and copy these files.

There are 4 physical rows. `expected.json` records visible rows; `options.json`, `snapshot.json` and `backup-options.json` describe the same input through three offline entries. The backup URI names the logical read-fixture bucket; the consumer explicitly binds it to the local native filesystem. Object-storage integration uses a separate real test bucket. Delete timestamps are strictly greater than old row timestamps; equality preserves a replacement row.
