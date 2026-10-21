#!/usr/bin/env python3
"""Explicit fixture maintenance only; never run during builds or CI.

Requires pyarrow 22.0.0 and fastavro 1.12.2. No connector writer is imported.
"""
import hashlib
import json
from pathlib import Path
import shutil
import struct

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
DEST = ROOT / "core/src/test/resources/read-fixtures"
AVRO = json.loads((ROOT / "core/src/main/resources/milvus-segment-manifest-v4.avsc").read_text())


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def avro_zero(schema, named=None):
    named = {} if named is None else named
    if isinstance(schema, dict):
        kind = schema["type"]
        if kind == "record":
            named[schema["name"]] = schema
            return {f["name"]: avro_zero(f["type"], named) for f in schema["fields"]}
        if kind == "array":
            return []
        return avro_zero(kind, named)
    if schema in named:
        return avro_zero(named[schema], named)
    return {"long": 0, "int": 0, "boolean": False, "string": "", "bytes": b""}[schema]


def field(fid, name, kind, nullable=False, **extra):
    return {"fieldID": fid, "name": name, "data_type": kind,
            "nullable": nullable, **extra}


def create(name, columns, fields, expected, deletes=()):
    directory = DEST / name
    directory.mkdir(parents=True, exist_ok=True)
    count = len(columns[0][2])
    names = {0: "RowID", 1: "Timestamp", **{f["fieldID"]: f["name"] for f in fields}}
    arrow_fields = [pa.field(names[fid], typ, nullable=True,
                           metadata={"PARQUET:field_id": str(fid)})
                    for fid, typ, _ in columns]
    schema = pa.schema(arrow_fields, metadata={
        "storage_version": "1.0.0",
        "group_field_id_list": ",".join(str(c[0]) for c in columns)})
    table = pa.Table.from_arrays([pa.array(values, type=typ) for _, typ, values in columns], schema=schema)
    data = directory / "data.parquet"
    pq.write_table(table, data, compression="NONE", row_group_size=10, version="2.6")
    collection_schema = {"name": "fixed_read", "fields": fields}
    snapshot = {"format_version": 4,
                "snapshot_info": {"name": "fixed_read", "id": 1, "collection_id": 444,
                                  "partition_ids": [555], "create_ts": 1000},
                "collection": {"schema": collection_schema},
                "manifest_list": ["segment.avro"], "storagev2_manifest_list": [], "indexes": []}
    dump(directory / "snapshot.json", snapshot)
    record = avro_zero(AVRO)
    record.update(segment_id=777, partition_id=555, segment_level=2,
                  num_of_rows=count, storage_version=2)
    log = {"entries_num": count, "timestamp_from": 99, "timestamp_to": 101,
           "log_path": "data.parquet", "log_size": data.stat().st_size, "log_id": 1, "memory_size": 0}
    record["binlog_files"] = [{"field_id": 103, "child_fields": [], "format": "", "binlogs": [log]}]
    delta_logs = []
    if deletes:
        pk_type = columns[2][1]
        pq.write_table(pa.table({"pk": pa.array([v[0] for v in deletes], type=pk_type),
                                "ts": pa.array([v[1] for v in deletes], type=pa.int64())}),
                       directory / "delete.parquet", compression="NONE")
        delta = {**log, "entries_num": len(deletes), "log_path": "delete.parquet",
                 "log_size": (directory / "delete.parquet").stat().st_size}
        record["deltalog_files"] = [{"field_id": 0, "child_fields": [], "format": "", "binlogs": [delta]}]
        delta_logs = [{"log_id": 1, "log_path": "delete.parquet", "entries_num": len(deletes)}]
    with (directory / "segment.avro").open("wb") as stream:
        fastavro.schemaless_writer(stream, AVRO, record)
    advanced = {"milvus.snapshot.mode": "true", "milvus.snapshot.collection.id": "444",
                "milvus.snapshot.partition.ids": "555",
                "milvus.snapshot.schema.json": json.dumps(snapshot),
                "milvus.snapshot.v2.segments": json.dumps([{
                    "segment_id": 777, "partition_id": 555, "num_of_rows": count, "storage_version": 2,
                    "column_groups": [{"field_ids": [c[0] for c in columns], "file_paths": ["data.parquet"],
                                       "file_row_counts": [count], "slot_field_id": 103}],
                    "delta_logs": delta_logs}])}
    dump(directory / "options.json", advanced)
    dump(directory / "expected.json", expected)
    # milvus-backup remaps original keys into its own group/segment layout.
    backup_data = directory / "backup/binlogs/insert_log/444/555/777/777/103/1"
    backup_data.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(data, backup_data)
    backup_deltas = []
    if deletes:
        backup_delta = directory / "backup/binlogs/delta_log/444/555/777/777/1"
        backup_delta.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(directory / "delete.parquet", backup_delta)
        backup_deltas = [{"fieldID": 0, "binlogs": [{"log_path": "files/delta_log/444/555/777/1", "log_id": 1}]}]
    segment = {"segment_id": 777, "collection_id": 444, "partition_id": 555, "group_id": 777,
               "storage_version": 2, "num_of_rows": count, "is_l0": False,
               "binlogs": [{"fieldID": 103, "binlogs": [{"log_path": "files/insert_log/444/555/777/103/1", "log_id": 1}]}],
               "deltalogs": backup_deltas}
    meta = {"id": "fixed-read", "name": "fixed_read", "format": "binlog", "milvus_version": "v2.6.0",
            "collection_backups": [{"collection_id": 444, "collection_name": "fixed_read", "db_name": "default",
                                    "schema": collection_schema,
                                    "partition_backups": [{"partition_id": 555, "partition_name": "_default",
                                                           "collection_id": 444, "segment_backups": [segment]}]}]}
    dump(directory / "backup/meta/full_meta.json", meta)
    dump(directory / "backup-options.json", {"milvus.backup.dir": "s3a://read-fixture/backup", "fs.bucket_name": "read-fixture", "milvus.database.name": "default",
                                             "milvus.collection.name": "fixed_read"})
    (directory / "README.md").write_text(
        "# Fixed read input\n\nGenerated explicitly by `scripts/fixtures/generate_read_inputs.py` at baseline "
        "`5dd6f42dadc70ed20b7b719ed479ba83581eaaa8`, with PyArrow 22.0.0 and fastavro 1.12.2. "
        "Synthetic data; no private inputs or Milvus service. Parquet field IDs and schemaless Avro follow "
        "the public format-v4 schema. V2 physical column names follow the collection field names "
        "(with RowID and Timestamp for system fields), while PARQUET:field_id stores the numeric IDs. "
        "Normal tests only verify and copy these files.\n\n"
        f"There are {count} physical rows. `expected.json` records visible rows; `options.json`, "
        "`snapshot.json` and `backup-options.json` describe the same input through three offline entries. "
        "The backup URI names the logical read-fixture bucket; the consumer explicitly binds it to "
        "the local native filesystem. Object-storage integration uses a separate real test bucket. "
        + ("Delete timestamps are strictly greater than old row timestamps; equality preserves a replacement row.\n"
         if deletes else "This integration group has no deletes; equal-timestamp deletion is covered by consumer/long-v2 and consumer/string-v2.\n"))
    paths = sorted(p for p in directory.rglob("*") if p.is_file() and p.name != "SHA256SUMS")
    (directory / "SHA256SUMS").write_text("".join(
        f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(directory).as_posix()}\n" for p in paths))


def main():
    for name, typ, ids, pk_kind in [("long-v2", pa.int64(), [1, 1, 2, 3], 5),
                                    ("string-v2", pa.string(), ["dup", "dup", "gone", "活着"], 21)]:
        create("consumer/" + name,
               [(0, pa.int64(), list(range(4))), (1, pa.int64(), [99, 100, 99, 101]),
                (100, typ, ids), (101, pa.string(), ["old", None, "deleted", "alive"])],
               [field(100, "id", pk_kind, is_primary_key=True,
                      type_params=([{ "key": "max_length", "value": "64"}] if pk_kind == 21 else [])),
                field(101, "label", 21, True, type_params=[{"key": "max_length", "value": "64"}])],
               {"columns": ["id", "label"], "rows": [[ids[1], None], [ids[3], "alive"]], "rawRows": 4},
               [(ids[0], 100), (ids[2], 100)])
    rows = list(range(30))
    create("integration/v2",
           [(0, pa.int64(), rows), (1, pa.int64(), [100] * 30), (100, pa.int64(), rows),
            (101, pa.int64(), [i % 10 for i in rows]), (102, pa.float32(), [i / 32 for i in rows]),
            (103, pa.string(), [f"test_string_{i}" for i in rows]),
            (104, pa.binary(8), [struct.pack("<ff", i, i + 1) for i in rows])],
           [field(100, "id", 5, is_primary_key=True), field(101, "int64", 5), field(102, "float", 10),
            field(103, "varchar", 21, type_params=[{"key": "max_length", "value": "64"}]),
            field(104, "vector", 101, type_params=[{"key": "dim", "value": "2"}])],
           {"rows": 30, "id": "0..29", "int64": "id % 10", "float": "id / 32", "vector": "[id,id+1]"})


if __name__ == "__main__":
    main()
