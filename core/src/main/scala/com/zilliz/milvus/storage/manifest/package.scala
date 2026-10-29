package com.zilliz.milvus.storage

/** What one segment holds: column groups, delete files, statistics and index
  * registrations, read from the two files that describe a segment.
  *
  * A Milvus snapshot lists one Avro record per segment (Milvus's
  * `ManifestEntry` in `internal/snapshotio/snapshot.go`, reached from the
  * snapshot JSON's `manifest_list`); it is Milvus metadata and only the
  * snapshot-directory read path has it. milvus-storage writes its own
  * `_metadata/manifest-{n}.avro` inside every V3 segment directory. The word
  * "manifest" in this package names the milvus-storage file only; the
  * snapshot's record is a snapshot segment record.
  *
  * Main types: SnapshotSegmentSchema (the source-defined full Avro schema),
  * SnapshotSegmentReader (the snapshot's Avro segment record),
  * SnapshotSegmentEntry and IndexFileEntry (data files, index files, exact
  * build ids, row counts and separate index format/path versions),
  * V3ManifestReader (milvus-storage's `_metadata/manifest-{n}.avro`); the
  * designed Manifest, ColumnGroup and ManifestReader are not written yet. The
  * reader takes the prefix of the schema it needs. External writers share the
  * full schema because Avro binary is positional and Milvus reads every field.
  * Version zero is the initial empty state and has no physical manifest to
  * read; it supplies zero rows, no deletes and no statistics without storage
  * access. A physical V3 manifest must contain a record; empty field lists,
  * including an L0 manifest's empty column groups, remain valid. Facts and
  * statistics readers reject a container with no record, as milvus-storage
  * does. Snapshot format 4 uses the writer schema defined as a Scala string in
  * SnapshotSegmentSchema. Version 6 uses an Avro object container with its
  * embedded writer schema. These cover stable Milvus tags v3.0.0 through v3.0.3
  * (format 4) and v3.0.4 (format 6 with the V4 writer schema). The snapshot
  * reader validates its required fields and consumes exactly one record.
  * Unsupported snapshot versions, missing fields, incompatible types and
  * damaged framing fail explicitly. Segment records retain source channel and
  * L0 role for delete planning.
  */
package object manifest
