package com.zilliz.milvus.storage.snapshot

/** The shapes of the JSON documents a snapshot read parses, one type per JSON
  * object, each named after the object with a `Json` suffix so that it is never
  * confused with the entity (`Snapshot`, `Segment`) or with the protobuf
  * message (`io.milvus.grpc.schema.CollectionSchema`) of the same name.
  *
  * `SnapshotJson` is the snapshot file Milvus DataCoord writes
  * (`{root}/snapshots/{collection}/metadata/{id}.json`, `SnapshotData` in
  * `internal/datacoord/snapshot.go`); `ManifestContentJson` is the small
  * document inside each `storagev2_manifest_list` entry; `SegmentListJson` is
  * the connector's own encoding of a segment list for a Spark option, the 1.x
  * form of a snapshot read, and goes with it.
  *
  * Everything here is a description of bytes on disk. Nothing is computed:
  * `SnapshotCatalog` turns a `SnapshotJson` into a `Snapshot`.
  *
  * Main types: SnapshotJson, CollectionSchemaJson, FieldJson, SegmentJson,
  * ManifestItemJson, SegmentListJson, JsonValues.
  */
package object json
