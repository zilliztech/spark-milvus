# core resources

The Avro schemas for segment manifests, `milvus-segment-manifest*.avsc`, moved
here from the 1.x `src/main/resources` (section 5 of docs/design/architecture/modules.md).
`core.manifest` reads them by format version. Versions 1 to 5 are carried; the
text of each is `internal/snapshotio/snapshot.go`'s `AvroSchemaV<n>()` in
Milvus, verbatim. Version 5 (Milvus v3.0.2) adds `manifest_has_index` after
`commit_timestamp`: the segment's own manifest, not the snapshot, registers
the index. Avro is positional, so a missing version is a parse failure rather
than a partly-read record.
