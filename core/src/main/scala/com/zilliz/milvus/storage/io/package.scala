package com.zilliz.milvus.storage

/** Opens files in object storage. Every read and write goes through here.
  *
  * The implementation holds a JNI handle onto milvus-storage's C filesystem and
  * offers open, read, list, stat and create. It is a wrapper, not an
  * implementation: the seven cloud backends, their credential providers and the
  * filesystem itself all live in the C layer.
  *
  * A handle is a pointer inside this process. It must never reach an
  * InputPartition, which Spark serializes and ships; partitions carry the
  * description (path, manifest version, column names, the `fs.*` map) and the
  * reader is opened on the executor.
  *
  * `loon_segment_reader_get_filtered_stream` is deliberately not exposed: the C
  * base class accepts a predicate, returns OK and ignores it for every format
  * except Vortex, which is out of scope. Predicates are evaluated in core.expr.
  *
  * `io.hadoop` is the migration-period path and goes away when the JNI wrapper
  * in native-storage lands.
  *
  * Main types: ObjectStore, FileInfo, SeekableInput. Capabilities: R2, R3, W1
  * (see docs/design/capabilities.md). Design:
  * docs/design/architecture/storage-access.html section 4.
  */
package object io
