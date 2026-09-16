package com.zilliz.milvus.storage

/** Opens files in object storage. Every read and write goes through here.
  *
  * NativeObjectStore owns the upstream MilvusStorageFileSystem and its
  * MilvusStorageProperties until close, and offers open, read, list, stat and
  * create. The upstream binding owns JNI and library loading. It is a wrapper,
  * not an implementation: the seven cloud backends, their credential providers
  * and the filesystem itself all live in the C layer.
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
  * NativeObjectStore is the only implementation. The Hadoop-backed one that
  * carried the migration is gone, and with it the last `org.apache.hadoop`
  * reference in core's main sources. Tests use LocalObjectStore, in core's test
  * sources, which reads the local disk and needs no native library. The
  * filesystem's exists operation distinguishes missing files from access or I/O
  * failures; those failures propagate to the caller.
  *
  * Main types: ObjectStore, ObjectStoreFactory, NativeObjectStore, FileInfo.
  * Capabilities: R2, R3, W1 (see docs/design/capabilities.md). Design:
  * docs/design/architecture/storage-access.html section 4.
  */
package object io
