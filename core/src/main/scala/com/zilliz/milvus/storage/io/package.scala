package com.zilliz.milvus.storage

/** Opens files in object storage and owns the single native session.
  * NativeObjectStore owns the upstream MilvusStorageFileSystem and its
  * MilvusStorageProperties until close, and offers readAll, readAt, size, list,
  * exists and a scoped low-level filesystem operation. Keys carry
  * `fs.root_path`; on the local backend the store takes that root off, and
  * NativeObjectStore.localPath names the filesystem path a local key resolves
  * to by the same rule. WritableObjectStore adds file output, conditional
  * creation and cleanup through the same session; NativeWritableObjectStore
  * borrows its existing filesystem. Conditional creation delegates to the
  * native writer and succeeds only after its close completes. Existing objects
  * and unsupported conditional writes fail without an existence-check fallback;
  * native failures retain their type and details. The binding has no writer
  * abort, so a failed write can still be finalized; callers needing
  * all-or-nothing objects keep them within one upload request. The local
  * backend's conditional exclusion is limited to one process. File writes
  * create parent directories only for the local backend; remote object writes
  * do not request bucket or directory creation. Explicit createDir retains the
  * upstream filesystem semantics, except in object-storage mode, which
  * NativeWritableObjectStore.open selects for every backend but the local one:
  * object storage has no directories, and the native recursive create starts
  * with a bucket check (S3 HeadBucket), which a role scoped to a key prefix
  * cannot run. Every operation still requires an open store. The upstream
  * binding owns JNI and library loading. It is a wrapper, not an
  * implementation: the seven cloud backends, their credential providers and the
  * filesystem itself all live in the C layer.
  *
  * NativeObjectStore.filesystemKey supplies the backend-relative path to native
  * manifest operations that run outside the store session.
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
  * NativeObjectStore is the only implementation; core's main sources have no
  * `org.apache.hadoop` reference. Tests use LocalObjectStore, in core's test
  * sources, which reads the local disk and needs no native library. The
  * filesystem's exists operation distinguishes missing files from access or I/O
  * failures; those failures propagate to the caller.
  *
  * ConcurrentRangeReads reads a list of object ranges several at a time and
  * hands them back in list order to the calling thread, within one byte budget
  * per JVM; a single range request to object storage is bounded by one round
  * trip and one connection's rate. `ObjectStore.readAt` is safe to call from
  * several threads at once.
  *
  * Main types: ObjectStore, ObjectStoreFactory, NativeObjectStore,
  * WritableObjectStore, NativeWritableObjectStore, FileInfo,
  * ConcurrentRangeReads.
  */
package object io
