package com.zilliz.milvus.storage.io

import com.zilliz.milvus.storage.path.StoragePath

/** One entry of a directory listing. */
final case class FileInfo(
    path: String,
    isDirectory: Boolean,
    size: Long,
    modifiedNanos: Long
)

/** Opens files in object storage.
  *
  * A store is bound to one bucket, so members take the key, not a URI: the C
  * layer wraps its backend in a subtree and appends whatever it is given, so a
  * full path arrives doubled. `StoragePath.key` is exactly what belongs here.
  *
  * This interface exposes reads only. Business writers use a separate adapter
  * over the same native filesystem session.
  */
trait ObjectStore extends AutoCloseable {

  def readAll(key: String): Array[Byte]

  def readAll(at: StoragePath): Array[Byte] = readAll(at.key)

  /** Bytes in the file. Throws when the key does not exist. */
  def size(key: String): Long

  def list(key: String, recursive: Boolean = false): Seq[FileInfo]

  /** True when the key exists, false only when the backend confirms it does
    * not. Any other failure to find out (permission, network, throttling)
    * throws: a failed lookup must not be treated as an absent object.
    */
  def exists(key: String): Boolean

  /** Reads one range. `fileSize` is passed in because the backend needs it to
    * open a reader, and a caller that already knows it should not pay for a
    * second round trip.
    *
    * Several threads may read through one store at once: each call opens its
    * own reader. [[ConcurrentRangeReads]] relies on this to keep several range
    * requests in flight against object storage.
    */
  def readAt(
      key: String,
      offset: Long,
      length: Long,
      fileSize: Long
  ): Array[Byte]

}

/** Opens stores on the executor.
  *
  * A store holds a handle into the native layer, which is a pointer inside one
  * process. This factory is what travels in an InputPartition instead.
  */
trait ObjectStoreFactory extends Serializable {

  /** `path` selects among `extfs.<name>.*` registrations by address and bucket;
    * an empty path uses the default `fs.*` configuration.
    */
  def open(path: String = ""): ObjectStore
}
