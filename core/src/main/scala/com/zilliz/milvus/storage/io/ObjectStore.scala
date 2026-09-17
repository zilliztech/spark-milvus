package com.zilliz.milvus.storage.io

import com.zilliz.milvus.storage.path.Located

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
  * full path arrives doubled. `Located.key` is exactly what belongs here.
  *
  * Writing does not create parent directories. Object storage has no
  * directories, but the local backend does, so a caller that may run on either
  * calls [[createDir]] first.
  */
trait ObjectStore extends AutoCloseable {

  def readAll(key: String): Array[Byte]

  def readAll(at: Located): Array[Byte] = readAll(at.key)

  /** Bytes in the file. Throws when the key does not exist. */
  def size(key: String): Long

  def list(key: String, recursive: Boolean = false): Seq[FileInfo]

  /** True when the key exists, false only when the backend confirms it does
    * not. Any other failure to find out (permission, network, throttling)
    * throws: callers decide what to write from this answer.
    */
  def exists(key: String): Boolean

  /** Reads one range. `fileSize` is passed in because the backend needs it to
    * open a reader, and a caller that already knows it should not pay for a
    * second round trip.
    */
  def readAt(
      key: String,
      offset: Long,
      length: Long,
      fileSize: Long
  ): Array[Byte]

  def write(key: String, data: Array[Byte]): Unit

  def createDir(key: String, recursive: Boolean = true): Unit

  /** Deletes one file. Directories are not accepted by the current native
    * binding, including object-store directory marker objects reported as
    * directories by [[list]].
    */
  def delete(key: String): Unit
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
