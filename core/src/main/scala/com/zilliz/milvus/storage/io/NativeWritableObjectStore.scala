package com.zilliz.milvus.storage.io

/** Adds business writes to an existing native session. This adapter owns the
  * supplied store; closing either view ends the shared session.
  *
  * A written file gets its parent directories only on the local backend, which
  * the store itself reports, so remote writes never ask for bucket or directory
  * creation. An explicit createDir keeps the upstream filesystem semantics,
  * except in `objectStorage` mode, which creates no directory at all: object
  * storage has none, and the native recursive create first checks the bucket
  * (S3 HeadBucket), which an identity scoped to a key prefix is not allowed to
  * do. Every operation still requires the store to be open.
  */
final class NativeWritableObjectStore(
    store: NativeObjectStore,
    objectStorage: Boolean = false
) extends WritableObjectStore {
  override def readAll(key: String): Array[Byte] = store.readAll(key)
  override def size(key: String): Long = store.size(key)
  override def list(key: String, recursive: Boolean): Seq[FileInfo] =
    store.list(key, recursive)
  override def exists(key: String): Boolean = store.exists(key)
  override def readAt(
      key: String,
      offset: Long,
      length: Long,
      fileSize: Long
  ): Array[Byte] = store.readAt(key, offset, length, fileSize)

  override def write(key: String, data: Array[Byte]): Unit =
    store.withFileSystem(key) { (filesystem, target) =>
      // Local files need parents; object storage has none to create.
      val parent = target.lastIndexOf('/')
      if (store.local && !objectStorage && parent > 0)
        filesystem.createDir(target.substring(0, parent), true)
      filesystem.writeFile(target, data)
    }

  override def createDir(key: String, recursive: Boolean): Unit =
    store.withFileSystem(key)((filesystem, target) =>
      if (!objectStorage) filesystem.createDir(target, recursive)
    )

  override def delete(key: String): Unit =
    store.withFileSystem(key)((filesystem, target) =>
      filesystem.deleteFile(target)
    )

  override def close(): Unit = store.close()
}

object NativeWritableObjectStore {

  /** A store over `properties` in `objectStorage` mode unless the store is on
    * the local backend, so it creates directories only there.
    */
  def open(properties: Map[String, String]): NativeWritableObjectStore = {
    val store = NativeObjectStore.Factory(properties).open()
    new NativeWritableObjectStore(store, objectStorage = !store.local)
  }
}
