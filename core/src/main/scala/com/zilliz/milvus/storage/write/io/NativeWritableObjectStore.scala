package com.zilliz.milvus.storage.write.io

import com.zilliz.milvus.storage.io.{FileInfo, NativeObjectStore}

/** Adds business writes to an existing native session. This adapter owns the
  * supplied store; closing either view ends the shared session.
  */
final class NativeWritableObjectStore(store: NativeObjectStore)
    extends WritableObjectStore {
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
      // Local files need parents; object storage treats this as a no-op.
      val parent = target.lastIndexOf('/')
      if (parent > 0) filesystem.createDir(target.substring(0, parent), true)
      filesystem.writeFile(target, data)
    }

  override def createDir(key: String, recursive: Boolean): Unit =
    store.withFileSystem(key)((filesystem, target) =>
      filesystem.createDir(target, recursive)
    )

  override def delete(key: String): Unit =
    store.withFileSystem(key)((filesystem, target) =>
      filesystem.deleteFile(target)
    )

  override def close(): Unit = store.close()
}
