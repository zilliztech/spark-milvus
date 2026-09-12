package com.zilliz.milvus.storage.io

import scala.collection.JavaConverters._

import com.zilliz.milvus.jni.storage.{StorageNative, StorageNativeException}

/** [[ObjectStore]] over milvus-storage's C filesystem.
  *
  * The handle is a pointer inside this process: never serialize it, and close
  * the store when the task ends.
  */
final class NativeObjectStore private (handle: Long) extends ObjectStore {

  private var closed = false

  override def readAll(key: String): Array[Byte] =
    StorageNative.readFileAll(active, key)

  override def size(key: String): Long = StorageNative.fileSize(active, key)

  override def list(key: String, recursive: Boolean): Seq[FileInfo] =
    StorageNative
      .listDir(active, key, recursive)
      .toSeq
      .map(e => FileInfo(e.path, e.isDirectory, e.size, e.modifiedNanos))

  override def exists(key: String): Boolean =
    try {
      StorageNative.fileSize(active, key)
      true
    } catch {
      case _: StorageNativeException => false
    }

  override def readAt(
      key: String,
      offset: Long,
      length: Long,
      fileSize: Long
  ): Array[Byte] = {
    val reader = StorageNative.openReader(active, key, fileSize)
    try StorageNative.readerReadAt(reader, offset, length)
    finally StorageNative.readerDestroy(reader)
  }

  override def write(key: String, data: Array[Byte]): Unit =
    StorageNative.writeFile(active, key, data)

  override def createDir(key: String, recursive: Boolean): Unit =
    StorageNative.createDir(active, key, recursive)

  override def delete(key: String): Unit =
    StorageNative.deleteFile(active, key)

  override def close(): Unit = synchronized {
    if (!closed) {
      StorageNative.filesystemDestroy(handle)
      closed = true
    }
  }

  private def active: Long = {
    if (closed) {
      throw new IllegalStateException("object store is closed")
    }
    handle
  }
}

object NativeObjectStore {

  /** Carries the `fs.*` map, not a handle, so it survives the trip to an
    * executor.
    */
  final case class Factory(properties: Map[String, String])
      extends ObjectStoreFactory {
    override def open(path: String): ObjectStore =
      new NativeObjectStore(
        StorageNative.filesystemGet(properties.asJava, path)
      )
  }
}
