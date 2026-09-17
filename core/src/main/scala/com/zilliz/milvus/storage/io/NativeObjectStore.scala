package com.zilliz.milvus.storage.io

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import io.milvus.storage.{MilvusStorageFileSystem, MilvusStorageProperties}

/** [[ObjectStore]] over milvus-storage's C filesystem.
  *
  * The handle is a pointer inside this process: never serialize it, and close
  * the store when the task ends.
  */
final class NativeObjectStore private (
    filesystem: MilvusStorageFileSystem,
    properties: MilvusStorageProperties
) extends ObjectStore {

  private var closed = false

  override def readAll(key: String): Array[Byte] =
    active.readFileAll(key)

  override def size(key: String): Long = active.fileSize(key)

  override def list(key: String, recursive: Boolean): Seq[FileInfo] =
    active
      .list(key, recursive)
      .toSeq
      .map(e => FileInfo(e.path, e.isDirectory, e.size, e.modifiedNanos))

  override def exists(key: String): Boolean = active.exists(key)

  override def readAt(
      key: String,
      offset: Long,
      length: Long,
      fileSize: Long
  ): Array[Byte] = {
    val reader = active.openReader(key, fileSize)
    try reader.readAt(offset, length)
    finally reader.close()
  }

  override def write(key: String, data: Array[Byte]): Unit =
    active.writeFile(key, data)

  override def createDir(key: String, recursive: Boolean): Unit =
    active.createDir(key, recursive)

  override def delete(key: String): Unit =
    active.deleteFile(key)

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      try filesystem.close()
      finally properties.free()
    }
  }

  private def active: MilvusStorageFileSystem = {
    if (closed) {
      throw new IllegalStateException("object store is closed")
    }
    filesystem
  }
}

object NativeObjectStore {

  /** Carries the `fs.*` map, not a handle, so it survives the trip to an
    * executor.
    */
  final case class Factory(properties: Map[String, String])
      extends ObjectStoreFactory {
    override def open(path: String): ObjectStore = {
      NativeStorageLibrary.load()
      val nativeProperties = new MilvusStorageProperties()
      var filesystem: MilvusStorageFileSystem = null
      try {
        nativeProperties.create(properties)
        filesystem = new MilvusStorageFileSystem(nativeProperties, path)
        new NativeObjectStore(filesystem, nativeProperties)
      } catch {
        case failure: Throwable =>
          try if (filesystem != null) filesystem.close()
          catch {
            case closeFailure: Throwable => failure.addSuppressed(closeFailure)
          }
          try nativeProperties.free()
          catch {
            case closeFailure: Throwable => failure.addSuppressed(closeFailure)
          }
          throw failure
      }
    }
  }
}
