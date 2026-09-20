package com.zilliz.milvus.storage.io

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.storage.credential.StorageProperties
import io.milvus.storage.{MilvusStorageFileSystem, MilvusStorageProperties}

/** [[ObjectStore]] over milvus-storage's C filesystem.
  *
  * The handle is a pointer inside this process: never serialize it, and close
  * the store when the task ends.
  */
final class NativeObjectStore private (
    filesystem: MilvusStorageFileSystem,
    properties: MilvusStorageProperties,
    rootPath: String
) extends ObjectStore {

  private var closed = false

  /** A key is relative to the bucket, so it carries `fs.root_path`
    * ([[com.zilliz.milvus.storage.write.commit.CommittedSegment]] states that).
    * The C layer roots a remote backend at the bucket and a local one at
    * `fs.root_path`, one level deeper, so on the local backend that segment is
    * taken off on the way down and put back on the way up. Callers keep one
    * convention whichever backend answers.
    */
  private def down(key: String): String =
    if (rootPath.isEmpty) key
    else if (key == rootPath) ""
    else if (key.startsWith(rootPath + "/")) key.substring(rootPath.length + 1)
    else key

  private def up(key: String): String =
    if (rootPath.isEmpty || key.startsWith(rootPath)) key
    else if (key.isEmpty) rootPath
    else rootPath + "/" + key

  override def readAll(key: String): Array[Byte] =
    active.readFileAll(down(key))

  override def size(key: String): Long = active.fileSize(down(key))

  override def list(key: String, recursive: Boolean): Seq[FileInfo] =
    active
      .list(down(key), recursive)
      .toSeq
      .map(e => FileInfo(up(e.path), e.isDirectory, e.size, e.modifiedNanos))

  override def exists(key: String): Boolean = active.exists(down(key))

  override def readAt(
      key: String,
      offset: Long,
      length: Long,
      fileSize: Long
  ): Array[Byte] = {
    val reader = active.openReader(down(key), fileSize)
    try reader.readAt(offset, length)
    finally reader.close()
  }

  override def write(key: String, data: Array[Byte]): Unit = {
    val target = down(key)
    // Object storage has no directories and a filesystem will not open a file
    // whose parents are missing; which one is behind the store is not the
    // caller's business.
    val parent = target.lastIndexOf('/')
    if (parent > 0) active.createDir(target.substring(0, parent), true)
    active.writeFile(target, data)
  }

  override def createDir(key: String, recursive: Boolean): Unit =
    active.createDir(down(key), recursive)

  override def delete(key: String): Unit =
    active.deleteFile(down(key))

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

    /** What the local backend roots at, and what a key built from
      * `fs.root_path` therefore repeats; empty for every backend the C layer
      * roots at the bucket.
      */
    private def localRootPath: String =
      if (
        properties
          .get(StorageProperties.StorageType)
          .map(_.trim)
          .contains(StorageProperties.StorageTypeLocal)
      )
        properties
          .get(StorageProperties.RootPath)
          .map(_.trim.stripSuffix("/"))
          .filter(_.nonEmpty)
          .getOrElse("")
      else ""

    override def open(path: String): ObjectStore = {
      NativeStorageLibrary.load()
      val nativeProperties = new MilvusStorageProperties()
      var filesystem: MilvusStorageFileSystem = null
      try {
        nativeProperties.create(properties)
        filesystem = new MilvusStorageFileSystem(nativeProperties, path)
        new NativeObjectStore(filesystem, nativeProperties, localRootPath)
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
