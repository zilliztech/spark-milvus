package com.zilliz.milvus.storage.io

import java.io.IOException
import java.nio.file.{Files, NoSuchFileException, Path, Paths}
import scala.collection.mutable

/** An [[ObjectStore]] over the local filesystem, for tests only.
  *
  * Production has exactly one store, [[NativeObjectStore]], which reaches every
  * backend through the C filesystem. The format parsers under test — parquet
  * footers, avro manifests, backup metadata — care only about the bytes they
  * are handed, so gating their suites on a 481 MB native library would buy
  * nothing and would stop them running anywhere that library is absent.
  *
  * Keys are paths. An absolute key is used as it stands; a relative one
  * resolves against `root`, which stands in for a bucket.
  */
final class LocalObjectStore(root: String = "") extends ObjectStore {

  private var closed = false

  override def readAll(key: String): Array[Byte] =
    try Files.readAllBytes(resolve(key))
    catch {
      case e: NoSuchFileException =>
        throw new IOException(s"failed to read bytes from $key", e)
    }

  override def size(key: String): Long = Files.size(resolve(key))

  // The ObjectStore contract: false only for a confirmed absence.
  override def exists(key: String): Boolean =
    try {
      Files.readAttributes(
        resolve(key),
        classOf[java.nio.file.attribute.BasicFileAttributes]
      )
      true
    } catch {
      case _: NoSuchFileException => false
    }

  override def list(key: String, recursive: Boolean): Seq[FileInfo] = {
    val start = resolve(key)
    if (!Files.exists(start)) return Seq.empty
    val out = mutable.ListBuffer.empty[FileInfo]
    val stream =
      if (recursive) Files.walk(start) else Files.list(start)
    try {
      stream.forEach { p =>
        if (p != start || !recursive) {
          out += FileInfo(
            p.toString,
            Files.isDirectory(p),
            if (Files.isDirectory(p)) 0L else Files.size(p),
            Files.getLastModifiedTime(p).toMillis * 1000000L
          )
        }
      }
    } finally stream.close()
    out.toList
  }

  override def readAt(
      key: String,
      offset: Long,
      length: Long,
      fileSize: Long
  ): Array[Byte] = {
    val channel = Files.newByteChannel(resolve(key))
    try {
      channel.position(offset)
      val buffer = java.nio.ByteBuffer.allocate(length.toInt)
      while (buffer.hasRemaining && channel.read(buffer) >= 0) {}
      buffer.array()
    } finally channel.close()
  }

  override def write(key: String, data: Array[Byte]): Unit = {
    val target = resolve(key)
    Option(target.getParent).foreach(Files.createDirectories(_))
    Files.write(target, data)
  }

  override def createDir(key: String, recursive: Boolean): Unit =
    Files.createDirectories(resolve(key))

  override def delete(key: String): Unit =
    Files.deleteIfExists(resolve(key))

  override def close(): Unit = closed = true

  /** Exposed so a test can assert the store was released. */
  def isClosed: Boolean = closed

  private def resolve(key: String): Path = {
    val stripped = stripScheme(key)
    val path = Paths.get(stripped)
    if (path.isAbsolute || root.isEmpty) path else Paths.get(root, stripped)
  }

  /** `file:///tmp/x` and `/tmp/x` name the same file. Tests hand over whichever
    * spelling the code under test produced.
    */
  private def stripScheme(key: String): String =
    if (key.startsWith("file://")) key.substring("file://".length) else key
}

/** A store whose every read fails, for the suites that assert an error reaches
  * the caller instead of turning into an empty result.
  */
final class FailingObjectStore(failure: => Throwable) extends ObjectStore {

  override def readAll(key: String): Array[Byte] = throw failure

  override def size(key: String): Long = 1024L

  override def exists(key: String): Boolean = true

  override def list(key: String, recursive: Boolean): Seq[FileInfo] =
    throw failure

  override def readAt(
      key: String,
      offset: Long,
      length: Long,
      fileSize: Long
  ): Array[Byte] = throw failure

  override def write(key: String, data: Array[Byte]): Unit = throw failure

  override def createDir(key: String, recursive: Boolean): Unit = throw failure

  override def delete(key: String): Unit = throw failure

  override def close(): Unit = ()
}
