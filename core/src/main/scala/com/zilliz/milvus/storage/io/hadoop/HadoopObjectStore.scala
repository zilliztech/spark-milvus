package com.zilliz.milvus.storage.io.hadoop

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import com.zilliz.milvus.storage.io.{FileInfo, ObjectStore, ObjectStoreFactory}

/** [[ObjectStore]] over the Hadoop FileSystem API.
  *
  * This is the migration-period implementation: it exists so the format
  * packages can stop taking a `Configuration` before `native-storage` reaches
  * every read path. It goes away with the last caller that needs Hadoop.
  *
  * Keys are rendered back to `scheme://bucket/key` here, which is the one place
  * that still needs a scheme.
  */
final class HadoopObjectStore(
    conf: Configuration,
    bucket: String,
    scheme: String
) extends ObjectStore {

  override def readAll(key: String): Array[Byte] =
    HadoopIO.readAllBytes(conf, uri(key))

  override def size(key: String): Long =
    withFs(key)((fs, p) => fs.getFileStatus(p).getLen)

  override def exists(key: String): Boolean =
    withFs(key)((fs, p) => fs.exists(p))

  override def list(key: String, recursive: Boolean): Seq[FileInfo] =
    withFs(key) { (fs, p) =>
      if (!fs.exists(p)) Seq.empty
      else if (recursive) {
        val it = fs.listFiles(p, true)
        val out = Seq.newBuilder[FileInfo]
        while (it.hasNext) {
          val s = it.next()
          out += FileInfo(
            s.getPath.toString,
            s.isDirectory,
            s.getLen,
            s.getModificationTime * 1000000L
          )
        }
        out.result()
      } else {
        fs.listStatus(p).toSeq.map { s =>
          FileInfo(
            s.getPath.toString,
            s.isDirectory,
            s.getLen,
            s.getModificationTime * 1000000L
          )
        }
      }
    }

  override def readAt(
      key: String,
      offset: Long,
      length: Long,
      fileSize: Long
  ): Array[Byte] = withFs(key) { (fs, p) =>
    val in = fs.open(p)
    try {
      val buffer = new Array[Byte](length.toInt)
      in.readFully(offset, buffer)
      buffer
    } finally in.close()
  }

  override def write(key: String, data: Array[Byte]): Unit = withFs(key) {
    (fs, p) =>
      val out = fs.create(p, true)
      try out.write(data)
      finally out.close()
  }

  override def createDir(key: String, recursive: Boolean): Unit =
    withFs(key)((fs, p) => fs.mkdirs(p))

  override def delete(key: String): Unit =
    withFs(key)((fs, p) => fs.delete(p, false))

  /** Closes the FileSystem only when its scheme has the cache disabled, which
    * means this store created it. A cached instance is shared process-wide and
    * closing it fails every other holder.
    */
  override def close(): Unit = synchronized {
    val current = filesystem
    if (
      current != null && conf.getBoolean(
        s"fs.$scheme.impl.disable.cache",
        false
      )
    ) {
      current.close()
    }
    filesystem = null
  }

  private var filesystem: FileSystem = _

  private def uri(key: String): String =
    if (bucket.isEmpty) key else s"$scheme://$bucket/$key"

  /** One FileSystem for the life of the store.
    *
    * With `fs.s3a.impl.disable.cache=true` every `FileSystem.get` builds a
    * whole S3A client and thread pool, so a store doing thousands of footer
    * reads must not resolve one per call.
    */
  private def withFs[A](key: String)(body: (FileSystem, Path) => A): A = {
    val target = new URI(uri(key))
    val fs = synchronized {
      if (filesystem == null) {
        filesystem = FileSystem.get(target, conf)
      }
      filesystem
    }
    body(fs, new Path(target))
  }
}

object HadoopObjectStore {

  /** Not serializable in the Spark sense — a `Configuration` is heavy and the
    * design routes executors through the native factory. This exists for the
    * driver-side reads that still run on Hadoop.
    */
  final case class Factory(
      conf: Configuration,
      bucket: String,
      scheme: String = "s3a"
  ) extends ObjectStoreFactory {
    override def open(path: String): ObjectStore =
      new HadoopObjectStore(conf, bucket, scheme)
  }
}
