package com.zilliz.milvus.storage.io.hadoop

import java.io.ByteArrayOutputStream
import java.net.URI
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/** Reads over the Hadoop FileSystem API.
  *
  * This is the migration-time shape: the signature still takes a live
  * Configuration. Once the ObjectStore interface lands, this becomes its
  * implementation and callers stop seeing Hadoop types. See constraint 13 in
  * section 4 of docs/design/modules.md.
  */
object HadoopIO {

  /** Reads a whole file into a byte array. Snapshots, manifests and delete
    * files are all small; nothing here needs streaming.
    */
  def readAllBytes(
      conf: Configuration,
      fullyQualifiedPath: String
  ): Array[Byte] = {
    var uri: URI = null
    var fs: FileSystem = null
    try {
      uri = new URI(fullyQualifiedPath)
      fs = FileSystem.get(uri, conf)
      val in = fs.open(new Path(uri))
      try {
        val out = new ByteArrayOutputStream()
        val buf = new Array[Byte](8192)
        var n = in.read(buf)
        while (n >= 0) {
          out.write(buf, 0, n)
          n = in.read(buf)
        }
        out.toByteArray
      } finally {
        in.close()
      }
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(
          s"failed to read bytes from $fullyQualifiedPath: ${e.getMessage}",
          e
        )
    } finally {
      // With the FileSystem cache disabled every get() builds a new instance,
      // so not closing it leaks the connection pool.
      Option(uri).flatMap(uri => Option(uri.getScheme)).foreach { scheme =>
        if (
          fs != null && conf.getBoolean(s"fs.$scheme.impl.disable.cache", false)
        ) {
          fs.close()
        }
      }
    }
  }
}
