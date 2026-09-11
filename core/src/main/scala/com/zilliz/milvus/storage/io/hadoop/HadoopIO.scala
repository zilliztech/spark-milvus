package com.zilliz.milvus.storage.io.hadoop

import java.io.ByteArrayOutputStream
import java.net.URI
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/** Hadoop FileSystem 上的读取。
  *
  * 迁移期的形态：签名还收活的 Configuration。ObjectStore 接口落地后这里换成 它的实现，调用方不用再见到 Hadoop 的类型。见
  * docs/design/modules.md 第 4 节 第 13 条。
  */
object HadoopIO {

  /** 整个文件读进字节数组。快照、Manifest、删除文件都是小文件，没有流式需求。 */
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
      // 关掉 FileSystem 缓存时每次 get 都新建一个，不关就泄漏连接池。
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
