package com.zilliz.milvus.storage.write.io

import java.nio.file.Files

import com.zilliz.milvus.storage.io.LocalObjectStore

/** Local test backend for the production write contracts that migrate later. */
final class LocalWritableObjectStore(root: String = "")
    extends LocalObjectStore(root)
    with WritableObjectStore {
  override def write(key: String, data: Array[Byte]): Unit = {
    val target = resolve(key)
    Option(target.getParent).foreach(Files.createDirectories(_))
    Files.write(target, data)
  }

  override def createDir(key: String, recursive: Boolean): Unit =
    Files.createDirectories(resolve(key))

  override def delete(key: String): Unit = Files.deleteIfExists(resolve(key))
}
