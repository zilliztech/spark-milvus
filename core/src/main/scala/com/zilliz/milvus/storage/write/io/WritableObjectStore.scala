package com.zilliz.milvus.storage.write.io

import com.zilliz.milvus.storage.io.ObjectStore

/** A store used by business writers and staging cleanup. */
trait WritableObjectStore extends ObjectStore {
  def write(key: String, data: Array[Byte]): Unit

  def createDir(key: String, recursive: Boolean = true): Unit

  /** Deletes one file. Directory entries are not accepted by the native
    * binding.
    */
  def delete(key: String): Unit
}
