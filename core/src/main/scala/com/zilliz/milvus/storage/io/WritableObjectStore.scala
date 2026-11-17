package com.zilliz.milvus.storage.io

/** A store used by business writers and staging cleanup. */
trait WritableObjectStore extends ObjectStore {
  def write(key: String, data: Array[Byte]): Unit

  /** Creates an absent object and returns only after the write is finalized.
    * Existing objects, unsupported conditional writes and other failures retain
    * their native exceptions. There is no existence-check fallback. The local
    * backend excludes competing creators only within one process.
    *
    * The binding cannot abort a writer: closing it after a failed write may
    * still finalize an empty or partial object. A caller that needs all or
    * nothing configures the store so that `data` is uploaded by one request.
    */
  def create(key: String, data: Array[Byte]): Unit

  def createDir(key: String, recursive: Boolean = true): Unit

  /** Deletes one file. Directory entries are not accepted by the native
    * binding.
    */
  def delete(key: String): Unit
}
