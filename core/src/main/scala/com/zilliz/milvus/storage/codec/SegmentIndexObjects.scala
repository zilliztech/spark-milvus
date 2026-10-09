package com.zilliz.milvus.storage.codec

import java.nio.ByteBuffer

import com.zilliz.milvus.storage.io.ObjectStore

/** Where a built index lands, and what a segment record will say about it.
  *
  * The ids and the two versions are what Milvus reads an index by: the store
  * path version decides the prefix, the index version is part of it, and the
  * build id names the build. A connector-built index carries the ids of the
  * segment it was built from; Milvus assigns its own build id when it restores
  * the snapshot (docs/design/architecture/vector-search.html section 2.7).
  */
final case class IndexObjectTarget(
    collectionId: Long,
    partitionId: Long,
    segmentId: Long,
    fieldId: Long,
    buildId: Long,
    indexVersion: Long,
    storePathVersion: Int,
    nullable: Boolean,
    rootPath: String = ""
)

/** One object a build wrote, as the segment record names it. */
final case class WrittenIndexObject(key: String, bytes: Long)

/** Writes a built index as the objects Milvus reads back.
  *
  * This is the Milvus `TableFormat` side of building: the computation hands
  * over named payloads, and this puts them under the prefix Milvus expects, in
  * the envelope it wrote them in, splitting a payload that is too long for one
  * object. Nothing above this decides a file name or a slice boundary.
  */
object SegmentIndexObjects {

  /** The prefix the objects go under. */
  def prefixOf(target: IndexObjectTarget): String =
    IndexFileCodec.prefixOf(codecTarget(target))

  /** Writes every payload and returns the objects in the order they were
    * written. `read` copies part of a payload into the buffer it is given, the
    * way `IndexWriter.Built` does.
    */
  def write(
      payloads: Seq[(String, Long)],
      read: (String, Long, ByteBuffer) => Unit,
      target: IndexObjectTarget,
      store: ObjectStore
  ): Seq[WrittenIndexObject] =
    IndexFileCodec
      .write(payloads, read, codecTarget(target), store)
      .map(written => WrittenIndexObject(written.key, written.bytes))

  private def codecTarget(target: IndexObjectTarget) =
    IndexFileCodec.IndexTarget(
      target.collectionId,
      target.partitionId,
      target.segmentId,
      target.fieldId,
      target.buildId,
      target.indexVersion,
      target.storePathVersion,
      target.nullable,
      target.rootPath
    )
}
