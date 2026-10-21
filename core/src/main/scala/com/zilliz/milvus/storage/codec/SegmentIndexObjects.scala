package com.zilliz.milvus.storage.codec

import java.nio.ByteBuffer

import com.fasterxml.jackson.databind.ObjectMapper

import com.zilliz.milvus.storage.codec.IndexFileFormat.{
  validName,
  ChunkBytes,
  MaxObjectBytes,
  Slice,
  SliceMeta
}
import com.zilliz.milvus.storage.write.codec.{BinlogEncoder, IndexFileEncoder}
import com.zilliz.milvus.storage.write.io.WritableObjectStore
import com.zilliz.milvus.storage.Logging

/** Where a built index lands, and what a segment record will say about it.
  *
  * The ids and the two versions are what Milvus reads an index by: the store
  * path version decides the prefix, the index version is part of it, and the
  * build id names the build. A connector-built index carries the ids of the
  * segment it was built from; Milvus assigns its own build id when it restores
  * the snapshot
  * (docs/design/projects/spark-milvus/architecture/vector-search.html section
  * 2.7).
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
object SegmentIndexObjects extends Logging {

  // Milvus slices an index payload at common.indexSliceSize, 16 MiB by default.
  private val DefaultSliceBytes = 16L * 1024 * 1024
  private val mapper = new ObjectMapper()

  /** The prefix Milvus reads an index from, which its store path version
    * decides (internal/core/src/storage/FileManager.h
    * `GetRemoteIndexObjectPrefix`).
    */
  def prefixOf(target: IndexObjectTarget): String = {
    val root = target.rootPath.stripSuffix("/")
    val parts =
      if (target.storePathVersion >= 1)
        Seq(
          "index_v1",
          target.collectionId,
          target.partitionId,
          target.segmentId,
          target.buildId,
          target.indexVersion
        )
      else
        Seq(
          "index_files",
          target.buildId,
          target.indexVersion,
          target.partitionId,
          target.segmentId
        )
    (if (root.isEmpty) parts.mkString("/")
     else root + "/" + parts.mkString("/"))
  }

  /** Writes a built index as the objects Milvus reads back.
    *
    * Each payload becomes one object under the index prefix, in the same
    * envelope Milvus writes: a descriptor naming the segment and field, then
    * one `IndexFileEvent` carrying the bytes. A payload longer than
    * `sliceBytes` is split into `name_0`, `name_1`…; a payload that fits is
    * still written as `name_0`, because that is what Milvus writes and what its
    * loader parses. `SLICE_META` declares each payload and how to assemble it;
    * the loader in section 2.5 reads that metadata. `read` copies part of a
    * payload into the buffer it is given, the way `IndexBuilder.Built` does.
    * The result lists objects in write order.
    */
  def write(
      payloads: Seq[(String, Long)],
      read: (String, Long, ByteBuffer) => Unit,
      target: IndexObjectTarget,
      store: WritableObjectStore,
      sliceBytes: Long = DefaultSliceBytes
  ): Seq[WrittenIndexObject] = {
    require(payloads.nonEmpty, "A built index has no payloads")
    require(
      payloads.map(_._1).distinct.size == payloads.size,
      "A built index repeats a payload name"
    )
    require(sliceBytes > 0, s"The slice size must be positive: $sliceBytes")
    val prefix = prefixOf(target)
    // A local filesystem needs the directory before the first object; on object
    // storage this is a marker and costs nothing.
    store.createDir(prefix, recursive = true)
    // Milvus writes originSize and indexBuildID as strings and nullable as a
    // boolean (internal/core/src/storage/Event.cpp).
    def extrasOf(originSize: Long) = {
      val node = mapper.createObjectNode()
      node.put("originSize", originSize.toString)
      node.put("indexBuildID", target.buildId.toString)
      node.put("nullable", target.nullable)
      node
    }
    val written = Seq.newBuilder[WrittenIndexObject]
    val slices = Vector.newBuilder[Slice]
    payloads.foreach { case (name, length) =>
      require(
        validName(name),
        s"A payload name has to be a plain object name: $name"
      )
      require(length > 0, s"Payload $name is empty")
      def objectOf(objectName: String, offset: Long, size: Long): Unit = {
        require(
          size <= MaxObjectBytes,
          s"Index object $objectName of $size bytes exceeds the supported size"
        )
        val bytes = new Array[Byte](size.toInt)
        // The upstream payloads copy into direct memory only, so the bytes come
        // back one chunk at a time.
        var copied = 0
        while (copied < bytes.length) {
          val count = math.min(ChunkBytes, bytes.length - copied)
          val chunk = ByteBuffer.allocateDirect(count)
          read(name, offset + copied, chunk)
          // Whether the copy left the position at the end or at the start is
          // the caller's business; the bytes are read from the start either way.
          val view = chunk.duplicate()
          view.clear()
          view.get(bytes, copied, count)
          copied += count
        }
        val envelope = BinlogEncoder.envelope(
          7,
          target.collectionId,
          target.partitionId,
          target.segmentId,
          target.fieldId,
          0,
          extrasOf(size),
          bytes
        )
        val key = s"$prefix/$objectName"
        store.write(key, envelope)
        written += WrittenIndexObject(key, envelope.length.toLong)
      }
      // Always the sliced names, even for a payload that fits in one object:
      // Milvus writes `name_0` whatever the size, and its segment loader takes
      // the number after the last underscore as the slice index, so a file
      // called `HNSW` fails there with `invalided index file path`.
      val count = math.max(1, ((length + sliceBytes - 1L) / sliceBytes).toInt)
      (0 until count).foreach { number =>
        val offset = number.toLong * sliceBytes
        objectOf(
          s"${name}_$number",
          offset,
          math.min(sliceBytes, length - offset)
        )
      }
      slices += Slice(name, count, length)
    }
    val sliced = slices.result()
    if (sliced.nonEmpty) {
      val bytes = IndexFileEncoder.sliceMeta(sliced)
      val envelope = BinlogEncoder.envelope(
        7,
        target.collectionId,
        target.partitionId,
        target.segmentId,
        target.fieldId,
        0,
        extrasOf(bytes.length.toLong),
        bytes
      )
      val key = s"$prefix/$SliceMeta"
      store.write(key, envelope)
      written += WrittenIndexObject(key, envelope.length.toLong)
    }
    val objects = written.result()
    logInfo(
      s"Index written: segment=${target.segmentId}, build=${target.buildId}, " +
        s"objects=${objects.size}, bytes=${objects.map(_.bytes).sum}, prefix=$prefix"
    )
    objects
  }
}
