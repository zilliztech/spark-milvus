package com.zilliz.milvus.storage.codec

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.arrow.memory.RootAllocator

import com.zilliz.milvus.jni.vector.{NativeVectorIndex, NativeVectorLibrary}
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.schema.VectorLayout
import com.zilliz.milvus.storage.snapshot.SegmentIndex
import com.zilliz.milvus.storage.Logging

import io.knowhere.DType

/** Loads the exact snapshot object set and restores named Knowhere payloads. */
private[storage] object IndexFileCodec extends Logging {
  private val SliceMeta = "SLICE_META"
  private val CardinalFile = "_mem.index.bin"
  private val ValidData = "valid_data"
  private val ValidDataNames = Set(ValidData, "valid_data_count")
  private val MaxValidDataBytes = 256L * 1024 * 1024
  private val MaxObjectBytes = 256L * 1024 * 1024
  // A payload has no size cap of its own: it streams into the native BinarySet
  // in 1 MiB chunks, and what is allocated for it is held against the bytes its
  // slice objects have on the store before anything is allocated. Only
  // SLICE_META and valid_data are materialized as Java arrays and have caps
  // (docs/design/architecture/search-resources.html section 3.4).
  private val ChunkBytes = 1024 * 1024
  // Milvus slices an index payload at common.indexSliceSize, 16 MiB by default.
  private val DefaultSliceBytes = 16L * 1024 * 1024
  private val mapper = new ObjectMapper()
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

  /** An index that has been read, decoded and handed to Knowhere, with what
    * that cost: the object bytes read and the wall time of the whole load. The
    * search reports both as `milvus.search.index.*`
    * (docs/design/architecture/vector-search.html section 1.3).
    */
  private[storage] final case class Loaded(
      index: NativeVectorIndex,
      bytes: Long,
      nanos: Long,
      validRows: Option[Array[Byte]] = None
  )

  private[codec] final case class Slice(name: String, count: Int, length: Long)

  /** Where a built index's objects go, and what the segment record will say
    * about them.
    */
  private[storage] final case class IndexTarget(
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

  private[storage] final case class IndexObject(key: String, bytes: Long)

  /** The prefix Milvus reads an index from, which its store path version
    * decides (internal/core/src/storage/FileManager.h
    * `GetRemoteIndexObjectPrefix`).
    */
  private[storage] def prefixOf(target: IndexTarget): String = {
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
    * loader parses. Every payload is declared in the `SLICE_META` object says
    * how to put it back, which is what the loader in section 2.5 reads.
    */
  private[storage] def write(
      payloads: Seq[(String, Long)],
      read: (String, Long, ByteBuffer) => Unit,
      target: IndexTarget,
      store: ObjectStore,
      sliceBytes: Long = DefaultSliceBytes
  ): Seq[IndexObject] = {
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
    val written = Seq.newBuilder[IndexObject]
    val slices = Vector.newBuilder[(String, Int, Long)]
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
        val envelope = BinlogCodec.envelope(
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
        written += IndexObject(key, envelope.length.toLong)
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
      slices += ((name, count, length))
    }
    val sliced = slices.result()
    if (sliced.nonEmpty) {
      val root = mapper.createObjectNode()
      val meta = root.putArray("meta")
      sliced.foreach { case (name, count, length) =>
        meta
          .addObject()
          .put("name", name)
          .put("slice_num", count)
          .put("total_len", length)
      }
      val bytes = mapper.writeValueAsBytes(root)
      val envelope = BinlogCodec.envelope(
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
      written += IndexObject(key, envelope.length.toLong)
    }
    val objects = written.result()
    logInfo(
      s"Index written: segment=${target.segmentId}, build=${target.buildId}, " +
        s"objects=${objects.size}, bytes=${objects.map(_.bytes).sum}, prefix=$prefix"
    )
    objects
  }

  private[codec] def parseSlices(bytes: Array[Byte]): Vector[Slice] = {
    require(
      bytes.nonEmpty && bytes.length <= ChunkBytes,
      "SLICE_META must contain at most one MiB of JSON"
    )
    val length = if (bytes.last == 0) bytes.length - 1 else bytes.length
    val root =
      mapper.readTree(new String(bytes, 0, length, StandardCharsets.UTF_8))
    require(
      root != null && root.isObject && root
        .has("meta") && root.get("meta").isArray,
      "SLICE_META must contain a meta array"
    )
    val slices = root
      .get("meta")
      .elements()
      .asScala
      .map { item =>
        require(
          item.isObject && item.has("name") && item.get("name").isTextual,
          "Index slice must name its logical payload"
        )
        val name = item.get("name").asText()
        require(
          validName(name) && name != SliceMeta,
          s"Invalid index slice name: $name"
        )
        require(
          item.has("slice_num") && item.get("slice_num").isIntegralNumber &&
            item
              .get("slice_num")
              .canConvertToInt && item.get("slice_num").asInt() > 0,
          s"Invalid slice count for $name"
        )
        require(
          item.has("total_len") && item.get("total_len").isIntegralNumber &&
            item
              .get("total_len")
              .canConvertToLong && item.get("total_len").asLong() > 0,
          s"Invalid slice payload length for $name"
        )
        Slice(
          name,
          item.get("slice_num").asInt(),
          item.get("total_len").asLong()
        )
      }
      .toVector
    require(
      slices.map(_.name).distinct.size == slices.size,
      "SLICE_META contains duplicate logical payload names"
    )
    slices
  }

  private def validName(name: String): Boolean =
    name.matches("[A-Za-z0-9_+.-]+") && name != "." && name != ".."

  private[codec] def validateIdentity(
      index: SegmentIndex,
      payload: DecodedIndexFile
  ): Unit = {
    require(
      payload.collectionId == index.collectionId &&
        payload.partitionId == index.partitionId && payload.segmentId == index.segmentId &&
        payload.fieldId == index.fieldId && payload.buildId == index.buildId,
      s"Index object identity differs from snapshot for segment ${index.segmentId}, build ${index.buildId}"
    )
    require(payload.payloadLength > 0, "Decoded index payload is empty")
  }

  /** Cardinal's native serializer ends in a 24-byte Footer, not a Milvus event.
    */
  private[codec] def validateCardinalFooter(bytes: Array[Byte]): Unit =
    validateCardinalFooter(bytes, bytes.length.toLong)

  private def validateCardinalFooter(
      bytes: Array[Byte],
      payloadLength: Long
  ): Unit = {
    require(
      bytes.length >= 24 && payloadLength >= 24,
      "Cardinal index file is shorter than its footer"
    )
    val footer = ByteBuffer
      .wrap(bytes, bytes.length - 24, 24)
      .order(ByteOrder.LITTLE_ENDIAN)
    require(
      footer.getInt() == 0x43415244,
      "Unsupported _mem.index.bin format: missing Cardinal CARD footer"
    )
    require(
      footer.getInt() == 1,
      "Unsupported Cardinal serialization footer version"
    )
    val globalMetadata = footer.getLong()
    val tenantMetadata = footer.getLong()
    require(
      tenantMetadata >= 0 && tenantMetadata < globalMetadata &&
        globalMetadata < payloadLength - 24,
      "Cardinal metadata offsets are outside the index file"
    )
  }

  /** Captures format markers while slices are copied, without rereading
    * objects.
    */
  private[codec] final class PayloadFormatProbe(length: Long) {
    require(
      length >= 24,
      "HNSW payload is too short to identify its persisted format"
    )
    private val head = new Array[Byte](4)
    private val tail = new Array[Byte](24)

    def capture(buffer: ByteBuffer, offset: Long): Unit = {
      def copy(target: Array[Byte], start: Long): Unit = {
        target.indices.foreach { i =>
          val source = start + i - offset
          if (source >= 0 && source < buffer.remaining()) {
            target(i) = buffer.get(buffer.position() + source.toInt)
          }
        }
      }
      copy(head, 0)
      copy(tail, length - 24)
    }

    /** The engine to deserialize with, and the check that the stream belongs to
      * the family the snapshot declared.
      *
      * A stream's first four bytes name the exact index class Faiss or Knowhere
      * wrote, and one index type has several of them: an HNSW_SQ over COSINE
      * with SQ4U uniform quantization writes `IHNa`, the same type over L2
      * writes `IHNs`. Pinning every marker would refuse a file the loaded
      * Knowhere can read, so the check is the family — `IH*` for the HNSW
      * types, `Iw*` and `IB*` for the IVF types — which still catches an IVF
      * stream under a declared HNSW index, and Knowhere refuses a stream it
      * cannot read for the type it was asked for.
      */
    def engine(
        indexType: String,
        version: Int,
        cardinalSupported: Boolean
    ): String = {
      if (
        ByteBuffer
          .wrap(tail)
          .order(ByteOrder.LITTLE_ENDIAN)
          .getInt() == 0x43415244
      ) {
        require(
          indexType == "HNSW",
          s"A Cardinal CARD stream carries an HNSW index, not $indexType"
        )
        validateCardinalFooter(tail, length)
        require(
          version >= 9,
          "A Cardinal CARD stream requires vector index format version 9 or later"
        )
        require(
          cardinalSupported,
          "This persisted index requires a verified Knowhere build with WITH_CARDINAL enabled"
        )
        logInfo(
          s"Persisted index format selected: format=Cardinal, engine=HNSW, version=$version"
        )
        "HNSW"
      } else {
        val magic = new String(head, StandardCharsets.US_ASCII)
        val family = magic.take(2)
        val expected = VectorIndexFamilies.magicsOf(indexType)
        require(
          expected.contains(family),
          s"Persisted $indexType payload starts with $magic; a $indexType index writes ${expected.toSeq.sorted
              .mkString(" or ")}* or a Cardinal CARD stream"
        )
        require(
          version >= 6,
          "A Faiss index stream requires vector index format version 6 or later"
        )
        val engine =
          if (indexType == "HNSW" && cardinalSupported) "HNSW_DEPRECATED"
          else indexType
        logInfo(
          s"Persisted index format selected: format=Faiss/$magic, engine=$engine, version=$version"
        )
        engine
      }
    }
  }

  private def readObject(
      store: ObjectStore,
      key: String,
      knownSize: Option[Long] = None
  ): Array[Byte] = {
    val started = System.nanoTime()
    val size = knownSize.getOrElse(store.size(key))
    require(
      size > 0 && size <= MaxObjectBytes,
      s"Index object exceeds the supported 256 MiB object size: $key"
    )
    // Keep allocation bounded even if an object changes after its size was read.
    val bytes = store.readAt(key, 0, size, size)
    require(
      bytes.length.toLong == size,
      s"Index object size changed while reading $key"
    )
    logInfo(
      s"Persisted index object read: key=$key, statBytes=$size, bytesRead=${bytes.length}, " +
        s"elapsedMillis=${(System.nanoTime() - started) / 1000000L}"
    )
    bytes
  }

  /** A raw Cardinal object is one unsliced payload. Only the footer and one
    * range at a time enter the JVM heap; the complete payload lives in
    * BinarySet.
    */
  private[codec] final class CardinalPayload(store: ObjectStore, key: String) {
    // The object's size on the store is what is allocated for it, so the
    // allocation can never exceed the bytes that exist; the only bound is
    // that a footer fits (docs/design/architecture/search-resources.html
    // section 3.4).
    val length: Long = store.size(key)
    require(
      length >= 24,
      s"Cardinal index payload must contain a footer: $key ($length bytes)"
    )
    private var transferred = 0L
    def bytesRead: Long = transferred

    private def read(offset: Long, count: Int): Array[Byte] = {
      val bytes = store.readAt(key, offset, count.toLong, length)
      require(
        bytes.length == count,
        s"Index object size changed while reading $key at offset $offset"
      )
      transferred = Math.addExact(transferred, count.toLong)
      bytes
    }
    validateCardinalFooter(read(length - 24, 24), length)

    /** The consumer must finish with each direct buffer before returning. */
    def copyTo(write: (Long, ByteBuffer) => Unit): Unit = {
      val started = System.nanoTime()
      val footer = new Array[Byte](24)
      val allocator = new RootAllocator(ChunkBytes.toLong)
      try {
        val chunk = allocator.buffer(ChunkBytes)
        try {
          var offset = 0L
          while (offset < length) {
            // Amortize remote requests while retaining bounded heap allocation.
            val count = math.min(8L * 1024 * 1024, length - offset).toInt
            val bytes = read(offset, count)
            val footerStart = math.max(0L, length - 24 - offset).toInt
            if (footerStart < count) {
              System.arraycopy(
                bytes,
                footerStart,
                footer,
                (offset + footerStart - (length - 24)).toInt,
                count - footerStart
              )
            }
            var copied = 0
            while (copied < count) {
              val size = math.min(ChunkBytes, count - copied)
              val buffer = chunk.nioBuffer(0, size)
              buffer.put(bytes, copied, size)
              buffer.flip()
              write(offset + copied, buffer)
              copied += size
            }
            offset += count
          }
          validateCardinalFooter(footer, length)
          logInfo(
            s"Persisted index object read: key=$key, statBytes=$length, bytesRead=$bytesRead, " +
              s"elapsedMillis=${(System.nanoTime() - started) / 1000000L}"
          )
        } finally chunk.close()
      } finally allocator.close()
    }
  }

  def load(
      index: SegmentIndex,
      layout: VectorLayout,
      store: ObjectStore,
      decoder: IndexFileDecoder
  ): Loaded = {
    val dimension = layout.dimension
    val dataType = layout.dtype
    val started = System.nanoTime()
    var objectsRead = 0
    var bytesRead = 0L
    // The slice objects' sizes, taken once for the check below and reused by
    // the reads, so a 67-slice index is not stat'ed twice.
    var objectSizes = Map.empty[String, Long]
    def read(key: String): Array[Byte] = {
      val bytes = readObject(store, key, objectSizes.get(key))
      objectsRead += 1
      bytesRead = Math.addExact(bytesRead, bytes.length.toLong)
      bytes
    }
    val indexType = index.indexType
      .map(_.toUpperCase(Locale.ROOT))
      .getOrElse(
        throw new IllegalArgumentException(
          "Index metadata is missing index_type"
        )
      )
    var validRows: Option[Array[Byte]] = None
    def finished(loaded: NativeVectorIndex): Loaded = {
      val elapsed = System.nanoTime() - started
      logInfo(
        s"Persisted index loaded: segment=${index.segmentId}, build=${index.buildId}, " +
          s"objectsRead=$objectsRead, bytesRead=$bytesRead, nativeDeserializeCalls=1, " +
          s"elapsedMillis=${elapsed / 1000000L}"
      )
      Loaded(loaded, bytesRead, elapsed, validRows)
    }
    val names = index.filePaths.map { key =>
      require(
        key.nonEmpty && !key.contains("://"),
        "Index paths must be exact normalized object keys"
      )
      val name = key.substring(key.lastIndexOf('/') + 1)
      require(validName(name), s"Invalid index object name: $name")
      name -> key
    }
    require(
      names.nonEmpty && names.map(_._1).distinct.size == names.size,
      "Index files must be nonempty and have unique payload names"
    )
    val files = names.toMap
    if (files.contains(CardinalFile)) {
      require(
        indexType == "HNSW",
        s"A Cardinal memory index carries an HNSW index, not $indexType"
      )
      require(
        files.size == 1,
        "A Cardinal memory index must name exactly one _mem.index.bin file"
      )
      require(
        index.currentIndexVersion.exists(_ >= 9),
        "A Cardinal CARD stream requires vector index format version 9 or later"
      )
      val payload = new CardinalPayload(store, files(CardinalFile))
      require(
        NativeVectorLibrary.load().cardinalSupported(),
        "This persisted index requires a verified Knowhere build with WITH_CARDINAL enabled"
      )
      logInfo(
        s"Persisted index format selected: format=Cardinal, engine=HNSW, " +
          s"version=${index.currentIndexVersion.get}"
      )
      val loaded = loadCardinal(index, dimension, dataType, payload)
      objectsRead += 1
      bytesRead = Math.addExact(bytesRead, payload.bytesRead)
      return finished(loaded)
    }

    def decoded(name: String): DecodedIndexFile = {
      val key = files.getOrElse(
        name,
        throw new IllegalArgumentException(
          s"Snapshot is missing index slice $name"
        )
      )
      val bytes = read(key)
      val result = decoder.decode(bytes)
      try {
        validateIdentity(index, result)
        result
      } catch {
        case failure: Throwable =>
          result.close()
          throw failure
      }
    }

    val slices = if (files.contains(SliceMeta)) {
      val metadata = decoded(SliceMeta)
      try {
        require(
          metadata.payloadLength <= ChunkBytes,
          "SLICE_META exceeds one MiB"
        )
        val bytes = new Array[Byte](metadata.payloadLength.toInt)
        metadata.readPayload(0, ByteBuffer.wrap(bytes))
        parseSlices(bytes)
      } finally metadata.close()
    } else Vector.empty
    require(
      slices.iterator.map(_.count.toLong).sum <= files.size.toLong,
      "SLICE_META declares more slices than the snapshot provides"
    )
    val slicedNames = slices.flatMap { slice =>
      require(
        slice.count <= files.size,
        s"Snapshot is missing slices for ${slice.name}"
      )
      (0 until slice.count).map(i => s"${slice.name}_$i")
    }
    require(
      slicedNames.distinct.size == slicedNames.size && slicedNames.forall(
        files.contains
      ),
      "Index slice files are missing or overlap"
    )
    // What SLICE_META declares is what the native BinarySet is allocated at,
    // so before anything is allocated it is held against the bytes the store
    // actually has: a payload cannot be longer than its slice objects together,
    // and after the copy it has to be exactly their payloads' sum. Forged
    // metadata cannot make the allocation larger than the files are.
    objectSizes =
      slicedNames.map(name => files(name) -> store.size(files(name))).toMap
    slices.foreach { slice =>
      val onStore =
        (0 until slice.count)
          .map(i => objectSizes(files(s"${slice.name}_$i")))
          .sum
      require(
        slice.length <= onStore,
        s"SLICE_META declares ${slice.length} bytes for ${slice.name} but its ${slice.count} slice objects hold $onStore bytes on the store"
      )
    }
    val remaining = names
      .map(_._1)
      .filterNot(name => name == SliceMeta || slicedNames.contains(name))
    val logicalNames = slices.map(_.name) ++ remaining
    require(
      logicalNames.distinct.size == logicalNames.size,
      "Duplicate assembled index payload"
    )
    require(
      logicalNames.contains(indexType),
      s"Unsupported persisted index payload layout: ${logicalNames
          .mkString(", ")}; expected $indexType. " +
        "The selected Knowhere build cannot load a different engine's format"
    )
    // A nullable vector field is indexed over its non-null rows only, and the
    // segment's own row numbers come back through this bitmap (section 2.4).
    if (logicalNames.contains(ValidData)) {
      val payload = decoded(ValidData)
      try {
        require(
          payload.payloadLength > 0 && payload.payloadLength <= MaxValidDataBytes,
          s"The valid_data bitmap of ${payload.payloadLength} bytes is outside the supported size"
        )
        val bytes = new Array[Byte](payload.payloadLength.toInt)
        payload.readPayload(0, ByteBuffer.wrap(bytes))
        validRows = Some(bytes)
      } finally payload.close()
    }

    val metric = index.metricType.get.toUpperCase(Locale.ROOT)
    val loader = new NativeVectorIndex.Loader(
      index.currentIndexVersion.get,
      dimension,
      index.rowCount,
      s"""{"metric_type":"$metric","dim":$dimension}"""
    )
    try
      finished(
        loadPayloads(
          index.segmentId,
          loader,
          indexType,
          dataType,
          index.currentIndexVersion.get,
          slices.filterNot(slice => ValidDataNames(slice.name)),
          remaining.filterNot(ValidDataNames),
          decoded
        )
      )
    finally loader.close()
  }

  private def loadCardinal(
      index: SegmentIndex,
      dimension: Int,
      dataType: DType,
      payload: CardinalPayload
  ): NativeVectorIndex = {
    val metric = index.metricType.get.toUpperCase(Locale.ROOT)
    val loader = new NativeVectorIndex.Loader(
      index.currentIndexVersion.get,
      dimension,
      index.rowCount,
      s"""{"metric_type":"$metric","dim":$dimension}"""
    )
    try {
      // Cardinal Serialize uses the same stream for FileManager output and
      // BinarySet[Type()]. This restores that documented in-memory interface.
      logInfo(
        s"Persisted index allocating: segment=${index.segmentId}, payload=HNSW, " +
          s"bytes=${payload.length}, totalBytes=${payload.length}"
      )
      loader.allocate("HNSW", payload.length)
      payload.copyTo((offset, buffer) => loader.write("HNSW", offset, buffer))
      loader.load("HNSW", dataType)
    } finally loader.close()
  }

  private def loadPayloads(
      segmentId: Long,
      loader: NativeVectorIndex.Loader,
      indexType: String,
      dataType: DType,
      version: Int,
      slices: Vector[Slice],
      remaining: Vector[String],
      decoded: String => DecodedIndexFile
  ): NativeVectorIndex = {
    val allocator = new RootAllocator(ChunkBytes.toLong)
    try {
      val chunk = allocator.buffer(ChunkBytes)
      try {
        var total = 0L
        var format: PayloadFormatProbe = null
        // The native allocation is logged before it is made: a failed native
        // allocation does not come back as a Java exception, so this line is
        // what says how much was asked for.
        def reserve(name: String, size: Long): Unit = {
          total = Math.addExact(total, size)
          logInfo(
            s"Persisted index allocating: segment=$segmentId, payload=$name, " +
              s"bytes=$size, totalBytes=$total"
          )
          if (name == indexType) format = new PayloadFormatProbe(size)
          loader.allocate(name, size)
        }
        def transfer(
            name: String,
            source: DecodedIndexFile,
            destinationOffset: Long
        ): Unit = {
          var offset = 0L
          while (offset < source.payloadLength) {
            val size =
              math.min(ChunkBytes.toLong, source.payloadLength - offset).toInt
            val buffer = chunk.nioBuffer(0, size)
            source.readPayload(offset, buffer)
            require(
              buffer.position() == size,
              "Index decoder did not copy the requested payload bytes"
            )
            buffer.flip()
            if (name == indexType)
              format.capture(buffer, destinationOffset + offset)
            loader.write(name, destinationOffset + offset, buffer)
            offset += size
          }
        }
        slices.foreach { slice =>
          reserve(slice.name, slice.length)
          var offset = 0L
          (0 until slice.count).foreach { number =>
            val payload = decoded(s"${slice.name}_$number")
            try {
              require(
                payload.payloadLength <= slice.length - offset,
                s"Index slices exceed declared length for ${slice.name}"
              )
              transfer(slice.name, payload, offset)
              offset += payload.payloadLength
            } finally payload.close()
          }
          require(
            offset == slice.length,
            s"Index slice total length differs for ${slice.name}"
          )
        }
        remaining.foreach { name =>
          val payload = decoded(name)
          try {
            reserve(name, payload.payloadLength)
            transfer(name, payload, 0)
          } finally payload.close()
        }
        loader.load(
          format.engine(
            indexType,
            version,
            NativeVectorLibrary.load().cardinalSupported()
          ),
          dataType
        )
      } finally chunk.close()
    } finally allocator.close()
  }
}
