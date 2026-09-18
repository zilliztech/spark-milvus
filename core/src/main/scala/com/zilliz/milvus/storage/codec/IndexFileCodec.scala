package com.zilliz.milvus.storage.codec

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.arrow.memory.RootAllocator

import com.zilliz.milvus.jni.vector.{NativeVectorIndex, NativeVectorLibrary}
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.snapshot.SegmentIndex
import com.zilliz.milvus.storage.Logging

/** Loads the exact snapshot object set and restores named Knowhere payloads. */
private[storage] object IndexFileCodec extends Logging {
  private val SliceMeta = "SLICE_META"
  private val CardinalFile = "_mem.index.bin"
  private val MaxObjectBytes = 256L * 1024 * 1024
  private val MaxPayloadBytes = 1024L * 1024 * 1024
  private val ChunkBytes = 1024 * 1024
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
      nanos: Long
  )

  private[codec] final case class Slice(name: String, count: Int, length: Long)

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
        val result = Slice(
          name,
          item.get("slice_num").asInt(),
          item.get("total_len").asLong()
        )
        require(
          result.length <= MaxPayloadBytes,
          s"Index payload $name exceeds the supported loading size"
        )
        result
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
    require(
      payload.payloadLength > 0 && payload.payloadLength <= MaxPayloadBytes,
      "Decoded index payload length is outside the supported loading size"
    )
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

    def engineType(version: Int, cardinalSupported: Boolean): String = {
      if (
        ByteBuffer
          .wrap(tail)
          .order(ByteOrder.LITTLE_ENDIAN)
          .getInt() == 0x43415244
      ) {
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
        require(
          Set("IHNf", "IHN9").contains(magic),
          "Unsupported HNSW payload format: expected Faiss float32 HNSW or Cardinal CARD"
        )
        require(
          version >= 6,
          "A Faiss HNSW stream requires vector index format version 6 or later"
        )
        val engine = if (cardinalSupported) "HNSW_DEPRECATED" else "HNSW"
        logInfo(
          s"Persisted index format selected: format=Faiss/$magic, engine=$engine, version=$version"
        )
        engine
      }
    }
  }

  private def readObject(store: ObjectStore, key: String): Array[Byte] = {
    val started = System.nanoTime()
    val size = store.size(key)
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

  def load(
      index: SegmentIndex,
      dimension: Int,
      store: ObjectStore,
      decoder: IndexFileDecoder
  ): Loaded = {
    val started = System.nanoTime()
    var objectsRead = 0
    var bytesRead = 0L
    def read(key: String): Array[Byte] = {
      val bytes = readObject(store, key)
      objectsRead += 1
      bytesRead = Math.addExact(bytesRead, bytes.length.toLong)
      bytes
    }
    def finished(loaded: NativeVectorIndex): Loaded = {
      val elapsed = System.nanoTime() - started
      logInfo(
        s"Persisted index loaded: segment=${index.segmentId}, build=${index.buildId}, " +
          s"objectsRead=$objectsRead, bytesRead=$bytesRead, nativeDeserializeCalls=1, " +
          s"elapsedMillis=${elapsed / 1000000L}"
      )
      Loaded(loaded, bytesRead, elapsed)
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
        files.size == 1,
        "A Cardinal memory index must name exactly one _mem.index.bin file"
      )
      require(
        index.currentIndexVersion.exists(_ >= 9),
        "A Cardinal CARD stream requires vector index format version 9 or later"
      )
      val bytes = read(files(CardinalFile))
      validateCardinalFooter(bytes)
      require(
        NativeVectorLibrary.load().cardinalSupported(),
        "This persisted index requires a verified Knowhere build with WITH_CARDINAL enabled"
      )
      logInfo(
        s"Persisted index format selected: format=Cardinal, engine=HNSW, " +
          s"version=${index.currentIndexVersion.get}"
      )
      return finished(loadCardinal(index, dimension, bytes))
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
    val remaining = names
      .map(_._1)
      .filterNot(name => name == SliceMeta || slicedNames.contains(name))
    val logicalNames = slices.map(_.name) ++ remaining
    require(
      logicalNames.distinct.size == logicalNames.size,
      "Duplicate assembled index payload"
    )
    require(
      !logicalNames.exists(name =>
        name == "valid_data" || name == "valid_data_count"
      ),
      "Nullable index row mappings are unsupported"
    )
    require(
      logicalNames.contains("HNSW"),
      s"Unsupported persisted HNSW payload layout: ${logicalNames.mkString(", ")}; expected HNSW. " +
        "The selected Knowhere build cannot load a different engine's format"
    )

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
          loader,
          index.currentIndexVersion.get,
          slices,
          remaining,
          decoded
        )
      )
    finally loader.close()
  }

  private def loadCardinal(
      index: SegmentIndex,
      dimension: Int,
      bytes: Array[Byte]
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
      loader.allocate("HNSW", bytes.length.toLong)
      val allocator = new RootAllocator(ChunkBytes.toLong)
      try {
        val chunk = allocator.buffer(ChunkBytes)
        try {
          var offset = 0
          while (offset < bytes.length) {
            val count = math.min(ChunkBytes, bytes.length - offset)
            val buffer = chunk.nioBuffer(0, count)
            buffer.put(bytes, offset, count)
            buffer.flip()
            loader.write("HNSW", offset.toLong, buffer)
            offset += count
          }
          loader.load("HNSW")
        } finally chunk.close()
      } finally allocator.close()
    } finally loader.close()
  }

  private def loadPayloads(
      loader: NativeVectorIndex.Loader,
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
        def reserve(name: String, size: Long): Unit = {
          total = Math.addExact(total, size)
          require(
            total <= MaxPayloadBytes,
            "Index payloads exceed the supported one GiB loading size"
          )
          if (name == "HNSW") format = new PayloadFormatProbe(size)
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
            if (name == "HNSW")
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
          format.engineType(
            version,
            NativeVectorLibrary.load().cardinalSupported()
          )
        )
      } finally chunk.close()
    } finally allocator.close()
  }
}
