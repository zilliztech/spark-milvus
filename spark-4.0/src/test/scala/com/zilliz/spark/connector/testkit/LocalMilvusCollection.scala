package com.zilliz.spark.connector.testkit

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardCopyOption}
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  FixedSizeBinaryVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import com.zilliz.milvus.storage.codec.{IndexObjectTarget, SegmentIndexObjects}
import com.zilliz.milvus.storage.index.IndexWriter
import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.manifest.{
  IndexFileEntry,
  SnapshotSegmentFixture
}
import com.zilliz.milvus.storage.schema.{
  MetricType,
  SchemaMapper,
  VectorElementType,
  VectorLayout
}
import com.zilliz.milvus.storage.write.exec.{
  ManifestTransaction,
  V3SegmentWriter
}
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}
import io.milvus.storage.{MilvusStorageProperties, MilvusStorageTransaction}

/** A Milvus collection written on the local backend the way Milvus lays one
  * out, read back through `milvus.snapshot.path`: V3 segments through
  * milvus-storage's writer, a delete file per segment committed as its delta
  * log, an HNSW index per segment built and written the way `build_index`
  * writes one, and the snapshot document that names them. Every row is written
  * at timestamp 100 and every delete at 200, so a deleted id is gone.
  *
  * The collection is `id` (Int64 primary key, field 100), `vector`
  * (FloatVector, field 101) and a nullable Int64 `category` (field 102). Needs
  * the native libraries.
  */
object LocalMilvusCollection {

  final case class Entity(
      id: Long,
      vector: Array[Float],
      category: Option[Long]
  )

  final case class Segment(
      id: Long,
      entities: Seq[Entity],
      deletedIds: Seq[Long] = Seq.empty,
      indexMetric: Option[String] = Some("L2")
  )

  private val CollectionId = 10L
  private val PartitionId = 20L

  def schema(dimension: Int): CollectionSchema = CollectionSchema(
    name = "local-collection",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(
        fieldID = 101L,
        name = "vector",
        dataType = DataType.FloatVector,
        typeParams = Seq(KeyValuePair("dim", dimension.toString))
      ),
      FieldSchema(
        fieldID = 102L,
        name = "category",
        dataType = DataType.Int64,
        nullable = true
      )
    )
  )

  /** Writes the segments under `directory` and returns the options that read
    * the collection.
    */
  def write(
      directory: Path,
      dimension: Int,
      segments: Seq[Segment]
  ): Map[String, String] = {
    val properties =
      Map("fs.storage_type" -> "local", "fs.root_path" -> directory.toString)
    val collection = schema(dimension)
    val allocator = new RootAllocator()
    try {
      val written = segments.map(segment =>
        writeSegment(
          directory,
          allocator,
          properties,
          collection,
          dimension,
          segment
        )
      )
      Files.write(
        directory.resolve("snapshot.json"),
        snapshotJson(directory, collection, dimension, written).getBytes(UTF_8)
      )
    } finally allocator.close()
    properties ++ Map(
      MilvusOption.SnapshotPath -> "snapshot.json",
      MilvusOption.ReadColumnar -> "true"
    )
  }

  private final case class Written(
      segment: Segment,
      base: String,
      version: Long,
      index: Option[IndexFileEntry]
  )

  private def writeSegment(
      directory: Path,
      allocator: RootAllocator,
      properties: Map[String, String],
      collection: CollectionSchema,
      dimension: Int,
      segment: Segment
  ): Written = {
    val base = s"files/insert_log/$CollectionId/$PartitionId/${segment.id}"
    val arrow = SchemaMapper.convertToArrowSchemaWithFieldIdNames(collection)
    val writer =
      new V3SegmentWriter(base, arrow, properties, allocator, Seq("^101$"))
    val appended =
      try {
        val batch = VectorSchemaRoot.create(arrow, allocator)
        try {
          batch.allocateNew()
          segment.entities.zipWithIndex.foreach { case (entity, row) =>
            batch
              .getVector("0")
              .asInstanceOf[BigIntVector]
              .setSafe(row, row.toLong)
            batch.getVector("1").asInstanceOf[BigIntVector].setSafe(row, 100L)
            batch
              .getVector("100")
              .asInstanceOf[BigIntVector]
              .setSafe(row, entity.id)
            val category = batch.getVector("102").asInstanceOf[BigIntVector]
            entity.category match {
              case Some(value) => category.setSafe(row, value)
              case None        => category.setNull(row)
            }
            require(
              entity.vector.length == dimension,
              s"Entity ${entity.id} has ${entity.vector.length} values, not $dimension"
            )
            val bytes =
              ByteBuffer.allocate(dimension * 4).order(ByteOrder.LITTLE_ENDIAN)
            entity.vector.foreach(bytes.putFloat)
            batch
              .getVector("101")
              .asInstanceOf[FixedSizeBinaryVector]
              .setSafe(row, bytes.array())
          }
          batch.setRowCount(segment.entities.size)
          writer.write(batch)
        } finally batch.close()
        val groups = writer.finish()
        try
          ManifestTransaction.commit(
            base,
            properties,
            groups,
            ManifestTransaction.AppendFiles
          )
        finally groups.close()
      } finally writer.close()

    val version =
      if (segment.deletedIds.isEmpty) appended
      else {
        writeDeletes(directory, allocator, properties, base, segment)
        val nativeProperties = new MilvusStorageProperties()
        var transaction: MilvusStorageTransaction = null
        try {
          nativeProperties.create(properties)
          transaction = new MilvusStorageTransaction()
          transaction.begin(base, nativeProperties.getPtr, -1L, 0, 1)
          transaction.addDeltaLog(
            "delete.parquet",
            segment.deletedIds.size.toLong
          )
          transaction.commit()
        } finally {
          try if (transaction != null) transaction.destroy()
          finally nativeProperties.free()
        }
      }
    val index = segment.indexMetric.map(metric =>
      writeIndex(properties, dimension, segment, metric)
    )
    Written(segment, base, version, index)
  }

  private def writeDeletes(
      directory: Path,
      allocator: RootAllocator,
      properties: Map[String, String],
      base: String,
      segment: Segment
  ): Unit = {
    val arrow = new Schema(
      Seq("pk", "ts")
        .map(n =>
          new Field(n, FieldType.notNullable(new ArrowType.Int(64, true)), null)
        )
        .asJava
    )
    val writer = new V3SegmentWriter(
      s"delete-fixture-${segment.id}",
      arrow,
      properties,
      allocator
    )
    try {
      val root = VectorSchemaRoot.create(arrow, allocator)
      try {
        root.allocateNew()
        val pk = root.getVector("pk").asInstanceOf[BigIntVector]
        val ts = root.getVector("ts").asInstanceOf[BigIntVector]
        segment.deletedIds.zipWithIndex.foreach { case (id, row) =>
          pk.setSafe(row, id)
          ts.setSafe(row, 200L)
        }
        root.setRowCount(segment.deletedIds.size)
        writer.write(root)
      } finally root.close()
      val groups = writer.finish()
      try {
        val target = s"$base/_delta/delete.parquet"
        Files.createDirectories(directory.resolve(target).getParent)
        Files.copy(
          directory.resolve(groups.files(0).head),
          directory.resolve(target),
          StandardCopyOption.REPLACE_EXISTING
        )
      } finally groups.close()
    } finally writer.close()
  }

  /** The segment's index, built and written the way `build_index` does. */
  private def writeIndex(
      properties: Map[String, String],
      dimension: Int,
      segment: Segment,
      metric: String
  ): IndexFileEntry = {
    val layout = VectorLayout(VectorElementType.Float32, dimension)
    val rows = segment.entities.size
    val allocator = new RootAllocator()
    val buffer = allocator.buffer(rows.toLong * layout.rowBytes)
    val build = 1000L + segment.id
    try {
      segment.entities.zipWithIndex.foreach { case (entity, i) =>
        entity.vector.indices.foreach(d =>
          buffer.setFloat(i.toLong * layout.rowBytes + d * 4L, entity.vector(d))
        )
      }
      val built = IndexWriter.build(
        buffer
          .nioBuffer(0, rows * layout.rowBytes)
          .order(ByteOrder.nativeOrder()),
        rows.toLong,
        layout,
        "HNSW",
        MetricType
          .fromName(metric)
          .getOrElse(sys.error(s"No metric is named $metric")),
        indexVersion = 8,
        parameters = Map("M" -> "4", "efConstruction" -> "32")
      )
      try {
        val store = NativeObjectStore.Factory(properties).open()
        val objects =
          try
            SegmentIndexObjects.write(
              built.names.map(name => name -> built.length(name)),
              built.read,
              IndexObjectTarget(
                collectionId = CollectionId,
                partitionId = PartitionId,
                segmentId = segment.id,
                fieldId = 101L,
                buildId = build,
                indexVersion = 1L,
                storePathVersion = 0,
                nullable = false,
                rootPath = "files"
              ),
              store
            )
          finally store.close()
        IndexFileEntry(
          segment.id,
          101L,
          900L,
          build,
          "vector_hnsw",
          Map(
            "index_type" -> "HNSW",
            "metric_type" -> metric,
            "dim" -> dimension.toString
          ),
          objects.map(_.key).toVector,
          rows.toLong,
          objects.map(_.bytes).sum,
          1L,
          Some(8),
          Some(0)
        )
      } finally built.close()
    } finally {
      buffer.close()
      allocator.close()
    }
  }

  private def snapshotJson(
      directory: Path,
      collection: CollectionSchema,
      dimension: Int,
      segments: Seq[Written]
  ): String = {
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    val info = root.putObject("snapshot_info")
    info
      .put("name", "local-collection")
      .put("id", 1L)
      .put("collection_id", CollectionId)
      .put("create_ts", 1L)
    info.putArray("partition_ids").add(PartitionId)
    val schema = root.putObject("collection").putObject("schema")
    schema.put("name", collection.name)
    val fields = schema.putArray("fields")
    collection.fields.foreach { field =>
      val node = fields
        .addObject()
        .put("fieldID", field.fieldID)
        .put("name", field.name)
        .put("data_type", field.dataType.toString)
        .put("nullable", field.nullable)
        .put("is_primary_key", field.isPrimaryKey)
      if (field.fieldID == 101L)
        node
          .putArray("type_params")
          .addObject()
          .put("key", "dim")
          .put("value", dimension.toString)
    }
    root.put("format_version", 4)
    val definitions = root
      .putArray("indexes")
      .addObject()
      .put("collectionID", CollectionId)
      .put("fieldID", 101L)
      .put("indexID", 900L)
      .put("index_name", "vector_hnsw")
    definitions
      .putArray("index_params")
      .addObject()
      .put("key", "index_type")
      .put("value", "HNSW")
    val builds = root.putArray("build_ids")
    val manifests = root.putArray("manifest_list")
    val dataManifests = root.putArray("storagev2_manifest_list")
    segments.foreach { written =>
      val key =
        s"files/snapshots/$CollectionId/manifests/1/${written.segment.id}.avro"
      Files.createDirectories(directory.resolve(key).getParent)
      Files.write(
        directory.resolve(key),
        SnapshotSegmentFixture.encode(
          segmentId = written.segment.id,
          rows = written.segment.entities.size.toLong,
          indexes = written.index.toVector
        )
      )
      manifests.add(key)
      written.index.foreach(index => builds.add(index.buildId))
      val manifest = mapper
        .createObjectNode()
        .put("ver", written.version)
        .put("base_path", written.base)
      dataManifests
        .addObject()
        .put("segmentID", written.segment.id)
        .put("manifest", mapper.writeValueAsString(manifest))
    }
    mapper.writeValueAsString(root)
  }
}
