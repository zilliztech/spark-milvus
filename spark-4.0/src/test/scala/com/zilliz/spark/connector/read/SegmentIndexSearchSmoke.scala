package com.zilliz.spark.connector.read

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.Comparator
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  FixedSizeBinaryVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.milvus.storage.codec.BinlogFixture
import com.zilliz.milvus.storage.manifest.{
  AvroIndexFileEntry,
  SegmentManifestFixture
}
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.{
  DeltaLogFile,
  SegmentIndex,
  SegmentIndexes,
  SegmentLayout,
  V2ColumnGroup
}
import com.zilliz.milvus.storage.write.exec.{
  ManifestTransaction,
  V3SegmentWriter
}
import com.zilliz.spark.connector.metrics.ScanMetrics
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}
import io.milvus.storage.{MilvusStorageProperties, MilvusStorageTransaction}

import io.knowhere.{DType, Knowhere}

/** Explicit runtime smoke: local[2], real persisted HNSW payloads, native
  * parquet reads and row retrieval. No mock reader or index participates.
  */
object SegmentIndexSearchSmoke {
  private val collection = CollectionSchema(
    name = "persisted-index-smoke",
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
        typeParams = Seq(KeyValuePair("dim", "2"))
      ),
      FieldSchema(
        fieldID = 102L,
        name = "category",
        dataType = DataType.Int64,
        nullable = true
      )
    )
  )

  private case class Fixture(
      task: SegmentReadTask,
      version: Long,
      descriptor: AvroIndexFileEntry,
      vectorFiles: Seq[String]
  )

  def main(args: Array[String]): Unit = {
    val directory = Files.createTempDirectory("persisted-index-spark-smoke-")
    val allocator = new RootAllocator()
    val properties =
      Map("fs.storage_type" -> "local", "fs.root_path" -> directory.toString)
    var spark: SparkSession = null
    try {
      NativeVectorLibrary.load()
      val fixtures = Seq(30L, 31L).map(id =>
        writeSegment(directory, allocator, properties, id, v2 = false)
      )
      val snapshot = snapshotJson(fixtures)
      Files.write(directory.resolve("snapshot.json"), snapshot.getBytes(UTF_8))
      fixtures
        .flatMap(_.vectorFiles)
        .foreach(path => Files.delete(directory.resolve(path)))
      spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("persisted-index-smoke")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config(
          "spark.sql.warehouse.dir",
          directory.resolve("warehouse").toString
        )
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      val options = properties ++ Map(
        MilvusOption.SnapshotPath -> "snapshot.json",
        MilvusOption.ReadColumnar -> "true",
        "VECTOR.SEARCH.MODE" -> "brute_force",
        "VECTOR.SEARCH.QUERY" -> "[100,100]",
        "MILVUS.EXTRA.COLUMNS" -> "partition"
      )
      val frame = MilvusSearch.search(
        spark,
        options,
        "vector",
        Array(0f, 0f),
        3,
        "L2",
        "index",
        Map.empty,
        Some("category >= 1"),
        Seq("id"),
        false
      )
      val rows = frame.orderBy("rank").collect().toVector
      assert(
        rows.map(_.getAs[Long]("id")) == Vector(3101L, 3003L, 3103L),
        rows.mkString(",")
      )
      assert(rows.map(_.getAs[Double]("_score")) == Vector(2.25, 9.0, 12.25))
      assert(rows.map(_.getAs[Long]("_segment_id")) == Vector(31L, 30L, 31L))
      assert(rows.map(_.getAs[Long]("_row_offset")) == Vector(1L, 3L, 3L))
      assert(frame.select("id").count() == 3L)
      assert(frame.select().count() == 3L)
      println(
        "PASS: local[2] merges two persisted HNSW segments; scalar filter, nulls and timestamped deletes precede ANN; squared L2 and physical row offsets agree"
      )
      println(
        "PASS: scalar-only query succeeds after removing every raw vector column-group file"
      )
      verifyProjection(fixtures.head.task, properties)
      val v2 = writeSegment(directory, allocator, properties, 32L, v2 = true)
      v2.vectorFiles.foreach(path => Files.delete(directory.resolve(path)))
      verifyProjection(v2.task, properties)
      println(
        "PASS: V2 and V3 native take preserve projected score position, metadata positions and score pruning without opening raw vectors"
      )
      verifyMixedL2Tie(spark, directory, allocator, properties)
      assert(
        allocator.getAllocatedMemory == 0,
        "Fixture construction leaked Arrow buffers"
      )
    } finally {
      if (spark != null) spark.stop()
      try allocator.close()
      finally {
        val paths = Files.walk(directory)
        try
          paths
            .sorted(Comparator.reverseOrder[Path]())
            .forEach(Files.deleteIfExists(_))
        finally paths.close()
      }
    }
  }

  private def verifyMixedL2Tie(
      spark: SparkSession,
      directory: Path,
      allocator: RootAllocator,
      properties: Map[String, String]
  ): Unit = {
    val values = (0 until 8).map { row =>
      if (row == 3) Array(1f, 1f) else Array(row.toFloat + 10f, 0f)
    }
    val fixtures = Seq(40L, 41L).map { segment =>
      writeSegment(
        directory,
        allocator,
        properties,
        segment,
        v2 = false,
        vectorValues = Some(values)
      )
    }
    Files.write(
      directory.resolve("mixed-l2.json"),
      snapshotJson(fixtures, unindexedSegments = Set(40L)).getBytes(UTF_8)
    )
    fixtures.last.vectorFiles.foreach(path =>
      Files.delete(directory.resolve(path))
    )
    val frame = MilvusSearch.search(
      spark,
      properties ++ Map(MilvusOption.SnapshotPath -> "mixed-l2.json"),
      "vector",
      Array(0f, 0f),
      2,
      "L2",
      "index",
      Map.empty,
      Some("id in [4003,4103]"),
      Seq("id"),
      true
    )
    val rows = frame.orderBy("rank").collect().toVector
    assert(rows.map(_.getAs[Double]("_score")) == Vector(2d, 2d))
    assert(rows.map(_.getAs[Long]("id")) == Vector(4003L, 4103L))
    assert(rows.map(_.getAs[Long]("_segment_id")) == Vector(40L, 41L))
    assert(rows.map(_.getAs[Long]("_row_offset")) == Vector(3L, 3L))
    assert(
      frame.orderBy("rank").limit(1).collect().head.getAs[Long]("id") == 4003L
    )
    println(
      "PASS: mixed indexed/unindexed L2 scores preserve native 2.0 exactly and global ties select the smaller segment id"
    )
  }

  private def verifyProjection(
      task: SegmentReadTask,
      properties: Map[String, String]
  ): Unit = {
    val options = properties ++ Map(
      MilvusOption.VectorSearchMode -> "index",
      MilvusOption.VectorSearchQueryVector -> "[0,0]",
      MilvusOption.VectorSearchTopK -> "2",
      MilvusOption.VectorSearchMetric -> "L2",
      MilvusOption.VectorSearchFilter -> "category >= 1",
      MilvusOption.MilvusExtraColumns -> "$segment_id,$row_offset"
    )
    val parsed = MilvusOption(options)
    val partition: MilvusInputPartition = task.layout match {
      case _: SegmentLayout.Manifest =>
        MilvusV3InputPartition(task, "20", parsed)
      case _: SegmentLayout.ColumnGroups => MilvusV2InputPartition(task, parsed)
    }
    val id = StructField("id", LongType, nullable = false)
    val score = StructField("_score", DoubleType, nullable = false)
    val segment = StructField(
      MilvusOption.MilvusExtraColumnSegmentID,
      LongType,
      nullable = false
    )
    val offset = StructField(
      MilvusOption.MilvusExtraColumnRowOffset,
      LongType,
      nullable = false
    )
    Seq(
      StructType(Seq(score, offset, id, segment)),
      StructType(Seq(id, score)),
      StructType(Seq(id)),
      StructType(Seq(score)),
      StructType(Seq.empty)
    ).foreach { projection =>
      val reader = new MilvusPartitionReaderFactory(projection, options)
        .createReader(partition)
      try {
        var count = 0
        while (reader.next()) {
          val row = reader.get()
          assert(row.numFields == projection.length)
          projection.fields.zipWithIndex.foreach { case (field, position) =>
            field.name match {
              case "_score" =>
                assert(
                  row.getDouble(position) == math.pow(
                    3 + count + (task.segmentId - 30) * 0.5,
                    2
                  )
                )
              case "id" =>
                assert(
                  row.getLong(position) == task.segmentId * 100 + 3 + count
                )
              case MilvusOption.MilvusExtraColumnSegmentID =>
                assert(row.getLong(position) == task.segmentId)
              case MilvusOption.MilvusExtraColumnRowOffset =>
                assert(row.getLong(position) == 3 + count)
            }
          }
          count += 1
        }
        assert(count == 2)
        val metrics =
          reader.currentMetricsValues().map(m => m.name() -> m.value()).toMap
        assert(metrics(ScanMetrics.JniCalls) > 0L)
        assert(
          metrics(ScanMetrics.ArrowBatches) >= 2L,
          "Filter and take batches must both be reported"
        )
        assert(metrics(ScanMetrics.ArrowBytes) > 0L)
        assert(metrics(ScanMetrics.ArrowAllocatedMax) > 0L)
        assert(metrics(ScanMetrics.RowsMaterialized) == count.toLong)
        reader.close()
        assert(
          reader
            .currentMetricsValues()
            .map(m => m.name() -> m.value())
            .toMap == metrics
        )
      } finally reader.close()
    }

    // Filtering reads succeed before the missing index makes this query fail.
    // Its work must remain visible after both the failure and reader closure.
    val indexes = task.indexes.asInstanceOf[SegmentIndexes.Available].indexes
    val brokenTask = task.copy(indexes =
      SegmentIndexes.Available(
        indexes.map(_.copy(filePaths = Vector("missing-index.bin")))
      )
    )
    val brokenPartition = partition match {
      case value: MilvusV3InputPartition => value.copy(task = brokenTask)
      case value: MilvusV2InputPartition => value.copy(task = brokenTask)
    }
    val failed =
      new MilvusPartitionReaderFactory(StructType(Seq(id, score)), options)
        .createReader(brokenPartition)
    try {
      var rejected = false
      try failed.next()
      catch { case NonFatal(_) => rejected = true }
      assert(rejected, "The missing index must fail the query")
      val metrics =
        failed.currentMetricsValues().map(m => m.name() -> m.value()).toMap
      assert(metrics(ScanMetrics.JniCalls) > 0L)
      assert(metrics(ScanMetrics.ArrowBatches) > 0L)
      assert(metrics(ScanMetrics.ArrowBytes) > 0L)
      assert(metrics(ScanMetrics.RowsMaterialized) == 0L)
      failed.close()
      assert(
        failed
          .currentMetricsValues()
          .map(m => m.name() -> m.value())
          .toMap == metrics
      )
    } finally failed.close()
  }

  private def writeSegment(
      directory: Path,
      allocator: RootAllocator,
      properties: Map[String, String],
      segment: Long,
      v2: Boolean,
      vectorValues: Option[Seq[Array[Float]]] = None
  ): Fixture = {
    val base = s"files/insert_log/10/20/$segment"
    val arrow =
      if (v2) SchemaMapper.convertToArrowSchema(collection)
      else SchemaMapper.convertToArrowSchemaWithFieldIdNames(collection)
    def name(id: Long): String = if (!v2) id.toString
    else
      id match {
        case 0     => "RowID"
        case 1     => "Timestamp"
        case other => collection.fields.find(_.fieldID == other).get.name
      }
    val vectors = vectorValues.getOrElse(
      (0 until 8).map(row => Array(row.toFloat + (segment - 30) * 0.5f, 0f))
    )
    require(vectors.size == 8, "The fixture requires eight physical rows")
    val writer = new V3SegmentWriter(
      base,
      arrow,
      properties,
      allocator,
      Seq(s"^${name(101)}$$")
    )
    var vectorFiles = Seq.empty[String]
    var columnGroups = Seq.empty[V2ColumnGroup]
    try {
      val batch = VectorSchemaRoot.create(arrow, allocator)
      try {
        batch.allocateNew()
        vectors.indices.foreach { row =>
          batch
            .getVector(name(0))
            .asInstanceOf[BigIntVector]
            .setSafe(row, row.toLong)
          batch.getVector(name(1)).asInstanceOf[BigIntVector].setSafe(row, 100L)
          batch
            .getVector(name(100))
            .asInstanceOf[BigIntVector]
            .setSafe(row, segment * 100 + row)
          val categories = batch.getVector(name(102)).asInstanceOf[BigIntVector]
          if (segment != 31 && row == 1) categories.setNull(row)
          else categories.setSafe(row, if (row == 0) 0L else 1L)
          val bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
          vectors(row).foreach(bytes.putFloat)
          batch
            .getVector(name(101))
            .asInstanceOf[FixedSizeBinaryVector]
            .setSafe(row, bytes.array())
        }
        batch.setRowCount(vectors.size)
        writer.write(batch)
      } finally batch.close()
      val groups = writer.finish()
      try {
        val namesToIds =
          (Seq("RowID" -> 0L, "Timestamp" -> 1L) ++ collection.fields.map(f =>
            f.name -> f.fieldID
          )).toMap
        columnGroups = (0 until groups.size).map { i =>
          val ids =
            groups.columns(i).map(n => if (v2) namesToIds(n) else n.toLong)
          if (ids == Seq(101L)) vectorFiles = groups.files(i)
          V2ColumnGroup(ids, groups.files(i), groups.rowCounts(i))
        }
        ManifestTransaction.commit(
          base,
          properties,
          groups,
          ManifestTransaction.AppendFiles
        )
      } finally groups.close()
    } finally writer.close()
    require(
      vectorFiles.nonEmpty,
      "Raw vectors must occupy their own column group"
    )
    val deltaPath =
      writeDeletes(directory, allocator, properties, base, segment)
    val nativeProperties = new MilvusStorageProperties()
    var transaction: MilvusStorageTransaction = null
    val version =
      try {
        nativeProperties.create(properties)
        transaction = new MilvusStorageTransaction()
        transaction.begin(base, nativeProperties.getPtr, -1L, 0, 1)
        transaction.addDeltaLog("delete.parquet", 2L)
        transaction.commit()
      } finally {
        try if (transaction != null) transaction.destroy()
        finally nativeProperties.free()
      }
    val index = writeIndex(directory, vectors, segment)
    val descriptor = SegmentIndex(
      10L,
      20L,
      segment,
      101L,
      index.indexId,
      index.buildId,
      index.name,
      index.parameters,
      index.filePaths,
      8L,
      index.serializedSize,
      index.indexVersion,
      index.currentIndexVersion,
      index.indexStorePathVersion
    )
    val task = SegmentReadTask(
      segment,
      20L,
      if (v2) SegmentLayout.ColumnGroups(columnGroups)
      else SegmentLayout.Manifest(base, version),
      collection.toByteArray,
      properties,
      deletes = DeleteSource.Files(Seq(DeltaLogFile(1L, deltaPath, 2L))),
      indexes = SegmentIndexes.Available(Vector(descriptor)),
      snapshotRows = Some(8L)
    )
    Fixture(task, version, index, vectorFiles)
  }

  private def writeDeletes(
      directory: Path,
      allocator: RootAllocator,
      properties: Map[String, String],
      base: String,
      segment: Long
  ): String = {
    val arrow = new Schema(
      Seq("pk", "ts")
        .map(n =>
          new Field(n, FieldType.notNullable(new ArrowType.Int(64, true)), null)
        )
        .asJava
    )
    val writer = new V3SegmentWriter(
      s"delete-fixture-$segment",
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
        pk.setSafe(0, segment * 100 + 2); ts.setSafe(0, 200L)
        pk.setSafe(1, segment * 100 + 1); ts.setSafe(1, 50L)
        root.setRowCount(2)
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
        target
      } finally groups.close()
    } finally writer.close()
  }

  private def writeIndex(
      directory: Path,
      vectors: Seq[Array[Float]],
      segment: Long
  ): AvroIndexFileEntry = {
    val engine =
      if (NativeVectorLibrary.load().cardinalSupported()) "HNSW_DEPRECATED"
      else "HNSW"
    val index = Knowhere.createIndex(engine, DType.FLOAT32, 8)
    val allocator = new RootAllocator()
    val buffer = allocator.buffer(vectors.size.toLong * 8L)
    try {
      vectors.indices.foreach(i =>
        vectors(i).indices.foreach(d =>
          buffer.setFloat(i.toLong * 8 + d * 4L, vectors(i)(d))
        )
      )
      index.build(
        buffer.nioBuffer(0, vectors.size * 8).order(ByteOrder.nativeOrder()),
        vectors.size,
        2,
        """{"metric_type":"L2","dim":2,"M":4,"efConstruction":32} """
      )
      val binary = index.serialize()
      val build = 1000L + segment
      try {
        val paths = binary.names().toVector.map { name =>
          val bytes = allocator.buffer(binary.length(name))
          try {
            val view = bytes.nioBuffer(0, Math.toIntExact(binary.length(name)))
            binary.read(name, 0, view)
            val payload = new Array[Byte](view.capacity())
            bytes.getBytes(0, payload)
            val encoded = BinlogFixture.encode(
              payload,
              extras = s"""{"indexBuildID":"$build","nullable":false}"""
            )
            val header = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN)
            header
              .putLong(21, 10L)
              .putLong(29, 20L)
              .putLong(37, segment)
              .putLong(45, 101L)
            val path = s"files/index_files/$build/1/20/$segment/$name"
            Files.createDirectories(directory.resolve(path).getParent)
            Files.write(directory.resolve(path), encoded)
            path
          } finally bytes.close()
        }
        AvroIndexFileEntry(
          segment,
          101L,
          900L,
          build,
          "vector_hnsw",
          Map("index_type" -> "HNSW", "metric_type" -> "L2", "dim" -> "2"),
          paths,
          vectors.size,
          paths.map(p => Files.size(directory.resolve(p))).sum,
          1L,
          Some(8),
          Some(0)
        )
      } finally binary.close()
    } finally {
      try index.close()
      finally { buffer.close(); allocator.close() }
    }
  }

  private def snapshotJson(
      fixtures: Seq[Fixture],
      unindexedSegments: Set[Long] = Set.empty
  ): String = {
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val root = mapper.createObjectNode()
    val info = root.putObject("snapshot_info")
    info
      .put("name", "persisted-index-smoke")
      .put("id", 1L)
      .put("collection_id", 10L)
      .put("create_ts", 1L)
    info.putArray("partition_ids").add(20L)
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
          .put("value", "2")
    }
    root.put("format_version", 4)
    val definitions = root
      .putArray("indexes")
      .addObject()
      .put("collectionID", 10L)
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
    fixtures.foreach { fixture =>
      val key = s"files/snapshots/10/manifests/1/${fixture.task.segmentId}.avro"
      val directory =
        java.nio.file.Paths.get(fixture.task.properties("fs.root_path"))
      Files.createDirectories(directory.resolve(key).getParent)
      Files.write(
        directory.resolve(key),
        SegmentManifestFixture.encode(
          segmentId = fixture.task.segmentId,
          rows = 8L,
          indexes =
            if (unindexedSegments(fixture.task.segmentId)) Vector.empty
            else Vector(fixture.descriptor)
        )
      )
      manifests.add(key)
      if (!unindexedSegments(fixture.task.segmentId)) {
        builds.add(fixture.descriptor.buildId)
      }
      val base =
        fixture.task.layout.asInstanceOf[SegmentLayout.Manifest].basePath
      val manifest = mapper
        .createObjectNode()
        .put("ver", fixture.version)
        .put("base_path", base)
      dataManifests
        .addObject()
        .put("segmentID", fixture.task.segmentId)
        .put("manifest", mapper.writeValueAsString(manifest))
    }
    mapper.writeValueAsString(root)
  }
}
