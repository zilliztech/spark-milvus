package com.zilliz.spark.connector.read

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files
import java.util.Arrays
import java.util.Comparator

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  FixedSizeBinaryVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types.{
  ArrayType,
  FloatType,
  LongType,
  MetadataBuilder,
  StructField,
  StructType
}

import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.{FieldMetadata, SchemaMapper}
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.milvus.storage.write.exec.{
  ManifestTransaction,
  V3SegmentWriter
}
import com.zilliz.spark.connector.options.{MilvusOption, VectorSearch}
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** Explicit adapter smoke with the real Knowhere library, without a Spark
  * service.
  */
object SegmentVectorSearchSmoke {
  def main(args: Array[String]): Unit = {
    val allocator = new RootAllocator()
    val metadata = new MetadataBuilder()
      .putLong(
        FieldMetadata.MilvusDataTypeMetadataKey,
        DataType.FloatVector.value.toLong
      )
      .build()
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField(
          "vector",
          ArrayType(FloatType, containsNull = false),
          nullable = true,
          metadata
        )
      )
    )
    def batch(ids: Seq[Long], values: Seq[Array[Float]]): VectorSchemaRoot = {
      val arrow = new Schema(
        Arrays.asList(
          new Field(
            "100",
            FieldType.notNullable(new ArrowType.Int(64, true)),
            null
          ),
          new Field(
            "101",
            FieldType.nullable(new ArrowType.FixedSizeBinary(8)),
            null
          )
        )
      )
      val root = VectorSchemaRoot.create(arrow, allocator)
      root.allocateNew()
      val idVector = root.getVector("100").asInstanceOf[BigIntVector]
      val vectors = root.getVector("101").asInstanceOf[FixedSizeBinaryVector]
      ids.indices.foreach { i =>
        idVector.setSafe(i, ids(i))
        if (values(i) == null) vectors.setNull(i)
        else {
          val bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
          values(i).foreach(bytes.putFloat)
          vectors.setSafe(i, bytes.array())
        }
      }
      root.setRowCount(ids.size)
      root
    }
    try {
      val first =
        batch(Seq(10L, 11L, 12L), Seq(Array(3f, 4f), null, Array(0f, 1f)))
      val second = batch(
        Seq(13L, 14L, 15L),
        Seq(Array(0f, 2f), Array(0f, 3f), Array(0f, 0f))
      )
      val hits = SegmentVectorSearch
        .run(
          VectorSearch(Array(0f, 0f), 3, "L2", "vector"),
          schema,
          Map("id" -> "100", "vector" -> "101"),
          Iterator(first, second),
          (root, row) =>
            root.getVector("100").asInstanceOf[BigIntVector].get(row) == 12L
        )
        .results
        .toVector
      assert(hits.map(_.row.getLong(0)) == Vector(15L, 13L, 14L))
      assert(hits.map(_.rowOffset) == Vector(5L, 3L, 4L))
      assert(hits.map(_.distance) == Vector(0d, 2d, 3d))
      assert(hits(1).row.getArray(1).getFloat(1) == 2f)
      assert(allocator.getAllocatedMemory == 0, "Arrow batches were not closed")

      Seq("brute_force" -> math.sqrt(2.0), "index" -> 2.0).foreach {
        case (mode, expected) =>
          val result = SegmentVectorSearch.run(
            VectorSearch(Array(0f, 0f), 1, "L2", "vector", mode = mode),
            schema,
            Map("id" -> "100", "vector" -> "101"),
            Iterator(batch(Seq(16L), Seq(Array(1f, 1f)))),
            (_, _) => false
          )
          val scores = result.results.map(_.distance).toVector
          assert(scores == Vector(expected), s"$mode returned $scores")
      }
      assert(
        allocator.getAllocatedMemory == 0,
        "L2 score checks leaked buffers"
      )

      val malformed = batch(Seq(20L), Seq(Array(0f, 0f)))
      try {
        SegmentVectorSearch.run(
          VectorSearch(Array(0f), 1, "L2", "vector"),
          schema,
          Map("id" -> "100", "vector" -> "101"),
          Iterator(malformed),
          (_, _) => false
        )
        throw new AssertionError("A dimension mismatch must fail")
      } catch {
        case error: IllegalArgumentException =>
          assert(error.getMessage.contains("dimension"))
      }
      assert(
        allocator.getAllocatedMemory == 0,
        "A failed query leaked its Arrow batch"
      )
      verifyReaderOwnership(allocator, schema)
      println(
        "PASS: Spark adapter and native segment reader use Knowhere; cross-batch TopK, deletes/nulls, offsets, distances, buffer release and prefetched-batch ownership"
      )
    } finally allocator.close()
  }

  /** Uses an actual local V3 segment so the reader prefetches an owned Arrow
    * batch before search validation. No service or external data is needed.
    */
  private def verifyReaderOwnership(
      allocator: RootAllocator,
      schema: StructType
  ): Unit = {
    val directory = Files.createTempDirectory("vector-reader-ownership")
    try {
      val collection = CollectionSchema(
        name = "vector-smoke",
        fields = Seq(
          FieldSchema(fieldID = 100L, name = "id", dataType = DataType.Int64),
          FieldSchema(
            fieldID = 101L,
            name = "vector",
            dataType = DataType.FloatVector,
            typeParams = Seq(KeyValuePair("dim", "2"))
          )
        )
      )
      val arrowSchema =
        SchemaMapper.convertToArrowSchemaWithFieldIdNames(collection)
      val properties = Map(
        "fs.storage_type" -> "local",
        "fs.root_path" -> directory.toAbsolutePath.toString
      )
      val writer =
        new V3SegmentWriter("segment", arrowSchema, properties, allocator)
      val version =
        try {
          val root = VectorSchemaRoot.create(arrowSchema, allocator)
          try {
            root.allocateNew()
            val values = Seq(Array(3f, 4f), Array(0f, 2f), Array(0f, 0f))
            values.indices.foreach { i =>
              root
                .getVector("0")
                .asInstanceOf[BigIntVector]
                .setSafe(i, i.toLong)
              root.getVector("1").asInstanceOf[BigIntVector].setSafe(i, 100L)
              root
                .getVector("100")
                .asInstanceOf[BigIntVector]
                .setSafe(i, 10L + i)
              val bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
              values(i).foreach(bytes.putFloat)
              root
                .getVector("101")
                .asInstanceOf[FixedSizeBinaryVector]
                .setSafe(i, bytes.array())
            }
            root.setRowCount(values.size)
            writer.write(root)
          } finally root.close()
          val groups = writer.finish()
          try
            ManifestTransaction.commit(
              "segment",
              properties,
              groups,
              ManifestTransaction.AppendFiles
            )
          finally groups.close()
        } finally writer.close()
      assert(
        allocator.getAllocatedMemory == 0,
        "Writing the local segment leaked buffers"
      )

      val task = SegmentReadTask(
        segmentId = 1L,
        partitionId = 1L,
        layout = SegmentLayout.Manifest("segment", version),
        schemaBytes = collection.toByteArray,
        properties = properties
      )
      val partition = MilvusV3InputPartition(task, "1", MilvusOption(""))
      val binding = V3ColumnBinding(partition, schema)
      val sharedAllocator = ArrowAllocator.get
      val baseline = sharedAllocator.getAllocatedMemory
      val invalidAllocator = ArrowAllocator.forReadTask(1L, 64L * 1024L * 1024L)
      val invalid = new MilvusRowPartitionReader(
        schema,
        binding,
        vectorSearch =
          Some(VectorSearch(Array(0f, 0f), 2, "HAMMING", "vector")),
        allocator = invalidAllocator.allocator,
        taskAllocatorOwner = Some(invalidAllocator)
      )
      try {
        assert(
          sharedAllocator.getAllocatedMemory > baseline,
          "The reader did not prefetch a batch"
        )
        try {
          invalid.next()
          throw new AssertionError(
            "Invalid metric must fail before consuming the batch"
          )
        } catch {
          case error: IllegalArgumentException =>
            assert(error.getMessage.contains("HAMMING"))
        }
      } finally invalid.close()
      invalid.close()
      assert(invalidAllocator.isClosed)
      assert(
        sharedAllocator.getAllocatedMemory == baseline,
        "Validation failure leaked the prefetched batch"
      )

      val readerAllocator = ArrowAllocator.forReadTask(1L, 64L * 1024L * 1024L)
      val reader = new MilvusRowPartitionReader(
        schema,
        binding,
        vectorSearch = Some(VectorSearch(Array(0f, 0f), 2, "L2", "vector")),
        allocator = readerAllocator.allocator,
        taskAllocatorOwner = Some(readerAllocator)
      )
      val results = Vector.newBuilder[(Long, Double, Long)]
      try {
        while (reader.next()) {
          val row = reader.get()
          results += ((
            row.getLong(0),
            row.getDouble(2),
            reader.lastReturnedRowOffset
          ))
        }
      } finally reader.close()
      assert(readerAllocator.isClosed)
      assert(results.result() == Vector((12L, 0d, 2L), (11L, 2d, 1L)))
      assert(
        sharedAllocator.getAllocatedMemory == baseline,
        "Successful reader search leaked buffers"
      )

      val indexAllocator = ArrowAllocator.forReadTask(1L, 1024L)
      val indexReader = new MilvusRowPartitionReader(
        schema,
        binding,
        vectorSearch = Some(
          VectorSearch(
            Array(0f, 0f),
            2,
            "L2",
            "vector",
            mode = "index"
          )
        ),
        allocator = indexAllocator.allocator,
        taskAllocatorOwner = Some(indexAllocator)
      )
      indexReader.close()
      indexReader.close()
      assert(indexAllocator.isClosed)
    } finally {
      val paths = Files.walk(directory)
      try
        paths
          .sorted(Comparator.reverseOrder())
          .forEach(Files.deleteIfExists(_))
      finally paths.close()
    }
  }
}
