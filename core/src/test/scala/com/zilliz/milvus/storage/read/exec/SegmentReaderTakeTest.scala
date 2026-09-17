package com.zilliz.milvus.storage.read.exec

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.{Collections, Comparator}
import scala.collection.JavaConverters._

import org.apache.arrow.memory.{
  BufferAllocator,
  OutOfMemoryException,
  RootAllocator
}
import org.apache.arrow.vector.{
  BigIntVector,
  FixedSizeBinaryVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.snapshot.{SegmentLayout, V2ColumnGroup}
import com.zilliz.milvus.storage.write.exec.{
  ManifestTransaction,
  V3SegmentWriter
}
import io.milvus.storage.MilvusStorageException

/** Native random row retrieval over real local parquet files. */
class SegmentReaderTakeTest extends AnyFunSuite with Matchers {

  private val schema = new Schema(
    Seq(
      field("100", new ArrowType.Int(64, true)),
      field("101", new ArrowType.Utf8(), nullable = true),
      field("102", new ArrowType.FixedSizeBinary(8))
    ).asJava
  )

  private def field(
      name: String,
      dataType: ArrowType,
      nullable: Boolean = false
  ): Field =
    new Field(
      name,
      new FieldType(nullable, dataType, null),
      Collections.emptyList[Field]()
    )

  private def withFixture(
      body: (Path, RootAllocator, Map[String, String]) => Unit
  ): Unit = {
    try NativeStorageLibrary.load()
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        cancel("libmilvus-storage-jni is not on this machine")
    }
    val directory = Files.createTempDirectory("segment-reader-take")
    val allocator = new RootAllocator(Long.MaxValue)
    try
      body(
        directory,
        allocator,
        Map("fs.storage_type" -> "local", "fs.root_path" -> directory.toString)
      )
    finally {
      try allocator.close()
      finally {
        val paths = Files.walk(directory)
        try
          paths
            .sorted(Comparator.reverseOrder())
            .forEach(Files.deleteIfExists(_))
        finally paths.close()
      }
    }
  }

  private def write(
      base: String,
      from: Int,
      count: Int,
      allocator: BufferAllocator,
      properties: Map[String, String],
      split: Boolean = false
  ): (SegmentReadTask, Seq[V2ColumnGroup]) = {
    val writer = new V3SegmentWriter(
      base,
      schema,
      properties,
      allocator,
      if (split) Seq("^102$") else Seq.empty
    )
    try {
      val root = VectorSchemaRoot.create(schema, allocator)
      try {
        root.allocateNew()
        val id = root.getVector("100").asInstanceOf[BigIntVector]
        val name = root.getVector("101").asInstanceOf[VarCharVector]
        val vector = root.getVector("102").asInstanceOf[FixedSizeBinaryVector]
        (0 until count).foreach { index =>
          val physical = from + index
          id.setSafe(index, physical.toLong * 11)
          if (physical % 7 == 0) name.setNull(index)
          else name.setSafe(index, s"row-$physical".getBytes(UTF_8))
          vector.setSafe(index, Array.fill[Byte](8)(physical.toByte))
        }
        root.setRowCount(count)
        writer.write(root)
      } finally root.close()
      val groups = writer.finish()
      try {
        val descriptors = (0 until groups.size).map { index =>
          V2ColumnGroup(
            groups.columns(index).map(_.toLong),
            groups.files(index),
            groups.rowCounts(index)
          )
        }
        val version = ManifestTransaction.commit(
          base,
          properties,
          groups,
          ManifestTransaction.AppendFiles
        )
        (
          SegmentReadTask(
            1L,
            1L,
            SegmentLayout.Manifest(base, version),
            Array.emptyByteArray,
            properties
          ),
          descriptors
        )
      } finally groups.close()
    } finally writer.close()
  }

  private def open(
      task: SegmentReadTask,
      allocator: BufferAllocator
  ): SegmentReader =
    SegmentReaderRegistry.open(
      task,
      schema,
      Seq("100", "101", "102"),
      id => Some(id.toString),
      allocator
    )

  private def readIds(result: SegmentReader.TakeResult): (Seq[Long], Int) = {
    val ids = Seq.newBuilder[Long]
    var batches = 0
    var next = result.next()
    while (next.nonEmpty) {
      val root = next.get
      try {
        batches += 1
        val vector = root.getVector("100").asInstanceOf[BigIntVector]
        (0 until root.getRowCount).foreach(index => ids += vector.get(index))
      } finally root.close()
      next = result.next()
    }
    (ids.result(), batches)
  }

  test(
    "manifest take projects scalar columns without opening the raw vector file"
  ) {
    withFixture { (directory, allocator, properties) =>
      val (task, groups) =
        write("segment", 0, 18000, allocator, properties, split = true)
      val vectors = groups.find(_.fieldIds == Seq(102L)).get
      vectors.filePaths.foreach(path => Files.delete(directory.resolve(path)))
      val reader = open(task, allocator)
      try {
        val offsets = Array[Long](0, 1, 7, 8191, 8192, 17999)
        val result = reader.take(offsets, Seq("101", "100"), parallelism = 2)
        try {
          var seen = 0
          var next = result.next()
          while (next.nonEmpty) {
            val root = next.get
            try {
              root.getSchema.getFields.asScala
                .map(_.getName)
                .toSeq shouldBe Seq("101", "100")
              val ids = root.getVector("100").asInstanceOf[BigIntVector]
              val names = root.getVector("101").asInstanceOf[VarCharVector]
              (0 until root.getRowCount).foreach { index =>
                val offset = offsets(seen)
                ids.get(index) shouldBe offset * 11
                if (offset % 7 == 0) names.isNull(index) shouldBe true
                else new String(names.get(index), UTF_8) shouldBe s"row-$offset"
                seen += 1
              }
            } finally root.close()
            next = result.next()
          }
          seen shouldBe offsets.length
          reader.deliveredRows shouldBe 0L
          reader.metrics.batches should be > 0L
          reader.metrics.arrowBytes should be > 0L
          reader.metrics.jniCalls should be > reader.metrics.batches
          reader.metrics.allocatedMax should be > 0L
        } finally result.close()
      } finally reader.close()
    }
  }

  test(
    "column group take preserves global row order across multiple files and batches"
  ) {
    withFixture { (_, allocator, properties) =>
      val files = (0 until 3).map { part =>
        write(s"part-$part", part * 5000, 5000, allocator, properties)._2.head
      }
      val task = SegmentReadTask(
        1L,
        1L,
        SegmentLayout.ColumnGroups(
          Seq(
            V2ColumnGroup(
              files.head.fieldIds,
              files.flatMap(_.filePaths),
              files.flatMap(_.fileRowCounts)
            )
          )
        ),
        Array.emptyByteArray,
        properties
      )
      val reader = open(task, allocator)
      try {
        val offsets = (0 until 15000 by 3).map(_.toLong).toArray
        val selected = reader.take(offsets, Seq("100"), parallelism = 2)
        try {
          val (ids, batches) = readIds(selected)
          ids shouldBe offsets.map(_ * 11).toSeq
          batches should be > 1
        } finally selected.close()
        // Random retrieval must not consume or shift the sequential stream.
        val first = reader.next().get
        try first.getVector("100").asInstanceOf[BigIntVector].get(0) shouldBe 0L
        finally first.close()
      } finally reader.close()
    }
  }

  test("take validates offsets and empty selections before native access") {
    withFixture { (_, allocator, properties) =>
      val (manifestTask, groups) =
        write("segment", 0, 10, allocator, properties)
      val task = manifestTask.copy(layout = SegmentLayout.ColumnGroups(groups))
      val reader = open(task, allocator)
      try {
        val empty = reader.take(Array.emptyLongArray, Seq("100"))
        empty.next() shouldBe None
        empty.close()
        empty.close()
        Seq(Array(-1L), Array(2L, 1L), Array(1L, 1L), Array(10L)).foreach {
          offsets =>
            intercept[IllegalArgumentException](
              reader.take(offsets, Seq("100"))
            )
        }
        intercept[IllegalArgumentException](
          reader.take(Array(0L), Seq("100"), parallelism = 0)
        )
        val selected = reader.take(Array(9L), Seq.empty)
        try {
          val batch = selected.next().get
          try {
            batch.getSchema.getFields.size() shouldBe 3
            batch
              .getVector("100")
              .asInstanceOf[BigIntVector]
              .get(0) shouldBe 99L
          } finally batch.close()
        } finally selected.close()
        reader.close()
        intercept[IllegalStateException](reader.take(Array(0L), Seq("100")))
      } finally reader.close()
    }
  }

  test(
    "selected batches outlive the source reader and returned roots outlive the result"
  ) {
    withFixture { (_, allocator, properties) =>
      val (task, _) = write("segment", 0, 100, allocator, properties)
      val reader = open(task, allocator)
      val result = reader.take(Array[Long](2, 17, 99), Seq("100"))
      reader.close()
      try {
        val batch = result.next().get
        val beforeClose = reader.metrics
        result.close()
        result.close()
        result.next() shouldBe None
        reader.metrics.batches shouldBe beforeClose.batches
        reader.metrics.arrowBytes shouldBe beforeClose.arrowBytes
        reader.metrics.jniCalls should be > beforeClose.jniCalls
        try
          batch.getVector("100").asInstanceOf[BigIntVector].get(0) shouldBe 22L
        finally batch.close()
      } finally {
        result.close()
        reader.close()
      }
    }
  }

  test("an import allocation failure closes the take result") {
    withFixture { (_, allocator, properties) =>
      val (task, _) = write("segment", 0, 100, allocator, properties)
      val child =
        allocator.newChildAllocator("take-import-failure", 0, 1024 * 1024)
      try {
        val reader = open(task, child)
        val result = reader.take(Array[Long](0, 99), Seq("100"))
        reader.close()
        try {
          child.setLimit(0)
          intercept[OutOfMemoryException](result.next())
          result.next() shouldBe None
          result.close()
          child.getAllocatedMemory shouldBe 0L
        } finally result.close()
      } finally child.close()
    }
  }

  test("native take failures propagate and leave the reader usable") {
    withFixture { (directory, allocator, properties) =>
      val (task, groups) = write("segment", 0, 10, allocator, properties)
      val file = directory.resolve(groups.head.filePaths.head)
      val hidden = file.resolveSibling(file.getFileName.toString + ".hidden")
      val reader = open(task, allocator)
      try {
        Files.move(file, hidden)
        try
          intercept[MilvusStorageException](reader.take(Array(0L), Seq("100")))
        finally Files.move(hidden, file)
        val selected = reader.take(Array(1L), Seq("100"))
        try readIds(selected)._1 shouldBe Seq(11L)
        finally selected.close()
      } finally reader.close()
    }
  }
}
