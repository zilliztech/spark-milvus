package com.zilliz.milvus.storage.write.exec

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.jni.storage.StorageNative
import com.zilliz.milvus.storage.read.exec.SegmentReaderRegistry
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.snapshot.{SegmentLayout, V2ColumnGroup}

/** The two segment writers against the real native library, on the local
  * backend: what they write reads back through the registry the readers use.
  * Cancels where the library is absent.
  */
class SegmentWriterTest extends AnyFunSuite with Matchers {

  private def skipWithoutLibrary(): Unit =
    try
      StorageNative.filesystemDestroy(
        StorageNative.filesystemGet(
          Map("fs.storage_type" -> "local").asJava,
          ""
        )
      )
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        cancel("libnative-storage-jni is not on this machine")
      case _: RuntimeException =>
        cancel("libnative-storage-jni is not on this machine")
    }

  private val schema = new Schema(
    Seq(
      new Field(
        "100",
        new FieldType(false, new ArrowType.Int(64, true), null),
        java.util.Collections.emptyList[Field]()
      ),
      new Field(
        "101",
        new FieldType(false, new ArrowType.Utf8(), null),
        java.util.Collections.emptyList[Field]()
      )
    ).asJava
  )

  /** The same columns with the `PARQUET:field_id` metadata the V2 packed writer
    * requires (Milvus's own V2 parquet carries it).
    */
  private val v2Schema = new Schema(
    schema.getFields.asScala.map { f =>
      new Field(
        f.getName,
        new FieldType(
          f.isNullable,
          f.getType,
          null,
          Map("PARQUET:field_id" -> f.getName).asJava
        ),
        java.util.Collections.emptyList[Field]()
      )
    }.asJava
  )

  private def properties(dir: Path): Map[String, String] = Map(
    "fs.storage_type" -> "local",
    "fs.root_path" -> dir.toAbsolutePath.toString
  )

  /** A fresh batch of `count` rows starting at `from`; the writer contract is
    * one root per batch.
    */
  private def batch(
      allocator: RootAllocator,
      from: Int,
      count: Int,
      of: Schema = schema
  ): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(of, allocator)
    val id = root.getVector("100").asInstanceOf[BigIntVector]
    val name = root.getVector("101").asInstanceOf[VarCharVector]
    id.allocateNew(count)
    name.allocateNew(count)
    var i = 0
    while (i < count) {
      id.setSafe(i, (from + i).toLong * 7)
      name.setSafe(i, s"row-${from + i}".getBytes("UTF-8"))
      i += 1
    }
    root.setRowCount(count)
    root
  }

  private def readAll(
      task: SegmentReadTask,
      allocator: RootAllocator
  ): Seq[(Long, String)] = {
    val reader = SegmentReaderRegistry.open(
      task,
      schema,
      Seq("100", "101"),
      id => Some(id.toString),
      allocator
    )
    try {
      val out = Seq.newBuilder[(Long, String)]
      var next = reader.next()
      while (next.isDefined) {
        val root = next.get
        try {
          val id = root.getVector("100").asInstanceOf[BigIntVector]
          val name = root.getVector("101").asInstanceOf[VarCharVector]
          (0 until root.getRowCount).foreach { i =>
            out += ((id.get(i), new String(name.get(i), "UTF-8")))
          }
        } finally root.close()
        next = reader.next()
      }
      out.result()
    } finally reader.close()
  }

  private def withDir(body: Path => Unit): Unit = {
    val dir = Files.createTempDirectory("segment-writer")
    try body(dir)
    finally
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
  }

  test(
    "V3: batches become column groups, the manifest commits, rows read back"
  ) {
    skipWithoutLibrary()
    withDir { dir =>
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val writer =
          new V3SegmentWriter("segment-1", schema, properties(dir), allocator)
        val first = batch(allocator, 0, 3000)
        writer.write(first)
        first.close()
        val second = batch(allocator, 3000, 2000)
        writer.write(second)
        second.close()
        writer.rows shouldBe 5000L

        val groups = writer.finish()
        val version =
          try {
            groups.size should be > 0
            groups.rows shouldBe 5000L
            ManifestTransaction.commit(
              "segment-1",
              properties(dir),
              groups,
              ManifestTransaction.AppendFiles
            )
          } finally groups.close()
        version should be >= 1L
        // finish released the writer; close is then a no-op, twice.
        writer.close()
        writer.close()

        val rows = readAll(
          SegmentReadTask(
            segmentId = 1L,
            partitionId = 1L,
            layout = SegmentLayout.Manifest("segment-1", version),
            schemaBytes = Array.emptyByteArray,
            properties = properties(dir)
          ),
          allocator
        )
        rows.size shouldBe 5000
        rows.head shouldBe ((0L, "row-0"))
        rows(4999) shouldBe ((4999L * 7, "row-4999"))
      } finally allocator.close()
    }
  }

  test("V3: close without finish releases the writer once") {
    skipWithoutLibrary()
    withDir { dir =>
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val writer =
          new V3SegmentWriter("segment-2", schema, properties(dir), allocator)
        val b = batch(allocator, 0, 10)
        writer.write(b)
        b.close()
        writer.close()
        writer.close()
        an[IllegalStateException] should be thrownBy writer.finish()
      } finally allocator.close()
    }
  }

  test("V2: one file per column group at the named paths, rows read back") {
    skipWithoutLibrary()
    withDir { dir =>
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val paths = Seq("insert_log/1/2/3/100/7", "insert_log/1/2/3/101/8")
        an[IllegalArgumentException] should be thrownBy new V2SegmentWriter(
          paths,
          Seq(Seq(0), Seq(1)),
          schema,
          properties(dir),
          allocator
        )
        val writer = new V2SegmentWriter(
          paths,
          Seq(Seq(0), Seq(1)),
          v2Schema,
          properties(dir),
          allocator
        )
        val b = batch(allocator, 0, 1000, v2Schema)
        writer.write(b)
        b.close()
        val rows = writer.finish()
        rows shouldBe 1000L
        writer.close()
        paths.foreach(p => Files.exists(dir.resolve(p)) shouldBe true)

        val read = readAll(
          SegmentReadTask(
            segmentId = 3L,
            partitionId = 2L,
            layout = SegmentLayout.ColumnGroups(
              Seq(
                V2ColumnGroup(Seq(100L), Seq(paths(0)), Seq(1000L)),
                V2ColumnGroup(Seq(101L), Seq(paths(1)), Seq(1000L))
              )
            ),
            schemaBytes = Array.emptyByteArray,
            properties = properties(dir)
          ),
          allocator
        )
        read.size shouldBe 1000
        read(999) shouldBe ((999L * 7, "row-999"))
      } finally allocator.close()
    }
  }

  test("StagingLayout keeps a job outside insert_log") {
    val layout = StagingLayout("files/", "job-1")
    layout.prefix shouldBe "files/staging/job-1"
    layout.segment(2, 7L) shouldBe "files/staging/job-1/2/task_2_7"
    layout.manifest shouldBe "files/staging/job-1/manifest.json"
    StagingLayout("", "j").prefix shouldBe "staging/j"
    an[IllegalArgumentException] should be thrownBy StagingLayout(
      "files",
      "a/b"
    )
  }
}
