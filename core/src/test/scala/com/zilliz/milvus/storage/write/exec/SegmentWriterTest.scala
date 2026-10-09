package com.zilliz.milvus.storage.write.exec

import java.nio.file.{Files, Path}
import java.util.{List => JavaList}
import java.util.{Map => JavaMap}
import java.util.Collections
import java.util.Comparator
import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
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
      NativeStorageLibrary.load()
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        cancel("libmilvus-storage-jni is not on this machine")
      case _: RuntimeException =>
        cancel("libmilvus-storage-jni is not on this machine")
    }

  private val schema = new Schema(
    Seq(
      new Field(
        "100",
        new FieldType(false, new ArrowType.Int(64, true), null),
        Collections.emptyList[Field]()
      ),
      new Field(
        "101",
        new FieldType(false, new ArrowType.Utf8(), null),
        Collections.emptyList[Field]()
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
        Collections.emptyList[Field]()
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
        .sorted(Comparator.reverseOrder())
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
        // G5: allocate and create properties, open, then write and flush per batch.
        writer.metrics.batches shouldBe 2L
        writer.metrics.jniCalls shouldBe 3L + 2L * 2L
        writer.metrics.arrowBytes should be > (5000L * 8)
        writer.metrics.allocatedMax should be > 0L

        val groups = writer.finish()
        writer.metrics.jniCalls shouldBe 3L + 2L * 2L + 3L
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

  test("V3: column-group patterns split the columns the way they say") {
    skipWithoutLibrary()
    withDir { dir =>
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val writer = new V3SegmentWriter(
          "segment-split",
          schema,
          properties(dir),
          allocator,
          columnGroupPatterns = Seq("^100$")
        )
        val rows = batch(allocator, 0, 100)
        writer.write(rows)
        rows.close()
        val groups = writer.finish()
        try {
          groups.size shouldBe 2
          (0 until groups.size).map(
            groups.columns(_)
          ) should contain theSameElementsAs
            Seq(Seq("100"), Seq("101"))
          (0 until groups.size).foreach(i =>
            groups.rowCounts(i).sum shouldBe 100L
          )
        } finally groups.close()
      } finally allocator.close()
    }
  }

  test("V3: a stats entry committed with the groups is in the manifest") {
    skipWithoutLibrary()
    withDir { dir =>
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val writer =
          new V3SegmentWriter(
            "segment-stats",
            schema,
            properties(dir),
            allocator
          )
        val rows = batch(allocator, 0, 10)
        writer.write(rows)
        rows.close()
        val groups = writer.finish()
        val statFile = dir.resolve("segment-stats/_stats/bloom_filter.100/7")
        Files.createDirectories(statFile.getParent)
        Files.write(statFile, "{\"fieldID\":100}".getBytes)
        val version =
          try
            ManifestTransaction.commit(
              "segment-stats",
              properties(dir),
              groups,
              ManifestTransaction.AppendFiles,
              Seq(
                ManifestTransaction.Stat(
                  "bloom_filter.100",
                  Seq("segment-stats/_stats/bloom_filter.100/7"),
                  Map("memory_size" -> "15")
                )
              )
            )
          finally groups.close()
        version shouldBe 1L

        // Read the manifest as Milvus's reader would: the stats map keyed by
        // the entry, its paths relative to _stats/, its metadata as given.
        val reader = new DataFileReader[
          GenericRecord
        ](
          dir.resolve("segment-stats/_metadata/manifest-1.avro").toFile,
          new GenericDatumReader[
            GenericRecord
          ]()
        )
        try {
          val manifest = reader.next()
          val stats = manifest
            .get("stats")
            .asInstanceOf[
              JavaMap[AnyRef, GenericRecord]
            ]
            .asScala
            .map { case (k, v) => k.toString -> v }
          val entry = stats("bloom_filter.100")
          entry
            .get("paths")
            .asInstanceOf[JavaList[AnyRef]]
            .asScala
            .map(_.toString) shouldBe
            Seq("bloom_filter.100/7")
          entry
            .get("metadata")
            .asInstanceOf[JavaMap[AnyRef, AnyRef]]
            .asScala
            .map { case (k, v) => k.toString -> v.toString } shouldBe Map(
            "memory_size" -> "15"
          )
        } finally reader.close()
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
    StagingLayout("files", "job.1").prefix shouldBe
      "files/staging/job.1"
    an[IllegalArgumentException] should be thrownBy StagingLayout(
      "files",
      "a/b"
    )
    Seq(".", "..", "a\\b", "a\nb", " job").foreach { jobId =>
      an[IllegalArgumentException] should be thrownBy StagingLayout(
        "files",
        jobId
      )
    }
  }
}
