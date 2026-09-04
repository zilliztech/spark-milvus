package com.zilliz.spark.connector.loon

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema => CArrowSchema, Data}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{
  ArrowType,
  Field => ArrowField,
  FieldType,
  Schema => ArrowSchema
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.storage._

/** Regression test for the multi-file column-group row-range bug
  * (milvus-storage#657): `BuildLoonColumnGroups` used to write group-cumulative
  * `start_index`/`end_index`, which the packed reader intersects against each
  * file's own zero-based row groups, silently dropping every file after the
  * first. The connector feeds per-file row counts via
  * [[MilvusStorageColumnGroups.createFromGroups]] on the snapshot/backfill/
  * backup read paths, so all three were affected.
  *
  * This pins the fixed behavior through the native reader exactly as the C++
  * `TestMultiFileGroupBuildLoonReadsAllRows` does: build one column group
  * spanning TWO parquet files with per-file row counts and assert the packed
  * reader returns total rows == sum of per-file rows (regression: file 2+ used
  * to disappear silently).
  */
class MilvusStorageMultiFileGroupTest extends AnyFunSuite with Matchers {

  private def localProperties: MilvusStorageProperties = {
    val properties = new MilvusStorageProperties()
    val props = new java.util.HashMap[String, String]()
    props.put("fs.storage_type", "local")
    props.put("fs.root_path", "/")
    properties.create(props)
    properties.isValid shouldBe true
    properties
  }

  private def testArrowSchema(): ArrowSchema = {
    val metadataId = Map("PARQUET:field_id" -> "100").asJava
    val metadataVec = Map("PARQUET:field_id" -> "103").asJava
    val fields = List(
      new ArrowField(
        "id",
        new FieldType(false, new ArrowType.Int(64, true), null, metadataId),
        null
      ),
      new ArrowField(
        "vector",
        new FieldType(false, new ArrowType.List(), null, metadataVec),
        List(
          new ArrowField(
            "element",
            new FieldType(
              false,
              new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
              null
            ),
            null
          )
        ).asJava
      )
    ).asJava
    new ArrowSchema(fields)
  }

  /** Export an ArrowSchema and return the wrapped C object + its address. */
  private def exportSchema(
      allocator: org.apache.arrow.memory.BufferAllocator,
      schema: ArrowSchema
  ): (CArrowSchema, Long) = {
    val cSchema = CArrowSchema.allocateNew(allocator)
    Data.exportSchema(allocator, schema, null, cSchema)
    (cSchema, cSchema.memoryAddress())
  }

  /** Write `numRows` rows via the native writer into `dir/_data` and return the
    * absolute path of the single parquet file produced.
    */
  private def writeSingleFile(
      dir: Path,
      numRows: Int
  ): (String, Int) = {
    val allocator = ArrowUtils.getAllocator
    val properties = localProperties
    val writer = new MilvusStorageWriter()

    val (schema, schemaPtr) = exportSchema(allocator, testArrowSchema())
    try {
      writer.create(dir.toAbsolutePath.toString, schemaPtr, properties)

      val root = VectorSchemaRoot.create(testArrowSchema(), allocator)
      val idVector = root.getVector("id").asInstanceOf[BigIntVector]
      val vectorList =
        root.getVector("vector").asInstanceOf[org.apache.arrow.vector.complex.ListVector]

      idVector.allocateNew(numRows)
      vectorList.allocateNew()
      val listWriter = vectorList.getWriter
      var i = 0
      while (i < numRows) {
        idVector.set(i, i.toLong)
        listWriter.setPosition(i)
        listWriter.startList()
        listWriter.float4().writeFloat4(i * 0.1f)
        listWriter.endList()
        i += 1
      }
      root.setRowCount(numRows)

      val arrowArray = ArrowArray.allocateNew(allocator)
      var closedPtr = 0L
      try {
        Data.exportVectorSchemaRoot(allocator, root, null, arrowArray)
        writer.write(arrowArray.memoryAddress())
        writer.flush()
        closedPtr = writer.close()
      } finally {
        arrowArray.close()
        root.close()
        MilvusStorageColumnGroups.destroy(closedPtr)
      }

      // The writer lays out one parquet per column group under `_data/`.
      val dataDir = dir.resolve("_data").toFile
      dataDir.listFiles().filter(_.getName.endsWith(".parquet")).toList match {
        case single :: Nil => (single.getAbsolutePath, numRows)
        case files =>
          fail(s"expected exactly one parquet under $dataDir, got $files")
      }
    } finally {
      writer.destroy()
      properties.free()
      schema.close()
    }
  }

  test(
    "multi-file column group via native reader returns total rows == sum of per-file rows"
  ) {
    NativeLibraryLoader.loadLibrary()

    val dirA = Files.createTempDirectory("milvus-multifile-a-")
    val dirB = Files.createTempDirectory("milvus-multifile-b-")
    try {
      // Two files of different sizes: file 2 must NOT be dropped.
      val (pathA, rowsA) = writeSingleFile(dirA, 100)
      val (pathB, rowsB) = writeSingleFile(dirB, 50)

      // Move file B into the same directory as file A so both live under one
      // base path (mirrors the C++ regression test, which uses one base_path).
      val pathBEff = {
        val moved = new java.io.File(pathA).getParentFile.toPath.resolve(
          new java.io.File(pathB).getName
        )
        Files.move(
          java.nio.file.Paths.get(pathB),
          moved,
          java.nio.file.StandardCopyOption.REPLACE_EXISTING
        )
        moved.toString
      }

      // Sanity: a single-file group for file B alone must read its own rows.
      val singleB = MilvusStorageColumnGroups.createFromGroups(
        Array(Array("id", "vector")),
        Array(Array(pathBEff)),
        Array(Array(rowsB.toLong))
      )
      try {
        val alloc0 = ArrowUtils.getAllocator
        val props0 = localProperties
        val r0 = new MilvusStorageReader()
        val (s0, s0p) = exportSchema(alloc0, testArrowSchema())
        try {
          r0.create(singleB, s0p, Array("id"), props0)
          val h = r0.openRecordBatchReaderScala()
          try {
            var rows = 0L
            var done = false
            while (!done) {
              val ba = ArrowArray.allocateNew(alloc0)
              val bs = CArrowSchema.allocateNew(alloc0)
              try {
                if (r0.readNextBatchScala(h, ba.memoryAddress(), bs.memoryAddress())) {
                  val rr = Data.importVectorSchemaRoot(alloc0, ba, bs, null)
                  try rows += rr.getRowCount finally rr.close()
                } else done = true
              } finally {
                ba.close()
                bs.close()
              }
            }
            rows shouldBe rowsB.toLong
          } finally r0.destroyRecordBatchReaderScala(h)
        } finally {
          r0.destroy()
          s0.close()
          props0.free()
        }
      } finally MilvusStorageColumnGroups.destroy(singleB)

      // Build one column group spanning BOTH files, exactly as the connector's
      // MilvusStorageColumnGroups.createFromGroups path does (per-file row
      // counts, one group).
      val cols = Array(Array("id", "vector"))
      val files = Array(Array(pathA, pathBEff))
      val rcs = Array(Array(rowsA.toLong, rowsB.toLong))
      val columnGroupsPtr =
        MilvusStorageColumnGroups.createFromGroups(cols, files, rcs)
      columnGroupsPtr should not be 0L

      val allocator = ArrowUtils.getAllocator
      val properties = localProperties
      val reader = new MilvusStorageReader()
      val (schema, schemaPtr) = exportSchema(allocator, testArrowSchema())
      try {
        reader.create(columnGroupsPtr, schemaPtr, Array("id"), properties)
        reader.isValid shouldBe true

        val rbrHandle = reader.openRecordBatchReaderScala()
        try {
          var totalRows = 0L
          var done = false
          while (!done) {
            val batchArray = ArrowArray.allocateNew(allocator)
            val batchSchema = CArrowSchema.allocateNew(allocator)
            try {
              val hasBatch = reader.readNextBatchScala(
                rbrHandle,
                batchArray.memoryAddress(),
                batchSchema.memoryAddress()
              )
              if (!hasBatch) {
                done = true
              } else {
                val readRoot = Data.importVectorSchemaRoot(
                  allocator,
                  batchArray,
                  batchSchema,
                  null
                )
                try {
                  totalRows += readRoot.getRowCount
                } finally {
                  readRoot.close()
                }
              }
            } finally {
              batchArray.close()
              batchSchema.close()
            }
          }
          // Regression: file 2+ used to be silently truncated.
          totalRows shouldBe (rowsA + rowsB).toLong
        } finally {
          reader.destroyRecordBatchReaderScala(rbrHandle)
        }
      } finally {
        reader.destroy()
        schema.close()
        MilvusStorageColumnGroups.destroy(columnGroupsPtr)
        properties.free()
      }
    } finally {
      deleteRecursively(dirA.toFile)
      deleteRecursively(dirB.toFile)
    }
  }

  private def deleteRecursively(dir: java.io.File): Unit = {
    if (dir.exists()) {
      if (dir.isDirectory) dir.listFiles().foreach(deleteRecursively)
      dir.delete()
    }
  }
}
