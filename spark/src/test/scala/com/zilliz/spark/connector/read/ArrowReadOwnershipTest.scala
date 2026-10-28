package com.zilliz.spark.connector.read

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Arrays

import org.apache.arrow.c.{
  ArrowArray,
  ArrowSchema,
  CDataDictionaryProvider,
  Data
}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  FixedSizeBinaryVector,
  IntVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.SPARK_VERSION
import org.scalatest.funsuite.AnyFunSuite

/** The C Data ownership contract used by readers, tested against each line's
  * Arrow version.
  */
class ArrowReadOwnershipTest extends AnyFunSuite {
  private val schema = new Schema(
    Arrays.asList(
      new Field(
        "category",
        FieldType.nullable(new ArrowType.Int(32, true)),
        null
      ),
      new Field(
        "vector",
        FieldType.nullable(new ArrowType.FixedSizeBinary(8)),
        null
      )
    )
  )

  private def vector(x: Float, y: Float): Array[Byte] =
    ByteBuffer
      .allocate(8)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putFloat(x)
      .putFloat(y)
      .array()

  private def requireSupportedRuntime(): Unit =
    assume(
      !SPARK_VERSION.startsWith("3.5.") || Runtime.version().feature() < 21,
      "Spark 3.5's Arrow 12 C Data runtime requires the supported Java 17 test JVM"
    )

  private def exported(
      check: (BufferAllocator, ArrowArray, ArrowSchema) => Unit
  ): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val source = VectorSchemaRoot.create(schema, allocator)
      var sourceClosed = false
      try {
        source.allocateNew()
        val category = source.getVector("category").asInstanceOf[IntVector]
        category.setSafe(0, 7)
        category.setNull(1)
        category.setSafe(2, -3)
        val vectors =
          source.getVector("vector").asInstanceOf[FixedSizeBinaryVector]
        vectors.setSafe(0, vector(1.25f, -2.5f))
        vectors.setNull(1)
        vectors.setSafe(2, vector(0f, 9.5f))
        source.setRowCount(3)

        val array = ArrowArray.allocateNew(allocator)
        array.save(new ArrowArray.Snapshot())
        try {
          val cSchema = ArrowSchema.allocateNew(allocator)
          cSchema.save(new ArrowSchema.Snapshot())
          try {
            Data.exportSchema(allocator, schema, null, cSchema)
            Data.exportVectorSchemaRoot(allocator, source, null, array)
            source.close()
            sourceClosed = true
            assert(allocator.getAllocatedMemory > 0L)
            check(allocator, array, cSchema)
          } finally {
            try if (cSchema.snapshot().release != 0L) cSchema.release()
            finally cSchema.close()
          }
        } finally {
          try if (array.snapshot().release != 0L) array.release()
          finally array.close()
        }
      } finally if (!sourceClosed) source.close()
    } finally {
      try assert(allocator.getAllocatedMemory == 0L)
      finally allocator.close()
    }
  }

  private def imported(
      allocator: BufferAllocator,
      array: ArrowArray,
      cSchema: ArrowSchema
  )(check: VectorSchemaRoot => Unit): Unit = {
    val dictionaries = new CDataDictionaryProvider()
    try {
      // Imports consume their wrappers. Borrow them so exported() still owns
      // the struct shells and can release callbacks left by a failed import.
      val root = Data.importVectorSchemaRoot(
        allocator,
        ArrowArray.wrap(array.memoryAddress()),
        ArrowSchema.wrap(cSchema.memoryAddress()),
        dictionaries
      )
      try check(root)
      finally root.close()
    } finally dictionaries.close()
  }

  test(
    "import retains nullable values and dense vectors after the source closes"
  ) {
    requireSupportedRuntime()
    exported { (allocator, array, cSchema) =>
      imported(allocator, array, cSchema) { root =>
        assert(root.getRowCount == 3)
        val category = root.getVector("category").asInstanceOf[IntVector]
        assert(
          category.get(0) == 7 && category.isNull(1) && category.get(2) == -3
        )
        val vectors =
          root.getVector("vector").asInstanceOf[FixedSizeBinaryVector]
        assert(vectors.get(0).sameElements(vector(1.25f, -2.5f)))
        assert(vectors.isNull(1))
        assert(vectors.get(2).sameElements(vector(0f, 9.5f)))
      }
    }
  }

  test(
    "a failed consumer closes imported buffers and releases all allocations"
  ) {
    requireSupportedRuntime()
    val failure = new IllegalStateException("consumer failed")
    val caught = intercept[IllegalStateException] {
      exported { (allocator, array, cSchema) =>
        imported(allocator, array, cSchema)(_ => throw failure)
      }
    }
    assert(caught eq failure)
  }

  test("an abandoned export releases its callbacks without an import") {
    requireSupportedRuntime()
    val failure = new IllegalStateException("read canceled before import")
    val caught = intercept[IllegalStateException] {
      exported { (_, _, _) => throw failure }
    }
    assert(caught eq failure)
  }
}
