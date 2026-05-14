package com.zilliz.spark.connector.serde

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ArrowConverterTest extends AnyFunSuite with Matchers {
  test("arrowToInternalRow reads Spark StringType from Arrow VarBinaryVector") {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowField = new Field(
      "103",
      FieldType.nullable(new ArrowType.Binary()),
      null
    )
    val root =
      VectorSchemaRoot.create(new Schema(Seq(arrowField).asJava), allocator)

    try {
      val vector = root.getVector("103").asInstanceOf[VarBinaryVector]
      vector.allocateNew()
      vector.setSafe(
        0,
        "{\"source\":\"json\"}".getBytes(StandardCharsets.UTF_8)
      )
      vector.setValueCount(1)
      root.setRowCount(1)

      val sparkSchema = StructType(Seq(StructField("$meta", StringType, true)))
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        sparkSchema,
        Map("$meta" -> "103")
      )

      row.getUTF8String(0).toString shouldBe "{\"source\":\"json\"}"
    } finally {
      root.close()
      allocator.close()
    }
  }
}
