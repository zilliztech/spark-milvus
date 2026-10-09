package com.zilliz.spark.connector.write

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VarBinaryVector, VectorSchemaRoot}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.codec.ArrayCodec
import com.zilliz.spark.connector.types.{ArrowConverter, SparkSchemaMapper}
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType => MilvusDataType,
  FieldSchema
}

/** An Int8 or Int16 array element goes out only within its type's range, as the
  * Milvus proxy checks on insert (internal/proxy/fieldvalidator/
  * validate_util.go verifyOverflowByRange). The three integer element types
  * share one Spark and one physical form, so the write schema has to carry the
  * collection's element type. These run the write path a task runs:
  * WriteSchema.resolve, the Arrow schema, then
  * ArrowConverter.internalRowToArrow.
  */
class WriteValueLimitsTest extends AnyFunSuite with Matchers {

  private def arrayField(elementType: MilvusDataType): FieldSchema =
    FieldSchema(
      fieldID = 101,
      name = "arr",
      dataType = MilvusDataType.Array,
      elementType = elementType,
      nullable = true,
      typeParams = Seq(KeyValuePair("max_capacity", "4"))
    )

  private val pk = FieldSchema(
    fieldID = 100,
    name = "id",
    dataType = MilvusDataType.Int64,
    isPrimaryKey = true
  )

  /** Writes one row with the given column value through the task path. */
  private def write(
      field: FieldSchema,
      sparkType: DataType,
      value: Any
  )(check: VarBinaryVector => Unit): Unit = {
    val collection = CollectionSchema(name = "c", fields = Seq(pk, field))
    val schema = WriteSchema.resolve(
      StructType(
        Seq(StructField("id", LongType), StructField(field.name, sparkType))
      ),
      collection,
      WriteSchema.Mode.Append
    )
    val arrow = SparkSchemaMapper.convertSparkSchemaToArrow(
      schema,
      fieldIds = Map("id" -> 100L, field.name -> field.fieldID)
    )
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = VectorSchemaRoot.create(arrow, allocator)
      try {
        root.allocateNew()
        ArrowConverter.internalRowToArrow(
          root,
          0,
          InternalRow(1L, value),
          schema
        )
        root.setRowCount(1)
        root.getVector(1) match {
          case v: VarBinaryVector => check(v)
          case _                  => ()
        }
      } finally root.close()
    } finally allocator.close()
  }

  private def shorts(values: Short*) = ArrayData.toArrayData(values.toArray)

  test(
    "an Int8 array element outside [-128, 127] is refused, the bounds pass"
  ) {
    val field = arrayField(MilvusDataType.Int8)
    Seq[Short](128, -129).foreach { bad =>
      val e = intercept[IllegalArgumentException](
        write(field, ArrayType(ShortType), shorts(bad))(_ => ())
      )
      e.getMessage should include("arr")
    }
    write(field, ArrayType(ShortType), shorts(-128, 127)) { v =>
      ArrayCodec.elements(v.get(0)) shouldBe Seq(-128, 127)
    }
  }

  test("an Int16 array keeps its own range: 128 is a valid Int16") {
    write(arrayField(MilvusDataType.Int16), ArrayType(ShortType), shorts(128)) {
      v => ArrayCodec.elements(v.get(0)) shouldBe Seq(128)
    }
  }
}
