package com.zilliz.milvus.storage.schema

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.types.pojo.ArrowType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.DataParseException
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType,
  FieldSchema,
  StructArrayFieldSchema,
  TypeSchema
}

/** Milvus CollectionSchema to Arrow Schema. */
class SchemaMapperTest extends AnyFunSuite with Matchers {

  test(
    "Milvus 3.0.6 flattens struct members with their physical Arrow element types"
  ) {
    val scalar = FieldSchema(
      fieldID = 102,
      name = "items[count]",
      dataType = DataType.Array,
      elementType = DataType.Int16
    )
    val scores =
      scalar.copy(fieldID = 103, name = "items[scores]", elementNullable = true)
    val vector = FieldSchema(
      fieldID = 104,
      name = "items[vector]",
      dataType = DataType.ArrayOfVector,
      elementType = DataType.FloatVector,
      typeParams = Seq(KeyValuePair(key = "dim", value = "4"))
    )
    val nullableVector = vector.copy(
      fieldID = 105,
      name = "items[optional]",
      elementNullable = true
    )
    val nested = scalar.copy(
      fieldID = 106,
      name = "items[tags]",
      elementType = DataType.Array,
      elementNullable = true,
      typeSchema = Some(
        TypeSchema().withArrayElement(
          TypeSchema(nullable = true).withArrayElement(
            TypeSchema().withLeafType(DataType.VarChar)
          )
        )
      )
    )
    val members = Seq(scalar, scores, vector, nullableVector, nested)
    val parent =
      StructArrayFieldSchema(fieldID = 101, name = "items", fields = members)
    val schema = CollectionSchema(structArrayFields = Seq(parent))
    SchemaMapper.physicalFields(schema).map(_.fieldID) shouldBe Seq(0L, 1L,
      102L, 103L, 104L, 105L, 106L)
    val named =
      SchemaMapper.convertToArrowSchema(schema).getFields.asScala.drop(2)
    val byId = SchemaMapper
      .convertToArrowSchemaWithFieldIdNames(schema)
      .getFields
      .asScala
      .drop(2)
    named.map(_.getName) shouldBe members.map(_.name)
    for ((field, i) <- byId.zipWithIndex) {
      field.getName shouldBe members(i).fieldID.toString
      field.getMetadata.get("PARQUET:field_id") shouldBe field.getName
      field.getMetadata.get("original_name") shouldBe members(i).name
      field.getType shouldBe named(i).getType
      field.getChildren shouldBe named(i).getChildren
    }
    byId.head.getType shouldBe new ArrowType.Binary()
    val scalarItem = byId(1).getChildren.get(0)
    scalarItem.getType shouldBe new ArrowType.Int(16, true)
    scalarItem.isNullable shouldBe true
    byId(2).getMetadata.get("dim") shouldBe "4"
    byId(2).getMetadata.get(
      "elementType"
    ) shouldBe DataType.FloatVector.value.toString
    byId(2).getChildren.get(0).getType shouldBe new ArrowType.FixedSizeBinary(
      16
    )
    byId(3).getChildren.get(0).getType shouldBe new ArrowType.Binary()
    val nestedItem = byId(4).getChildren.get(0)
    nestedItem.getType shouldBe new ArrowType.List()
    nestedItem.isNullable shouldBe true
    nestedItem.getChildren.get(0).getType shouldBe new ArrowType.Utf8()
    nestedItem.getChildren.get(0).isNullable shouldBe false

    intercept[DataParseException] {
      SchemaMapper.convertToArrowSchema(
        CollectionSchema(fields = Seq(nested.copy(elementNullable = false)))
      )
    }.getMessage should include("items[tags]")
  }
  // Milvus reserves only the exact names RowID and Timestamp, so row_id(100)
  // and timestamp(101) are user fields and ids 0 and 1 still have to be there.
  test(
    "system fields are appended by id even when a user field is named like one"
  ) {
    import io.milvus.grpc.schema.{
      CollectionSchema => MilvusCollectionSchema,
      DataType => MilvusDataType,
      FieldSchema => MilvusFieldSchema
    }

    val schema = MilvusCollectionSchema(
      fields = Seq(
        MilvusFieldSchema(
          name = "row_id",
          fieldID = 100,
          dataType = MilvusDataType.Int64
        ),
        MilvusFieldSchema(
          name = "timestamp",
          fieldID = 101,
          dataType = MilvusDataType.Int64
        )
      )
    )

    SchemaMapper
      .convertToArrowSchema(schema)
      .getFields
      .asScala
      .map(_.getName) shouldBe Seq("RowID", "Timestamp", "row_id", "timestamp")
    SchemaMapper
      .convertToArrowSchemaWithFieldIdNames(schema)
      .getFields
      .asScala
      .map(_.getName) shouldBe Seq("0", "1", "100", "101")
    SchemaMapper.missingSystemFields(schema).map(_.fieldID) shouldBe Seq(0L, 1L)
  }

  test("Milvus collection schema uses Binary for nullable dense vectors") {
    import io.milvus.grpc.common.KeyValuePair
    import io.milvus.grpc.schema.{
      CollectionSchema => MilvusCollectionSchema,
      DataType => MilvusDataType,
      FieldSchema => MilvusFieldSchema
    }
    import org.apache.arrow.vector.types.pojo.ArrowType

    def vector(
        name: String,
        fieldId: Long,
        dataType: MilvusDataType,
        nullable: Boolean
    ) = MilvusFieldSchema(
      name = name,
      fieldID = fieldId,
      dataType = dataType,
      nullable = nullable,
      typeParams = Seq(KeyValuePair(key = "dim", value = "4"))
    )

    val schema = MilvusCollectionSchema(
      fields = Seq(
        vector("fixed", 100, MilvusDataType.FloatVector, nullable = false),
        vector("nullable", 101, MilvusDataType.FloatVector, nullable = true)
      )
    )

    val byLogicalName = SchemaMapper
      .convertToArrowSchema(schema)
      .getFields
      .asScala
      .map(field => field.getName -> field)
      .toMap
    byLogicalName("fixed").getType shouldBe new ArrowType.FixedSizeBinary(16)
    byLogicalName("fixed").isNullable shouldBe false
    byLogicalName("nullable").getType shouldBe new ArrowType.Binary()
    byLogicalName("nullable").isNullable shouldBe true
    byLogicalName("nullable").getMetadata.get("dim") shouldBe "4"

    val byFieldId = SchemaMapper
      .convertToArrowSchemaWithFieldIdNames(schema)
      .getFields
      .asScala
      .map(field => field.getName -> field)
      .toMap
    byFieldId("100").getType shouldBe new ArrowType.FixedSizeBinary(16)
    byFieldId("101").getType shouldBe new ArrowType.Binary()
    byFieldId("101").getMetadata.get("dim") shouldBe "4"
  }
}
