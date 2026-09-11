package com.zilliz.milvus.storage.schema

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Milvus CollectionSchema 到 Arrow Schema。 */
class SchemaMapperTest extends AnyFunSuite with Matchers {

  test("system fields are not appended over case-insensitive name conflicts") {
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

    val arrowSchema = SchemaMapper.convertToArrowSchema(schema)
    val names = arrowSchema.getFields.asScala.map(_.getName)

    names shouldBe Seq("row_id", "timestamp")
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
