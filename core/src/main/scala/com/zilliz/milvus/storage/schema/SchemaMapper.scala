package com.zilliz.milvus.storage.schema

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import com.zilliz.milvus.storage.DataParseException
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** Milvus 的 CollectionSchema 到 Arrow Schema。
  *
  * 两种列名形态：[[convertToArrowSchema]] 用字段名，给上层看；
  * [[convertToArrowSchemaWithFieldIdNames]] 用字段 id 的字符串， 因为 Manifest 里的列组按字段 id
  * 记列，milvus-storage 的 reader 按 id 匹配。
  */
object SchemaMapper {

  /** Milvus 每个段都有的两个系统列。schema 里没声明时补上，读侧才能投影它们。 */
  val CanonicalSystemFields: Seq[FieldSchema] = Seq(
    FieldSchema(name = "RowID", fieldID = 0, dataType = DataType.Int64),
    FieldSchema(name = "Timestamp", fieldID = 1, dataType = DataType.Int64)
  )

  /** 系统列在不同版本里出现过几种写法，按 id 认，名字只作兜底。 */
  def systemFieldNameAliases(field: FieldSchema): Set[String] = {
    field.fieldID match {
      case 0 => Set("rowid", "row_id")
      case 1 => Set("timestamp")
      case _ => Set(field.name.toLowerCase)
    }
  }

  def missingSystemFields(
      collectionSchema: CollectionSchema
  ): Seq[FieldSchema] = {
    val existingNames = collectionSchema.fields.map(_.name.toLowerCase).toSet
    val existingFieldIds = collectionSchema.fields.map(_.fieldID).toSet
    CanonicalSystemFields.filterNot(field =>
      systemFieldNameAliases(field).exists(existingNames.contains) ||
        existingFieldIds.contains(field.fieldID)
    )
  }

  /** 向量字段取 dim，其余传 0。 */
  private def dimensionFor(field: FieldSchema): Int = field.dataType match {
    case DataType.BinaryVector | DataType.Float16Vector |
        DataType.BFloat16Vector | DataType.Int8Vector | DataType.FloatVector |
        DataType.ArrayOfVector =>
      try MilvusTypes.dimensionOf(field)
      catch {
        case e: DataParseException =>
          throw new DataParseException(
            s"dim not found in field [${field.name}] params: ${e.getMessage}"
          )
      }
    case _ => 0
  }

  /** 标量字段的 Arrow Field：一律 nullable，只带 field_id。 */
  def convertToArrowField(field: FieldSchema, arrowType: ArrowType): Field = {
    val metadata = Map("PARQUET:field_id" -> field.fieldID.toString).asJava
    new Field(field.name, new FieldType(true, arrowType, null, metadata), null)
  }

  def convertToArrowSchema(collectionSchema: CollectionSchema): Schema = {
    val fields = allFields(collectionSchema).map { field =>
      val dim = dimensionFor(field)
      val arrowType = ArrowTypes.toArrowType(dim, field.dataType)

      if (field.dataType == DataType.ArrayOfVector) {
        // 变长向量数组：元素类型和维度只能靠字段元数据带下去。
        val metadata = Map(
          "PARQUET:field_id" -> field.fieldID.toString,
          "elementType" -> field.elementType.value.toString,
          "dim" -> dim.toString
        ).asJava
        new Field(
          field.name,
          new FieldType(true, arrowType, null, metadata),
          null
        )
      } else if (isVectorType(field.dataType)) {
        new Field(
          field.name,
          new FieldType(
            field.nullable,
            physicalTypeOf(field, dim, arrowType),
            null,
            vectorMetadata(field, dim).asJava
          ),
          null
        )
      } else {
        convertToArrowField(field, arrowType)
      }
    }
    new Schema(fields.asJava)
  }

  /** 列名换成字段 id 的字符串，另带 original_name 供上层还原。 */
  def convertToArrowSchemaWithFieldIdNames(
      collectionSchema: CollectionSchema
  ): Schema = {
    val fields = allFields(collectionSchema).map { field =>
      val dim = dimensionFor(field)
      val arrowType = ArrowTypes.toArrowType(dim, field.dataType)
      val metadata = vectorMetadata(field, dim) +
        ("original_name" -> field.name)
      new Field(
        field.fieldID.toString,
        new FieldType(
          if (isVectorType(field.dataType)) field.nullable else true,
          physicalTypeOf(field, dim, arrowType),
          null,
          metadata.asJava
        ),
        null
      )
    }
    new Schema(fields.asJava)
  }

  private def allFields(collectionSchema: CollectionSchema): Seq[FieldSchema] =
    missingSystemFields(collectionSchema) ++ collectionSchema.fields

  private def isVectorType(dataType: DataType): Boolean =
    MilvusTypes.isDenseVectorType(dataType) ||
      dataType == DataType.SparseFloatVector

  /** 可空的稠密向量落变长二进制：空行不必再占一个定长载荷。维度改由元数据带。 */
  private def physicalTypeOf(
      field: FieldSchema,
      dim: Int,
      arrowType: ArrowType
  ): ArrowType =
    if (MilvusTypes.isDenseVectorType(field.dataType) && field.nullable)
      new ArrowType.Binary()
    else arrowType

  private def vectorMetadata(
      field: FieldSchema,
      dim: Int
  ): Map[String, String] =
    Map("PARQUET:field_id" -> field.fieldID.toString) ++
      (if (MilvusTypes.isDenseVectorType(field.dataType) && field.nullable)
         Map("dim" -> dim.toString)
       else Map.empty)
}
