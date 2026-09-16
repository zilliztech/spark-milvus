package com.zilliz.milvus.storage.schema

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import com.zilliz.milvus.storage.DataParseException
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** Milvus CollectionSchema to Arrow Schema.
  *
  * Two column-naming shapes. [[convertToArrowSchema]] uses field names, which
  * is what the layers above want. [[convertToArrowSchemaWithFieldIdNames]] uses
  * the field id as a string, because a manifest records column groups by field
  * id and the milvus-storage reader matches columns by id.
  */
object SchemaMapper {

  /** The two system columns every Milvus segment carries. They are appended
    * when the schema does not declare them, so the read path can project them.
    */
  val CanonicalSystemFields: Seq[FieldSchema] = Seq(
    FieldSchema(name = "RowID", fieldID = 0, dataType = DataType.Int64),
    FieldSchema(name = "Timestamp", fieldID = 1, dataType = DataType.Int64)
  )

  /** System columns have been spelled several ways across versions. Match on
    * the id; the name is only a fallback.
    */
  def systemFieldNameAliases(field: FieldSchema): Set[String] = {
    field.fieldID match {
      case 0 => Set("rowid", "row_id")
      case 1 => Set("timestamp")
      case _ => Set(field.name.toLowerCase)
    }
  }

  /** The system columns the schema does not declare, decided by field id alone.
    * Milvus reserves only the exact names RowID and Timestamp (proxy
    * validateReservedFieldNames), so a user field spelled `timestamp` or
    * `row_id` is an ordinary field with its own id and never stands in for id 0
    * or 1 (review 749178e #06).
    */
  def missingSystemFields(
      collectionSchema: CollectionSchema
  ): Seq[FieldSchema] = {
    val existingFieldIds = collectionSchema.fields.map(_.fieldID).toSet
    CanonicalSystemFields.filterNot(field =>
      existingFieldIds.contains(field.fieldID)
    )
  }

  /** Reads dim for vector fields; every other field gets 0. */
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

  /** The Arrow field for a scalar column: always nullable, carrying only
    * field_id.
    */
  def convertToArrowField(field: FieldSchema, arrowType: ArrowType): Field = {
    val metadata = Map("PARQUET:field_id" -> field.fieldID.toString).asJava
    new Field(field.name, new FieldType(true, arrowType, null, metadata), null)
  }

  def convertToArrowSchema(collectionSchema: CollectionSchema): Schema = {
    val fields = allFields(collectionSchema).map { field =>
      val dim = dimensionFor(field)
      val arrowType = ArrowTypes.toArrowType(dim, field.dataType)

      if (field.dataType == DataType.ArrayOfVector) {
        // A variable-length array of vectors: the element type and the
        // dimension can only travel in the field metadata.
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

  /** Names each column after its field id, and keeps original_name in the
    * metadata so the layers above can restore the logical name.
    */
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

  /** A nullable dense vector lands as variable-width binary so a null row does
    * not have to carry a fixed-width payload. Its dimension moves to the
    * metadata instead.
    */
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
