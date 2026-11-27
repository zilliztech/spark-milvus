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

  /** The canonical Milvus system fields, shared by readers and writers. */
  val RowIdFieldId: Long = 0L
  val TimestampFieldId: Long = 1L

  /** The two system columns every Milvus segment carries. They are appended
    * when the schema does not declare them, so the read path can project them.
    */
  val CanonicalSystemFields: Seq[FieldSchema] = Seq(
    FieldSchema(
      name = "RowID",
      fieldID = RowIdFieldId,
      dataType = DataType.Int64
    ),
    FieldSchema(
      name = "Timestamp",
      fieldID = TimestampFieldId,
      dataType = DataType.Int64
    )
  )

  /** The system columns the schema does not declare, decided by field id alone.
    * Milvus reserves only the exact names RowID and Timestamp (proxy
    * validateReservedFieldNames), so a user field spelled `timestamp` or
    * `row_id` is an ordinary field with its own id and never stands in for id 0
    * or 1.
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

  /** Milvus stores each struct-array member as its own column, named by the
    * member field id. The parent id identifies the group only in the schema.
    */
  def physicalFields(collectionSchema: CollectionSchema): Seq[FieldSchema] =
    missingSystemFields(collectionSchema) ++ collectionSchema.fields ++
      collectionSchema.structArrayFields.flatMap(_.fields)

  def convertToArrowSchema(collectionSchema: CollectionSchema): Schema =
    physicalSchema(collectionSchema, useFieldIds = false)

  /** Names each column after its field id and keeps original_name in metadata.
    */
  def convertToArrowSchemaWithFieldIdNames(
      collectionSchema: CollectionSchema
  ): Schema = physicalSchema(collectionSchema, useFieldIds = true)

  private def physicalSchema(
      schema: CollectionSchema,
      useFieldIds: Boolean
  ): Schema = {
    val memberIds =
      schema.structArrayFields.flatMap(_.fields.map(_.fieldID)).toSet
    val fields = physicalFields(schema).map { field =>
      val dim = dimensionFor(field)
      val id = field.fieldID.toString
      val (name, nameMetadata) =
        if (useFieldIds)
          (id, Map("original_name" -> field.name))
        else (field.name, Map.empty[String, String])
      val metadata = Map("PARQUET:field_id" -> id) ++ nameMetadata
      val isVectorArray = field.dataType == DataType.ArrayOfVector
      val isNativeScalarArray = field.dataType == DataType.Array &&
        (field.elementNullable || field.elementType == DataType.Array || field.typeSchema.nonEmpty)
      val nullableDense =
        MilvusTypes.isDenseVectorType(field.dataType) && field.nullable
      val vectorMetadata =
        if (isVectorArray)
          Map(
            "elementType" -> field.elementType.value.toString,
            "dim" -> dim.toString
          )
        else if (nullableDense) Map("dim" -> dim.toString)
        else Map.empty[String, String]

      val (physicalType, children) = if (isVectorArray) {
        if (!MilvusTypes.isDenseVectorType(field.elementType)) {
          throw new DataParseException(
            s"Unsupported vector array element ${field.elementType} for field '${field.name}'"
          )
        }
        val elementType =
          if (field.elementNullable) new ArrowType.Binary()
          else ArrowTypes.toArrowType(dim, field.elementType)
        (new ArrowType.List(), Seq(listItem(elementType, nullable = true)))
      } else if (isNativeScalarArray) {
        (new ArrowType.List(), Seq(scalarArrayItem(field)))
      } else {
        val arrowType =
          if (nullableDense) new ArrowType.Binary()
          else ArrowTypes.toArrowType(dim, field.dataType)
        (arrowType, Seq.empty[Field])
      }
      val nullable =
        if (
          MilvusTypes.isVectorType(field.dataType) || isVectorArray ||
          isNativeScalarArray || memberIds(field.fieldID)
        ) field.nullable
        else true
      new Field(
        name,
        new FieldType(
          nullable,
          physicalType,
          null,
          (metadata ++ vectorMetadata).asJava
        ),
        children.asJava
      )
    }
    new Schema(fields.asJava)
  }

  private def listItem(
      arrowType: ArrowType,
      nullable: Boolean,
      children: Seq[Field] = Seq.empty
  ): Field =
    new Field("item", new FieldType(nullable, arrowType, null), children.asJava)

  private def scalarArrayItem(field: FieldSchema): Field = {
    val nested =
      field.elementType == DataType.Array || field.typeSchema.nonEmpty
    val (elementType, nullable) = if (nested) {
      val leaf = MilvusTypes.nestedArrayLeaf(field)
      (leaf.getLeafType, leaf.nullable)
    } else (field.elementType, field.elementNullable)
    elementType match {
      case DataType.Bool | DataType.Int8 | DataType.Int16 | DataType.Int32 |
          DataType.Int64 | DataType.Float | DataType.Double |
          DataType.VarChar =>
        val item = listItem(ArrowTypes.toArrowType(0, elementType), nullable)
        if (nested)
          listItem(new ArrowType.List(), field.elementNullable, Seq(item))
        else item
      case _ =>
        throw new DataParseException(
          s"Unsupported native array element $elementType for field '${field.name}'"
        )
    }
  }
}
