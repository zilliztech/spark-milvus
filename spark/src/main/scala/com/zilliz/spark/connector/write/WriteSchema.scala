package com.zilliz.spark.connector.write

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types.{
  BinaryType,
  DataType,
  MetadataBuilder,
  StructField,
  StructType
}

import com.zilliz.milvus.storage.schema.{MilvusTypes, SchemaMapper}
import com.zilliz.spark.connector.types.{SparkSchemaMapper, SparkTypes}
import io.milvus.grpc.schema.{CollectionSchema, FieldSchema}

/** The schema a write goes out with: the DataFrame's columns, checked against
  * the collection schema and annotated from it.
  *
  * Spark hands a TableProvider the DataFrame's own schema and resolves
  * `AppendData` against that, so nothing upstream compares the DataFrame with
  * the collection; this is where that happens, once, on the driver, before any
  * task starts. The result carries the Milvus type, the vector dimension and
  * the field id in each column's metadata, which is all the task writer needs
  * to build the Arrow schema and name the columns by field id.
  *
  * Resolves collection field identities and physical column types for shared
  * Storage V3 writers without owning a submission or ingestion lifecycle.
  */
object WriteSchema {

  /** The Arrow schema a segment is written with: the declared columns, and the
    * row identity and timestamp columns when this write makes a new segment. A
    * column is named by its Milvus field id, which is how the manifest line
    * matches them up.
    */
  def arrowSchemaFor(
      sparkSchema: StructType,
      fieldIds: Map[String, Long],
      withSystemFields: Boolean
  ): Schema = {
    val declared = SparkSchemaMapper.convertSparkSchemaToArrow(
      sparkSchema,
      fieldIds = fieldIds
    )
    if (!withSystemFields) declared
    else {
      require(
        !declared.getFields.asScala.exists(field =>
          field.getName == SchemaMapper.RowIdFieldId.toString ||
            field.getName == SchemaMapper.TimestampFieldId.toString
        ),
        "A write that carries RowID or Timestamp itself cannot also be given placeholders"
      )
      val system =
        Seq(SchemaMapper.RowIdFieldId, SchemaMapper.TimestampFieldId).map(id =>
          new Field(
            id.toString,
            FieldType.notNullable(new ArrowType.Int(64, true)),
            null
          )
        )
      new Schema((declared.getFields.asScala ++ system).asJava)
    }
  }

  sealed trait Mode

  object Mode {

    /** A whole row per record: every field of the collection except function
      * outputs must be present.
      */
    case object Append extends Mode

    /** Only the columns given (column replacement): each must be a collection
      * field, nothing has to be complete.
      */
    case object Columns extends Mode
  }

  def resolve(
      dataFrame: StructType,
      collection: CollectionSchema,
      mode: Mode
  ): StructType = {
    val byName = collection.fields.map(f => f.name -> f).toMap
    val unknown = dataFrame.fieldNames.filterNot(byName.contains)
    if (unknown.nonEmpty) {
      throw new IllegalArgumentException(
        s"Columns ${unknown.mkString(", ")} are not fields of collection '${collection.name}'; " +
          s"its fields are ${collection.fields.map(_.name).mkString(", ")}"
      )
    }
    // A new row has to be routed to the partition its key hashes to, which
    // the connector does not do. Replacing other columns of stored rows keeps
    // every row where it is, so only the key column itself is refused.
    collection.fields.find(_.isPartitionKey).foreach { key =>
      if (mode == Mode.Append) {
        throw new IllegalArgumentException(
          s"Collection '${collection.name}' has partition key '${key.name}'; " +
            "writing to a partition-key collection is not supported"
        )
      }
      if (dataFrame.fieldNames.contains(key.name)) {
        throw new IllegalArgumentException(
          s"Column '${key.name}' is the partition key of collection '${collection.name}' and cannot be replaced"
        )
      }
    }
    val fields = dataFrame.fields.map { column =>
      val field = byName(column.name)
      if (field.isFunctionOutput) {
        throw new IllegalArgumentException(
          s"Column '${column.name}' is the output of a Milvus function and cannot be written"
        )
      }
      if (mode == Mode.Append && field.isPrimaryKey && field.autoID) {
        throw new IllegalArgumentException(
          s"Collection '${collection.name}' allocates its primary key '${field.name}' (autoID); " +
            "the connector cannot allocate ids, so it cannot write this collection"
        )
      }
      val expected = SparkTypes.toDataType(field)
      val accepted =
        if (MilvusTypes.isVectorType(field.dataType)) Seq(expected, BinaryType)
        else Seq(expected)
      if (
        !accepted
          .exists(t => DataType.equalsIgnoreNullability(t, column.dataType))
      ) {
        throw new IllegalArgumentException(
          s"Column '${column.name}' is ${column.dataType.simpleString}, " +
            s"field '${field.name}' (${field.dataType}) takes ${accepted.map(_.simpleString).mkString(" or ")}"
        )
      }
      annotate(column, field)
    }
    if (mode == Mode.Append) {
      val present = dataFrame.fieldNames.toSet
      val missing = collection.fields.filterNot(f =>
        f.isFunctionOutput || present.contains(f.name)
      )
      if (missing.nonEmpty) {
        throw new IllegalArgumentException(
          s"Fields ${missing.map(_.name).mkString(", ")} of collection '${collection.name}' " +
            "are missing from the DataFrame; a written segment carries every field, " +
            "so give a nullable field a null column"
        )
      }
    }
    StructType(fields)
  }

  /** The column with its DataFrame type kept and the collection's type, field
    * id, dimension and nullability put on it.
    */
  private def annotate(column: StructField, field: FieldSchema): StructField =
    column.copy(
      nullable = field.nullable,
      metadata = new MetadataBuilder()
        .withMetadata(column.metadata)
        .withMetadata(SparkTypes.metadata(field))
        .build()
    )
}
