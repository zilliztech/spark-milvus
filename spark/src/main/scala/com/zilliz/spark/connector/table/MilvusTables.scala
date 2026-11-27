package com.zilliz.spark.connector.table

import java.util.Locale

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.snapshot.Snapshot
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.SparkTypes

/** Builds a table and its schema from the same validated options and snapshot
  * source for DataSource loads and schema inference.
  */
private[connector] object MilvusTables {

  def load(
      options: CaseInsensitiveStringMap,
      sparkSchema: Option[StructType]
  ): MilvusTable = {
    val milvusOption = validate(options)
    MilvusTable(resolve(milvusOption), milvusOption, sparkSchema)
  }

  /** The Spark schema derived from the resolved snapshot's collection schema.
    */
  def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val milvusOption = validate(options)
    val snapshot = resolve(milvusOption)
    sparkSchemaOf(
      snapshot,
      MilvusOption.readVectorRaw(options)
    )
  }

  /** The snapshot this table is about, resolved once. A failure here is the
    * table's failure: there is no schema or plan without it.
    */
  private def resolve(milvusOption: MilvusOption): Snapshot =
    SnapshotSources
      .forRead(milvusOption)
      .snapshot()
      .fold(
        {
          case e: IllegalArgumentException => throw e
          case e =>
            throw new IllegalArgumentException(
              s"Cannot resolve the snapshot to read: ${e.getMessage}",
              e
            )
        },
        identity
      )

  private def validate(options: CaseInsensitiveStringMap): MilvusOption = {
    val milvusOption = MilvusOption(options)
    MilvusOption.validateSnapshotModeOptions(options)
    if (!MilvusOption.isSnapshotMode(options)) {
      throw new IllegalArgumentException(
        s"Public reads require '${MilvusOption.SnapshotPath}' or explicit V3 snapshot metadata"
      )
    }
    milvusOption
  }

  private[connector] def sparkSchemaOf(
      snapshot: Snapshot,
      rawVectors: Boolean
  ): StructType = {
    val fields =
      snapshot.schema.fields.map(SparkTypes.toStructField(_, rawVectors)) ++
        snapshot.schema.structArrayFields.map(
          SparkTypes.toStructArrayField(_, rawVectors)
        )
    checkedSchema(StructType(fields))
  }

  /** Resolve duplicate names on the driver, after an external schema has
    * selected its fields. Executor column binding uses exact stored identities.
    */
  private[table] def checkedSchema(schema: StructType): StructType = {
    val caseSensitive = SQLConf.get.caseSensitiveAnalysis
    def check(fields: Array[StructField], owner: String): Unit = {
      val clashes = fields
        .groupBy { field =>
          if (caseSensitive) field.name else field.name.toLowerCase(Locale.ROOT)
        }
        .values
        .filter(_.length > 1)
        .map(_.map(_.name).mkString(" and "))
        .toSeq
      if (clashes.nonEmpty) {
        val mode = if (caseSensitive) "case-sensitive" else "case-insensitive"
        throw new IllegalArgumentException(
          s"$owner contains field names that collide under $mode resolution: ${clashes.mkString(", ")}"
        )
      }
    }
    check(schema.fields, "Snapshot schema")
    schema.fields.foreach { field =>
      field.dataType match {
        case ArrayType(members: StructType, _) =>
          check(members.fields, s"Struct array '${field.name}'")
        case _ =>
      }
    }
    schema
  }
}
