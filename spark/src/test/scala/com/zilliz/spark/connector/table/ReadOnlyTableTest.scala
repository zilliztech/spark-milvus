package com.zilliz.spark.connector.table

import java.util.Base64
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{
  SupportsMetadataColumns,
  SupportsRead,
  SupportsWrite,
  TableCapability
}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.sources.MilvusDataSource
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class ReadOnlyTableTest extends AnyFunSuite {
  private val schema = CollectionSchema(
    name = "fixed",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      )
    )
  )
  private val options = Map(
    MilvusOption.SnapshotMode -> "true",
    MilvusOption.SnapshotSchemaBytes -> Base64.getEncoder.encodeToString(
      schema.toByteArray
    ),
    "fs.storage_type" -> "local"
  )

  test("the public provider creates a read-only table from fixed metadata") {
    val source = MilvusDataSource()
    val inferred =
      source.inferSchema(new CaseInsensitiveStringMap(options.asJava))
    val table = source.getTable(inferred, Array.empty, options.asJava)
    assert(table.name() == "fixed")
    assert(table.isInstanceOf[SupportsRead])
    assert(!table.isInstanceOf[SupportsWrite])
    assert(
      table.capabilities().asScala.toSet == Set(TableCapability.BATCH_READ)
    )
    assert(table.schema().fieldNames.toSeq == Seq("id"))
    val metadataColumns =
      table.asInstanceOf[SupportsMetadataColumns].metadataColumns()
    assert(
      metadataColumns.map(_.name()).toSet == Set(
        "_partition_id",
        "_segment_id",
        "_row_offset",
        "_timestamp"
      )
    )
    assert(metadataColumns.forall(_.dataType() == LongType))
    assert(metadataColumns.forall(column => !column.isNullable()))
  }

  test("collection column names take precedence over hidden metadata columns") {
    val shadowingSchema = schema.addFields(
      FieldSchema(
        fieldID = 101L,
        name = "_ROW_OFFSET",
        dataType = DataType.VarChar
      )
    )
    val properties = (options + (
      MilvusOption.SnapshotSchemaBytes -> Base64.getEncoder.encodeToString(
        shadowingSchema.toByteArray
      )
    )).asJava
    val source = MilvusDataSource()
    val inferred = source.inferSchema(new CaseInsensitiveStringMap(properties))
    val table = source.getTable(inferred, Array.empty, properties)
    assert(table.schema().fieldNames.toSeq == Seq("id", "_ROW_OFFSET"))
    assert(
      table
        .asInstanceOf[SupportsMetadataColumns]
        .metadataColumns()
        .map(_.name())
        .toSet ==
        Set("_partition_id", "_segment_id", "_timestamp")
    )
  }

  test("a read that sets the removed milvus.filter option fails planning") {
    val source = MilvusDataSource()
    val schema =
      source.inferSchema(new CaseInsensitiveStringMap(options.asJava))
    val withFilter = (options + ("milvus.filter" -> "id == 1")).asJava
    val failures = Seq(
      intercept[IllegalArgumentException] {
        source.inferSchema(new CaseInsensitiveStringMap(withFilter))
      },
      intercept[IllegalArgumentException] {
        source.getTable(schema, Array.empty, withFilter)
      }
    )
    failures.foreach { failure =>
      assert(failure.getMessage.contains("'milvus.filter' was removed"))
    }
  }

  test(
    "the provider rejects unsupported options before resolving a snapshot, receipt or write"
  ) {
    val source = MilvusDataSource()
    val inferred =
      source.inferSchema(new CaseInsensitiveStringMap(options.asJava))
    for {
      (key, value) <- Seq(
        "milvus.partitions" -> "10,11",
        "fieldIDs" -> "100",
        "milvus.extra.columns" -> "_row_offset",
        "milvus.read.include_row_id" -> "true",
        "milvus.read.apply.deletes" -> "false",
        "milvus.read.columnar" -> "false"
      )
      base <- Seq(
        options,
        Map(MilvusOption.ReadMode -> MilvusOption.ReadModeWriteResult),
        Map(
          MilvusOption.WriteOperation -> MilvusOption.WriteOperationReplaceColumns
        )
      )
    } {
      val raw =
        (base + (key.toUpperCase(java.util.Locale.ROOT) -> value)).asJava
      val failures = Seq(
        intercept[IllegalArgumentException](
          source.inferSchema(new CaseInsensitiveStringMap(raw))
        ),
        intercept[IllegalArgumentException](
          source.getTable(inferred, Array.empty, raw)
        )
      )
      failures.foreach { failure =>
        assert(failure.getMessage == s"Unsupported option '$key'")
      }
    }
  }

  test("empty options cannot fall back to an online collection source") {
    val failure = intercept[IllegalArgumentException] {
      MilvusDataSource().inferSchema(
        new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      )
    }
    assert(failure.getMessage.contains(MilvusOption.SnapshotPath))
  }
}
