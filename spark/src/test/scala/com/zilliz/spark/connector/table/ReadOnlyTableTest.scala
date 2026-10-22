package com.zilliz.spark.connector.table

import java.util.Base64
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  SupportsWrite,
  TableCapability
}
import org.apache.spark.sql.types.StructType
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
  }

  test(
    "service names, backup and V2 metadata fail before native or online access"
  ) {
    Seq(
      Map(
        "milvus.database.name" -> "db",
        "milvus.collection.name" -> "collection"
      ),
      Map("milvus.backup.dir" -> "s3://bucket/backup"),
      Map("milvus.snapshot.v2.segments" -> "[]"),
      Map("milvus.writer.customPath" -> "staging")
    ).foreach { retired =>
      val failure = intercept[IllegalArgumentException] {
        MilvusDataSource().getTable(
          StructType(Nil),
          Array.empty,
          (options ++ retired).asJava
        )
      }
      assert(
        retired.keys.exists(key =>
          failure.getMessage.toLowerCase.contains(key.toLowerCase)
        )
      )
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
