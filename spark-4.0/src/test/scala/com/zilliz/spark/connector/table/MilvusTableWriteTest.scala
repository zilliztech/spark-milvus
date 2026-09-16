package com.zilliz.spark.connector.table

import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.snapshot.{
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin
}
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** The write half of MilvusTable: which tables take writes, and that the
  * builder checks the DataFrame against the collection.
  */
class MilvusTableWriteTest extends AnyFunSuite with Matchers {

  private val collection = CollectionSchema(
    name = "c",
    fields = Seq(
      FieldSchema(
        fieldID = 100,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(fieldID = 101, name = "n", dataType = DataType.Int64)
    )
  )

  private def snapshotOf(origin: SnapshotOrigin): Snapshot =
    SnapshotCatalog
      .fromLists(
        name = "s",
        collectionId = 1L,
        createdAt = None,
        partitionIds = Seq(0L),
        schemaBytes = collection.toByteArray,
        v3Items = Seq.empty,
        v2Segments = Seq.empty,
        bucket = "",
        origin = origin
      )
      .fold(e => throw e, identity)

  private val options = MilvusOption(
    Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.SnapshotSchemaBytes -> java.util.Base64.getEncoder
        .encodeToString(collection.toByteArray),
      MilvusOption.MilvusCollectionName -> "c"
    )
  )

  test("a table from a snapshot or from Milvus takes batch writes") {
    Seq(SnapshotOrigin.Options, SnapshotOrigin.Catalog("s3://b/snap.json"))
      .foreach { origin =>
        val caps = MilvusTable(snapshotOf(origin), options, None).capabilities()
        caps should contain(TableCapability.BATCH_WRITE)
        caps should contain(TableCapability.BATCH_READ)
      }
  }

  test("a table from a backup is read-only") {
    val caps =
      MilvusTable(
        snapshotOf(SnapshotOrigin.Backup("s3://b/backup")),
        options,
        None
      )
        .capabilities()
    caps should not contain TableCapability.BATCH_WRITE
    caps should contain(TableCapability.BATCH_READ)
  }

  test("the write builder checks the DataFrame against the collection") {
    val table = MilvusTable(snapshotOf(SnapshotOrigin.Options), options, None)
    val info = new LogicalWriteInfo {
      override def queryId(): String = "q"
      override def schema(): StructType =
        StructType(Seq(StructField("id", LongType)))
      override def options(): CaseInsensitiveStringMap =
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    }
    val e =
      intercept[IllegalArgumentException](table.newWriteBuilder(info).build())
    e.getMessage should include(
      "Fields n of collection 'c' are missing from the DataFrame"
    )
  }

  // Review 749178e #10: the executor writer took the raw options while the
  // read resolved aliases, Hadoop keys and the IAM fallback; a read that works
  // must not fail the write with "fs.access_key_id must be set".
  test("the task writer gets the storage properties the read side resolves") {
    import com.zilliz.milvus.storage.credential.StorageProperties
    val iamOptions = MilvusOption(
      Map(
        MilvusOption.SnapshotMode -> "true",
        MilvusOption.SnapshotSchemaBytes -> java.util.Base64.getEncoder
          .encodeToString(collection.toByteArray),
        MilvusOption.MilvusCollectionName -> "c",
        StorageProperties.BucketName -> "b",
        StorageProperties.Address -> "s3.us-west-2.amazonaws.com",
        StorageProperties.UseSSL -> "true"
      )
    )
    val table =
      MilvusTable(snapshotOf(SnapshotOrigin.Options), iamOptions, None)
    val info = new LogicalWriteInfo {
      override def queryId(): String = "q"
      override def schema(): StructType =
        StructType(Seq(StructField("id", LongType), StructField("n", LongType)))
      override def options(): CaseInsensitiveStringMap =
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    }
    val batch = table.newWriteBuilder(info).build().toBatch
    val writer = batch.createBatchWriterFactory(null).createWriter(0, 0L)
    try {
      val storage = batch
        .asInstanceOf[com.zilliz.spark.connector.write.MilvusV3BatchWrite]
        .storage
      storage(StorageProperties.UseIam) shouldBe "true"
      storage(StorageProperties.BucketName) shouldBe "b"
      storage(StorageProperties.UseSSL) shouldBe "true"
    } finally writer.close()
  }
}
