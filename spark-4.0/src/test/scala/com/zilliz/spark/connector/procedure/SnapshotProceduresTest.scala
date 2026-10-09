package com.zilliz.spark.connector.procedure

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.types.{
  ArrayType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.client.api.{
  MilvusClient,
  MilvusConnectionParams,
  MilvusSnapshotInfo
}

class SnapshotProceduresTest extends AnyFunSuite with Matchers {
  private val target = ProcedureTarget(
    "db1",
    "collection1",
    MilvusConnectionParams("http://localhost:19530")
  )
  private val args =
    ProcedureArgs(Map("collection" -> "db1.collection1"), Map.empty)
  private val snapshot = MilvusSnapshotInfo(
    name = "snapshot1",
    description = "daily snapshot",
    collectionName = "collection1",
    partitionNames = Seq("p1", "p2"),
    createTs = 1234L,
    s3Location = "s3://bucket/snapshots/snapshot1"
  )

  test("snapshot procedures expose the designed parameters and schemas") {
    CreateSnapshotProcedure.parameters shouldBe Seq(
      Parameter("collection", StringType),
      Parameter("name", StringType),
      Parameter("description", StringType, required = false),
      Parameter("compaction_protection_seconds", LongType, required = false)
    )
    DropSnapshotProcedure.parameters.map(_.name) shouldBe Seq(
      "collection",
      "name"
    )
    ListSnapshotsProcedure.parameters.map(_.name) shouldBe Seq("collection")
    DescribeSnapshotProcedure.parameters.map(_.name) shouldBe Seq(
      "collection",
      "name"
    )

    val metadataSchema = StructType(
      Seq(
        StructField("database", StringType, nullable = false),
        StructField("collection", StringType, nullable = false),
        StructField("snapshot", StringType, nullable = false),
        StructField("description", StringType, nullable = false),
        StructField(
          "partition_names",
          ArrayType(StringType, containsNull = false),
          nullable = false
        ),
        StructField("create_ts", LongType, nullable = false),
        StructField("s3_location", StringType, nullable = false)
      )
    )
    CreateSnapshotProcedure.outputSchema shouldBe metadataSchema
    DescribeSnapshotProcedure.outputSchema shouldBe metadataSchema
    DropSnapshotProcedure.outputSchema.fieldNames shouldBe Array(
      "database",
      "collection",
      "snapshot",
      "status"
    )
    ListSnapshotsProcedure.outputSchema.fieldNames shouldBe Array(
      "database",
      "collection",
      "snapshot"
    )
  }

  test("create_snapshot forwards defaults and returns snapshot metadata") {
    val client = new FakeMilvusClient
    client.created = Success(())

    CreateSnapshotProcedure.run(
      args.copy(values = args.values + ("name" -> "snapshot1")),
      target,
      client
    ) shouldBe Seq(metadataRow)
    client.createCalls shouldBe Seq(
      ("db1", "collection1", "snapshot1", "", 0L)
    )
    client.describeCalls shouldBe Seq(("db1", "collection1", "snapshot1"))
  }

  test(
    "create_snapshot forwards optional values and rejects a negative protection period"
  ) {
    val client = new FakeMilvusClient
    client.created = Success(())
    val values = args.values ++ Map(
      "name" -> "snapshot1",
      "description" -> "daily snapshot",
      "compaction_protection_seconds" -> 3600L
    )

    CreateSnapshotProcedure.run(args.copy(values = values), target, client)
    client.createCalls shouldBe Seq(
      ("db1", "collection1", "snapshot1", "daily snapshot", 3600L)
    )

    val invalid = args.copy(
      values = values.updated("compaction_protection_seconds", -1L)
    )
    val error = intercept[IllegalArgumentException] {
      CreateSnapshotProcedure.run(invalid, target, client)
    }
    error.getMessage should include("must be non-negative")
    client.createCalls.size shouldBe 1
  }

  test(
    "drop_snapshot validates the name, calls Milvus, and reports completion"
  ) {
    val client = new FakeMilvusClient
    val callArgs = args.copy(values = args.values + ("name" -> " snapshot1 "))

    DropSnapshotProcedure.run(callArgs, target, client) shouldBe Seq(
      Row("db1", "collection1", " snapshot1 ", "dropped")
    )
    client.dropCalls shouldBe Seq(("db1", "collection1", " snapshot1 "))

    val error = intercept[IllegalArgumentException] {
      DropSnapshotProcedure.run(
        args.copy(values = args.values + ("name" -> "  ")),
        target,
        client
      )
    }
    error.getMessage should include("must not be empty")
    client.dropCalls.size shouldBe 1
  }

  test("list_snapshots preserves server order and an empty response") {
    val client = new FakeMilvusClient
    client.listed = Success(Seq("s2", "s1"))

    ListSnapshotsProcedure.run(args, target, client) shouldBe Seq(
      Row("db1", "collection1", "s2"),
      Row("db1", "collection1", "s1")
    )
    client.listCalls shouldBe Seq(("db1", "collection1"))

    client.listed = Success(Seq.empty)
    ListSnapshotsProcedure.run(args, target, client) shouldBe empty
  }

  test("describe_snapshot returns the server metadata") {
    val client = new FakeMilvusClient
    client.described = Success(snapshot)
    val callArgs = args.copy(values = args.values + ("name" -> "snapshot1"))

    DescribeSnapshotProcedure.run(callArgs, target, client) shouldBe Seq(
      metadataRow
    )
    client.describeCalls shouldBe Seq(("db1", "collection1", "snapshot1"))
  }

  test("snapshot client failures remain procedure failures") {
    val client = new FakeMilvusClient
    client.listed = Failure(new IllegalStateException("list failed"))

    val error = intercept[IllegalStateException] {
      ListSnapshotsProcedure.run(args, target, client)
    }
    error.getMessage shouldBe "list failed"
  }

  test("create_snapshot does not drop a created snapshot when describe fails") {
    val client = new FakeMilvusClient
    client.described = Failure(new IllegalStateException("describe failed"))

    val error = intercept[IllegalStateException] {
      CreateSnapshotProcedure.run(
        args.copy(values = args.values + ("name" -> "snapshot1")),
        target,
        client
      )
    }
    error.getMessage should include("was created")
    error.getMessage should include("left intact")
    error.getCause.getMessage shouldBe "describe failed"
    client.createCalls should have size 1
    client.dropCalls shouldBe empty
  }

  private def metadataRow: Row =
    Row(
      "db1",
      "collection1",
      "snapshot1",
      "daily snapshot",
      Seq("p1", "p2"),
      1234L,
      "s3://bucket/snapshots/snapshot1"
    )

  private final class FakeMilvusClient
      extends MilvusClient(target.connectionParams) {
    var created: Try[Unit] = Success(())
    var listed: Try[Seq[String]] = Success(Seq.empty)
    var described: Try[MilvusSnapshotInfo] = Success(snapshot)
    var dropped: Try[Unit] = Success(())

    var createCalls = Seq.empty[(String, String, String, String, Long)]
    var listCalls = Seq.empty[(String, String)]
    var describeCalls = Seq.empty[(String, String, String)]
    var dropCalls = Seq.empty[(String, String, String)]

    override def createSnapshot(
        dbName: String,
        collectionName: String,
        snapshotName: String,
        description: String,
        compactionProtectionSeconds: Long
    ): Try[Unit] = {
      createCalls :+= (
        dbName,
        collectionName,
        snapshotName,
        description,
        compactionProtectionSeconds
      )
      created
    }

    override def listSnapshots(
        dbName: String,
        collectionName: String
    ): Try[Seq[String]] = {
      listCalls :+= ((dbName, collectionName))
      listed
    }

    override def describeSnapshot(
        dbName: String,
        collectionName: String,
        snapshotName: String
    ): Try[MilvusSnapshotInfo] = {
      describeCalls :+= ((dbName, collectionName, snapshotName))
      described
    }

    override def describeSnapshotWithRetry(
        dbName: String,
        collectionName: String,
        snapshotName: String,
        maxAttempts: Int
    ): Try[MilvusSnapshotInfo] = {
      describeCalls :+= ((dbName, collectionName, snapshotName))
      described
    }

    override def dropSnapshot(
        dbName: String,
        collectionName: String,
        snapshotName: String
    ): Try[Unit] = {
      dropCalls :+= ((dbName, collectionName, snapshotName))
      dropped
    }
  }
}
