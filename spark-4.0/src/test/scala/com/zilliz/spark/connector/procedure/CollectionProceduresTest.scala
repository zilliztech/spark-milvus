package com.zilliz.spark.connector.procedure

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.types.{BooleanType, LongType}
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.client.api.{
  MilvusClient,
  MilvusCollectionInfo,
  MilvusCompactionInfo,
  MilvusCompactionState,
  MilvusConnectionParams,
  MilvusIndexInfo,
  MilvusSegmentInfo
}
import io.milvus.grpc.common.{
  CompactionState,
  IndexState,
  KeyValuePair,
  LoadState,
  SegmentLevel,
  SegmentState,
  Status
}
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class CollectionProceduresTest extends AnyFunSuite with Matchers {
  private val target = ProcedureTarget(
    "db1",
    "collection1",
    MilvusConnectionParams("http://localhost:19530")
  )
  private val baseArgs =
    ProcedureArgs(Map("collection" -> "db1.collection1"), Map.empty)

  test("collection procedures expose stable result schemas") {
    LoadProcedure.outputSchema.fieldNames shouldBe Array(
      "database",
      "collection",
      "state"
    )
    ReleaseProcedure.outputSchema.fieldNames shouldBe Array(
      "database",
      "collection",
      "status"
    )
    FlushProcedure.outputSchema shouldBe ReleaseProcedure.outputSchema
    CompactProcedure.outputSchema.fieldNames shouldBe Array(
      "database",
      "collection",
      "compaction_id",
      "plan_count",
      "state",
      "executing_plan_count",
      "completed_plan_count",
      "failed_plan_count",
      "timeout_plan_count"
    )
    CompactProcedure.outputSchema("plan_count").dataType shouldBe LongType
    CompactProcedure.outputSchema("executing_plan_count").nullable shouldBe true
    DescribeProcedure
      .outputSchema("partition_key")
      .dataType shouldBe BooleanType
    DescribeProcedure.outputSchema("dimension").nullable shouldBe true
    DescribeProcedure.outputSchema("index_id").nullable shouldBe true
  }

  test("load submits once without polling and can wait for Loaded") {
    val client = new FakeMilvusClient

    LoadProcedure.run(baseArgs, target, client) shouldBe Seq(
      Row("db1", "collection1", "submitted")
    )
    client.loadCalls shouldBe Seq(("db1", "collection1"))
    client.loadStateCalls shouldBe empty

    client.loadState = Success(LoadState.LoadStateLoaded)
    val waitArgs = baseArgs.copy(
      values = baseArgs.values + ("wait" -> true)
    )
    LoadProcedure.run(waitArgs, target, client) shouldBe Seq(
      Row("db1", "collection1", "LoadStateLoaded")
    )
    client.loadStateCalls shouldBe Seq(("db1", "collection1"))
    client.loadStateTimeouts should have size 1
    client.loadStateTimeouts.head should be > 0L
  }

  test("load fails on LoadStateNotExist") {
    val client = new FakeMilvusClient
    client.loadState = Success(LoadState.LoadStateNotExist)
    val args = baseArgs.copy(values = baseArgs.values + ("wait" -> true))

    val error = intercept[IllegalStateException] {
      LoadProcedure.run(args, target, client)
    }
    error.getMessage should include("LoadStateNotExist")
  }

  test("release and flush call the corresponding RPC once") {
    val client = new FakeMilvusClient

    ReleaseProcedure.run(baseArgs, target, client) shouldBe Seq(
      Row("db1", "collection1", "released")
    )
    FlushProcedure.run(baseArgs, target, client) shouldBe Seq(
      Row("db1", "collection1", "submitted")
    )
    client.releaseCalls shouldBe Seq(("db1", "collection1"))
    client.flushCalls shouldBe Seq(("db1", Seq("collection1")))
  }

  test("compact returns submitted metadata without inventing plan counts") {
    val client = new FakeMilvusClient
    client.compaction = Success(MilvusCompactionInfo(42L, 3))

    CompactProcedure.run(baseArgs, target, client) shouldBe Seq(
      Row("db1", "collection1", 42L, 3L, "submitted", null, null, null, null)
    )
    client.compactionStateCalls shouldBe empty
  }

  test("compact waits for a successful Completed state") {
    val client = new FakeMilvusClient
    client.compaction = Success(MilvusCompactionInfo(42L, 3))
    client.compactionState = Success(
      MilvusCompactionState(
        CompactionState.Completed,
        executingPlanCount = 0L,
        timeoutPlanCount = 0L,
        completedPlanCount = 3L,
        failedPlanCount = 0L
      )
    )
    val args = baseArgs.copy(values = baseArgs.values + ("wait" -> true))

    CompactProcedure.run(args, target, client) shouldBe Seq(
      Row("db1", "collection1", 42L, 3L, "Completed", 0L, 3L, 0L, 0L)
    )
    client.compactionStateCalls shouldBe Seq(42L)
    client.compactionStateTimeouts should have size 1
    client.compactionStateTimeouts.head should be > 0L
  }

  test("compact rejects a completed operation with failed plans") {
    val client = new FakeMilvusClient
    client.compactionState = Success(
      MilvusCompactionState(
        CompactionState.Completed,
        executingPlanCount = 0L,
        timeoutPlanCount = 0L,
        completedPlanCount = 1L,
        failedPlanCount = 1L
      )
    )
    val args = baseArgs.copy(values = baseArgs.values + ("wait" -> true))

    val error = intercept[IllegalStateException] {
      CompactProcedure.run(args, target, client)
    }
    error.getMessage should include("1 failed")
  }

  test("describe preserves field order and expands indexes per field") {
    val client = new FakeMilvusClient
    client.collection = Success(collectionInfo)
    client.segments = Success(Seq(segment(1L), segment(2L)))
    client.loadState = Success(LoadState.LoadStateLoaded)
    client.indexes = Success(
      Seq(index("z_idx", 9L), index("a_idx", 8L))
    )

    val rows = DescribeProcedure.run(baseArgs, target, client)
    rows.size shouldBe 3
    rows.head shouldBe Row(
      "db1",
      "collection1",
      11L,
      2L,
      "LoadStateLoaded",
      100L,
      "id",
      "Int64",
      false,
      true,
      false,
      false,
      true,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    )
    rows(1) shouldBe vectorRow("a_idx", 8L)
    rows(2) shouldBe vectorRow("z_idx", 9L)
  }

  test("collection client failures remain procedure failures") {
    val client = new FakeMilvusClient
    client.released = Failure(new IllegalStateException("release failed"))

    val error = intercept[IllegalStateException] {
      ReleaseProcedure.run(baseArgs, target, client)
    }
    error.getMessage shouldBe "release failed"
  }

  private val collectionInfo = MilvusCollectionInfo(
    dbName = "db1",
    collectionName = "collection1",
    collectionID = 11L,
    schema = CollectionSchema(
      name = "collection1",
      fields = Seq(
        FieldSchema(
          fieldID = 100L,
          name = "id",
          isPrimaryKey = true,
          dataType = DataType.Int64,
          autoID = true
        ),
        FieldSchema(
          fieldID = 101L,
          name = "embedding",
          dataType = DataType.FloatVector,
          typeParams = Seq(KeyValuePair(key = "dim", value = "4")),
          isPartitionKey = true,
          isClusteringKey = true,
          nullable = true
        )
      )
    )
  )

  private def index(name: String, id: Long): MilvusIndexInfo =
    MilvusIndexInfo(
      indexName = name,
      indexID = id,
      params = Map.empty,
      fieldName = "embedding",
      indexedRows = 7L,
      totalRows = 10L,
      state = IndexState.InProgress,
      failReason = "",
      pendingIndexRows = 3L,
      minIndexVersion = 1,
      maxIndexVersion = 2
    )

  private def segment(id: Long): MilvusSegmentInfo =
    MilvusSegmentInfo(
      segmentID = id,
      collectionID = 11L,
      partitionID = 1L,
      numRows = 100L,
      state = SegmentState.Sealed,
      level = SegmentLevel.L1
    )

  private def vectorRow(indexName: String, indexID: Long): Row =
    Row(
      "db1",
      "collection1",
      11L,
      2L,
      "LoadStateLoaded",
      101L,
      "embedding",
      "FloatVector",
      true,
      false,
      true,
      true,
      false,
      4L,
      indexID,
      indexName,
      "InProgress",
      7L,
      10L,
      3L,
      ""
    )

  private final class FakeMilvusClient
      extends MilvusClient(target.connectionParams) {
    var loaded: Try[Status] = Success(Status())
    var loadState: Try[LoadState] = Success(LoadState.LoadStateNotLoad)
    var released: Try[Unit] = Success(())
    var flushed: Try[Status] = Success(Status())
    var compaction: Try[MilvusCompactionInfo] =
      Success(MilvusCompactionInfo(1L, 1))
    var compactionState: Try[MilvusCompactionState] = Success(
      MilvusCompactionState(CompactionState.Completed, 0L, 0L, 1L, 0L)
    )
    var collection: Try[MilvusCollectionInfo] = Success(collectionInfo)
    var segments: Try[Seq[MilvusSegmentInfo]] = Success(Seq.empty)
    var indexes: Try[Seq[MilvusIndexInfo]] = Success(Seq.empty)

    var loadCalls = Seq.empty[(String, String)]
    var loadStateCalls = Seq.empty[(String, String)]
    var loadStateTimeouts = Seq.empty[Long]
    var releaseCalls = Seq.empty[(String, String)]
    var flushCalls = Seq.empty[(String, Seq[String])]
    var compactionCalls = Seq.empty[(String, String)]
    var compactionStateCalls = Seq.empty[Long]
    var compactionStateTimeouts = Seq.empty[Long]

    override def loadCollection(
        dbName: String,
        collectionName: String
    ): Try[Status] = {
      loadCalls :+= ((dbName, collectionName))
      loaded
    }

    override def getLoadState(
        dbName: String,
        collectionName: String,
        timeoutMillis: Long
    ): Try[LoadState] = {
      loadStateCalls :+= ((dbName, collectionName))
      loadStateTimeouts :+= timeoutMillis
      loadState
    }

    override def releaseCollection(
        dbName: String,
        collectionName: String
    ): Try[Unit] = {
      releaseCalls :+= ((dbName, collectionName))
      released
    }

    override def flush(
        dbName: String,
        collectionNames: Seq[String]
    ): Try[Status] = {
      flushCalls :+= ((dbName, collectionNames))
      flushed
    }

    override def manualCompaction(
        dbName: String,
        collectionName: String
    ): Try[MilvusCompactionInfo] = {
      compactionCalls :+= ((dbName, collectionName))
      compaction
    }

    override def getCompactionState(
        compactionID: Long,
        timeoutMillis: Long
    ): Try[MilvusCompactionState] = {
      compactionStateCalls :+= compactionID
      compactionStateTimeouts :+= timeoutMillis
      compactionState
    }

    override def getCollectionInfo(
        dbName: String,
        collectionName: String
    ): Try[MilvusCollectionInfo] = collection

    override def getSegments(
        dbName: String,
        collectionName: String
    ): Try[Seq[MilvusSegmentInfo]] = segments

    override def describeIndexes(
        dbName: String,
        collectionName: String,
        fieldName: String,
        indexName: String,
        timeoutMillis: Long
    ): Try[Seq[MilvusIndexInfo]] = indexes
  }
}
