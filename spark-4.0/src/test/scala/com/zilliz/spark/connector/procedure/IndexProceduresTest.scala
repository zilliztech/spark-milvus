package com.zilliz.spark.connector.procedure

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.types.{BooleanType, LongType, StringType}
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.client.api.{
  MilvusClient,
  MilvusConnectionParams,
  MilvusIndexInfo
}
import io.milvus.grpc.common.{IndexState, Status}

class IndexProceduresTest extends AnyFunSuite with Matchers {
  private val target = ProcedureTarget(
    "db1",
    "collection1",
    MilvusConnectionParams("http://localhost:19530")
  )
  private val baseArgs =
    ProcedureArgs(Map("collection" -> "db1.collection1"), Map.empty)

  test("index procedures expose the designed parameters and schemas") {
    CreateIndexProcedure.parameters shouldBe Seq(
      Parameter("collection", StringType),
      Parameter("field", StringType),
      Parameter("index_name", StringType),
      Parameter("index_type", StringType, required = false),
      Parameter("metric_type", StringType, required = false),
      Parameter("params", StringType, required = false),
      Parameter("wait", BooleanType, required = false),
      Parameter("timeout_seconds", LongType, required = false)
    )
    CreateIndexProcedure.outputSchema.fieldNames shouldBe Array(
      "database",
      "collection",
      "field",
      "index_name",
      "state"
    )
    DropIndexProcedure.parameters.map(_.name) shouldBe Seq(
      "collection",
      "index_name"
    )
    DropIndexProcedure.outputSchema.fieldNames shouldBe Array(
      "database",
      "collection",
      "index_name",
      "status"
    )
  }

  test("create_index submits defaults without polling") {
    val client = new FakeMilvusClient
    val args = createArgs()

    CreateIndexProcedure.run(args, target, client) shouldBe Seq(
      Row("db1", "collection1", "embedding", "vector_idx", "submitted")
    )
    client.createCalls shouldBe Seq(
      (
        "db1",
        "collection1",
        "embedding",
        Map("index_type" -> "AUTOINDEX", "metric_type" -> "L2"),
        "vector_idx"
      )
    )
    client.describeCalls shouldBe empty
  }

  test("create_index forwards explicit parameters and waits for Finished") {
    val client = new FakeMilvusClient
    client.indexes = Success(Seq(index(IndexState.Finished)))
    val args = createArgs(
      "index_type" -> "IVF_FLAT",
      "metric_type" -> "COSINE",
      "params" -> "{\"nlist\":128}",
      "wait" -> true,
      "timeout_seconds" -> 10L
    )

    CreateIndexProcedure.run(args, target, client) shouldBe Seq(
      Row("db1", "collection1", "embedding", "vector_idx", "Finished")
    )
    client.createCalls.head._4 shouldBe Map(
      "index_type" -> "IVF_FLAT",
      "metric_type" -> "COSINE",
      "params" -> "{\"nlist\":128}"
    )
    client.describeCalls shouldBe Seq(
      ("db1", "collection1", "embedding", "vector_idx")
    )
    client.describeTimeouts shouldBe Seq(10000L)
  }

  test("create_index reports Milvus terminal failure") {
    val client = new FakeMilvusClient
    client.indexes = Success(Seq(index(IndexState.Failed, "bad params")))

    val error = intercept[IllegalStateException] {
      CreateIndexProcedure.run(createArgs("wait" -> true), target, client)
    }
    error.getMessage should include("bad params")
  }

  test("create_index validates required names before sending an RPC") {
    val client = new FakeMilvusClient
    val error = intercept[IllegalArgumentException] {
      CreateIndexProcedure.run(
        createArgs("field" -> "  "),
        target,
        client
      )
    }
    error.getMessage should include("field")
    client.createCalls shouldBe empty
  }

  test("drop_index calls Milvus and reports completion") {
    val client = new FakeMilvusClient
    val args = baseArgs.copy(
      values = baseArgs.values + ("index_name" -> " vector_idx ")
    )

    DropIndexProcedure.run(args, target, client) shouldBe Seq(
      Row("db1", "collection1", " vector_idx ", "dropped")
    )
    client.dropCalls shouldBe Seq(("db1", "collection1", " vector_idx "))
  }

  test("index client failures remain procedure failures") {
    val client = new FakeMilvusClient
    client.created = Failure(new IllegalStateException("create failed"))

    val error = intercept[IllegalStateException] {
      CreateIndexProcedure.run(createArgs(), target, client)
    }
    error.getMessage shouldBe "create failed"
  }

  private def createArgs(entries: (String, Any)*): ProcedureArgs =
    baseArgs.copy(
      values = baseArgs.values ++ Map[String, Any](
        "field" -> "embedding",
        "index_name" -> "vector_idx"
      ) ++ entries
    )

  private def index(
      state: IndexState,
      failReason: String = ""
  ): MilvusIndexInfo =
    MilvusIndexInfo(
      indexName = "vector_idx",
      indexID = 7L,
      params = Map.empty,
      fieldName = "embedding",
      indexedRows = 10L,
      totalRows = 10L,
      state = state,
      failReason = failReason,
      pendingIndexRows = 0L,
      minIndexVersion = 1,
      maxIndexVersion = 1
    )

  private final class FakeMilvusClient
      extends MilvusClient(target.connectionParams) {
    var created: Try[Status] = Success(Status())
    var indexes: Try[Seq[MilvusIndexInfo]] = Success(Seq.empty)
    var dropped: Try[Unit] = Success(())

    var createCalls = Seq.empty[
      (String, String, String, Map[String, String], String)
    ]
    var describeCalls = Seq.empty[(String, String, String, String)]
    var describeTimeouts = Seq.empty[Long]
    var dropCalls = Seq.empty[(String, String, String)]

    override def createIndex(
        dbName: String,
        collectionName: String,
        fieldName: String,
        params: Map[String, String],
        indexName: String
    ): Try[Status] = {
      createCalls :+= (
        dbName,
        collectionName,
        fieldName,
        params,
        indexName
      )
      created
    }

    override def describeIndexes(
        dbName: String,
        collectionName: String,
        fieldName: String,
        indexName: String,
        timeoutMillis: Long
    ): Try[Seq[MilvusIndexInfo]] = {
      describeCalls :+= ((dbName, collectionName, fieldName, indexName))
      describeTimeouts :+= timeoutMillis
      indexes
    }

    override def dropIndex(
        dbName: String,
        collectionName: String,
        indexName: String
    ): Try[Unit] = {
      dropCalls :+= ((dbName, collectionName, indexName))
      dropped
    }
  }
}
