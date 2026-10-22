package com.zilliz.spark.connector.search

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  Literal
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.types.{
  ArrayType,
  FloatType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.milvus.storage.snapshot.{Snapshot, SnapshotOrigin}
import com.zilliz.milvus.storage.vector.RankingFunction
import com.zilliz.spark.connector.extensions.{
  MilvusNearestByJoinExec,
  MilvusNearestByJoinFrameExec,
  SparkNearestBy
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.read.MilvusScan
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class SearchDriverBoundaryTest extends AnyFunSuite {
  import SearchDriverBoundaryTest._

  private def serialized(value: AnyRef): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(value)
    finally output.close()
    bytes.toByteArray
  }

  private def restored[A](bytes: Array[Byte]): A = {
    val input = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try input.readObject().asInstanceOf[A]
    finally input.close()
  }

  private def assertDriverCredentialsAbsent(bytes: Array[Byte]): Unit = {
    val text = new String(bytes, StandardCharsets.ISO_8859_1)
    Seq(Token, Endpoint, TlsPath).foreach(value =>
      assert(!text.contains(value))
    )
  }

  test(
    "query classification functions serialize without their driver scan or credentials"
  ) {
    val searcher = NearestByInput.searcher(
      spark = null,
      input = NearestByInput.MilvusTable(driverScan(), Seq.empty, "vector"),
      approx = false,
      k = 1,
      metric = MetricType.L2,
      function = null,
      metrics = Map.empty
    )
    // MilvusScan is not serializable. Capturing the searcher or scan would fail
    // here even before the credential markers could reach the executor.
    val bytes = serialized((searcher.judge, searcher.packedBytes))
    assertDriverCredentialsAbsent(bytes)
    val (judge, packedBytes) =
      restored[(Any => QueryVector, Int => Long)](bytes)
    judge(new GenericArrayData(Array(1f, 2f))) match {
      case QueryVector.Searched(vector) =>
        assert(vector.sameElements(Array(1f, 2f)))
      case other => fail(s"Expected a searchable vector, got $other")
    }
    assert(packedBytes(2) == 8L)
  }

  test("serialized nearest-by plans omit the driver scan and driver fallback") {
    val vector =
      Literal.create(null, ArrayType(FloatType, containsNull = false))
    val plan = MilvusNearestByJoinExec(
      left = EmptyPlan(),
      scan = driverScan(),
      baseOutput = Seq.empty,
      baseColumns = Seq.empty,
      joinType = Inner,
      approx = false,
      k = 1,
      metric = MetricType.L2,
      queryVector = vector,
      vectorColumn = "vector",
      spark = new DriverFallback
    )
    val bytes = serialized(plan)
    assertDriverCredentialsAbsent(bytes)
    val copy = restored[MilvusNearestByJoinExec](bytes)
    assert(copy.scan == null)
    assert(copy.spark == null)
    assert(copy.vectorColumn == "vector")

    val frame = MilvusNearestByJoinFrameExec(
      left = EmptyPlan(),
      right = EmptyPlan(),
      baseOutput = Seq.empty,
      joinType = Inner,
      approx = false,
      k = 1,
      metric = MetricType.L2,
      queryVector = vector,
      baseVector = vector,
      reason = "computed input",
      spark = new DriverFallback
    )
    val frameBytes = serialized(frame)
    assertDriverCredentialsAbsent(frameBytes)
    assert(restored[MilvusNearestByJoinFrameExec](frameBytes).spark == null)
  }
}

private object SearchDriverBoundaryTest {
  val Token = "storage-secret-must-stay-on-driver"
  val Endpoint = "http://driver-only-endpoint:19530"
  val TlsPath = "/driver-only/client-private.key"

  def driverScan(): MilvusScan = {
    val schema = StructType(Seq(StructField("vector", ArrayType(FloatType))))
    val snapshot = Snapshot(
      name = "fixed",
      collectionId = 1L,
      createdAt = Some(1L),
      schema = CollectionSchema(fields =
        Seq(
          FieldSchema(
            fieldID = 100L,
            name = "vector",
            dataType = DataType.FloatVector,
            typeParams = Seq(KeyValuePair("dim", "2"))
          )
        )
      ),
      partitionIds = Seq.empty,
      segments = Seq.empty,
      origin = SnapshotOrigin.Options,
      bucket = ""
    )
    val options = new CaseInsensitiveStringMap(
      Map(
        "fs.address" -> Endpoint,
        "fs.secret_access_key" -> Token,
        "fs.ssl_ca_cert" -> TlsPath
      ).asJava
    )
    new MilvusScan(schema, options, snapshot) {
      override def planInputPartitions(): Array[InputPartition] = Array.empty
    }
  }

  final case class EmptyPlan() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty
    override protected def doExecute(): RDD[InternalRow] =
      throw new AssertionError("Serialization must not execute the plan")
  }

  // Deliberately not serializable: this adapter only runs on the driver.
  final class DriverFallback extends SparkNearestBy {
    val token: String = Token

    override def function: RankingFunction =
      throw new AssertionError("Serialization must not evaluate a ranking")

    override def over(base: LogicalPlan, ranking: Expression): SparkNearestBy =
      throw new AssertionError("Serialization must not rewrite a fallback")

    override def execute(
        spark: SparkSession,
        queries: RDD[InternalRow],
        queryOutput: Seq[Attribute],
        output: Seq[Attribute]
    ): RDD[InternalRow] =
      throw new AssertionError("Serialization must not run a fallback")
  }
}
