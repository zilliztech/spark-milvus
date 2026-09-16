package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import com.zilliz.spark.connector.procedure.{
  Parameter,
  Procedure,
  ProcedureArgs,
  RegisterProcedure
}

class MilvusSqlParserTest extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("milvus-sql-parser")
    .config("spark.ui.enabled", "false")
    .config(
      "spark.sql.extensions",
      classOf[MilvusSparkSessionExtensions].getName
    )
    .getOrCreate()

  override protected def afterAll(): Unit = spark.stop()

  private def parse(sql: String) = spark.sessionState.sqlParser.parsePlan(sql)

  test("only a statement starting with CALL milvus. is ours") {
    assert(MilvusSqlParserBase.isProcedureCall("CALL milvus.system.register()"))
    assert(MilvusSqlParserBase.isProcedureCall("  call MILVUS.system.x(1)"))
    assert(
      MilvusSqlParserBase.isProcedureCall(
        "-- a comment\n/* block */ CALL milvus.system.register()"
      )
    )
    assert(!MilvusSqlParserBase.isProcedureCall("CALL other.proc()"))
    assert(!MilvusSqlParserBase.isProcedureCall("SELECT 'call milvus.x'"))
  }

  test("a CALL becomes a CallProcedure with checked arguments and options") {
    val plan = parse(
      """CALL milvus.system.register('db1.coll_a',
        |  staging => 'root/staging/job-1',
        |  `milvus.uri` => 'http://localhost:19530',
        |  `fs.bucket_name` => 'b')""".stripMargin
    )
    val call = plan.asInstanceOf[CallProcedure]
    assert(call.procedure eq RegisterProcedure)
    assert(call.args.string("collection") == "db1.coll_a")
    assert(call.args.string("staging") == "root/staging/job-1")
    assert(
      call.args.options == Map(
        "milvus.uri" -> "http://localhost:19530",
        "fs.bucket_name" -> "b"
      )
    )
    assert(
      call.output.map(_.name) == Seq(
        "job_id",
        "segment_id",
        "manifest_version",
        "status"
      )
    )
  }

  test("named arguments may replace positional ones, in any case") {
    val call = parse(
      "CALL milvus.system.REGISTER(Staging => 'p', COLLECTION => 'c')"
    ).asInstanceOf[CallProcedure]
    assert(call.args.string("collection") == "c")
    assert(call.args.string("staging") == "p")

    val options = parse(
      "CALL milvus.system.register('c', 'p', `MILVUS.URI` => 'http://localhost:19530')"
    ).asInstanceOf[CallProcedure]
    assert(
      options.args.options == Map(
        "MILVUS.URI" -> "http://localhost:19530"
      )
    )
  }

  test("every management procedure is reachable through the shared parser") {
    val calls = Seq(
      "create_snapshot" ->
        "CALL milvus.system.create_snapshot('db.c', 's', description => 'daily', compaction_protection_seconds => 60)",
      "drop_snapshot" ->
        "CALL milvus.system.drop_snapshot('db.c', 's')",
      "list_snapshots" ->
        "CALL milvus.system.list_snapshots('db.c')",
      "describe_snapshot" ->
        "CALL milvus.system.describe_snapshot('db.c', 's')",
      "create_index" ->
        "CALL milvus.system.create_index('db.c', 'vector', 'vector_idx', wait => true, timeout_seconds => 30)",
      "drop_index" ->
        "CALL milvus.system.drop_index('db.c', 'vector_idx')",
      "load" ->
        "CALL milvus.system.load('db.c', wait => true, timeout_seconds => 30)",
      "release" ->
        "CALL milvus.system.release('db.c')",
      "flush" ->
        "CALL milvus.system.flush('db.c')",
      "compact" ->
        "CALL milvus.system.compact('db.c', wait => false)",
      "describe" ->
        "CALL milvus.system.describe('db.c')"
    )

    val parsed = calls.map { case (name, sql) =>
      val call = parse(sql).asInstanceOf[CallProcedure]
      assert(call.procedure.name == name)
      name -> call
    }.toMap

    assert(
      parsed("create_snapshot").args.long(
        "compaction_protection_seconds"
      ) == 60L
    )
    assert(parsed("create_index").args.boolean("wait"))
    assert(parsed("create_index").args.long("timeout_seconds") == 30L)
    assert(!parsed("compact").args.boolean("wait"))
  }

  test("string escapes: doubled quote and backslash") {
    val call = parse(
      """CALL milvus.system.register('it''s', staging => "a\"b")"""
    ).asInstanceOf[CallProcedure]
    assert(call.args.string("collection") == "it's")
    assert(call.args.string("staging") == "a\"b")
  }

  test("every mistake is refused at parse time with the parameters named") {
    def refused(sql: String): String =
      intercept[IllegalArgumentException](parse(sql)).getMessage
    assert(refused("CALL milvus.system.nope('c')").contains("known: register"))
    assert(
      refused("CALL milvus.other.register('c')").contains("milvus.system")
    )
    assert(
      refused("CALL milvus.system.register('c')")
        .contains("missing argument 'staging'")
    )
    assert(
      refused("CALL milvus.system.register('c', staging => 'p', extra => 1)")
        .contains("unknown argument 'extra'")
    )
    assert(
      refused("CALL milvus.system.register('c', staging => 42)")
        .contains("must be STRING, got an integer")
    )
    assert(
      refused("CALL milvus.system.register('c', 'p', 'q')")
        .contains("3 positional arguments")
    )
    assert(
      refused("CALL milvus.system.register(staging => 'p', 'c')")
        .contains("positional arguments must come before")
    )
    assert(
      refused(
        "CALL milvus.system.register('c', staging => 'p', staging => 'q')"
      )
        .contains("given twice")
    )
    assert(
      refused(
        "CALL milvus.system.register('c', 'p', `milvus.uri` => 'a', `MILVUS.URI` => 'b')"
      ).contains("option 'MILVUS.URI' given twice")
    )
    assert(
      refused(
        "CALL milvus.system.register('c', collection => 'd', staging => 'p')"
      )
        .contains("both by position and by name")
    )
    assert(refused("CALL milvus.system.register('c'").contains("syntax error"))
  }

  test("everything else still goes to Spark's parser") {
    assert(parse("SELECT 1 AS x").isInstanceOf[Project])
    // Spark 4's own CALL is not ours: it parses in Spark's grammar and never
    // becomes a CallProcedure.
    assert(!parse("CALL other.system.proc()").isInstanceOf[CallProcedure])
  }

  test("a CallProcedure runs on the driver and its rows come back as a table") {
    val fake = new Procedure {
      val name = "fake"
      val parameters = Seq(Parameter("n", LongType))
      val outputSchema = StructType(
        Seq(StructField("i", LongType), StructField("s", StringType))
      )
      def run(args: ProcedureArgs): Seq[Row] =
        (1L to args.values("n").asInstanceOf[Long]).map(i => Row(i, s"row-$i"))
    }
    val plan = CallProcedure(fake, ProcedureArgs(Map("n" -> 3L), Map.empty))
    val rows =
      spark.sessionState.executePlan(plan).executedPlan.executeCollect()
    assert(rows.length == 3)
    assert(rows.map(_.getLong(0)).toSeq == Seq(1L, 2L, 3L))
    assert(rows(2).getUTF8String(1).toString == "row-3")
  }

  test("a real CALL reaches execution: the options are checked there") {
    // The parse succeeds; running needs a connection, which the options lack.
    val e = intercept[Exception](
      spark.sql("CALL milvus.system.register('c', staging => 'root/staging/j')")
    )
    assert(e.getMessage.contains("must be set"))
  }
}
