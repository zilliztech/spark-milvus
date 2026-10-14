package com.zilliz.spark.connector.procedure

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Assertion

import com.zilliz.milvus.client.api.{MilvusClient, MilvusConnectionParams}

class RegisterProcedureTest extends AnyFunSuite {

  test("'db.coll' names both, 'coll' leaves the database to the connection") {
    assert(
      ProcedureSupport.parseCollection("db1.c", "test") ==
        (Some("db1"), "c")
    )
    assert(ProcedureSupport.parseCollection(" c ", "test") == (None, "c"))
  }

  test("an empty or malformed collection is refused") {
    intercept[IllegalArgumentException](
      ProcedureSupport.parseCollection("", "test")
    )
    intercept[IllegalArgumentException](
      ProcedureSupport.parseCollection(".c", "test")
    )
    intercept[IllegalArgumentException](
      ProcedureSupport.parseCollection("d.", "test")
    )
    intercept[IllegalArgumentException](
      ProcedureSupport.parseCollection("a.b.c", "test")
    )
  }

  test("the registry contains every public procedure exactly once") {
    assert(
      Procedures.all.map(_.name) == Seq(
        "build_index",
        "write_snapshot",
        "restore_snapshot",
        "register",
        "cleanup_staging",
        "create_snapshot",
        "drop_snapshot",
        "list_snapshots",
        "describe_snapshot",
        "create_index",
        "drop_index",
        "load",
        "release",
        "flush",
        "compact",
        "describe"
      )
    )
    assert(Procedures.all.map(_.name).distinct.size == Procedures.all.size)
    assert(Procedures.byName("register").contains(RegisterProcedure))
    assert(Procedures.byName("REGISTER").contains(RegisterProcedure))
    assert(
      Procedures
        .byName("CLEANUP_STAGING")
        .contains(CleanupStagingProcedure)
    )
    assert(Procedures.byName("CREATE_INDEX").contains(CreateIndexProcedure))
    assert(Procedures.byName("nope").isEmpty)
    assert(
      RegisterProcedure.parameters.map(_.name) == Seq("collection", "staging")
    )
    assert(
      CleanupStagingProcedure.parameters.map(_.name) ==
        Seq("collection", "retention_seconds", "dry_run")
    )
  }

  test("a procedure target requires a URI and resolves the collection") {
    val args = ProcedureArgs(
      Map("collection" -> "db1.c"),
      Map(
        "MILVUS.URI" -> "http://localhost:19530",
        "MILVUS.DATABASE.NAME" -> "wrong_database"
      )
    )
    val target = ProcedureSupport.target(args, "test")
    assert(target.database == "db1")
    assert(target.collection == "c")
    assert(target.connectionParams.databaseName == "db1")
    assert(target.connectionParams.uri == "http://localhost:19530")

    val e = intercept[IllegalArgumentException](
      ProcedureSupport.target(
        ProcedureArgs(Map("collection" -> "c"), Map.empty),
        "test"
      )
    )
    assert(e.getMessage.contains("milvus.uri"))

    val configuredDatabase = ProcedureSupport.target(
      ProcedureArgs(
        Map("collection" -> "c"),
        Map(
          "MILVUS.URI" -> "http://localhost:19530",
          "MILVUS.DATABASE.NAME" -> "analytics"
        )
      ),
      "test"
    )
    assert(configuredDatabase.database == "analytics")

    assertThrows[IllegalArgumentException](
      ProcedureSupport.target(
        ProcedureArgs(
          Map("collection" -> "c"),
          Map(
            "milvus.uri" -> "http://localhost:19530",
            "MILVUS.URI" -> "http://other:19530"
          )
        ),
        "test"
      )
    )
  }

  test("procedure clients close after success and failure") {
    val args = ProcedureArgs(
      Map("collection" -> "db1.c"),
      Map("milvus.uri" -> "http://localhost:19530")
    )

    def verify(run: MilvusClient => Assertion): Unit = {
      val client = new ClosingClient
      run(client)
      assert(client.closed)
    }

    verify { client =>
      val value = ProcedureSupport.withClient(args, "test", _ => client) {
        (_, _) => 7
      }
      assert(value == 7)
    }

    verify { client =>
      val error = intercept[IllegalStateException](
        ProcedureSupport.withClient(args, "test", _ => client) { (_, _) =>
          throw new IllegalStateException("failed")
        }
      )
      assert(error.getMessage == "failed")
    }
  }

  private final class ClosingClient
      extends MilvusClient(
        MilvusConnectionParams("http://localhost:19530", databaseName = "db1")
      ) {
    var closed = false
    override def close(): Unit = closed = true
  }
}
