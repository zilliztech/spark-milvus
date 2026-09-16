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
        "register",
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
    assert(Procedures.byName("CREATE_INDEX").contains(CreateIndexProcedure))
    assert(Procedures.byName("nope").isEmpty)
    assert(
      RegisterProcedure.parameters.map(_.name) == Seq("collection", "staging")
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

  test("wait options reject ignored and invalid timeouts") {
    val ignored = ProcedureArgs(
      Map("collection" -> "c", "timeout_seconds" -> 1L),
      Map.empty
    )
    assertThrows[IllegalArgumentException](
      ProcedureSupport.waitOptions(ignored, "test")
    )

    val invalid = ProcedureArgs(
      Map(
        "collection" -> "c",
        "wait" -> true,
        "timeout_seconds" -> 0L
      ),
      Map.empty
    )
    assertThrows[IllegalArgumentException](
      ProcedureSupport.waitOptions(invalid, "test")
    )
  }

  test("bounded polling returns a terminal value and reports a timeout") {
    var probes = 0
    var now = 0L
    var budgets = Vector.empty[Long]
    val done = ProcedureSupport.await(
      "test operation",
      timeoutSeconds = 1L,
      pollMillis = 1L,
      nanoTime = () => now,
      sleep = millis => now += millis * 1000000L
    )(remainingMillis => {
      budgets :+= remainingMillis
      probes += 1
      if (probes == 1) "loading" else "done"
    }) {
      case "done" => WaitDecision.Done
      case _      => WaitDecision.Continue
    }
    assert(done == "done")
    assert(budgets == Vector(1000L, 999L))

    var timeoutNow = 0L
    val timeout = intercept[IllegalStateException](
      ProcedureSupport.await(
        "slow operation",
        timeoutSeconds = 1L,
        pollMillis = 1L,
        nanoTime = () => {
          val current = timeoutNow
          timeoutNow += 1000000000L
          current
        },
        sleep = _ => ()
      )(_ => "loading")(_ => WaitDecision.Continue)
    )
    assert(timeout.getMessage.contains("did not complete"))
    assert(timeout.getMessage.contains("loading"))

    var lateNow = 0L
    val lateSuccess = intercept[IllegalStateException](
      ProcedureSupport.await(
        "late operation",
        timeoutSeconds = 1L,
        pollMillis = 1L,
        nanoTime = () => {
          val current = lateNow
          lateNow += 1000000000L
          current
        },
        sleep = _ => ()
      )(_ => "done")(_ => WaitDecision.Done)
    )
    assert(lateSuccess.getMessage.contains("did not complete"))
  }

  test("bounded polling preserves interruption while sleeping") {
    try {
      val interrupted = intercept[InterruptedException] {
        ProcedureSupport.await(
          "interrupted operation",
          timeoutSeconds = 1L,
          pollMillis = 1L,
          nanoTime = () => 0L,
          sleep = _ => throw new InterruptedException("cancelled")
        )(_ => "loading")(_ => WaitDecision.Continue)
      }
      assert(interrupted.getMessage == "cancelled")
      assert(Thread.currentThread().isInterrupted)
    } finally {
      Thread.interrupted()
    }
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
