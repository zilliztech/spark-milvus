package com.zilliz.spark.connector.procedure

import org.scalatest.funsuite.AnyFunSuite

class RegisterProcedureTest extends AnyFunSuite {

  test("'db.coll' names both, 'coll' leaves the database to the connection") {
    assert(RegisterProcedure.splitCollection("db1.c") == (Some("db1"), "c"))
    assert(RegisterProcedure.splitCollection(" c ") == (None, "c"))
  }

  test("an empty or malformed collection is refused") {
    intercept[IllegalArgumentException](RegisterProcedure.splitCollection(""))
    intercept[IllegalArgumentException](RegisterProcedure.splitCollection(".c"))
    intercept[IllegalArgumentException](RegisterProcedure.splitCollection("d."))
    intercept[IllegalArgumentException](
      RegisterProcedure.splitCollection("a.b.c")
    )
  }

  test("register is the one procedure the registry knows") {
    assert(Procedures.byName("register").contains(RegisterProcedure))
    assert(Procedures.byName("REGISTER").contains(RegisterProcedure))
    assert(Procedures.byName("nope").isEmpty)
    assert(
      RegisterProcedure.parameters.map(_.name) == Seq("collection", "staging")
    )
  }
}
