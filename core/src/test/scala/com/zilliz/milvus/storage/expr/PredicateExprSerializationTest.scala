package com.zilliz.milvus.storage.expr

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.expr.ComparisonOperator._
import com.zilliz.milvus.storage.expr.Literal._
import io.milvus.grpc.schema.{DataType => MilvusDataType}

class PredicateExprSerializationTest extends AnyFunSuite with Matchers {

  test("an expression and its typed literals survive Java serialization") {
    val field = FieldRef(100L, MilvusDataType.VarChar)
    val expression: PredicateExpr = And(
      In(field, Vector(StringValue("北京"), NullValue)),
      Not(Comparison(field, EqualNullSafe, StringValue("上海")))
    )

    roundTrip(expression) shouldBe expression
  }

  test("a bitmap survives Java serialization") {
    val bitmap = Bitmap.fromWords(65, Array(3L, 1L))

    roundTrip(bitmap) shouldBe bitmap
  }

  private def roundTrip[T](value: T): T = {
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(value)
    finally output.close()

    val input = new ObjectInputStream(
      new ByteArrayInputStream(bytes.toByteArray)
    )
    try input.readObject().asInstanceOf[T]
    finally input.close()
  }
}
